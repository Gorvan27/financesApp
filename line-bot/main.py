"""
Shop Accountant — LINE-бот учёта расходов DJ Shop.

Цепочка: LINE-группа → /callback → пачка сообщений одного отправителя
→ Claude разбирает слипы / PDF / скрины заказов → запись в Google Sheet
(тот же webhook, что у приложения) с категорией «DJ Shop» → ответ в группе.

Режим DRY_RUN=1 (по умолчанию): бот только отвечает «записал бы», в таблицу не пишет.
"""
import asyncio, base64, hashlib, hmac, io, json, logging, os, re, sqlite3, time
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import FastAPI, Request, Response, BackgroundTasks
from PIL import Image

log = logging.getLogger("shopbot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

LINE_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_TOKEN = os.environ.get("LINE_CHANNEL_TOKEN", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
SHEETS_URL = os.environ.get("SHEETS_URL", "")
GROUP_ID = os.environ.get("LINE_GROUP_ID", "")          # пусто → бот назовёт ID группы
DRY_RUN = os.environ.get("DRY_RUN", "1") != "0"
CATEGORY = os.environ.get("CATEGORY", "DJ Shop")
BATCH_WAIT = int(os.environ.get("BATCH_WAIT", "40"))   # сек тишины от отправителя
DB_PATH = os.environ.get("DB_PATH", "/data/shopbot.db" if os.path.isdir("/data") else "shopbot.db")
TH = timezone(timedelta(hours=7))

app = FastAPI()

# ── хранилище: дубли по номеру транзакции + ожидающие подтверждения ──
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute("create table if not exists seen(ref text primary key, tx_id text, ts integer)")
db.execute("create table if not exists pending(msg_id text primary key, payload text, ts integer)")
db.execute("create table if not exists written(msg_id text primary key, tx_id text, ts integer)")
db.execute("delete from seen where ref like '%XX%' or ref like '%xx%'")  # старые «ref» = маска счёта
db.commit()
db.commit()

# ── пачки: (group, user) → {"items": [...], "task": Task} ──
batches: dict = {}


# ════════ LINE ════════
def verify(body: bytes, sig: str) -> bool:
    mac = hmac.new(LINE_SECRET.encode(), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(mac).decode(), sig or "")


async def line_content(message_id: str) -> bytes:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(f"https://api-data.line.me/v2/bot/message/{message_id}/content",
                        headers={"Authorization": f"Bearer {LINE_TOKEN}"})
        r.raise_for_status()
        return r.content


async def line_push(to: str, text: str, quote: str | None = None) -> str | None:
    """Отправляет сообщение в группу, возвращает ID отправленного (для ответов-цитат)."""
    msg = {"type": "text", "text": text[:4900]}
    if quote:
        msg["quoteToken"] = quote
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post("https://api.line.me/v2/bot/message/push",
                         headers={"Authorization": f"Bearer {LINE_TOKEN}"},
                         json={"to": to, "messages": [msg]})
        if r.status_code != 200:
            log.error("push failed %s %s", r.status_code, r.text[:300])
            return None
        sent = r.json().get("sentMessages") or []
        return sent[0]["id"] if sent else None


async def send_many(group: str, reply_token: str | None, texts: list) -> list:
    """До 5 сообщений одним вызовом. Сначала reply (бесплатно), если токен истёк — push."""
    msgs = [{"type": "text", "text": t[:4900]} for t in texts[:5]]
    async with httpx.AsyncClient(timeout=30) as c:
        r = None
        if reply_token:
            r = await c.post("https://api.line.me/v2/bot/message/reply",
                             headers={"Authorization": f"Bearer {LINE_TOKEN}"},
                             json={"replyToken": reply_token, "messages": msgs})
        if not r or r.status_code != 200:
            r = await c.post("https://api.line.me/v2/bot/message/push",
                             headers={"Authorization": f"Bearer {LINE_TOKEN}"},
                             json={"to": group, "messages": msgs})
        if r.status_code != 200:
            log.error("send failed %s %s", r.status_code, r.text[:300])
            return []
        return [m.get("id") for m in r.json().get("sentMessages") or []]


async def line_reply(token: str, text: str) -> str | None:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post("https://api.line.me/v2/bot/message/reply",
                         headers={"Authorization": f"Bearer {LINE_TOKEN}"},
                         json={"replyToken": token, "messages": [{"type": "text", "text": text[:4900]}]})
        sent = (r.json().get("sentMessages") or []) if r.status_code == 200 else []
        return sent[0]["id"] if sent else None


# ════════ файлы ════════
def to_jpeg(data: bytes, max_side=1400, q=80) -> bytes:
    im = Image.open(io.BytesIO(data)).convert("RGB")
    im.thumbnail((max_side, max_side))
    out = io.BytesIO(); im.save(out, "JPEG", quality=q)
    return out.getvalue()


def pdf_first_page_jpeg(data: bytes) -> bytes | None:
    try:
        import pypdfium2 as pdfium
        page = pdfium.PdfDocument(data)[0]
        img = page.render(scale=2).to_pil().convert("RGB")
        out = io.BytesIO(); img.thumbnail((1400, 1400)); img.save(out, "JPEG", quality=80)
        return out.getvalue()
    except Exception as e:
        log.warning("pdf render failed: %s", e)
        return None


# ════════ Claude ════════
SYSTEM = f"""Ты бухгалтер магазина DJ Shop (Паттайя, Таиланд). Тебе дают пачку сообщений
одного человека из рабочей LINE-группы: картинки (слипы банков K PLUS, KBIZ, Krungsri и др.,
скрины заказов Shopee/Lazada/Alibaba/1688, переписки), PDF-инвойсы и текстовые подписи.

Задача — найти ОПЛАТЫ и записать каждую как расход в THB.
Подпись ([подпись]) относится к вложению(ям), которые идут НЕПОСРЕДСТВЕННО ПЕРЕД ней.
Если подпись стоит в самом начале, до всех вложений (или помечена «написана до картинки»), —
она относится к следующему за ней вложению.

ГЛАВНОЕ ПРАВИЛО: оплата = ТОЛЬКО банковский документ о переводе:
  • слип банковского приложения (K PLUS, KBIZ, Krungsri, SCB, Bangkok Bank, PromptPay…):
    есть отправитель → получатель, «จำนวนเงิน»/сумма и номер операции;
  • PDF-документ (подтверждение перевода, e-slip банка, оплаченный инвойс/чек/ใบเสร็จ/ใบกำกับภาษี):
    сумма к оплате/итог (ยอดรวม / Total / Grand total) — это оплата, source="bank_pdf".
    Если в той же пачке есть слип на ту же сумму — это одна оплата, не две.
  Сколько банковских слипов — столько оплат, не больше.
ВСЁ ОСТАЛЬНОЕ — только КОНТЕКСТ, из него сумму НЕ брать никогда:
  чеки магазинов и почты, кассовые ленты, скрины заказов Shopee/Lazada/1688, корзины,
  накладные доставки, переписки, фото товара. Используй их, чтобы понять, ЗА ЧТО оплата:
  сопоставь по сумме (например, чек почты ฿67+฿3=฿70 → объясняет слип на 70) и по смыслу.
  Знак ฿ перед числом — это символ бата, а не цифра 8 («฿70» = 70, не 870).
Если банковского слипа в пачке нет вовсе, но есть чек — верни одну запись с
  source="receipt_only" (возможно, оплата наличными), сумму с чека, confidence="low".

- Даты в тайском формате: «16 ต.ค. 69» / 2569 — это буддийский год, вычти 543 (69 → 2026).
  Тайские месяцы: ม.ค.=01 ก.พ.=02 มี.ค.=03 เม.ย.=04 พ.ค.=05 มิ.ย.=06 ก.ค.=07 ส.ค.=08 ก.ย.=09
  ต.ค.=10 พ.ย.=11 ธ.ค.=12. Внимательно: ต.ค. — октябрь, ก.ค. — июль, พ.ค. — май.
  Дата оплаты — со слипа, не с чека.
- ref — номер ОПЕРАЦИИ со слипа (หมายเลขอ้างอิง / เลขที่รายการ / Transaction ID), обычно длинный,
  например «KSA00000001007829174». НЕ номер счёта (счета маскированы: «XXX-XXX-5611», «xxx-x-x1244-x»).
- description — коротко НА АНГЛИЙСКОМ, ЗА ЧТО платили: из подписи человека (Wood → «Wood», Water → «Water») и/или из контекста («BOSCH drill, Shopee», «Delivery, Thailand Post»).
  Название банка или способа оплаты (PromptPay, KBIZ) — НЕ описание. Не выдумывай товар.
  Если ничего не известно — пустая строка.
- context_indexes — номера вложений-контекста, которые относятся к этой оплате.
- internal_transfer=true, если деньги переведены на собственный счёт отправителя/компании
  (тот же человек или та же компания с обеих сторон), это не расход.
- confidence: high — сумма и дата чётко видны на слипе; low — что-то обрезано, размыто или неясно.
- image_index — номер вложения (с 0), где лежит банковский слип этой оплаты.
Верни результат строго через инструмент record_payments."""

TOOL = {
    "name": "record_payments",
    "description": "Записать найденные оплаты",
    "input_schema": {
        "type": "object",
        "properties": {
            "payments": {"type": "array", "items": {"type": "object", "properties": {
                "amount": {"type": ["number", "null"]},
                "date": {"type": ["string", "null"], "description": "YYYY-MM-DD, григорианский"},
                "date_raw": {"type": ["string", "null"],
                             "description": "дата ДОСЛОВНО как напечатана на слипе, тайскими буквами, напр. «19 ต.ค. 2569»"},
                "ref": {"type": ["string", "null"]},
                "bank": {"type": ["string", "null"]},
                "payee": {"type": ["string", "null"]},
                "description": {"type": "string"},
                "internal_transfer": {"type": "boolean"},
                "confidence": {"type": "string", "enum": ["high", "low"]},
                "issue": {"type": ["string", "null"], "description": "что не так, если confidence=low"},
                "image_index": {"type": "integer"},
                "source": {"type": "string", "enum": ["bank_slip", "bank_pdf", "receipt_only"]},
                "context_indexes": {"type": "array", "items": {"type": "integer"}},
            }, "required": ["amount", "date", "description", "internal_transfer", "confidence",
                            "image_index", "source", "context_indexes", "date_raw"]}},
            "ignored": {"type": "string", "description": "что из вложений не оплата и почему (коротко)"},
        },
        "required": ["payments"],
    },
}


async def analyze(items: list) -> dict:
    content = []
    for i, it in enumerate(items):
        if it["kind"] == "image":
            content.append({"type": "text", "text": f"[вложение {i}: изображение]"})
            content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg",
                                                        "data": base64.b64encode(it["jpeg"]).decode()}})
        elif it["kind"] == "pdf":
            content.append({"type": "text", "text": f"[вложение {i}: PDF {it.get('name','')}]"})
            content.append({"type": "document", "source": {"type": "base64", "media_type": "application/pdf",
                                                           "data": base64.b64encode(it["raw"]).decode()}})
        elif it["kind"] == "text":
            content.append({"type": "text", "text": f"[подпись]: {it['text']}"})
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post("https://api.anthropic.com/v1/messages",
                         headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"},
                         json={"model": CLAUDE_MODEL, "max_tokens": 4000, "system": SYSTEM,
                               "tools": [TOOL], "tool_choice": {"type": "tool", "name": "record_payments"},
                               "messages": [{"role": "user", "content": content}]})
        r.raise_for_status()
        for block in r.json()["content"]:
            if block.get("type") == "tool_use":
                return block["input"]
    return {"payments": []}


# ════════ Google Sheets (тот же webhook, что у приложения) ════════
def _sheets_client(timeout):
    # IPv4: с Fly к Google по IPv6 бывают зависания
    return httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                             transport=httpx.AsyncHTTPTransport(local_address="0.0.0.0", retries=1))


async def sheet_has(txid: str) -> bool | None:
    """Есть ли строка с этим id в таблице. None — не удалось проверить (не путать с «нет»)."""
    t0 = time.time()
    try:
        async with _sheets_client(httpx.Timeout(90, connect=15)) as c:
            r = await c.get(SHEETS_URL)
            rows = (r.json() or {}).get("rows") or []
        log.info("sheet_has %.1fs, %d rows", time.time() - t0, len(rows))
        # строка есть и не помечена удалённой (приложение удаляет мягко: status=deleted)
        return any(len(x) > 9 and str(x[9]) == txid and str(x[10] if len(x) > 10 else "") != "deleted"
                   for x in rows)
    except Exception as e:
        log.warning("sheet_has failed after %.1fs: %r", time.time() - t0, e)
        return None


async def sheets_post(obj: dict) -> dict | None:
    t0 = time.time()
    async with _sheets_client(httpx.Timeout(240, connect=20)) as c:
        r = await c.post(SHEETS_URL, content=json.dumps(obj),
                         headers={"Content-Type": "text/plain;charset=utf-8"})
        log.info("sheets_post %s %.1fs", obj.get("action"), time.time() - t0)
        try:
            j = r.json()
            if not (isinstance(j, dict) and j.get("ok")):
                log.error("sheets rejected %s %s", r.status_code, str(j)[:300])
            return j
        except Exception:
            log.error("sheets non-json %s %s %s", r.status_code, r.headers.get("content-type"), r.text[:300])
            return {"ok": False, "error": f"HTTP {r.status_code}, not JSON (access to the script?)"}


def clean_ref(ref: str | None) -> str | None:
    """Номер операции, а не маскированный номер счёта."""
    r = (ref or "").strip()
    if not r or re.search(r"[Xx]{2,}", r) or len(re.sub(r"[^A-Za-z0-9]", "", r)) < 8:
        return None
    return r


def tx_id_for(p: dict, items: list) -> str:
    ref = (p.get("ref") or "").strip()
    if ref:
        return "line_" + re.sub(r"[^A-Za-z0-9]", "", ref)[:60]
    h = hashlib.sha1(f"{p.get('amount')}|{p.get('date')}|{items[p.get('image_index', 0)]['msg_id']}".encode()).hexdigest()
    return "line_h" + h[:16]


def _img(it):
    b = it.get("jpeg") or it.get("preview")
    try:
        return Image.open(io.BytesIO(b)).convert("RGB") if b else None
    except Exception:
        return None


def collage(imgs: list, cell=700, max_bytes=900_000) -> bytes:
    """Склейка: слип первым, затем контекст; 1–2 колонки, белый фон."""
    thumbs = []
    for im in imgs:
        im = im.copy(); im.thumbnail((cell, int(cell * 1.8))); thumbs.append(im)
    cols = 1 if len(thumbs) == 1 else 2
    rows = [thumbs[i:i + cols] for i in range(0, len(thumbs), cols)]
    pad = 12
    W = cols * cell + (cols + 1) * pad
    H = sum(max(t.height for t in r) for r in rows) + (len(rows) + 1) * pad
    canvas = Image.new("RGB", (W, H), "white")
    y = pad
    for r in rows:
        x = pad
        for t in r:
            canvas.paste(t, (x + (cell - t.width) // 2, y)); x += cell + pad
        y += max(t.height for t in r) + pad
    q = 82
    while True:
        out = io.BytesIO(); canvas.save(out, "JPEG", quality=q)
        if out.tell() <= max_bytes or q <= 40:
            return out.getvalue()
        q -= 10


def photo_for(p: dict, items: list, all_pays: list | None = None) -> str | None:
    """Фото записи = слип оплаты + её контекст (чеки, заказы, переписка) одной картинкой."""
    idx = p.get("image_index", 0)
    order = [idx] if 0 <= idx < len(items) else []
    ctx = [i for i in (p.get("context_indexes") or []) if 0 <= i < len(items) and i != idx]
    if not ctx:  # модель не связала — берём весь контекст пачки, кроме чужих слипов
        slips = {q.get("image_index") for q in (all_pays or [])}
        ctx = [i for i, it in enumerate(items) if i not in slips and i != idx
               and it.get("kind") in ("image", "pdf")]
    imgs = [im for im in (_img(items[i]) for i in order + ctx) if im]
    if not imgs:
        return None
    return "data:image/jpeg;base64," + base64.b64encode(collage(imgs)).decode()


TH_MONTHS = [("ม.ค", "มกราคม", 1), ("ก.พ", "กุมภาพันธ์", 2), ("มี.ค", "มีนาคม", 3), ("เม.ย", "เมษายน", 4),
             ("พ.ค", "พฤษภาคม", 5), ("มิ.ย", "มิถุนายน", 6), ("ก.ค", "กรกฎาคม", 7), ("ส.ค", "สิงหาคม", 8),
             ("ก.ย", "กันยายน", 9), ("ต.ค", "ตุลาคม", 10), ("พ.ย", "พฤศจิกายน", 11), ("ธ.ค", "ธันวาคม", 12)]
EN_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}


def parse_slip_date(raw: str | None) -> str | None:
    """«19 ต.ค. 2569», «19 ต.ค. 69», «19 ตุลาคม 2569», «19 Oct 2026», «19/10/2026» → 2026-10-19."""
    if not raw:
        return None
    t = raw.strip()
    month = None
    norm = t.replace(" ", "")
    for abbr, full, n in TH_MONTHS:  # полные названия раньше сокращений
        if full in norm:
            month = n; break
    if month is None:
        nodots = norm.replace(".", "")
        for abbr, full, n in TH_MONTHS:
            if abbr.replace(".", "") in nodots:
                month = n; break
    if month is None:
        m = re.search(r"[A-Za-z]{3}", t)
        if m and m.group(0).lower() in EN_MONTHS:
            month = EN_MONTHS[m.group(0).lower()]
    nums = [int(x) for x in re.findall(r"\d+", t.split(":")[0] if ":" in t else t)]
    if month is None:
        m = re.search(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})", t)
        if not m:
            return None
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        if len(nums) < 2:
            return None
        day, year = nums[0], nums[1]
    if year < 100:
        year += 2500 if year >= 60 else 2000   # 69 → 2569 (BE)
    if year > 2400:
        year -= 543
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def date_suspicious(p: dict, items: list) -> str | None:
    """Дата слипа не может быть позже сообщения и вряд ли старше 60 дней."""
    try:
        d = datetime.strptime(p.get("date") or "", "%Y-%m-%d").date()
    except Exception:
        return None
    ts = [i.get("ts") for i in items if i.get("ts")]
    if not ts:
        return None
    sent = datetime.fromtimestamp(max(ts) / 1000, tz=timezone(timedelta(hours=7))).date()
    if d > sent + timedelta(days=1):
        return "date is after the message — check the month"
    if d < sent - timedelta(days=60):
        return "date is older than 60 days — check the month"
    return None


def fmt(p: dict) -> str:
    amt = f"{p['amount']:,.2f}".replace(",", " ") if p.get("amount") is not None else "?"
    d = p.get("date") or "?"
    try:
        d = datetime.strptime(d, "%Y-%m-%d").strftime("%d.%m")
    except Exception:
        pass
    desc = p.get("description") or "no description"
    w = "💵 Cash" if p.get("wallet") == "w_cash" else "💳 Card"
    return f"{amt} ฿ · {desc} · {d} · {w}"


async def write(p: dict, items: list, all_pays: list | None = None) -> tuple[bool, str]:
    txid = p.get("_txid") or tx_id_for(p, items)
    row = db.execute("select tx_id from seen where ref=?", (p["ref"],)).fetchone() if p.get("ref") else None
    if row:
        # «уже записано» — только если строка до сих пор в таблице; удалили → пишем заново
        present = None if (DRY_RUN or not SHEETS_URL) else await sheet_has(row[0])
        if present is not False:   # есть или не удалось проверить → не пишем повторно
            return False, "dup"
        log.info("ref %s был удалён из таблицы — записываю заново", p["ref"])
        db.execute("delete from seen where ref=?", (p["ref"],)); db.commit()
        # новый id: приложение помнит удалённый id и не стало бы показывать запись с тем же номером
        txid = f"{txid}_r{int(time.time()) % 1000000}"
    if DRY_RUN:
        return True, txid
    tx = {"id": txid, "type": "expense", "amount": round(float(p["amount"]), 2), "cat": CATEGORY,
          "note": p.get("description") or "", "date": p["date"], "cur": "THB",
          "tags": "line,wallet:" + (p.get("wallet") or "w_card"), "walletId": p.get("wallet") or "w_card", "createdAt": datetime.now(timezone.utc).isoformat(), "hasReceipt": bool(items)}
    ph = photo_for(p, items, all_pays)
    if ph:
        tx["photo"] = ph
    if not SHEETS_URL:
        return False, "SHEETS_URL is not set"
    try:
        r = await sheets_post({"action": "add", "tx": tx})
    except Exception as e:
        log.warning("sheets post %s — проверяю, дошла ли строка", type(e).__name__)
        r = {"ok": False, "error": type(e).__name__}
    if not (r and r.get("ok")):
        # Таймаут ≠ провал: Apps Script мог дописать строку. Проверяем, прежде чем слать повторно.
        has = await sheet_has(txid)
        if has:
            r = {"ok": True}
        elif has is None:
            r = {"ok": False, "error": "sheet not responding — check the app before re-sending"}
        elif "photo" in tx:
            tx.pop("photo")
            try:
                r2 = await sheets_post({"action": "add", "tx": tx})
            except Exception as e:
                r2 = {"ok": False, "error": type(e).__name__}
            if (r2 and r2.get("ok")) or (await sheet_has(txid)) is True:
                r = {"ok": True}; log.warning("saved without photo")
    if not (r and r.get("ok")):
        err = (r or {}).get("error") or (r or {}).get("message") or str(r)[:120]
        return False, f"failed to write to the sheet: {err}"
    if p.get("ref"):
        db.execute("insert or ignore into seen values(?,?,?)", (p["ref"], txid, int(time.time())))
        db.commit()
    return True, txid


# ════════ записи наличными: «Wood -1200 cash», «ค่าน้ำ 350 บาท เงินสด» ════════
CASH_RE = re.compile(r"cash|кэш|кеш|нал(?:ичн|ом|ик)|เงินสด|จ่ายสด", re.I)
CUR_RE = re.compile(r"thb|baht|bath|бат\w*|б\.|บาท|฿", re.I)
NUM_RE = re.compile(r"(?<![\w.])-?\d{1,3}(?:,\d{3})+(?:\.\d+)?|(?<![\w.])-?\d+(?:\.\d+)?")


def parse_cash_note(text: str, ts_ms: int | None) -> list:
    """Строки с суммой из сообщения, где есть пометка «наличные». Без пометки — не трата."""
    if not text or not CASH_RE.search(text):
        return []
    sent = datetime.fromtimestamp((ts_ms or time.time() * 1000) / 1000, tz=timezone(timedelta(hours=7)))
    out = []
    for line in text.splitlines():
        nums = [m for m in NUM_RE.finditer(line) if abs(float(m.group().replace(",", ""))) >= 1]
        if not nums:
            continue
        if len(nums) > 1:   # несколько чисел — берём то, что рядом с валютой/кэшем, иначе спрашиваем
            near = [m for m in nums if CUR_RE.search(line[m.end():m.end() + 8]) or CASH_RE.search(line[m.end():m.end() + 8])]
            if len(near) != 1:
                out.append({"amount": None, "raw": line.strip()}); continue
            nums = near
        m = nums[0]
        amount = abs(float(m.group().replace(",", "")))
        desc = line[:m.start()] + " " + line[m.end():]
        desc = CASH_RE.sub(" ", CUR_RE.sub(" ", desc))
        desc = re.sub(r"[()\[\],:;+\-–—=/]+", " ", desc)
        desc = re.sub(r"\s+", " ", desc).strip()
        out.append({"amount": amount, "date": sent.strftime("%Y-%m-%d"), "description": desc[:1].upper() + desc[1:],
                    "wallet": "w_cash", "raw": line.strip()})
    return out


async def process_cash_notes(group, items, reply_token):
    lines, written = [], []
    for it in items:
        if it["kind"] != "text":
            continue
        for k, p in enumerate(parse_cash_note(it.get("text", ""), it.get("ts"))):
            if p["amount"] is None:
                lines.append(f"⚠️ «{p['raw'][:60]}» — several amounts, not saved. Please write one amount per line.")
                continue
            p["ref"] = None
            p["_txid"] = f"line_c{it['msg_id']}_{k}"
            try:
                ok, info = await write(p, [], None)
            except Exception as e:
                log.exception("cash write failed")
                ok, info = False, f"error {type(e).__name__}"
            if ok:
                lines.append(("📝 Would save: " if DRY_RUN else "✓ ") + fmt(p)); written.append(info)
            else:
                lines.append(f"⚠️ {fmt(p)} — {info}")
    if not lines:
        return False
    text = "\n".join(lines) + ("" if DRY_RUN or not written else "\n(mistake? reply \"cancel\" to this message)")
    ids = await send_many(group, reply_token, [text])
    if ids and written and not DRY_RUN:
        db.execute("insert or replace into written values(?,?,?)", (ids[0], json.dumps(written), int(time.time())))
        db.commit()
    return True


# ════════ обработка пачки ════════
# Текст без картинки, пришедший раньше (>40 с до слипа): держим 5 минут как возможную подпись
recent_text: dict = {}
CAPTION_LOOKBACK = 300
MAX_ATT_PER_CALL = int(os.environ.get("MAX_ATT_PER_CALL", "5"))


def split_units(items: list) -> list:
    """Пачка → смысловые блоки: «вложения + подпись после них». Новый блок начинается
    с вложения, если у текущего уже есть и вложение, и подпись. Слип + скрин заказа + подпись = один блок."""
    units, cur, has_att, has_txt = [], [], False, False
    for it in items:
        is_att = it["kind"] in ("image", "pdf")
        if is_att and has_att and has_txt:
            units.append(cur); cur, has_att, has_txt = [], False, False
        cur.append(it)
        has_att |= is_att
        has_txt |= not is_att
    if cur:
        units.append(cur)
    return units


def chunk_units(units: list, limit: int) -> list:
    """Блоки → вызовы модели по ≤limit вложений; блок не режется (кроме блока больше лимита)."""
    chunks, cur, n = [], [], 0
    for u in units:
        k = sum(i["kind"] in ("image", "pdf") for i in u)
        if k > limit:   # 19 картинок без подписей — режем по limit
            if cur:
                chunks.append(cur); cur, n = [], 0
            part, m = [], 0
            for it in u:
                if it["kind"] in ("image", "pdf") and m == limit:
                    chunks.append(part); part, m = [], 0
                part.append(it); m += it["kind"] in ("image", "pdf")
            if part:
                chunks.append(part)
            continue
        if cur and n + k > limit:
            chunks.append(cur); cur, n = [], 0
        cur += u; n += k
    if cur:
        chunks.append(cur)
    return chunks


async def process_batch(key):
    await asyncio.sleep(BATCH_WAIT)
    b = batches.pop(key, None)
    if not b:
        return
    group, all_items = key[0], b["items"]
    if not any(i["kind"] in ("image", "pdf") for i in all_items):
        # без вложений: пишем только явные записи наличными («Wood 1200 cash»), остальное — переписка
        if not await process_cash_notes(group, all_items, b.get("reply")):
            recent_text[key] = (time.time(), [i for i in all_items if i["kind"] == "text"])
        return
    if not any(i["kind"] == "text" for i in all_items) and key in recent_text:
        t0, txts = recent_text.pop(key)
        if time.time() - t0 <= CAPTION_LOOKBACK and txts:
            all_items = [{**txts[-1], "text": txts[-1].get("text", "") + " (написана до картинки)"}] + all_items
    else:
        recent_text.pop(key, None)
    chunks = chunk_units(split_units(all_items), MAX_ATT_PER_CALL)
    log.info("batch: %d items → %d calls", len(all_items), len(chunks))
    n_att = sum(i["kind"] in ("image", "pdf") for i in all_items)
    if len(chunks) > 1 and b.get("reply"):
        # большая пачка идёт минуты — сразу говорим, что приняли (reply бесплатный), итог придёт push'ем
        try:
            await line_reply(b["reply"], f"⏳ Got {n_att} images — processing, the summary will follow in a few minutes.")
            b["reply"] = None
        except Exception:
            log.warning("ack failed")
    lines, doubts, written = [], [], []
    for items in chunks:
        if not any(i["kind"] in ("image", "pdf") for i in items):
            continue
        try:
            res = await analyze(items)
        except Exception as e:
            log.exception("analyze failed")
            lines.append(f"⚠️ Could not read {sum(i['kind'] in ('image','pdf') for i in items)} attachment(s) "
                         f"({type(e).__name__}). Please re-send them.")
            continue
        pays = res.get("payments") or []
        if not pays:
            names = ", ".join(i.get("name") or "image" for i in items if i["kind"] in ("image", "pdf"))
            lines.append(f"ℹ️ No payment found in: {names[:120]}"
                         + (f" — {res.get('ignored')[:160]}" if res.get("ignored") else "")
                         + ". If it is an expense, send the bank slip or add it manually.")
        for p in pays:
            p["ref"] = clean_ref(p.get("ref"))
            fixed = parse_slip_date(p.get("date_raw"))
            if fixed and fixed != p.get("date"):
                log.info("date fixed %s -> %s (raw %r)", p.get("date"), fixed, p.get("date_raw"))
                p["date"] = fixed
            bad_date = date_suspicious(p, items)
            doubtful = p.get("internal_transfer") or p.get("confidence") != "high" \
                or p.get("amount") in (None, 0) or not p.get("date") or bad_date \
                or p.get("source") == "receipt_only"
            if doubtful:
                why = ("transfer to own account?" if p.get("internal_transfer")
                       else "receipt only, no bank slip — paid in cash?" if p.get("source") == "receipt_only"
                       else bad_date or p.get("issue") or "not sure")
                doubts.append((p, items, f"⚠️ {fmt(p)} — {why}\nNot saved. If this is a {CATEGORY} expense, reply \"+\" to this message."))
                continue
            try:
                ok, info = await write(p, items, pays)
            except Exception as e:  # никогда не молчим: любая ошибка записи — сообщение в группу
                log.exception("write failed")
                ok, info = False, f"error {type(e).__name__}"
            if ok:
                lines.append(("📝 Would save: " if DRY_RUN else "✓ ") + fmt(p)); written.append(info)
            elif info == "dup":
                lines.append(f"↺ Already saved: {fmt(p)}")
            else:
                lines.append(f"⚠️ {fmt(p)} — {info}")
    if not lines and not doubts:
        return
    texts = []
    if lines:
        total = sum(1 for l in lines if l.startswith("✓"))
        head = f"{total} saved:\n" if total > 1 else ""
        texts.append(head + "\n".join(lines) + ("" if DRY_RUN or not written else "\n(mistake? reply \"cancel\" to this message — cancels all lines above)"))
    room = 5 - len(texts)
    shown = doubts[:room] if len(doubts) <= room else doubts[:room - 1]
    texts += [t for _, _, t in shown]
    if len(doubts) > len(shown):
        rest = doubts[len(shown):]
        texts.append(f"⚠️ {len(rest)} more unclear: " + "; ".join(fmt(p) for p, _, _ in rest)
                     + "\nNot saved — re-send these slips one by one.")
    ids = await send_many(group, b.get("reply"), texts)
    off = 1 if lines else 0
    if lines and ids and written and not DRY_RUN:
        db.execute("insert or replace into written values(?,?,?)", (ids[0], json.dumps(written), int(time.time())))
    for (p, items, _), mid in zip(shown, ids[off:]):
        if mid:
            db.execute("insert or replace into pending values(?,?,?)",
                       (mid, json.dumps({"p": p, "items": serialize(items)}), int(time.time())))
    db.commit()


def serialize(items):
    out = []
    for it in items:
        d = {k: v for k, v in it.items() if k not in ("jpeg", "raw", "preview")}
        if it.get("jpeg") or it.get("preview"):
            d["jpeg_b64"] = base64.b64encode(it.get("jpeg") or it.get("preview")).decode()
        out.append(d)
    return out


def deserialize(items):
    for it in items:
        if it.get("jpeg_b64"):
            it["jpeg"] = base64.b64decode(it.pop("jpeg_b64"))
    return items


async def handle_text(ev, group):
    text = ev["message"].get("text", "").strip().lower()
    quoted = ev["message"].get("quotedMessageId")
    if not quoted and text in ("+", "yes", "да"):
        last = db.execute("select msg_id from pending where ts>? order by ts desc limit 1",
                          (int(time.time()) - 900,)).fetchone()
        quoted = last[0] if last else None
    if quoted and text in ("+", "да", "yes", "y", "ok", "расход", "ใช่"):
        row = db.execute("select payload from pending where msg_id=?", (quoted,)).fetchone()
        if row:
            db.execute("delete from pending where msg_id=?", (quoted,)); db.commit()  # забрали — второй «+» не запишет
            data = json.loads(row[0]); p = data["p"]; items = deserialize(data["items"])
            if p.get("amount") in (None, 0) or not p.get("date"):
                await line_reply(ev["replyToken"], "Amount or date is missing — please add this expense manually.")
                return
            ok, info = await write(p, items)
            db.execute("delete from pending where msg_id=?", (quoted,)); db.commit()
            await line_reply(ev["replyToken"], ("📝 Would save: " if DRY_RUN else "✓ ") + fmt(p) if ok else f"⚠️ {info}")
            return
    if quoted and text in ("отмена", "cancel", "undo", "x", "ยกเลิก") and not DRY_RUN:
        row = db.execute("select tx_id from written where msg_id=?", (quoted,)).fetchone()
        if row:
            for txid in json.loads(row[0]):
                await sheets_post({"action": "delete", "id": txid})
            db.execute("delete from written where msg_id=?", (quoted,)); db.commit()
            await line_reply(ev["replyToken"], "✓ Cancelled")
            return
    if text in ("/id", "id группы", "group id"):
        await line_reply(ev["replyToken"], f"Group ID: {group}")
        return
    add_item(ev, group, {"kind": "text", "text": ev["message"].get("text", "")[:500]})


def add_item(ev, group, item):
    key = (group, ev["source"].get("userId", "?"))
    item["msg_id"] = ev["message"]["id"]
    item["ts"] = ev.get("timestamp")
    b = batches.setdefault(key, {"items": [], "task": None})
    b["items"].append(item)
    b["reply"] = ev.get("replyToken")  # ответим на последнее сообщение пачки
    if b["task"]:
        b["task"].cancel()
    b["task"] = asyncio.create_task(process_batch(key))


async def handle_event(ev):
    src = ev.get("source", {})
    group = src.get("groupId") or src.get("roomId")
    if ev.get("type") == "join" and group:
        await line_reply(ev["replyToken"], f"Hi! I'm Shop Accountant 🧾 I record DJ Shop expenses from bank slips and PDFs.\nGroup ID: {group}")
        return
    if ev.get("type") != "message" or not group:
        return
    if GROUP_ID and group != GROUP_ID:
        return  # бот работает только в одной группе
    m = ev["message"]; t = m["type"]
    try:
        if t == "text":
            await handle_text(ev, group)
        elif t == "image":
            add_item(ev, group, {"kind": "image", "jpeg": to_jpeg(await line_content(m["id"]))})
        elif t == "file" and m.get("fileName", "").lower().endswith(".pdf"):
            raw = await line_content(m["id"])  # файлы в LINE истекают — качаем сразу
            add_item(ev, group, {"kind": "pdf", "raw": raw, "name": m.get("fileName"),
                                 "preview": pdf_first_page_jpeg(raw)})
    except Exception as e:
        log.exception("event failed")
        if t in ("image", "file") and ev.get("replyToken"):
            try:
                await line_reply(ev["replyToken"], f"⚠️ Could not download this {t} ({type(e).__name__}). Please re-send it.")
            except Exception:
                pass


@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    if not verify(body, request.headers.get("x-line-signature", "")):
        return Response(status_code=403)
    for ev in json.loads(body).get("events", []):
        asyncio.create_task(handle_event(ev))
    return {"ok": True}


@app.get("/")
def health():
    return {"bot": "Shop Accountant", "dry_run": DRY_RUN, "group_locked": bool(GROUP_ID),
            "pending_batches": len(batches)}
