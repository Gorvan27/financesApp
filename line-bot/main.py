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

ГЛАВНОЕ ПРАВИЛО: оплата = ТОЛЬКО банковский документ о переводе:
  • слип банковского приложения (K PLUS, KBIZ, Krungsri, SCB, Bangkok Bank, PromptPay…):
    есть отправитель → получатель, «จำนวนเงิน»/сумма и номер операции;
  • PDF-выписка/подтверждение перевода из банка.
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
- ref — номер транзакции со слипа (เลขที่รายการ / หมายเลขอ้างอิง / Transaction ID).
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
                            "image_index", "source", "context_indexes"]}},
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
                         json={"model": CLAUDE_MODEL, "max_tokens": 2000, "system": SYSTEM,
                               "tools": [TOOL], "tool_choice": {"type": "tool", "name": "record_payments"},
                               "messages": [{"role": "user", "content": content}]})
        r.raise_for_status()
        for block in r.json()["content"]:
            if block.get("type") == "tool_use":
                return block["input"]
    return {"payments": []}


# ════════ Google Sheets (тот же webhook, что у приложения) ════════
async def sheets_post(obj: dict) -> dict | None:
    async with httpx.AsyncClient(timeout=90, follow_redirects=True) as c:
        r = await c.post(SHEETS_URL, content=json.dumps(obj),
                         headers={"Content-Type": "text/plain;charset=utf-8"})
        try:
            return r.json()
        except Exception:
            log.error("sheets non-json %s %s", r.status_code, r.text[:200])
            return None


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
    return f"{amt} ฿ · {desc} · {d}"


async def write(p: dict, items: list, all_pays: list | None = None) -> tuple[bool, str]:
    txid = tx_id_for(p, items)
    if p.get("ref") and db.execute("select 1 from seen where ref=?", (p["ref"],)).fetchone():
        return False, "dup"
    if DRY_RUN:
        return True, txid
    tx = {"id": txid, "type": "expense", "amount": round(float(p["amount"]), 2), "cat": CATEGORY,
          "note": p.get("description") or "", "tags": "line", "date": p["date"], "cur": "THB",
          "walletId": "", "createdAt": datetime.now(timezone.utc).isoformat(), "hasReceipt": True}
    ph = photo_for(p, items, all_pays)
    if ph:
        tx["photo"] = ph
    r = await sheets_post({"action": "add", "tx": tx})
    if not (r and r.get("ok")):
        return False, "failed to write to the sheet"
    if p.get("ref"):
        db.execute("insert or ignore into seen values(?,?,?)", (p["ref"], txid, int(time.time())))
        db.commit()
    return True, txid


# ════════ обработка пачки ════════
async def process_batch(key):
    await asyncio.sleep(BATCH_WAIT)
    b = batches.pop(key, None)
    if not b:
        return
    group, items = key[0], b["items"]
    if not any(i["kind"] in ("image", "pdf") for i in items):
        return  # только текст без вложений — не наше дело
    try:
        res = await analyze(items)
    except Exception as e:
        log.exception("analyze failed")
        await line_push(group, f"⚠️ Could not read the attachments ({type(e).__name__}). Please add it manually.")
        return
    pays = res.get("payments") or []
    if not pays:
        return
    lines, doubts, written = [], [], []
    for p in pays:
        bad_date = date_suspicious(p, items)
        doubtful = p.get("internal_transfer") or p.get("confidence") != "high" \
            or p.get("amount") in (None, 0) or not p.get("date") or bad_date \
            or p.get("source") == "receipt_only"
        if doubtful:
            why = ("transfer to own account?" if p.get("internal_transfer")
                   else "receipt only, no bank slip — paid in cash?" if p.get("source") == "receipt_only"
                   else bad_date or p.get("issue") or "not sure")
            doubts.append((p, f"⚠️ {fmt(p)} — {why}\nNot saved. If this is a {CATEGORY} expense, reply \"+\" to this message."))
            continue
        ok, info = await write(p, items, pays)
        if ok:
            lines.append(("📝 Would save: " if DRY_RUN else "✓ ") + fmt(p)); written.append(info)
        elif info == "dup":
            lines.append(f"↺ Already saved: {fmt(p)}")
        else:
            lines.append(f"⚠️ {fmt(p)} — {info}")
    texts = []
    if lines:
        texts.append("\n".join(lines) + ("" if DRY_RUN or not written else "\n(mistake? reply \"cancel\" to this message)"))
    texts += [t for _, t in doubts[:5 - len(texts)]]
    ids = await send_many(group, b.get("reply"), texts)
    off = 1 if lines else 0
    if lines and ids and written and not DRY_RUN:
        db.execute("insert or replace into written values(?,?,?)", (ids[0], json.dumps(written), int(time.time())))
    for (p, _), mid in zip(doubts, ids[off:]):
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
    if quoted and text in ("+", "да", "yes", "y", "ok", "расход", "ใช่"):
        row = db.execute("select payload from pending where msg_id=?", (quoted,)).fetchone()
        if row:
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
    except Exception:
        log.exception("event failed")


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
