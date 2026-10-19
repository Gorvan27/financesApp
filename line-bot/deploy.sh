#!/usr/bin/env bash
# Развёртывание Shop Accountant на Fly.io. Ключи вводятся скрыто и не сохраняются в истории.
set -euo pipefail
cd "$(dirname "$0")"
APP=dj-shop-accountant
command -v fly >/dev/null || { echo "Ставлю flyctl..."; brew install flyctl; }
fly auth whoami >/dev/null 2>&1 || fly auth login

# создаём только если ещё нет (повторный запуск безопасен)
fly status --app "$APP" >/dev/null 2>&1 || fly apps create "$APP"
fly volumes list --app "$APP" 2>/dev/null | grep -q shopbot_data \
  || fly volumes create shopbot_data --app "$APP" --region sin --size 1 --yes

echo; echo "Вставь ключи (ввод скрыт, Enter после каждого):"
read -rsp "1) LINE Channel secret: " LS; echo
read -rsp "2) LINE Channel access token (long-lived): " LT; echo
read -rsp "3) Anthropic API key: " AK; echo
read -rp  "4) Webhook таблицы Vanya_FinanceApp (…/exec): " SU; echo
fly secrets set --app "$APP" --stage \
  LINE_CHANNEL_SECRET="$LS" LINE_CHANNEL_TOKEN="$LT" ANTHROPIC_API_KEY="$AK" SHEETS_URL="$SU" >/dev/null
unset LS LT AK

fly deploy --app "$APP" --ha=false
echo; echo "✅ Готово. Webhook URL для LINE:  https://$APP.fly.dev/callback"
