#!/usr/bin/env bash
set -euo pipefail

REPO="/home/blbot/bl_bot"
PY="/home/blbot/bl_bot/venv/bin/python"
LOGDIR="$REPO/logs"
LOG="$LOGDIR/scraping_$(date +%F).log"
HOST="$(hostname)"

mkdir -p "$LOGDIR"
cd "$REPO"

start_human="$(date '+%F %T')"

$PY bin/notify.py "🚀 *BL_BOT – Scraping gestartet*
• Zeit: $start_human
• Host: \`$HOST\`
• Log: \`$LOG\`" >> "$LOG" 2>&1 || true

set +e
$PY main.py >> "$LOG" 2>&1
code=$?
set -e

summary_line="$(grep -a 'SCRAPE_SUMMARY ' "$LOG" | tail -n 1 || true)"

links_total="$(echo "$summary_line" | sed -n 's/.*links_total=\([0-9]\+\).*/\1/p')"
items_saved="$(echo "$summary_line" | sed -n 's/.*items_saved=\([0-9]\+\).*/\1/p')"
filtered="$(echo "$summary_line" | sed -n 's/.*filtered=\([0-9]\+\).*/\1/p')"
errors_count="$(echo "$summary_line" | sed -n 's/.*errors=\([0-9]\+\).*/\1/p')"
duration_s="$(echo "$summary_line" | sed -n 's/.*duration_s=\([0-9]\+\).*/\1/p')"

links_total="${links_total:-0}"
items_saved="${items_saved:-0}"
filtered="${filtered:-0}"
errors_count="${errors_count:-0}"
duration_s="${duration_s:-0}"

if [[ "$duration_s" =~ ^[0-9]+$ ]]; then
  printf -v dur_fmt '%02dm %02ds' $((duration_s/60)) $((duration_s%60))
else
  dur_fmt="?"
fi

if [ "$code" -eq 0 ]; then
  $PY bin/notify.py "🔍 *BL_BOT: Scraping Report*
📥 *Neu:* $items_saved Bücher eingelesen
🛒 *Bereit:* $items_saved (für eBay qualifiziert)
✂️ *Gefiltert:* $filtered Bücher (Marge/Zustand)
⚠️ *Fehler:* $errors_count

⏱️ *Dauer:* $dur_fmt
💻 *Host:* \`$HOST\`" >> "$LOG" 2>&1 || true
  exit 0
else
  $PY bin/notify.py "❌ *BL_BOT: Scraping FEHLER* (exit $code)
📥 *Eingelesen:* $items_saved
✂️ *Gefiltert:* $filtered
⚠️ *Fehler:* $errors_count

⏱️ *Dauer:* $dur_fmt
💻 *Host:* \`$HOST\`" >> "$LOG" 2>&1 || true
  exit "$code"
fi
