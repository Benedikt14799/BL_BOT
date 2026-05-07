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
errors_count="$(echo "$summary_line" | sed -n 's/.*errors=\([0-9]\+\).*/\1/p')"
duration_s="$(echo "$summary_line" | sed -n 's/.*duration_s=\([0-9]\+\).*/\1/p')"

links_total="${links_total:-?}"
items_saved="${items_saved:-?}"
errors_count="${errors_count:-?}"
duration_s="${duration_s:-?}"

if [[ "$duration_s" =~ ^[0-9]+$ ]]; then
  printf -v dur_fmt '%02dm %02ds' $((duration_s/60)) $((duration_s%60))
else
  dur_fmt="?"
fi

if [ "$code" -eq 0 ]; then
  $PY bin/notify.py "✅ *BL_BOT – Scraping erfolgreich*
• Dauer: $dur_fmt
• Links (aus links.txt): *$links_total*
• Items gespeichert: *$items_saved*
• Fehler: *$errors_count*
• Host: \`$HOST\`
• Log: \`$LOG\`" >> "$LOG" 2>&1 || true
  exit 0
else
  $PY bin/notify.py "❌ *BL_BOT – Scraping FEHLER* (exit $code)
• Dauer: $dur_fmt
• Links (aus links.txt): *$links_total*
• Items gespeichert: *$items_saved*
• Fehler: *$errors_count*
• Host: \`$HOST\`
• Log: \`$LOG\`" >> "$LOG" 2>&1 || true
  exit "$code"
fi
