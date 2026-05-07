#!/usr/bin/env bash
set -euo pipefail

REPO="/home/blbot/bl_bot"
PY="/home/blbot/bl_bot/venv/bin/python"
LOGDIR="$REPO/logs"
LOG="$LOGDIR/upload_$(date +%F).log"
HOST="$(hostname)"

mkdir -p "$LOGDIR"
cd "$REPO"

start_human="$(date '+%F %T')"

$PY bin/notify.py "🚀 *BL_BOT – Upload gestartet*
• Zeit: $start_human
• Host: \`$HOST\`
• Log: \`$LOG\`" >> "$LOG" 2>&1 || true

set +e
$PY run_upload_all.py >> "$LOG" 2>&1
code=$?
set -e

summary_line="$(grep -a 'UPLOAD_SUMMARY ' "$LOG" | tail -n 1 || true)"

success="$(echo "$summary_line" | sed -n "s/.*success=\([0-9]\+\).*/\1/p")"
failed="$(echo "$summary_line" | sed -n "s/.*failed=\([0-9]\+\).*/\1/p")"
skipped="$(echo "$summary_line" | sed -n "s/.*skipped=\([0-9]\+\).*/\1/p")"
duration_s="$(echo "$summary_line" | sed -n "s/.*duration_s=\([0-9]\+\).*/\1/p")"
top_error="$(echo "$summary_line" | sed -n "s/.*top_error='\(.*\)'.*/\1/p")"

success="${success:-0}"
failed="${failed:-0}"
skipped="${skipped:-0}"
duration_s="${duration_s:-0}"
top_error="${top_error:-Keine}"

if [[ "$duration_s" =~ ^[0-9]+$ ]]; then
  printf -v dur_fmt '%02dm %02ds' $((duration_s/60)) $((duration_s%60))
else
  dur_fmt="?"
fi

# Bestandszahl aus DB holen (Optional, falls gewünscht)
# total_active="$($PY -c 'import asyncio, asyncpg, os; from dotenv import load_dotenv; load_dotenv(); async def g(): p=await asyncpg.connect(os.getenv("DATABASE_URL")); print(await p.fetchval("SELECT count(*) FROM library WHERE status_id=4")); await p.close(); asyncio.run(g())' || echo '?')"

if [ "$code" -eq 0 ]; then
  $PY bin/notify.py "🚀 *BL_BOT: eBay Upload Report*
✅ *Erfolg:* $success Bücher online
❌ *Fehler:* $failed fehlgeschlagen
⚠️ *Top Fehler:* \`$top_error\`

⏱️ *Dauer:* $dur_fmt
💻 *Host:* \`$HOST\`" >> "$LOG" 2>&1 || true
  exit 0
else
  $PY bin/notify.py "❌ *BL_BOT: eBay Upload FEHLER* (exit $code)
✅ *Erfolg:* $success ok
❌ *Fehler:* $failed fehlgeschlagen
⚠️ *Top Fehler:* \`$top_error\`

⏱️ *Dauer:* $dur_fmt
💻 *Host:* \`$HOST\`" >> "$LOG" 2>&1 || true
  exit "$code"
fi
