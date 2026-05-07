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

success="$(echo "$summary_line" | sed -n 's/.*success=\([0-9]\+\).*/\1/p')"
failed="$(echo "$summary_line" | sed -n 's/.*failed=\([0-9]\+\).*/\1/p')"
skipped="$(echo "$summary_line" | sed -n 's/.*skipped=\([0-9]\+\).*/\1/p')"
duration_s="$(echo "$summary_line" | sed -n 's/.*duration_s=\([0-9]\+\).*/\1/p')"

success="${success:-?}"
failed="${failed:-?}"
skipped="${skipped:-?}"
duration_s="${duration_s:-?}"

if [[ "$duration_s" =~ ^[0-9]+$ ]]; then
  printf -v dur_fmt '%02dm %02ds' $((duration_s/60)) $((duration_s%60))
else
  dur_fmt="?"
fi

if [ "$code" -eq 0 ]; then
  $PY bin/notify.py "✅ *BL_BOT – Upload erfolgreich*
• Dauer: $dur_fmt
• Uploads: *$success* ok / *$failed* failed / *$skipped* skipped
• Host: \`$HOST\`
• Log: \`$LOG\`" >> "$LOG" 2>&1 || true
  exit 0
else
  $PY bin/notify.py "❌ *BL_BOT – Upload FEHLER* (exit $code)
• Dauer: $dur_fmt
• Uploads: *$success* ok / *$failed* failed / *$skipped* skipped
• Host: \`$HOST\`
• Log: \`$LOG\`" >> "$LOG" 2>&1 || true
  exit "$code"
fi
