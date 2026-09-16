#!/bin/sh
set -eu

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <event> <report-json> <command> [args ...]" >&2
  exit 2
fi

event=$1
report=$2
shift 2

started=$(date +%s)
output=$(mktemp)
trap 'rm -f "$output"' EXIT HUP INT TERM

printf '{"event":"%s","status":"starting"}\n' "$event"
if "$@" >"$output" 2>&1; then
  finished=$(date +%s)
  if [ -s "$report" ] && jq -e 'type == "object"' "$report" >/dev/null 2>&1; then
    jq -c \
      --arg event "$event" \
      --argjson duration_seconds "$((finished - started))" \
      '{
        event: $event,
        status: (.result.status // .status // "completed"),
        warnings: (.result.warnings // .warnings // []),
        critical: (.result.critical // .critical // []),
        duration_seconds: $duration_seconds
      }' "$report"
  else
    printf '{"event":"%s","status":"completed","duration_seconds":%s,"report_summary":"unavailable"}\n' \
      "$event" "$((finished - started))"
  fi
  exit 0
else
  status=$?
fi

finished=$(date +%s)
tail -c 65536 "$output" >&2 || true
printf '{"event":"%s","status":"failed","exit_code":%s,"duration_seconds":%s}\n' \
  "$event" "$status" "$((finished - started))" >&2
exit "$status"
