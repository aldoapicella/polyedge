#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT HUP INT TERM

success_command="$TMP/success.sh"
failure_command="$TMP/failure.sh"
report="$TMP/report.json"
stdout="$TMP/stdout"
stderr="$TMP/stderr"

printf '%s\n' \
  '#!/bin/sh' \
  'set -eu' \
  'i=0' \
  'while [ "$i" -lt 10000 ]; do echo "verbose report row $i"; i=$((i + 1)); done' \
  "printf '%s\\n' '{\"result\":{\"status\":\"warning\",\"warnings\":[\"missing minute blob\"],\"critical\":[]}}' > '$report'" \
  >"$success_command"
chmod +x "$success_command"

sh "$ROOT/research/run_compact_report_job.sh" \
  polyedge_hourly_quality "$report" "$success_command" \
  >"$stdout" 2>"$stderr"

test "$(wc -l <"$stdout")" -eq 2
test ! -s "$stderr"
tail -n 1 "$stdout" | jq -e '
  .event == "polyedge_hourly_quality"
  and .status == "warning"
  and .warnings == ["missing minute blob"]
  and .critical == []
  and (.duration_seconds >= 0)
' >/dev/null
! grep -Fq 'verbose report row' "$stdout"

printf '%s\n' \
  '#!/bin/sh' \
  'echo "specific failure evidence" >&2' \
  'exit 7' \
  >"$failure_command"
chmod +x "$failure_command"

if sh "$ROOT/research/run_compact_report_job.sh" \
  polyedge_data_freshness "$report" "$failure_command" \
  >"$stdout" 2>"$stderr"; then
  echo "failure command unexpectedly succeeded" >&2
  exit 1
else
  status=$?
fi
test "$status" -eq 7
grep -Fq 'specific failure evidence' "$stderr"
grep -Fq '"status":"failed"' "$stderr"
grep -Fq '"exit_code":7' "$stderr"

echo "compact report job tests passed"
