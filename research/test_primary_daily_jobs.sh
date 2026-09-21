#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
TEST_GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

mkdir -p "$TMP/bin" "$TMP/work"
cat >"$TMP/bin/polyedge-rs" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$POLYEDGE_TEST_ARGS"
command=${2:-}
if [ "$command" = "restore-normalized-snapshot" ] && [ "${MOCK_RESTORE_FAIL:-0}" = 1 ]; then
  echo '{"large_verbose_payload":"restore missing"}'
  exit 42
fi
out=
markdown=
normalized=
previous=
for argument in "$@"; do
  if [ "$previous" = "--out" ]; then out=$argument; fi
  if [ "$previous" = "--markdown" ]; then markdown=$argument; fi
  if [ "$previous" = "--normalized" ]; then normalized=$argument; fi
  previous=$argument
done
if [ -n "$out" ]; then
  if [ "$command" = "normalize" ] || [ "$command" = "restore-normalized-snapshot" ] || [ "$command" = "build-replay-index" ]; then
    mkdir -p "$out"
  else
    mkdir -p "$(dirname "$out")"
  fi
fi
if [ "$command" = "audit" ]; then
  jq -n --arg date "${POLYEDGE_RESEARCH_DATE}" --argjson failed "${MOCK_QUALITY_FAIL:-0}" '
    {result:{
      total_events:1,
      first_event_timestamp:($date + "T00:00:00Z"),
      last_event_timestamp:($date + "T23:59:00Z"),
      event_count_by_hour:([range(0;24) | {key:($date + "T" + (. | if . < 10 then "0" + tostring else tostring end)),value:1}] | from_entries),
      start_price_capture_rate:(if $failed == 1 then 0.94 else 1 end),
      settlement_rate:1,exact_resolution_reference_hour_coverage:1,decision_metadata_coverage:1,
      decision_grade_applicable:false,decision_grade_coverage:null,final_decision_grade_coverage:null,
      execution_field_coverage:1,decision_parity_rate:1,fatal_data_quality_issues:[],warnings:[]
    }}' >"$out"
fi
if [ "$command" = "execution-quality" ]; then
  jq -n '{result:{queue_position_coverage:null,queue_position_applicable:false,
    markouts:{"1":{completion_rate:null,applicable:false},"5":{completion_rate:null,applicable:false},"30":{completion_rate:null,applicable:false}},warnings:[]}}' >"$out"
fi
if [ "$command" = "check-primary-daily-quality" ]; then
  if [ "${MOCK_QUALITY_FAIL:-0}" = 1 ]; then status=failed; else status=passed; fi
  stage=$(dirname "$out")
  manifest_sha="sha256:$(sha256sum "$normalized/events_manifest.json" | cut -d' ' -f1)"
  audit_sha="sha256:$(sha256sum "$stage/data_audit.json" | cut -d' ' -f1)"
  execution_sha="sha256:$(sha256sum "$stage/execution_quality.json" | cut -d' ' -f1)"
  jq -n --arg status "$status" --arg date "$POLYEDGE_RESEARCH_DATE" --arg git_sha "$GIT_SHA" \
    --arg manifest_sha "$manifest_sha" --arg audit_sha "$audit_sha" --arg execution_sha "$execution_sha" \
    --arg inventory_sha "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    '{schema:"polyedge.primary_data_quality_gate.v1",schema_version:1,date:$date,git_sha:$git_sha,status:$status,promotion_eligible:false,normalized_manifest_sha256:$manifest_sha,source_inventory_sha256:$inventory_sha,data_audit_sha256:$audit_sha,execution_quality_sha256:$execution_sha,source:{raw_source_inventory_sha256:$inventory_sha}}' >"$out"
fi
if [ "$command" = "normalize" ] || [ "$command" = "restore-normalized-snapshot" ]; then
  printf '%s' '{"format":"jsonl-indexed-gzip-sharded","events":1,"raw_source_inventory":{"schema_version":1,"canonical_sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","canonical":{"domain":"polyedge.raw-source-inventory.v1","schema_version":1,"source_kind":"azure_blob","account":"st","container":"events","prefix":"events/2026/07/30/","max_blobs":null,"max_bytes":null,"ordering":"blob_name_ascii_ascending","exhaustive_listing":true,"blob_count":0,"total_bytes":0,"blobs":[]}}}' >"$out/events_manifest.json"
fi
if [ -n "$out" ] && [ ! -d "$out" ] && [ "$command" != audit ] \
  && [ "$command" != execution-quality ] && [ "$command" != check-primary-daily-quality ]; then
  printf '%s' '{}' >"$out"
fi
if [ -n "$markdown" ]; then
  mkdir -p "$(dirname "$markdown")"
  printf '%s\n' report >"$markdown"
fi
echo '{"large_verbose_payload":"this must not reach successful job logs"}'
EOF
chmod +x "$TMP/bin/polyedge-rs"

run_daily() {
  local_root=${1:-}
  rm -rf "$TMP/work"
  mkdir -p "$TMP/work"
  : >"$TMP/args"
  (
    cd "$TMP/work"
    PATH="$TMP/bin:$PATH" \
      POLYEDGE_TEST_ARGS="$TMP/args" \
      POLYEDGE_RESEARCH_DATE=2026-07-30 \
      POLYEDGE_LOCAL_RAW_ROOT="$local_root" \
      GIT_SHA="$TEST_GIT_SHA" \
      AZURE_STORAGE_ACCOUNT_NAME=st \
      AZURE_STORAGE_CONTAINER_NAME=events \
      sh "$ROOT/research/run_primary_daily.sh"
  ) >"$TMP/daily-stdout" 2>"$TMP/daily-stderr"
}

run_daily
test "$(grep -c '^research normalize ' "$TMP/args")" -eq 1
test "$(grep -c '^research publish-normalized-snapshot ' "$TMP/args")" -eq 1
test "$(grep -c '^research build-replay-index ' "$TMP/args" || true)" -eq 0
grep -F '"stage":"normalize"' "$TMP/daily-stdout" >/dev/null
grep -F '"snapshot":"published"' "$TMP/daily-stdout" >/dev/null
grep -F -- '--input azure://st/events/events/2026/07/30/?prefetch_blobs=16' "$TMP/args" >/dev/null
marker="$TMP/work/data/research/daily/2026-07-30/normalized/.polyedge-daily-complete.json"
test "$(jq -r '.git_sha' "$marker")" = "$TEST_GIT_SHA"
test "$(jq -r '.events_manifest_sha256' "$marker")" = "sha256:$(sha256sum "$TMP/work/data/research/daily/2026-07-30/normalized/events_manifest.json" | cut -d' ' -f1)"
if grep -F 'large_verbose_payload' "$TMP/daily-stdout" >/dev/null; then
  echo "successful command output leaked into daily logs" >&2
  exit 1
fi

run_daily /input/events
grep -F -- '--input /input/events/2026/07/30' "$TMP/args" >/dev/null

run_daily_quality() {
  quality_fail=$1
  rm -rf "$TMP/work"
  mkdir -p "$TMP/work"
  : >"$TMP/args"
  (
    cd "$TMP/work"
    PATH="$TMP/bin:$PATH" \
      POLYEDGE_TEST_ARGS="$TMP/args" \
      POLYEDGE_RESEARCH_DATE=2026-07-30 \
      POLYEDGE_LOCAL_RAW_ROOT=/input/events \
      POLYEDGE_DATA_QUALITY_ONLY=true \
      MOCK_QUALITY_FAIL="$quality_fail" \
      GIT_SHA="$TEST_GIT_SHA" \
      sh "$ROOT/research/run_primary_daily.sh"
  ) >"$TMP/quality-stdout" 2>"$TMP/quality-stderr"
}

if run_daily_quality 1; then
  echo 'failed data-quality pilot unexpectedly completed' >&2
  exit 1
fi
test -f "$(find "$TMP/work/reports/research/staging" -name data_quality_gate.json)"
test ! -e "$TMP/work/data/research/daily/2026-07-30/normalized/.polyedge-daily-complete.json"
if grep -F 'research build-markets ' "$TMP/args" >/dev/null; then
  echo 'failed pilot ran candidate commands' >&2
  exit 1
fi
test "$(grep -c '^research publish-normalized-snapshot ' "$TMP/args" || true)" -eq 0

: >"$TMP/args"
if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" POLYEDGE_TEST_ARGS="$TMP/args" \
    POLYEDGE_RESEARCH_DATE=2026-07-30 POLYEDGE_LOCAL_RAW_ROOT=/input/events \
    POLYEDGE_DATA_QUALITY_ONLY=true MOCK_QUALITY_FAIL=0 GIT_SHA="$TEST_GIT_SHA" \
    sh "$ROOT/research/run_primary_daily.sh"
) >"$TMP/quality-retry-stdout" 2>"$TMP/quality-retry-stderr"; then
  echo 'failed data-quality pilot unexpectedly retried' >&2
  exit 1
fi
grep -F 'refusing to overwrite failed pilot evidence' "$TMP/quality-retry-stderr" >/dev/null
test "$(grep -c '^research normalize ' "$TMP/args" || true)" -eq 0

run_daily_quality 0
quality_marker="$TMP/work/data/research/daily/2026-07-30/normalized/.polyedge-daily-complete.json"
test "$(jq -r '.mode' "$quality_marker")" = data_quality_only
test "$(jq -r '.status' "$(find "$TMP/work/reports/research/staging" -name data_quality_gate.json)")" = passed
test "$(grep -c '^research build-markets ' "$TMP/args" || true)" -eq 0

: >"$TMP/args"
(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
    POLYEDGE_TEST_ARGS="$TMP/args" \
    POLYEDGE_RESEARCH_DATE=2026-07-30 \
    POLYEDGE_LOCAL_RAW_ROOT=/input/events \
    POLYEDGE_DATA_QUALITY_ONLY=true \
    GIT_SHA="$TEST_GIT_SHA" \
    sh "$ROOT/research/run_primary_daily.sh"
) >"$TMP/quality-reuse-stdout" 2>"$TMP/quality-reuse-stderr"
grep -F '"status":"reused"' "$TMP/quality-reuse-stdout" >/dev/null
test "$(grep -c '^research normalize ' "$TMP/args" || true)" -eq 0

quality_gate="$TMP/work/$(jq -r '.quality_gate_path' "$quality_marker")"
cp "$quality_gate" "$TMP/quality-gate.json"
rm -f "$quality_gate"
: >"$TMP/args"
if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" POLYEDGE_TEST_ARGS="$TMP/args" \
    POLYEDGE_RESEARCH_DATE=2026-07-30 POLYEDGE_LOCAL_RAW_ROOT=/input/events \
    POLYEDGE_DATA_QUALITY_ONLY=true GIT_SHA="$TEST_GIT_SHA" \
    sh "$ROOT/research/run_primary_daily.sh"
) >"$TMP/quality-missing-gate-stdout" 2>"$TMP/quality-missing-gate-stderr"; then
  echo 'missing quality gate unexpectedly allowed reuse' >&2
  exit 1
fi
test "$(grep -c '^research normalize ' "$TMP/args" || true)" -eq 0

cp "$TMP/quality-gate.json" "$quality_gate"
printf '\n' >>"$quality_gate"
if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" POLYEDGE_TEST_ARGS="$TMP/args" \
    POLYEDGE_RESEARCH_DATE=2026-07-30 POLYEDGE_LOCAL_RAW_ROOT=/input/events \
    POLYEDGE_DATA_QUALITY_ONLY=true GIT_SHA="$TEST_GIT_SHA" \
    sh "$ROOT/research/run_primary_daily.sh"
) >"$TMP/quality-changed-gate-stdout" 2>"$TMP/quality-changed-gate-stderr"; then
  echo 'changed quality gate unexpectedly allowed reuse' >&2
  exit 1
fi

runner="$ROOT/ops/conduit/bin/polyedge-run-job"
daily_command=$(sed -n '/  daily)/,/  replay)/p' "$runner")
replay_command=$(sed -n '/  replay)/,/  prospective)/p' "$runner")
! printf '%s\n' "$daily_command$replay_command" | grep -F 'with-azure-lease' >/dev/null
grep -F '[ "${POLYEDGE_DATA_QUALITY_ONLY:-false}" = true ] && exit 0' "$runner" >/dev/null

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
    POLYEDGE_TEST_ARGS="$TMP/args" \
    POLYEDGE_RESEARCH_DATE=2026-07-30 \
    POLYEDGE_LOCAL_RAW_ROOT=relative \
    sh "$ROOT/research/run_primary_daily.sh"
) >"$TMP/invalid-stdout" 2>"$TMP/invalid-stderr"; then
  echo 'relative local raw root unexpectedly passed' >&2
  exit 1
fi
grep -F 'POLYEDGE_LOCAL_RAW_ROOT must be absolute' "$TMP/invalid-stderr" >/dev/null

run_replay() {
  restore_fail=$1
  local_root=${2:-}
  local_completion=${3:-none}
  rm -rf "$TMP/work"
  mkdir -p "$TMP/work"
  if [ "$local_completion" != none ]; then
    daily="$TMP/work/data/research/daily/2026-07-30/normalized"
    mkdir -p "$daily"
    printf '%s' '{}' >"$daily/events_manifest.json"
    manifest_sha="sha256:$(sha256sum "$daily/events_manifest.json" | cut -d' ' -f1)"
    if [ "$local_completion" = invalid ]; then
      manifest_sha=sha256:0000000000000000000000000000000000000000000000000000000000000000
    fi
    jq -n \
      --arg git_sha "$TEST_GIT_SHA" \
      --arg manifest_sha "$manifest_sha" \
      '{schema_version: 1, date: "2026-07-30", git_sha: $git_sha, events_manifest_sha256: $manifest_sha}' \
      >"$daily/.polyedge-daily-complete.json"
  fi
  : >"$TMP/args"
  (
    cd "$TMP/work"
    PATH="$TMP/bin:$PATH" \
      POLYEDGE_TEST_ARGS="$TMP/args" \
      POLYEDGE_RESEARCH_DATE=2026-07-30 \
      POLYEDGE_LOCAL_RAW_ROOT="$local_root" \
      GIT_SHA="$TEST_GIT_SHA" \
      AZURE_STORAGE_ACCOUNT_NAME=st \
      AZURE_STORAGE_CONTAINER_NAME=events \
      MOCK_RESTORE_FAIL="$restore_fail" \
      sh "$ROOT/research/run_replay_index.sh"
  ) >"$TMP/replay-stdout" 2>"$TMP/replay-stderr"
}

run_replay 1 '' valid
test "$(grep -c '^research restore-normalized-snapshot ' "$TMP/args" || true)" -eq 0
test "$(grep -c '^research normalize ' "$TMP/args" || true)" -eq 0
grep -F '"normalized_source":"local_daily"' "$TMP/replay-stdout" >/dev/null

rm -rf "$TMP/work"
mkdir -p "$TMP/work"
: >"$TMP/args"
if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
    POLYEDGE_TEST_ARGS="$TMP/args" \
    POLYEDGE_RESEARCH_DATE=2026-07-30 \
    POLYEDGE_LOCAL_RAW_ROOT=/input/events \
    POLYEDGE_DATA_QUALITY_ONLY=true \
    GIT_SHA="$TEST_GIT_SHA" \
    sh "$ROOT/research/run_replay_index.sh"
) >"$TMP/quality-replay-stdout" 2>"$TMP/quality-replay-stderr"; then
  echo 'data-quality-only replay accepted missing local daily completion' >&2
  exit 1
fi
test "$(grep -c '^research restore-normalized-snapshot ' "$TMP/args" || true)" -eq 0
test "$(grep -c '^research normalize ' "$TMP/args" || true)" -eq 0

run_replay 1 '' invalid
test "$(grep -c '^research restore-normalized-snapshot ' "$TMP/args")" -eq 1
test "$(grep -c '^research normalize ' "$TMP/args")" -eq 1
grep -F '"normalized_source":"raw_fallback"' "$TMP/replay-stdout" >/dev/null

run_replay 0
test "$(grep -c '^research restore-normalized-snapshot ' "$TMP/args")" -eq 1
test "$(grep -c '^research normalize ' "$TMP/args" || true)" -eq 0
grep -F '"normalized_source":"normalized_snapshot"' "$TMP/replay-stdout" >/dev/null

run_replay 1
test "$(grep -c '^research restore-normalized-snapshot ' "$TMP/args")" -eq 1
test "$(grep -c '^research normalize ' "$TMP/args")" -eq 1
test "$(grep -c '^research publish-normalized-snapshot ' "$TMP/args")" -eq 1
grep -F '"normalized_source":"raw_fallback"' "$TMP/replay-stdout" >/dev/null
grep -F -- '--input azure://st/events/events/2026/07/30/?prefetch_blobs=16' "$TMP/args" >/dev/null
if grep -F 'large_verbose_payload' "$TMP/replay-stdout" "$TMP/replay-stderr" >/dev/null; then
  echo "handled snapshot fallback leaked verbose output into replay logs" >&2
  exit 1
fi

run_replay 1 /input/events
grep -F -- '--input /input/events/2026/07/30' "$TMP/args" >/dev/null
