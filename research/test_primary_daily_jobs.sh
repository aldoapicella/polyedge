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
previous=
for argument in "$@"; do
  if [ "$previous" = "--out" ]; then out=$argument; fi
  if [ "$previous" = "--markdown" ]; then markdown=$argument; fi
  previous=$argument
done
if [ -n "$out" ]; then
  if [ "$command" = "normalize" ] || [ "$command" = "restore-normalized-snapshot" ] || [ "$command" = "build-replay-index" ]; then
    mkdir -p "$out"
  else
    mkdir -p "$(dirname "$out")"
  fi
fi
if [ "$command" = "normalize" ] || [ "$command" = "restore-normalized-snapshot" ]; then
  printf '%s' '{"format":"jsonl-indexed-gzip-sharded","events":1,"raw_source_inventory":{"schema_version":1,"canonical_sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","canonical":{"domain":"polyedge.raw-source-inventory.v1","schema_version":1,"source_kind":"azure_blob","account":"st","container":"events","prefix":"events/2026/07/30/","max_blobs":null,"max_bytes":null,"ordering":"blob_name_ascii_ascending","exhaustive_listing":true,"blob_count":0,"total_bytes":0,"blobs":[]}}}' >"$out/events_manifest.json"
fi
if [ -n "$out" ] && [ ! -d "$out" ]; then
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
