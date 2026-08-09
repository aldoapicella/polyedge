#!/bin/sh
set -eu

DATE=${POLYEDGE_RESEARCH_DATE:-$(date -u -d "yesterday" +%Y-%m-%d)}
DAY=$(date -u -d "$DATE" +%Y/%m/%d)
. "$(dirname "$0")/resolve_raw_input.sh"
INPUT=$(polyedge_raw_input "$DAY")
ROOT="data/research/replay-index/$DATE"
NORMALIZED="$ROOT/normalized"
DAILY_NORMALIZED="data/research/daily/$DATE/normalized"
DAILY_COMPLETE="$DAILY_NORMALIZED/.polyedge-daily-complete.json"
mkdir -p "$ROOT"

run_stage() {
  label=$1
  shift
  started=$(date +%s)
  output=$(mktemp)
  printf '{"event":"polyedge_replay_index_stage","stage":"%s","date":"%s","status":"starting"}\n' "$label" "$DATE"
  if "$@" >"$output" 2>&1; then
    finished=$(date +%s)
    rm -f "$output"
    printf '{"event":"polyedge_replay_index_stage","stage":"%s","date":"%s","status":"completed","duration_seconds":%s}\n' "$label" "$DATE" "$((finished - started))"
    return 0
  else
    status=$?
  fi
  finished=$(date +%s)
  tail -c 65536 "$output" >&2 || true
  rm -f "$output"
  printf '{"event":"polyedge_replay_index_stage","stage":"%s","date":"%s","status":"failed","exit_code":%s,"duration_seconds":%s}\n' "$label" "$DATE" "$status" "$((finished - started))" >&2
  return "$status"
}

try_restore_snapshot() {
  started=$(date +%s)
  output=$(mktemp)
  printf '{"event":"polyedge_replay_index_stage","stage":"restore-normalized-snapshot","date":"%s","status":"starting"}\n' "$DATE"
  if polyedge-rs research restore-normalized-snapshot \
    --out "$NORMALIZED" \
    --date "$DATE" >"$output" 2>&1; then
    finished=$(date +%s)
    rm -f "$output"
    printf '{"event":"polyedge_replay_index_stage","stage":"restore-normalized-snapshot","date":"%s","status":"completed","duration_seconds":%s}\n' "$DATE" "$((finished - started))"
    return 0
  else
    status=$?
  fi
  finished=$(date +%s)
  rm -f "$output"
  printf '{"event":"polyedge_replay_index_stage","stage":"restore-normalized-snapshot","date":"%s","status":"raw_fallback","exit_code":%s,"duration_seconds":%s}\n' "$DATE" "$status" "$((finished - started))"
  return "$status"
}

try_local_daily() {
  [ -d "$DAILY_NORMALIZED" ] && [ ! -L "$DAILY_NORMALIZED" ] || return 1
  [ -f "$DAILY_NORMALIZED/events_manifest.json" ] && [ ! -L "$DAILY_NORMALIZED/events_manifest.json" ] || return 1
  [ -f "$DAILY_COMPLETE" ] && [ ! -L "$DAILY_COMPLETE" ] || return 1
  expected=$(jq -er \
    --arg date "$DATE" \
    --arg git_sha "${GIT_SHA:-}" \
    'select(.schema_version == 1 and .date == $date and .git_sha == $git_sha) | .events_manifest_sha256 | select(type == "string" and test("^sha256:[0-9a-f]{64}$"))' \
    "$DAILY_COMPLETE" 2>/dev/null) || return 1
  actual="sha256:$(sha256sum "$DAILY_NORMALIZED/events_manifest.json" | cut -d' ' -f1)"
  [ "$expected" = "$actual" ] || return 1
  NORMALIZED="$DAILY_NORMALIZED"
}

source_kind=local_daily
if try_local_daily; then
  printf '{"event":"polyedge_replay_index_stage","stage":"reuse-local-daily","date":"%s","status":"completed"}\n' "$DATE"
else
  source_kind=normalized_snapshot
  if ! try_restore_snapshot; then
    source_kind=raw_fallback
    run_stage normalize-fallback polyedge-rs research normalize \
      --input "$INPUT" \
      --out "$NORMALIZED" \
      --format jsonl-indexed-gzip-sharded \
      --overwrite true
    run_stage publish-normalized-snapshot polyedge-rs research publish-normalized-snapshot \
      --input "$NORMALIZED" \
      --date "$DATE"
  fi
fi

run_stage build-replay-index polyedge-rs research build-replay-index \
  --input "$NORMALIZED" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$ROOT"

printf '{"event":"polyedge_replay_index","date":"%s","status":"completed","normalized_source":"%s"}\n' "$DATE" "$source_kind"
