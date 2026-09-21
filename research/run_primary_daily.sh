#!/bin/sh
set -eu

DATE=${POLYEDGE_RESEARCH_DATE:-$(date -u -d "yesterday" +%Y-%m-%d)}
DAY=$(date -u -d "$DATE" +%Y/%m/%d)
RUN_ID="daily-$DATE-$(date -u +%Y%m%dT%H%M%SZ)-$$"
QUALITY_ONLY=${POLYEDGE_DATA_QUALITY_ONLY:-false}
. "$(dirname "$0")/resolve_raw_input.sh"
INPUT=$(polyedge_raw_input "$DAY")
NORMALIZED="data/research/daily/$DATE/normalized"
NORMALIZED_COMPLETE="$NORMALIZED/.polyedge-daily-complete.json"
QUALITY_ATTEMPT="data/research/daily/$DATE/.polyedge-data-quality-attempt.json"
STAGING="reports/research/staging/$RUN_ID"
MARKETS="$STAGING/markets_summary.json"
QUALITY_GATE="$STAGING/data_quality_gate.json"

case "$QUALITY_ONLY" in true|false) ;; *) echo 'POLYEDGE_DATA_QUALITY_ONLY must equal true or false' >&2; exit 64 ;; esac

if [ "$QUALITY_ONLY" = true ]; then
  if ! printf '%s\n' "${GIT_SHA:-}" | grep -Eq '^[0-9a-f]{40}$'; then
    echo 'GIT_SHA must be the immutable 40-character source commit' >&2
    exit 1
  fi
  today=$(date -u +%Y-%m-%d)
  [ "$DATE" \< "$today" ] || {
    echo 'data-quality-only processing requires a completed UTC date' >&2
    exit 64
  }
fi

valid_completed_quality_day() {
  [ -d "$NORMALIZED" ] && [ ! -L "$NORMALIZED" ] || return 1
  [ -f "$NORMALIZED/events_manifest.json" ] && [ ! -L "$NORMALIZED/events_manifest.json" ] || return 1
  [ -f "$NORMALIZED_COMPLETE" ] && [ ! -L "$NORMALIZED_COMPLETE" ] || return 1
  expected=$(jq -er --arg date "$DATE" --arg git_sha "${GIT_SHA:-}" '
    select(.schema_version == 1 and .date == $date and .git_sha == $git_sha and .mode == "data_quality_only")
    | .events_manifest_sha256 | select(type == "string" and test("^sha256:[0-9a-f]{64}$"))
  ' "$NORMALIZED_COMPLETE" 2>/dev/null) || return 1
  actual="sha256:$(sha256sum "$NORMALIZED/events_manifest.json" | cut -d' ' -f1)"
  [ "$expected" = "$actual" ] || return 1
  gate_path=$(jq -er --arg date "$DATE" '
    .quality_gate_path | select(type == "string" and test("^reports/research/staging/daily-" + $date + "-[0-9]{8}T[0-9]{6}Z-[0-9]+/data_quality_gate\\.json$"))
  ' "$NORMALIZED_COMPLETE" 2>/dev/null) || return 1
  gate_sha=$(jq -er '.quality_gate_sha256 | select(type == "string" and test("^sha256:[0-9a-f]{64}$"))' "$NORMALIZED_COMPLETE" 2>/dev/null) || return 1
  [ -f "$gate_path" ] && [ ! -L "$gate_path" ] || return 1
  [ "$gate_sha" = "sha256:$(sha256sum "$gate_path" | cut -d' ' -f1)" ] || return 1
  stage_dir=$(dirname "$gate_path")
  audit="$stage_dir/data_audit.json"
  execution="$stage_dir/execution_quality.json"
  [ -f "$audit" ] && [ ! -L "$audit" ] && [ -f "$execution" ] && [ ! -L "$execution" ] || return 1
  inventory=$(jq -er '.raw_source_inventory.canonical_sha256 | select(type == "string" and test("^sha256:[0-9a-f]{64}$"))' "$NORMALIZED/events_manifest.json") || return 1
  jq -e --arg date "$DATE" --arg git_sha "${GIT_SHA:-}" --arg manifest "$actual" --arg inventory "$inventory" \
    --arg audit_sha "sha256:$(sha256sum "$audit" | cut -d' ' -f1)" \
    --arg execution_sha "sha256:$(sha256sum "$execution" | cut -d' ' -f1)" '
      .schema == "polyedge.primary_data_quality_gate.v1" and .status == "passed"
      and .date == $date and .git_sha == $git_sha
      and .normalized_manifest_sha256 == $manifest and .source_inventory_sha256 == $inventory
      and .data_audit_sha256 == $audit_sha and .execution_quality_sha256 == $execution_sha
    ' "$gate_path" >/dev/null
}

if [ "$QUALITY_ONLY" = true ] && [ -e "$NORMALIZED_COMPLETE" ]; then
  valid_completed_quality_day || {
    echo 'existing data-quality-only completion is invalid; refusing to overwrite it' >&2
    exit 1
  }
  printf '{"event":"polyedge_primary_daily","date":"%s","status":"reused","mode":"data_quality_only"}\n' "$DATE"
  exit 0
fi

if [ "$QUALITY_ONLY" = true ]; then
  mkdir -p "data/research/daily/$DATE"
  if ! (set -C; : >"$QUALITY_ATTEMPT") 2>/dev/null; then
    echo 'data-quality-only attempt already exists; refusing to overwrite failed pilot evidence' >&2
    exit 1
  fi
  jq -n --arg date "$DATE" --arg git_sha "$GIT_SHA" --arg run_id "$RUN_ID" \
    '{schema_version:1,date:$date,git_sha:$git_sha,run_id:$run_id,status:"started"}' >"$QUALITY_ATTEMPT"
  chmod 600 "$QUALITY_ATTEMPT"
fi

mkdir -p "$STAGING" "data/research/daily/$DATE"
if [ "$QUALITY_ONLY" = false ]; then
  rm -f -- "$NORMALIZED_COMPLETE"
fi

run_stage() {
  label=$1
  shift
  started=$(date +%s)
  output=$(mktemp)
  printf '{"event":"polyedge_primary_daily_stage","stage":"%s","date":"%s","status":"starting"}\n' "$label" "$DATE"
  if "$@" >"$output" 2>&1; then
    finished=$(date +%s)
    rm -f "$output"
    printf '{"event":"polyedge_primary_daily_stage","stage":"%s","date":"%s","status":"completed","duration_seconds":%s}\n' "$label" "$DATE" "$((finished - started))"
    return 0
  else
    status=$?
  fi
  finished=$(date +%s)
  tail -c 65536 "$output" >&2 || true
  rm -f "$output"
  printf '{"event":"polyedge_primary_daily_stage","stage":"%s","date":"%s","status":"failed","exit_code":%s,"duration_seconds":%s}\n' "$label" "$DATE" "$status" "$((finished - started))" >&2
  return "$status"
}

if [ "$QUALITY_ONLY" = false ]; then
  run_stage raw-audit polyedge-rs research audit \
    --input "$INPUT" \
    --exclude-file data_quality/exclusion_windows.yaml \
    --out "$STAGING/raw_data_audit.json" \
    --markdown "$STAGING/raw_data_audit.md"
fi
run_stage normalize polyedge-rs research normalize \
  --input "$INPUT" \
  --out "$NORMALIZED" \
  --format jsonl-indexed-gzip-sharded \
  --overwrite true
run_stage normalized-audit polyedge-rs research audit \
  --input "$NORMALIZED" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/data_audit.json" \
  --markdown "$STAGING/data_audit.md"
run_stage execution-quality polyedge-rs research execution-quality \
  --input "$NORMALIZED" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/execution_quality.json" \
  --markdown "$STAGING/execution_quality.md"

if ! printf '%s\n' "${GIT_SHA:-}" | grep -Eq '^[0-9a-f]{40}$'; then
  echo 'GIT_SHA must be the immutable 40-character source commit' >&2
  exit 1
fi
INPUT_SHA="sha256:$(sha256sum "$NORMALIZED/events_manifest.json" | cut -d' ' -f1)"

write_quality_gate() {
  polyedge-rs research check-primary-daily-quality \
    --date "$DATE" --git-sha "$GIT_SHA" --audit "$STAGING/data_audit.json" \
    --execution-quality "$STAGING/execution_quality.json" \
    --normalized "$NORMALIZED" --out "$QUALITY_GATE"
  jq -e '.status == "passed"' "$QUALITY_GATE" >/dev/null
}

if [ "$QUALITY_ONLY" = true ]; then
  run_stage data-quality-gate write_quality_gate
  gate_sha="sha256:$(sha256sum "$QUALITY_GATE" | cut -d' ' -f1)"
  marker_tmp=$(mktemp "$NORMALIZED_COMPLETE.tmp.XXXXXX")
  jq -n --arg date "$DATE" --arg git_sha "$GIT_SHA" --arg events_manifest_sha256 "$INPUT_SHA" --arg quality_gate_path "$QUALITY_GATE" --arg quality_gate_sha256 "$gate_sha" \
    --arg raw_source_inventory_sha256 "$(jq -r '.source.raw_source_inventory_sha256' "$QUALITY_GATE")" \
    '{schema_version:1,date:$date,git_sha:$git_sha,events_manifest_sha256:$events_manifest_sha256,raw_source_inventory_sha256:$raw_source_inventory_sha256,mode:"data_quality_only",quality_gate_path:$quality_gate_path,quality_gate_sha256:$quality_gate_sha256}' >"$marker_tmp"
  chmod 600 "$marker_tmp"
  mv -f -- "$marker_tmp" "$NORMALIZED_COMPLETE"
  printf '{"event":"polyedge_primary_daily","date":"%s","status":"completed","mode":"data_quality_only","promotion_eligible":false}\n' "$DATE"
  exit 0
fi

run_stage publish-normalized-snapshot polyedge-rs research publish-normalized-snapshot \
  --input "$NORMALIZED" \
  --date "$DATE"
run_stage build-markets polyedge-rs research build-markets \
  --input "$NORMALIZED" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$MARKETS" \
  --markdown "$STAGING/markets_summary.md"
run_stage baseline polyedge-rs research baseline \
  --input "$NORMALIZED" \
  --markets "$MARKETS" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/baseline.json" \
  --markdown "$STAGING/baseline.md"
run_stage regimes polyedge-rs research regimes \
  --input "$NORMALIZED" \
  --markets "$MARKETS" \
  --fill-model queue_proxy_conservative \
  --profile-config research/configs/frozen_candidates.yaml \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/regimes.json" \
  --markdown "$STAGING/regimes.md"
run_stage calibration polyedge-rs research calibration \
  --input "$NORMALIZED" \
  --markets "$MARKETS" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/calibration.json" \
  --markdown "$STAGING/calibration.md"
run_stage sample-size polyedge-rs research sample-size \
  --fill-model queue_proxy_conservative \
  --results "$STAGING/baseline.json" \
  --out "$STAGING/sample_size.json" \
  --markdown "$STAGING/sample_size.md"
run_stage final-report polyedge-rs research report \
  --reports-dir "$STAGING" \
  --out "$STAGING/final_report.json" \
  --markdown "$STAGING/final_report.md"
run_stage publish-daily-bundle polyedge-rs research publish-daily-bundle \
  --date "$DATE" \
  --run-id "$RUN_ID" \
  --input-sha256 "$INPUT_SHA" \
  --expected-runtime-role primary \
  --source-dir "$STAGING" \
  --output-root reports/research/daily \
  --data-audit "$STAGING/data_audit.json"
run_stage latest-report polyedge-rs research report \
  --reports-dir "$STAGING" \
  --out reports/research/latest_daily_report.json \
  --markdown reports/research/latest_daily_report.md
run_stage validate-prospective polyedge-rs research validate-prospective \
  --since 2026-07-13T00:00:00Z \
  --candidates research/configs/frozen_candidates.yaml \
  --reports-dir reports/research/daily \
  --expected-daily-date "$DATE" \
  --out reports/research/prospective/prospective_validation.json \
  --markdown reports/research/prospective/prospective_validation.md

marker_tmp=$(mktemp "$NORMALIZED_COMPLETE.tmp.XXXXXX")
jq -n \
  --arg date "$DATE" \
  --arg git_sha "$GIT_SHA" \
  --arg events_manifest_sha256 "$INPUT_SHA" \
  '{schema_version: 1, date: $date, git_sha: $git_sha, events_manifest_sha256: $events_manifest_sha256}' \
  >"$marker_tmp"
chmod 600 "$marker_tmp"
mv -f -- "$marker_tmp" "$NORMALIZED_COMPLETE"

printf '{"event":"polyedge_primary_daily","date":"%s","status":"completed","snapshot":"published"}\n' "$DATE"
