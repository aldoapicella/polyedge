#!/bin/sh
set -eu

DATE=${POLYEDGE_RESEARCH_DATE:-$(date -u -d "yesterday" +%Y-%m-%d)}
DAY=$(date -u -d "$DATE" +%Y/%m/%d)
RUN_ID="daily-$DATE-$(date -u +%Y%m%dT%H%M%SZ)"
INPUT="azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/events/$DAY/?prefetch_blobs=${POLYEDGE_RESEARCH_PREFETCH_BLOBS:-16}"
NORMALIZED="data/research/daily/$DATE/normalized"
STAGING="reports/research/staging/$RUN_ID"
MARKETS="$STAGING/markets_summary.json"

mkdir -p "$STAGING" "data/research/daily/$DATE"

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

run_stage raw-audit polyedge-rs research audit \
  --input "$INPUT" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/raw_data_audit.json" \
  --markdown "$STAGING/raw_data_audit.md"
run_stage normalize polyedge-rs research normalize \
  --input "$INPUT" \
  --out "$NORMALIZED" \
  --format jsonl-indexed-gzip-sharded \
  --overwrite true
run_stage publish-normalized-snapshot polyedge-rs research publish-normalized-snapshot \
  --input "$NORMALIZED" \
  --date "$DATE"
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
  --results "$STAGING/baseline.json" \
  --out "$STAGING/sample_size.json" \
  --markdown "$STAGING/sample_size.md"
run_stage final-report polyedge-rs research report \
  --reports-dir "$STAGING" \
  --out "$STAGING/final_report.json" \
  --markdown "$STAGING/final_report.md"
INPUT_SHA="sha256:$(sha256sum "$NORMALIZED/events_manifest.json" | cut -d' ' -f1)"
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

printf '{"event":"polyedge_primary_daily","date":"%s","status":"completed","snapshot":"published"}\n' "$DATE"
