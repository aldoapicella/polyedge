#!/bin/sh
set -eu

if [ "${POLYEDGE_CAMPAIGN_LEASE_ACTIVE:-false}" != "true" ] \
  || [ -z "${POLYEDGE_CAMPAIGN_LEASE_ID:-}" ] \
  || [ -z "${POLYEDGE_CAMPAIGN_LEASE_BLOB:-}" ]; then
  echo "shadow daily writer requires the Azure campaign lease wrapper" >&2
  exit 1
fi

TODAY="$(date -u +%Y-%m-%d)"
DATE="${SHADOW_REPORT_DATE:-$(date -u -d 'yesterday' +%Y-%m-%d)}"
if [ "$(date -u -d "$DATE" +%Y-%m-%d 2>/dev/null || true)" != "$DATE" ]; then
  echo "SHADOW_REPORT_DATE must be a valid YYYY-MM-DD UTC date" >&2
  exit 1
fi
if [ "$(date -u -d "$DATE" +%s)" -ge "$(date -u -d "$TODAY" +%s)" ]; then
  echo "SHADOW_REPORT_DATE must be a sealed UTC day before today" >&2
  exit 1
fi
CASCADE_THROUGH="${SHADOW_CASCADE_THROUGH:-$(date -u -d 'yesterday' +%Y-%m-%d)}"
if [ "$(date -u -d "$CASCADE_THROUGH" +%Y-%m-%d 2>/dev/null || true)" != "$CASCADE_THROUGH" ] \
  || [ "$(date -u -d "$CASCADE_THROUGH" +%s)" -ge "$(date -u -d "$TODAY" +%s)" ] \
  || [ "$(date -u -d "$CASCADE_THROUGH" +%s)" -lt "$(date -u -d "$DATE" +%s)" ]; then
  echo "SHADOW_CASCADE_THROUGH must be a sealed UTC date on or after SHADOW_REPORT_DATE" >&2
  exit 1
fi
CAMPAIGN_ID="campaign-2026-07-12"

# A historical correction always rebuilds every dependent wallet day in
# chronological order under the same Azure lease. Normal scheduled runs have
# DATE == CASCADE_THROUGH and therefore execute exactly once.
if [ "${POLYEDGE_SHADOW_CASCADE_CHILD:-false}" != "true" ]; then
  CORRECTION_ID="${SHADOW_CORRECTION_ID:-shadow-$DATE-through-$CASCADE_THROUGH}"
  CORRECTION_REASON="${SHADOW_CORRECTION_REASON:-chronological schema-v2 evidence rebuild}"
  polyedge-rs research begin-shadow-correction \
    --campaign-id "$CAMPAIGN_ID" \
    --correction-id "$CORRECTION_ID" \
    --from "$DATE" \
    --through "$CASCADE_THROUGH" \
    --reason "$CORRECTION_REASON" \
    --out reports/research/shadow/corrections/active.json >/dev/null
  CURRENT="$DATE"
  while [ "$(date -u -d "$CURRENT" +%s)" -le "$(date -u -d "$CASCADE_THROUGH" +%s)" ]; do
    echo "polyedge_shadow_cascade date=$CURRENT through=$CASCADE_THROUGH status=starting"
    POLYEDGE_SHADOW_CASCADE_CHILD=true \
      SHADOW_REPORT_DATE="$CURRENT" \
      SHADOW_CASCADE_THROUGH="$CASCADE_THROUGH" \
      sh "$0"
    echo "polyedge_shadow_cascade date=$CURRENT through=$CASCADE_THROUGH status=completed"
    CURRENT="$(date -u -d "$CURRENT + 1 day" +%Y-%m-%d)"
  done
  polyedge-rs research complete-shadow-correction \
    --campaign-id "$CAMPAIGN_ID" \
    --from "$DATE" \
    --through "$CASCADE_THROUGH" \
    --out reports/research/shadow/corrections/active.json >/dev/null
  exit 0
fi

DAY="$(date -u -d "$DATE" +%Y/%m/%d)"
RUN_ID="shadow-$DATE-$(date -u +%Y%m%dT%H%M%SZ)"
SOURCE_CONTAINER="${SHADOW_SOURCE_CONTAINER_NAME:?SHADOW_SOURCE_CONTAINER_NAME is required}"
EXECUTION_MODEL_BLOB_NAME="${SHADOW_EXECUTION_MODEL_BLOB_NAME:?SHADOW_EXECUTION_MODEL_BLOB_NAME is required}"
RESEARCH_CONTAINER="${AZURE_STORAGE_CONTAINER_NAME:?AZURE_STORAGE_CONTAINER_NAME is required}"
ROOT="azure://$AZURE_STORAGE_ACCOUNT_NAME/$SOURCE_CONTAINER"
CAMPAIGN_PREFIX="shadow-events/campaign-2026-07-12"
# The deployed shadow stream first contains events on 2026-07-13. Keep the
# campaign/wallet identity at July 12, but never fabricate an empty cache day.
PROJECTED_DATA_START="${SHADOW_PROJECTED_DATA_START:-2026-07-13}"
if [ "$(date -u -d "$PROJECTED_DATA_START" +%Y-%m-%d 2>/dev/null || true)" != "$PROJECTED_DATA_START" ] \
  || [ "$(date -u -d "$PROJECTED_DATA_START" +%s)" -gt "$(date -u -d "$DATE" +%s)" ]; then
  echo "SHADOW_PROJECTED_DATA_START must be a valid UTC date on or before SHADOW_REPORT_DATE" >&2
  exit 1
fi
INPUT="$ROOT/$CAMPAIGN_PREFIX/$DAY/?prefetch_blobs=16"
NORMALIZED="data/research/shadow/$DATE/normalized"
CUMULATIVE_NORMALIZED="data/research/shadow/cumulative/$DATE/normalized"
STAGING="reports/research/shadow/staging/$RUN_ID"
CACHE_ROOT="${SHADOW_PROJECTED_CACHE_ROOT:-azure://$AZURE_STORAGE_ACCOUNT_NAME/$RESEARCH_CONTAINER/data/research/shadow/$CAMPAIGN_ID/projected-cache}"
CACHE_DAY_MANIFEST="$STAGING/projected_day_manifest.json"
CUMULATIVE_INPUT_MANIFEST="$STAGING/cumulative_input_manifest.json"
MARKETS="$STAGING/markets_summary.json"
CUMULATIVE_MARKETS="$STAGING/cumulative_markets_summary.json"
CUMULATIVE_REGIMES="$STAGING/cumulative_regimes.json"

mkdir -p "$STAGING" "$NORMALIZED" "$CUMULATIVE_NORMALIZED"

run_stage() {
  label="$1"
  shift
  echo "polyedge_shadow_daily stage=$label date=$DATE status=starting"
  "$@" >/dev/null
  echo "polyedge_shadow_daily stage=$label date=$DATE status=completed"
}

run_stage raw-audit polyedge-rs research audit --input "$INPUT" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/raw_data_audit.json" --markdown "$STAGING/raw_data_audit.md"
run_stage normalize-day polyedge-rs research normalize --input "$INPUT" --out "$NORMALIZED" --format jsonl-indexed-gzip-sharded --overwrite true --decision-grade-projection true
run_stage publish-projected-day polyedge-rs research publish-projected-day --normalized "$NORMALIZED" --date "$DATE" --campaign-id "$CAMPAIGN_ID" --cache-root "$CACHE_ROOT" --out "$CACHE_DAY_MANIFEST" --require-azure-source true --expected-source-container "$SOURCE_CONTAINER"
run_stage materialize-projected-campaign polyedge-rs research materialize-projected-campaign --since "$PROJECTED_DATA_START" --through "$DATE" --campaign-id "$CAMPAIGN_ID" --cache-root "$CACHE_ROOT" --out "$CUMULATIVE_NORMALIZED" --manifest "$CUMULATIVE_INPUT_MANIFEST" --require-azure-source true --expected-source-container "$SOURCE_CONTAINER"
run_stage normalized-audit polyedge-rs research audit --input "$NORMALIZED" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/data_audit.json" --markdown "$STAGING/data_audit.md"
run_stage execution-quality polyedge-rs research execution-quality --input "$NORMALIZED" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/execution_quality.json" --markdown "$STAGING/execution_quality.md"
run_stage build-markets-day polyedge-rs research build-markets --input "$NORMALIZED" --exclude-file data_quality/exclusion_windows.yaml --out "$MARKETS" --markdown "$STAGING/markets_summary.md"
run_stage baseline-day polyedge-rs research baseline --input "$NORMALIZED" --markets "$MARKETS" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/baseline.json" --markdown "$STAGING/baseline.md"
run_stage regimes-day polyedge-rs research regimes --input "$NORMALIZED" --markets "$MARKETS" --fill-model queue_proxy_conservative --profile-config research/configs/frozen_candidates.yaml --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/regimes.json" --markdown "$STAGING/regimes.md"
run_stage calibration-day polyedge-rs research calibration --input "$NORMALIZED" --markets "$MARKETS" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/calibration.json" --markdown "$STAGING/calibration.md"
run_stage report-day polyedge-rs research report --reports-dir "$STAGING" --out "$STAGING/final_report.json" --markdown "$STAGING/final_report.md"

# Full cross-day replay now consumes only verified, immutable projected-day
# shards through DATE. It never reads or normalizes the open current UTC day.
run_stage build-markets-cumulative polyedge-rs research build-markets --input "$CUMULATIVE_NORMALIZED" --exclude-file data_quality/exclusion_windows.yaml --out "$CUMULATIVE_MARKETS" --markdown "$STAGING/cumulative_markets_summary.md"
run_stage regimes-cumulative polyedge-rs research regimes --input "$CUMULATIVE_NORMALIZED" --markets "$CUMULATIVE_MARKETS" --fill-model queue_proxy_conservative --profile-config research/configs/frozen_candidates.yaml --exclude-file data_quality/exclusion_windows.yaml --out "$CUMULATIVE_REGIMES" --markdown "$STAGING/cumulative_regimes.md"
run_stage build-cumulative-wallet polyedge-rs research build-cumulative-wallet --regimes "$CUMULATIVE_REGIMES" --campaign-manifest "$CUMULATIVE_INPUT_MANIFEST" --snapshot-date "$DATE" --out "$STAGING/cumulative_wallet.json"

INPUT_SHA="sha256:$(sha256sum "$NORMALIZED/events_manifest.json" | cut -d' ' -f1)"
run_stage publish-daily-bundle polyedge-rs research publish-daily-bundle --date "$DATE" --run-id "$RUN_ID" --input-sha256 "$INPUT_SHA" --expected-runtime-role profitability_shadow --source-dir "$STAGING" --output-root reports/research/shadow/daily --data-audit "$STAGING/data_audit.json"
if [ "$DATE" = "$CASCADE_THROUGH" ]; then
  run_stage validate-prospective polyedge-rs research validate-prospective --since 2026-07-12T00:00:00Z --candidates research/configs/frozen_candidates.yaml --reports-dir reports/research/shadow/daily --expected-daily-date "$DATE" --out reports/research/shadow/prospective/prospective_validation.json --markdown reports/research/shadow/prospective/prospective_validation.md
  run_stage evaluate-profitability polyedge-rs research evaluate-profitability --daily-root reports/research/shadow/daily --prospective reports/research/shadow/prospective/prospective_validation.json --gate-config research/configs/profitability_gate.yaml --execution-model "$EXECUTION_MODEL_BLOB_NAME" --out reports/research/profitability/latest.json
else
  echo "polyedge_shadow_daily stage=terminal-evidence date=$DATE status=deferred-through-$CASCADE_THROUGH"
fi
