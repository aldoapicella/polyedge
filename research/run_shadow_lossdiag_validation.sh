#!/bin/sh
set -eu

CONFIG="${LOSSDIAG_VALIDATION_CONFIG:-/app/research/configs/shadow_lossdiag_validation_2026-07-23_v3.json}"
test -f "$CONFIG"
jq -e '
  .schema == "polyedge.shadow_lossdiag_validation.v1"
  and .schema_version == 1
  and .validation_id == "campaign-2026-07-23-lossdiag-v3"
  and .promotion_eligible == false
  and .counts_toward_protocol_v3_evidence == false
  and .date == "2026-07-23"
  and .source.account == "stpolyedge6urdjr5nmwx7w"
  and .source.container == "polyedge-shadow-events"
  and .source.campaign_id == "campaign-2026-07-23"
  and .source.prefix == "shadow-events/campaign-2026-07-23/2026/07/23/"
  and .output.container == "polyedge-research-validation"
  and (.output.work_root | contains("campaign-2026-07-23-lossdiag-v3"))
  and (.output.projected_cache_root | contains("campaign-2026-07-23-lossdiag-v3"))
  and (.output.report_root | contains("campaign-2026-07-23-lossdiag-v3"))
  and (.output.correction_root | contains("campaign-2026-07-23-lossdiag-v3"))
  and (.output.lease_blob | contains("campaign-2026-07-23-lossdiag-v3"))
  and .limits.cpu == 4
  and .limits.memory == "8Gi"
  and .limits.max_loss_diagnostics_rss_kib == 6815744
  and .limits.max_container_peak_bytes == 7301448704
  and .limits.replica_retry_limit == 0
  and .safety.execution_mode == "paper"
  and .safety.allow_live == false
  and .safety.run_bot_on_startup == false
  and .safety.enable_taker_orders == false
  and .safety.literal_fifo_available == false
  and .safety.queue_position_source == "inferred_size_ahead"
' "$CONFIG" >/dev/null

test "${POLYEDGE_CAMPAIGN_LEASE_ACTIVE:-false}" = "true"
test -n "${POLYEDGE_CAMPAIGN_LEASE_ID:-}"
test "${EXECUTION_MODE:-}" = "paper"
test "${ALLOW_LIVE:-}" = "false"
test "${RUN_BOT_ON_STARTUP:-}" = "false"
test "${ENABLE_TAKER_ORDERS:-}" = "false"
test "${AZURE_STORAGE_ACCOUNT_NAME:-}" = "$(jq -r '.source.account' "$CONFIG")"
test "${SHADOW_SOURCE_CONTAINER_NAME:-}" = "$(jq -r '.source.container' "$CONFIG")"
test "${AZURE_STORAGE_CONTAINER_NAME:-}" = "$(jq -r '.output.container' "$CONFIG")"
test "${POLYEDGE_CAMPAIGN_LEASE_BLOB:-}" = "$(jq -r '.output.lease_blob' "$CONFIG")"
test -n "${EXPECTED_GIT_SHA:-}"
test "${#EXPECTED_GIT_SHA}" -eq 40
test "${GIT_SHA:-}" = "$EXPECTED_GIT_SHA"
test -n "${EXPECTED_RAW_SOURCE_INVENTORY_SHA256:-}"
test -n "${SOURCE_PROJECTED_FILESET_SHA256:-}"

VALIDATION_ID="$(jq -r '.validation_id' "$CONFIG")"
DATE="$(jq -r '.date' "$CONFIG")"
SOURCE_CAMPAIGN_ID="$(jq -r '.source.campaign_id' "$CONFIG")"
SOURCE_PREFIX="$(jq -r '.source.prefix' "$CONFIG")"
WORK_ROOT="$(jq -r '.output.work_root' "$CONFIG")"
REPORT_ROOT="$(jq -r '.output.report_root' "$CONFIG")"
CORRECTION_ROOT="$(jq -r '.output.correction_root' "$CONFIG")"
CACHE_PATH="$(jq -r '.output.projected_cache_root' "$CONFIG")"
MAX_RSS_KIB="$(jq -r '.limits.max_loss_diagnostics_rss_kib' "$CONFIG")"
MAX_CONTAINER_PEAK_BYTES="$(jq -r '.limits.max_container_peak_bytes' "$CONFIG")"
CONFIG_SHA256="sha256:$(sha256sum "$CONFIG" | cut -d' ' -f1)"
RUN_ID="$VALIDATION_ID-$(date -u +%Y%m%dT%H%M%SZ)"
INPUT="azure://$AZURE_STORAGE_ACCOUNT_NAME/$SHADOW_SOURCE_CONTAINER_NAME/$SOURCE_PREFIX?prefetch_blobs=16"
CACHE_ROOT="azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/$CACHE_PATH"
NORMALIZED="$WORK_ROOT/$DATE/normalized"
CUMULATIVE="$WORK_ROOT/cumulative/$DATE/normalized"
STAGING="$REPORT_ROOT/staging/$RUN_ID"
LOSS_DIAGNOSTICS="$STAGING/loss_diagnostics"
PROJECTED_DAY_MANIFEST="$STAGING/projected_day_manifest.json"
CUMULATIVE_MANIFEST="$STAGING/cumulative_input_manifest.json"

case "$INPUT $CACHE_ROOT $WORK_ROOT $REPORT_ROOT $CORRECTION_ROOT" in
  *"reports/research/shadow/campaigns/campaign-2026-07-23/"*)
    echo "validation attempted to target the canonical campaign report root" >&2
    exit 1
    ;;
esac

mkdir -p "$NORMALIZED" "$CUMULATIVE" "$STAGING"

run_stage() {
  label="$1"
  shift
  echo "polyedge_lossdiag_validation stage=$label status=starting"
  "$@" >/dev/null
  echo "polyedge_lossdiag_validation stage=$label status=completed"
}

run_stage begin-correction polyedge-rs research begin-shadow-correction \
  --campaign-id "$SOURCE_CAMPAIGN_ID" \
  --correction-id "$RUN_ID" \
  --from "$DATE" \
  --through "$DATE" \
  --reason "isolated loss-diagnostics v3 timestamp-precision, memory, and semantic validation; promotion ineligible" \
  --out "$CORRECTION_ROOT/active.json"
run_stage raw-audit polyedge-rs research audit \
  --input "$INPUT" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/raw_data_audit.json" \
  --markdown "$STAGING/raw_data_audit.md"
run_stage normalize polyedge-rs research normalize \
  --input "$INPUT" \
  --out "$NORMALIZED" \
  --format jsonl-indexed-gzip-sharded \
  --overwrite true \
  --decision-grade-projection true

RAW_AUDIT_INVENTORY="$(jq -r '.result.raw_source_inventory.canonical_sha256' "$STAGING/raw_data_audit.json")"
NORMALIZED_INVENTORY="$(jq -r '.raw_source_inventory.canonical_sha256' "$NORMALIZED/events_manifest.json")"
test "$RAW_AUDIT_INVENTORY" = "$EXPECTED_RAW_SOURCE_INVENTORY_SHA256"
test "$NORMALIZED_INVENTORY" = "$EXPECTED_RAW_SOURCE_INVENTORY_SHA256"
jq -e --arg prefix "$SOURCE_PREFIX" '
  .raw_source_inventory.canonical.source_kind == "azure_blob"
  and .raw_source_inventory.canonical.account == "stpolyedge6urdjr5nmwx7w"
  and .raw_source_inventory.canonical.container == "polyedge-shadow-events"
  and .raw_source_inventory.canonical.prefix == $prefix
  and .raw_source_inventory.canonical.exhaustive_listing == true
  and .raw_source_inventory.canonical.max_blobs == null
  and .raw_source_inventory.canonical.max_bytes == null
  and ([.raw_source_inventory.canonical.blobs[] |
    (.etag != null and .sha256 != null and .content_length >= 0 and .sealed != null)] | all)
' "$NORMALIZED/events_manifest.json" >/dev/null

run_stage publish-projected-day polyedge-rs research publish-projected-day \
  --normalized "$NORMALIZED" \
  --date "$DATE" \
  --campaign-id "$SOURCE_CAMPAIGN_ID" \
  --cache-root "$CACHE_ROOT" \
  --out "$PROJECTED_DAY_MANIFEST" \
  --require-azure-source true \
  --expected-source-container "$SHADOW_SOURCE_CONTAINER_NAME"
PROJECTED_INVENTORY="$(jq -r '.canonical.raw_source_inventory.canonical_sha256' "$PROJECTED_DAY_MANIFEST")"
PROJECTED_FILESET_SHA256="sha256:$(jq -cS '[.canonical.files[] | {relative_path,rows,bytes,sha256}] | sort_by(.relative_path)' "$PROJECTED_DAY_MANIFEST" | sha256sum | cut -d' ' -f1)"
test "$PROJECTED_INVENTORY" = "$EXPECTED_RAW_SOURCE_INVENTORY_SHA256"
for fileset_sha in "$PROJECTED_FILESET_SHA256" "$SOURCE_PROJECTED_FILESET_SHA256"; do
  test "$(jq -nr --arg value "$fileset_sha" '$value | test("^sha256:[0-9a-f]{64}$")')" = "true"
done
test "$PROJECTED_FILESET_SHA256" != "$SOURCE_PROJECTED_FILESET_SHA256"

run_stage materialize polyedge-rs research materialize-projected-campaign \
  --since "$DATE" \
  --through "$DATE" \
  --campaign-id "$SOURCE_CAMPAIGN_ID" \
  --cache-root "$CACHE_ROOT" \
  --out "$CUMULATIVE" \
  --manifest "$CUMULATIVE_MANIFEST" \
  --require-azure-source true \
  --expected-source-container "$SHADOW_SOURCE_CONTAINER_NAME"
test "$(jq -r '.segments | length' "$CUMULATIVE_MANIFEST")" = "1"
test "$(jq -r '.segments[0].raw_source_inventory_sha256' "$CUMULATIVE_MANIFEST")" = "$EXPECTED_RAW_SOURCE_INVENTORY_SHA256"

TIME_JSON="$STAGING/loss_diagnostics_time.json"
echo "polyedge_lossdiag_validation stage=loss-diagnostics status=starting"
if ! /usr/bin/time -f '{"exit_status":%x,"elapsed_seconds":%e,"user_seconds":%U,"system_seconds":%S,"max_rss_kib":%M}' \
  -o "$TIME_JSON" \
  polyedge-rs research loss-diagnostics --input "$CUMULATIVE" --out "$LOSS_DIAGNOSTICS" >/dev/null; then
  FAILURE_MAX_RSS_KIB=unknown
  if [ -s "$TIME_JSON" ]; then
    FAILURE_MAX_RSS_KIB="$(jq -r '.max_rss_kib // "unknown"' "$TIME_JSON" 2>/dev/null || echo unknown)"
  fi
  FAILURE_CGROUP_PEAK_BYTES=unavailable
  if [ -r /sys/fs/cgroup/memory.peak ]; then
    FAILURE_CGROUP_PEAK_BYTES="$(cat /sys/fs/cgroup/memory.peak)"
  fi
  echo "polyedge_lossdiag_validation stage=loss-diagnostics status=failed max_rss_kib=$FAILURE_MAX_RSS_KIB cgroup_peak_bytes=$FAILURE_CGROUP_PEAK_BYTES" >&2
  exit 1
fi
echo "polyedge_lossdiag_validation stage=loss-diagnostics status=completed"

MAX_RSS_ACTUAL="$(jq -r '.max_rss_kib' "$TIME_JSON")"
test "$MAX_RSS_ACTUAL" -le "$MAX_RSS_KIB"
CGROUP_PEAK_BYTES=null
CGROUP_PEAK_AVAILABLE=false
if [ -r /sys/fs/cgroup/memory.peak ]; then
  CGROUP_PEAK_BYTES="$(cat /sys/fs/cgroup/memory.peak)"
  CGROUP_PEAK_AVAILABLE=true
  test "$CGROUP_PEAK_BYTES" -le "$MAX_CONTAINER_PEAK_BYTES"
fi
jq -e '
  .result.status == "complete_diagnostic"
  and .result.counts.duplicate_event_lines == 0
  and .result.completion_checks.no_exact_duplicate_event_lines == true
  and .result.snapshot_identity.stable_before_after_read == true
  and .result.queue_position_field == "inferred_size_ahead"
  and .result.literal_fifo_rank_available == false
' "$LOSS_DIAGNOSTICS/loss_diagnostics.json" >/dev/null

for artifact in order_lifecycle_fact.jsonl fill_markout_fact.jsonl loss_diagnostics.json loss_diagnostics.md; do
  expected="$(jq -r --arg filename "$artifact" '.artifacts[] | select(.filename == $filename) | .sha256' "$LOSS_DIAGNOSTICS/loss_diagnostics_artifact_manifest.json")"
  actual="sha256:$(sha256sum "$LOSS_DIAGNOSTICS/$artifact" | cut -d' ' -f1)"
  test -n "$expected"
  test "$actual" = "$expected"
done

SEMANTIC_SHA256="sha256:$(jq -cS '{
  config,
  fill_model,
  split_method,
  warnings,
  data_window,
  result: (.result | del(.artifacts, .snapshot_identity))
}' "$LOSS_DIAGNOSTICS/loss_diagnostics.json" | sha256sum | cut -d' ' -f1)"
ORDER_FACT_SHA256="sha256:$(sha256sum "$LOSS_DIAGNOSTICS/order_lifecycle_fact.jsonl" | cut -d' ' -f1)"
FILL_FACT_SHA256="sha256:$(sha256sum "$LOSS_DIAGNOSTICS/fill_markout_fact.jsonl" | cut -d' ' -f1)"
CAMPAIGN_INDEX_SHA256="sha256:$(sha256sum "$CUMULATIVE/campaign_index.json" | cut -d' ' -f1)"
run_stage final-source-audit polyedge-rs research audit \
  --input "$INPUT" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/final_raw_data_audit.json" \
  --markdown "$STAGING/final_raw_data_audit.md"
FINAL_RAW_INVENTORY="$(jq -r '.result.raw_source_inventory.canonical_sha256' "$STAGING/final_raw_data_audit.json")"
test "$FINAL_RAW_INVENTORY" = "$EXPECTED_RAW_SOURCE_INVENTORY_SHA256"
jq -n \
  --arg schema "polyedge.shadow_lossdiag_validation_metrics.v1" \
  --arg validation_id "$VALIDATION_ID" \
  --arg git_sha "$GIT_SHA" \
  --arg config_sha256 "$CONFIG_SHA256" \
  --arg source_inventory_sha256 "$EXPECTED_RAW_SOURCE_INVENTORY_SHA256" \
  --arg source_projected_fileset_sha256 "$SOURCE_PROJECTED_FILESET_SHA256" \
  --arg projected_fileset_sha256 "$PROJECTED_FILESET_SHA256" \
  --arg campaign_index_sha256 "$CAMPAIGN_INDEX_SHA256" \
  --arg semantic_sha256 "$SEMANTIC_SHA256" \
  --arg order_fact_sha256 "$ORDER_FACT_SHA256" \
  --arg fill_fact_sha256 "$FILL_FACT_SHA256" \
  --argjson max_rss_kib "$MAX_RSS_ACTUAL" \
  --argjson cgroup_peak_bytes "$CGROUP_PEAK_BYTES" \
  --argjson cgroup_peak_available "$CGROUP_PEAK_AVAILABLE" \
  '{
    schema: $schema,
    validation_id: $validation_id,
    promotion_eligible: false,
    counts_toward_protocol_v3_evidence: false,
    git_sha: $git_sha,
    config_sha256: $config_sha256,
    source_inventory_sha256: $source_inventory_sha256,
    source_projected_fileset_sha256: $source_projected_fileset_sha256,
    projected_fileset_sha256: $projected_fileset_sha256,
    normalized_timestamp_precision: "rfc3339_autosi",
    campaign_index_sha256: $campaign_index_sha256,
    semantic_sha256: $semantic_sha256,
    order_fact_sha256: $order_fact_sha256,
    fill_fact_sha256: $fill_fact_sha256,
    max_rss_kib: $max_rss_kib,
    cgroup_peak_available: $cgroup_peak_available,
    cgroup_peak_bytes: $cgroup_peak_bytes,
    queue_position_source: "inferred_size_ahead",
    literal_fifo_available: false
  }' >"$STAGING/loss_diagnostics_metrics.json"

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
  --out "$STAGING/markets_summary.json" \
  --markdown "$STAGING/markets_summary.md"
run_stage baseline polyedge-rs research baseline \
  --input "$NORMALIZED" \
  --markets "$STAGING/markets_summary.json" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/baseline.json" \
  --markdown "$STAGING/baseline.md"
run_stage regimes polyedge-rs research regimes \
  --input "$NORMALIZED" \
  --markets "$STAGING/markets_summary.json" \
  --fill-model queue_proxy_conservative \
  --profile-config research/configs/frozen_candidates.yaml \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/regimes.json" \
  --markdown "$STAGING/regimes.md"
run_stage calibration polyedge-rs research calibration \
  --input "$NORMALIZED" \
  --markets "$STAGING/markets_summary.json" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "$STAGING/calibration.json" \
  --markdown "$STAGING/calibration.md"
run_stage report polyedge-rs research report \
  --reports-dir "$STAGING" \
  --out "$STAGING/final_report.json" \
  --markdown "$STAGING/final_report.md"
INPUT_SHA256="sha256:$(sha256sum "$NORMALIZED/events_manifest.json" | cut -d' ' -f1)"
run_stage publish-validation-bundle polyedge-rs research publish-daily-bundle \
  --date "$DATE" \
  --run-id "$RUN_ID" \
  --input-sha256 "$INPUT_SHA256" \
  --expected-runtime-role profitability_shadow \
  --source-dir "$STAGING" \
  --output-root "$REPORT_ROOT/daily" \
  --data-audit "$STAGING/data_audit.json"
run_stage complete-correction polyedge-rs research complete-shadow-correction \
  --campaign-id "$SOURCE_CAMPAIGN_ID" \
  --from "$DATE" \
  --through "$DATE" \
  --out "$CORRECTION_ROOT/active.json"

echo "polyedge_lossdiag_validation status=succeeded validation_id=$VALIDATION_ID semantic_sha256=$SEMANTIC_SHA256 max_rss_kib=$MAX_RSS_ACTUAL cgroup_peak_bytes=$CGROUP_PEAK_BYTES"
