#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/work"

RAW_SHA="sha256:$(printf 'b%.0s' $(seq 1 64))"
FILE_SHA="sha256:$(printf 'c%.0s' $(seq 1 64))"
PROJECTED_FILESET_SHA="sha256:$(jq -cnS --arg sha "$FILE_SHA" \
  '[{relative_path:"events.jsonl.gz",rows:1,bytes:2,sha256:$sha}] | sort_by(.relative_path)' |
  sha256sum | cut -d' ' -f1)"
SOURCE_PROJECTED_FILESET_SHA="sha256:$(printf 'd%.0s' $(seq 1 64))"
export RAW_SHA FILE_SHA

cat >"$TMP/bin/polyedge-rs" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$POLYEDGE_TEST_ARGS"
command="${2:-}"
out=""
markdown=""
manifest=""
previous=""
for argument in "$@"; do
  case "$previous" in
    --out) out="$argument" ;;
    --markdown) markdown="$argument" ;;
    --manifest) manifest="$argument" ;;
  esac
  previous="$argument"
done
inventory="$(jq -cn \
  --arg sha "$RAW_SHA" \
  '{
    schema_version:1,
    canonical_sha256:$sha,
    canonical:{
      domain:"polyedge.raw_source_inventory.v1",
      schema_version:1,
      source_kind:"azure_blob",
      account:"stpolyedge6urdjr5nmwx7w",
      container:"polyedge-shadow-events",
      prefix:"shadow-events/campaign-2026-07-23/2026/07/23/",
      max_blobs:null,
      max_bytes:null,
      ordering:"blob_name_ascii_ascending",
      exhaustive_listing:true,
      blob_count:1,
      total_bytes:2,
      blobs:[{
        ordinal:0,
        name:"shadow-events/campaign-2026-07-23/2026/07/23/events.jsonl",
        etag:"etag",
        version_id:null,
        content_md5:null,
        blob_type:"AppendBlob",
        sealed:true,
        content_length:2,
        last_modified:"2026-07-24T00:00:00Z",
        sha256:$sha
      }]
    }
  }')"
case "$command" in
  begin-shadow-correction|complete-shadow-correction)
    mkdir -p "$(dirname "$out")"
    printf '%s\n' '{}' >"$out"
    ;;
  audit)
    mkdir -p "$(dirname "$out")"
    jq -n --argjson inventory "$inventory" \
      '{result:{raw_source_inventory:$inventory,warnings:[],fatal_issues:[]}}' >"$out"
    printf '%s\n' '# audit' >"$markdown"
    ;;
  normalize)
    mkdir -p "$out"
    jq -n --argjson inventory "$inventory" \
      '{raw_source_inventory:$inventory,events:1,input_events:1,event_counts:{reference:1}}' \
      >"$out/events_manifest.json"
    ;;
  publish-projected-day)
    mkdir -p "$(dirname "$out")"
    jq -n --argjson inventory "$inventory" --arg file_sha "$FILE_SHA" \
      '{
        canonical:{
          raw_source_inventory:$inventory,
          files:[{relative_path:"events.jsonl.gz",rows:1,bytes:2,sha256:$file_sha}]
        }
      }' >"$out"
    ;;
  materialize-projected-campaign)
    mkdir -p "$out"
    jq -n --arg sha "$RAW_SHA" \
      '{segments:[{raw_source_inventory_sha256:$sha}]}' >"$manifest"
    cp "$manifest" "$out/campaign_index.json"
    ;;
  loss-diagnostics)
    mkdir -p "$out"
    : >"$out/order_lifecycle_fact.jsonl"
    : >"$out/fill_markout_fact.jsonl"
    printf '%s\n' '# loss diagnostics' >"$out/loss_diagnostics.md"
    jq -n '{
      config:{},
      fill_model:"immutable_protocol_v3_snapshot",
      split_method:"diagnostic_only",
      warnings:[],
      data_window:{},
      result:{
        status:"complete_diagnostic",
        counts:{duplicate_event_lines:0},
        completion_checks:{no_exact_duplicate_event_lines:true},
        snapshot_identity:{stable_before_after_read:true},
        queue_position_field:"inferred_size_ahead",
        literal_fifo_rank_available:false
      }
    }' >"$out/loss_diagnostics.json"
    jq -n \
      --arg order_sha "sha256:$(sha256sum "$out/order_lifecycle_fact.jsonl" | cut -d' ' -f1)" \
      --arg fill_sha "sha256:$(sha256sum "$out/fill_markout_fact.jsonl" | cut -d' ' -f1)" \
      --arg json_sha "sha256:$(sha256sum "$out/loss_diagnostics.json" | cut -d' ' -f1)" \
      --arg md_sha "sha256:$(sha256sum "$out/loss_diagnostics.md" | cut -d' ' -f1)" \
      '{artifacts:[
        {filename:"order_lifecycle_fact.jsonl",sha256:$order_sha},
        {filename:"fill_markout_fact.jsonl",sha256:$fill_sha},
        {filename:"loss_diagnostics.json",sha256:$json_sha},
        {filename:"loss_diagnostics.md",sha256:$md_sha}
      ]}' >"$out/loss_diagnostics_artifact_manifest.json"
    ;;
  execution-quality|build-markets|baseline|regimes|calibration|report)
    mkdir -p "$(dirname "$out")"
    printf '%s\n' '{}' >"$out"
    if [ -n "$markdown" ]; then
      printf '%s\n' '# fixture' >"$markdown"
    fi
    ;;
  publish-daily-bundle) ;;
  *)
    echo "unexpected mock command: $command" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$TMP/bin/polyedge-rs"

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-07-23-lossdiag-v3/control/validation.lock \
  EXECUTION_MODE=paper \
  ALLOW_LIVE=false \
  RUN_BOT_ON_STARTUP=false \
  ENABLE_TAKER_ORDERS=false \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge6urdjr5nmwx7w \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-validation \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  EXPECTED_GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  EXPECTED_RAW_SOURCE_INVENTORY_SHA256="$RAW_SHA" \
  SOURCE_PROJECTED_FILESET_SHA256="$SOURCE_PROJECTED_FILESET_SHA" \
  LOSSDIAG_VALIDATION_CONFIG="$REPO/research/configs/shadow_lossdiag_validation_2026-07-23_v3.json" \
  sh "$REPO/research/run_shadow_lossdiag_validation.sh" >"$TMP/stdout"
)

grep -F 'research audit --input azure://stpolyedge6urdjr5nmwx7w/polyedge-shadow-events/shadow-events/campaign-2026-07-23/2026/07/23/?prefetch_blobs=16' "$TMP/args" >/dev/null
grep -F -- '--cache-root azure://stpolyedge6urdjr5nmwx7w/polyedge-research-validation/data/research/shadow/campaign-2026-07-23-lossdiag-v3/projected-cache' "$TMP/args" >/dev/null
grep -F 'research loss-diagnostics --input data/research/shadow/campaign-2026-07-23-lossdiag-v3/cumulative/2026-07-23/normalized' "$TMP/args" >/dev/null
grep -F 'research publish-daily-bundle ' "$TMP/args" |
  grep -F -- '--output-root reports/research/shadow/validations/campaign-2026-07-23-lossdiag-v3/daily' >/dev/null
if grep -E '^research (validate-prospective|evaluate-profitability|loss-regime-oos|venue-model|funded|canary|probe|redeem|promotion)' "$TMP/args" >/dev/null; then
  echo "isolated loss-diagnostics validation invoked a forbidden command" >&2
  exit 1
fi
grep -F 'polyedge_lossdiag_validation status=succeeded validation_id=campaign-2026-07-23-lossdiag-v3' "$TMP/stdout" >/dev/null
METRICS="$(find "$TMP/work/reports/research/shadow/validations/campaign-2026-07-23-lossdiag-v3" \
  -type f -name loss_diagnostics_metrics.json -print -quit)"
test -n "$METRICS"
jq -e \
  --arg source "$SOURCE_PROJECTED_FILESET_SHA" \
  --arg projected "$PROJECTED_FILESET_SHA" '
  .source_projected_fileset_sha256 == $source
  and .projected_fileset_sha256 == $projected
  and .projected_fileset_sha256 != .source_projected_fileset_sha256
  and .normalized_timestamp_precision == "rfc3339_autosi"
' "$METRICS" >/dev/null

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-07-23/control/replay.lock \
  EXECUTION_MODE=paper \
  ALLOW_LIVE=false \
  RUN_BOT_ON_STARTUP=false \
  ENABLE_TAKER_ORDERS=false \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge6urdjr5nmwx7w \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-validation \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  EXPECTED_GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  EXPECTED_RAW_SOURCE_INVENTORY_SHA256="$RAW_SHA" \
  SOURCE_PROJECTED_FILESET_SHA256="$SOURCE_PROJECTED_FILESET_SHA" \
  LOSSDIAG_VALIDATION_CONFIG="$REPO/research/configs/shadow_lossdiag_validation_2026-07-23_v3.json" \
  sh "$REPO/research/run_shadow_lossdiag_validation.sh" >/dev/null 2>&1
); then
  echo "isolated validation accepted the canonical campaign lease" >&2
  exit 1
fi
