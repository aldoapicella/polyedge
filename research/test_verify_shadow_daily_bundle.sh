#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
DATE=2026-08-22
CAMPAIGN_ID=campaign-2026-08-22-qset-v2
DAILY_ROOT="reports/research/shadow/campaigns/$CAMPAIGN_ID/daily"
RUN_ID=test-run
RUN_ROOT="$DAILY_ROOT/$DATE/runs/$RUN_ID"
mkdir -p "$TMP/bin" "$TMP/blobs/$RUN_ROOT"

cat >"$TMP/bin/az" <<'EOF'
#!/bin/sh
set -eu
name=
file=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --name)
      name="$2"
      shift 2
      ;;
    --file)
      file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
test -n "$name"
test -n "$file"
cp "$POLYEDGE_TEST_BLOB_ROOT/$name" "$file"
EOF
chmod +x "$TMP/bin/az"

cat >"$TMP/source-freeze.json" <<'EOF'
{"source_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
EOF
cat >"$TMP/blobs/$RUN_ROOT/code_freeze_binding.json" <<'EOF'
{
  "schema": "polyedge.shadow_code_freeze_binding.v1",
  "campaign_id": "campaign-2026-08-22-qset-v2",
  "evidence_version": "protocol-v3-qset-v2",
  "manifest_path": "azure://st/polyedge-qset-control/reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
  "manifest_sha256": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
EOF
for artifact in data_audit.json baseline.json regimes.json final_report.json execution_quality.json; do
  printf '{"artifact":"%s"}\n' "$artifact" >"$TMP/blobs/$RUN_ROOT/$artifact"
done

ARTIFACTS='{}'
for artifact in data_audit.json baseline.json regimes.json final_report.json execution_quality.json code_freeze_binding.json; do
  artifact_sha="$(sha256sum "$TMP/blobs/$RUN_ROOT/$artifact" | cut -d' ' -f1)"
  artifact_bytes="$(wc -c <"$TMP/blobs/$RUN_ROOT/$artifact" | tr -d ' ')"
  artifact_key="$(printf '%s' "$artifact" | tr '.-' '__')"
  ARTIFACTS="$(
    printf '%s\n' "$ARTIFACTS" |
      jq -c \
        --arg key "$artifact_key" \
        --arg path "$artifact" \
        --arg sha "$artifact_sha" \
        --argjson bytes "$artifact_bytes" \
        '. + {($key): {name:$key, relative_path:$path, sha256:$sha, bytes:$bytes}}'
  )"
done

jq -n \
  --argjson artifacts "$ARTIFACTS" \
  '{
    schema_version:2,
    git_sha:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    runtime_role:"profitability_shadow",
    date:"2026-08-22",
    run_id:"test-run",
    status:"COMPLETE",
    artifacts:$artifacts,
    data_quality:{
      registry_version:"research-data-quality-v5",
      total_events:1000,
      decision_grade_coverage:"1.0",
      fatal_issues:[],
      warnings:[],
      out_of_order_events:0,
      event_time_ordering_restored:true,
      coverage_breakdown:{
        start_price_capture_rate:"1.0",
        settlement_rate:"1.0",
        exact_reference_hour_coverage:"1.0",
        decision_metadata_coverage:"1.0",
        decision_grade_coverage:"1.0",
        final_decision_grade_coverage:"1.0",
        execution_field_coverage:"1.0",
        decision_parity_rate:"1.0",
        queue_position_coverage:null,
        queue_position_applicable:false,
        markout_1s_completion:null,
        markout_1s_applicable:false,
        markout_5s_completion:null,
        markout_5s_applicable:false,
        markout_30s_completion:null,
        markout_30s_applicable:false
      }
    }
  }' >"$TMP/blobs/$RUN_ROOT/run_manifest.json"
cp "$TMP/blobs/$RUN_ROOT/run_manifest.json" "$TMP/valid-manifest.json"

write_pointer() {
  manifest_sha="$(sha256sum "$TMP/blobs/$RUN_ROOT/run_manifest.json" | cut -d' ' -f1)"
  mkdir -p "$TMP/blobs/$DAILY_ROOT/$DATE"
  jq -n --arg sha "$manifest_sha" '{
    schema_version:1,
    date:"2026-08-22",
    run_id:"test-run",
    manifest_path:"runs/test-run/run_manifest.json",
    manifest_sha256:$sha
  }' >"$TMP/blobs/$DAILY_ROOT/$DATE/latest.json"
}
write_pointer

export PATH="$TMP/bin:$PATH"
export POLYEDGE_TEST_BLOB_ROOT="$TMP/blobs"
export STORAGE_ACCOUNT=st
export QSET_RESEARCH_CONTAINER=research
export QSET_CONTROL_CONTAINER=polyedge-qset-control
export CAMPAIGN_ID
export EVIDENCE_VERSION=protocol-v3-qset-v2
export SOURCE_FREEZE_SHA256=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export SOURCE_FREEZE_PATH=reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json
export SOURCE_FREEZE_FILE="$TMP/source-freeze.json"

sh "$REPO/research/verify_shadow_daily_bundle.sh" "$DATE"

printf '{"tampered":true}\n' >"$TMP/blobs/$RUN_ROOT/baseline.json"
if sh "$REPO/research/verify_shadow_daily_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "daily verifier accepted a corrupt artifact" >&2
  exit 1
fi
printf '{"artifact":"baseline.json"}\n' >"$TMP/blobs/$RUN_ROOT/baseline.json"

jq 'del(.artifacts.execution_quality_json)' \
  "$TMP/valid-manifest.json" >"$TMP/blobs/$RUN_ROOT/run_manifest.json"
write_pointer
if sh "$REPO/research/verify_shadow_daily_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "daily verifier accepted a missing primary artifact" >&2
  exit 1
fi

jq '.data_quality.warnings=[{severity:"blocking"}]' \
  "$TMP/valid-manifest.json" >"$TMP/blocked.json"
mv "$TMP/blocked.json" "$TMP/blobs/$RUN_ROOT/run_manifest.json"
write_pointer
if sh "$REPO/research/verify_shadow_daily_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "daily verifier accepted a blocking warning" >&2
  exit 1
fi
