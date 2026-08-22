#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
DATE=2026-08-24
CAMPAIGN_ID=campaign-2026-08-24-qset-v4
DAILY_ROOT="reports/research/shadow/campaigns/$CAMPAIGN_ID/daily"
RUN_ID=test-run
RUN_ROOT="$DAILY_ROOT/$DATE/runs/$RUN_ID"
mkdir -p "$TMP/bin" "$TMP/blobs/$RUN_ROOT"

cat >"$TMP/bin/az" <<'EOF'
#!/bin/sh
set -eu
name= file=
while [ "$#" -gt 0 ]; do
  case "$1" in --name) name="$2"; shift 2 ;; --file) file="$2"; shift 2 ;; *) shift ;; esac
done
cp "$POLYEDGE_TEST_BLOB_ROOT/$name" "$file"
EOF
chmod +x "$TMP/bin/az"

cat >"$TMP/source-freeze.json" <<'EOF'
{"schema":"polyedge.shadow_source_freeze.v1","campaign_id":"campaign-2026-08-24-qset-v4","evidence_version":"protocol-v3-qset-v4","source_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","research_image":"ghcr.io/polyedge/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","critical_files":["research/run_shadow_daily_v4.sh"]}
EOF
FREEZE_DIGEST="$(sha256sum "$TMP/source-freeze.json" | cut -d" " -f1)"
SOURCE_FREEZE_FILE="$TMP/source-$FREEZE_DIGEST.json"
mv "$TMP/source-freeze.json" "$SOURCE_FREEZE_FILE"
FREEZE_SHA="sha256:$FREEZE_DIGEST"
FREEZE_PATH="reports/research/shadow/campaigns/campaign-2026-08-24-qset-v4/control/code-freeze/source-$FREEZE_DIGEST.json"
FREEZE_MANIFEST="azure://st/polyedge-qset-v4-control/$FREEZE_PATH"
cat >"$TMP/blobs/$RUN_ROOT/code_freeze_binding.json" <<EOF
{"schema":"polyedge.shadow_code_freeze_binding.v1","campaign_id":"campaign-2026-08-24-qset-v4","evidence_version":"protocol-v3-qset-v4","manifest_path":"azure://st/polyedge-qset-v4-control/reports/research/shadow/campaigns/campaign-2026-08-24-qset-v4/control/code-freeze/source-$FREEZE_DIGEST.json","manifest_sha256":"$FREEZE_SHA"}
EOF
for artifact in data_audit.json baseline.json regimes.json final_report.json execution_quality.json; do printf '{"artifact":"%s"}\n' "$artifact" >"$TMP/blobs/$RUN_ROOT/$artifact"; done

ARTIFACTS='{}'
for artifact in data_audit.json baseline.json regimes.json final_report.json execution_quality.json code_freeze_binding.json; do
  sha="$(sha256sum "$TMP/blobs/$RUN_ROOT/$artifact" | cut -d' ' -f1)"
  bytes="$(wc -c <"$TMP/blobs/$RUN_ROOT/$artifact" | tr -d ' ')"
  ARTIFACTS="$(printf '%s\n' "$ARTIFACTS" | jq -c --arg key "$(printf '%s' "$artifact" | tr '.-' '__')" --arg path "$artifact" --arg sha "$sha" --argjson bytes "$bytes" '. + {($key): {relative_path:$path, sha256:$sha, bytes:$bytes}}')"
done
jq -n --argjson artifacts "$ARTIFACTS" '{schema_version:2,git_sha:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",runtime_role:"profitability_shadow",date:"2026-08-24",run_id:"test-run",status:"COMPLETE",artifacts:$artifacts,data_quality:{registry_version:"research-data-quality-v5",total_events:1000,decision_grade_coverage:"1.0",fatal_issues:[],warnings:[],out_of_order_events:0,event_time_ordering_restored:true,coverage_breakdown:{start_price_capture_rate:"1.0",settlement_rate:"1.0",exact_reference_hour_coverage:"1.0",decision_metadata_coverage:"1.0",decision_grade_coverage:"1.0",final_decision_grade_coverage:"1.0",execution_field_coverage:"1.0",decision_parity_rate:"1.0",queue_position_coverage:null,queue_position_applicable:false,markout_1s_completion:null,markout_1s_applicable:false,markout_5s_completion:null,markout_5s_applicable:false,markout_30s_completion:null,markout_30s_applicable:false}}}' >"$TMP/blobs/$RUN_ROOT/run_manifest.json"
cp "$TMP/blobs/$RUN_ROOT/run_manifest.json" "$TMP/valid-manifest.json"

write_pointer() {
  sha="$(sha256sum "$TMP/blobs/$RUN_ROOT/run_manifest.json" | cut -d" " -f1)"
  mkdir -p "$TMP/blobs/$DAILY_ROOT/$DATE"
  jq -n --arg sha "$sha" '{schema_version:1,date:"2026-08-24",run_id:"test-run",manifest_path:"runs/test-run/run_manifest.json",manifest_sha256:$sha}' >"$TMP/blobs/$DAILY_ROOT/$DATE/latest.json"
}
write_pointer

export PATH="$TMP/bin:$PATH" POLYEDGE_TEST_BLOB_ROOT="$TMP/blobs" STORAGE_ACCOUNT=st AZURE_STORAGE_ACCOUNT_NAME=st
export QSET_RESEARCH_CONTAINER=polyedge-research-qset-v4 QSET_CONTROL_CONTAINER=polyedge-qset-v4-control QSET_V4_CONTROL_CONTAINER_NAME=polyedge-qset-v4-control
export SHADOW_CAMPAIGN_ID=campaign-2026-08-24-qset-v4 SHADOW_EVIDENCE_VERSION=protocol-v3-qset-v4 SHADOW_CODE_FREEZE_FINALIZED=true
export SHADOW_CODE_FREEZE_SHA256="$FREEZE_SHA" EXECUTION_FREEZE_ARTIFACT_PATH="$FREEZE_PATH" SHADOW_CODE_FREEZE_MANIFEST="$FREEZE_MANIFEST"
export SOURCE_FREEZE_FILE

sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" "$DATE" "$DATE"
if sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" 2026-10-23 >/dev/null 2>&1; then echo "v4 verifier accepted terminal-past date" >&2; exit 1; fi
if sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" 2026-08-23 >/dev/null 2>&1; then echo "v4 verifier accepted pre-start date" >&2; exit 1; fi

if SHADOW_CODE_FREEZE_FINALIZED=false sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "v4 verifier accepted a draft freeze" >&2
  exit 1
fi
if QSET_V4_CONTROL_CONTAINER_NAME=polyedge-qset-control sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "v4 verifier accepted the legacy control container" >&2
  exit 1
fi
printf '{"source_commit":"draft"}\n' >"$TMP/source-freeze-draft.json"
if SOURCE_FREEZE_FILE="$TMP/source-freeze-draft.json" sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "v4 verifier accepted a draft source-freeze manifest" >&2
  exit 1
fi
ln -s "$SOURCE_FREEZE_FILE" "$TMP/source-freeze-link.json"
if SOURCE_FREEZE_FILE="$TMP/source-freeze-link.json" sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "v4 verifier accepted a symlinked source-freeze file" >&2
  exit 1
fi
printf "\n" >>"$SOURCE_FREEZE_FILE"
if sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "v4 verifier accepted a source-freeze hash mismatch" >&2
  exit 1
fi
dd if=/dev/zero of="$TMP/source-freeze-oversize.json" bs=1048577 count=1 status=none
if SOURCE_FREEZE_FILE="$TMP/source-freeze-oversize.json" sh "$REPO/research/verify_shadow_daily_v4_bundle.sh" "$DATE" >/dev/null 2>&1; then
  echo "v4 verifier accepted an oversized source-freeze file" >&2
  exit 1
fi
