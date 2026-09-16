#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
CAMPAIGN_ID="${SHADOW_CAMPAIGN_ID:?SHADOW_CAMPAIGN_ID is required}"

case "$CAMPAIGN_ID" in
  campaign-2026-07-28-qset-v1|campaign-2026-08-22-qset-v2|campaign-2026-08-23-qset-v3)
    echo "$CAMPAIGN_ID is historical and cannot produce new evidence" >&2
    exit 1
    ;;
  campaign-2026-08-26-qset-v5) ;;
  *)
    echo "shadow daily v5 accepts only campaign-2026-08-26-qset-v5" >&2
    exit 1
    ;;
esac

validate_qset_v5_date() {
  value=$1 label=$2
  case "$value" in 2026-08-2[6-9]|2026-08-3[01]|2026-09-0[1-9]|2026-09-[12][0-9]|2026-09-30|2026-10-0[1-9]|2026-10-1[0-9]|2026-10-2[0-4]) ;; *) echo "$label is outside the qset-v5 2026-08-26 through 2026-10-24 boundary" >&2; exit 1 ;; esac
}
qset_v5_report_date=${SHADOW_REPORT_DATE:-$(date -u -d '2 days ago' +%Y-%m-%d)}
qset_v5_cascade_through=${SHADOW_CASCADE_THROUGH:-$(date -u -d '2 days ago' +%Y-%m-%d)}
validate_qset_v5_date "$qset_v5_report_date" SHADOW_REPORT_DATE
validate_qset_v5_date "$qset_v5_cascade_through" SHADOW_CASCADE_THROUGH

test "${SHADOW_CAMPAIGN_START:?SHADOW_CAMPAIGN_START is required}" = "2026-08-26" \
  && test "${SHADOW_CAMPAIGN_PREFIX:-shadow-events/$CAMPAIGN_ID}" = "shadow-events/$CAMPAIGN_ID" \
  && test "${SHADOW_CAMPAIGN_REPORT_ROOT:-reports/research/shadow/campaigns/$CAMPAIGN_ID}" = "reports/research/shadow/campaigns/$CAMPAIGN_ID" \
  && test "${SHADOW_CORRECTION_ROOT:-reports/research/shadow/campaigns/$CAMPAIGN_ID/corrections}" = "reports/research/shadow/campaigns/$CAMPAIGN_ID/corrections" \
  && test "${SHADOW_CAMPAIGN_CONTRACT:?SHADOW_CAMPAIGN_CONTRACT is required}" = "research/configs/profitability_gate_v3_2026-08-26_qset_v5.yaml" \
  && test "${SHADOW_EVIDENCE_VERSION:-}" = "protocol-v3-qset-v5" \
  && test "${SHADOW_SOURCE_CONTAINER_NAME:-}" = "polyedge-shadow-qset-v5-events" \
  && test "${AZURE_STORAGE_CONTAINER_NAME:-}" = "polyedge-research-qset-v5" \
  && test "${QSET_V5_CONTROL_CONTAINER_NAME:?QSET_V5_CONTROL_CONTAINER_NAME is required}" = "polyedge-qset-v5-control" \
  && test "${POLYEDGE_CAMPAIGN_LEASE_BLOB:-}" = "data/research/shadow/$CAMPAIGN_ID/control/replay.lock" || {
    echo "qset-v5 campaign binding is inexact" >&2
    exit 1
  }

printf '%s\n' "${SHADOW_CODE_FREEZE_SHA256:-}" | grep -Eq '^sha256:[0-9a-f]{64}$' || {
  echo "SHADOW_CODE_FREEZE_SHA256 must bind qset-v5 to an immutable source manifest" >&2
  exit 1
}
test "${SHADOW_CODE_FREEZE_FINALIZED:-false}" = "true" || {
  echo "qset-v5 requires a finalized source-freeze binding" >&2
  exit 1
}
case "${SHADOW_CODE_FREEZE_MANIFEST:-}" in
  azure://"${AZURE_STORAGE_ACCOUNT_NAME:?AZURE_STORAGE_ACCOUNT_NAME is required}"/polyedge-qset-v5-control/reports/research/shadow/campaigns/"$CAMPAIGN_ID"/control/code-freeze/source-*.json) ;;
  *) echo "SHADOW_CODE_FREEZE_MANIFEST must stay in the isolated qset-v5 freeze-control path" >&2; exit 1 ;;
esac
FREEZE_DIGEST="${SHADOW_CODE_FREEZE_SHA256#sha256:}"
test "$(basename "$SHADOW_CODE_FREEZE_MANIFEST")" = "source-$FREEZE_DIGEST.json" || { echo "SHADOW_CODE_FREEZE_MANIFEST filename must bind SHADOW_CODE_FREEZE_SHA256" >&2; exit 1; }
case "${SHADOW_PROJECTED_CACHE_ROOT:-}" in
  ""|azure://"$AZURE_STORAGE_ACCOUNT_NAME"/polyedge-research-qset-v5/data/research/shadow/"$CAMPAIGN_ID"/projected-cache) ;;
  *) echo "SHADOW_PROJECTED_CACHE_ROOT must stay in the isolated qset-v5 research path" >&2; exit 1 ;;
esac

exec sh "$REPO/research/run_shadow_daily.sh"
