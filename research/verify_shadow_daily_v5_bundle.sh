#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
test "$#" -ge 1 || {
  echo "usage: verify_shadow_daily_v5_bundle.sh YYYY-MM-DD [YYYY-MM-DD ...]" >&2
  exit 2
}
validate_qset_v5_bundle_date() {
  value=$1
  case "$value" in 2026-08-2[4-9]|2026-08-3[01]|2026-09-0[1-9]|2026-09-[12][0-9]|2026-09-30|2026-10-0[1-9]|2026-10-1[0-9]|2026-10-2[0-2]) ;; *) echo "qset-v5 verifier rejects date outside 2026-08-26 through 2026-10-24: $value" >&2; exit 2 ;; esac
}
for requested_date in "$@"; do validate_qset_v5_bundle_date "$requested_date"; done


test "${SHADOW_CAMPAIGN_ID:?SHADOW_CAMPAIGN_ID is required}" = "campaign-2026-08-26-qset-v5" \
  && test "${SHADOW_EVIDENCE_VERSION:?SHADOW_EVIDENCE_VERSION is required}" = "protocol-v3-qset-v5" \
  && test "${QSET_V5_CONTROL_CONTAINER_NAME:?QSET_V5_CONTROL_CONTAINER_NAME is required}" = "polyedge-qset-v5-control" \
  && test "${SHADOW_CODE_FREEZE_FINALIZED:-false}" = "true" || {
    echo "qset-v5 bundle verifier binding is inexact or draft" >&2
    exit 1
  }
printf '%s\n' "${SHADOW_CODE_FREEZE_SHA256:?SHADOW_CODE_FREEZE_SHA256 is required}" | grep -Eq '^sha256:[0-9a-f]{64}$' || exit 1
FREEZE_DIGEST="${SHADOW_CODE_FREEZE_SHA256#sha256:}"
case "${EXECUTION_FREEZE_ARTIFACT_PATH:?EXECUTION_FREEZE_ARTIFACT_PATH is required}" in
  reports/research/shadow/campaigns/campaign-2026-08-26-qset-v5/control/code-freeze/source-"$FREEZE_DIGEST".json) ;;
  *) echo "qset-v5 source freeze path is outside its control boundary" >&2; exit 1 ;;
esac
case "${SHADOW_CODE_FREEZE_MANIFEST:?SHADOW_CODE_FREEZE_MANIFEST is required}" in
  azure://"${AZURE_STORAGE_ACCOUNT_NAME:?AZURE_STORAGE_ACCOUNT_NAME is required}"/polyedge-qset-v5-control/"$EXECUTION_FREEZE_ARTIFACT_PATH") ;;
  *) echo "qset-v5 source freeze manifest is outside its control boundary" >&2; exit 1 ;;
esac
SOURCE_FREEZE_FILE="${SOURCE_FREEZE_FILE:?SOURCE_FREEZE_FILE is required}"
test -f "$SOURCE_FREEZE_FILE" && test ! -L "$SOURCE_FREEZE_FILE" || exit 1
test "$(wc -c <"$SOURCE_FREEZE_FILE")" -le 1048576 || exit 1
test "sha256:$(sha256sum "$SOURCE_FREEZE_FILE" | cut -d' ' -f1)" = "$SHADOW_CODE_FREEZE_SHA256" || exit 1
CAMPAIGN_ID="$SHADOW_CAMPAIGN_ID"
EVIDENCE_VERSION="$SHADOW_EVIDENCE_VERSION"
SOURCE_FREEZE_SHA256="$SHADOW_CODE_FREEZE_SHA256"
SOURCE_FREEZE_PATH="$EXECUTION_FREEZE_ARTIFACT_PATH"
export CAMPAIGN_ID EVIDENCE_VERSION SOURCE_FREEZE_SHA256 SOURCE_FREEZE_PATH
jq -e \
  --arg campaign_id "$CAMPAIGN_ID" \
  --arg evidence_version "$EVIDENCE_VERSION" \
  '
    .schema == "polyedge.shadow_source_freeze.v1"
    and .campaign_id == $campaign_id
    and .evidence_version == $evidence_version
    and (.source_commit | type == "string" and test("^[0-9a-f]{40}$"))
    and (.research_image | type == "string" and test("^ghcr\\.io/.+@sha256:[0-9a-f]{64}$"))
    and (.critical_files | type == "array" and length > 0)
  ' "$SOURCE_FREEZE_FILE" >/dev/null

for date in "$@"; do
  sh "$REPO/research/verify_shadow_daily_bundle.sh" "$date"
done
