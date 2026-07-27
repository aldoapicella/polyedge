#!/bin/sh
set -eu

DATE="${1:-}"
case "$DATE" in
  ????-??-??) ;;
  *)
    echo "usage: verify_shadow_daily_bundle.sh YYYY-MM-DD" >&2
    exit 2
    ;;
esac
test "$(date -u -d "$DATE" +%Y-%m-%d 2>/dev/null || true)" = "$DATE"

: "${STORAGE_ACCOUNT:?STORAGE_ACCOUNT is required}"
: "${QSET_RESEARCH_CONTAINER:?QSET_RESEARCH_CONTAINER is required}"
: "${QSET_CONTROL_CONTAINER:?QSET_CONTROL_CONTAINER is required}"
: "${CAMPAIGN_ID:?CAMPAIGN_ID is required}"
: "${SOURCE_FREEZE_SHA256:?SOURCE_FREEZE_SHA256 is required}"
: "${SOURCE_FREEZE_PATH:?SOURCE_FREEZE_PATH is required}"
: "${SOURCE_FREEZE_FILE:=source-freeze.json}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
DAILY_ROOT="reports/research/shadow/campaigns/$CAMPAIGN_ID/daily"
POINTER_PATH="$DAILY_ROOT/$DATE/latest.json"

az storage blob download \
  --account-name "$STORAGE_ACCOUNT" \
  --container-name "$QSET_RESEARCH_CONTAINER" \
  --name "$POINTER_PATH" \
  --file "$WORK/pointer.json" \
  --auth-mode login \
  --overwrite \
  --only-show-errors \
  -o none

test "$(jq -r '.schema_version' "$WORK/pointer.json")" = "1"
test "$(jq -r '.date' "$WORK/pointer.json")" = "$DATE"
RUN_ID="$(jq -r '.run_id' "$WORK/pointer.json")"
MANIFEST_RELATIVE="$(jq -r '.manifest_path' "$WORK/pointer.json")"
test -n "$RUN_ID"
test "$MANIFEST_RELATIVE" = "runs/$RUN_ID/run_manifest.json"
MANIFEST_PATH="$DAILY_ROOT/$DATE/$MANIFEST_RELATIVE"

az storage blob download \
  --account-name "$STORAGE_ACCOUNT" \
  --container-name "$QSET_RESEARCH_CONTAINER" \
  --name "$MANIFEST_PATH" \
  --file "$WORK/manifest.json" \
  --auth-mode login \
  --overwrite \
  --only-show-errors \
  -o none

test "$(sha256sum "$WORK/manifest.json" | cut -d' ' -f1)" = "$(jq -r '.manifest_sha256' "$WORK/pointer.json")"
SOURCE_COMMIT="$(jq -r '.source_commit' "$SOURCE_FREEZE_FILE")"
jq -e \
  --arg date "$DATE" \
  --arg run_id "$RUN_ID" \
  --arg source_commit "$SOURCE_COMMIT" \
  '
    def number:
      if type == "string" then tonumber else . end;
    def covered($value):
      $value != null and (($value | number) >= 0.95);
    def complete_or_not_applicable($rate; $applicable):
      if $applicable == false then $rate == null else covered($rate) end;
    .schema_version == 2
    and .date == $date
    and .run_id == $run_id
    and .status == "COMPLETE"
    and .runtime_role == "profitability_shadow"
    and .git_sha == $source_commit
    and (.artifacts | type) == "object"
    and (.artifacts | length) > 0
    and .data_quality.registry_version == "research-data-quality-v5"
    and .data_quality.total_events > 0
    and covered(.data_quality.decision_grade_coverage)
    and covered(.data_quality.coverage_breakdown.start_price_capture_rate)
    and covered(.data_quality.coverage_breakdown.settlement_rate)
    and covered(.data_quality.coverage_breakdown.exact_reference_hour_coverage)
    and covered(.data_quality.coverage_breakdown.decision_metadata_coverage)
    and covered(.data_quality.coverage_breakdown.decision_grade_coverage)
    and covered(.data_quality.coverage_breakdown.final_decision_grade_coverage)
    and covered(.data_quality.coverage_breakdown.execution_field_coverage)
    and ((.data_quality.coverage_breakdown.decision_parity_rate | number) == 1)
    and complete_or_not_applicable(
      .data_quality.coverage_breakdown.queue_position_coverage;
      .data_quality.coverage_breakdown.queue_position_applicable
    )
    and complete_or_not_applicable(
      .data_quality.coverage_breakdown.markout_1s_completion;
      .data_quality.coverage_breakdown.markout_1s_applicable
    )
    and complete_or_not_applicable(
      .data_quality.coverage_breakdown.markout_5s_completion;
      .data_quality.coverage_breakdown.markout_5s_applicable
    )
    and complete_or_not_applicable(
      .data_quality.coverage_breakdown.markout_30s_completion;
      .data_quality.coverage_breakdown.markout_30s_applicable
    )
    and (.data_quality.fatal_issues | length) == 0
    and .data_quality.event_time_ordering_restored == true
    and ((.data_quality.out_of_order_events | number) / (.data_quality.total_events | number)) <= 0.0001
    and all(.data_quality.warnings[]?; .severity == "informational")
  ' "$WORK/manifest.json" >/dev/null

jq -e '
  (.artifacts | map(.relative_path)) as $paths
  | ($paths | length) == ($paths | unique | length)
    and all(
      .artifacts[];
      (.relative_path | type) == "string"
      and (.relative_path | test("^[A-Za-z0-9._/-]+$"))
      and (.relative_path | startswith("/") | not)
      and (.relative_path | split("/") | all(.[]; . != "" and . != "." and . != ".."))
      and (.sha256 | type) == "string"
      and (.sha256 | test("^[0-9a-f]{64}$"))
      and (.bytes | type) == "number"
      and (.bytes | floor) == .bytes
      and .bytes >= 0
    )
    and all(
      [
        "data_audit.json",
        "baseline.json",
        "regimes.json",
        "final_report.json",
        "execution_quality.json",
        "code_freeze_binding.json"
      ][];
      . as $required | $paths | index($required) != null
    )
' "$WORK/manifest.json" >/dev/null

ARTIFACT_INDEX=0
jq -r '.artifacts[] | [.relative_path, .sha256, (.bytes | tostring)] | @tsv' \
  "$WORK/manifest.json" |
while IFS="$(printf '\t')" read -r RELATIVE EXPECTED_SHA EXPECTED_BYTES; do
  ARTIFACT_INDEX=$((ARTIFACT_INDEX + 1))
  LOCAL_ARTIFACT="$WORK/artifact-$ARTIFACT_INDEX"
  az storage blob download \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$QSET_RESEARCH_CONTAINER" \
    --name "$(dirname "$MANIFEST_PATH")/$RELATIVE" \
    --file "$LOCAL_ARTIFACT" \
    --auth-mode login \
    --overwrite \
    --only-show-errors \
    -o none
  test "$(sha256sum "$LOCAL_ARTIFACT" | cut -d' ' -f1)" = "$EXPECTED_SHA"
  test "$(wc -c <"$LOCAL_ARTIFACT" | tr -d ' ')" = "$EXPECTED_BYTES"
  if [ "$RELATIVE" = "code_freeze_binding.json" ]; then
    cp "$LOCAL_ARTIFACT" "$WORK/code-freeze-binding.json"
  fi
done

test -f "$WORK/code-freeze-binding.json"
jq -e \
  --arg campaign_id "$CAMPAIGN_ID" \
  --arg sha "$SOURCE_FREEZE_SHA256" \
  --arg uri "azure://$STORAGE_ACCOUNT/$QSET_CONTROL_CONTAINER/$SOURCE_FREEZE_PATH" \
  '
    .schema == "polyedge.shadow_code_freeze_binding.v1"
    and .campaign_id == $campaign_id
    and .evidence_version == "protocol-v3-qset-v1"
    and .manifest_sha256 == $sha
    and .manifest_path == $uri
  ' "$WORK/code-freeze-binding.json" >/dev/null
