#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/work"
export SHADOW_CODE_FREEZE_SHA256=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export SHADOW_CODE_FREEZE_MANIFEST=azure://stpolyedge/polyedge-qset-control/reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json
export SHADOW_CAMPAIGN_ID=campaign-2026-08-22-qset-v2
export SHADOW_CAMPAIGN_START=2026-08-22
export SHADOW_CAMPAIGN_PREFIX=shadow-events/campaign-2026-08-22-qset-v2
export SHADOW_CAMPAIGN_REPORT_ROOT=reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2
export SHADOW_CORRECTION_ROOT=reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/corrections
export SHADOW_CAMPAIGN_CONTRACT=research/configs/profitability_gate_v3_2026-08-22_qset_v2.yaml
export SHADOW_EVIDENCE_VERSION=protocol-v3-qset-v2

cat >"$TMP/bin/polyedge-rs" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$POLYEDGE_TEST_ARGS"
out=""
command=""
previous=""
for argument in "$@"; do
  if [ "$previous" = "--out" ]; then
    out="$argument"
  fi
  if [ "$argument" = "normalize" ]; then
    command="normalize"
  elif [ "$argument" = "materialize-projected-campaign" ]; then
    command="materialize"
  elif [ "$argument" = "loss-diagnostics" ]; then
    command="loss-diagnostics"
  fi
  previous="$argument"
done
if [ -n "$out" ]; then
  if [ "$command" = "normalize" ]; then
    mkdir -p "$out"
    printf '%s\n' '{"events":1}' >"$out/events_manifest.json"
  elif [ "$command" = "materialize" ]; then
    mkdir -p "$out"
  elif [ "$command" = "loss-diagnostics" ]; then
    mkdir -p "$out"
    if [ "${POLYEDGE_TEST_LOSS_MALFORMED:-false}" = "true" ]; then
      printf '%s\n' '{malformed' >"$out/loss_diagnostics.json"
    elif [ "${POLYEDGE_TEST_LOSS_INELIGIBLE:-false}" = "true" ]; then
      printf '%s\n' '{"result":{"status":"complete_diagnostic","counts":{"duplicate_event_lines":3},"completion_checks":{"no_exact_duplicate_event_lines":false}}}' >"$out/loss_diagnostics.json"
    else
      printf '%s\n' '{"result":{"status":"complete_diagnostic","counts":{"duplicate_event_lines":0},"completion_checks":{"no_exact_duplicate_event_lines":true}}}' >"$out/loss_diagnostics.json"
    fi
    printf '%s\n' '{}' >"$out/loss_diagnostics_artifact_manifest.json"
  else
    mkdir -p "$(dirname "$out")"
    printf '%s\n' '{}' >"$out"
  fi
fi
exit 0
EOF
chmod +x "$TMP/bin/polyedge-rs"

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-08-22-qset-v2/control/replay.lock \
  POLYEDGE_UTC_TODAY=2026-08-24 \
  SHADOW_REPORT_DATE=2026-08-22 \
  SHADOW_CASCADE_THROUGH=2026-08-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  sh "$REPO/research/run_shadow_daily.sh" >"$TMP/stdout"
)

test "$(grep -c '^research normalize ' "$TMP/args")" -eq 2
test "$(grep -c '^research begin-shadow-correction ' "$TMP/args")" -eq 1
test "$(grep -c '^research complete-shadow-correction ' "$TMP/args")" -eq 1
grep -F 'research begin-shadow-correction --campaign-id campaign-2026-08-22-qset-v2 --correction-id shadow-2026-08-22-through-2026-08-22 --from 2026-08-22 --through 2026-08-22 ' "$TMP/args" >/dev/null
grep -F -- '--out reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/corrections/active.json' "$TMP/args" >/dev/null
grep -F 'research normalize --input azure://stpolyedge/polyedge-shadow-qset-events/shadow-events/campaign-2026-08-22-qset-v2/2026/08/22/' "$TMP/args" >/dev/null
grep -F 'research normalize --input azure://stpolyedge/polyedge-shadow-qset-events/shadow-events/campaign-2026-08-22-qset-v2/2026/08/23/' "$TMP/args" >/dev/null
if grep -E 'research normalize --input .*campaign-2026-08-22-qset-v2/\?prefetch' "$TMP/args" >/dev/null; then
  echo "campaign-wide raw normalization was invoked" >&2
  exit 1
fi
grep -F 'research publish-projected-day ' "$TMP/args" >/dev/null
grep -F -- '--require-azure-source true --expected-source-container polyedge-shadow-qset-events' "$TMP/args" >/dev/null
grep -F 'research publish-projected-day --normalized data/research/shadow/campaign-2026-08-22-qset-v2/2026-08-23/settlement-carry-normalized --date 2026-08-23 --campaign-id campaign-2026-08-22-qset-v2 --cache-root reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/staging/' "$TMP/args" | grep -F '/settlement-carry-verified-cache ' >/dev/null
grep -F 'research materialize-projected-campaign --since 2026-08-22 --through 2026-08-22 ' "$TMP/args" >/dev/null
grep -F 'research loss-diagnostics --input data/research/shadow/campaign-2026-08-22-qset-v2/cumulative/2026-08-22/normalized --settlement-carry-input data/research/shadow/campaign-2026-08-22-qset-v2/2026-08-23/settlement-carry-normalized --settlement-carry-manifest reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/staging/' "$TMP/args" | grep -F -- '--settlement-carry-campaign-id campaign-2026-08-22-qset-v2 --settlement-carry-source-account stpolyedge --settlement-carry-source-container polyedge-shadow-qset-events --market-day 2026-08-22 --out reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/staging/' >/dev/null
test "$(grep -c -- '--settlement-carry-campaign-id campaign-2026-08-22-qset-v2 --settlement-carry-source-account stpolyedge --settlement-carry-source-container polyedge-shadow-qset-events --market-day 2026-08-22' "$TMP/args")" -eq 4
grep -F '.result.status == "complete_diagnostic"' research/run_shadow_daily.sh >/dev/null
grep -F '.result.counts.duplicate_event_lines == 0' research/run_shadow_daily.sh >/dev/null
grep -F '.result.completion_checks.no_exact_duplicate_event_lines == true' research/run_shadow_daily.sh >/dev/null
grep -F 'research build-cumulative-wallet ' "$TMP/args" | grep -F -- '--campaign-contract research/configs/profitability_gate_v3_2026-08-22_qset_v2.yaml' >/dev/null
grep -F 'research publish-daily-bundle ' "$TMP/args" | grep -F -- '--output-root reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/daily' >/dev/null
grep -F 'research validate-prospective ' "$TMP/args" | grep -F -- '--reports-dir reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/daily' >/dev/null
grep -F 'research evaluate-profitability ' "$TMP/args" | grep -F -- '--out reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/profitability/latest.json' >/dev/null
test "$(jq -r '.manifest_sha256' "$TMP/work/reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/staging/"*/code_freeze_binding.json)" = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
test "$(jq -r '.manifest_path' "$TMP/work/reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/staging/"*/code_freeze_binding.json)" = "azure://stpolyedge/polyedge-qset-control/reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
grep -F 'stage=normalize-day date=2026-08-22 status=starting' "$TMP/stdout" >/dev/null
if grep -F '{' "$TMP/stdout" >/dev/null; then
  echo "shadow daily emitted verbose JSON instead of stage markers" >&2
  exit 1
fi

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-legacy" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=legacy-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-07-23/control/replay.lock \
  POLYEDGE_UTC_TODAY=2026-07-25 \
  SHADOW_REPORT_DATE=2026-07-23 \
  SHADOW_CASCADE_THROUGH=2026-07-23 \
  SHADOW_CAMPAIGN_ID=campaign-2026-07-23 \
  SHADOW_CAMPAIGN_START=2026-07-23 \
  SHADOW_CAMPAIGN_PREFIX=shadow-events/campaign-2026-07-23 \
  SHADOW_CAMPAIGN_REPORT_ROOT=reports/research/shadow/campaigns/campaign-2026-07-23 \
  SHADOW_CORRECTION_ROOT=reports/research/shadow/campaigns/campaign-2026-07-23/corrections \
  SHADOW_CAMPAIGN_CONTRACT=research/configs/profitability_gate_v3_2026-07-23.yaml \
  SHADOW_EVIDENCE_VERSION=protocol-v3 \
  SHADOW_CODE_FREEZE_SHA256= \
  SHADOW_CODE_FREEZE_MANIFEST= \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null
)
grep -F -- '--campaign-id campaign-2026-07-23' "$TMP/args-legacy" >/dev/null

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-no-lease" \
  POLYEDGE_UTC_TODAY=2026-08-24 \
  SHADOW_REPORT_DATE=2026-08-22 \
  SHADOW_CASCADE_THROUGH=2026-08-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "shadow daily accepted a direct writer without the Azure campaign lease" >&2
  exit 1
fi

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-no-freeze" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-08-22-qset-v2/control/replay.lock \
  POLYEDGE_UTC_TODAY=2026-08-24 \
  SHADOW_REPORT_DATE=2026-08-22 \
  SHADOW_CASCADE_THROUGH=2026-08-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  SHADOW_CODE_FREEZE_SHA256= \
  SHADOW_CODE_FREEZE_MANIFEST= \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "qset-v2 shadow daily accepted an unbound code freeze" >&2
  exit 1
fi

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-cascade" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-08-22-qset-v2/control/replay.lock \
  POLYEDGE_UTC_TODAY=2026-08-25 \
  SHADOW_REPORT_DATE=2026-08-22 \
  SHADOW_CASCADE_THROUGH=2026-08-23 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  sh "$REPO/research/run_shadow_daily.sh" >"$TMP/stdout-cascade"
)
test "$(grep -c '^research normalize ' "$TMP/args-cascade")" -eq 4
test "$(grep -c '^research loss-diagnostics ' "$TMP/args-cascade")" -eq 2
test "$(grep -c '^research begin-shadow-correction ' "$TMP/args-cascade")" -eq 1
test "$(grep -c '^research complete-shadow-correction ' "$TMP/args-cascade")" -eq 1
test "$(grep -c '^research validate-prospective ' "$TMP/args-cascade")" -eq 1
test "$(grep -c '^research evaluate-profitability ' "$TMP/args-cascade")" -eq 1
grep -F 'cascade date=2026-08-22 through=2026-08-23 status=starting' "$TMP/stdout-cascade" >/dev/null
grep -F 'cascade date=2026-08-23 through=2026-08-23 status=completed' "$TMP/stdout-cascade" >/dev/null

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-prestart" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-08-22-qset-v2/control/replay.lock \
  POLYEDGE_UTC_TODAY=2026-08-23 \
  SHADOW_REPORT_DATE=2026-08-21 \
  SHADOW_CASCADE_THROUGH=2026-08-21 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  sh "$REPO/research/run_shadow_daily.sh" >"$TMP/stdout-prestart"
)
test ! -s "$TMP/args-prestart"
grep -F 'status=not_started first_eligible_date=2026-08-22 requested_through=2026-08-21' "$TMP/stdout-prestart" >/dev/null

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-08-22-qset-v2/control/replay.lock \
  POLYEDGE_UTC_TODAY=2026-08-24 \
  SHADOW_REPORT_DATE=2026-08-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "shadow daily accepted the test-only clock override without the test harness" >&2
  exit 1
fi

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-current" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-08-22-qset-v2/control/replay.lock \
  SHADOW_REPORT_DATE="$(date -u +%Y-%m-%d)" \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "shadow daily accepted an unsealed current UTC day" >&2
  exit 1
fi

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-ineligible" \
  POLYEDGE_TEST_LOSS_INELIGIBLE=true \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-08-22-qset-v2/control/replay.lock \
  POLYEDGE_UTC_TODAY=2026-08-24 \
  SHADOW_REPORT_DATE=2026-08-22 \
  SHADOW_CASCADE_THROUGH=2026-08-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "shadow daily published diagnostic-ineligible evidence" >&2
  exit 1
fi
if grep -F '^research publish-daily-bundle ' "$TMP/args-ineligible" >/dev/null; then
  echo "shadow daily reached publication after diagnostic-ineligible evidence" >&2
  exit 1
fi

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-malformed" \
  POLYEDGE_TEST_LOSS_MALFORMED=true \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-08-22-qset-v2/control/replay.lock \
  POLYEDGE_UTC_TODAY=2026-08-24 \
  SHADOW_REPORT_DATE=2026-08-22 \
  SHADOW_CASCADE_THROUGH=2026-08-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "shadow daily published malformed loss diagnostics" >&2
  exit 1
fi
if grep -F '^research publish-daily-bundle ' "$TMP/args-malformed" >/dev/null; then
  echo "shadow daily reached publication after malformed loss diagnostics" >&2
  exit 1
fi
