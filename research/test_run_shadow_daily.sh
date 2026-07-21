#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/work"

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
  fi
  previous="$argument"
done
if [ -n "$out" ]; then
  if [ "$command" = "normalize" ]; then
    mkdir -p "$out"
    printf '%s\n' '{"events":1}' >"$out/events_manifest.json"
  elif [ "$command" = "materialize" ]; then
    mkdir -p "$out"
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
  POLYEDGE_CAMPAIGN_LEASE_BLOB=test/replay.lock \
  POLYEDGE_UTC_TODAY=2026-07-23 \
  SHADOW_REPORT_DATE=2026-07-22 \
  SHADOW_CASCADE_THROUGH=2026-07-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
  sh "$REPO/research/run_shadow_daily.sh" >"$TMP/stdout"
)

test "$(grep -c '^research normalize ' "$TMP/args")" -eq 1
test "$(grep -c '^research begin-shadow-correction ' "$TMP/args")" -eq 1
test "$(grep -c '^research complete-shadow-correction ' "$TMP/args")" -eq 1
grep -F 'research begin-shadow-correction --campaign-id campaign-2026-07-22 --correction-id shadow-2026-07-22-through-2026-07-22 --from 2026-07-22 --through 2026-07-22 ' "$TMP/args" >/dev/null
grep -F -- '--out reports/research/shadow/campaigns/campaign-2026-07-22/corrections/active.json' "$TMP/args" >/dev/null
grep -F 'research normalize --input azure://stpolyedge/polyedge-shadow-events/shadow-events/campaign-2026-07-22/2026/07/22/' "$TMP/args" >/dev/null
if grep -E 'research normalize --input .*campaign-2026-07-22/\?prefetch' "$TMP/args" >/dev/null; then
  echo "campaign-wide raw normalization was invoked" >&2
  exit 1
fi
grep -F 'research publish-projected-day ' "$TMP/args" >/dev/null
grep -F -- '--require-azure-source true --expected-source-container polyedge-shadow-events' "$TMP/args" >/dev/null
grep -F 'research materialize-projected-campaign --since 2026-07-22 --through 2026-07-22 ' "$TMP/args" >/dev/null
grep -F 'research build-cumulative-wallet ' "$TMP/args" | grep -F -- '--campaign-contract research/configs/profitability_gate_v3_2026-07-22.yaml' >/dev/null
grep -F 'research publish-daily-bundle ' "$TMP/args" | grep -F -- '--output-root reports/research/shadow/campaigns/campaign-2026-07-22/daily' >/dev/null
grep -F 'research validate-prospective ' "$TMP/args" | grep -F -- '--reports-dir reports/research/shadow/campaigns/campaign-2026-07-22/daily' >/dev/null
grep -F 'research evaluate-profitability ' "$TMP/args" | grep -F -- '--out reports/research/shadow/campaigns/campaign-2026-07-22/profitability/latest.json' >/dev/null
grep -F 'stage=normalize-day date=2026-07-22 status=starting' "$TMP/stdout" >/dev/null
if grep -F '{' "$TMP/stdout" >/dev/null; then
  echo "shadow daily emitted verbose JSON instead of stage markers" >&2
  exit 1
fi

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-no-lease" \
  POLYEDGE_UTC_TODAY=2026-07-23 \
  SHADOW_REPORT_DATE=2026-07-22 \
  SHADOW_CASCADE_THROUGH=2026-07-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "shadow daily accepted a direct writer without the Azure campaign lease" >&2
  exit 1
fi

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-cascade" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=test/replay.lock \
  POLYEDGE_UTC_TODAY=2026-07-24 \
  SHADOW_REPORT_DATE=2026-07-22 \
  SHADOW_CASCADE_THROUGH=2026-07-23 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
  sh "$REPO/research/run_shadow_daily.sh" >"$TMP/stdout-cascade"
)
test "$(grep -c '^research normalize ' "$TMP/args-cascade")" -eq 2
test "$(grep -c '^research begin-shadow-correction ' "$TMP/args-cascade")" -eq 1
test "$(grep -c '^research complete-shadow-correction ' "$TMP/args-cascade")" -eq 1
test "$(grep -c '^research validate-prospective ' "$TMP/args-cascade")" -eq 1
test "$(grep -c '^research evaluate-profitability ' "$TMP/args-cascade")" -eq 1
grep -F 'cascade date=2026-07-22 through=2026-07-23 status=starting' "$TMP/stdout-cascade" >/dev/null
grep -F 'cascade date=2026-07-23 through=2026-07-23 status=completed' "$TMP/stdout-cascade" >/dev/null

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-prestart" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=test/replay.lock \
  POLYEDGE_UTC_TODAY=2026-07-22 \
  SHADOW_REPORT_DATE=2026-07-21 \
  SHADOW_CASCADE_THROUGH=2026-07-21 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
  sh "$REPO/research/run_shadow_daily.sh" >"$TMP/stdout-prestart"
)
test ! -s "$TMP/args-prestart"
grep -F 'status=not_started first_eligible_date=2026-07-22 requested_through=2026-07-21' "$TMP/stdout-prestart" >/dev/null

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true \
  POLYEDGE_CAMPAIGN_LEASE_ID=test-lease \
  POLYEDGE_CAMPAIGN_LEASE_BLOB=test/replay.lock \
  POLYEDGE_UTC_TODAY=2026-07-23 \
  SHADOW_REPORT_DATE=2026-07-22 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
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
  POLYEDGE_CAMPAIGN_LEASE_BLOB=test/replay.lock \
  SHADOW_REPORT_DATE="$(date -u +%Y-%m-%d)" \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "shadow daily accepted an unsealed current UTC day" >&2
  exit 1
fi
