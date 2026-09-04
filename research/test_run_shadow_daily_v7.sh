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
out= command= previous=
for argument in "$@"; do
  test "$previous" != --out || out="$argument"
  case "$argument" in normalize) command=normalize ;; materialize-projected-campaign) command=materialize ;; loss-diagnostics) command=loss ;; esac
  previous="$argument"
done
test -z "$out" && exit 0
case "$command" in
  normalize) mkdir -p "$out"; printf '{"events":1}\n' >"$out/events_manifest.json" ;;
  materialize) mkdir -p "$out" ;;
  loss) mkdir -p "$out"; printf '{"result":{"status":"complete_diagnostic","counts":{"duplicate_event_lines":0},"completion_checks":{"no_exact_duplicate_event_lines":true}}}\n' >"$out/loss_diagnostics.json"; printf '{}\n' >"$out/loss_diagnostics_artifact_manifest.json" ;;
  *) mkdir -p "$(dirname "$out")"; printf '{}\n' >"$out" ;;
esac
EOF
chmod +x "$TMP/bin/polyedge-rs"

export SHADOW_CAMPAIGN_ID=campaign-2026-09-02-qset-v7
export SHADOW_CAMPAIGN_START=2026-09-02
export SHADOW_CAMPAIGN_PREFIX=shadow-events/campaign-2026-09-02-qset-v7
export SHADOW_CAMPAIGN_REPORT_ROOT=reports/research/shadow/campaigns/campaign-2026-09-02-qset-v7
export SHADOW_CORRECTION_ROOT=reports/research/shadow/campaigns/campaign-2026-09-02-qset-v7/corrections
export SHADOW_CAMPAIGN_CONTRACT=research/configs/profitability_gate_v3_2026-09-02_qset_v7.yaml
export SHADOW_EVIDENCE_VERSION=protocol-v3-qset-v7
export SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-v7-events
export AZURE_STORAGE_ACCOUNT_NAME=stpolyedge
export AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset-v7
export QSET_V7_CONTROL_CONTAINER_NAME=polyedge-qset-v7-control
export POLYEDGE_CAMPAIGN_LEASE_ACTIVE=true
export POLYEDGE_CAMPAIGN_LEASE_ID=test-lease
export POLYEDGE_CAMPAIGN_LEASE_BLOB=data/research/shadow/campaign-2026-09-02-qset-v7/control/replay.lock
export SHADOW_CODE_FREEZE_FINALIZED=true
export SHADOW_CODE_FREEZE_SHA256=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export SHADOW_CODE_FREEZE_MANIFEST=azure://stpolyedge/polyedge-qset-v7-control/reports/research/shadow/campaigns/campaign-2026-09-02-qset-v7/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json
export SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" POLYEDGE_TEST_ARGS="$TMP/args" POLYEDGE_UTC_TODAY=2026-09-04 \
    SHADOW_REPORT_DATE=2026-09-02 SHADOW_CASCADE_THROUGH=2026-09-02 \
    sh "$REPO/research/run_shadow_daily_v7.sh" >"$TMP/stdout"
)
grep -F 'research normalize --input azure://stpolyedge/polyedge-shadow-qset-v7-events/shadow-events/campaign-2026-09-02-qset-v7/2026/09/02/' "$TMP/args" >/dev/null
grep -F -- '--campaign-contract research/configs/profitability_gate_v3_2026-09-02_qset_v7.yaml' "$TMP/args" >/dev/null
test "$(jq -r '.manifest_path' "$TMP/work/reports/research/shadow/campaigns/campaign-2026-09-02-qset-v7/staging/"*/code_freeze_binding.json)" = "$SHADOW_CODE_FREEZE_MANIFEST"
if ( cd "$TMP/work"; PATH="$TMP/bin:$PATH" POLYEDGE_TEST_ARGS="$TMP/args" POLYEDGE_UTC_TODAY=2026-09-04 SHADOW_REPORT_DATE=2026-10-25 SHADOW_CASCADE_THROUGH=2026-10-25 sh "$REPO/research/run_shadow_daily_v7.sh" >/dev/null 2>&1 ); then echo "v7 runner accepted a terminal-past report date" >&2; exit 1; fi
if ( cd "$TMP/work"; PATH="$TMP/bin:$PATH" POLYEDGE_TEST_ARGS="$TMP/args" POLYEDGE_UTC_TODAY=2026-09-04 SHADOW_REPORT_DATE=2026-09-02 SHADOW_CASCADE_THROUGH=2026-10-25 sh "$REPO/research/run_shadow_daily_v7.sh" >/dev/null 2>&1 ); then echo "v7 runner accepted a terminal-past cascade date" >&2; exit 1; fi

for retired in campaign-2026-07-28-qset-v1 campaign-2026-08-22-qset-v2 campaign-2026-08-23-qset-v3 campaign-2026-08-24-qset-v4 campaign-2026-08-26-qset-v5 campaign-2026-09-01-qset-v6; do
  if (
    cd "$TMP/work"
    PATH="$TMP/bin:$PATH" SHADOW_CAMPAIGN_ID="$retired" sh "$REPO/research/run_shadow_daily_v7.sh" >/dev/null 2>&1
  ); then
    echo "v7 runner accepted retired $retired" >&2
    exit 1
  fi
done

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-events sh "$REPO/research/run_shadow_daily_v7.sh" >/dev/null 2>&1
); then
  echo "v7 runner accepted the legacy raw container" >&2
  exit 1
fi
if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" SHADOW_CODE_FREEZE_FINALIZED=false sh "$REPO/research/run_shadow_daily_v7.sh" >/dev/null 2>&1
); then
  echo "v7 runner accepted a draft freeze" >&2
  exit 1
fi
if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" SHADOW_CODE_FREEZE_MANIFEST=azure://stpolyedge/polyedge-qset-control/wrong.json sh "$REPO/research/run_shadow_daily_v7.sh" >/dev/null 2>&1
); then
  echo "v7 runner accepted a non-v7 control path" >&2
  exit 1
fi
if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" SHADOW_CODE_FREEZE_MANIFEST="azure://stpolyedge/polyedge-qset-v7-control/reports/research/shadow/campaigns/campaign-2026-09-02-qset-v7/control/code-freeze/source-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json" sh "$REPO/research/run_shadow_daily_v7.sh" >/dev/null 2>&1
); then
  echo "v7 runner accepted a freeze filename with a mismatched digest" >&2
  exit 1
fi
