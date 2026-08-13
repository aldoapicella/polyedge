#!/bin/sh
set -eu

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT HUP INT TERM
recorder=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-parity-record-daily
uid=$(id -u)
gid=$(id -g)
fake=$root/fake-bin
mkdir -p "$fake"
cat >"$fake/mountpoint" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$fake/df" <<'EOF'
#!/bin/sh
printf '%s\n' 'Filesystem 1-blocks Used Available Capacity Mounted on'
printf '%s\n' 'fixture 100000000000 1 50000000000 1% /'
EOF
chmod 0755 "$fake"/*

make_bundle() {
  case_root=$1 date=$2 completed=$3 role=${4:-primary} quality=${5:-pass} git=${6:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}
  run="daily-$date-$(printf '%s' "$completed" | tr -d ':-')"
  date_dir=$case_root/ring/jobs/research/reports/research/daily/$date
  bundle=$date_dir/runs/$run
  marker=$case_root/ring/jobs/research/data/research/daily/$date/normalized/.polyedge-daily-complete.json
  mkdir -p "$bundle" "${marker%/*}"
  artifacts='{}'
  for name in baseline calibration data_audit execution_quality final_report markets_summary raw_data_audit regimes sample_size; do
    printf '{"result":{"fixture":"%s"}}\n' "$name" >"$bundle/$name.json"
    hash=$(sha256sum "$bundle/$name.json" | awk '{print $1}')
    bytes=$(stat -c %s "$bundle/$name.json")
    artifacts=$(printf '%s' "$artifacts" | jq --arg key "${name}_json" --arg path "$name.json" --arg hash "$hash" --argjson bytes "$bytes" \
      '. + {($key):{name:$key,relative_path:$path,sha256:$hash,bytes:$bytes}}')
  done
  input=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  jq -n --arg date "$date" --arg run "$run" --arg completed "$completed" --arg input "$input" --arg git "$git" --arg role "$role" \
    --arg quality "$quality" --argjson artifacts "$artifacts" '{schema_version:2,git_sha:$git,runtime_role:$role,date:$date,run_id:$run,created_at:$completed,completed_at:$completed,input_sha256:$input,status:"COMPLETE",artifacts:$artifacts,data_quality:{registry_version:"research-data-quality-v5",total_events:1,decision_grade_coverage:"1",fatal_issues:(if $quality == "pass" then [] else ["fixture"] end),warnings:[],out_of_order_events:0,event_time_ordering_restored:true,coverage_breakdown:{start_price_capture_rate:"1",settlement_rate:"1",exact_reference_hour_coverage:"1",decision_metadata_coverage:"1",decision_grade_coverage:"1",final_decision_grade_coverage:"1",execution_field_coverage:"1",decision_parity_rate:"1",queue_position_coverage:null,queue_position_applicable:false,markout_1s_completion:null,markout_1s_applicable:false,markout_5s_completion:null,markout_5s_applicable:false,markout_30s_completion:null,markout_30s_applicable:false}}}' \
    >"$bundle/run_manifest.json"
  manifest_sha=$(sha256sum "$bundle/run_manifest.json" | awk '{print $1}')
  jq -n --arg date "$date" --arg run "$run" --arg path "runs/$run/run_manifest.json" --arg sha "$manifest_sha" --arg completed "$completed" \
    '{schema_version:1,date:$date,run_id:$run,manifest_path:$path,manifest_sha256:$sha,promoted_at:$completed}' >"$date_dir/latest.json"
  jq -n --arg date "$date" --arg git "$git" --arg input "$input" \
    '{schema_version:1,date:$date,git_sha:$git,events_manifest_sha256:$input}' >"$marker"
  chmod 0600 "$marker"
}

fixture() {
  case_root=$1
  mkdir -p "$case_root/run" "$case_root/ring/parity" "$case_root/ring/jobs/research/reports/research/daily" \
    "$case_root/ring/jobs/research/data/research/daily"
  chmod 0750 "$case_root/ring/parity"
  jq -n '{schemaVersion:1,status:"in_progress",windowStartUtc:"2026-08-11T06:00:00Z",azureAuthoritative:true,azureDeletionAllowed:false,acceptedCleanLiveHours:7,acceptedHourlyEvidence:[{fixture:true}],completedDailyCycles:0,acceptedDailyEvidence:[],rebootRecoveryPassed:false,shadowQsetEnabled:false,fundedSignerEnabled:false}' \
    >"$case_root/ring/parity/ledger.json"
  chmod 0640 "$case_root/ring/parity/ledger.json"
  jq -n '{capacity_ok:true,free_ok:true,upload_fresh:true,unsealed_closed_count:0,unuploaded_count:0}' >"$case_root/ring/status.json"
  chmod 0640 "$case_root/ring/status.json"
  cat >"$case_root/parity.env" <<EOF
POLYEDGE_PARITY_WINDOW_START_UTC=2026-08-11T06:00:00Z
POLYEDGE_PARITY_LEDGER=$case_root/ring/parity/ledger.json
POLYEDGE_PARITY_RING_ROOT=$case_root/ring
POLYEDGE_PARITY_DAILY_REPORT_ROOT=$case_root/ring/jobs/research/reports/research/daily
POLYEDGE_PARITY_DAILY_DATA_ROOT=$case_root/ring/jobs/research/data/research/daily
POLYEDGE_PARITY_RING_STATUS=$case_root/ring/status.json
POLYEDGE_PARITY_BOOT_ROOT=$case_root
POLYEDGE_PARITY_PAUSE_FILE=$case_root/run/image-pulls-paused
POLYEDGE_PARITY_LOCK_FILE=$case_root/run/ledger.lock
POLYEDGE_PARITY_EXPECTED_GIT_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
POLYEDGE_PARITY_EXPECTED_RESEARCH_IMAGE=ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EOF
  chmod 0640 "$case_root/parity.env"
}

run_recorder() {
  case_root=$1 date=$2
  env PATH="$fake:$PATH" POLYEDGE_PARITY_EXPECTED_UID="$uid" POLYEDGE_PARITY_EXPECTED_GID="$gid" \
    POLYEDGE_RESEARCH_IMAGE=ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    POLYEDGE_PARITY_ENV_FILE="$case_root/parity.env" "$recorder" "$date"
}

prewindow=$root/prewindow
fixture "$prewindow"
prewindow_ledger_sha=$(sha256sum "$prewindow/ring/parity/ledger.json")
run_recorder "$prewindow" 2026-08-10 >/dev/null
[ "$(sha256sum "$prewindow/ring/parity/ledger.json")" = "$prewindow_ledger_sha" ]
[ ! -e "$prewindow/ring/parity/daily" ]

success=$root/success
fixture "$success"
make_bundle "$success" 2026-08-11 2026-08-12T08:00:00Z
before=$(jq -cS '{status,azureAuthoritative,azureDeletionAllowed,rebootRecoveryPassed,shadowQsetEnabled,fundedSignerEnabled,acceptedCleanLiveHours,acceptedHourlyEvidence}' "$success/ring/parity/ledger.json")
run_recorder "$success" 2026-08-11 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(jq -r '.acceptedDailyEvidence | length' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(jq -cS '{status,azureAuthoritative,azureDeletionAllowed,rebootRecoveryPassed,shadowQsetEnabled,fundedSignerEnabled,acceptedCleanLiveHours,acceptedHourlyEvidence}' "$success/ring/parity/ledger.json")" = "$before" ]
evidence_sha=$(sha256sum "$success/ring/parity/daily/2026-08-11/evidence.json")
run_recorder "$success" 2026-08-11 >/dev/null
[ "$(sha256sum "$success/ring/parity/daily/2026-08-11/evidence.json")" = "$evidence_sha" ]
make_bundle "$success" 2026-08-12 2026-08-13T08:00:00Z
run_recorder "$success" 2026-08-12 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$success/ring/parity/ledger.json")" = 2 ]

legacy=$root/legacy
fixture "$legacy"
jq 'del(.completedDailyCycles,.acceptedDailyEvidence)' "$legacy/ring/parity/ledger.json" >"$legacy/ledger.tmp"
chmod 0640 "$legacy/ledger.tmp"
mv "$legacy/ledger.tmp" "$legacy/ring/parity/ledger.json"
make_bundle "$legacy" 2026-08-11 2026-08-12T08:00:00Z
run_recorder "$legacy" 2026-08-11 >/dev/null
jq -e '.completedDailyCycles == 1 and (.acceptedDailyEvidence | length) == 1' "$legacy/ring/parity/ledger.json" >/dev/null

gap=$root/gap
fixture "$gap"
make_bundle "$gap" 2026-08-12 2026-08-13T08:00:00Z
if run_recorder "$gap" 2026-08-12 >/dev/null 2>&1; then
  echo 'gapped daily evidence unexpectedly passed' >&2
  exit 1
fi
make_bundle "$gap" 2026-08-11 2026-08-12T08:00:00Z
run_recorder "$gap" 2026-08-11 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$gap/ring/parity/ledger.json")" = 1 ]
run_recorder "$gap" 2026-08-12 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$gap/ring/parity/ledger.json")" = 2 ]

tamper=$root/tamper
fixture "$tamper"
make_bundle "$tamper" 2026-08-11 2026-08-12T08:00:00Z
printf 'tampered\n' >>"$(find "$tamper/ring/jobs/research/reports/research/daily/2026-08-11/runs" -name baseline.json)"
if run_recorder "$tamper" 2026-08-11 >/dev/null 2>&1; then
  echo 'tampered daily artifact unexpectedly passed' >&2
  exit 1
fi

role=$root/role
fixture "$role"
make_bundle "$role" 2026-08-11 2026-08-12T08:00:00Z profitability_shadow
if run_recorder "$role" 2026-08-11 >/dev/null 2>&1; then
  echo 'shadow daily bundle unexpectedly passed as primary' >&2
  exit 1
fi

quality=$root/quality
fixture "$quality"
make_bundle "$quality" 2026-08-11 2026-08-12T08:00:00Z primary fail
if run_recorder "$quality" 2026-08-11 >/dev/null 2>&1; then
  echo 'fatal daily data quality unexpectedly passed' >&2
  exit 1
fi

source=$root/source
fixture "$source"
make_bundle "$source" 2026-08-11 2026-08-12T08:00:00Z primary pass cccccccccccccccccccccccccccccccccccccccc
if run_recorder "$source" 2026-08-11 >/dev/null 2>&1; then
  echo 'unapproved daily source unexpectedly passed' >&2
  exit 1
fi

sh -n "$recorder"
grep -F 'POLYEDGE_PARITY_LOCK_FILE' "$recorder" >/dev/null
echo 'parity daily recorder self-test passed'
