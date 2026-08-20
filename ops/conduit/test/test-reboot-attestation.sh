#!/bin/sh
set -eu

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT HUP INT TERM
attestor=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-reboot-attestation
uid=$(id -u)
gid=$(id -g)
fake=$root/fake-bin
mkdir -p "$fake"

cat >"$fake/mountpoint" <<'FAKE'
#!/bin/sh
exit 0
FAKE
cat >"$fake/df" <<'FAKE'
#!/bin/sh
printf '%s\n' 'Filesystem 1-blocks Used Available Capacity Mounted on'
printf '%s\n' 'fixture 500000000000 1 200000000000 1% /'
FAKE
cat >"$fake/systemctl" <<'FAKE'
#!/bin/sh
action=$1
shift
unit=
for arg do
  [ "$arg" = --quiet ] || unit=$arg
done
case "$action:$unit" in
  is-enabled:polyedge-api.service|is-enabled:polyedge-frontend.service) echo generated ;;
  is-enabled:polyedge-funded-signer.service)
    [ "${FAKE_FUNDED_ACTIVE:-1}" = 1 ] && echo generated || echo masked
    ;;
  is-enabled:polyedge-federated-token@funded-signer.timer)
    [ "${FAKE_FUNDED_ACTIVE:-1}" = 1 ] && echo enabled || echo disabled
    ;;
  is-enabled:polyedge-shadow-qset.timer) echo disabled ;;
  is-enabled:*) echo enabled ;;
  is-active:polyedge-shadow-qset.timer|is-active:polyedge-job@shadow-qset.service) exit 1 ;;
  is-active:polyedge-funded-signer.service|is-active:polyedge-federated-token@funded-signer.timer)
    [ "${FAKE_FUNDED_ACTIVE:-1}" = 1 ]
    ;;
  is-active:*) exit 0 ;;
  show:*)
    echo 'polyedge-api.service polyedge-frontend.service polyedge-funded-signer.service'
    ;;
  *) exit 1 ;;
esac
FAKE
cat >"$fake/podman" <<'FAKE'
#!/bin/sh
format=
name=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --format) shift; format=$1 ;;
    *) name=$1 ;;
  esac
  shift
done
case "$format" in
  *'.State.Health'*)
    case "$name" in
      polyedge-funded-signer) printf '\n' ;;
      *) printf '%s\n' healthy ;;
    esac
    ;;
  *'.Config.Env'*)
    jq -nc --arg session "${FAKE_FUNDED_SESSION:-fixture-funded-v3}" \
      --arg manifest "${FAKE_FUNDED_SESSION_SHA:-sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}" \
      --arg config "${FAKE_FUNDED_CONFIG_SHA:-sha256:9999999999999999999999999999999999999999999999999999999999999999}" \
      '["VENUE_PROBE_FUNDED_CAMPAIGN_ID="+$session,
        "FUNDED_DIRECT_SESSION_MANIFEST_SHA256="+$manifest,
        "STRATEGY_CANARY_CANDIDATE_CONFIG_HASH="+$config]'
    ;;
  *'.Config.Image'*)
    case "$name" in
      polyedge-api) printf '%s\n' 'ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' ;;
      polyedge-frontend) printf '%s\n' "${FAKE_FRONTEND_IMAGE:-ghcr.io/fixture/polyedge-frontend@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}" ;;
      polyedge-funded-signer) printf '%s\n' 'ghcr.io/fixture/polyedge-venue-probe@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ;;
      *) exit 1 ;;
    esac
    ;;
  *'.Config.User'*)
    case "$name" in
      polyedge-funded-signer) printf '%s\n' "${FAKE_FUNDED_USER}" ;;
      *) printf '%s\n' '' ;;
    esac
    ;;
  *'.State.Status'*) printf '%s\n' running ;;
  *'.Id'*)
    case "$name" in
      polyedge-api) printf '%064d\n' 1 ;;
      polyedge-frontend) printf '%064d\n' 2 ;;
      polyedge-funded-signer) printf '%064d\n' 3 ;;
      *) exit 1 ;;
    esac
    ;;
  *) exit 1 ;;
esac
FAKE
chmod 0755 "$fake"/*

case_root=$root/case
mkdir -p "$case_root/run" "$case_root/ring/parity" "$case_root/tokens/api" "$case_root/tokens/research" "$case_root/tokens/funded"
chmod 0700 "$case_root/run" "$case_root/tokens/api" "$case_root/tokens/research" "$case_root/tokens/funded"
chmod 0750 "$case_root/ring/parity"
printf '%s\n' fixture-jwt-api >"$case_root/tokens/api/azure-federated-token"
printf '%s\n' fixture-jwt-research >"$case_root/tokens/research/azure-federated-token"
printf '%s\n' fixture-jwt-funded >"$case_root/tokens/funded/azure-federated-token"
chmod 0600 "$case_root/tokens/api/azure-federated-token" "$case_root/tokens/research/azure-federated-token" "$case_root/tokens/funded/azure-federated-token"
jq -n '{capacity_ok:true,free_ok:true,upload_fresh:true,unsealed_closed_count:0,unuploaded_count:0}' >"$case_root/ring/status.json"
chmod 0640 "$case_root/ring/status.json"
printf '%s\n' '11111111-1111-4111-8111-111111111111' >"$case_root/boot-id"
printf '%s\n' 'cpu 1 1 1 1' 'btime 1000' >"$case_root/proc-stat"
jq -n --arg start '2026-08-20T22:00:00Z' --arg user "$uid:$gid" '{schemaVersion:1,status:"in_progress",windowStartUtc:$start,
  azureAuthoritative:true,azureDeletionAllowed:false,acceptedCleanLiveHours:72,
  acceptedHourlyEvidence:[range(72) | {hourIndex:.}],completedDailyCycles:2,
  acceptedDailyEvidence:[range(2) | {cycleIndex:.}],rebootRecoveryPassed:false,
  shadowQsetEnabled:false,fundedSignerEnabled:true,fundedSignerMode:"active",
  fundedSignerImage:"ghcr.io/fixture/polyedge-venue-probe@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
  fundedSignerUser:$user,fundedSessionId:"fixture-funded-v3",
  fundedSessionManifestSha256:"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
  fundedConfigSha256:"sha256:9999999999999999999999999999999999999999999999999999999999999999"}' >"$case_root/ring/parity/20260820T220000Z-funded-active.json"
chmod 0640 "$case_root/ring/parity/20260820T220000Z-funded-active.json"
cat >"$case_root/parity.env" <<ENV
POLYEDGE_PARITY_WINDOW_START_UTC=2026-08-20T22:00:00Z
POLYEDGE_PARITY_LEDGER=$case_root/ring/parity/20260820T220000Z-funded-active.json
POLYEDGE_PARITY_RING_ROOT=$case_root/ring
POLYEDGE_PARITY_RING_STATUS=$case_root/ring/status.json
POLYEDGE_PARITY_BOOT_ROOT=$case_root
POLYEDGE_PARITY_BOOT_MIN_FREE_BYTES=1
POLYEDGE_JOB_MIN_FREE_BYTES=1
POLYEDGE_PARITY_PAUSE_FILE=$case_root/run/image-pulls-paused
POLYEDGE_PARITY_LOCK_FILE=$case_root/run/ledger.lock
POLYEDGE_PARITY_EXPECTED_GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
POLYEDGE_PARITY_EXPECTED_AZURE_RESEARCH_IMAGE=crfixture.azurecr.io/polyedge-rust-research@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
POLYEDGE_PARITY_EXPECTED_OCI_RESEARCH_IMAGE=ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
POLYEDGE_PARITY_FUNDED_MODE=active
POLYEDGE_PARITY_EXPECTED_FUNDED_IMAGE=ghcr.io/fixture/polyedge-venue-probe@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
POLYEDGE_PARITY_FUNDED_UID=$uid
POLYEDGE_PARITY_FUNDED_GID=$gid
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_ID=fixture-funded-v3
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_SHA256=sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
POLYEDGE_PARITY_EXPECTED_FUNDED_CONFIG_SHA256=sha256:9999999999999999999999999999999999999999999999999999999999999999
ENV
chmod 0640 "$case_root/parity.env"
: >"$case_root/run/ledger.lock"
chmod 0640 "$case_root/run/ledger.lock"

run_attestor() {
  env PATH="$fake:$PATH" POLYEDGE_REBOOT_EXPECTED_UID="$uid" POLYEDGE_REBOOT_EXPECTED_GID="$gid" \
    POLYEDGE_PARITY_ENV_FILE="${ATTEST_ENV_FILE:-$case_root/parity.env}" POLYEDGE_REBOOT_RUNTIME_DIR="$case_root/run/reboot-attestation" \
    POLYEDGE_REBOOT_BOOT_ID_FILE="$case_root/boot-id" POLYEDGE_REBOOT_PROC_STAT="$case_root/proc-stat" \
    POLYEDGE_REBOOT_API_TOKEN_FILE="$case_root/tokens/api/azure-federated-token" \
    POLYEDGE_REBOOT_RESEARCH_TOKEN_FILE="$case_root/tokens/research/azure-federated-token" \
    POLYEDGE_REBOOT_FUNDED_TOKEN_FILE="$case_root/tokens/funded/azure-federated-token" \
    POLYEDGE_REBOOT_API_UID="$uid" POLYEDGE_REBOOT_API_GID="$gid" \
    POLYEDGE_REBOOT_RESEARCH_UID="$uid" POLYEDGE_REBOOT_RESEARCH_GID="$gid" \
    FAKE_FUNDED_USER="$uid:$gid" FAKE_FUNDED_ACTIVE="${FAKE_FUNDED_ACTIVE:-1}" \
    FAKE_FUNDED_SESSION="${FAKE_FUNDED_SESSION:-fixture-funded-v3}" \
    FAKE_FUNDED_SESSION_SHA="${FAKE_FUNDED_SESSION_SHA:-sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}" \
    FAKE_FUNDED_CONFIG_SHA="${FAKE_FUNDED_CONFIG_SHA:-sha256:9999999999999999999999999999999999999999999999999999999999999999}" \
    FAKE_FRONTEND_IMAGE="${FAKE_FRONTEND_IMAGE:-}" "$attestor" "$1"
}

ledger=$case_root/ring/parity/20260820T220000Z-funded-active.json
raw=$case_root/raw-ledger.json
jq '.rebootRecoveryPassed = true' "$ledger" >"$raw"
chmod 0640 "$raw"
mv "$raw" "$ledger"
if run_attestor validate-ledger >/dev/null 2>&1; then
  echo 'raw rebootRecoveryPassed=true unexpectedly passed' >&2
  exit 1
fi
jq '.rebootRecoveryPassed = false | del(.rebootRecovery)' "$ledger" >"$raw"
chmod 0640 "$raw"
mv "$raw" "$ledger"
run_attestor validate-ledger

[ "$(stat -c %s "$case_root/run/ledger.lock")" -eq 0 ]
disabled_ledger=$case_root/ring/parity/disabled-ledger.json
jq '.fundedSignerEnabled = false | .fundedSignerMode = "disabled" |
  del(.fundedSignerImage,.fundedSignerUser,.fundedSessionId,.fundedSessionManifestSha256,.fundedConfigSha256)' \
  "$ledger" >"$disabled_ledger"
chmod 0640 "$disabled_ledger"
disabled_env=$case_root/disabled-parity.env
awk -v ledger="$disabled_ledger" '
  /^POLYEDGE_PARITY_LEDGER=/ {print "POLYEDGE_PARITY_LEDGER=" ledger; next}
  /^POLYEDGE_PARITY_FUNDED_MODE=/ {print "POLYEDGE_PARITY_FUNDED_MODE=disabled"; next}
  /^POLYEDGE_PARITY_EXPECTED_FUNDED_/ {next}
  /^POLYEDGE_PARITY_FUNDED_UID=/ {next}
  /^POLYEDGE_PARITY_FUNDED_GID=/ {next}
  {print}
' "$case_root/parity.env" >"$disabled_env"
chmod 0640 "$disabled_env"
if ATTEST_ENV_FILE="$disabled_env" FAKE_FUNDED_ACTIVE=1 run_attestor prepare >/dev/null 2>&1; then
  echo 'active funded signer unexpectedly passed disabled-mode prepare' >&2
  exit 1
fi

if FAKE_FUNDED_SESSION=drifted-session run_attestor prepare >/dev/null 2>&1; then
  echo 'funded session drift unexpectedly passed prepare' >&2
  exit 1
fi
if FAKE_FUNDED_SESSION_SHA=sha256:8888888888888888888888888888888888888888888888888888888888888888 \
  run_attestor prepare >/dev/null 2>&1; then
  echo 'funded manifest drift unexpectedly passed prepare' >&2
  exit 1
fi
if FAKE_FUNDED_CONFIG_SHA=sha256:7777777777777777777777777777777777777777777777777777777777777777 \
  run_attestor prepare >/dev/null 2>&1; then
  echo 'funded config drift unexpectedly passed prepare' >&2
  exit 1
fi
run_attestor prepare >/dev/null
pending=$case_root/ring/parity/reboot/pending.json
[ -f "$pending" ]
pre=$(jq -r '.preboot.path' "$pending")
[ "$(stat -c %a "$pre")" = 640 ]
[ "$(stat -c %a "${pre%/*}")" = 750 ]
jq -e 'all(.tokens[]; has("sha256") | not)' "$pre" >/dev/null
! grep -R -F 'fixture-jwt' "$case_root/ring/parity/reboot" >/dev/null

if run_attestor complete >/dev/null 2>&1; then
  echo 'same-boot complete unexpectedly passed' >&2
  exit 1
fi
[ -f "$pending" ]
jq -e '.rebootRecoveryPassed == false and (has("rebootRecovery") | not)' "$ledger" >/dev/null

printf '%s\n' '22222222-2222-4222-8222-222222222222' >"$case_root/boot-id"
printf '%s\n' 'cpu 1 1 1 1' 'btime 2000' >"$case_root/proc-stat"
FAKE_FRONTEND_IMAGE='ghcr.io/fixture/polyedge-frontend@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'
export FAKE_FRONTEND_IMAGE
if run_attestor complete >/dev/null 2>&1; then
  echo 'frontend image drift unexpectedly passed' >&2
  exit 1
fi
unset FAKE_FRONTEND_IMAGE
[ -f "$pending" ]
jq -e '.rebootRecoveryPassed == false and (has("rebootRecovery") | not)' "$ledger" >/dev/null
post_dir=$case_root/ring/parity/reboot/22222222-2222-4222-8222-222222222222
mkdir -p "$post_dir"
chmod 0750 "$post_dir"
invalid_post=$post_dir/postboot.json
pre_sha=sha256:$(sha256sum "$pre" | awk '{print $1}')
jq --arg boot '22222222-2222-4222-8222-222222222222' --arg pre "$pre" --arg pre_sha "$pre_sha" '
  .phase = "postboot" |
  .generatedAtUtc = "2026-08-20T22:10:00Z" |
  .boot = {id:$boot,btime:2000,observedAtUtc:"2026-08-20T22:10:00Z"} |
  .preboot = {path:$pre,sha256:$pre_sha} |
  .protectedBindings.gitSha = "dddddddddddddddddddddddddddddddddddddddd"
' "$pre" >"$invalid_post"
chmod 0640 "$invalid_post"
if run_attestor complete >/dev/null 2>&1; then
  echo 'cross-evidence-invalid existing post unexpectedly passed complete' >&2
  exit 1
fi
[ -f "$pending" ]
jq -e '.rebootRecoveryPassed == false and (has("rebootRecovery") | not)' "$ledger" >/dev/null
rm "$invalid_post"


run_attestor complete >/dev/null
[ ! -e "$pending" ]
run_attestor validate-ledger
jq -e '.rebootRecoveryPassed == true and .rebootRecovery.status == "validated"' "$ledger" >/dev/null
post=$(jq -r '.rebootRecovery.postboot.path' "$ledger")
jq -e 'all(.tokens[]; has("sha256") | not)' "$post" >/dev/null

cp "$post" "$case_root/post.saved"
jq '.status = "tampered"' "$post" >"$case_root/post.tmp"
chmod 0640 "$case_root/post.tmp"
mv "$case_root/post.tmp" "$post"
if run_attestor validate-ledger >/dev/null 2>&1; then
  echo 'tampered postboot evidence unexpectedly passed' >&2
  exit 1
fi
chmod 0640 "$case_root/post.saved"
mv "$case_root/post.saved" "$post"
run_attestor validate-ledger

echo 'reboot attestation self-test passed'
