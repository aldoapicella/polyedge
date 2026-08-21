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
  is-enabled:polyedge-funded-signer.service|is-enabled:polyedge-funded-intent-producer.service)
    [ "${FAKE_FUNDED_ACTIVE:-1}" = 1 ] && echo generated || echo masked
    ;;
  is-enabled:polyedge-federated-token@funded-signer.timer|is-enabled:polyedge-federated-token@funded-intent-producer.timer)
    [ "${FAKE_FUNDED_ACTIVE:-1}" = 1 ] && echo enabled || echo disabled
    ;;
  is-enabled:polyedge-shadow-qset.timer) echo disabled ;;
  is-enabled:*) echo enabled ;;
  is-active:polyedge-shadow-qset.timer|is-active:polyedge-job@shadow-qset.service) exit 1 ;;
  is-active:polyedge-funded-signer.service|is-active:polyedge-federated-token@funded-signer.timer|is-active:polyedge-funded-intent-producer.service|is-active:polyedge-federated-token@funded-intent-producer.timer)
    [ "${FAKE_FUNDED_ACTIVE:-1}" = 1 ]
    ;;
  is-active:*) exit 0 ;;
  show:*)
    echo 'polyedge-api.service polyedge-frontend.service polyedge-funded-signer.service polyedge-funded-intent-producer.service'
    ;;
  *) exit 1 ;;
esac
FAKE
cat >"$fake/podman" <<'FAKE'
#!/bin/sh
if [ "${1:-}" = exec ]; then
  case "$*" in
    *'/api/v1/health'*)
      printf 'HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n'
      jq -nc '{ok:true,backend_impl:"rust",runtime_role:"profitability_shadow",
        shadow_only:true,runtime_active:true,execution_mode:"paper",kill_switch:false}'
      ;;
    *'/api/v1/status'*)
      printf 'HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n'
      sleep "${FAKE_PRODUCER_STATUS_DELAY:-0}"
      updated=$(date -u +%Y-%m-%dT%H:%M:%SZ)
      jq -nc --arg updated "$updated" --argjson queued "${FAKE_PRODUCER_QUEUED:-0}" \
        --argjson prepared "${FAKE_PRODUCER_PREPARED:-true}" '
        {app:"polyedge-funded-intent-producer",backend_impl:"rust",runtime_role:"profitability_shadow",
         shadow_only:true,execution_mode:"paper",intent_publisher:{configured:true,prepared:$prepared,pointer_only_preflight:false},task_health:{api:"ok",runtime_loop:"running",feeds:"running"},
         feed_status:{Discovery:{status:"ok",updated_at:$updated},
           PolymarketClobMarket:{status:"ok",updated_at:$updated},
           PolymarketRtdsChainlink:{status:"ok",updated_at:$updated},
           PolymarketRtdsBinance:{status:"ok",updated_at:$updated}},
         drop_counts:{},recorder_status:{error_count:0,dropped_count:0},
         recorder_metrics:{recorder_instance_id:"11111111-1111-4111-8111-111111111111",
           last_assigned_sequence:12,queued:$queued,enqueued_total:12,persisted_total:12,
           failed_total:0,unrecovered_durable_events:0,flush_unrecovered:false}}'
      ;;
    *) exit 1 ;;
  esac
  exit 0
fi
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
    if [ "$name" = polyedge-funded-intent-producer ]; then
      jq -nc --arg run_bot "${FAKE_PRODUCER_RUN_BOT:-true}" '[
        "APP_NAME=polyedge-funded-intent-producer","RUNTIME_ROLE=profitability_shadow","EXECUTION_MODE=paper","ALLOW_LIVE=false",
        "RUN_BOT_ON_STARTUP="+$run_bot,"STRATEGY_INTENT_OPERATOR_DIRECT=true","PUBLISH_STRATEGY_CANARY_INTENTS=true",
        "STRATEGY_CANARY_INTENT_PREFIX=reports/research/venue-probe/control/strategy-canary/intents",
        "STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION=conservative-execution-prior-v1",
        "STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI=azure://stpolyedge6urdjr5nmwx7w/polyedge-research/reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json",
        "STRATEGY_CANARY_EXECUTION_MODEL_SHA256=sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4",
        "STRATEGY_INTENT_TARGET_ORDER_NOTIONAL=10.5","STRATEGY_INTENT_MAX_ORDER_NOTIONAL=10.5","STRATEGY_INTENT_MIN_SECONDS_TO_EXPIRY=360","STRATEGY_INTENT_MAX_SECONDS_TO_EXPIRY=900",
        "TARGET_ASSET=BTC","TARGET_ASSET_NAME=Bitcoin","TARGET_HORIZON=15m","TARGET_BINANCE_SYMBOL=btcusdt","TARGET_CHAINLINK_SYMBOL=btc/usd","TARGET_COINBASE_PRODUCT_ID=BTC-USD",
        "AZURE_TENANT_ID=9767f0dc-e83f-4cc1-94e1-0d5f9d287d32","AZURE_CLIENT_ID=54f0136b-5e72-4ad1-b23e-cb1269d356c1",
        "AZURE_FEDERATED_TOKEN_FILE=/run/credentials/azure-federated-token","AZURE_TOKEN_CREDENTIALS=WorkloadIdentityCredential",
        "AZURE_STORAGE_ACCOUNT_NAME=stpolyedge6urdjr5nmwx7w","AZURE_STORAGE_CONTAINER_NAME=polyedge-shadow-events","AZURE_STORAGE_TABLE_NAME=ShadowBotEventIndex",
        "AZURE_CHART_TABLE_NAME=ShadowBotChartSeries","AZURE_MARKET_TABLE_NAME=ShadowBotMarketCatalog","AZURE_FUNDED_STORAGE_CONTAINER_NAME=polyedge-funded-evidence",
        "AZURE_MODEL_STORAGE_CONTAINER_NAME=polyedge-models","FUNDED_DIRECT_SERVICE_BUS_ENABLED=true",
        "FUNDED_DIRECT_SERVICE_BUS_NAMESPACE=sb-polyedge-funded-cl-6urdjr5nmwx7w","FUNDED_DIRECT_SERVICE_BUS_QUEUE=funded-dynamic-quote-intents"]'
    else
      jq -nc --arg session "${FAKE_FUNDED_SESSION:-fixture-funded-v3}" \
        --arg manifest "${FAKE_FUNDED_SESSION_SHA:-sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}" \
        --arg config "${FAKE_FUNDED_CONFIG_SHA:-sha256:9999999999999999999999999999999999999999999999999999999999999999}" \
        '["VENUE_PROBE_FUNDED_CAMPAIGN_ID="+$session,"FUNDED_DIRECT_SESSION_MANIFEST_SHA256="+$manifest,
          "STRATEGY_CANARY_CANDIDATE_CONFIG_HASH="+$config]'
    fi
    ;;
  *'.Mounts'*)
    jq -nc --arg source "$FAKE_PRODUCER_TOKEN_DIR" --argjson rw "${FAKE_PRODUCER_MOUNT_RW:-false}" \
      '[{Source:$source,Destination:"/run/credentials",RW:$rw}]'
    ;;
  *'.Config.Image'*)
    case "$name" in
      polyedge-api) printf '%s\n' 'ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' ;;
      polyedge-frontend) printf '%s\n' "${FAKE_FRONTEND_IMAGE:-ghcr.io/fixture/polyedge-frontend@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}" ;;
      polyedge-funded-signer) printf '%s\n' 'ghcr.io/fixture/polyedge-venue-probe@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ;;
      polyedge-funded-intent-producer) printf '%s\n' "${FAKE_PRODUCER_IMAGE:-ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc}" ;;
      *) exit 1 ;;
    esac
    ;;
  *'.Config.User'*)
    case "$name" in
      polyedge-funded-signer) printf '%s\n' "${FAKE_FUNDED_USER}" ;;
      polyedge-funded-intent-producer) printf '%s\n' '984:980' ;;
      *) printf '%s\n' '' ;;
    esac
    ;;
  *'.State.Status'*) printf '%s\n' running ;;
  *'.Id'*)
    case "$name" in
      polyedge-api) printf '%064d\n' 1 ;;
      polyedge-frontend) printf '%064d\n' 2 ;;
      polyedge-funded-signer) printf '%064d\n' 3 ;;
      polyedge-funded-intent-producer) printf '%064d\n' 4 ;;
      *) exit 1 ;;
    esac
    ;;
  *) exit 1 ;;
esac
FAKE
cat >"$fake/stat" <<'FAKE'
#!/bin/sh
format=
path=
while [ "$#" -gt 0 ]; do
  case "$1" in
    -c) shift; format=$1 ;;
    *) path=$1 ;;
  esac
  shift
done
case "$path:$format" in
  "${FAKE_PRODUCER_TOKEN_DIR}:%u %g %a") printf '%s\n' '984 980 700' ;;
  "${FAKE_PRODUCER_TOKEN_PATH}:%u %g %a %h %s %Y")
    printf '984 980 600 1 21 %s\n' "$(date -u +%s)"
    ;;
  *) exec /usr/bin/stat -c "$format" "$path" ;;
esac
FAKE
cat >"$fake/sha256sum" <<'FAKE'
#!/bin/sh
if [ "${1:-}" = "${FAKE_PRODUCER_CONFIG_PATH:-}" ]; then
  printf '%s  %s\n' '56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b' "$1"
else
  exec /usr/bin/sha256sum "$@"
fi
FAKE
chmod 0755 "$fake"/*

case_root=$root/case
mkdir -p "$case_root/run" "$case_root/ring/parity" "$case_root/tokens/api" "$case_root/tokens/research" "$case_root/tokens/funded" "$case_root/tokens/producer"
chmod 0700 "$case_root/run" "$case_root/tokens/api" "$case_root/tokens/research" "$case_root/tokens/funded" "$case_root/tokens/producer"
chmod 0750 "$case_root/ring/parity"
printf '%s\n' fixture-jwt-api >"$case_root/tokens/api/azure-federated-token"
printf '%s\n' fixture-jwt-research >"$case_root/tokens/research/azure-federated-token"
printf '%s\n' fixture-jwt-funded >"$case_root/tokens/funded/azure-federated-token"
printf '%s\n' fixture-jwt-producer >"$case_root/tokens/producer/azure-federated-token"
printf '%s\n' fixture-producer-config >"$case_root/funded-intent-producer.env"
chmod 0600 "$case_root/tokens/api/azure-federated-token" "$case_root/tokens/research/azure-federated-token" "$case_root/tokens/funded/azure-federated-token" \
  "$case_root/tokens/producer/azure-federated-token" "$case_root/funded-intent-producer.env"
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
  fundedConfigSha256:"sha256:9999999999999999999999999999999999999999999999999999999999999999",
  fundedIntentProducerEnabled:true,
  fundedIntentProducerImage:"ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc",
  fundedIntentProducerUser:"984:980",
  fundedIntentProducerConfigSha256:"sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b"}' >"$case_root/ring/parity/20260820T220000Z-funded-active.json"
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
POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_IMAGE=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc
POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_CONFIG_SHA256=sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b
POLYEDGE_PARITY_FUNDED_PRODUCER_ENV_FILE=$case_root/funded-intent-producer.env
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
    POLYEDGE_REBOOT_FUNDED_PRODUCER_TOKEN_FILE="$case_root/tokens/producer/azure-federated-token" \
    POLYEDGE_REBOOT_API_UID="$uid" POLYEDGE_REBOOT_API_GID="$gid" \
    POLYEDGE_REBOOT_RESEARCH_UID="$uid" POLYEDGE_REBOOT_RESEARCH_GID="$gid" \
    FAKE_PRODUCER_TOKEN_DIR="$case_root/tokens/producer" \
    FAKE_PRODUCER_TOKEN_PATH="$case_root/tokens/producer/azure-federated-token" \
    FAKE_PRODUCER_CONFIG_PATH="$case_root/funded-intent-producer.env" \
    FAKE_FUNDED_USER="$uid:$gid" FAKE_FUNDED_ACTIVE="${FAKE_FUNDED_ACTIVE:-1}" \
    FAKE_FUNDED_SESSION="${FAKE_FUNDED_SESSION:-fixture-funded-v3}" \
    FAKE_FUNDED_SESSION_SHA="${FAKE_FUNDED_SESSION_SHA:-sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}" \
    FAKE_FUNDED_CONFIG_SHA="${FAKE_FUNDED_CONFIG_SHA:-sha256:9999999999999999999999999999999999999999999999999999999999999999}" \
    FAKE_FRONTEND_IMAGE="${FAKE_FRONTEND_IMAGE:-}" FAKE_PRODUCER_IMAGE="${FAKE_PRODUCER_IMAGE:-}" \
    FAKE_PRODUCER_QUEUED="${FAKE_PRODUCER_QUEUED:-0}" FAKE_PRODUCER_PREPARED="${FAKE_PRODUCER_PREPARED:-true}" \
    FAKE_PRODUCER_STATUS_DELAY="${FAKE_PRODUCER_STATUS_DELAY:-0}" \
    FAKE_PRODUCER_RUN_BOT="${FAKE_PRODUCER_RUN_BOT:-true}" FAKE_PRODUCER_MOUNT_RW="${FAKE_PRODUCER_MOUNT_RW:-false}" \
    "$attestor" "$1"
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
FAKE_PRODUCER_STATUS_DELAY=1 run_attestor validate-ledger

[ "$(stat -c %s "$case_root/run/ledger.lock")" -eq 0 ]
disabled_ledger=$case_root/ring/parity/disabled-ledger.json
jq '.fundedSignerEnabled = false | .fundedSignerMode = "disabled" | .fundedIntentProducerEnabled = false |
  del(.fundedSignerImage,.fundedSignerUser,.fundedSessionId,.fundedSessionManifestSha256,.fundedConfigSha256,
    .fundedIntentProducerImage,.fundedIntentProducerUser,.fundedIntentProducerConfigSha256)' \
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
if FAKE_PRODUCER_IMAGE=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:7777777777777777777777777777777777777777777777777777777777777777 \
  run_attestor prepare >/dev/null 2>&1; then
  echo 'funded producer image drift unexpectedly passed prepare' >&2
  exit 1
fi
if FAKE_PRODUCER_QUEUED=1 run_attestor prepare >/dev/null 2>&1; then
  echo 'funded producer recorder backlog unexpectedly passed prepare' >&2
  exit 1
fi
if FAKE_PRODUCER_RUN_BOT=false run_attestor prepare >/dev/null 2>&1; then
  echo 'wrong producer executable environment unexpectedly passed prepare' >&2
  exit 1
fi
if FAKE_PRODUCER_MOUNT_RW=true run_attestor prepare >/dev/null 2>&1; then
  echo 'read-write producer token mount unexpectedly passed prepare' >&2
  exit 1
fi
if FAKE_PRODUCER_PREPARED=false run_attestor prepare >/dev/null 2>&1; then
  echo 'unprepared producer publisher unexpectedly passed prepare' >&2
  exit 1
fi
run_attestor prepare >/dev/null
pending=$case_root/ring/parity/reboot/pending.json
[ -f "$pending" ]
pre=$(jq -r '.preboot.path' "$pending")
[ "$(stat -c %a "$pre")" = 640 ]
[ "$(stat -c %a "${pre%/*}")" = 750 ]
jq -e 'all(.tokens[]; has("sha256") | not)' "$pre" >/dev/null
jq -e --arg token "$case_root/tokens/producer/azure-federated-token" '
  .protectedBindings.fundedIntentProducerEnabled == true and
  .protectedBindings.fundedIntentProducerImage == "ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc" and
  .protectedBindings.fundedIntentProducerUser == "984:980" and
  .protectedBindings.fundedIntentProducerConfigSha256 == "sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b" and
  any(.units[]; .name == "polyedge-funded-intent-producer.service" and .active == true) and
  any(.units[]; .name == "polyedge-federated-token@funded-intent-producer.timer" and .active == true) and
  any(.containers[]; .name == "polyedge-funded-intent-producer" and .user == "984:980" and .health == "healthy") and
  any(.tokens[]; .name == "funded-intent-producer" and .path == $token and .uid == 984 and .gid == 980) and
  .funded.producerNative.health.ok == true and .funded.producerNative.status.executionMode == "paper" and
  .funded.producerNative.status.intentPublisher == {configured:true,prepared:true,pointerOnlyPreflight:false} and
  (.funded.producerConfigEnvBindingSha256 | test("^sha256:[0-9a-f]{64}$")) and
  (.funded.producerTokenMountBindingSha256 | test("^sha256:[0-9a-f]{64}$")) and
  .funded.producerNative.status.recorder.queued == 0
' "$pre" >/dev/null
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
