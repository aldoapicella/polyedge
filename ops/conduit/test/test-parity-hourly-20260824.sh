#!/bin/sh
set -eu

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT HUP INT TERM
collector=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-parity-hourly-20260824
reboot_attestor=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-reboot-attestation-20260824
collector_sha=sha256:$(sha256sum "$collector" | awk '{print $1}')
validator_sha=sha256:$(sha256sum "$reboot_attestor" | awk '{print $1}')
uid=$(id -u)
gid=$(id -g)
fake=$root/fake-bin
mkdir -p "$fake"

grep -Fq '(.[0] + .[1]) as $runs |' "$collector"
grep -Fq 'validate_evidence_artifacts() (' "$collector"
printf '%s\n%s\n' \
  '[{"recorder_instance_id":"a","recorder_first_sequence":1,"recorder_last_sequence":1}]' \
  '[{"recorder_instance_id":"a","recorder_first_sequence":2,"recorder_last_sequence":2}]' | jq -se '
  (.[0] + .[1]) as $runs |
  all(range(1; ($runs | length)); . as $index |
    $runs[$index].recorder_first_sequence == ($runs[$index - 1].recorder_last_sequence + 1))
' >/dev/null
if printf '%s\n%s\n' \
    '[{"recorder_instance_id":"a","recorder_first_sequence":1,"recorder_last_sequence":1}]' \
    '[{"recorder_instance_id":"a","recorder_first_sequence":3,"recorder_last_sequence":3}]' | jq -se '
    (.[0] + .[1]) as $runs |
    all(range(1; ($runs | length)); . as $index |
      $runs[$index].recorder_first_sequence == ($runs[$index - 1].recorder_last_sequence + 1))
  ' >/dev/null; then
  echo 'gapped cross-hour recorder sequence unexpectedly passed' >&2
  exit 1
fi

cat >"$fake/curl" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$FAKE_CALLS/curl"
out=
previous=
for arg do
  [ "$previous" != output ] || out=$arg
  previous=
  [ "$arg" != --output ] || previous=output
done
[ -n "$out" ] || exit 2
case "$*" in
  *oauth2/v2.0/token*) printf '%s\n' '{"access_token":"fixture-access-token"}' >"$out" ;;
  *management.azure.com*) cp "$FAKE_AZURE_EXECUTION" "$out" ;;
  *audit.json.attestation.json*) cp "$FAKE_AZURE_ATTESTATION" "$out" ;;
  *) cp "$FAKE_AZURE_REPORT" "$out" ;;
esac
EOF

cat >"$fake/podman" <<'EOF'
#!/bin/sh
case "$1" in
  inspect)
    case "$*" in
      *State.Status*polyedge-funded-intent-producer*) [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ] && printf '%s\n' running || exit 2 ;;
      *State.Health.Status*polyedge-funded-intent-producer*) printf '%s\n' healthy ;;
      *Config.Image*polyedge-funded-intent-producer*) printf '%s\n' "$FAKE_FUNDED_PRODUCER_IMAGE" ;;
      *Config.User*polyedge-funded-intent-producer*) printf '%s\n' "${FAKE_FUNDED_PRODUCER_USER:-984:980}" ;;
      *Config.Env*polyedge-funded-intent-producer*) jq -nc --arg run_bot "${FAKE_PRODUCER_RUN_BOT:-true}" '[
        "APP_NAME=polyedge-funded-intent-producer","RUNTIME_ROLE=profitability_shadow","EXECUTION_MODE=paper",
        "ALLOW_LIVE=false","RUN_BOT_ON_STARTUP="+$run_bot,"STRATEGY_INTENT_OPERATOR_DIRECT=true",
        "PUBLISH_STRATEGY_CANARY_INTENTS=true",
        "STRATEGY_CANARY_INTENT_PREFIX=reports/research/venue-probe/control/strategy-canary/intents",
        "STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION=conservative-execution-prior-v1",
        "STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI=azure://stpolyedge6urdjr5nmwx7w/polyedge-research/reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json",
        "STRATEGY_CANARY_EXECUTION_MODEL_SHA256=sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4",
        "STRATEGY_INTENT_TARGET_ORDER_NOTIONAL=10.5","STRATEGY_INTENT_MAX_ORDER_NOTIONAL=10.5",
        "STRATEGY_INTENT_MIN_SECONDS_TO_EXPIRY=360","STRATEGY_INTENT_MAX_SECONDS_TO_EXPIRY=900",
        "TARGET_ASSET=BTC","TARGET_ASSET_NAME=Bitcoin","TARGET_HORIZON=15m","TARGET_BINANCE_SYMBOL=btcusdt",
        "TARGET_CHAINLINK_SYMBOL=btc/usd","TARGET_COINBASE_PRODUCT_ID=BTC-USD",
        "AZURE_TENANT_ID=9767f0dc-e83f-4cc1-94e1-0d5f9d287d32",
        "AZURE_CLIENT_ID=54f0136b-5e72-4ad1-b23e-cb1269d356c1",
        "AZURE_FEDERATED_TOKEN_FILE=/run/credentials/azure-federated-token",
        "AZURE_TOKEN_CREDENTIALS=WorkloadIdentityCredential","AZURE_STORAGE_ACCOUNT_NAME=stpolyedge6urdjr5nmwx7w",
        "AZURE_STORAGE_CONTAINER_NAME=polyedge-shadow-events","AZURE_STORAGE_TABLE_NAME=ShadowBotEventIndex",
        "AZURE_CHART_TABLE_NAME=ShadowBotChartSeries","AZURE_MARKET_TABLE_NAME=ShadowBotMarketCatalog",
        "AZURE_FUNDED_STORAGE_CONTAINER_NAME=polyedge-funded-evidence","AZURE_MODEL_STORAGE_CONTAINER_NAME=polyedge-models",
        "FUNDED_DIRECT_SERVICE_BUS_ENABLED=true","FUNDED_DIRECT_SERVICE_BUS_NAMESPACE=sb-polyedge-funded-cl-6urdjr5nmwx7w",
        "FUNDED_DIRECT_SERVICE_BUS_QUEUE=funded-dynamic-quote-intents"]' ;;
      *Mounts*polyedge-funded-intent-producer*) jq -nc --arg source "$FAKE_PRODUCER_MOUNT_SOURCE" \
        --argjson rw "${FAKE_PRODUCER_MOUNT_RW:-false}" '[{Source:$source,Destination:"/run/credentials",RW:$rw}]' ;;
      *Id*polyedge-funded-intent-producer*) printf '%064d\n' 4 ;;
      *State.Status*polyedge-funded-signer*) [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ] && printf '%s\n' running || exit 2 ;;
      *Config.Image*polyedge-funded-signer*) printf '%s\n' "$FAKE_FUNDED_IMAGE" ;;
      *Config.User*polyedge-funded-signer*) printf '%s\n' "$FAKE_FUNDED_UID:$FAKE_FUNDED_GID" ;;
      *Config.Env*polyedge-funded-signer*) jq -nc \
        --arg session "${FAKE_FUNDED_SESSION:-dynamic-quote-funded-2026-08-13-v10}" \
        --arg manifest "${FAKE_FUNDED_SESSION_SHA:-sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}" \
        --arg config "${FAKE_FUNDED_CONFIG_SHA:-sha256:9999999999999999999999999999999999999999999999999999999999999999}" \
        '["VENUE_PROBE_FUNDED_CAMPAIGN_ID="+$session,"FUNDED_DIRECT_SESSION_MANIFEST_SHA256="+$manifest,
          "STRATEGY_CANARY_CANDIDATE_CONFIG_HASH="+$config]' ;;
      *State.Health.Status*) printf '%s\n' healthy ;;
      *Config.Image*) printf '%s\n' "$FAKE_OCI_IMAGE" ;;
      *ImageDigest*) printf '%s\n' "$FAKE_OCI_IMAGE_DIGEST" ;;
      *Id*) printf '%s\n' eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ;;
      *) exit 2 ;;
    esac
    ;;
  image)
    [ "$2" = inspect ] || exit 2
    case "$*" in *org.opencontainers.image.revision*) printf '%s\n' 6666666666666666666666666666666666666666 ;; *) exit 2 ;; esac
    ;;
  exec)
    printf 'HTTP/1.0 200 OK\r\n\r\n'
    case "$*" in
      *polyedge-funded-intent-producer*'/api/v1/health'*) cat "$FAKE_PRODUCER_HEALTH"; exit ;;
      *polyedge-funded-intent-producer*'/api/v1/status'*) cat "$FAKE_PRODUCER_STATUS"; exit ;;
    esac
    attempt_file=$FAKE_CALLS/api-status-attempts
    attempt=0
    [ ! -f "$attempt_file" ] || attempt=$(cat "$attempt_file")
    attempt=$((attempt + 1))
    printf '%s\n' "$attempt" >"$attempt_file"
    if [ "$attempt" -le "${FAKE_API_BUSY_ATTEMPTS:-0}" ]; then
      jq '.recorder_status.error_count=null | .recorder_status.dropped_count=null | .recorder_metrics.queued=1' \
        "$FAKE_API_STATUS"
    else
      cat "$FAKE_API_STATUS"
    fi
    ;;
  run)
    printf '%s\n' "$*" >>"$FAKE_CALLS/podman"
    execution=${POLYEDGE_GENERATOR_EXECUTION_ID:-}
    [ -n "$execution" ] || exit 2
    evidence_dir=
    previous=
    for arg do
      if [ "$previous" = volume ]; then
        case "$arg" in *:/evidence:rw,Z) evidence_dir=${arg%:/evidence:rw,Z} ;; esac
      fi
      previous=
      [ "$arg" != -v ] || previous=volume
    done
    [ -n "$evidence_dir" ] || exit 2
    jq --arg execution "$execution" '.generator_provenance.execution_id=$execution' \
      "$FAKE_SAME_REPORT" >"$FAKE_CALLS/container-audit.json"
    : >"$FAKE_CALLS/container-audit.md"
    cp "$FAKE_CALLS/container-audit.json" "$evidence_dir/audit.json"
    cp "$FAKE_CALLS/container-audit.md" "$evidence_dir/audit.md"
    ;;
  cp)
    shift
    [ "${1:-}" != --archive=false ] || shift
    source=$1 destination=$2
    case "$source" in
      *:/evidence/audit.json) cp "$FAKE_CALLS/container-audit.json" "$destination" ;;
      *:/evidence/audit.md) cp "$FAKE_CALLS/container-audit.md" "$destination" ;;
      *) exit 2 ;;
    esac
    ;;
  rm) printf '%s\n' "$*" >>"$FAKE_CALLS/podman-rm" ;;
  *) exit 2 ;;
esac
EOF

cat >"$fake/systemctl" <<'EOF'
#!/bin/sh
case "$1" in
  is-active)
    unit=${3:-${2:-}}
    case "$unit" in
      polyedge-job@shadow-qset.service) [ "${FAKE_QSET_ACTIVE:-0}" = 1 ] && exit 0 || exit 3 ;;
      polyedge-funded-signer.service|polyedge-federated-token@funded-signer.timer|polyedge-funded-intent-producer.service|polyedge-federated-token@funded-intent-producer.timer)
        [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ] && exit 0 || exit 3
        ;;
      *) exit 0 ;;
    esac
    ;;
  is-enabled)
    case "$2" in
      polyedge-shadow-qset.timer) printf '%s\n' not-found; exit 1 ;;
      polyedge-funded-signer.service)
        if [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ]; then printf '%s\n' "${FAKE_FUNDED_ENABLEMENT:-enabled}"; else printf '%s\n' masked; fi
        ;;
      polyedge-federated-token@funded-signer.timer|polyedge-federated-token@funded-intent-producer.timer)
        if [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ]; then printf '%s\n' enabled; else printf '%s\n' masked; fi
        ;;
      polyedge-funded-intent-producer.service)
        if [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ]; then printf '%s\n' "${FAKE_FUNDED_PRODUCER_ENABLEMENT:-enabled}"; else printf '%s\n' masked; fi
        ;;
      *) exit 2 ;;
    esac
    ;;
  show)
    case "${2:-}" in
      multi-user.target) printf '%s\n' "${FAKE_FUNDED_TARGET_WANTS:-polyedge-funded-signer.service polyedge-funded-intent-producer.service}" ;;
      polyedge-funded-intent-producer.service) printf '%s\n' bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb ;;
      *) printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ;;
    esac
    ;;
  *) exit 2 ;;
esac
EOF

cat >"$fake/stat" <<'EOF'
#!/bin/sh
last=
for arg do last=$arg; done
case "$last" in
  */funded-producer-azure-federated-token)
    set -- $(/usr/bin/stat -c '%a %h %s %Y' "$last")
    printf '984 980 %s %s %s %s\n' "$1" "$2" "$3" "$4"
    ;;
  *) exec /usr/bin/stat "$@" ;;
esac
EOF
cat >"$fake/sha256sum" <<'EOF'
#!/bin/sh
case "$1" in
  */funded-intent-producer.env) printf '%s  %s\n' 56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b "$1" ;;
  *) exec /usr/bin/sha256sum "$@" ;;
esac
EOF
cat >"$fake/journalctl" <<'EOF'
#!/bin/sh
[ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ] || exit 0
start=$(date -u -d '2026-08-09T15:00:00Z' +%s)
case "$*" in
  *polyedge-federated-token@funded-intent-producer.service*)
    i=0
    while [ "$i" -lt 30 ]; do
      if [ "${FAKE_PRODUCER_TOKEN_GAP:-0}" = 1 ] && [ "$i" -eq 15 ]; then i=$((i + 1)); continue; fi
      timestamp=$(( (start + 60 + i * 120) * 1000000 ))
      message='Finished polyedge-federated-token@funded-intent-producer.service - Rotate the funded intent producer PolyEdge JWT-SVID token file.'
      jq -nc --arg timestamp "$timestamp" --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,MESSAGE:$message}'
      i=$((i + 1))
    done
    [ "${FAKE_PRODUCER_TOKEN_FAILURE:-0}" != 1 ] || jq -nc --arg timestamp "$(( (start + 1800) * 1000000 ))" \
      '{__REALTIME_TIMESTAMP:$timestamp,MESSAGE:"Failed to start producer token refresh"}'
    ;;
  *polyedge-funded-intent-producer.service*)
    i=0
    while [ "$i" -lt 60 ]; do
      timestamp=$(( (start + 30 + i * 60) * 1000000 ))
      queued=$((i % 2))
      failed=0
      dropped=0
      errors=0
      if [ "$i" -eq 15 ]; then dropped=null; errors=null; fi
      if [ "${FAKE_PRODUCER_RECORDER_BACKLOG:-0}" = 1 ] && [ "$i" -eq 15 ]; then queued=2; dropped=0; errors=0; fi
      if [ "${FAKE_PRODUCER_UNBOUND_NULL_STATUS:-0}" = 1 ] && [ "$i" -eq 15 ]; then queued=0; dropped=null; errors=null; fi
      if [ "${FAKE_PRODUCER_RECORDER_FAILURE:-0}" = 1 ] && [ "$i" -eq 15 ]; then failed=1; fi
      event=$(jq -nc --argjson queued "$queued" --argjson failed "$failed" --argjson dropped "$dropped" --argjson errors "$errors" \
        '{event:"runtime_health",execution_mode:"paper",runtime_loop:"running",feeds:"running",
          recorder_queued:$queued,recorder_failed_total:$failed,recorder_unrecovered_durable_events:0,
          recorder_flush_unrecovered:false,recorder_dropped_count:$dropped,recorder_error_count:$errors}')
      message=$(printf '\033[2m%s\033[0m' "$event" | jq -R 'explode')
      jq -nc --arg timestamp "$timestamp" --argjson message "$message" \
        --arg invocation bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
        --arg container 0000000000000000000000000000000000000000000000000000000000000004 \
        '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
      i=$((i + 1))
    done
    jq -nc --arg timestamp "$(( (start + 90) * 1000000 ))" \
      --arg invocation bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
      --arg container 0000000000000000000000000000000000000000000000000000000000000004 \
      '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:"funded market warmup sent"}'
    [ "${FAKE_PRODUCER_PUBLISH_FAILURE:-0}" != 1 ] || jq -nc --arg timestamp "$(( (start + 120) * 1000000 ))" \
      --arg invocation bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
      --arg container 0000000000000000000000000000000000000000000000000000000000000004 \
      '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:"funded market warmup not sent"}'
    [ "${FAKE_PRODUCER_INTENT_FAILURE:-0}" != 1 ] || jq -nc --arg timestamp "$(( (start + 150) * 1000000 ))" \
      --arg invocation bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
      --arg container 0000000000000000000000000000000000000000000000000000000000000004 \
      '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:"execution intent publication failed"}'
    [ "${FAKE_JOURNAL_PARTIAL_FAILURE:-0}" != 1 ] || exit 9
    ;;
  *polyedge-federated-token@funded-signer.service*)
    i=0
    while [ "$i" -lt 30 ]; do
      if [ "${FAKE_FUNDED_TOKEN_GAP:-0}" = 1 ] && [ "$i" -eq 15 ]; then i=$((i + 1)); continue; fi
      timestamp=$(( (start + 60 + i * 120) * 1000000 ))
      message='Finished polyedge-federated-token@funded-signer.service - Rotate the funded-signer PolyEdge JWT-SVID token file.'
      jq -nc --arg timestamp "$timestamp" --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,MESSAGE:$message}'
      i=$((i + 1))
    done
    [ "${FAKE_FUNDED_TOKEN_FAILURE:-0}" != 1 ] || jq -nc --arg timestamp "$(( (start + 1800) * 1000000 ))" \
      '{__REALTIME_TIMESTAMP:$timestamp,MESSAGE:"Failed to start funded token refresh"}'
    ;;
  *)
    i=0
    while [ "$i" -lt 60 ]; do
      if [ "${FAKE_FUNDED_GAP:-0}" = 1 ] && [ "$i" -eq 30 ]; then i=$((i + 1)); continue; fi
      if [ "${FAKE_FUNDED_BURST:-0}" = 1 ]; then second=$((start + 30 + i)); else second=$((start + 30 + i * 60)); fi
      timestamp=$((second * 1000000))
      invocation=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      [ "${FAKE_FUNDED_RESTART:-0}" != 1 ] || [ "$i" -ne 30 ] || invocation=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
      busy=false
      snapshot_age=100
      open_orders=0
      unresolved_positions=0
      unresolved_reservations=0
      if [ "$i" -eq 30 ] && [ "${FAKE_FUNDED_MID_HOUR_ACTIVE:-0}" = 1 ]; then
        busy=true
        open_orders=1
        unresolved_positions=1
        unresolved_reservations=1
      fi
      if [ "$i" -eq 59 ]; then
        [ "${FAKE_FUNDED_BUSY:-0}" != 1 ] || busy=true
        [ "${FAKE_FUNDED_STALE_SNAPSHOT:-0}" != 1 ] || snapshot_age=651
        [ "${FAKE_FUNDED_OPEN_ORDER:-0}" != 1 ] || open_orders=1
        [ "${FAKE_FUNDED_UNRESOLVED_POSITION:-0}" != 1 ] || unresolved_positions=1
        [ "${FAKE_FUNDED_UNRESOLVED_RESERVATION:-0}" != 1 ] || unresolved_reservations=1
      fi
      processed=1
      [ "$i" -ne 0 ] || processed=0
      [ "${FAKE_FUNDED_NO_PROCESSED_DELTA:-0}" != 1 ] || processed=1
      failed_attempts=0
      [ "${FAKE_FUNDED_FAILED_ATTEMPTS:-0}" != 1 ] || failed_attempts=1
      message=$(jq -nc --argjson busy "$busy" --argjson snapshot_age "$snapshot_age" \
        --argjson processed "$processed" --argjson failed_attempts "$failed_attempts" \
        --argjson open_orders "$open_orders" --argjson unresolved_positions "$unresolved_positions" \
        --argjson unresolved_reservations "$unresolved_reservations" '
        {schema:"polyedge.funded_direct_service.v2",status:"persistent_service_heartbeat",
        processed_messages:$processed,failed_messages:0,failed_attempts:$failed_attempts,consecutive_latency_breaches:0,redemption_failures:0,
        executor:{user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,
          user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,busy:$busy,
          safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:$snapshot_age,
          safety_snapshot_open_order_count:$open_orders,
          safety_snapshot_unresolved_position_count:$unresolved_positions,
          safety_snapshot_unresolved_risk_reservation_count:$unresolved_reservations,
          safety_snapshot_cache_error:null,risk_reservation_index_ready:true}}')
      jq -nc --arg timestamp "$timestamp" --arg invocation "$invocation" \
        --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
      i=$((i + 1))
    done
    if [ "${FAKE_FUNDED_NO_MARKET_WARMED:-0}" != 1 ]; then
      message=$(jq -nc '{schema:"polyedge.funded_direct_service.v2",status:"market_warmed",market_id:"1",token_id:"2"}')
      jq -nc --arg timestamp "$(( (start + 90) * 1000000 ))" --arg invocation aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --arg message "$message" '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
    fi
    if [ "${FAKE_FUNDED_PERSISTENT_MESSAGE_FAILED_CLOSED:-0}" = 1 ]; then
      message=$(jq -nc '{schema:"polyedge.funded_direct_service.v2",status:"persistent_message_failed_closed"}')
      jq -nc --arg timestamp "$(( (start + 1800) * 1000000 ))" --arg invocation aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --arg message "$message" '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
    fi
    if [ "${FAKE_FUNDED_ALERT:-0}" = 1 ]; then
      message=$(jq -nc '{schema:"polyedge.funded_direct_alert.v1",status:"websocket_gap_or_reconciliation_required"}')
      jq -nc --arg timestamp "$(( (start + 1800) * 1000000 ))" --arg invocation aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
        --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
    fi
    if [ "${FAKE_FUNDED_FAILED_CLOSED:-0}" = 1 ]; then
      message=$(jq -nc '{schema:"polyedge.funded_direct_service.v1",status:"failed_closed"}')
      jq -nc --arg timestamp "$(( (start + 1800) * 1000000 ))" --arg invocation aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
        --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
    fi
    ;;
esac
EOF

cat >"$fake/runuser" <<'EOF'
#!/bin/sh
[ "$1" = -u ] && [ "$2" = ubuntu ] && [ "$3" = -- ] || exit 2
shift 3
exec "$@"
EOF

cat >"$fake/az" <<'EOF'
#!/bin/sh
case "$*" in
  "servicebus queue show --subscription 11111111-1111-1111-1111-111111111111 --resource-group rg-polyedge-dev --namespace-name sb-polyedge-funded-cl-6urdjr5nmwx7w --name funded-dynamic-quote-intents --only-show-errors -o json"|\
  "servicebus queue show --subscription 73783c0c-5a53-4f9b-b244-6f64e813814c --resource-group rg-polyedge-dev --namespace-name sb-polyedge-funded-cl-6urdjr5nmwx7w --name funded-dynamic-quote-intents --only-show-errors -o json") ;;
  *) exit 2 ;;
esac
jq -nc --arg status "${FAKE_SERVICE_BUS_STATUS:-Active}" \
  --argjson active "${FAKE_SERVICE_BUS_ACTIVE:-0}" --argjson scheduled "${FAKE_SERVICE_BUS_SCHEDULED:-0}" \
  --argjson dlq "${FAKE_SERVICE_BUS_DLQ:-1311}" \
  '{status:$status,countDetails:{activeMessageCount:$active,scheduledMessageCount:$scheduled,deadLetterMessageCount:$dlq}}'
EOF

cat >"$fake/mountpoint" <<'EOF'
#!/bin/sh
[ "$FAKE_MOUNTPOINT_OK" = 1 ]
EOF

cat >"$fake/df" <<'EOF'
#!/bin/sh
printf '%s\n' 'Filesystem 1-blocks Used Available Capacity Mounted on'
printf 'fixture 100000000000 1 %s 1%% /\n' "$FAKE_DF_AVAILABLE"
EOF

cat >"$fake/timeout" <<'EOF'
#!/bin/sh
[ "$1" != --preserve-status ] || shift
shift
exec "$@"
EOF
chmod 0755 "$fake"/*

azure_image=crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-rust-research@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
oci_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
oci_image_digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
azure_execution=polyedge-hourly-quality-job-fixture
azure_args='TARGET=$(date -u -d "1 hour ago" +%Y/%m/%d/%H); DAY=${TARGET%/*}; HOUR=${TARGET##*/}; OUT="reports/research/hourly/$DAY/$HOUR/audit.json"; /bin/sh /app/research/run_compact_report_job.sh polyedge_hourly_quality "$OUT" polyedge-rs research audit --input "azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/events/$DAY/$HOUR/?prefetch_blobs=8" --out "$OUT" --markdown "reports/research/hourly/$DAY/$HOUR/audit.md" --exclude-file "data_quality/exclusion_windows.yaml"'

report() {
  kind=$1 output=$2
  marker=azure
  platform=oci_podman
  image=$oci_image
  execution=$kind-fixture
  job=null
  if [ "$kind" = azure ]; then
    platform=azure_container_apps_job
    image=$azure_image
    execution=$azure_execution
    job=polyedge-hourly-quality-job
  elif [ "$kind" = mismatch ]; then
    marker=mismatch
  fi
  jq -n --arg marker "$marker" --arg platform "$platform" --arg image "$image" \
    --arg execution "$execution" --argjson job "$(printf '%s' "$job" | jq -R 'if . == "null" then null else . end')" '{
    git_sha:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    generated_at:"2026-08-09T16:13:00Z",
    input_path:"azure://stpolyedge6urdjr5nmwx7w/bot-events/events/2026/08/09/15/?prefetch_blobs=8",
    generator_provenance:{schema_version:1,platform:$platform,image:$image,execution_id:$execution,job_name:$job},
    result:{
      total_events:123,fatal_data_quality_issues:[],fixture_marker:$marker,
      runtime_provenance:{
        observations:60,valid_observations:60,invalid_observations:0,distinct_identity_count:1,
        first_timestamp:"2026-08-09T15:00:01Z",last_timestamp:"2026-08-09T15:59:59Z",max_gap_ms:60000,
        identities:[{
          app_name:"polyedge",runtime_role:"primary",execution_mode:"paper",allow_live:false,enable_taker_orders:false,
          allow_emergency_account_cancel:false,research_only:true,shadow_only:false,backend_impl:"rust",
          storage_container:"bot-events",event_blob_prefix:"events",adaptive_regime_enabled:false,
          adaptive_regime_mode:"paper_only",paper_maker_fill_policy:"touch_after_quote_was_live",
          publish_strategy_canary_intents:false,candidate:null,
          git_sha:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          runtime_config_hash:"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          decision_config_schema:"polyedge.decision_config.v1",
          decision_config_sha256:"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          decision_pipeline_schema:"polyedge.strategy_decision_batch.v4",
          decision_pipeline_parity_scope:"full_decision_pipeline_recomputation"
        }]
      },
      strategy_batches:1,strategy_batch_replayed:1,strategy_batch_matches:1,
      strategy_batch_invalid:0,strategy_batch_contract_invalid:0,
      strategy_batch_contract_invalid_reasons:{},strategy_batch_missing_independent_start:0,
      strategy_batch_ineligible:0,strategy_batch_conflicts:0,strategy_binding_ineligible:0,
      strategy_binding_conflicts:0,unbound_strategy_decisions:0,decision_application_invalid:0,
      decision_application_conflicts:0,orphan_decision_applications:0,
      decision_pipeline_replay_rate:1,decision_output_binding_rate:1,decision_parity_rate:1
    },
    warnings:[]
  }' >"$output"
}

attest() {
  report_file=$1 output=$2
  jq --arg report_sha256 "sha256:$(sha256sum "$report_file" | awk '{print $1}')" \
    --arg image_digest "$oci_image_digest" \
    '.generator_provenance + {report_sha256:$report_sha256} +
      (if .generator_provenance.platform == "oci_podman" then
        {image_digest:$image_digest,container_id:"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
       else {} end)' "$report_file" >"$output"
  chmod 0640 "$output"
}

fixture() {
  case_root=$1
  target=${2:-2026-08-09T15:00:00Z}
  window=${3:-2026-08-09T14:10:00Z}
  manifest_schema=${4:-3}
  mkdir -p "$case_root/calls" "$case_root/run" "$case_root/token" "$case_root/ring/parity" \
    "$case_root/ring/segments/2026/08/09/15" "$case_root/ring/archive/2026/08/09/15" \
    "$case_root/reports/2026/08/09/15"
  chmod 0700 "$case_root/run" "$case_root/token"
  chmod 0750 "$case_root/ring/parity"
  : >"$case_root/token/azure-federated-token"
  : >"$case_root/token/funded-azure-federated-token"
  printf "%s\n" fixture-jwt-producer >"$case_root/token/funded-producer-azure-federated-token"
  chmod 0600 "$case_root/token/azure-federated-token"
  chmod 0600 "$case_root/token/funded-azure-federated-token" "$case_root/token/funded-producer-azure-federated-token"
  printf '%s\n' 'fixture producer env; contents never emitted' >"$case_root/funded-intent-producer.env"
  chmod 0600 "$case_root/funded-intent-producer.env"
  jq -n --arg start "$window" '{
    schemaVersion:1,status:"in_progress",azureAuthoritative:true,azureDeletionAllowed:false,
    windowStartUtc:$start,acceptedCleanLiveHours:0,acceptedHourlyEvidence:[],
    completedDailyCycles:0,acceptedDailyEvidence:[],rebootRecoveryPassed:false,
    shadowQsetEnabled:false,fundedSignerEnabled:false
  }' >"$case_root/ring/parity/ledger.json"
  chmod 0640 "$case_root/ring/parity/ledger.json"
  jq -n '{capacity_ok:true,free_ok:true,upload_fresh:true,unsealed_closed_count:0,unuploaded_count:0}' >"$case_root/ring/status.json"
  chmod 0640 "$case_root/ring/status.json"
  fixture_now=$(date -u -d '2026-08-09T16:20:00Z' +%s)
  touch -d "@$fixture_now" "$case_root/ring/status.json"
  touch -d "@$fixture_now" "$case_root/token/funded-azure-federated-token" "$case_root/token/funded-producer-azure-federated-token"
  cat >"$case_root/hourly.env" <<'EOF'
POLYEDGE_RESEARCH_IMAGE=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
POLYEDGE_DISABLE_RESEARCH_ARTIFACT_PUBLISH=true
AZURE_STORAGE_ACCOUNT_NAME=stpolyedge6urdjr5nmwx7w
AZURE_STORAGE_CONTAINER_NAME=bot-events
AZURE_TENANT_ID=tenant
AZURE_CLIENT_ID=client
AZURE_FEDERATED_TOKEN_FILE=/run/credentials/azure-federated-token
EOF
  chmod 0600 "$case_root/hourly.env"
  cat >"$case_root/parity.env" <<EOF
POLYEDGE_PARITY_WINDOW_START_UTC=$window
POLYEDGE_PARITY_LEDGER=$case_root/ring/parity/ledger.json
POLYEDGE_PARITY_HOURLY_ENV=$case_root/hourly.env
POLYEDGE_PARITY_RING_ROOT=$case_root/ring
POLYEDGE_PARITY_ROOT=$case_root/ring/parity
POLYEDGE_PARITY_SEGMENTS_ROOT=$case_root/ring/segments
POLYEDGE_PARITY_ARCHIVE_ROOT=$case_root/ring/archive
POLYEDGE_PARITY_REPORT_ROOT=$case_root/reports
POLYEDGE_PARITY_RING_STATUS=$case_root/ring/status.json
POLYEDGE_PARITY_BOOT_ROOT=$case_root
POLYEDGE_PARITY_PAUSE_FILE=$case_root/run/image-pulls-paused
POLYEDGE_PARITY_TOKEN_FILE=$case_root/token/azure-federated-token
POLYEDGE_PARITY_RUNTIME_DIR=$case_root/run
POLYEDGE_PARITY_LOCK_FILE=$case_root/run/ledger.lock
POLYEDGE_PARITY_TARGET_HOUR_UTC=$target
POLYEDGE_PARITY_NOW_EPOCH=$fixture_now
POLYEDGE_PARITY_STATUS_NOW_EPOCH=$fixture_now
POLYEDGE_PARITY_TOKEN_UID=$uid
POLYEDGE_PARITY_TOKEN_GID=$gid
POLYEDGE_PARITY_EXPECTED_GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
POLYEDGE_PARITY_EXPECTED_AZURE_RESEARCH_IMAGE=$azure_image
POLYEDGE_PARITY_EXPECTED_OCI_RESEARCH_IMAGE=$oci_image
AZURE_SUBSCRIPTION_ID=11111111-1111-1111-1111-111111111111
POLYEDGE_PARITY_AZURE_RESOURCE_GROUP=rg-polyedge-dev
POLYEDGE_PARITY_AZURE_JOB_NAME=polyedge-hourly-quality-job
EOF
  chmod 0640 "$case_root/parity.env"
  report azure "$case_root/azure.json"
  attest "$case_root/azure.json" "$case_root/azure.json.attestation.json"
  report oci "$case_root/reports/2026/08/09/15/audit.json"
  jq '.input_path="/input/events/2026/08/09/15/"' "$case_root/reports/2026/08/09/15/audit.json" >"$case_root/oci.tmp"
  mv "$case_root/oci.tmp" "$case_root/reports/2026/08/09/15/audit.json"
  attest "$case_root/reports/2026/08/09/15/audit.json" "$case_root/reports/2026/08/09/15/audit.json.attestation.json"
  report same "$case_root/same.json"
  jq -n --arg execution "$azure_execution" --arg image "$azure_image" --arg args "$azure_args" '{
    name:$execution,properties:{status:"Succeeded",startTime:"2026-08-09T16:10:00+00:00",endTime:"2026-08-09T16:14:00+00:00",
      template:{containers:[{name:"research-job",image:$image,command:["/bin/sh","-lc"],args:[$args],env:[
        {name:"ALLOW_LIVE",value:"false"},{name:"EXECUTION_MODE",value:"paper"},
        {name:"RUN_BOT_ON_STARTUP",value:"false"},{name:"ENABLE_TAKER_ORDERS",value:"false"},
        {name:"ALLOW_EMERGENCY_ACCOUNT_CANCEL",value:"false"},{name:"REQUIRE_API_AUTH",value:"true"},
        {name:"API_BEARER_TOKEN",secretRef:"cappjob-polyedge-hourly-quality-job"},
        {name:"POLYEDGE_GENERATOR_PLATFORM",value:"azure_container_apps_job"},
        {name:"POLYEDGE_GENERATOR_IMAGE",value:$image}
      ]}]}}
  }' >"$case_root/azure-execution.json"
  jq -n '{
    task_health:{api:"ok",runtime_loop:"running",feeds:"running"},
    feed_status:{
      Discovery:{status:"ok",updated_at:"2026-08-09T16:19:59Z"},
      PolymarketClobMarket:{status:"ok",updated_at:"2026-08-09T16:19:59.123456789Z"},
      PolymarketRtdsChainlink:{status:"ok",updated_at:"2026-08-09T16:19:59Z"},
      PolymarketRtdsBinance:{status:"ok",updated_at:"2026-08-09T16:19:59Z"}
    },
    drop_counts:{},recorder_status:{error_count:0,dropped_count:0},
    recorder_metrics:{recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",last_assigned_sequence:60,queued:0,enqueued_total:60,persisted_total:60,failed_total:0,unrecovered_durable_events:0,flush_unrecovered:false}
  }' >"$case_root/api-status.json"
  jq -n '{ok:true,backend_impl:"rust",runtime_role:"profitability_shadow",shadow_only:true,
    runtime_active:true,execution_mode:"paper",kill_switch:false}' >"$case_root/producer-health.json"
  jq '. + {app:"polyedge-funded-intent-producer",backend_impl:"rust",runtime_role:"profitability_shadow",
    shadow_only:true,execution_mode:"paper",
    intent_publisher:{configured:true,prepared:true,pointer_only_preflight:false}}' \
    "$case_root/api-status.json" >"$case_root/producer-status.json"

  i=0
  start=$(date -u -d '2026-08-09T15:00:00Z' +%s)
  while [ "$i" -lt 6 ]; do
    epoch=$((start + i * 600))
    first=$((i * 10 + 1))
    last=$((first + 9))
    source=$case_root/ring/segments/2026/08/09/15/$epoch.jsonl
    gzip_file=$case_root/ring/archive/2026/08/09/15/$epoch.jsonl.gz
    manifest=$gzip_file.manifest.json
    receipt=$manifest.uploaded.json
    minute=0
    while [ "$minute" -lt 10 ]; do
      seq=$((first + minute))
      observed=$(date -u -d "@$((epoch + minute * 60 + 1))" +%Y-%m-%dT%H:%M:%SZ)
      jq -nc --arg observed "$observed" --argjson seq "$seq" '{
        event_type:"runtime_provenance",recorded_ts:$observed,
        payload:{essential_feed_health:{summary:"running",feed_status:{
          Discovery:{status:"ok",updated_at:$observed},
          PolymarketClobMarket:{status:"ok",updated_at:$observed},
          PolymarketRtdsChainlink:{status:"ok",updated_at:$observed},
          PolymarketRtdsBinance:{status:"ok",updated_at:$observed}
        }}},recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_sequence:$seq
      }' >>"$source"
      minute=$((minute + 1))
    done
    gzip -1 -n -c "$source" >"$gzip_file"
    source_sha=sha256:$(sha256sum "$source" | awk '{print $1}')
    gzip_sha=sha256:$(sha256sum "$gzip_file" | awk '{print $1}')
    jq -n --arg segment "segments/2026/08/09/15/$epoch.jsonl" \
      --arg archive "archive/2026/08/09/15/$epoch.jsonl.gz" --arg blob "events-oci-hot7-v1/2026/08/09/15/$epoch.jsonl.gz" \
      --arg source_sha "$source_sha" --arg gzip_sha "$gzip_sha" --argjson start "$epoch" --argjson first "$first" --argjson last "$last" \
      --argjson schema "$manifest_schema" \
      '{schema_version:$schema,lines:10,segment_path:$segment,archive_path:$archive,blob_name:$blob,compression:"gzip",sha256:$gzip_sha,source_sha256:$source_sha,segment_start_epoch:$start,segment_end_epoch:($start+600)} +
       (if $schema == 4 then {recorder_runs:[{recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_first_sequence:$first,recorder_last_sequence:$last,recorder_event_count:10}]}
        else {recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_first_sequence:$first,recorder_last_sequence:$last,recorder_event_count:10} end)' >"$manifest"
    manifest_sha=sha256:$(sha256sum "$manifest" | awk '{print $1}')
    jq -n --arg blob "events-oci-hot7-v1/2026/08/09/15/$epoch.jsonl.gz" --arg sha "$manifest_sha" \
      '{schema_version:1,blob_name:$blob,manifest_blob_name:($blob+".manifest.json"),manifest_sha256:$sha,verified_ts:"2026-08-09T16:12:00Z"}' >"$receipt"
    i=$((i + 1))
  done
}

activate_funded_fixture() {
  case_root=$1
  funded_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
  funded_revision=7777777777777777777777777777777777777777
  funded_dlq=1311
  activation=$case_root/ring/parity/activation
  recovery=$case_root/ring/parity/funded-recovery/20260824T052321Z-acknowledged-evicted-no-fill.json
  settlement=$case_root/ring/parity/funded-recovery/20260824T060435Z-settlement-loss-dlq-1311.json
  rollout=$activation/20260824T203004Z-post-restart-redemption-attestation.json
  install -d -m 0750 "$activation"
  jq -n --arg image "$funded_image" --arg revision "$funded_revision" --argjson dlq "$funded_dlq" \
    --arg producer_image "ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc" '{
      schema:"polyedge.funded_signer_post_redemption_attestation.v1",status:"attested",createdAtUtc:"2026-08-24T18:30:00Z",
      helperSha256:("sha256:" + ("1" * 64)),authorizedDeadLetterBaseline:$dlq,
      redemption:{transactionHash:("0x" + ("2" * 64)),settlementBlob:"fixture-settlement"},
      evidence:{liveSummary:{path:"fixture-live",sha256:("sha256:" + ("3" * 64))},
        followUpDryRun:{path:"fixture-dry",sha256:("sha256:" + ("4" * 64))},
        internalSettlement:{path:"fixture-settlement",sha256:("sha256:" + ("5" * 64))}},
      runtime:{signer:{invocationId:("a" * 32),containerId:("e" * 64),restartCount:0,image:$image,revision:$revision,user:"986:982"},
        producer:{invocationId:("b" * 32),containerId:(("0" * 63) + "4"),restartCount:0,image:$producer_image,
          revision:("6" * 40),user:"984:980",status:"running",health:"healthy"}},
      heartbeat:{capturedAtEpoch:1787596150,processedMessages:1,failedMessages:0,failedAttempts:0,
        executor:{busy:false,user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,
          user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,
          safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:100,safety_snapshot_open_order_count:0,
          safety_snapshot_unresolved_position_count:0,safety_snapshot_unresolved_risk_reservation_count:0,
          safety_snapshot_cache_error:null,risk_reservation_index_ready:true}},
      queue:{before:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:$dlq},
        after:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:$dlq},deadLetterNonGrowth:true},
      servicesMutated:false,staleRecoveryReceiptsAccepted:false,parityTimerRemainsPaused:true,azureDeletionAllowed:false}' >"$rollout"
  chmod 0640 "$rollout"
  rollout_sha=sha256:$(sha256sum "$rollout" | awk '{print $1}')
  active_ledger=$case_root/ring/parity/20260809T141000Z-funded-active.json
  jq --arg user "$uid:$gid" --arg rollout "$rollout" --arg rollout_sha "$rollout_sha" \
    --arg collector_sha "$collector_sha" --arg validator_sha "$validator_sha" '.fundedSignerEnabled=true | .fundedSignerMode="active" |
    .fundedSignerImage="ghcr.io/aldoapicella/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" |
    .fundedSignerRevision="7777777777777777777777777777777777777777" |
    .fundedRolloutReceiptPath=$rollout | .fundedRolloutReceiptSha256=$rollout_sha |
    .fundedServiceBusDlqBaseline=1311 |
    .parityCollectorSha256=$collector_sha | .rebootValidatorSha256=$validator_sha |
    .fundedSignerUser=$user | .fundedSessionId="dynamic-quote-funded-2026-08-13-v10" |
    .fundedSessionManifestSha256="sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" |
    .fundedConfigSha256="sha256:9999999999999999999999999999999999999999999999999999999999999999" |
    .fundedIntentProducerEnabled=true | .fundedIntentProducerImage="ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc" |
    .fundedIntentProducerUser="984:980" | .fundedIntentProducerConfigSha256="sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b"' \
    "$case_root/ring/parity/ledger.json" >"$active_ledger"
  rm "$case_root/ring/parity/ledger.json"
  chmod 0640 "$active_ledger"
  sed -i "s#POLYEDGE_PARITY_LEDGER=.*#POLYEDGE_PARITY_LEDGER=$active_ledger#" "$case_root/parity.env"
  cat >>"$case_root/parity.env" <<EOF
POLYEDGE_PARITY_FUNDED_MODE=active
POLYEDGE_PARITY_EXPECTED_FUNDED_IMAGE=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
POLYEDGE_PARITY_FUNDED_UID=$uid
POLYEDGE_PARITY_EXPECTED_FUNDED_REVISION=$funded_revision
POLYEDGE_PARITY_FUNDED_ROLLOUT_RECEIPT=$rollout
POLYEDGE_PARITY_EXPECTED_FUNDED_ROLLOUT_RECEIPT_SHA256=$rollout_sha
POLYEDGE_PARITY_FUNDED_GID=$gid
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_ID=dynamic-quote-funded-2026-08-13-v10
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_SHA256=sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
POLYEDGE_PARITY_EXPECTED_FUNDED_CONFIG_SHA256=sha256:9999999999999999999999999999999999999999999999999999999999999999
POLYEDGE_PARITY_FUNDED_TOKEN_FILE=$case_root/token/funded-azure-federated-token
POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_IMAGE=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc
POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_CONFIG_SHA256=sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b
POLYEDGE_PARITY_EXPECTED_FUNDED_SERVICE_BUS_DLQ=1311
POLYEDGE_PARITY_EXPECTED_COLLECTOR_SHA256=$collector_sha
POLYEDGE_PARITY_EXPECTED_REBOOT_VALIDATOR_SHA256=$validator_sha
POLYEDGE_PARITY_FUNDED_PRODUCER_ENV_FILE=$case_root/funded-intent-producer.env
POLYEDGE_PARITY_FUNDED_PRODUCER_TOKEN_FILE=$case_root/token/funded-producer-azure-federated-token
EOF
  cat >>"$case_root/hourly.env" <<EOF
POLYEDGE_PARITY_EXPECTED_FUNDED_REVISION=$funded_revision
POLYEDGE_PARITY_FUNDED_ROLLOUT_RECEIPT=$rollout
POLYEDGE_PARITY_EXPECTED_FUNDED_ROLLOUT_RECEIPT_SHA256=$rollout_sha
POLYEDGE_PARITY_EXPECTED_FUNDED_SERVICE_BUS_DLQ=$funded_dlq
POLYEDGE_PARITY_EXPECTED_COLLECTOR_SHA256=$collector_sha
POLYEDGE_PARITY_EXPECTED_REBOOT_VALIDATOR_SHA256=$validator_sha
EOF
}

refresh_segment() {
  source=$1
  gzip_file=$(printf '%s\n' "$source" | sed 's#/segments/#/archive/#').gz
  manifest=$gzip_file.manifest.json
  receipt=$manifest.uploaded.json
  gzip -1 -n -c "$source" >"$gzip_file"
  source_sha=sha256:$(sha256sum "$source" | awk '{print $1}')
  gzip_sha=sha256:$(sha256sum "$gzip_file" | awk '{print $1}')
  lines=$(wc -l <"$source")
  first=$(jq -r -s '.[0].recorder_sequence' "$source")
  last=$(jq -r -s '.[-1].recorder_sequence' "$source")
  jq --arg source_sha "$source_sha" --arg gzip_sha "$gzip_sha" --argjson lines "$lines" \
    --argjson first "$first" --argjson last "$last" \
    '.source_sha256=$source_sha | .sha256=$gzip_sha | .lines=$lines |
     .recorder_first_sequence=$first | .recorder_last_sequence=$last | .recorder_event_count=$lines' \
    "$manifest" >"$manifest.tmp"
  mv "$manifest.tmp" "$manifest"
  manifest_sha=sha256:$(sha256sum "$manifest" | awk '{print $1}')
  jq --arg sha "$manifest_sha" '.manifest_sha256=$sha' "$receipt" >"$receipt.tmp"
  mv "$receipt.tmp" "$receipt"
}

run_collector() {
  case_root=$1
  env PATH="$fake:$PATH" \
    POLYEDGE_PARITY_UTILITY_LOCKED=1 \
    POLYEDGE_RUNUSER_BIN="$fake/runuser" POLYEDGE_AZ_BIN="$fake/az" \
    POLYEDGE_PARITY_EXPECTED_UID="$uid" POLYEDGE_PARITY_EXPECTED_GID="$gid" \
    POLYEDGE_PARITY_COLLECTOR_BIN="${TEST_COLLECTOR_BIN:-$collector}" POLYEDGE_REBOOT_ATTESTATION_BIN="${TEST_VALIDATOR_BIN:-$reboot_attestor}" POLYEDGE_REBOOT_EXPECTED_UID="$uid" POLYEDGE_REBOOT_EXPECTED_GID="$gid" \
    POLYEDGE_PARITY_ENV_FILE="$case_root/parity.env" \
    FAKE_CALLS="$case_root/calls" FAKE_DF_AVAILABLE="${FAKE_DF_AVAILABLE:-20000000000}" FAKE_MOUNTPOINT_OK="${FAKE_MOUNTPOINT_OK:-1}" \
    FAKE_AZURE_REPORT="$case_root/azure.json" FAKE_AZURE_ATTESTATION="$case_root/azure.json.attestation.json" \
    FAKE_AZURE_EXECUTION="$case_root/azure-execution.json" FAKE_SAME_REPORT="$case_root/same.json" \
    FAKE_OCI_IMAGE="$oci_image" FAKE_OCI_IMAGE_DIGEST="$oci_image_digest" FAKE_API_STATUS="$case_root/api-status.json" \
    FAKE_PRODUCER_HEALTH="$case_root/producer-health.json" FAKE_PRODUCER_STATUS="$case_root/producer-status.json" \
    FAKE_FUNDED_PRODUCER_IMAGE="ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc" \
    FAKE_FUNDED_ACTIVE="${FAKE_FUNDED_ACTIVE:-0}" FAKE_QSET_ACTIVE="${FAKE_QSET_ACTIVE:-0}" \
    FAKE_FUNDED_ENABLEMENT="${FAKE_FUNDED_ENABLEMENT:-enabled}" \
    FAKE_FUNDED_TARGET_WANTS="${FAKE_FUNDED_TARGET_WANTS:-polyedge-funded-signer.service}" \
    FAKE_FUNDED_ALERT="${FAKE_FUNDED_ALERT:-0}" FAKE_FUNDED_FAILED_CLOSED="${FAKE_FUNDED_FAILED_CLOSED:-0}" \
    FAKE_FUNDED_PERSISTENT_MESSAGE_FAILED_CLOSED="${FAKE_FUNDED_PERSISTENT_MESSAGE_FAILED_CLOSED:-0}" FAKE_FUNDED_FAILED_ATTEMPTS="${FAKE_FUNDED_FAILED_ATTEMPTS:-0}" FAKE_FUNDED_NO_MARKET_WARMED="${FAKE_FUNDED_NO_MARKET_WARMED:-0}" FAKE_FUNDED_NO_PROCESSED_DELTA="${FAKE_FUNDED_NO_PROCESSED_DELTA:-0}" \
    FAKE_FUNDED_BURST="${FAKE_FUNDED_BURST:-0}" FAKE_FUNDED_GAP="${FAKE_FUNDED_GAP:-0}" \
    FAKE_FUNDED_RESTART="${FAKE_FUNDED_RESTART:-0}" FAKE_FUNDED_TOKEN_GAP="${FAKE_FUNDED_TOKEN_GAP:-0}" \
    FAKE_FUNDED_TOKEN_FAILURE="${FAKE_FUNDED_TOKEN_FAILURE:-0}" \
    FAKE_FUNDED_MID_HOUR_ACTIVE="${FAKE_FUNDED_MID_HOUR_ACTIVE:-0}" \
    FAKE_FUNDED_BUSY="${FAKE_FUNDED_BUSY:-0}" FAKE_FUNDED_STALE_SNAPSHOT="${FAKE_FUNDED_STALE_SNAPSHOT:-0}" \
    FAKE_FUNDED_OPEN_ORDER="${FAKE_FUNDED_OPEN_ORDER:-0}" \
    FAKE_FUNDED_UNRESOLVED_POSITION="${FAKE_FUNDED_UNRESOLVED_POSITION:-0}" \
    FAKE_FUNDED_UNRESOLVED_RESERVATION="${FAKE_FUNDED_UNRESOLVED_RESERVATION:-0}" \
    FAKE_SERVICE_BUS_STATUS="${FAKE_SERVICE_BUS_STATUS:-Active}" FAKE_SERVICE_BUS_ACTIVE="${FAKE_SERVICE_BUS_ACTIVE:-0}" \
    FAKE_SERVICE_BUS_SCHEDULED="${FAKE_SERVICE_BUS_SCHEDULED:-0}" FAKE_SERVICE_BUS_DLQ="${FAKE_SERVICE_BUS_DLQ:-1311}" \
    FAKE_PRODUCER_RUN_BOT="${FAKE_PRODUCER_RUN_BOT:-true}" \
    FAKE_PRODUCER_MOUNT_SOURCE="${FAKE_PRODUCER_MOUNT_SOURCE:-$case_root/token}" \
    FAKE_PRODUCER_MOUNT_RW="${FAKE_PRODUCER_MOUNT_RW:-false}" \
    FAKE_PRODUCER_TOKEN_GAP="${FAKE_PRODUCER_TOKEN_GAP:-0}" \
    FAKE_PRODUCER_TOKEN_FAILURE="${FAKE_PRODUCER_TOKEN_FAILURE:-0}" \
    FAKE_PRODUCER_PUBLISH_FAILURE="${FAKE_PRODUCER_PUBLISH_FAILURE:-0}" \
    FAKE_PRODUCER_INTENT_FAILURE="${FAKE_PRODUCER_INTENT_FAILURE:-0}" \
    FAKE_JOURNAL_PARTIAL_FAILURE="${FAKE_JOURNAL_PARTIAL_FAILURE:-0}" \
    FAKE_FUNDED_IMAGE="${FAKE_FUNDED_IMAGE:-ghcr.io/aldoapicella/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd}" \
    FAKE_FUNDED_UID="${FAKE_FUNDED_UID:-$uid}" FAKE_FUNDED_GID="${FAKE_FUNDED_GID:-$gid}" \
    FAKE_FUNDED_SESSION="${FAKE_FUNDED_SESSION:-dynamic-quote-funded-2026-08-13-v10}" \
    FAKE_FUNDED_SESSION_SHA="${FAKE_FUNDED_SESSION_SHA:-sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}" \
    FAKE_FUNDED_CONFIG_SHA="${FAKE_FUNDED_CONFIG_SHA:-sha256:9999999999999999999999999999999999999999999999999999999999999999}" \
    FAKE_API_BUSY_ATTEMPTS="${FAKE_API_BUSY_ATTEMPTS:-0}" \
    "$collector"
}

protected() {
  jq -cS '{status,azureAuthoritative,azureDeletionAllowed,rebootRecoveryPassed,rebootRecovery,shadowQsetEnabled,fundedSignerEnabled,
    fundedSignerMode,fundedSignerImage,fundedSignerUser,fundedSessionId,fundedSessionManifestSha256,fundedConfigSha256}' "$1"
}
seed_artifacts() {
  case_root=$1
  which=$2
  artifact_dir=$case_root/ring/parity/hourly/20260809T15
  mkdir -p "$artifact_dir"
  chmod 0750 "$case_root/ring/parity/hourly" "$artifact_dir"
  case "$which" in
    azure|both)
      cp "$case_root/azure.json" "$artifact_dir/azure-scheduled-audit.json"
      cp "$case_root/azure.json.attestation.json" "$artifact_dir/azure-scheduled-audit.json.attestation.json"
      chmod 0640 "$artifact_dir/azure-scheduled-audit.json" "$artifact_dir/azure-scheduled-audit.json.attestation.json"
      ;;
  esac
  case "$which" in
    same|both)
      cp "$case_root/same.json" "$artifact_dir/same-input-audit.json"
      attest "$artifact_dir/same-input-audit.json" "$artifact_dir/same-input-audit.json.attestation.json"
      chmod 0640 "$artifact_dir/same-input-audit.json" "$artifact_dir/same-input-audit.json.attestation.json"
      ;;
  esac
}

image_mismatch=$root/image-mismatch
fixture "$image_mismatch"
jq --arg image "$oci_image" '.properties.template.containers[0].image=$image' \
  "$image_mismatch/azure-execution.json" >"$image_mismatch/execution.tmp"
mv "$image_mismatch/execution.tmp" "$image_mismatch/azure-execution.json"
if run_collector "$image_mismatch" >/dev/null 2>&1; then
  echo 'image mismatch unexpectedly passed' >&2
  exit 1
fi

wrong_execution_time=$root/wrong-execution-time
fixture "$wrong_execution_time"
seed_artifacts "$wrong_execution_time" both
jq '.properties.startTime="2026-08-09T15:59:59Z"' "$wrong_execution_time/azure-execution.json" >"$wrong_execution_time/execution.tmp"
mv "$wrong_execution_time/execution.tmp" "$wrong_execution_time/azure-execution.json"
if run_collector "$wrong_execution_time" >/dev/null 2>&1; then
  echo 'wrong Azure execution time unexpectedly passed' >&2
  exit 1
fi

wrong_execution_command=$root/wrong-execution-command
fixture "$wrong_execution_command"
seed_artifacts "$wrong_execution_command" both
jq '.properties.template.containers[0].args=["polyedge-rs research audit --wrong-input"]' \
  "$wrong_execution_command/azure-execution.json" >"$wrong_execution_command/execution.tmp"
mv "$wrong_execution_command/execution.tmp" "$wrong_execution_command/azure-execution.json"
if run_collector "$wrong_execution_command" >/dev/null 2>&1; then
  echo 'wrong Azure execution command unexpectedly passed' >&2
  exit 1
fi

wrong_execution_env=$root/wrong-execution-env
fixture "$wrong_execution_env"
seed_artifacts "$wrong_execution_env" both
jq '(.properties.template.containers[0].env[] | select(.name=="ALLOW_LIVE").value)="true"' \
  "$wrong_execution_env/azure-execution.json" >"$wrong_execution_env/execution.tmp"
mv "$wrong_execution_env/execution.tmp" "$wrong_execution_env/azure-execution.json"
if run_collector "$wrong_execution_env" >/dev/null 2>&1; then
  echo 'wrong Azure execution safety environment unexpectedly passed' >&2
  exit 1
fi

unknown_secret_ref=$root/unknown-secret-ref
fixture "$unknown_secret_ref"
seed_artifacts "$unknown_secret_ref" both
jq '(.properties.template.containers[0].env[] | select(.name=="API_BEARER_TOKEN").secretRef)="untrusted-secret"' \
  "$unknown_secret_ref/azure-execution.json" >"$unknown_secret_ref/execution.tmp"
mv "$unknown_secret_ref/execution.tmp" "$unknown_secret_ref/azure-execution.json"
if run_collector "$unknown_secret_ref" >/dev/null 2>&1; then
  echo 'unknown Azure execution bearer secret reference unexpectedly passed' >&2
  exit 1
fi

builtin_execution_override=$root/builtin-execution-override
fixture "$builtin_execution_override"
seed_artifacts "$builtin_execution_override" both
jq '.properties.template.containers[0].env += [{name:"CONTAINER_APP_JOB_EXECUTION_NAME",value:"forged"}]' \
  "$builtin_execution_override/azure-execution.json" >"$builtin_execution_override/execution.tmp"
mv "$builtin_execution_override/execution.tmp" "$builtin_execution_override/azure-execution.json"
if run_collector "$builtin_execution_override" >/dev/null 2>&1; then
  echo 'explicit Container Apps execution override unexpectedly passed' >&2
  exit 1
fi

wrong_execution=$root/wrong-execution
fixture "$wrong_execution"
jq '.generator_provenance.execution_id="polyedge-hourly-quality-job-wrong"' \
  "$wrong_execution/azure.json" >"$wrong_execution/azure.tmp"
mv "$wrong_execution/azure.tmp" "$wrong_execution/azure.json"
attest "$wrong_execution/azure.json" "$wrong_execution/azure.json.attestation.json"
if run_collector "$wrong_execution" >/dev/null 2>&1; then
  echo 'wrong Azure execution unexpectedly passed' >&2
  exit 1
fi

report_hash_mismatch=$root/report-hash-mismatch
fixture "$report_hash_mismatch"
jq '.report_sha256="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"' \
  "$report_hash_mismatch/reports/2026/08/09/15/audit.json.attestation.json" >"$report_hash_mismatch/attestation.tmp"
mv "$report_hash_mismatch/attestation.tmp" "$report_hash_mismatch/reports/2026/08/09/15/audit.json.attestation.json"
chmod 0640 "$report_hash_mismatch/reports/2026/08/09/15/audit.json.attestation.json"
if run_collector "$report_hash_mismatch" >/dev/null 2>&1; then
  echo 'report hash mismatch unexpectedly passed' >&2
  exit 1
fi

missing_attestation=$root/missing-attestation
fixture "$missing_attestation"
rm "$missing_attestation/reports/2026/08/09/15/audit.json.attestation.json"
if run_collector "$missing_attestation" >/dev/null 2>&1; then
  echo 'missing attestation unexpectedly passed' >&2
  exit 1
fi

stale_attestation=$root/stale-attestation
fixture "$stale_attestation"
seed_artifacts "$stale_attestation" same
cp "$stale_attestation/reports/2026/08/09/15/audit.json.attestation.json" \
  "$stale_attestation/ring/parity/hourly/20260809T15/same-input-audit.json.attestation.json"
if run_collector "$stale_attestation" >/dev/null 2>&1; then
  echo 'stale copied attestation unexpectedly passed' >&2
  exit 1
fi

git_mismatch=$root/git-mismatch
fixture "$git_mismatch"
sed -i 's/^POLYEDGE_PARITY_EXPECTED_GIT_SHA=.*/POLYEDGE_PARITY_EXPECTED_GIT_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/' "$git_mismatch/parity.env"
if run_collector "$git_mismatch" >/dev/null 2>&1; then
  echo 'git mismatch unexpectedly passed' >&2
  exit 1
fi

early_runtime=$root/early-runtime
fixture "$early_runtime"
for file in "$early_runtime/azure.json" "$early_runtime/same.json" \
  "$early_runtime/reports/2026/08/09/15/audit.json"; do
  jq '.result.runtime_provenance.first_timestamp="2026-08-09T14:59:59Z"' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
done
if run_collector "$early_runtime" >/dev/null 2>&1; then
  echo 'early runtime provenance unexpectedly passed' >&2
  exit 1
fi

late_runtime=$root/late-runtime
fixture "$late_runtime"
for file in "$late_runtime/azure.json" "$late_runtime/same.json" \
  "$late_runtime/reports/2026/08/09/15/audit.json"; do
  jq '.result.runtime_provenance.last_timestamp="2026-08-09T16:00:00Z"' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
done
if run_collector "$late_runtime" >/dev/null 2>&1; then
  echo 'late runtime provenance unexpectedly passed' >&2
  exit 1
fi

copied_source=$root/copied-source
fixture "$copied_source"
run_collector "$copied_source" >/dev/null
copied_evidence=$root/copied-evidence
fixture "$copied_evidence"
mkdir -p -m 0750 "$copied_evidence/ring/parity/hourly/20260809T15"
cp "$copied_source/ring/parity/hourly/20260809T15/evidence.json" "$copied_evidence/ring/parity/hourly/20260809T15/evidence.json"
jq --arg ledger "$copied_evidence/ring/parity/ledger.json" '.ledgerPath=$ledger' \
  "$copied_evidence/ring/parity/hourly/20260809T15/evidence.json" >"$copied_evidence/evidence.tmp"
mv "$copied_evidence/evidence.tmp" "$copied_evidence/ring/parity/hourly/20260809T15/evidence.json"
chmod 0640 "$copied_evidence/ring/parity/hourly/20260809T15/evidence.json"
if run_collector "$copied_evidence" >/dev/null 2>&1; then
  echo 'copied evidence unexpectedly credited' >&2
  exit 1
fi

hourly_pin_override=$root/hourly-pin-override
fixture "$hourly_pin_override"
printf '%s\n' 'POLYEDGE_PARITY_EXPECTED_GIT_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' >>"$hourly_pin_override/hourly.env"
if run_collector "$hourly_pin_override" >/dev/null 2>&1; then
  echo 'hourly pin override unexpectedly passed' >&2
  exit 1
fi

formal_identity_override=$root/formal-identity-override
fixture "$formal_identity_override"
printf '%s\n' 'AZURE_SUBSCRIPTION_ID=22222222-2222-2222-2222-222222222222' >>"$formal_identity_override/hourly.env"
if run_collector "$formal_identity_override" >/dev/null 2>&1; then
  echo 'hourly formal identity override unexpectedly passed' >&2
  exit 1
fi

v2_manifest=$root/v2-manifest
fixture "$v2_manifest"
manifest=$(find "$v2_manifest/ring/archive" -name '*.manifest.json' | head -n 1)
jq '.schema_version=2' "$manifest" >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"
if run_collector "$v2_manifest" >/dev/null 2>&1; then echo 'v2 manifest unexpectedly passed' >&2; exit 1; fi

gap=$root/sequence-gap
fixture "$gap"
manifest=$(find "$gap/ring/archive" -name '*.manifest.json' | sort | sed -n '2p')
jq '.recorder_first_sequence=8 | .recorder_last_sequence=8' "$manifest" >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"
if run_collector "$gap" >/dev/null 2>&1; then echo 'sequence gap unexpectedly passed' >&2; exit 1; fi

duplicate=$root/sequence-duplicate
fixture "$duplicate"
manifest=$(find "$duplicate/ring/archive" -name '*.manifest.json' | sort | sed -n '2p')
jq '.recorder_first_sequence=1 | .recorder_last_sequence=1' "$manifest" >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"
if run_collector "$duplicate" >/dev/null 2>&1; then echo 'duplicate sequence unexpectedly passed' >&2; exit 1; fi

instance_change=$root/instance-change
fixture "$instance_change"
manifest=$(find "$instance_change/ring/archive" -name '*.manifest.json' | sort | sed -n '2p')
jq '.recorder_instance_id="223e4567-e89b-42d3-a456-426614174000"' "$manifest" >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"
if run_collector "$instance_change" >/dev/null 2>&1; then echo 'instance change unexpectedly passed' >&2; exit 1; fi

metric_failure=$root/metric-failure
fixture "$metric_failure"
jq '.recorder_metrics.queued=1' "$metric_failure/api-status.json" >"$metric_failure/status.tmp" && mv "$metric_failure/status.tmp" "$metric_failure/api-status.json"
if run_collector "$metric_failure" >/dev/null 2>&1; then echo 'recorder metric failure unexpectedly passed' >&2; exit 1; fi

transient_recorder_busy=$root/transient-recorder-busy
fixture "$transient_recorder_busy"
FAKE_API_BUSY_ATTEMPTS=3 run_collector "$transient_recorder_busy" >/dev/null
[ "$(cat "$transient_recorder_busy/calls/api-status-attempts")" -eq 4 ]

feed_failure=$root/feed-failure
fixture "$feed_failure"
jq '.feed_status.PolymarketClobMarket.status="error"' "$feed_failure/api-status.json" >"$feed_failure/status.tmp" && mv "$feed_failure/status.tmp" "$feed_failure/api-status.json"
if run_collector "$feed_failure" >/dev/null 2>&1; then
  echo 'essential feed failure unexpectedly passed' >&2
  exit 1
else
  [ "$?" -eq 1 ]
fi

stale_feed=$root/stale-feed
fixture "$stale_feed"
jq '.feed_status.PolymarketRtdsChainlink.updated_at="2026-08-09T16:14:59Z"' "$stale_feed/api-status.json" >"$stale_feed/status.tmp" && mv "$stale_feed/status.tmp" "$stale_feed/api-status.json"
if run_collector "$stale_feed" >/dev/null 2>&1; then echo 'stale essential feed unexpectedly passed' >&2; exit 1; fi

hour_feed_failure=$root/hour-feed-failure
fixture "$hour_feed_failure"
source=$(find "$hour_feed_failure/ring/segments" -name '*.jsonl' | sort | tail -n 1)
jq -nc '{event_type:"feed_error",recorded_ts:"2026-08-09T15:59:30Z",
  payload:{feed:"PolymarketClobMarket",error:"fixture disconnect"},
  recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_sequence:61}' >>"$source"
refresh_segment "$source"
jq '.recorder_metrics.last_assigned_sequence=61 | .recorder_metrics.enqueued_total=61 | .recorder_metrics.persisted_total=61' \
  "$hour_feed_failure/api-status.json" >"$hour_feed_failure/status.tmp" && mv "$hour_feed_failure/status.tmp" "$hour_feed_failure/api-status.json"
if run_collector "$hour_feed_failure" >/dev/null 2>&1; then
  echo 'target-hour feed error unexpectedly passed' >&2
  exit 1
else
  [ "$?" -eq 78 ]
fi

hour_health_gap=$root/hour-health-gap
fixture "$hour_health_gap"
source=$(find "$hour_health_gap/ring/segments" -name '*.jsonl' | sort | sed -n '3p')
jq -c 'if .recorder_sequence == 21 then .payload.essential_feed_health.summary="degraded" else . end' \
  "$source" >"$source.tmp" && mv "$source.tmp" "$source"
refresh_segment "$source"
if run_collector "$hour_health_gap" >/dev/null 2>&1; then echo 'unhealthy feed observation unexpectedly passed' >&2; exit 1; fi

late_status_sample=$root/late-status-sample
fixture "$late_status_sample"
early=$(date -u -d '2026-08-09T16:15:00Z' +%s)
sed -i "s/POLYEDGE_PARITY_NOW_EPOCH=.*/POLYEDGE_PARITY_NOW_EPOCH=$early/" "$late_status_sample/parity.env"
touch -d "@$early" "$late_status_sample/ring/status.json"
run_collector "$late_status_sample" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$late_status_sample/ring/parity/ledger.json")" = 1 ]

unexpected_active_ledger=$root/unexpected-active-ledger
fixture "$unexpected_active_ledger"
jq '.fundedSignerEnabled=true' "$unexpected_active_ledger/ring/parity/ledger.json" >"$unexpected_active_ledger/ledger.tmp"
mv "$unexpected_active_ledger/ledger.tmp" "$unexpected_active_ledger/ring/parity/ledger.json"
chmod 0640 "$unexpected_active_ledger/ring/parity/ledger.json"
if run_collector "$unexpected_active_ledger" >/dev/null 2>&1; then
  echo 'active funded ledger unexpectedly passed in the default masked mode' >&2
  exit 1
fi

active_funded=$root/active-funded
fixture "$active_funded"
activate_funded_fixture "$active_funded"
FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_MID_HOUR_ACTIVE=1 run_collector "$active_funded" >/dev/null
jq -e '.acceptedCleanLiveHours == 1 and .fundedSignerEnabled == true and
  (.acceptedHourlyEvidence | length) == 1' "$active_funded/ring/parity/20260809T141000Z-funded-active.json" >/dev/null
jq -e '.services.fundedSignerMode == "active" and .services.fundedSignerEnabled == true and
  .services.fundedSignerActive == true and .services.fundedSignerMasked == false and
  .services.fundedSignerRevision == "7777777777777777777777777777777777777777" and
  (.services.fundedRolloutReceipt.path | endswith("/activation/20260824T203004Z-post-restart-redemption-attestation.json")) and
  (.services.fundedRolloutReceipt.sha256 | test("^sha256:[0-9a-f]{64}$")) and
  .services.fundedServiceBusDlqBaseline == 1311 and
  .services.fundedSignerUser == "'"$uid:$gid"'" and .services.fundedSessionId == "dynamic-quote-funded-2026-08-13-v10" and
  .services.fundedRuntime.heartbeatCount == 60 and .services.fundedRuntime.alertCount == 0 and
  .services.fundedRuntime.maxHeartbeatGapSeconds == 60 and .services.fundedRuntime.tokenRefreshCount == 30 and
  .services.fundedRuntime.maxTokenRefreshGapSeconds == 120 and
  .services.fundedRuntime.executor.busy == false and
  .services.fundedRuntime.executor.safetySnapshotCacheAgeMs == 100 and
  .services.fundedRuntime.executor.openOrderCount == 0 and
  .services.fundedRuntime.executor.unresolvedPositionCount == 0 and
  .services.fundedRuntime.executor.unresolvedRiskReservationCount == 0 and
  .services.fundedServiceBusRuntime == {namespace:"sb-polyedge-funded-cl-6urdjr5nmwx7w",
    queue:"funded-dynamic-quote-intents",status:"Active",activeMessageCount:0,scheduledMessageCount:0,
    deadLetterMessageCount:1311,expectedDeadLetterMessageCount:1311} and
  .services.fundedIntentProducerRuntime.tokenContinuity.tokenRefreshCount == 30 and
  .services.fundedIntentProducerRuntime.tokenContinuity.maxTokenRefreshGapSeconds == 120 and
  .services.fundedIntentProducerRuntime.continuity.publisher.successCount == 1 and
  .services.fundedIntentProducerRuntime.continuity.publisher.infrastructureFailureCount == 0 and
  .services.fundedIntentProducerRuntime.continuity.maxRecorderQueueDepth == 1 and
  .services.fundedIntentProducerRuntime.continuity.recorderBusyObservationCount == 1 and
  .services.fundedIntentProducerRuntime.status.intentPublisher == {configured:true,prepared:true,pointerOnlyPreflight:false} and
  (.services.fundedIntentProducerRuntime.configEnvBindingSha256 | test("^sha256:[0-9a-f]{64}$")) and
  (.services.fundedIntentProducerRuntime.tokenMountBindingSha256 | test("^sha256:[0-9a-f]{64}$"))' \
  "$active_funded/ring/parity/hourly/20260809T15/evidence.json" >/dev/null

collector_drift=$root/collector-drift-bin
cp "$collector" "$collector_drift"
printf '\n' >>"$collector_drift"
chmod 0755 "$collector_drift"
helper_drift=$root/helper-drift
fixture "$helper_drift"
activate_funded_fixture "$helper_drift"
if TEST_COLLECTOR_BIN="$collector_drift" FAKE_FUNDED_ACTIVE=1 run_collector "$helper_drift" >/dev/null 2>&1; then
  echo 'drifted installed collector unexpectedly passed hourly parity' >&2
  exit 1
fi


for producer_case in wrong-env rw-mount unprepared token-gap token-failure warmup-failure intent-failure journal-partial recorder-backlog recorder-unbound-null recorder-failure final-backlog; do
  case_root=$root/producer-$producer_case
  fixture "$case_root"
  activate_funded_fixture "$case_root"
  case "$producer_case" in
    wrong-env) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_RUN_BOT=false run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    rw-mount) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_MOUNT_RW=true run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    unprepared)
      jq '.intent_publisher.prepared=false' "$case_root/producer-status.json" >"$case_root/status.tmp"
      mv "$case_root/status.tmp" "$case_root/producer-status.json"
      if FAKE_FUNDED_ACTIVE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    token-gap) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_TOKEN_GAP=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    token-failure) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_TOKEN_FAILURE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    warmup-failure) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_PUBLISH_FAILURE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    intent-failure) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_INTENT_FAILURE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    journal-partial) if FAKE_FUNDED_ACTIVE=1 FAKE_JOURNAL_PARTIAL_FAILURE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    recorder-backlog) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_RECORDER_BACKLOG=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    recorder-unbound-null) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_UNBOUND_NULL_STATUS=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    recorder-failure) if FAKE_FUNDED_ACTIVE=1 FAKE_PRODUCER_RECORDER_FAILURE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    final-backlog)
      jq '.recorder_metrics.queued=1 | .recorder_metrics.persisted_total=59' "$case_root/producer-status.json" >"$case_root/status.tmp"
      mv "$case_root/status.tmp" "$case_root/producer-status.json"
      if FAKE_FUNDED_ACTIVE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
  esac
  if [ "$passed" -eq 1 ]; then echo "invalid producer runtime unexpectedly passed: $producer_case" >&2; exit 1; fi
done

generated_funded=$root/generated-funded
fixture "$generated_funded"
activate_funded_fixture "$generated_funded"
FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_ENABLEMENT=generated run_collector "$generated_funded" >/dev/null

generated_not_wanted=$root/generated-not-wanted
fixture "$generated_not_wanted"
activate_funded_fixture "$generated_not_wanted"
if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_ENABLEMENT=generated FAKE_FUNDED_TARGET_WANTS=polyedge-api.service \
  run_collector "$generated_not_wanted" >/dev/null 2>&1; then
  echo 'generated funded signer without a reboot-target dependency unexpectedly passed' >&2
  exit 1
fi

reused_masked_ledger=$root/reused-masked-ledger
fixture "$reused_masked_ledger"
activate_funded_fixture "$reused_masked_ledger"
mv "$reused_masked_ledger/ring/parity/20260809T141000Z-funded-active.json" "$reused_masked_ledger/ring/parity/ledger.json"
sed -i "s#POLYEDGE_PARITY_LEDGER=.*#POLYEDGE_PARITY_LEDGER=$reused_masked_ledger/ring/parity/ledger.json#" "$reused_masked_ledger/parity.env"
if FAKE_FUNDED_ACTIVE=1 run_collector "$reused_masked_ledger" >/dev/null 2>&1; then
  echo 'reused masked ledger path unexpectedly passed active funded parity' >&2
  exit 1
fi

inactive_funded=$root/inactive-funded
fixture "$inactive_funded"
activate_funded_fixture "$inactive_funded"
if run_collector "$inactive_funded" >/dev/null 2>&1; then
  echo 'inactive funded signer unexpectedly passed an active parity window' >&2
  exit 1
fi

active_qset=$root/active-qset
fixture "$active_qset"
activate_funded_fixture "$active_qset"
if FAKE_FUNDED_ACTIVE=1 FAKE_QSET_ACTIVE=1 run_collector "$active_qset" >/dev/null 2>&1; then
  echo 'active qset unexpectedly passed the funded parity window' >&2
  exit 1
fi

wrong_funded_image=$root/wrong-funded-image
fixture "$wrong_funded_image"
activate_funded_fixture "$wrong_funded_image"
if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_IMAGE=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
    run_collector "$wrong_funded_image" >/dev/null 2>&1; then
  echo 'wrong funded image unexpectedly passed the funded parity window' >&2
  exit 1
fi

stale_funded_token=$root/stale-funded-token
fixture "$stale_funded_token"
activate_funded_fixture "$stale_funded_token"
touch -d "@$((fixture_now - 241))" "$stale_funded_token/token/funded-azure-federated-token"
if FAKE_FUNDED_ACTIVE=1 run_collector "$stale_funded_token" >/dev/null 2>&1; then
  echo 'stale funded token unexpectedly passed the funded parity window' >&2
  exit 1
fi

funded_alert=$root/funded-alert
fixture "$funded_alert"
activate_funded_fixture "$funded_alert"
if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_ALERT=1 run_collector "$funded_alert" >/dev/null 2>&1; then
    FAKE_FUNDED_PERSISTENT_MESSAGE_FAILED_CLOSED="${FAKE_FUNDED_PERSISTENT_MESSAGE_FAILED_CLOSED:-0}" FAKE_FUNDED_FAILED_ATTEMPTS="${FAKE_FUNDED_FAILED_ATTEMPTS:-0}" FAKE_FUNDED_NO_MARKET_WARMED="${FAKE_FUNDED_NO_MARKET_WARMED:-0}" FAKE_FUNDED_NO_PROCESSED_DELTA="${FAKE_FUNDED_NO_PROCESSED_DELTA:-0}" \
  echo 'funded alert unexpectedly passed the funded parity window' >&2
  exit 1
fi

for funded_case in burst gap restart failed-closed persistent-message-failed-closed failed-attempts no-market-warmed no-processed-delta token-gap token-failure wrong-user wrong-config busy stale-snapshot open-order unresolved-position unresolved-reservation; do
  case_root=$root/funded-$funded_case
  fixture "$case_root"
  activate_funded_fixture "$case_root"
  case "$funded_case" in
    burst) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_BURST=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    gap) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_GAP=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    restart) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_RESTART=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    failed-closed) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_FAILED_CLOSED=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    persistent-message-failed-closed) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_PERSISTENT_MESSAGE_FAILED_CLOSED=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    failed-attempts) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_FAILED_ATTEMPTS=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    no-market-warmed) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_NO_MARKET_WARMED=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    no-processed-delta) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_NO_PROCESSED_DELTA=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    token-gap) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_TOKEN_GAP=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    token-failure) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_TOKEN_FAILURE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    wrong-user) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_UID=999 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    wrong-config) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_CONFIG_SHA=sha256:8888888888888888888888888888888888888888888888888888888888888888 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    busy) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_BUSY=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    stale-snapshot) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_STALE_SNAPSHOT=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    open-order) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_OPEN_ORDER=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    unresolved-position) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_UNRESOLVED_POSITION=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    unresolved-reservation) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_UNRESOLVED_RESERVATION=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
  esac
  if [ "$passed" -eq 1 ]; then
    echo "invalid funded runtime unexpectedly passed: $funded_case" >&2
    exit 1
  fi
done

for broker_case in inactive active-message scheduled-message dlq-drift; do
  case_root=$root/broker-$broker_case
  fixture "$case_root"
  activate_funded_fixture "$case_root"
  case "$broker_case" in
    inactive) if FAKE_FUNDED_ACTIVE=1 FAKE_SERVICE_BUS_STATUS=Disabled run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    active-message) if FAKE_FUNDED_ACTIVE=1 FAKE_SERVICE_BUS_ACTIVE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    scheduled-message) if FAKE_FUNDED_ACTIVE=1 FAKE_SERVICE_BUS_SCHEDULED=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    dlq-drift) if FAKE_FUNDED_ACTIVE=1 FAKE_SERVICE_BUS_DLQ=1312 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
  esac
  if [ "$passed" -eq 1 ]; then
    echo "unsafe funded Service Bus runtime unexpectedly passed: $broker_case" >&2
    exit 1
  fi
done

success=$root/success
fixture "$success"
before=$(protected "$success/ring/parity/ledger.json")
run_collector "$success" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(find "$success/ring/segments" -name '*.sequence.*' | wc -l)" -eq 0 ]
jq -e '.acceptedForParityWindow == true and .sameInput.deterministicResultExactMatch == true and (.segments | length) == 6' \
  "$success/ring/parity/hourly/20260809T15/evidence.json" >/dev/null
grep -q -- "--user $uid:$gid" "$success/calls/podman"
grep -q -- "$success/token/azure-federated-token:/run/credentials/azure-federated-token:ro,Z" "$success/calls/podman"
grep -q -- ':/evidence:rw,Z' "$success/calls/podman"
! grep -q -- '--security-opt=no-new-privileges' "$success/calls/podman"
[ "$(protected "$success/ring/parity/ledger.json")" = "$before" ]
runs=$(wc -l <"$success/calls/podman")
run_collector "$success" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(find "$success/ring/segments" -name '*.sequence.*' | wc -l)" -eq 0 ]
[ "$(wc -l <"$success/calls/podman")" = "$runs" ]

v4_success=$root/v4-success
fixture "$v4_success" "2026-08-09T15:00:00Z" "2026-08-09T14:10:00Z" 4
run_collector "$v4_success" >/dev/null
jq -e '.acceptedCleanLiveHours == 1 and (.acceptedHourlyEvidence | length) == 1' \
  "$v4_success/ring/parity/ledger.json" >/dev/null

legacy_feed_evidence=$root/legacy-feed-evidence
fixture "$legacy_feed_evidence"
run_collector "$legacy_feed_evidence" >/dev/null
evidence=$legacy_feed_evidence/ring/parity/hourly/20260809T15/evidence.json
jq 'del(.continuousFeedHealth)' "$evidence" >"$evidence.tmp" && mv "$evidence.tmp" "$evidence"
chmod 0640 "$evidence"
jq '.acceptedCleanLiveHours=0 | .acceptedHourlyEvidence=[]' "$legacy_feed_evidence/ring/parity/ledger.json" >"$legacy_feed_evidence/ledger.tmp"
chmod 0640 "$legacy_feed_evidence/ledger.tmp"
mv "$legacy_feed_evidence/ledger.tmp" "$legacy_feed_evidence/ring/parity/ledger.json"
if run_collector "$legacy_feed_evidence" >/dev/null 2>&1; then echo 'legacy feed evidence unexpectedly passed' >&2; exit 1; fi

legacy=$root/legacy
fixture "$legacy"
jq 'del(.completedDailyCycles,.acceptedDailyEvidence)' "$legacy/ring/parity/ledger.json" >"$legacy/ledger.tmp"
chmod 0640 "$legacy/ledger.tmp"
mv "$legacy/ledger.tmp" "$legacy/ring/parity/ledger.json"
run_collector "$legacy" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$legacy/ring/parity/ledger.json")" = 1 ]
jq -e 'has("completedDailyCycles") == false and has("acceptedDailyEvidence") == false' "$legacy/ring/parity/ledger.json" >/dev/null

invalid_parity=$root/invalid-parity
fixture "$invalid_parity"
for file in "$invalid_parity/azure.json" "$invalid_parity/same.json" \
  "$invalid_parity/reports/2026/08/09/15/audit.json"; do
  jq '.result.strategy_batch_replayed=0 | .result.strategy_batch_matches=0 |
    .result.strategy_batch_invalid=1 | .result.decision_pipeline_replay_rate=0 |
    .result.decision_output_binding_rate=null | .result.decision_parity_rate=0' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
done
if run_collector "$invalid_parity" >/dev/null 2>&1; then
  echo 'invalid decision parity unexpectedly passed' >&2
  exit 1
fi
[ "$(jq -r '.acceptedCleanLiveHours' "$invalid_parity/ring/parity/ledger.json")" = 0 ]

decision_config_mismatch=$root/decision-config-mismatch
fixture "$decision_config_mismatch"
jq '.result.runtime_provenance.identities[0].decision_config_sha256="sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"' \
  "$decision_config_mismatch/reports/2026/08/09/15/audit.json" >"$decision_config_mismatch/oci.tmp"
mv "$decision_config_mismatch/oci.tmp" "$decision_config_mismatch/reports/2026/08/09/15/audit.json"
if run_collector "$decision_config_mismatch" >/dev/null 2>&1; then
  echo 'cross-report decision config mismatch unexpectedly passed' >&2
  exit 1
fi

gapped=$root/gapped
fixture "$gapped"
for file in "$gapped/azure.json" "$gapped/same.json" "$gapped/reports/2026/08/09/15/audit.json"; do
  jq '.result.runtime_provenance.max_gap_ms=600000' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
done
if run_collector "$gapped" >/dev/null 2>&1; then
  echo 'gapped runtime provenance unexpectedly passed' >&2
  exit 1
fi
[ "$(jq -r '.acceptedCleanLiveHours' "$gapped/ring/parity/ledger.json")" = 0 ]

second=$success/ring/parity/hourly/20260809T16
mkdir -p -m 0750 "$second" "$success/ring/segments/2026/08/09/16" "$success/ring/archive/2026/08/09/16" "$success/reports/2026/08/09/16"
for file in "$success"/ring/segments/2026/08/09/15/*.jsonl; do cp "$file" "$success/ring/segments/2026/08/09/16/"; done
for file in "$success"/ring/archive/2026/08/09/15/*; do cp "$file" "$success/ring/archive/2026/08/09/16/"; done
cp "$success/reports/2026/08/09/15/audit.json" "$success/reports/2026/08/09/16/audit.json"
cp "$success/reports/2026/08/09/15/audit.json.attestation.json" "$success/reports/2026/08/09/16/audit.json.attestation.json"
cp "$success/ring/parity/hourly/20260809T15/azure-scheduled-audit.json" "$second/azure-scheduled-audit.json"
cp "$success/ring/parity/hourly/20260809T15/azure-scheduled-audit.json.attestation.json" "$second/azure-scheduled-audit.json.attestation.json"
cp "$success/ring/parity/hourly/20260809T15/same-input-audit.json" "$second/same-input-audit.json"
cp "$success/ring/parity/hourly/20260809T15/same-input-audit.json.attestation.json" "$second/same-input-audit.json.attestation.json"
chmod 0640 "$success/reports/2026/08/09/16/audit.json" "$success/reports/2026/08/09/16/audit.json.attestation.json" \
  "$second/azure-scheduled-audit.json" "$second/azure-scheduled-audit.json.attestation.json" \
  "$second/same-input-audit.json" "$second/same-input-audit.json.attestation.json"
jq '.generatedAtUtc="2026-08-09T17:19:45Z" | .hourStartUtc="2026-08-09T16:00:00Z" | .hourEndUtc="2026-08-09T17:00:00Z" |
  (.scheduledAudits.azure.path |= sub("20260809T15"; "20260809T16")) |
  (.scheduledAudits.azure.attestation.path |= sub("20260809T15"; "20260809T16")) |
  (.scheduledAudits.oci.path |= sub("/15/"; "/16/")) |
  (.scheduledAudits.oci.attestation.path |= sub("/15/"; "/16/")) |
  (.sameInput.path |= sub("20260809T15"; "20260809T16")) |
  (.sameInput.attestation.path |= sub("20260809T15"; "20260809T16")) |
  (.segments[].source.path |= sub("/15/"; "/16/")) |
  (.segments[].gzip.path |= sub("/15/"; "/16/")) |
  (.segments[].manifest.path |= sub("/15/"; "/16/")) |
  (.segments[].receipt.path |= sub("/15/"; "/16/")) |
  .recorderSequence.recorder_first_sequence=61 | .recorderSequence.recorder_last_sequence=120 |
  .recorderStatus.sampledAtUtc="2026-08-09T17:20:00Z" |
  (.recorderStatus.feedStatus[]).updated_at="2026-08-09T17:19:59Z" |
  .continuousFeedHealth.firstObservationUtc="2026-08-09T16:00:01Z" |
  .continuousFeedHealth.lastObservationUtc="2026-08-09T16:59:01Z"' \
  "$success/ring/parity/hourly/20260809T15/evidence.json" >"$second/evidence.json"
chmod 0640 "$second/evidence.json"
sed -i 's/POLYEDGE_PARITY_TARGET_HOUR_UTC=.*/POLYEDGE_PARITY_TARGET_HOUR_UTC=2026-08-09T16:00:00Z/' "$success/parity.env"
sed -i "s/POLYEDGE_PARITY_NOW_EPOCH=.*/POLYEDGE_PARITY_NOW_EPOCH=$(date -u -d '2026-08-09T17:20:00Z' +%s)/" "$success/parity.env"
run_collector "$success" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$success/ring/parity/ledger.json")" = 2 ]
jq -e '(.acceptedHourlyEvidence | length) == 2 and .acceptedHourlyEvidence[1].hourStartUtc == "2026-08-09T16:00:00Z"' \
  "$success/ring/parity/ledger.json" >/dev/null
[ "$(wc -l <"$success/calls/podman")" = "$runs" ]

cross_hour_gap=$root/cross-hour-gap
fixture "$cross_hour_gap"
run_collector "$cross_hour_gap" >/dev/null
mkdir -m 0750 "$cross_hour_gap/ring/parity/hourly/20260809T16"
jq '.generatedAtUtc="2026-08-09T17:19:45Z" | .hourStartUtc="2026-08-09T16:00:00Z" | .hourEndUtc="2026-08-09T17:00:00Z" |
  .recorderSequence.recorder_first_sequence=62 | .recorderSequence.recorder_last_sequence=121 |
  .recorderStatus.sampledAtUtc="2026-08-09T17:20:00Z" |
  (.recorderStatus.feedStatus[]).updated_at="2026-08-09T17:19:59Z" |
  .continuousFeedHealth.firstObservationUtc="2026-08-09T16:00:01Z" |
  .continuousFeedHealth.lastObservationUtc="2026-08-09T16:59:01Z"' \
  "$cross_hour_gap/ring/parity/hourly/20260809T15/evidence.json" >"$cross_hour_gap/ring/parity/hourly/20260809T16/evidence.json"
chmod 0640 "$cross_hour_gap/ring/parity/hourly/20260809T16/evidence.json"
sed -i 's/POLYEDGE_PARITY_TARGET_HOUR_UTC=.*/POLYEDGE_PARITY_TARGET_HOUR_UTC=2026-08-09T16:00:00Z/' "$cross_hour_gap/parity.env"
sed -i "s/POLYEDGE_PARITY_NOW_EPOCH=.*/POLYEDGE_PARITY_NOW_EPOCH=$(date -u -d '2026-08-09T17:20:00Z' +%s)/" "$cross_hour_gap/parity.env"
if run_collector "$cross_hour_gap" >/dev/null 2>&1; then echo 'cross-hour sequence gap unexpectedly passed' >&2; exit 1; fi

superseded=$root/superseded
fixture "$superseded"
historical=$superseded/ring/parity/hourly/20260809T14
mkdir -p -m 0750 "$historical"
jq -n --arg ledger "$superseded/ring/parity/old-ledger.json" '{
  schemaVersion:1,status:"validated",acceptedForParityWindow:true,
  windowStartUtc:"2026-08-09T14:00:00Z",hourStartUtc:"2026-08-09T14:00:00Z",hourEndUtc:"2026-08-09T15:00:00Z",
  ledgerPath:$ledger,azureAuthoritative:true,azureDeletionAllowed:false,
  sameInput:{deterministicResultExactMatch:true}
}' >"$historical/evidence.json"
chmod 0640 "$historical/evidence.json"
run_collector "$superseded" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$superseded/ring/parity/ledger.json")" = 1 ]

recovery=$root/recovery
fixture "$recovery"
seed_artifacts "$recovery" both
run_collector "$recovery" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$recovery/ring/parity/ledger.json")" = 1 ]
[ "$(wc -l <"$recovery/calls/curl")" = 2 ] && [ ! -e "$recovery/calls/podman" ]

azure_only=$root/azure-only
fixture "$azure_only"
seed_artifacts "$azure_only" azure
run_collector "$azure_only" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$azure_only/ring/parity/ledger.json")" = 1 ]
[ "$(wc -l <"$azure_only/calls/curl")" = 2 ] && [ "$(wc -l <"$azure_only/calls/podman")" = 1 ]

same_only=$root/same-only
fixture "$same_only"
seed_artifacts "$same_only" same
run_collector "$same_only" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$same_only/ring/parity/ledger.json")" = 1 ]
[ "$(wc -l <"$same_only/calls/curl")" = 5 ] && [ ! -e "$same_only/calls/podman" ]

mismatch=$root/mismatch
fixture "$mismatch"
report mismatch "$mismatch/same.json"
if run_collector "$mismatch" >/dev/null 2>&1; then
  echo 'comparison mismatch unexpectedly passed' >&2
  exit 1
fi
[ "$(jq -r '.acceptedCleanLiveHours' "$mismatch/ring/parity/ledger.json")" = 0 ]
[ ! -e "$mismatch/ring/parity/hourly/20260809T15/evidence.json" ]

disk=$root/disk
fixture "$disk"
if FAKE_DF_AVAILABLE=16106127359 run_collector "$disk" >/dev/null 2>&1; then
  echo 'disk-floor failure unexpectedly passed' >&2
  exit 1
fi
[ "$(jq -r '.acceptedCleanLiveHours' "$disk/ring/parity/ledger.json")" = 0 ]

excluded=$root/excluded
boundary_epoch=$(date -u -d '2026-08-08T20:15:00Z' +%s)
first_full_epoch=$(( (boundary_epoch + 3599) / 3600 * 3600 ))
[ "$(date -u -d "@$first_full_epoch" +%Y-%m-%dT%H:%M:%SZ)" = 2026-08-08T21:00:00Z ]
mount_failure=$root/mount-failure
fixture "$mount_failure"
if FAKE_MOUNTPOINT_OK=0 run_collector "$mount_failure" >/dev/null 2>&1; then
  echo 'non-mount ring root unexpectedly passed' >&2
  exit 1
fi

redirect=$root/redirect
fixture "$redirect"
cp "$redirect/ring/parity/ledger.json" "$redirect/redirected.json"
chmod 0640 "$redirect/redirected.json"
sed -i "s#POLYEDGE_PARITY_LEDGER=.*#POLYEDGE_PARITY_LEDGER=$redirect/redirected.json#" "$redirect/parity.env"
if run_collector "$redirect" >/dev/null 2>&1; then
  echo 'redirected ledger unexpectedly passed' >&2
  exit 1
fi
fixture "$excluded" 2026-08-08T20:00:00Z 2026-08-08T20:15:00Z
before=$(protected "$excluded/ring/parity/ledger.json")
run_collector "$excluded" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$excluded/ring/parity/ledger.json")" = 0 ]
jq -e '.status == "excluded_pre_window" and .acceptedForParityWindow == false' \
  "$excluded/ring/parity/hourly/20260808T20/evidence.json" >/dev/null
[ "$(protected "$excluded/ring/parity/ledger.json")" = "$before" ]

launcher_root=$root/run-job
mkdir -p "$launcher_root/bin" "$launcher_root/etc/jobs" "$launcher_root/run/polyedge-federated-research" "$launcher_root/ring"
: >"$launcher_root/etc/jobs/hourly.env"
: >"$launcher_root/run/polyedge-federated-research/azure-federated-token"
sed -e "s#/etc/polyedge/jobs/#$launcher_root/etc/jobs/#g" \
  -e "s#/srv/polyedge-ring#$launcher_root/ring#g" \
  -e "s#/run/polyedge-federated-#$launcher_root/run/polyedge-federated-#g" \
  -e "s#/run/polyedge/utility.lock#$launcher_root/run/utility.lock#g" \
  -e "s#/etc/polyedge/credentials/#$launcher_root/etc/credentials/#g" \
  -e "s#/usr/bin/timeout#$launcher_root/bin/timeout#g" \
  -e "s#/usr/bin/podman#$launcher_root/bin/podman#g" \
  -e "s/chown 0:0/chown $uid:$gid/" \
  -e "s/'0:0:640:1'/'$uid:$gid:640:1'/" \
  "$(dirname "$collector")/polyedge-run-job" >"$launcher_root/run-job"
chmod 0755 "$launcher_root/run-job"
cat >"$launcher_root/bin/timeout" <<'EOF'
#!/bin/sh
[ "$1" != --preserve-status ] || shift
shift
exec "$@"
EOF
cat >"$launcher_root/bin/podman" <<'EOF'
#!/bin/sh
case "$1" in
  run)
    printf '%s\n' "$*" >>"$LAUNCH_CALLS/run"
    report="$LAUNCH_RING/jobs/research/reports/research/hourly/$POLYEDGE_AUDIT_DAY/$POLYEDGE_AUDIT_HOUR/audit.json"
    mkdir -p "${report%/*}"
    execution=$POLYEDGE_GENERATOR_EXECUTION_ID
    [ "${LAUNCH_MODE:-}" != provenance ] || execution=forged-execution
    jq -n --arg image "$POLYEDGE_GENERATOR_IMAGE" --arg execution "$execution" '{
      generator_provenance:{schema_version:1,platform:"oci_podman",image:$image,execution_id:$execution,job_name:null}
    }' >"$report"
    ;;
  inspect)
    case "$*" in
      *Config.Image*) printf '%s\n' "$POLYEDGE_RESEARCH_IMAGE" ;;
      *ImageDigest*)
        if [ "${LAUNCH_MODE:-}" = inspect ]; then printf '%s\n' invalid; else printf '%s\n' "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"; fi
        ;;
      *Id*) printf '%s\n' eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ;;
      *) exit 2 ;;
    esac
    ;;
  rm) printf '%s\n' "$*" >>"$LAUNCH_CALLS/rm" ;;
  *) exit 2 ;;
esac
EOF
cat >"$launcher_root/bin/sha256sum" <<'EOF'
#!/bin/sh
if [ "${LAUNCH_MODE:-}" = hash ] && [ "${1##*/}" = audit.json ]; then
  count=0
  [ ! -f "$LAUNCH_CALLS/hash-count" ] || count=$(cat "$LAUNCH_CALLS/hash-count")
  count=$((count + 1))
  printf '%s\n' "$count" >"$LAUNCH_CALLS/hash-count"
  if [ "$count" = 1 ]; then
    printf '%064d  %s\n' 0 "$1"
    exit 0
  fi
fi
exec /usr/bin/sha256sum "$@"
EOF
cat >"$launcher_root/bin/disk-guard" <<'EOF'
#!/bin/sh
[ "$*" = --assert-headroom ]
EOF
chmod 0755 "$launcher_root/bin"/*

run_launcher() {
  mode=$1
  rm -rf "$launcher_root/ring/jobs" "$launcher_root/calls"
  mkdir -p "$launcher_root/calls"
  env PATH="$launcher_root/bin:$fake:$PATH" FAKE_MOUNTPOINT_OK=1 FAKE_DF_AVAILABLE=100000000000 \
    LAUNCH_MODE="$mode" LAUNCH_CALLS="$launcher_root/calls" LAUNCH_RING="$launcher_root/ring" \
    POLYEDGE_BOOT_DISK_GUARD_BIN="$launcher_root/bin/disk-guard" \
    POLYEDGE_RESEARCH_IMAGE="$oci_image" POLYEDGE_LOCAL_RAW_ROOT=/input/events \
    POLYEDGE_DISABLE_RESEARCH_ARTIFACT_PUBLISH=true POLYEDGE_JOB_MIN_FREE_BYTES=1 \
    "$launcher_root/run-job" hourly
}

run_launcher success
launcher_report=$(find "$launcher_root/ring/jobs/research/reports/research/hourly" -name audit.json -print)
launcher_attestation=$launcher_report.attestation.json
jq -e --arg sha "sha256:$(/usr/bin/sha256sum "$launcher_report" | awk '{print $1}')" --arg image "$oci_image" '
  .schema_version == 1 and .report_sha256 == $sha and .platform == "oci_podman" and .image == $image and
  (.image_digest | test("^sha256:[0-9a-f]{64}$")) and (.container_id | test("^[0-9a-f]{64}$"))
' "$launcher_attestation" >/dev/null
! grep -q -- '--rm' "$launcher_root/calls/run"
grep -q '^rm polyedge-job-hourly-' "$launcher_root/calls/rm"

for launcher_failure in provenance hash inspect; do
  if run_launcher "$launcher_failure" >/dev/null 2>&1; then
    echo "launcher $launcher_failure failure unexpectedly passed" >&2
    exit 1
  fi
  [ -s "$launcher_root/calls/rm" ] || { echo "launcher $launcher_failure failure skipped container cleanup" >&2; exit 1; }
  ! find "$launcher_root/ring/jobs/research/reports" -name '*.attestation.json' -print -quit | grep -q .
done

if grep -R -F 'fixture-access-token' "$root"/*/calls >/dev/null || grep -R -F 'fixture-jwt' "$root"/*/calls >/dev/null; then
  echo 'a token leaked into command arguments' >&2
  exit 1
fi
grep -F '/usr/bin/flock -w 300 9' "$collector" >/dev/null
grep -Fx 'RestartPreventExitStatus=78' "$(dirname "$collector")/../systemd/polyedge-parity-hourly.service" >/dev/null

echo 'parity hourly collector self-test passed'
