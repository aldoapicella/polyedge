#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
writer=$root/quadlets/polyedge-shadow-qset-v3.container
writer_env=$root/env/shadow-qset-v3.env.example
sealer_env=$root/env/qset-v3-sealer.env.example
sealer=$root/bin/polyedge-qset-v3-seal-days
token=$root/bin/polyedge-federated-token-refresh
service=$root/systemd/polyedge-qset-v3-seal-days.service
timer=$root/systemd/polyedge-qset-v3-first-seal.timer
override=$root/systemd/polyedge-federated-token@shadow-qset-v3-writer.service.d/override.conf
infra=$root/../../infra/shadow-profitability-qset-v3.bicep

for file in "$sealer" "$token"; do sh -n "$file"; done
grep -F 'c8133837-ba14-4b6b-8f58-52ce675a33e4' "$infra" >/dev/null
! grep -F '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3' "$infra" >/dev/null
grep -Fx 'APP_NAME=polyedge-shadow-qset-v3' "$writer_env" >/dev/null
grep -Fx 'SHADOW_CAMPAIGN_ID=campaign-2026-08-23-qset-v3' "$writer_env" >/dev/null
grep -Fx 'EXECUTION_MODE=paper' "$writer_env" >/dev/null
grep -Fx 'ALLOW_LIVE=false' "$writer_env" >/dev/null
grep -Fx 'SHADOW_CODE_FREEZE_FINALIZED=' "$writer_env" >/dev/null
grep -Fx 'AZURE_STORAGE_CONTAINER_NAME=polyedge-shadow-qset-v3-events' "$writer_env" >/dev/null
grep -Fx 'AZURE_RESEARCH_STORAGE_CONTAINER_NAME=polyedge-research-qset-v3' "$writer_env" >/dev/null
grep -Fx 'POLYEDGE_QSET_V3_CONTROL_CONTAINER_NAME=polyedge-qset-v3-control' "$writer_env" >/dev/null
grep -Fx 'AZURE_STORAGE_TABLE_NAME=ShadowQsetV3EventIndex' "$writer_env" >/dev/null
grep -Fx 'AZURE_CHART_TABLE_NAME=ShadowQsetV3ChartSeries' "$writer_env" >/dev/null
grep -Fx 'AZURE_MARKET_TABLE_NAME=ShadowQsetV3MarketCatalog' "$writer_env" >/dev/null
grep -Fx 'AZURE_EVENT_BLOB_PREFIX=shadow-events/preflight/campaign-2026-08-23-qset-v3' "$writer_env" >/dev/null
grep -Fx 'AZURE_EVENT_BLOB_PREFIX_AFTER_CUTOVER=shadow-events/campaign-2026-08-23-qset-v3' "$writer_env" >/dev/null
grep -Fx 'AZURE_EVENT_BLOB_PREFIX_CUTOVER_UTC=2026-08-23T00:00:00Z' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_INTENT_OPERATOR_DIRECT=true' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_INTENT_POINTER_ONLY_PREFLIGHT=true' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_CANARY_INTENT_PREFIX=control/strategy-canary/intents/campaign-2026-08-23-qset-v3/intents' "$writer_env" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_ENABLED=false' "$writer_env" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_NAMESPACE=' "$writer_env" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_QUEUE=' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI=azure://stpolyedge6urdjr5nmwx7w/polyedge-research-qset-v3/reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_CANARY_EXECUTION_MODEL_SHA256=sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4' "$writer_env" >/dev/null
grep -Fx 'SHADOW_CAMPAIGN_ID=campaign-2026-08-23-qset-v3' "$sealer_env" >/dev/null
grep -Fx 'SHADOW_CODE_FREEZE_FINALIZED=' "$sealer_env" >/dev/null
grep -Fx 'AZURE_STORAGE_CONTAINER_NAME=polyedge-shadow-qset-v3-events' "$sealer_env" >/dev/null
grep -Fx 'AZURE_RESEARCH_STORAGE_CONTAINER_NAME=polyedge-research-qset-v3' "$sealer_env" >/dev/null
grep -Fx 'POLYEDGE_QSET_V3_CONTROL_CONTAINER_NAME=polyedge-qset-v3-control' "$sealer_env" >/dev/null
grep -Fx 'ContainerName=polyedge-shadow-qset-v3' "$writer" >/dev/null
grep -Fx 'EnvironmentFile=/etc/polyedge/shadow-qset-v3.env' "$writer" >/dev/null
grep -Fx 'Volume=/run/polyedge-federated-shadow-qset-v3-writer:/run/credentials:ro,Z' "$writer" >/dev/null
grep -Fx 'Wants=network-online.target polyedge-federated-token@shadow-qset-v3-writer.service' "$writer" >/dev/null
grep -F -- '--cpus=0.5 --memory=1g --pids-limit=512' "$writer" >/dev/null
grep -F -- '--read-only --tmpfs=/tmp:rw,noexec,nosuid,size=64m --cap-drop=all --pull=never --log-driver=journald' "$writer" >/dev/null
if grep -q '^PublishPort=' "$writer"; then echo 'qset-v3 writer must not publish a host port' >&2; exit 1; fi
grep -Fx 'OnCalendar=2026-08-25 02:15:00 UTC' "$timer" >/dev/null
grep -Fx 'Persistent=true' "$timer" >/dev/null
grep -Fx 'Unit=polyedge-qset-v3-seal-days.service' "$timer" >/dev/null
grep -Fx 'ExecStart=/usr/bin/flock -w 300 /run/polyedge/research.lock /usr/local/libexec/polyedge-qset-v3-seal-days' "$service" >/dev/null
grep -Fx 'User=polyedge-qset-v3-writer' "$override" >/dev/null
grep -Fx 'Group=polyedge-qset-v3-writer' "$override" >/dev/null
grep -F 'shadow-qset-v3-writer' "$token" >/dev/null
grep -F 'for date in 2026-08-23 2026-08-24' "$sealer" >/dev/null
grep -F 'seal-qset-v3-day' "$sealer" >/dev/null
grep -F 'SHADOW_CODE_FREEZE_FINALIZED:-}" = true' "$sealer" >/dev/null
grep -F 'polyedge-shadow-qset-v3-events' "$sealer" >/dev/null
grep -F 'polyedge-research-qset-v3' "$sealer" >/dev/null
grep -F 'polyedge-qset-v3-control' "$sealer" >/dev/null
preflight_line=$(grep -n 'run_sealer "$date" --validate-only' "$sealer" | cut -d: -f1)
fence_line=$(grep -n 'systemctl stop "$writer"' "$sealer" | cut -d: -f1)
seal_line=$(grep -n 'run_sealer "$date" >"$temporary"' "$sealer" | cut -d: -f1)
[ "$preflight_line" -lt "$fence_line" ] && [ "$fence_line" -lt "$seal_line" ]
grep -F 'and (has("generated_ts") | not) and (has("sealed_at") | not)' "$sealer" >/dev/null
grep -Fx 'User=983:979' "$writer" >/dev/null
grep -F -- '--user 983:979' "$sealer" >/dev/null
grep -F -- '--source-freeze-blob "$EXECUTION_FREEZE_ARTIFACT_PATH" --source-freeze-sha256 "$EXECUTION_FREEZE_SHA256"' "$sealer" >/dev/null
grep -F 'source_freeze == {container:"polyedge-qset-v3-control",blob:$freeze_blob,sha256:$freeze,verified:true}' "$sealer" >/dev/null
