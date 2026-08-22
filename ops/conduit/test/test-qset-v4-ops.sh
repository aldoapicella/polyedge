#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
writer=$root/quadlets/polyedge-shadow-qset-v4.container
writer_env=$root/env/shadow-qset-v4.env.example
sealer_env=$root/env/qset-v4-sealer.env.example
sealer=$root/bin/polyedge-qset-v4-seal-days
token=$root/bin/polyedge-federated-token-refresh
service=$root/systemd/polyedge-qset-v4-seal-days.service
timer=$root/systemd/polyedge-qset-v4-first-seal.timer
infra=$root/../../infra/shadow-profitability-qset-v4.bicep
handoff=$root/bin/polyedge-qset-v4-rbac-handoff
freeze_builder=$root/bin/polyedge-qset-v4-source-freeze
guard=$root/bin/polyedge-qset-v4-boundary-guard
boundary_service=$root/systemd/polyedge-qset-v4-boundary@.service
boundary_pre=$root/systemd/polyedge-qset-v4-boundary-pre.timer
boundary_post=$root/systemd/polyedge-qset-v4-boundary-post.timer
policy=$root/../../research/configs/campaign_freeze_2026-08-24_qset_v4.json
disposition=$root/../../research/configs/protocol-v3-campaign-disposition-2026-08-24-qset-v4.json
repo=$root/../..

bash -n "$guard"
bash -n "$handoff" "$freeze_builder"
for file in "$sealer" "$token"; do sh -n "$file"; done
grep -F 'c8133837-ba14-4b6b-8f58-52ce675a33e4' "$infra" >/dev/null
! grep -F '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3' "$infra" >/dev/null
grep -Fx "var writerIdentityName = 'id-polyedge-conduit-shadow-qset-v3-writer'" "$infra" >/dev/null
grep -Fx "var processorIdentityName = 'id-polyedge-conduit-shadow-qset-v3-processor'" "$infra" >/dev/null
grep -Fx "resource writerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {" "$infra" >/dev/null
grep -Fx "resource processorIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {" "$infra" >/dev/null
grep -Fx 'APP_NAME=polyedge-shadow-qset-v4' "$writer_env" >/dev/null
grep -Fx 'SHADOW_CAMPAIGN_ID=campaign-2026-08-24-qset-v4' "$writer_env" >/dev/null
grep -Fx 'EXECUTION_MODE=paper' "$writer_env" >/dev/null
grep -Fx 'ALLOW_LIVE=false' "$writer_env" >/dev/null
grep -Fx 'SHADOW_CODE_FREEZE_FINALIZED=' "$writer_env" >/dev/null
grep -Fx 'POLYEDGE_QSET_V4_WRITER_IMAGE=' "$writer_env" >/dev/null
grep -Fx 'POLYEDGE_QSET_V4_WRITER_GIT_SHA=' "$writer_env" >/dev/null
grep -Fx 'AZURE_STORAGE_CONTAINER_NAME=polyedge-shadow-qset-v4-events' "$writer_env" >/dev/null
grep -Fx 'AZURE_RESEARCH_STORAGE_CONTAINER_NAME=polyedge-research-qset-v4' "$writer_env" >/dev/null
grep -Fx 'POLYEDGE_QSET_V4_CONTROL_CONTAINER_NAME=polyedge-qset-v4-control' "$writer_env" >/dev/null
grep -Fx 'AZURE_STORAGE_TABLE_NAME=ShadowQsetV4EventIndex' "$writer_env" >/dev/null
grep -Fx 'AZURE_CHART_TABLE_NAME=ShadowQsetV4ChartSeries' "$writer_env" >/dev/null
grep -Fx 'AZURE_MARKET_TABLE_NAME=ShadowQsetV4MarketCatalog' "$writer_env" >/dev/null
grep -Fx 'AZURE_EVENT_BLOB_PREFIX=shadow-events/preflight/campaign-2026-08-24-qset-v4' "$writer_env" >/dev/null
grep -Fx 'AZURE_EVENT_BLOB_PREFIX_AFTER_CUTOVER=shadow-events/campaign-2026-08-24-qset-v4' "$writer_env" >/dev/null
grep -Fx 'AZURE_EVENT_BLOB_PREFIX_CUTOVER_UTC=2026-08-24T00:00:00Z' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_INTENT_OPERATOR_DIRECT=true' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_INTENT_POINTER_ONLY_PREFLIGHT=true' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_CANARY_INTENT_PREFIX=control/strategy-canary/intents/campaign-2026-08-24-qset-v4/intents' "$writer_env" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_ENABLED=false' "$writer_env" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_NAMESPACE=' "$writer_env" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_QUEUE=' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI=azure://stpolyedge6urdjr5nmwx7w/polyedge-research-qset-v4/reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json' "$writer_env" >/dev/null
grep -Fx 'STRATEGY_CANARY_EXECUTION_MODEL_SHA256=sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4' "$writer_env" >/dev/null
grep -Fx 'SHADOW_CAMPAIGN_ID=campaign-2026-08-24-qset-v4' "$sealer_env" >/dev/null
grep -Fx 'SHADOW_CODE_FREEZE_FINALIZED=' "$sealer_env" >/dev/null
grep -Fx 'SHADOW_CODE_FREEZE_SHA256=' "$sealer_env" >/dev/null
grep -Fx 'SHADOW_CODE_FREEZE_MANIFEST=' "$sealer_env" >/dev/null
grep -Fx 'AZURE_STORAGE_CONTAINER_NAME=polyedge-shadow-qset-v4-events' "$sealer_env" >/dev/null
grep -Fx 'AZURE_RESEARCH_STORAGE_CONTAINER_NAME=polyedge-research-qset-v4' "$sealer_env" >/dev/null
grep -Fx 'POLYEDGE_QSET_V4_CONTROL_CONTAINER_NAME=polyedge-qset-v4-control' "$sealer_env" >/dev/null
grep -Fx 'ContainerName=polyedge-shadow-qset-v4' "$writer" >/dev/null
grep -Fx 'EnvironmentFile=/etc/polyedge/shadow-qset-v4.env' "$writer" >/dev/null
grep -Fx 'Volume=/run/polyedge-federated-shadow-qset-v3-writer:/run/credentials:ro,Z' "$writer" >/dev/null
grep -Fx 'Requires=network-online.target polyedge-federated-token@shadow-qset-v3-writer.service' "$writer" >/dev/null
grep -F -- '--cpus=0.5 --memory=1g --pids-limit=512' "$writer" >/dev/null
grep -F -- '--read-only --tmpfs=/tmp:rw,noexec,nosuid,size=64m --cap-drop=all --pull=never --log-driver=journald' "$writer" >/dev/null
if grep -q '^PublishPort=' "$writer"; then echo 'qset-v4 writer must not publish a host port' >&2; exit 1; fi
grep -Fx 'OnCalendar=2026-08-26 02:15:00 UTC' "$timer" >/dev/null
grep -Fx 'Persistent=true' "$timer" >/dev/null
grep -Fx 'Unit=polyedge-qset-v4-seal-days.service' "$timer" >/dev/null
grep -Fx 'ExecStart=/usr/bin/flock -w 300 /run/polyedge/research.lock /usr/local/libexec/polyedge-qset-v4-seal-days' "$service" >/dev/null
grep -F 'shadow-qset-v3-writer' "$token" >/dev/null
grep -F 'for date in 2026-08-24 2026-08-25' "$sealer" >/dev/null
for accepted_date in 2026-08-24 2026-08-25; do QSET_V4_DATE_VALIDATION_ONLY=true sh "$sealer" "$accepted_date"; done
for rejected_date in 2026-10-23 ""; do if QSET_V4_DATE_VALIDATION_ONLY=true sh "$sealer" "$rejected_date" >/dev/null 2>&1; then echo "qset-v4 sealer accepted invalid date: $rejected_date" >&2; exit 1; fi; done
grep -F 'seal-qset-v4-day' "$sealer" >/dev/null
grep -F 'SHADOW_CODE_FREEZE_FINALIZED:-}" = true' "$sealer" >/dev/null
grep -F 'polyedge-shadow-qset-v4-events' "$sealer" >/dev/null
grep -F 'polyedge-research-qset-v4' "$sealer" >/dev/null
grep -F 'polyedge-qset-v4-control' "$sealer" >/dev/null
preflight_line=$(grep -n 'run_sealer "$date" --validate-only' "$sealer" | cut -d: -f1)
fence_line=$(grep -n 'systemctl stop "$writer"' "$sealer" | cut -d: -f1)
seal_line=$(grep -n 'run_sealer "$date" >"$temporary"' "$sealer" | cut -d: -f1)
[ "$preflight_line" -lt "$fence_line" ] && [ "$fence_line" -lt "$seal_line" ]
grep -F 'and (has("generated_ts") | not) and (has("sealed_at") | not)' "$sealer" >/dev/null
grep -Fx 'User=983:979' "$writer" >/dev/null
grep -F -- '--user 983:979' "$sealer" >/dev/null
grep -F -- '--source-freeze-blob "$EXECUTION_FREEZE_ARTIFACT_PATH" --source-freeze-sha256 "$EXECUTION_FREEZE_SHA256"' "$sealer" >/dev/null
grep -F 'source_freeze == {container:"polyedge-qset-v4-control",blob:$freeze_blob,sha256:$freeze,verified:true}' "$sealer" >/dev/null
grep -Fx 'ExecStart=/usr/local/libexec/polyedge-qset-v4-boundary-guard %i' "$boundary_service" >/dev/null
grep -Fx 'Requires=network-online.target polyedge-federated-token@shadow-qset-v3-writer.service' "$boundary_service" >/dev/null
! grep -F 'Conflicts=polyedge-shadow-qset-v3.service' "$boundary_service" >/dev/null
! grep -F 'Requires=network-online.target polyedge-federated-token@shadow-qset-v3-writer.service polyedge-shadow-qset-v4.service' "$boundary_service" >/dev/null
grep -Fx 'OnCalendar=2026-08-23 23:59:30 UTC' "$boundary_pre" >/dev/null
grep -Fx 'Unit=polyedge-qset-v4-boundary@pre.service' "$boundary_pre" >/dev/null
grep -Fx 'OnCalendar=2026-08-24 00:01:30 UTC' "$boundary_post" >/dev/null
grep -Fx 'Unit=polyedge-qset-v4-boundary@post.service' "$boundary_post" >/dev/null
grep -F 'polyedge-qset-v3-boundary-pre.timer' "$guard" >/dev/null
grep -F 'polyedge-qset-v3-boundary-post.timer' "$guard" >/dev/null
grep -F 'polyedge-qset-v3-first-seal.timer' "$guard" >/dev/null
grep -F '.intent_publisher == {configured:true,prepared:true,pointer_only_preflight:true}' "$guard" >/dev/null
grep -F 'systemctl disable --now "$v2_seal_timer"' "$guard" >/dev/null
grep -F 'azureEvidenceMutationPerformed:false' "$guard" >/dev/null
receipt_test_dir=$(mktemp -d)
printf '{"schema":"polyedge.qset_v4_boundary_pre.v1"}\n' >"$receipt_test_dir/pre.json"
receipt_test_sha="sha256:$(sha256sum "$receipt_test_dir/pre.json" | cut -d ' ' -f1)"
jq -n --arg sha "$receipt_test_sha" '{schema:"polyedge.qset_v4_boundary_post.v1",boundaryUtc:"2026-08-24T00:00:00Z",preReceiptSha256:$sha,writerContinued:true,qsetV3:{stoppedAndDisabled:true},qsetV2:{healthyBeforeTimerDisable:true},azureEvidenceMutationPerformed:false}' >"$receipt_test_dir/post.json"
QSET_V4_BOUNDARY_RECEIPT_TEST_ONLY=true "$guard" check "$receipt_test_dir/pre.json" "$receipt_test_dir/post.json"
if QSET_V4_BOUNDARY_RECEIPT_TEST_ONLY=true "$guard" check "$receipt_test_dir/post.json" "$receipt_test_dir/pre.json" >/dev/null 2>&1; then echo 'qset-v4 boundary accepted an invalid post checkpoint' >&2; exit 1; fi
rm -rf "$receipt_test_dir"
jq -e '.generated_at == "2026-08-22T00:00:00Z" and .active_campaign.activation_status == "planned_draft_not_effective" and .active_campaign.effective_at_utc == "2026-08-24T00:00:00Z"' "$disposition" >/dev/null
jq -r '.protected_files[]' "$policy" | while IFS= read -r protected; do test -f "$repo/$protected"; done
grep -F 'research/run_shadow_daily.sh' "$policy" >/dev/null
grep -F 'research/verify_shadow_daily_bundle.sh' "$policy" >/dev/null
grep -F 'data_quality/exclusion_windows.yaml' "$policy" >/dev/null
grep -F 'Dockerfile.rust' "$policy" >/dev/null
grep -F 'ops/conduit/bin/polyedge-qset-v4-source-freeze' "$policy" >/dev/null
grep -F 'ops/conduit/bin/polyedge-qset-v4-rbac-handoff' "$policy" >/dev/null
! grep -Fx '    "ops/conduit/systemd/",' "$policy" >/dev/null
grep -F 'Conflicts=polyedge-shadow-qset-v3.service' "$writer" >/dev/null
grep -F 'Requires=network-online.target polyedge-federated-token@shadow-qset-v3-writer.service' "$writer" >/dev/null
grep -F 'Conflicts=polyedge-shadow-qset-v3.service' "$service" >/dev/null
grep -F 'qset-v4 writer and sealer freeze hashes differ' "$sealer" >/dev/null
grep -F 'qset-v4 refuses date after terminal 2026-10-22' "$sealer" >/dev/null
grep -F 'assert_exact_eight' "$handoff" >/dev/null
grep -F 'az role assignment delete --ids' "$handoff" >/dev/null
grep -F 'restore_old_assignments' "$handoff" >/dev/null
grep -F 'delete_v4_assignments' "$handoff" >/dev/null
grep -F 'assert_exact_eight qset-v4 "$after"' "$handoff" >/dev/null
grep -F '/subscriptions/$subscription_id/providers/Microsoft.Authorization/roleDefinitions/' "$handoff" >/dev/null
grep -F 'retired_seal_service' "$handoff" >/dev/null
grep -F 'az containerapp job list' "$handoff" >/dev/null
grep -F 'v4-assignments-after.json' "$handoff" >/dev/null
grep -F 'assignment_is_restored' "$handoff" >/dev/null
grep -F 'reconcile_rollback' "$handoff" >/dev/null
! grep -F 'reconcile_rollback || true' "$handoff" >/dev/null
grep -F 'delete_v4_assignments || return 1' "$handoff" >/dev/null
grep -F 'CRITICAL: qset-v4 RBAC rollback failed' "$handoff" >/dev/null
grep -F 'rollback-result.json' "$handoff" >/dev/null
grep -F 'if test -e "$before_receipt"' "$handoff" >/dev/null
grep -F -- '--condition-version' "$handoff" >/dev/null
grep -F -- '--parameters deployProcessorJob=false' "$handoff" >/dev/null
! grep -F 'V4_PROCESSOR_IMAGE:?V4_PROCESSOR_IMAGE is required' "$handoff" >/dev/null
grep -F 'V4_ACCESS_PROBE_GIT_SHA:?V4_ACCESS_PROBE_GIT_SHA is required' "$handoff" >/dev/null
grep -F 'oldContainersDenied:true,v4WriterStarted:false' "$handoff" >/dev/null
grep -F 'prove_old_containers_denied' "$handoff" >/dev/null
grep -F 'lock-and-upload' "$freeze_builder" >/dev/null
grep -F 'immutability-policy lock' "$freeze_builder" >/dev/null
grep -F 'git -C "$repo" show "HEAD:$file"' "$freeze_builder" >/dev/null
grep -F 'podman manifest inspect' "$freeze_builder" >/dev/null
! grep -F 'docker buildx' "$freeze_builder" >/dev/null
grep -F 'show HEAD:research/configs/campaign_freeze_2026-08-24_qset_v4.json' "$freeze_builder" >/dev/null
grep -F '.source_commit == $commit and .git_tree == $tree' "$freeze_builder" >/dev/null
grep -F 'immutabilityPolicy:{state:$policy.state,days:$policy.immutabilityPeriodSinceCreationInDays}' "$freeze_builder" >/dev/null
grep -F 'source-$digest.json' "$freeze_builder" >/dev/null
