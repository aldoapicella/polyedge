#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
runner=$root/bin/polyedge-run-job
preflight=$root/bin/polyedge-qset-v6-processor-preflight
handoff=$root/bin/polyedge-qset-v6-processor-handoff
env_file=$root/env/qset-v6-processor.env.example
service=$root/systemd/polyedge-qset-v6-processor.service
mapping=$root/compute-plane-mapping.json
disk_guard=$root/bin/polyedge-boot-disk-guard

sh -n "$runner" "$preflight" "$handoff"
if env -i PATH="$PATH" "$preflight" >/dev/null 2>&1; then
  echo 'qset-v6 processor preflight accepted missing bindings' >&2
  exit 1
fi
grep -F '  qset-v6-processor)' "$runner" >/dev/null
grep -F 'cpus=4 memory=8g limit=16800' "$runner" >/dev/null
grep -F '/usr/local/libexec/polyedge-qset-v6-processor-preflight' "$runner" >/dev/null
grep -F 'credential=shadow-qset-v6-processor' "$runner" >/dev/null
grep -F 'qset processor federated token is missing or unsafe' "$runner" >/dev/null
grep -F -- '--user "$token_uid:$token_gid" --read-only --tmpfs=/tmp:rw,noexec,nosuid,size=64m --cap-drop=all --entrypoint='"'"'["/usr/bin/setpriv","--no-new-privs"]'"'"'' "$runner" >/dev/null
grep -F -- '--pull=never --log-driver=journald' "$runner" >/dev/null
grep -F 'daily|replay|prospective|chart-backfill|backfill|shadow-qset|qset-v4-processor|qset-v5-processor|qset-v6-processor)' "$runner" >/dev/null
grep -F '2026-09-01 --source-freeze-blob' "$runner" >/dev/null
grep -F '2026-09-02 --source-freeze-blob' "$runner" >/dev/null
grep -F '/app/research/run_shadow_daily_v6.sh' "$runner" >/dev/null
mount_line=$(grep -n 'mountpoint -q "$ring"' "$runner" | cut -d: -f1)
preflight_line=$(grep -n '/usr/local/libexec/polyedge-qset-v6-processor-preflight' "$runner" | cut -d: -f1)
podman_line=$(grep -n '/usr/bin/podman run $remove_after' "$runner" | cut -d: -f1)
[ "$mount_line" -lt "$preflight_line" ] && [ "$preflight_line" -lt "$podman_line" ]

grep -F '16106127360' "$disk_guard" >/dev/null
grep -F 'image-pulls-paused' "$disk_guard" >/dev/null
grep -F 'source-${EXECUTION_FREEZE_SHA256#sha256:}.json' "$preflight" >/dev/null
grep -F 'qset-v6-seal/2026-09-01.json' "$preflight" >/dev/null
grep -F 'qset-v6-seal/2026-09-02.json' "$preflight" >/dev/null
grep -F '.immutabilityPolicy.state == "Locked" and .immutabilityPolicy.days >= 90' "$preflight" >/dev/null
grep -F '.blob_count == 1440 and .sealed_blob_count == 1440 and .all_sealed == true' "$preflight" >/dev/null
grep -F 'polyedge.qset_v6_source_freeze_upload_receipt.v2' "$preflight" >/dev/null
grep -F '.researchImage == $image and .sourceCommit == $commit' "$preflight" >/dev/null
grep -F 'refuses funded, Service Bus, Key Vault, or client-secret bindings' "$preflight" >/dev/null

for exact in \
  'RUN_BOT_ON_STARTUP=false' \
  'ALLOW_LIVE=false' \
  'AZURE_STORAGE_CONTAINER_NAME=polyedge-research-qset-v6' \
  'SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-qset-v6-events' \
  'QSET_V6_CONTROL_CONTAINER_NAME=polyedge-qset-v6-control' \
  'FUNDED_DIRECT_SERVICE_BUS_ENABLED=false' \
  'AZURE_FUNDED_STORAGE_CONTAINER_NAME=' \
  'AZURE_KEY_VAULT_URL=' \
  'AZURE_CLIENT_SECRET='; do
  grep -Fx "$exact" "$env_file" >/dev/null
done
! grep -F 'AZURE_CLIENT_SECRET_FILE=' "$env_file" >/dev/null

grep -Fx 'Requires=network-online.target polyedge-network.service polyedge-federated-token@shadow-qset-v6-processor.service' "$service" >/dev/null
grep -Fx 'ConditionPathExists=/etc/polyedge/ENABLE_QSET_V6_PROCESSOR_MANUAL' "$service" >/dev/null
grep -Fx 'ExecStartPre=/usr/bin/rm -f /etc/polyedge/ENABLE_QSET_V6_PROCESSOR_MANUAL' "$service" >/dev/null
grep -Fx 'ExecStartPre=/usr/bin/sync -f /etc/polyedge' "$service" >/dev/null
grep -Fx 'CPUQuota=400%' "$service" >/dev/null
grep -Fx 'MemoryMax=8G' "$service" >/dev/null
grep -Fx 'TasksMax=1024' "$service" >/dev/null
! grep -F '[Install]' "$service" >/dev/null
! test -e "$root/systemd/polyedge-qset-v6-processor.timer"

jq -e '
  .azureJobCount == (.jobs | length)
  and ([.ociOnlyJobs[] | select(.name == "qset-v6-processor")] == [{
    name:"qset-v6-processor",classification:"configured_manual_only_not_executed",
    ociUnit:"polyedge-qset-v6-processor.service",identityLane:"shadow-qset-v6-processor",
    azureContainerAppsJob:null,azureProcessorJobDeploymentAllowed:false,
    manualFirstExecution:true,recurringEnabled:false,timerUnit:null,imagePullPolicy:"never",
    minimumDiskHeadroomBytes:16106127360,
    resources:{cpu:4,memory:"8GiB",pids:1024},
    requiredInputs:{sourceFreezeReceipt:"/srv/polyedge-ring/migration/qset-v6/source-freeze/source-<final-sha256>.json",sealedDayReceipts:["/srv/polyedge-ring/migration/qset-v6-seal/2026-09-01.json","/srv/polyedge-ring/migration/qset-v6-seal/2026-09-02.json"]},
    requiredProofBeforeFirstExecution:["final_source_freeze_receipt_hash_image_revision_binding","two_exact_closed_day_receipt_and_inventory_hashes","local_linux_arm64_image_revision","dedicated_federated_token"],
    requiredProofBeforeRecurringEnablement:["manual_processor_success","verified_output_hash_and_readback","negative_access_probe","resource_and_disk_guard_evidence"]
  }])
  and (.protectedTrustRules.shadowQsetV6Processor | contains("no funded, qset-v1/v2/v3/v4/v5, Key Vault, Service Bus"))
' "$mapping" >/dev/null
"$root/test/test-qset-v6-processor-handoff.sh"
