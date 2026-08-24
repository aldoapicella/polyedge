#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd); repo=$root/../..
infra=$repo/infra/shadow-profitability-qset-v5.bicep; identity=$repo/infra/conduit-federated-identity.bicep
writer=$root/quadlets/polyedge-shadow-qset-v5.container; sealer=$root/bin/polyedge-qset-v5-seal-days
token=$root/bin/polyedge-federated-token-refresh; guard=$root/bin/polyedge-qset-v5-boundary-guard
handoff=$root/bin/polyedge-qset-v5-rbac-handoff; freeze=$root/bin/polyedge-qset-v5-source-freeze
service=$root/systemd/polyedge-qset-v5-seal-days.service; boundary_service=$root/systemd/polyedge-qset-v5-boundary@.service
boundary_pre=$root/systemd/polyedge-qset-v5-boundary-pre.timer; boundary_post=$root/systemd/polyedge-qset-v5-boundary-post.timer
writer_override=$root/systemd/polyedge-federated-token@shadow-qset-v5-writer.service.d/override.conf
processor_override=$root/systemd/polyedge-federated-token@shadow-qset-v5-processor.service.d/override.conf
policy=$repo/research/configs/campaign_freeze_2026-08-26_qset_v5.json

bash -n "$guard" "$handoff" "$freeze"; sh -n "$sealer" "$token"
grep -F 'local path=$1 expected_mode=$2 max=$3' "$guard" >/dev/null
grep -Fx "var writerIdentityName = 'id-polyedge-conduit-shadow-qset-v5-writer'" "$infra" >/dev/null
grep -Fx "var processorIdentityName = 'id-polyedge-conduit-shadow-qset-v5-processor'" "$infra" >/dev/null
grep -Fx "resource writerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {" "$infra" >/dev/null
grep -Fx "resource processorIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {" "$infra" >/dev/null
! grep -Eq 'Microsoft\.App|processorJob|deployProcessorJob|processorImage|expectedSourceRevision' "$infra"
compiled=$(mktemp); trap 'rm -f "$compiled"' EXIT HUP INT TERM
az bicep build --file "$infra" --stdout >"$compiled"
jq -e '(.resources|length)==16 and ([.resources[]|select(.type=="Microsoft.Authorization/roleAssignments")]|length)==9 and all(.resources[];(.type|test("Microsoft.App|Microsoft.Compute"))|not)' "$compiled" >/dev/null

for lane in writer processor; do
  grep -F "'shadow-qset-v5-$lane'" "$identity" >/dev/null
  grep -F "id-polyedge-conduit-shadow-qset-v5-$lane" "$identity" >/dev/null
  grep -F "fic-spire-conduit-shadow-qset-v5-$lane" "$identity" >/dev/null
  grep -F "spiffe://polyedge.local/conduit/shadow-qset-v5-$lane" "$identity" >/dev/null
  grep -F "shadow-qset-v5-$lane" "$token" >/dev/null
done
grep -Fx 'User=976' "$writer_override" >/dev/null; grep -Fx 'Group=976' "$writer_override" >/dev/null
grep -Fx 'User=975' "$processor_override" >/dev/null; grep -Fx 'Group=975' "$processor_override" >/dev/null
grep -Fx 'User=976:976' "$writer" >/dev/null
grep -F -- '--user 976:976 --cpus' "$sealer" >/dev/null
grep -F '$POLYEDGE_QSET_V5_WRITER_IMAGE|976:976|running|healthy' "$guard" >/dev/null
grep -F -- '-socketPath /run/spire-server/api.sock' "$root/QSET_V5_OCI_RUNBOOK.md" >/dev/null
grep -Fx 'Volume=/run/polyedge-federated-shadow-qset-v5-writer:/run/credentials:ro,Z' "$writer" >/dev/null
grep -Fx 'Requires=network-online.target polyedge-federated-token@shadow-qset-v5-writer.service' "$writer" >/dev/null
! grep -F 'shadow-qset-v3-writer' "$writer"; ! grep -F 'Conflicts=polyedge-shadow-qset-v3.service' "$writer"
grep -F '/run/polyedge-federated-shadow-qset-v5-writer' "$sealer" >/dev/null
grep -F '/run/polyedge-federated-shadow-qset-v5-writer' "$service" >/dev/null
! grep -F 'shadow-qset-v3-writer' "$sealer"; ! grep -F 'Conflicts=polyedge-shadow-qset-v3.service' "$service"

! grep -Eq '^Requires=.*polyedge-shadow-qset-v5\.service' "$service"
grep -Fx 'sync -f "$receipt_root"' "$sealer" >/dev/null
grep -F 'id-polyedge-conduit-shadow-qset-v5-writer' "$handoff" >/dev/null
grep -F 'id-polyedge-conduit-shadow-qset-v5-processor' "$handoff" >/dev/null
grep -F 'v5PrincipalsHaveZeroAssignments:true' "$handoff" >/dev/null
grep -F 'resources:16,roleAssignments:9,computeResources:0' "$handoff" >/dev/null
grep -F 'assert_campaign_assignments 5' "$handoff" >/dev/null
grep -F 'assert_api_reader' "$handoff" >/dev/null
grep -F 'polyedge-shadow-events polyedge-shadow-qset-events' "$handoff" >/dev/null
grep -F 'polyedge-shadow-qset-v3-events' "$handoff" >/dev/null
grep -F 'polyedge-funded-evidence' "$handoff" >/dev/null
grep -F 'vault.azure.net' "$handoff" >/dev/null; grep -F 'servicebus.windows.net' "$handoff" >/dev/null
grep -F 'v3AssignmentsRemoved:0' "$handoff" >/dev/null; grep -F 'v5WriterStarted:false' "$handoff" >/dev/null
grep -F 'containersTablesAndEvidenceRetained:true' "$handoff" >/dev/null
! grep -F 'az role assignment delete' "$handoff" | grep -F 'qset-v3'
! grep -F 'systemctl start "$v5_writer_service"' "$handoff"

grep -F 'polyedge.qset_v5_source_freeze_upload_receipt.v2' "$freeze" >/dev/null
for field in 'manifest:{uri:$uri' 'researchImage:$image' 'sourceCommit:$commit' 'gitTree:$tree'; do grep -F "$field" "$freeze" >/dev/null; done
grep -F 'source-$digest.json' "$freeze" >/dev/null; grep -F 'chmod 0640' "$freeze" >/dev/null
grep -F 'polyedge.qset_v5_source_freeze_upload_receipt.v2' "$guard" >/dev/null
grep -F '.researchImage==$writerImage' "$guard" >/dev/null; grep -F '.sourceCommit==$writerCommit' "$guard" >/dev/null
grep -F 'polyedge.qset_v5_source_freeze_upload_receipt.v2' "$sealer" >/dev/null
grep -F '.researchImage==$sealImage' "$sealer" >/dev/null; grep -F '.sourceCommit==$sealCommit' "$sealer" >/dev/null

grep -F 'systemctl is-active --quiet "$v3_service"' "$guard" >/dev/null
grep -F 'polyedge-qset-v3-first-seal.timer' "$guard" >/dev/null
grep -F 'disabled "$v2_seal_timer"' "$guard" >/dev/null
grep -F 'systemctl is-active --quiet "$v2_service"' "$guard" >/dev/null
! grep -Eq 'systemctl (start|stop|restart|enable|disable) ' "$guard"
! grep -F 'Requires=' "$boundary_service"; ! grep -F 'Conflicts=' "$boundary_service"
grep -Fx 'OnCalendar=2026-08-25 23:59:30 UTC' "$boundary_pre" >/dev/null
grep -Fx 'OnCalendar=2026-08-26 00:01:30 UTC' "$boundary_post" >/dev/null
grep -F 'stateMutationPerformed:false' "$guard" >/dev/null

receipt_dir=$(mktemp -d); printf '{"schema":"polyedge.qset_v5_boundary_pre.v2"}\n' >"$receipt_dir/pre.json"; sha="sha256:$(sha256sum "$receipt_dir/pre.json"|cut -d' ' -f1)"
jq -n --arg sha "$sha" '{schema:"polyedge.qset_v5_boundary_post.v2",boundaryUtc:"2026-08-26T00:00:00Z",preReceiptSha256:$sha,writerContinued:true,qsetV3:{activeHealthyUnchanged:true,timersDisabled:true},qsetV2:{activeHealthy:true,firstSealTimerDisabled:true},stateMutationPerformed:false,azureEvidenceMutationPerformed:false}' >"$receipt_dir/post.json"
QSET_V5_BOUNDARY_RECEIPT_TEST_ONLY=true "$guard" check "$receipt_dir/pre.json" "$receipt_dir/post.json"
if QSET_V5_BOUNDARY_RECEIPT_TEST_ONLY=true "$guard" check "$receipt_dir/post.json" "$receipt_dir/pre.json" >/dev/null 2>&1; then echo 'invalid boundary receipt accepted' >&2; exit 1; fi
rm -rf "$receipt_dir"

for date in 2026-08-26 2026-08-27; do QSET_V5_DATE_VALIDATION_ONLY=true "$sealer" "$date"; done
for date in 2026-08-23 2026-10-23 ''; do if QSET_V5_DATE_VALIDATION_ONLY=true "$sealer" "$date" >/dev/null 2>&1; then echo "invalid seal date accepted: $date" >&2; exit 1; fi; done
jq -r '.protected_files[]' "$policy" | while IFS= read -r protected; do test -f "$repo/$protected"; done
for protected in infra/conduit-federated-identity.bicep ops/conduit/systemd/polyedge-federated-token@shadow-qset-v5-writer.service.d/override.conf ops/conduit/systemd/polyedge-federated-token@shadow-qset-v5-processor.service.d/override.conf; do jq -e --arg file "$protected" '.protected_files|index($file)' "$policy" >/dev/null; done
