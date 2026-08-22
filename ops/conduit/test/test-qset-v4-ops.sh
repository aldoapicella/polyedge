#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd); repo=$root/../..
infra=$repo/infra/shadow-profitability-qset-v4.bicep; identity=$repo/infra/conduit-federated-identity.bicep
writer=$root/quadlets/polyedge-shadow-qset-v4.container; sealer=$root/bin/polyedge-qset-v4-seal-days
token=$root/bin/polyedge-federated-token-refresh; guard=$root/bin/polyedge-qset-v4-boundary-guard
handoff=$root/bin/polyedge-qset-v4-rbac-handoff; freeze=$root/bin/polyedge-qset-v4-source-freeze
service=$root/systemd/polyedge-qset-v4-seal-days.service; boundary_service=$root/systemd/polyedge-qset-v4-boundary@.service
writer_override=$root/systemd/polyedge-federated-token@shadow-qset-v4-writer.service.d/override.conf
processor_override=$root/systemd/polyedge-federated-token@shadow-qset-v4-processor.service.d/override.conf
policy=$repo/research/configs/campaign_freeze_2026-08-24_qset_v4.json

bash -n "$guard" "$handoff" "$freeze"; sh -n "$sealer" "$token"
grep -Fx "var writerIdentityName = 'id-polyedge-conduit-shadow-qset-v4-writer'" "$infra" >/dev/null
grep -Fx "var processorIdentityName = 'id-polyedge-conduit-shadow-qset-v4-processor'" "$infra" >/dev/null
grep -Fx "resource writerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {" "$infra" >/dev/null
grep -Fx "resource processorIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {" "$infra" >/dev/null
! grep -Eq 'Microsoft\.App|processorJob|deployProcessorJob|processorImage|expectedSourceRevision' "$infra"
compiled=$(mktemp); trap 'rm -f "$compiled"' EXIT HUP INT TERM
az bicep build --file "$infra" --stdout >"$compiled"
jq -e '(.resources|length)==16 and ([.resources[]|select(.type=="Microsoft.Authorization/roleAssignments")]|length)==9 and all(.resources[];(.type|test("Microsoft.App|Microsoft.Compute"))|not)' "$compiled" >/dev/null

for lane in writer processor; do
  grep -F "'shadow-qset-v4-$lane'" "$identity" >/dev/null
  grep -F "id-polyedge-conduit-shadow-qset-v4-$lane" "$identity" >/dev/null
  grep -F "fic-spire-conduit-shadow-qset-v4-$lane" "$identity" >/dev/null
  grep -F "spiffe://polyedge.local/conduit/shadow-qset-v4-$lane" "$identity" >/dev/null
  grep -F "shadow-qset-v4-$lane" "$token" >/dev/null
done
grep -Fx 'User=982' "$writer_override" >/dev/null; grep -Fx 'Group=978' "$writer_override" >/dev/null
grep -Fx 'User=981' "$processor_override" >/dev/null; grep -Fx 'Group=977' "$processor_override" >/dev/null
grep -Fx 'Volume=/run/polyedge-federated-shadow-qset-v4-writer:/run/credentials:ro,Z' "$writer" >/dev/null
grep -Fx 'Requires=network-online.target polyedge-federated-token@shadow-qset-v4-writer.service' "$writer" >/dev/null
! grep -F 'shadow-qset-v3-writer' "$writer"; ! grep -F 'Conflicts=polyedge-shadow-qset-v3.service' "$writer"
grep -F '/run/polyedge-federated-shadow-qset-v4-writer' "$sealer" >/dev/null
grep -F '/run/polyedge-federated-shadow-qset-v4-writer' "$service" >/dev/null
! grep -F 'shadow-qset-v3-writer' "$sealer"; ! grep -F 'Conflicts=polyedge-shadow-qset-v3.service' "$service"

grep -F 'id-polyedge-conduit-shadow-qset-v4-writer' "$handoff" >/dev/null
grep -F 'id-polyedge-conduit-shadow-qset-v4-processor' "$handoff" >/dev/null
grep -F 'v4PrincipalsHaveZeroAssignments:true' "$handoff" >/dev/null
grep -F 'resources:16,roleAssignments:9,computeResources:0' "$handoff" >/dev/null
grep -F 'assert_campaign_assignments 4' "$handoff" >/dev/null
grep -F 'assert_api_reader' "$handoff" >/dev/null
grep -F 'polyedge-shadow-events polyedge-shadow-qset-events' "$handoff" >/dev/null
grep -F 'polyedge-shadow-qset-v3-events' "$handoff" >/dev/null
grep -F 'polyedge-funded-evidence' "$handoff" >/dev/null
grep -F 'vault.azure.net' "$handoff" >/dev/null; grep -F 'servicebus.windows.net' "$handoff" >/dev/null
grep -F 'v3AssignmentsRemoved:0' "$handoff" >/dev/null; grep -F 'v4WriterStarted:false' "$handoff" >/dev/null
grep -F 'containersTablesAndEvidenceRetained:true' "$handoff" >/dev/null
! grep -F 'az role assignment delete' "$handoff" | grep -F 'qset-v3'
! grep -F 'systemctl start "$v4_writer_service"' "$handoff"

grep -F 'polyedge.qset_v4_source_freeze_upload_receipt.v2' "$freeze" >/dev/null
for field in 'manifest:{uri:$uri' 'researchImage:$image' 'sourceCommit:$commit' 'gitTree:$tree'; do grep -F "$field" "$freeze" >/dev/null; done
grep -F 'source-$digest.json' "$freeze" >/dev/null; grep -F 'chmod 0640' "$freeze" >/dev/null
grep -F 'polyedge.qset_v4_source_freeze_upload_receipt.v2' "$guard" >/dev/null
grep -F '.researchImage==$writerImage' "$guard" >/dev/null; grep -F '.sourceCommit==$writerCommit' "$guard" >/dev/null
grep -F 'polyedge.qset_v4_source_freeze_upload_receipt.v2' "$sealer" >/dev/null
grep -F '.researchImage==$sealImage' "$sealer" >/dev/null; grep -F '.sourceCommit==$sealCommit' "$sealer" >/dev/null

grep -F 'systemctl is-active --quiet "$v3_service"' "$guard" >/dev/null
grep -F 'polyedge-qset-v3-first-seal.timer' "$guard" >/dev/null
grep -F 'disabled "$v2_seal_timer"' "$guard" >/dev/null
grep -F 'systemctl is-active --quiet "$v2_service"' "$guard" >/dev/null
! grep -Eq 'systemctl (start|stop|restart|enable|disable) ' "$guard"
! grep -F 'Requires=' "$boundary_service"; ! grep -F 'Conflicts=' "$boundary_service"
grep -F 'stateMutationPerformed:false' "$guard" >/dev/null

receipt_dir=$(mktemp -d); printf '{"schema":"polyedge.qset_v4_boundary_pre.v2"}\n' >"$receipt_dir/pre.json"; sha="sha256:$(sha256sum "$receipt_dir/pre.json"|cut -d' ' -f1)"
jq -n --arg sha "$sha" '{schema:"polyedge.qset_v4_boundary_post.v2",boundaryUtc:"2026-08-24T00:00:00Z",preReceiptSha256:$sha,writerContinued:true,qsetV3:{activeHealthyUnchanged:true,timersDisabled:true},qsetV2:{activeHealthy:true,firstSealTimerDisabled:true},stateMutationPerformed:false,azureEvidenceMutationPerformed:false}' >"$receipt_dir/post.json"
QSET_V4_BOUNDARY_RECEIPT_TEST_ONLY=true "$guard" check "$receipt_dir/pre.json" "$receipt_dir/post.json"
if QSET_V4_BOUNDARY_RECEIPT_TEST_ONLY=true "$guard" check "$receipt_dir/post.json" "$receipt_dir/pre.json" >/dev/null 2>&1; then echo 'invalid boundary receipt accepted' >&2; exit 1; fi
rm -rf "$receipt_dir"

for date in 2026-08-24 2026-08-25; do QSET_V4_DATE_VALIDATION_ONLY=true "$sealer" "$date"; done
for date in 2026-08-23 2026-10-23 ''; do if QSET_V4_DATE_VALIDATION_ONLY=true "$sealer" "$date" >/dev/null 2>&1; then echo "invalid seal date accepted: $date" >&2; exit 1; fi; done
jq -r '.protected_files[]' "$policy" | while IFS= read -r protected; do test -f "$repo/$protected"; done
for protected in infra/conduit-federated-identity.bicep ops/conduit/systemd/polyedge-federated-token@shadow-qset-v4-writer.service.d/override.conf ops/conduit/systemd/polyedge-federated-token@shadow-qset-v4-processor.service.d/override.conf; do jq -e --arg file "$protected" '.protected_files|index($file)' "$policy" >/dev/null; done
