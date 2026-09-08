#!/usr/bin/env bash
set -euo pipefail

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
handoff=$repo/ops/conduit/bin/polyedge-qset-v6-rbac-handoff
test_root=$(mktemp -d)
trap 'rm -rf "$test_root"' EXIT HUP INT TERM

subscription=/subscriptions/sub
storage=$subscription/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/acct
blob_reader=$subscription/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1
blob_writer=$subscription/providers/Microsoft.Authorization/roleDefinitions/44fd8b56-84f7-403c-a44a-7aabab1d28b1
table_writer=$subscription/providers/Microsoft.Authorization/roleDefinitions/c8133837-ba14-4b6b-8f58-52ce675a33e4

make_campaign() {
  local version=$1 writer=$2 processor=$3 output_writer=$4 output_processor=$5 date raw research control event chart market campaign writer_condition processor_condition
  case "$version" in 5) date=2026-08-26 ;; 6) date=2026-09-01 ;; esac
  raw=$storage/blobServices/default/containers/polyedge-shadow-qset-v${version}-events
  research=$storage/blobServices/default/containers/polyedge-research-qset-v${version}
  control=$storage/blobServices/default/containers/polyedge-qset-v${version}-control
  event=$storage/tableServices/default/tables/ShadowQsetV${version}EventIndex
  chart=$storage/tableServices/default/tables/ShadowQsetV${version}ChartSeries
  market=$storage/tableServices/default/tables/ShadowQsetV${version}MarketCatalog
  campaign=campaign-${date}-qset-v${version}
  writer_condition="((!(ActionMatches{'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write'}) AND !(ActionMatches{'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action'})) OR ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals 'polyedge-shadow-qset-v${version}-events') AND ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'shadow-events/preflight/$campaign/') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'shadow-events/$campaign/') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'control/strategy-canary/intents/$campaign/'))))"
  processor_condition="((!(ActionMatches{'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write'}) AND !(ActionMatches{'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action'})) OR ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals 'polyedge-research-qset-v${version}') AND ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'data/research/shadow/$campaign/') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'reports/research/shadow/campaigns/$campaign/'))))"
  jq -n --arg p "$writer" --arg raw "$raw" --arg control "$control" --arg event "$event" --arg chart "$chart" --arg market "$market" --arg br "$blob_reader" --arg bw "$blob_writer" --arg tw "$table_writer" --arg condition "$writer_condition" '[
    {id:"/assign/writer-raw",scope:$raw,roleDefinitionId:$bw,principalId:$p,condition:$condition,conditionVersion:"2.0"},
    {id:"/assign/writer-control",scope:$control,roleDefinitionId:$br,principalId:$p,condition:null,conditionVersion:null},
    {id:"/assign/writer-event",scope:$event,roleDefinitionId:$tw,principalId:$p,condition:null,conditionVersion:null},
    {id:"/assign/writer-chart",scope:$chart,roleDefinitionId:$tw,principalId:$p,condition:null,conditionVersion:null},
    {id:"/assign/writer-market",scope:$market,roleDefinitionId:$tw,principalId:$p,condition:null,conditionVersion:null}]' >"$output_writer"
  jq -n --arg p "$processor" --arg raw "$raw" --arg research "$research" --arg control "$control" --arg br "$blob_reader" --arg bw "$blob_writer" --arg condition "$processor_condition" '[
    {id:"/assign/processor-raw",scope:$raw,roleDefinitionId:$br,principalId:$p,condition:null,conditionVersion:null},
    {id:"/assign/processor-research",scope:$research,roleDefinitionId:$bw,principalId:$p,condition:$condition,conditionVersion:"2.0"},
    {id:"/assign/processor-control",scope:$control,roleDefinitionId:$br,principalId:$p,condition:null,conditionVersion:null}]' >"$output_processor"
  if test "$version" = 5; then sed -i 's#/assign/writer-#/assign/v5-writer-#g;s#/assign/processor-#/assign/v5-processor-#g' "$output_writer" "$output_processor"; fi
}

make_stubs() {
  local fake=$1
  mkdir -p "$fake"
  tee "$fake/systemctl" >/dev/null <<'STUB'
#!/usr/bin/env bash
case "$1" in
  is-active) case "${@: -1}" in polyedge-shadow-qset-v5.service) exit 0 ;; *) exit 3 ;; esac ;;
  is-enabled) case "$2" in polyedge-shadow-qset-v5.service) echo enabled ;; *) echo disabled ;; esac ;;
  show) case "$*" in *MainPID*) echo 4242 ;; *) echo invocation-v5 ;; esac ;;
esac
STUB
  tee "$fake/podman" >/dev/null <<'STUB'
#!/usr/bin/env bash
echo 'container-id|image@sha256:test|running|healthy'
STUB
  tee "$fake/install" >/dev/null <<'STUB'
#!/usr/bin/env bash
mkdir -p "${@: -1}"
STUB
  tee "$fake/chown" >/dev/null <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
  tee "$fake/sync" >/dev/null <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
  tee "$fake/sleep" >/dev/null <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
  tee "$fake/stat" >/dev/null <<'STUB'
#!/usr/bin/env bash
case "$2" in
  %u:%g:%a:%h) echo 0:0:640:1 ;;
  %a:%h) echo 600:1 ;;
  %s) /usr/bin/stat -c %s "$3" ;;
  *) /usr/bin/stat "$@" ;;
esac
STUB
  tee "$fake/az" >/dev/null <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
args="$*"
arg_after() { local key=$1 previous= value; shift; for value in "$@"; do if test "$previous" = "$key"; then printf '%s' "$value"; return; fi; previous=$value; done; }
case "$args" in
  'account show'*) echo sub ;;
  'storage account show'*) echo "$STORAGE_ID" ;;
  'identity show'*)
    name=$(arg_after --name "$@")
    case "$name" in
      id-polyedge-conduit-shadow-qset-v6-writer) pid=writer-pid; cid=writer-client ;;
      id-polyedge-conduit-shadow-qset-v6-processor) pid=processor-pid; cid=processor-client ;;
      id-polyedge-conduit-shadow-qset-v5-writer) pid=v5-writer-pid; cid=v5-writer-client ;;
      id-polyedge-conduit-shadow-qset-v5-processor) pid=v5-processor-pid; cid=v5-processor-client ;;
      id-polyedge-conduit-api) pid=api-pid; cid=api-client ;;
    esac
    case "$args" in *'--query principalId'*) echo "$pid" ;; *) jq -nc --arg principalId "$pid" --arg clientId "$cid" '{principalId:$principalId,clientId:$clientId}' ;; esac ;;
  'identity federated-credential list'*)
    identity=$(arg_after --identity-name "$@")
    lane=${identity##*-v6-}; subject=spiffe://polyedge.local/conduit/shadow-qset-v6-$lane
    jq -nc --arg name "fic-spire-conduit-shadow-qset-v6-$lane" --arg subject "$subject" '[{name:$name,issuer:"https://oidc.jupiterlabs.dev",subject:$subject,audiences:["api://AzureADTokenExchange"]}]' |
      if test "${EXTRA_FIC:-}" = "$lane"; then jq '.+[{name:"extra",issuer:"https://evil.invalid",subject:"extra",audiences:["api://AzureADTokenExchange"]}]'; else cat; fi ;;
  'containerapp job list'*) echo '[]' ;;
  'role definition list'*)
    name=$(arg_after --name "$@")
    if test "$name" = 44fd8b56-84f7-403c-a44a-7aabab1d28b1; then
      actions='["Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read","Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write","Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action"]'
      if test "${DRIFT_ROLE:-}" = blob; then actions='["Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read","Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write","Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action","Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete"]'; fi
    else
      actions='["Microsoft.Storage/storageAccounts/tableServices/tables/entities/read","Microsoft.Storage/storageAccounts/tableServices/tables/entities/write","Microsoft.Storage/storageAccounts/tableServices/tables/entities/add/action","Microsoft.Storage/storageAccounts/tableServices/tables/entities/update/action"]'
    fi
    jq -nc --arg name "$name" --arg scope /subscriptions/sub --argjson data "$actions" '[{name:$name,roleType:"CustomRole",assignableScopes:[$scope],permissions:[{actions:[],notActions:[],dataActions:$data,notDataActions:[]}]}]' ;;
  'role assignment list'*) principal=$(arg_after --assignee-object-id "$@"); cat "$STATE/assign-$principal.json" ;;
  'role assignment delete'*)
    id=$(arg_after --ids "$@"); echo "$id" >>"$STATE/delete.log"
    for file in "$STATE"/assign-*.json; do jq --arg id "$id" 'map(select(.id!=$id))' "$file" >"$file.tmp"; mv "$file.tmp" "$file"; done ;;
  'deployment group create'*)
    echo deployment >>"$STATE/deployment.log"
    cp "$STATE/full-writer.json" "$STATE/assign-writer-pid.json"
    cp "$STATE/full-processor.json" "$STATE/assign-processor-pid.json"
    cp "$STATE/full-api.json" "$STATE/assign-api-pid.json" ;;
  'bicep build'*) jq -nc '{resources:[range(0;16)|{type:(if .<9 then "Microsoft.Authorization/roleAssignments" else "Microsoft.Storage/test" end)}]}' ;;
  *) echo "unexpected az: $args" >&2; exit 2 ;;
esac
STUB
  tee "$fake/curl" >/dev/null <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
output=/dev/null method=GET data= url= previous=
for value in "$@"; do
  case "$previous" in -o) output=$value ;; -X) method=$value ;; --data-binary) data=${value#@} ;; esac
  case "$value" in https://*) url=$value ;; esac
  previous=$value
done
body=''
case "$url" in
  *login.microsoftonline.com*) code=200; body='{"access_token":"test-token"}' ;;
  *table.core.windows.net*) code=204 ;;
  *polyedge-shadow-qset-v6-events/shadow-events/preflight/*)
    if test "${DENY_POSITIVE:-}" = writer-preflight; then code=403; else code=201; fi ;;
  *polyedge-shadow-qset-v6-events/shadow-events/*) code=201 ;;
  *polyedge-research-qset-v6/data/research/*)
    if test "$method" = PUT; then cp "$data" "$STATE/research-blob"; code=201; else cp "$STATE/research-blob" "$output"; output=/dev/null; code=200; fi ;;
  *polyedge-shadow-qset-v6-events\?*|*polyedge-qset-v6-control\?*) code=200; body='<EnumerationResults />' ;;
  *.vault.azure.net/*|*.servicebus.windows.net/*) code=403 ;;
  *.blob.core.windows.net/*) code=403 ;;
  *) echo "unexpected curl URL: $url" >&2; exit 2 ;;
esac
if test "$output" != /dev/null; then printf '%s' "$body" >"$output"; fi
printf '%s' "$code"
STUB
  chmod +x "$fake"/*
}

setup_case() {
  CASE=$test_root/$1; STATE=$CASE/state; RECEIPTS=$CASE/receipts; FAKE=$CASE/bin
  mkdir -p "$STATE" "$RECEIPTS"
  make_stubs "$FAKE"
  make_campaign 5 v5-writer-pid v5-processor-pid "$STATE/assign-v5-writer-pid.json" "$STATE/assign-v5-processor-pid.json"
  make_campaign 6 writer-pid processor-pid "$STATE/full-writer.json" "$STATE/full-processor.json"
  jq -n --arg scope "$storage/blobServices/default/containers/polyedge-research-qset-v6" --arg role "$blob_reader" '[{id:"/assign/api-research",scope:$scope,roleDefinitionId:$role,principalId:"api-pid",condition:null,conditionVersion:null}]' >"$STATE/full-api.json"
  printf '[]\n' >"$STATE/assign-writer-pid.json"; printf '[]\n' >"$STATE/assign-processor-pid.json"; printf '[]\n' >"$STATE/assign-api-pid.json"
  WRITER_TOKEN=$CASE/writer-token; PROCESSOR_TOKEN=$CASE/processor-token
  printf 'writer-assertion\n' >"$WRITER_TOKEN"; printf 'processor-assertion\n' >"$PROCESSOR_TOKEN"; chmod 600 "$WRITER_TOKEN" "$PROCESSOR_TOKEN"
  export STATE STORAGE_ID=$storage
}

run_handoff() {
  env PATH="$FAKE:$PATH" AZURE_RESOURCE_GROUP=rg AZURE_STORAGE_ACCOUNT_NAME=acct AZURE_TENANT_ID=tenant \
    QSET_V6_RBAC_RECEIPT_ROOT_TEST_ONLY="$RECEIPTS" QSET_V6_RBAC_WRITER_TOKEN_TEST_ONLY="$WRITER_TOKEN" QSET_V6_RBAC_PROCESSOR_TOKEN_TEST_ONLY="$PROCESSOR_TOKEN" \
    EXTRA_FIC="${EXTRA_FIC:-}" DRIFT_ROLE="${DRIFT_ROLE:-}" DENY_POSITIVE="${DENY_POSITIVE:-}" "$handoff" "$1"
}

casefold_live_resource_ids() {
  local file
  for file in "$@"; do
    jq 'map(.scope |= gsub("/resourceGroups/"; "/resourcegroups/") | .roleDefinitionId |= gsub("/Microsoft.Authorization/"; "/microsoft.authorization/"))' "$file" >"$file.tmp"
    mv "$file.tmp" "$file"
  done
}

setup_case mixed-case-live
casefold_live_resource_ids "$STATE/assign-v5-writer-pid.json" "$STATE/assign-v5-processor-pid.json" \
  "$STATE/full-writer.json" "$STATE/full-processor.json" "$STATE/full-api.json"
run_handoff check >/dev/null
run_handoff apply >/dev/null
jq -e '.schema=="polyedge.qset_v6_rbac_apply.v2" and .writerAssignments==5 and .processorAssignments==3 and .apiReaderAssignments==1' "$RECEIPTS/apply-result.json" >/dev/null
jq '(.writer[].id,.processor[].id,.apiResearchReader[].id) |= ascii_upcase' "$RECEIPTS/v6-assignments.json" >"$RECEIPTS/v6-assignments.json.tmp"
mv "$RECEIPTS/v6-assignments.json.tmp" "$RECEIPTS/v6-assignments.json"
run_handoff rollback >/dev/null
test "$(wc -l <"$STATE/delete.log")" = 9
jq -se 'all(.[];length==0)' "$STATE/assign-writer-pid.json" "$STATE/assign-processor-pid.json" "$STATE/assign-api-pid.json" >/dev/null

setup_case partial-rollback
jq '[.[0],.[2]]' "$STATE/full-writer.json" >"$STATE/assign-writer-pid.json"
jq '[.[1]]' "$STATE/full-processor.json" >"$STATE/assign-processor-pid.json"
cp "$STATE/full-api.json" "$STATE/assign-api-pid.json"
jq -n '{schema:"polyedge.qset_v6_rbac_before.v1",writerPrincipal:"writer-pid",processorPrincipal:"processor-pid",v6PrincipalsHaveZeroAssignments:true,apiV6ScopeHasZeroAssignments:true}' >"$RECEIPTS/before.json"; chmod 640 "$RECEIPTS/before.json"
jq -n --slurpfile writer "$STATE/full-writer.json" --slurpfile processor "$STATE/full-processor.json" --slurpfile api "$STATE/full-api.json" '{schema:"polyedge.qset_v6_exact_assignments.v1",writer:$writer[0],processor:$processor[0],apiResearchReader:$api[0]}' >"$RECEIPTS/v6-assignments.json"; chmod 640 "$RECEIPTS/v6-assignments.json"
run_handoff rollback >/dev/null
test "$(wc -l <"$STATE/delete.log")" = 4
jq -se 'all(.[];length==0)' "$STATE/assign-writer-pid.json" "$STATE/assign-processor-pid.json" "$STATE/assign-api-pid.json" >/dev/null
jq -e '.schema=="polyedge.qset_v6_rbac_rollback.v2" and .exactAssignmentsRemovedThisRun==4 and .exactV6AndApiAssignmentsAbsent==9' "$RECEIPTS/rollback-result.json" >/dev/null
jq -e 'length==5' "$STATE/assign-v5-writer-pid.json" >/dev/null; jq -e 'length==3' "$STATE/assign-v5-processor-pid.json" >/dev/null
run_handoff rollback >/dev/null

setup_case partial-apply
jq '[.[0]]' "$STATE/full-writer.json" >"$STATE/assign-writer-pid.json"
jq '[.[1]]' "$STATE/full-processor.json" >"$STATE/assign-processor-pid.json"
jq -n '{schema:"polyedge.qset_v6_rbac_before.v1",writerPrincipal:"writer-pid",processorPrincipal:"processor-pid",v6PrincipalsHaveZeroAssignments:true,apiV6ScopeHasZeroAssignments:true}' >"$RECEIPTS/before.json"; chmod 640 "$RECEIPTS/before.json"
run_handoff rollback >/dev/null
test "$(wc -l <"$STATE/delete.log")" = 2
jq -se 'all(.[];length==0)' "$STATE/assign-writer-pid.json" "$STATE/assign-processor-pid.json" "$STATE/assign-api-pid.json" >/dev/null
test ! -e "$RECEIPTS/v6-assignments.json"

setup_case verify-live
cp "$STATE/full-writer.json" "$STATE/assign-writer-pid.json"
cp "$STATE/full-processor.json" "$STATE/assign-processor-pid.json"
cp "$STATE/full-api.json" "$STATE/assign-api-pid.json"
run_handoff verify-live | jq -e '
  .schema=="polyedge.qset_v6_rbac_verify_live.v1"
  and .writerAssignments==5 and .processorAssignments==3 and .apiReaderAssignments==1
  and (.v1ThroughV5FundedKeyVaultAndServiceBusDenied|length==2)
  and ([.v1ThroughV5FundedKeyVaultAndServiceBusDenied[].lane]|sort==["processor","writer"])
  and all(.v1ThroughV5FundedKeyVaultAndServiceBusDenied[]; .v1ThroughV5AndFundedStorageDenied and .keyVaultDenied and .serviceBusDenied)
' >/dev/null
jq '[.[0]]' "$STATE/full-writer.json" >"$STATE/assign-writer-pid.json"
if run_handoff verify-live >/dev/null 2>&1; then echo 'verify-live accepted missing assignment' >&2; exit 1; fi
cp "$STATE/full-writer.json" "$STATE/assign-writer-pid.json"
jq '. + [.[0] | .id="/assign/writer-extra"]' "$STATE/full-writer.json" >"$STATE/assign-writer-pid.json"
if run_handoff verify-live >/dev/null 2>&1; then echo 'verify-live accepted extra assignment' >&2; exit 1; fi

a="${EXTRA_FIC:-}"; setup_case extra-fic; EXTRA_FIC=writer
if run_handoff check >/dev/null 2>&1; then echo 'extra federated credential accepted' >&2; exit 1; fi
EXTRA_FIC=$a

setup_case drift-role; DRIFT_ROLE=blob
if run_handoff apply >/dev/null 2>&1; then echo 'drifted custom role accepted' >&2; exit 1; fi
test ! -e "$RECEIPTS/before.json"; test ! -e "$STATE/deployment.log"
unset DRIFT_ROLE

setup_case denied-positive; DENY_POSITIVE=writer-preflight
if run_handoff apply >/dev/null 2>&1; then echo 'denied positive data-plane operation accepted' >&2; exit 1; fi
test -e "$RECEIPTS/before.json"; test ! -e "$RECEIPTS/apply-result.json"
test "$(wc -l <"$STATE/deployment.log")" = 1
unset DENY_POSITIVE

printf '%s\n' 'qset-v6 RBAC proof tests passed'
