#!/usr/bin/env bash
set -euo pipefail

readonly verifier="$(dirname "$0")/verify-funded-receiver-role-contract.sh"
readonly queue_scope='/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-polyedge-dev/providers/Microsoft.ServiceBus/namespaces/funded/queues/intents'
readonly azure='15167ab2-ad4c-4f9d-8f1c-ed9b67b1990f'
readonly oci='ab2527e7-06d0-4be4-af95-35b7fa353f62'
readonly wrong='00000000-0000-0000-0000-000000000001'
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT

fixture() {
  jq -cn --arg scope "$queue_scope" "$@" '
    [($ARGS.named | del(.scope)) | to_entries[] | {
      roleDefinitionName: "Azure Service Bus Data Receiver",
      principalId: .value,
      scope: $scope
    }]
  '
}

accept() {
  fixture "${@:2}" > "$test_dir/assignments.json"
  "$verifier" "$test_dir/assignments.json" "$queue_scope" "$1"
}

reject() {
  if accept "$@" 2>/dev/null; then
    printf 'unexpected accepted receiver contract: %s\n' "$*" >&2
    exit 1
  fi
}

fixture --arg azure "$azure" > "$test_dir/assignments.json"
"$verifier" "$test_dir/assignments.json" "$queue_scope"
accept azure-only --arg azure "$azure"
accept azure-oci-transition --arg azure "$azure" --arg oci "$oci"
reject azure-only --arg azure "$azure" --arg oci "$oci"
reject azure-oci-transition --arg azure "$azure"
reject azure-oci-transition --arg azure "$azure" --arg oci "$oci" --arg extra "$wrong"
reject azure-oci-transition --arg wrong "$wrong" --arg oci "$oci"
