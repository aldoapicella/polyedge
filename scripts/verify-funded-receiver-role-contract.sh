#!/usr/bin/env bash
set -euo pipefail

assignments_file=${1:?usage: verify-funded-receiver-role-contract.sh ASSIGNMENTS_JSON QUEUE_SCOPE [PHASE]}
queue_scope=${2:?usage: verify-funded-receiver-role-contract.sh ASSIGNMENTS_JSON QUEUE_SCOPE [PHASE]}
phase=${3:-azure-only}

readonly azure_executor_principal='15167ab2-ad4c-4f9d-8f1c-ed9b67b1990f'
readonly oci_funded_principal='ab2527e7-06d0-4be4-af95-35b7fa353f62'

case "$phase" in
  azure-only)
    expected=$(jq -cn --arg azure "$azure_executor_principal" '[$azure]')
    ;;
  azure-oci-transition)
    expected=$(jq -cn --arg azure "$azure_executor_principal" --arg oci "$oci_funded_principal" '[$azure, $oci] | sort')
    ;;
  *)
    printf 'unsupported funded receiver transition phase: %s\n' "$phase" >&2
    exit 1
    ;;
esac

actual=$(jq -cer --arg scope "${queue_scope,,}" '
  [ .[]
    | select(
        .roleDefinitionName == "Azure Service Bus Data Receiver"
        and (.scope | ascii_downcase) == $scope
      )
    | .principalId | ascii_downcase
  ] | sort
' "$assignments_file")

if [ "$actual" != "$expected" ]; then
  printf 'funded queue receiver contract mismatch for %s: expected %s, found %s\n' \
    "$phase" "$expected" "$actual" >&2
  exit 1
fi
