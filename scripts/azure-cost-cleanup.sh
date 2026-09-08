#!/usr/bin/env bash
set -euo pipefail

subscription_id="${AZURE_SUBSCRIPTION_ID:-73783c0c-5a53-4f9b-b244-6f64e813814c}"
resource_group="${AZURE_RESOURCE_GROUP:-rg-polyedge-dev}"
apply=false
alert_deletions=()
public_ip_deletions=()

if [[ "${1:-}" == "--apply" ]]; then
  apply=true
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--apply]" >&2
  exit 2
fi

actual_subscription=$(az account show --query id -o tsv)
if [[ "$actual_subscription" != "$subscription_id" ]]; then
  echo "Azure CLI is on subscription $actual_subscription; expected $subscription_id" >&2
  exit 1
fi

delete_alert_if_redundant() {
  local legacy_name=$1
  local retained_name=$2
  local required_query_fragment=${3:-}
  local legacy_json retained_json legacy_query retained_query legacy_controls retained_controls

  if ! legacy_json=$(az monitor scheduled-query show \
    --subscription "$subscription_id" \
    --resource-group "$resource_group" \
    --name "$legacy_name" \
    --output json 2>/dev/null); then
    echo "already absent: scheduled query $legacy_name"
    return
  fi
  retained_json=$(az monitor scheduled-query show \
    --subscription "$subscription_id" \
    --resource-group "$resource_group" \
    --name "$retained_name" \
    --output json)
  [[ "$(jq -r '.enabled' <<<"$retained_json")" == "true" ]]
  legacy_query=$(jq -r '.criteria.allOf[0].query' <<<"$legacy_json")
  retained_query=$(jq -r '.criteria.allOf[0].query' <<<"$retained_json")
  legacy_controls=$(jq -Sc '{severity,evaluationFrequency,windowSize,scopes:(.scopes | sort),actionGroups:(.actions.actionGroups | sort)}' <<<"$legacy_json")
  retained_controls=$(jq -Sc '{severity,evaluationFrequency,windowSize,scopes:(.scopes | sort),actionGroups:(.actions.actionGroups | sort)}' <<<"$retained_json")
  [[ "$legacy_controls" == "$retained_controls" ]] || {
    echo "$legacy_name and $retained_name do not have identical alert controls; refusing cleanup" >&2
    exit 1
  }

  if [[ -z "$required_query_fragment" ]]; then
    [[ "$legacy_query" == "$retained_query" ]] || {
      echo "$legacy_name is not identical to $retained_name; refusing cleanup" >&2
      exit 1
    }
  else
    [[ "$retained_query" == *"$required_query_fragment"* ]] || {
      echo "$retained_name does not contain the required query fragment; refusing cleanup" >&2
      exit 1
    }
    for failure_fragment in JobName_s BackoffLimitExceeded FailedMount ErrImagePull ImagePullBackOff ContainerCrashing panicked 'child command failed with status exit status:' '"status":"failed"'; do
      [[ "$retained_query" == *"$failure_fragment"* ]] || {
        echo "$retained_name does not contain explicit failure fragment $failure_fragment" >&2
        exit 1
      }
    done
  fi

  alert_deletions+=("$legacy_name")
  echo "validated redundant scheduled query: $legacy_name"
}

delete_unattached_public_ip() {
  local name=$1
  local ip_json

  if ! ip_json=$(az network public-ip show \
    --subscription "$subscription_id" \
    --resource-group "$resource_group" \
    --name "$name" \
    --output json 2>/dev/null); then
    echo "already absent: public IP $name"
    return
  fi
  [[ "$(jq -r '.ipConfiguration == null and .natGateway == null' <<<"$ip_json")" == "true" ]] || {
    echo "$name is attached to a resource; refusing cleanup" >&2
    exit 1
  }

  public_ip_deletions+=("$name")
  echo "validated unattached public IP: $name"
}

delete_alert_if_redundant \
  polyedge-dev-missing-latest-blob \
  polyedge-dev-no-new-blob-for-3-minutes
delete_alert_if_redundant \
  polyedge-dev-research-job-failure \
  polyedge-dev-job-failed \
  ContainerJobName_s
delete_unattached_public_ip pip-polyedge-venue-neu-egress

if $apply; then
  for name in "${alert_deletions[@]}"; do
    az monitor scheduled-query delete \
      --subscription "$subscription_id" \
      --resource-group "$resource_group" \
      --name "$name" \
      --yes
    echo "deleted redundant scheduled query: $name"
  done
  for name in "${public_ip_deletions[@]}"; do
    az network public-ip delete \
      --subscription "$subscription_id" \
      --resource-group "$resource_group" \
      --name "$name"
    echo "deleted unattached public IP: $name"
  done
else
  if ((${#alert_deletions[@]} > 0)); then
    printf 'would delete redundant scheduled query: %s\n' "${alert_deletions[@]}"
  fi
  if ((${#public_ip_deletions[@]} > 0)); then
    printf 'would delete unattached public IP: %s\n' "${public_ip_deletions[@]}"
  fi
  echo "dry run complete; rerun with --apply after reviewing this output"
fi
