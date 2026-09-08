#!/usr/bin/env bash
set -euo pipefail

readonly oci_funded_egress_ip='149.130.186.60'
readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

verify_contract() {
  local network_file=$1
  local phase=$2
  local producer_ip=$3
  local executor_ip=$4
  local expected

  jq -en --arg producer "$producer_ip" --arg executor "$executor_ip" \
    --arg oci "$oci_funded_egress_ip" '
    def ipv4:
      split(".") as $octets
      | ($octets | length) == 4
      and all($octets[]; test("^[0-9]{1,3}$") and (tonumber >= 0 and tonumber <= 255));
    ([$producer, $executor, $oci] | unique | length) == 3
    and ([$producer, $executor, $oci] | all(ipv4))
  ' >/dev/null || {
    printf 'invalid or duplicate Azure funded egress IPs\n' >&2
    return 1
  }

  case "$phase" in
    azure-only)
      expected=$(jq -cn --arg producer "$producer_ip" --arg executor "$executor_ip" \
        '[$producer, $executor] | sort')
      ;;
    azure-oci-transition)
      expected=$(jq -cn --arg producer "$producer_ip" --arg executor "$executor_ip" \
        --arg oci "$oci_funded_egress_ip" '[$producer, $executor, $oci] | sort')
      ;;
    *)
      printf 'unsupported funded receiver transition phase: %s\n' "$phase" >&2
      return 1
      ;;
  esac

  jq -e --argjson expected "$expected" '
    .publicNetworkAccess == "Enabled"
    and .defaultAction == "Deny"
    and .trustedServiceAccessEnabled == false
    and (.virtualNetworkRules | length) == 0
    and (.ipRules | type) == "array"
    and ([.ipRules[].ipMask] | sort) == $expected
    and ([.ipRules[] | select(.action != "Allow")] | length) == 0
  ' "$network_file" >/dev/null || {
    printf 'funded Service Bus network rule contract mismatch for %s\n' "$phase" >&2
    return 1
  }
}

phase_pair_allowed() {
  case "$1:$2:$3" in
    azure-only:azure-only:azure-only | \
      azure-oci-transition:azure-only:azure-only | \
      azure-oci-transition:azure-oci-transition:azure-only | \
      azure-oci-transition:azure-oci-transition:azure-oci-transition) return 0 ;;
    *) return 1 ;;
  esac
}

verify_pre_mutation() {
  local network_file=$1
  local assignments_file=$2
  local queue_scope=$3
  local desired_phase=$4
  local producer_ip=$5
  local executor_ip=$6

  local candidate receiver_phase=invalid network_phase=invalid
  for candidate in azure-only azure-oci-transition; do
    if "$script_dir/verify-funded-receiver-role-contract.sh" \
        "$assignments_file" "$queue_scope" "$candidate" 2>/dev/null; then
      receiver_phase=$candidate
    fi
    if verify_contract "$network_file" "$candidate" "$producer_ip" "$executor_ip" 2>/dev/null; then
      network_phase=$candidate
    fi
  done
  phase_pair_allowed "$desired_phase" "$receiver_phase" "$network_phase" || {
    printf 'funded pre-mutation phase state rejected: desired=%s receiver=%s network=%s\n' \
      "$desired_phase" "$receiver_phase" "$network_phase" >&2
    return 1
  }
}

self_test() (
  network_test_dir=$(mktemp -d)
  network_file=$network_test_dir/network.json
  trap 'rm -rf -- "$network_test_dir"' EXIT

  fixture() {
    printf '%s\n' "$@" | jq -Rsc '
      split("\n")[:-1] as $ips
      | {
          publicNetworkAccess: "Enabled",
          defaultAction: "Deny",
          trustedServiceAccessEnabled: false,
          virtualNetworkRules: [],
          ipRules: ($ips | map({ipMask: ., action: "Allow"}))
        }
    ' > "$network_file"
  }
  accept() {
    local phase=$1
    shift
    fixture "$@"
    verify_contract "$network_file" "$phase" 57.156.67.93 20.82.208.98
  }
  reject() {
    if accept "$@" 2>/dev/null; then
      printf 'unexpected accepted funded network contract: %s\n' "$*" >&2
      return 1
    fi
  }

  accept azure-only 57.156.67.93 20.82.208.98
  accept azure-oci-transition 57.156.67.93 20.82.208.98 149.130.186.60
  reject azure-only 57.156.67.93 20.82.208.98 149.130.186.60
  reject azure-oci-transition 57.156.67.93 20.82.208.98
  reject azure-oci-transition 57.156.67.93 20.82.208.98 149.130.186.60 192.0.2.1
  reject azure-oci-transition 57.156.67.93 20.82.208.98 149.130.186.60 149.130.186.60
  reject azure-oci-transition 57.156.67.93 20.82.208.98 192.0.2.1

  accept_pair() {
    phase_pair_allowed "$@" || {
      printf 'unexpected rejected funded pre-mutation pair: %s\n' "$*" >&2
      return 1
    }
  }
  reject_pair() {
    if accept_pair "$@" 2>/dev/null; then
      printf 'unexpected accepted funded pre-mutation pair: %s\n' "$*" >&2
      return 1
    fi
  }

  accept_pair azure-only azure-only azure-only
  reject_pair azure-only azure-oci-transition azure-oci-transition
  reject_pair azure-only azure-only azure-oci-transition
  reject_pair azure-only azure-oci-transition azure-only
  accept_pair azure-oci-transition azure-only azure-only
  accept_pair azure-oci-transition azure-oci-transition azure-oci-transition
  reject_pair azure-oci-transition azure-only azure-oci-transition
  accept_pair azure-oci-transition azure-oci-transition azure-only
  reject_pair azure-oci-transition invalid azure-oci-transition
  reject_pair azure-oci-transition azure-oci-transition invalid
)

case "${1:-}" in
  --self-test)
    self_test
    ;;
  --pre-mutation)
    verify_pre_mutation \
      "${2:?usage: verify-funded-network-rule-contract.sh --pre-mutation NETWORK_JSON ASSIGNMENTS_JSON QUEUE_SCOPE PHASE PRODUCER_IP EXECUTOR_IP}" \
      "${3:?usage: verify-funded-network-rule-contract.sh --pre-mutation NETWORK_JSON ASSIGNMENTS_JSON QUEUE_SCOPE PHASE PRODUCER_IP EXECUTOR_IP}" \
      "${4:?usage: verify-funded-network-rule-contract.sh --pre-mutation NETWORK_JSON ASSIGNMENTS_JSON QUEUE_SCOPE PHASE PRODUCER_IP EXECUTOR_IP}" \
      "${5:?usage: verify-funded-network-rule-contract.sh --pre-mutation NETWORK_JSON ASSIGNMENTS_JSON QUEUE_SCOPE PHASE PRODUCER_IP EXECUTOR_IP}" \
      "${6:?usage: verify-funded-network-rule-contract.sh --pre-mutation NETWORK_JSON ASSIGNMENTS_JSON QUEUE_SCOPE PHASE PRODUCER_IP EXECUTOR_IP}" \
      "${7:?usage: verify-funded-network-rule-contract.sh --pre-mutation NETWORK_JSON ASSIGNMENTS_JSON QUEUE_SCOPE PHASE PRODUCER_IP EXECUTOR_IP}"
    ;;
  *)
    verify_contract \
      "${1:?usage: verify-funded-network-rule-contract.sh NETWORK_JSON PHASE PRODUCER_IP EXECUTOR_IP}" \
      "${2:?usage: verify-funded-network-rule-contract.sh NETWORK_JSON PHASE PRODUCER_IP EXECUTOR_IP}" \
      "${3:?usage: verify-funded-network-rule-contract.sh NETWORK_JSON PHASE PRODUCER_IP EXECUTOR_IP}" \
      "${4:?usage: verify-funded-network-rule-contract.sh NETWORK_JSON PHASE PRODUCER_IP EXECUTOR_IP}"
    ;;
esac
