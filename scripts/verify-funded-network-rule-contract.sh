#!/usr/bin/env bash
set -euo pipefail

readonly oci_funded_egress_ip='149.130.186.60'

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
)

if [ "${1:-}" = '--self-test' ]; then
  self_test
  exit
fi

verify_contract \
  "${1:?usage: verify-funded-network-rule-contract.sh NETWORK_JSON PHASE PRODUCER_IP EXECUTOR_IP}" \
  "${2:?usage: verify-funded-network-rule-contract.sh NETWORK_JSON PHASE PRODUCER_IP EXECUTOR_IP}" \
  "${3:?usage: verify-funded-network-rule-contract.sh NETWORK_JSON PHASE PRODUCER_IP EXECUTOR_IP}" \
  "${4:?usage: verify-funded-network-rule-contract.sh NETWORK_JSON PHASE PRODUCER_IP EXECUTOR_IP}"
