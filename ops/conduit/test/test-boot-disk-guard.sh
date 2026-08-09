#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
guard=$root/bin/polyedge-boot-disk-guard

run_guard() {
  usage=$1
  available=$2
  POLYEDGE_BOOT_DISK_TEST=1 \
  POLYEDGE_BOOT_DISK_TEST_LOG="$tmp/cleanup.log" \
  POLYEDGE_BOOT_DISK_TEST_USAGE_PERCENT="$usage" \
  POLYEDGE_BOOT_DISK_TEST_AVAILABLE_BYTES="$available" \
  POLYEDGE_BOOT_DISK_STATE_DIR="$tmp/run" \
  POLYEDGE_LOGGER=/bin/true \
    "$guard" --pull-gate
}

run_guard 74 17179869184
test ! -e "$tmp/run/image-pulls-paused"

run_guard 80 17179869184
grep -Fx cleanup "$tmp/cleanup.log" >/dev/null
test ! -e "$tmp/run/image-pulls-paused"

if run_guard 85 16106127359; then
  echo '85 percent usage did not pause image pulls' >&2
  exit 1
fi
test -e "$tmp/run/image-pulls-paused"

run_guard 70 17179869184
test ! -e "$tmp/run/image-pulls-paused"
grep -F '"minimumFreeBytes":16106127360' "$tmp/run/boot-disk-status.json" >/dev/null
