#!/usr/bin/env bash
set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT
mkdir -p "$test_dir/bin"

cat > "$test_dir/bin/az" <<'FAKE_AZ'
#!/usr/bin/env bash
set -Eeuo pipefail

counter() {
  local file=$1 count=0
  [ ! -f "$file" ] || count=$(<"$file")
  count=$((count + 1))
  printf '%s\n' "$count" > "$file"
  printf '%s\n' "$count"
}

case "$1 $2 $3" in
  "servicebus queue update")
    count=$(counter "$FUNDED_FENCE_TEST_DIR/update-count")
    if [ "$FUNDED_FENCE_SCENARIO" = update-fails ] ||
       { [ "$FUNDED_FENCE_SCENARIO" = update-retries ] && [ "$count" -lt 3 ]; }; then
      exit 1
    fi
    ;;
  "servicebus queue show")
    count=$(counter "$FUNDED_FENCE_TEST_DIR/show-count")
    active=0
    if [ "$FUNDED_FENCE_SCENARIO" = never-drains ] ||
       { [ "$FUNDED_FENCE_SCENARIO" = success ] && [ "$count" -lt 2 ]; }; then
      active=1
    fi
    printf '{"status":"SendDisabled","countDetails":{"activeMessageCount":%s,"scheduledMessageCount":0}}\n' "$active"
    ;;
  *) exit 64 ;;
esac
FAKE_AZ
cat > "$test_dir/bin/sleep" <<'FAKE_SLEEP'
#!/bin/sh
exit 0
FAKE_SLEEP
chmod 0755 "$test_dir/bin/az" "$test_dir/bin/sleep"

run_fence() {
  local scenario=$1
  local output=$test_dir/$scenario.json
  rm -f -- "$test_dir/update-count" "$test_dir/show-count" "$output"
  PATH="$test_dir/bin:$PATH" \
  FUNDED_FENCE_TEST_DIR="$test_dir" \
  FUNDED_FENCE_SCENARIO="$scenario" \
  AZURE_RESOURCE_GROUP=rg-test \
  SERVICE_BUS_NAMESPACE=sb-test \
  SERVICE_BUS_QUEUE=queue-test \
    bash "$repo_root/scripts/fence-funded-queue-and-wait-empty.sh" "$output"
}

run_fence success
test "$(<"$test_dir/update-count")" = 1
test "$(<"$test_dir/show-count")" = 2
jq -e '.status == "SendDisabled" and .countDetails.activeMessageCount == 0' \
  "$test_dir/success.json" >/dev/null

run_fence update-retries
test "$(<"$test_dir/update-count")" = 3
test "$(<"$test_dir/show-count")" = 1

if run_fence update-fails; then
  echo 'queue fence accepted three failed update attempts' >&2
  exit 1
fi
test "$(<"$test_dir/update-count")" = 3
test ! -e "$test_dir/show-count"

if run_fence never-drains; then
  echo 'queue fence accepted a non-empty queue' >&2
  exit 1
fi
test "$(<"$test_dir/update-count")" = 1
test "$(<"$test_dir/show-count")" = 36

echo 'funded queue fence self-test passed'
