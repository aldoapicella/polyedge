#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/quadlets" "$tmp/rollback" "$tmp/bin"

old=ghcr.io/example/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
new=ghcr.io/example/polyedge-rust-backend@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
next=ghcr.io/example/polyedge-rust-backend@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
signer=ghcr.io/example/polyedge-venue-probe@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
printf '[Container]\nImage=%s\nContainerName=polyedge-api\n' "$old" >"$tmp/quadlets/polyedge-api.container"
printf '[Container]\nImage=%s\nContainerName=polyedge-funded-signer\n' "$old" >"$tmp/quadlets/polyedge-funded-signer.container"

printf '%s\n' '#!/bin/sh' 'set -eu' 'printf "%s\\n" "$*" >>"$TEST_LOG"' \
  'case "$1 $2" in' "  'image inspect') printf 'linux/arm64\\n' ;;" \
  "  'inspect --type') printf 'true %s %s\\n' \"\$TEST_IMAGE_ID\" \"\$TEST_IMAGE\" ;;" 'esac' >"$tmp/bin/podman"
printf '%s\n' '#!/bin/sh' 'set -eu' 'printf "%s\\n" "$*" >>"$TEST_LOG"' >"$tmp/bin/systemctl"
chmod +x "$tmp/bin/podman" "$tmp/bin/systemctl"

run() {
  image=$1
  running_image=$2
  timestamp=$3
  unit=${4:-polyedge-api}
  TEST_LOG="$tmp/log" TEST_IMAGE_ID=linux/arm64 TEST_IMAGE="$running_image" \
    POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 \
    POLYEDGE_QUADLET_DIR="$tmp/quadlets" \
    POLYEDGE_ROLLBACK_DIR="$tmp/rollback" \
    POLYEDGE_PODMAN="$tmp/bin/podman" \
    POLYEDGE_SYSTEMCTL="$tmp/bin/systemctl" \
    POLYEDGE_DEPLOY_TIMESTAMP="$timestamp" \
    "$root/bin/polyedge-quadlet-deploy" "$unit" "$image"
}

run "$new" "$new" 20260805T000000Z
grep -Fx "Image=$new" "$tmp/quadlets/polyedge-api.container" >/dev/null
grep -Fx "Image=$old" "$tmp/rollback/20260805T000000Z-polyedge-api.container" >/dev/null
grep -Fx "pull $new" "$tmp/log" >/dev/null
grep -Fx 'restart polyedge-api.service' "$tmp/log" >/dev/null

run "$signer" "$signer" 20260805T000002Z polyedge-funded-signer
grep -Fx "Image=$signer" "$tmp/quadlets/polyedge-funded-signer.container" >/dev/null

if run "$new" "$new" 20260805T000003Z polyedge-funded-signer; then
  echo 'wrong repository was accepted for funded signer' >&2
  exit 1
fi

# A wrong running digest must leave the previously working Quadlet in place.
if run "$next" "$new" 20260805T000001Z; then
  echo 'digest mismatch was accepted' >&2
  exit 1
fi
grep -Fx "Image=$new" "$tmp/quadlets/polyedge-api.container" >/dev/null
test "$(grep -c '^restart polyedge-api.service$' "$tmp/log")" -eq 3

if POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 "$root/bin/polyedge-quadlet-deploy" nope "$new"; then
  echo 'unknown service was accepted' >&2
  exit 1
fi
