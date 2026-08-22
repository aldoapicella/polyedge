#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
digest=ghcr.io/example/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
command="sudo -n /usr/local/sbin/polyedge-quadlet-deploy polyedge-api $digest"

output=$(SSH_ORIGINAL_COMMAND="$command" POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy")
[ "$output" = "polyedge-api $digest" ]

for unit in polyedge-shadow-qset polyedge-funded-intent-producer; do
  command="sudo -n /usr/local/sbin/polyedge-quadlet-deploy $unit $digest"
  output=$(SSH_ORIGINAL_COMMAND="$command" POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy")
  [ "$output" = "$unit $digest" ]
done

frontend=ghcr.io/example/polyedge-frontend@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
signer=ghcr.io/example/polyedge-venue-probe@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
for pair in "polyedge-frontend $frontend" "polyedge-funded-signer $signer"; do
  command="sudo -n /usr/local/sbin/polyedge-quadlet-deploy $pair"
  output=$(SSH_ORIGINAL_COMMAND="$command" POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy")
  [ "$output" = "$pair" ]
done

if SSH_ORIGINAL_COMMAND="sudo -n /usr/local/sbin/polyedge-quadlet-deploy polyedge-shadow-qset $frontend" \
  POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy"; then
  echo 'repository mismatch was accepted' >&2
  exit 1
fi

if SSH_ORIGINAL_COMMAND="uname -a" POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy"; then
  echo 'arbitrary command was accepted' >&2
  exit 1
fi
if SSH_ORIGINAL_COMMAND="$command
uname -a" POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy"; then
  echo 'multiline command was accepted' >&2
  exit 1
fi
