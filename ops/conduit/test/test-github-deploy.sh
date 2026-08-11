#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
digest=ghcr.io/example/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
command="sudo -n /usr/local/sbin/polyedge-quadlet-deploy polyedge-api $digest"

output=$(SSH_ORIGINAL_COMMAND="$command" POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy")
[ "$output" = "polyedge-api $digest" ]

if SSH_ORIGINAL_COMMAND="uname -a" POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy"; then
  echo 'arbitrary command was accepted' >&2
  exit 1
fi
if SSH_ORIGINAL_COMMAND="$command
uname -a" POLYEDGE_GITHUB_DEPLOY_TEST=1 "$root/bin/polyedge-github-deploy"; then
  echo 'multiline command was accepted' >&2
  exit 1
fi
