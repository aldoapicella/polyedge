#!/bin/bash
set -euo pipefail

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
helper=$repo/ops/conduit/bin/polyedge-funded-secret-bootstrap
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

tenant=9767f0dc-e83f-4cc1-94e1-0d5f9d287d32
client=d9ce9154-66a6-4bdb-839f-0da7b02b38da
jwt='jwt-header.jwt-claims.jwt-signature'
access_token='fake-access-token-never-log'

make_case() {
  case_root=$root/$1
  mkdir -p "$case_root/bin" "$case_root/runtime" "$case_root/token" "$case_root/store"
  chmod 0700 "$case_root/token"
  printf '%s' "$jwt" >"$case_root/token/azure-federated-token"
  chmod 0600 "$case_root/token/azure-federated-token"

  cat >"$case_root/bin/systemctl" <<'SH'
#!/bin/sh
[ "${FAKE_SYSTEMCTL_ACTIVE:-0}" = 0 ] && exit 3
exit 0
SH
  cat >"$case_root/bin/curl" <<'SH'
#!/bin/bash
set -eu
printf '%q ' "$@" >>"$FAKE_CALLS/curl.argv"
printf '\n' >>"$FAKE_CALLS/curl.argv"
args=" $* "
if [[ "$args" == *login.microsoftonline.com/* ]]; then
  [[ ${FAKE_CURL_MODE:-success} != oauth_fail ]] || exit 22
  printf '{"access_token":"%s"}\n' "$FAKE_ACCESS_TOKEN"
  exit
fi
[[ "$args" == *" --header @$FAKE_RUNTIME/"* ]] || exit 91
grep -R -Fx "Authorization: Bearer $FAKE_ACCESS_TOKEN" "$FAKE_RUNTIME" >/dev/null || exit 92
secret=${args#*'/secrets/'}
secret=${secret%%\?*}
if [[ ${FAKE_CURL_MODE:-success} == http_fail && $secret == polymarket-api-secret ]]; then
  exit 22
fi
case "${FAKE_CURL_MODE:-success}:$secret" in
  malformed:polymarket-api-secret) printf '{"value":7}\n'; exit ;;
  empty:polymarket-api-secret) printf '{"value":""}\n'; exit ;;
  oversized:polymarket-api-secret)
    printf '{"value":"'
    head -c 65537 /dev/zero | tr '\0' x
    printf '"}\n'
    exit
    ;;
esac
printf '{"value":"fixture-%s"}\n' "$secret"
SH
  cat >"$case_root/bin/podman" <<'SH'
#!/bin/bash
set -eu
printf '%q ' "$@" >>"$FAKE_CALLS/podman.argv"
printf '\n' >>"$FAKE_CALLS/podman.argv"
[[ $1 == secret ]] || exit 125
case "$2" in
  exists) [[ -f "$FAKE_STORE/$3" ]] && exit 0 || exit 1 ;;
  create)
    name=$3
    [[ ${FAKE_PODMAN_FAIL_NAME:-} != "$name" ]] || { read -r _ || true; exit 125; }
    [[ ! -e "$FAKE_STORE/$name" ]] || exit 125
    dd status=none of="$FAKE_STORE/$name"
    printf 'fake-id\n'
    ;;
  *) exit 125 ;;
esac
SH
  chmod 0755 "$case_root/bin/"*
  mkdir "$case_root/calls"
}

run_case() {
  case_root=$1
  shift
  env \
    AZURE_TENANT_ID=$tenant \
    AZURE_CLIENT_ID=$client \
    POLYEDGE_FUNDED_SECRET_BOOTSTRAP_TEST=1 \
    POLYEDGE_FUNDED_SECRET_CURL=$case_root/bin/curl \
    POLYEDGE_FUNDED_SECRET_JQ=$(command -v jq) \
    POLYEDGE_FUNDED_SECRET_PODMAN=$case_root/bin/podman \
    POLYEDGE_FUNDED_SECRET_SYSTEMCTL=$case_root/bin/systemctl \
    POLYEDGE_FUNDED_SECRET_TOKEN_FILE=$case_root/token/azure-federated-token \
    POLYEDGE_FUNDED_SECRET_RUNTIME_PARENT=$case_root/runtime \
    FAKE_CALLS=$case_root/calls \
    FAKE_STORE=$case_root/store \
    FAKE_RUNTIME=$case_root/runtime \
    FAKE_ACCESS_TOKEN=$access_token \
    "$@" "$helper"
}

assert_clean() {
  [[ -z $(find "$1/runtime" -mindepth 1 -print -quit) ]]
}

make_case success
run_case "$case_root" >"$case_root/stdout" 2>"$case_root/stderr"
grep -Fx 'polyedge-funded-secret-bootstrap: created 5 funded Podman secrets' "$case_root/stdout" >/dev/null
for name in private-key api-key api-secret api-passphrase relayer-api-key; do
  cmp -s <(printf 'fixture-polymarket-%s' "$name") "$case_root/store/polyedge-polymarket-$name"
done
assert_clean "$case_root"
! grep -R -F -e "$jwt" -e "$access_token" -e 'fixture-polymarket-' "$case_root/calls" "$case_root/stdout" "$case_root/stderr" >/dev/null
grep -F -- '--header @' "$case_root/calls/curl.argv" >/dev/null
grep -F -- 'client_assertion@/proc/self/fd/3' "$case_root/calls/curl.argv" >/dev/null

make_case preexisting
printf 'keep-me' >"$case_root/store/polyedge-polymarket-api-secret"
if run_case "$case_root" >"$case_root/stdout" 2>"$case_root/stderr"; then
  echo 'pre-existing Podman secret was accepted' >&2
  exit 1
fi
grep -F 'refusing pre-existing Podman secret: polyedge-polymarket-api-secret' "$case_root/stderr" >/dev/null
[[ $(<"$case_root/store/polyedge-polymarket-api-secret") == keep-me ]]
[[ ! -e "$case_root/calls/curl.argv" ]]
! grep -F 'secret create' "$case_root/calls/podman.argv" >/dev/null
assert_clean "$case_root"

for binding in AZURE_TENANT_ID=00000000-0000-0000-0000-000000000000 AZURE_CLIENT_ID=00000000-0000-0000-0000-000000000000; do
  make_case "wrong-${binding%%=*}"
  if run_case "$case_root" env "$binding" >"$case_root/stdout" 2>"$case_root/stderr"; then
    echo "wrong ${binding%%=*} was accepted" >&2
    exit 1
  fi
  grep -F 'is missing or not' "$case_root/stderr" >/dev/null
  [[ ! -e "$case_root/calls/curl.argv" ]]
done

for mode in malformed oversized empty; do
  make_case "$mode"
  if run_case "$case_root" env FAKE_CURL_MODE=$mode >"$case_root/stdout" 2>"$case_root/stderr"; then
    echo "$mode Key Vault value was accepted" >&2
    exit 1
  fi
  grep -F 'Key Vault read failed or returned an invalid value: polymarket-api-secret' "$case_root/stderr" >/dev/null
  grep -F 'incomplete; created before failure: polyedge-polymarket-private-key, polyedge-polymarket-api-key' "$case_root/stderr" >/dev/null
  [[ ! -e "$case_root/store/polyedge-polymarket-api-secret" ]]
  assert_clean "$case_root"
done

make_case oauth-failure
if run_case "$case_root" env FAKE_CURL_MODE=oauth_fail >"$case_root/stdout" 2>"$case_root/stderr"; then
  echo 'OAuth failure was accepted' >&2
  exit 1
fi
grep -F 'funded-signer UAMI token exchange failed' "$case_root/stderr" >/dev/null
[[ -z $(find "$case_root/store" -type f -print -quit) ]]
assert_clean "$case_root"

make_case http-failure
if run_case "$case_root" env FAKE_CURL_MODE=http_fail >"$case_root/stdout" 2>"$case_root/stderr"; then
  echo 'Key Vault HTTP failure was accepted' >&2
  exit 1
fi
grep -F 'incomplete; created before failure: polyedge-polymarket-private-key, polyedge-polymarket-api-key' "$case_root/stderr" >/dev/null
[[ $(find "$case_root/store" -type f | wc -l) == 2 ]]
assert_clean "$case_root"

make_case podman-failure
if run_case "$case_root" env FAKE_PODMAN_FAIL_NAME=polyedge-polymarket-api-secret >"$case_root/stdout" 2>"$case_root/stderr"; then
  echo 'Podman partial failure was accepted' >&2
  exit 1
fi
grep -F 'Podman secret creation failed: polyedge-polymarket-api-secret' "$case_root/stderr" >/dev/null
grep -F 'incomplete; created before failure: polyedge-polymarket-private-key, polyedge-polymarket-api-key' "$case_root/stderr" >/dev/null
[[ $(find "$case_root/store" -type f | wc -l) == 2 ]]
assert_clean "$case_root"

make_case active-service
if run_case "$case_root" env FAKE_SYSTEMCTL_ACTIVE=1 >"$case_root/stdout" 2>"$case_root/stderr"; then
  echo 'active funded service was accepted' >&2
  exit 1
fi
grep -F 'funded signer is active' "$case_root/stderr" >/dev/null
[[ ! -e "$case_root/calls/curl.argv" ]]

make_case symlink-token
mv "$case_root/token/azure-federated-token" "$case_root/token/real-token"
ln -s real-token "$case_root/token/azure-federated-token"
if run_case "$case_root" >"$case_root/stdout" 2>"$case_root/stderr"; then
  echo 'symlink JWT-SVID was accepted' >&2
  exit 1
fi
grep -F 'funded-signer JWT-SVID is missing or unsafe' "$case_root/stderr" >/dev/null

printf 'test-funded-secret-bootstrap: ok\n'
