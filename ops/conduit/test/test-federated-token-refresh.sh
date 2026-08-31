#!/bin/sh
set -eu

root=$(mktemp -d); socket=$root/agent.sock; fake=$root/spire-agent; issuer=https://oidc.example.invalid
lanes='promotion research funded-intent-producer shadow-qset-v3-writer shadow-qset-v4-writer shadow-qset-v4-processor shadow-qset-v5-writer shadow-qset-v5-processor shadow-qset-v6-writer shadow-qset-v6-processor shadow-qset-v7-writer shadow-qset-v7-processor'
for lane in $lanes; do mkdir -m 0700 "$root/polyedge-federated-$lane"; done
python3 - "$socket" <<'PY' &
import socket, sys, time
server = socket.socket(socket.AF_UNIX)
server.bind(sys.argv[1]); server.listen(); time.sleep(30)
PY
socket_pid=$!
cleanup() { kill "$socket_pid" 2>/dev/null || true; find "$root" -type f -exec unlink {} \;; find "$root" -type s -exec unlink {} \;; for lane in $lanes; do rmdir "$root/polyedge-federated-$lane" 2>/dev/null || true; done; rmdir "$root" 2>/dev/null || true; }
trap cleanup EXIT HUP INT TERM
while [ ! -S "$socket" ]; do sleep 0.1; done

cat >"$fake" <<'SH'
#!/bin/sh
python3 - "$@" <<'PY'
import base64, json, os, sys, time
encode=lambda value: base64.urlsafe_b64encode(json.dumps(value,separators=(",",":")).encode()).rstrip(b"=").decode()
now=int(time.time()); subject=sys.argv[sys.argv.index("-spiffeID")+1]
expected=os.environ.get("FAKE_EXPECTED_SUBJECT")
if expected and subject != expected: raise SystemExit("unexpected SPIFFE ID")
token=".".join([encode({"alg":"HS256" if os.environ.get("FAKE_BAD") else "RS256","typ":"JWT"}),encode({"iss":"https://oidc.example.invalid","sub":subject,"aud":["api://AzureADTokenExchange"],"iat":now,"exp":now+300}),"c2lnbmF0dXJl"])
print(json.dumps([{"svids":[{"spiffeId":subject,"svid":token}]},{"bundles":{}}]))
PY
SH
chmod 0755 "$fake"

refresh() {
  lane=$1; token=$root/polyedge-federated-$lane/azure-federated-token
  expected=$lane; [ "$lane" = promotion ] && expected=promotion-controller
  FAKE_EXPECTED_SUBJECT=spiffe://polyedge.local/conduit/$expected POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake POLYEDGE_FEDERATED_TOKEN_EXPECTED_UID=$(id -u) POLYEDGE_FEDERATED_TOKEN_EXPECTED_GID=$(id -g) \
    ops/conduit/bin/polyedge-federated-token-refresh "$lane" "$token" "$issuer"
  [ "$(stat -c %a "$token")" = 600 ]
}
for lane in $lanes; do refresh "$lane"; done

writer=$root/polyedge-federated-shadow-qset-v4-writer/azure-federated-token
processor=$root/polyedge-federated-shadow-qset-v4-processor/azure-federated-token
[ "$writer" != "$processor" ] && [ "$(stat -c %i "$writer")" != "$(stat -c %i "$processor")" ]
before=$(sha256sum "$writer"|awk '{print $1}')
if FAKE_BAD=1 POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake POLYEDGE_FEDERATED_TOKEN_EXPECTED_UID=$(id -u) POLYEDGE_FEDERATED_TOKEN_EXPECTED_GID=$(id -g) ops/conduit/bin/polyedge-federated-token-refresh shadow-qset-v4-writer "$writer" "$issuer" 2>/dev/null; then echo 'invalid JWT-SVID was accepted' >&2; exit 1; fi
[ "$before" = "$(sha256sum "$writer"|awk '{print $1}')" ]
if POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake ops/conduit/bin/polyedge-federated-token-refresh invalid-lane "$writer" "$issuer" 2>/dev/null; then echo 'unknown identity lane was accepted' >&2; exit 1; fi
