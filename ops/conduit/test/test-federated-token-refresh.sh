#!/bin/sh
set -eu

root=$(mktemp -d)
socket=$root/agent.sock
fake=$root/spire-agent
token_dir=$root/polyedge-federated-research
token=$token_dir/azure-federated-token
issuer=https://oidc.example.invalid
mkdir -m 0700 "$token_dir"
python3 - "$socket" <<'PY' &
import socket
import sys
import time

server = socket.socket(socket.AF_UNIX)
server.bind(sys.argv[1])
server.listen()
time.sleep(30)
PY
socket_pid=$!
cleanup() {
  kill "$socket_pid" 2>/dev/null || true
  find "$root" -type f -exec unlink {} \;
  find "$root" -type s -exec unlink {} \;
  rmdir "$token_dir" "$root" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM
while [ ! -S "$socket" ]; do sleep 0.1; done

cat > "$fake" <<'SH'
#!/bin/sh
python3 - <<'PY'
import base64
import json
import os
import time

encode = lambda value: base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).rstrip(b"=").decode()
now = int(time.time())
algorithm = "HS256" if os.environ.get("FAKE_BAD") else "RS256"
token = ".".join([
    encode({"alg": algorithm, "typ": "JWT"}),
    encode({"iss": "https://oidc.example.invalid", "sub": "spiffe://polyedge.local/conduit/research", "aud": ["api://AzureADTokenExchange"], "iat": now, "exp": now + 300}),
    "c2lnbmF0dXJl",
])
print(json.dumps({"svids": [{"spiffeId": "spiffe://polyedge.local/conduit/research", "svid": token}]}))
PY
SH
chmod 0755 "$fake"

POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake \
  ops/conduit/bin/polyedge-federated-token-refresh research "$token" "$issuer"
[ "$(stat -c %a "$token")" = 600 ]
before=$(sha256sum "$token" | awk '{print $1}')
if FAKE_BAD=1 POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake \
  ops/conduit/bin/polyedge-federated-token-refresh research "$token" "$issuer" 2>/dev/null; then
  echo 'invalid JWT-SVID was accepted' >&2
  exit 1
fi
[ "$before" = "$(sha256sum "$token" | awk '{print $1}')" ]
