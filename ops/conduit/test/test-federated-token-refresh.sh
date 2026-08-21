#!/bin/sh
set -eu

root=$(mktemp -d)
socket=$root/agent.sock
fake=$root/spire-agent
token_dir=$root/polyedge-federated-promotion
token=$token_dir/azure-federated-token
research_dir=$root/polyedge-federated-research
research_token=$research_dir/azure-federated-token
producer_dir=$root/polyedge-federated-funded-intent-producer
producer_token=$producer_dir/azure-federated-token
issuer=https://oidc.example.invalid
mkdir -m 0700 "$token_dir"
mkdir -m 0700 "$research_dir"
mkdir -m 0700 "$producer_dir"
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
  rmdir "$token_dir" "$research_dir" "$producer_dir" "$root" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM
while [ ! -S "$socket" ]; do sleep 0.1; done

cat > "$fake" <<'SH'
#!/bin/sh
python3 - "$@" <<'PY'
import base64
import json
import os
import time
import sys

encode = lambda value: base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).rstrip(b"=").decode()
now = int(time.time())
algorithm = "HS256" if os.environ.get("FAKE_BAD") else "RS256"
subject = sys.argv[sys.argv.index("-spiffeID") + 1]
if expected := os.environ.get("FAKE_EXPECTED_SUBJECT"):
    if subject != expected:
        raise SystemExit("unexpected SPIFFE ID")
token = ".".join([
    encode({"alg": algorithm, "typ": "JWT"}),
    encode({"iss": "https://oidc.example.invalid", "sub": subject, "aud": ["api://AzureADTokenExchange"], "iat": now, "exp": now + 300}),
    "c2lnbmF0dXJl",
])
print(json.dumps([
    {"svids": [{"spiffeId": subject, "svid": token}]},
    {"bundles": {}},
]))
PY
SH
chmod 0755 "$fake"

POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake \
  POLYEDGE_FEDERATED_TOKEN_EXPECTED_UID=$(id -u) POLYEDGE_FEDERATED_TOKEN_EXPECTED_GID=$(id -g) \
  ops/conduit/bin/polyedge-federated-token-refresh research "$research_token" "$issuer"
[ "$(stat -c %a "$research_token")" = 600 ]
before=$(sha256sum "$research_token" | awk '{print $1}')
if FAKE_BAD=1 POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake \
  POLYEDGE_FEDERATED_TOKEN_EXPECTED_UID=$(id -u) POLYEDGE_FEDERATED_TOKEN_EXPECTED_GID=$(id -g) \
  ops/conduit/bin/polyedge-federated-token-refresh research "$research_token" "$issuer" 2>/dev/null; then
  echo 'invalid JWT-SVID was accepted' >&2
  exit 1
fi
[ "$before" = "$(sha256sum "$research_token" | awk '{print $1}')" ]

FAKE_EXPECTED_SUBJECT=spiffe://polyedge.local/conduit/funded-intent-producer \
  POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake \
  POLYEDGE_FEDERATED_TOKEN_EXPECTED_UID=$(id -u) POLYEDGE_FEDERATED_TOKEN_EXPECTED_GID=$(id -g) \
  ops/conduit/bin/polyedge-federated-token-refresh funded-intent-producer "$producer_token" "$issuer"
[ "$(stat -c %a "$producer_token")" = 600 ]

POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake \
  POLYEDGE_FEDERATED_TOKEN_EXPECTED_UID=$(id -u) POLYEDGE_FEDERATED_TOKEN_EXPECTED_GID=$(id -g) \
  ops/conduit/bin/polyedge-federated-token-refresh promotion "$token" "$issuer"
[ "$(stat -c %a "$token")" = 600 ]
if POLYEDGE_FEDERATED_TOKEN_ROOT=$root SPIRE_AGENT_SOCKET=$socket SPIRE_AGENT_BIN=$fake \
  ops/conduit/bin/polyedge-federated-token-refresh invalid-lane "$token" "$issuer" 2>/dev/null; then
  echo 'unknown identity lane was accepted' >&2
  exit 1
fi
