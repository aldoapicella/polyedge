# Conduit host bundle

This is a rootful Podman/systemd bundle for the ARM64 `conduit-dev` host. It
uses immutable image digests, a private Podman network, a loopback-only
frontend, and Caddy as the sole public ingress. It has no Docker, Compose,
secrets, or automatic deployment.

## Hard gates

Do **not** start this stack or enable any timer until its exact Azure identity
and data scopes are assigned. The ring uploader supports the system-assigned
identity of an Azure Arc-enabled host, with no client secret. API, research,
and funded identities remain separate gates; a read SAS is not sufficient for
the writers or leases.

The API Quadlet persists its local recorder under `/srv/polyedge-ring`. Verify
that path is the 260-GB block-volume mount (not a directory on `/`) before every
start: `findmnt -T /srv/polyedge-ring && df -h /srv/polyedge-ring`.

Do **not** enable daily, replay, or qset shadow work on the boot disk. Their
working directories live under `/srv/polyedge-ring/jobs`; the runner refuses to
start with less than 40 GiB free. All Azure jobs remain disabled unless
`/etc/polyedge/ENABLE_AZURE_JOBS` exists, and their timers are not enabled by
the install sequence.

The legacy shadow schedule is intentionally absent. `shadow-qset` permits only
`campaign-2026-07-28-qset-v1` and remains disabled until manually approved.

## Install after both gates are approved

On Ubuntu/Debian, first verify the actual OS and packages, then install the
minimal runtime:

```sh
sudo apt-get install --no-install-recommends podman caddy curl gzip jq
sudo install -d -m 0750 /etc/polyedge/jobs /srv/polyedge-ring/jobs
sudo install -d -m 0700 /etc/polyedge/credentials/{api,research,shadow-qset}
# Bootstrap only: on upgrades preserve the installed Image= digests and use
# polyedge-quadlet-deploy; repository Quadlets intentionally contain placeholders.
sudo install -m 0644 ops/conduit/quadlets/* /etc/containers/systemd/
sudo install -m 0644 ops/conduit/systemd/* /etc/systemd/system/
sudo install -m 0755 ops/conduit/bin/polyedge-run-job /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-ring-sync /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-ring-health /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-boot-disk-guard /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-parity-hourly /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-parity-record-daily /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-github-deploy /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-quadlet-deploy /usr/local/sbin/
sudo install -m 0440 ops/conduit/sudoers/polyedge-deploy /etc/sudoers.d/
sudo install -m 0644 ops/conduit/journald/polyedge.conf /etc/systemd/journald.conf.d/
sudo install -m 0644 ops/conduit/caddy/Caddyfile /etc/caddy/Caddyfile
sudo install -m 0600 ops/conduit/env/api.env.example /etc/polyedge/api.env
sudo install -m 0600 ops/conduit/env/frontend.env.example /etc/polyedge/frontend.env
sudo install -m 0600 ops/conduit/env/funded-signer.env.example /etc/polyedge/funded-signer.env
sudo install -m 0600 ops/conduit/env/ring.env.example /etc/polyedge/ring.env
sudo install -m 0640 ops/conduit/env/parity-hourly.env.example /etc/polyedge/parity-hourly.env
sudo systemctl daemon-reload
sudo systemctl restart systemd-journald
```

The verified boot volume is 97 GB and the mounted root filesystem must retain
at least 15 GiB free. `polyedge-boot-disk-guard.timer` warns at 75%, prunes only
disposable package/Rust caches and images explicitly labeled
`io.polyedge.disposable=true` after seven days at 80%, and creates
`/run/polyedge/image-pulls-paused` at 85% or below the free-space floor.
Unlabeled images are preserved, so containers, volumes, databases, evidence,
digest-pinned rollback images, and rollback Quadlets are not automatic cleanup
targets. Approved host run paths
use `--pull=never`; only the digest deploy helper pulls after passing the guard
and rolls back if the post-deploy headroom check fails. Podman logs use journald,
whose persistent and runtime growth is capped by `journald/polyedge.conf`.
Each five-minute check also records its usage and available bytes in journald,
providing the minimum-free history required throughout the parity window.
The OCI freshness mapping accepts raw or gzip JSONL and uses a 900-second age
limit with a 600-second expected interval, matching the recorder segment cadence
without multiplying Azure blob transactions.

During dual-running, OCI schedules deliberately trail Azure: freshness by three
minutes and hourly quality by two minutes. Azure daily and replay remain at
00:30 and 03:00; OCI daily starts at 03:10 after both, and OCI replay at 03:15
waits behind it on the host lock. Azure remains authoritative. Daily, replay,
shadow, and manual data-producing jobs share the research lock; the bounded
freshness, hourly, and parity audits stay independent so a long daily cycle
cannot create a monitoring or parity gap. Their combined CPU caps remain within
the four-core host. Revisit the offsets only after Azure compute deletion, not
during parity.
The extra freshness minute lets the local ring upload finish before the Azure
blob-age query runs.

Ledger `/srv/polyedge-ring/parity/20260816T100000Z.json` is the current formal
window. It starts with zero inherited credit at `2026-08-16T10:00:00Z`, keeps
Azure authoritative and `azureDeletionAllowed:false`, and cannot finish before
`2026-08-19T10:00:00Z`. The pre-window catch-up was recorded only as excluded
evidence. Completion still requires 72 consecutive accepted hours, two
successful OCI daily cycles, reboot and rollback proof, and explicit qset,
funded, and deletion gates.

The superseded `20260816T090000Z.json` ledger retained zero credit. Its first
hour had 60/60 healthy observations and a 60-second maximum gap, but the
immutable ring recorded one `PolymarketClobMarket` WebSocket connection reset
at `2026-08-16T09:35:16Z`. The strict zero-essential-feed-error rule rejected
the hour; no credit was carried into the `10:00` window.

The superseded `20260815T070000Z.json` ledger retained three accepted hours at
`07:00`, `08:00`, and `09:00`. Each has six immutable segment and upload proofs,
60/60 healthy feed observations, zero essential-feed errors, a 60-second
maximum gap, and exact Azure/OCI same-input results. The first multi-hour
collection also exposed two collector defects: missing parentheses in the
cross-hour jq merge, then a shell-function state leak that selected the prior
hour's Azure report. Both root causes are fixed and covered by the collector
self-test. Pre-fix scripts remain under
`/etc/polyedge/rollback/20260815T092707Z-parity-adjacency-fix` and
`/etc/polyedge/rollback/20260815T093800Z-parity-validator-scope`.

The superseded `20260815T050000Z.json` ledger retained one accepted hour. Its
`06:00` hour recorded two `PolymarketClobMarket` feed errors, so strict
continuous-feed parity rejected the hour and no credit was carried forward.

The superseded `20260815T010000Z.json` ledger retained zero credit. The clean
`01:00` feed hour could not be accepted because the Azure hourly container ran
the correct image while its nested generator provenance still named the prior
digest. The `02:00` hour then recorded one `SourceStalled` RTDS handoff at
`2026-08-15T02:11:58Z`; the peer recovered in under five seconds, but strict
continuous-feed parity correctly rejected the hour. Source `8af04f6` fixes the
root cause by allowing the already-configured 30-second source timeout for peer
handoff instead of a 250-millisecond grace period. No rejected-hour credit is
carried into the new source/image binding.

The superseded `20260814T210000Z.json` ledger retained zero credit. Its `21:00`
hour recorded two RTDS EOF disconnects, and the strict continuous-feed gate
correctly rejected it. Earlier same-day ledgers starting at `16:00`, `18:00`,
and `20:00` were also retained with their immutable rejection evidence; no
failed or partial hour was carried into the new source/image binding.

The superseded `20260814T110000Z.json` ledger retained zero credit. Its first
hour had all 60 minute observations but recorded a Chainlink RTDS clean end at
`2026-08-14T11:52:51Z`; the second recorded a Binance RTDS clean end at
`2026-08-14T12:04:01Z`. Both recovered automatically, but the strict continuous
feed gate correctly rejected both hours. The `13:00` hour included the guarded
runtime image transition and its bounded startup reconnects, so the `14:18`
collector also failed closed. None of these hours was carried into the new
ledger.

The superseded `20260814T060000Z.json` ledger retained zero credit. A canary
from source `6fe5e23` serialized the Binance RTDS subscription filter as a bare
symbol, which the upstream service accepted but did not deliver. The watchdog
detected the stalled venue and the API was rolled back. The documented RTDS
contract requires `filters` to contain a JSON string, so source `777d9e1`
encodes the exact `{"symbol":"btcusdt"}` object. The corrected image was
deployed at `2026-08-14T09:52:49Z` and passed a ten-minute soak across 10:00 UTC
with all four essential feeds advancing, zero explicit recorder errors or
drops, zero restarts, and a healthy API. The next untouched hourly boundary is
therefore 11:00 UTC. The complete pre-reset bindings and old ledger are retained
under `/etc/polyedge/rollback/20260814T100407Z-rtds-json-filter-parity-reset`.

The superseded `20260814T030000Z.json` ledger also retained zero credit. Its
first hour had all 60 one-minute health observations, a 60-second maximum gap,
and no unhealthy observation, but the immutable ring recorded one
`PolymarketRtdsChainlink` disconnect at `2026-08-14T03:48:24Z`. The feed
recovered within the next observation, but the strict zero-essential-feed-error
gate correctly rejected the hour. The old bindings and ledger remain
recoverable under `/etc/polyedge/rollback/20260814T050739Z-parity-0600-reset`.

The superseded `20260814T010000Z.json` ledger retained zero credit. Its first
hour had all 60 one-minute health observations, a 60-second maximum observation
gap, and no unhealthy observation, but the immutable ring recorded one
`PolymarketRtdsChainlink` disconnect at `2026-08-14T01:22:16Z`. The strict
zero-essential-feed-error gate rejected the hour. The old bindings and ledger
remain recoverable under
`/etc/polyedge/rollback/20260814T022900Z-parity-0300-reset`.

The OCI API and all seven primary research jobs are pinned to
multi-architecture digest
`sha256:274b9b942bb415eaed56a0fa27c5a0bb95e1db9c72ccd32d31b8084fb46618c0`
from source `60f1a7cb7b61ffaa95a9743db65ffcf031cdfac6`. The registry index
contains the runnable Linux AMD64 and ARM64 images, and the same manifest was
imported without rebuild into ACR. OCI passed its guarded deployment and
15-minute soak with a healthy API, healthy frontend, zero restarts, current
essential feeds, and all seven primary job bindings on the exact source and
digest.

The isolated promotion controller then moved the Azure primary app, hourly
job template, and a bounded hourly proof execution to the exact imported ACR
digest. Its proof passed before the fresh one-use marker was archived; the
controller remains disabled at boot and inactive. Azure remains in
single-revision authoritative mode with its protected prior template retained
for rollback. None of the deployment or promotion soak receives parity credit;
the untouched `2026-08-16T10:00:00Z` boundary begins the current counter.

The hot ring was expanded online from 210 GB to 260 GB after a burst raised the
48-hour worst-case projection above the old capacity gate. The mounted
filesystem now exposes 273,655,873,536 bytes, the conservative projection is
213,297,534,144 bytes, and the 32-GiB ring reserve, sealing, and upload gates are
all green. The boot filesystem remains separate with 26 GB free at 74% used,
above the 15-GiB hard deployment floor. The five-minute disk guard and capped
journald growth remain active; image pulls are not paused.
Rollback state is retained under `/etc/polyedge/rollback`, including
`20260814T134524Z-azure-primary-e504c51`,
`20260814T134805Z-azure-hourly-e504c51`,
`20260814T135152Z-polyedge-api.container`,
`20260814T135500Z-unified-e504c51-parity-rebind`, and
`20260814T142500Z-parity-1500-reset`. The current guarded deployment added
`20260815T001144Z-polyedge-api.container` and
`20260815T043829Z-polyedge-api.container`; the seven job environments have
dated pre-`8af04f6` copies from `20260815T044216Z` and `20260815T044240Z`, and
the formal parity bindings have copies from `20260815T044322Z`. The old image,
Azure revision, failed ledgers, and evidence data remain present.

In the superseded August 12 window, the `15:00` hour received zero credit
because seven durable essential-feed reconnect errors occurred even though
recorder sequence durability and all six strict segment uploads remained
intact. The clean `16:00` feed hour also
receives zero credit because the collector found different Azure and OCI
decision-config hashes. Azure primary revision `polyedge-dev--0000124`
temporarily aligned `RTDS_CHAINLINK_WATCHDOG_SECONDS` to 290 seconds, but
immutable ring evidence showed that both RTDS topics froze together while
ping/pong stayed live. Azure revision `polyedge-dev--0000125` and OCI therefore
restored the native 30-second watchdog while preserving immutable images, paper
execution, and `ALLOW_LIVE=false`. Azure provenance at
`2026-08-12T17:48:18Z` had emitted
the exact OCI decision-config hash
`sha256:001c4279754bea3c85c16081b631cea4ae522736a36370cfae5835b096b1d5f0`.
The first formal-window OCI provenance observation at
`2026-08-12T18:00:56Z` had that hash and all four essential feeds healthy, but
the hour contained one Chainlink stall and four CLOB disconnect or HTTP 503
errors. The fail-closed collector rejected that hour. Later replacement windows
also remain at zero because of feed continuity or superseded source/image
bindings; no credit is inherited. The exact Azure hourly deployment completed
too late to prove its first scheduled execution before `02:00`, so that
historical window started at the untouched `03:00` boundary. Qset remains
disabled, the local funded signer remains masked, and no Azure compute or
network resource is deletion-eligible yet.

`polyedge-parity-hourly.timer` runs at `:18` after the Azure `:10` and OCI
`:12` audits. It hash-verifies the six local segments and upload receipts,
requires one recorder instance with exact in-segment and cross-hour sequence
continuity, requires zero recorder failures or backlog, and requires canonical
`Discovery`, `PolymarketClobMarket`, `PolymarketRtdsChainlink`, and
`PolymarketRtdsBinance` status to be `ok` and no older than five minutes. The
same conditions must appear in at least 60 minute-level runtime provenance
observations spanning the hour, with at most 75 seconds between observations
and no essential-feed error event. It compares the Azure scheduled result with
a local same-input audit and requires one identical `decision_config_sha256`
across all three reports. It advances only the sequential clean-hour count. It
never changes Azure authority, deletion, reboot, qset, or funded gates and
fails closed on a gap, duplicate, instance change, unhealthy recorder,
degraded or stale essential feed, decision-config mismatch, or result mismatch.
After a successful OCI daily container exits, `polyedge-parity-record-daily`
verifies the immutable primary bundle, normalized completion marker, every
artifact hash/size, approved source/image, promotion-quality predicate, ring
health, and both disk floors before advancing the
sequential daily-cycle count. The daily and hourly collectors share only the
short parity-ledger lock. Daily-cycle evidence never changes Azure authority,
deletion, reboot, qset, funded, or hourly-continuity fields and is not by itself
output-parity or deletion proof.

Primary OCI job environments must set
`POLYEDGE_DISABLE_RESEARCH_ARTIFACT_PUBLISH=true`. This keeps reports local
during parity while preserving Azure reads and the shared daily/replay lease;
the runner fails closed if the flag is absent. Shadow-qset is excluded because
its separately approved lane must publish only to its own evidence scope.

On a systemd-resolved host, point `/etc/resolv.conf` at its non-stub resolver
file so Aardvark can forward external DNS. If UFW has a deny-input policy, allow
DNS only from the inspected Podman subnet and interface to its gateway. The
verified `conduit-dev` values are:

```sh
sudo ln -sfn /run/systemd/resolve/resolv.conf /etc/resolv.conf
sudo ufw allow in on podman1 from 10.89.0.0/24 to 10.89.0.1 port 53 proto udp
sudo ufw allow in on podman1 from 10.89.0.0/24 to 10.89.0.1 port 53 proto tcp
```

Re-run `podman network inspect polyedge` after recreating the network; do not
reuse these addresses if its interface, subnet, or gateway changed.

The GitHub deploy key belongs to a locked `polyedge-deploy` account. Its only
authorized-key command is `/usr/local/libexec/polyedge-github-deploy` with the
`restrict` option. The wrapper and sudoers rule permit only digest-pinned API,
frontend, or funded-signer deployments through the validated Quadlet helper.

Replace each zero digest and every remaining `REPLACE_...` value in the
installed files, not in this repository. Provision the three API/frontend
Podman secrets named by their Quadlets before starting either service. Keep
API/frontend image digests in their Quadlets; set the same reviewed backend
digest in each enabled job env file. Configure Caddy with a real DNS name and
allow only SSH, TCP/80, and TCP/443 in OCI and the host firewall. Never expose
port 3000 or 8081.

`ring.env.example` starts with `POLYEDGE_RING_SEAL_ONLY=1`, so it can hash local
segments before Azure identity approval. Set it to `0` only after filling the
digest and either verifying the Arc identity's no-delete blob role or installing
the client-secret-file fallback. For the one-time sequenced-recorder handoff,
set `POLYEDGE_RING_LEGACY_CUTOFF_EPOCH` to the exact 10-minute epoch where the
new API instance begins. The sealer emits schema 2 only before that boundary
and requires source-verified schema 3 at or after it; schema 2 never receives
new parity credit. Remove the cutoff after all earlier segments are sealed.

For Arc, connect the host as `conduit-dev`, disable remote management features,
and assign `PolyEdge OCI Blob Writer` only at the `bot-events` container scope:

```sh
sudo azcmagent config set incomingconnections.enabled false
sudo azcmagent config set guestconfiguration.enabled false
sudo azcmagent config set extensions.enabled false
sudo azcmagent show
```

The uploader alone uses host networking to reach Arc's loopback identity
endpoint and mounts only `/var/opt/azcmagent/tokens` read-only. Other containers
remain on the private `polyedge` network.

Federated JWT-SVIDs rotate into `/run/polyedge-federated-LANE/azure-federated-token`
with mode `0600`; each container receives only its lane at `/run/credentials`.
The Rust client still supports the reviewed client-secret-file rollback, but a
lane must set exactly one credential-file variable. Never put a JWT, client
secret, private key, or join token in Git, shell arguments, chat, or logs.

## Separate UAMI workload identities

Do not create Entra applications. The tenant disables application creation.
Four Azure user-assigned managed identities (UAMIs) and their exact federated
identity credentials now exist. The isolated
`infra/conduit-federated-identity.bicep` template created one lane per
deployment and no role assignment. Each lane what-if produced exactly one UAMI
and one FIC, with no updates or deletes; the deployments ran sequentially only
after the public issuer and observed JWT claims matched the template.

The issuer is `https://oidc.jupiterlabs.dev` and requires a reserved OCI public IP, valid
TLS, and inbound TCP/80 and TCP/443 in OCI and UFW. Do not use an ephemeral
platform hostname. Keep the same exact issuer in the three SPIRE configs,
`spire.env`, Caddy, and the Bicep `issuer` parameter.

Use the verified SPIRE 1.15.2 ARM64 musl artifacts. Their SHA-256 values are
`92e782b285c50c62cdf37fdfa8917ea68fa57685b3bf99d03db36da4095678fa`
for SPIRE and
`ec67c4d5e21b20a129d1f368f401e4fbb2bcd4fd5c13aa08a97778da94f52717`
for SPIRE extras. Install only after both downloaded files pass `sha256sum -c`.
The server is configured for a five-minute JWT-SVID, RSA-2048/RS256 signing,
SQLite, disk keys, and the fixed `polyedge.local` trust domain. This remains a
single-host root trust and availability ceiling.

Create fixed service accounts once, then install the reviewed files:

```sh
sudo groupadd --system spire-workload
sudo groupadd --system spire-server
sudo useradd --system --no-create-home --gid spire-server --shell /usr/sbin/nologin spire-server
sudo useradd --system --no-create-home --gid caddy --shell /usr/sbin/nologin spire-oidc
for lane in api research shadow-qset funded-signer; do
  sudo groupadd --system "polyedge-identity-$lane"
  sudo useradd --system --no-create-home --gid "polyedge-identity-$lane" --shell /usr/sbin/nologin "polyedge-identity-$lane"
done
sudo install -d -m 0755 /opt/spire/bin /etc/spire
sudo install -m 0755 spire-server spire-agent /opt/spire/bin/
sudo install -m 0755 oidc-discovery-provider /opt/spire/bin/
sudo install -m 0644 ops/conduit/spire/server.conf /etc/spire/server.conf
sudo install -m 0644 ops/conduit/spire/agent.conf /etc/spire/agent.conf
sudo install -m 0644 ops/conduit/spire/oidc-discovery-provider.conf /etc/spire/oidc-discovery-provider.conf
sudo install -m 0600 ops/conduit/env/spire.env.example /etc/polyedge/spire.env
sudo install -m 0755 ops/conduit/bin/polyedge-federated-token-refresh /usr/local/libexec/
sudo install -m 0644 ops/conduit/systemd/spire-*.service /etc/systemd/system/
sudo install -m 0644 ops/conduit/systemd/polyedge-federated-token@.* /etc/systemd/system/
```

Bootstrap the one agent with a root-only join-token file, then remove that file
after its first attestation and start the persistent unit without a token. Use
the node ID `spiffe://polyedge.local/conduit-dev`. Register the OIDC
provider and each fetcher with both its Unix user and the fixed official SPIRE
Agent binary path. The four workload IDs are exactly:

```text
spiffe://polyedge.local/conduit/api
spiffe://polyedge.local/conduit/research
spiffe://polyedge.local/conduit/shadow-qset
spiffe://polyedge.local/conduit/funded-signer
```

The bootstrap and registrations are credential-safe when the short-lived join
token stays in a root-only file and never appears in a process argument:

```sh
server_socket=/run/spire-server/api.sock
node_id=spiffe://polyedge.local/conduit-dev
sudo systemctl enable --now spire-server.service
sudo sh -c 'umask 077; /opt/spire/bin/spire-server bundle show -socketPath /run/spire-server/api.sock -format pem > /etc/spire/bootstrap.crt'
sudo sh -c '/opt/spire/bin/spire-server token generate -socketPath /run/spire-server/api.sock -spiffeID spiffe://polyedge.local/conduit-dev -output json | jq -er .value > /etc/spire/join-token && chmod 0600 /etc/spire/join-token'
sudo sh -c 'timeout --signal=TERM 25 /opt/spire/bin/spire-agent run -config /etc/spire/agent.conf -joinTokenFile /etc/spire/join-token > /run/spire-agent-bootstrap.log 2>&1 || [ "$?" -eq 124 ]'
sudo grep -q 'Node attestation was successful' /run/spire-agent-bootstrap.log
sudo unlink /etc/spire/join-token
sudo unlink /run/spire-agent-bootstrap.log
sudo systemctl enable --now spire-agent.service

sudo /opt/spire/bin/spire-server entry create -socketPath "$server_socket" \
  -parentID "$node_id" \
  -spiffeID spiffe://polyedge.local/oidc-discovery-provider \
  -selector unix:user:spire-oidc \
  -selector unix:path:/opt/spire/bin/oidc-discovery-provider
for lane in api research shadow-qset funded-signer; do
  sudo /opt/spire/bin/spire-server entry create -socketPath "$server_socket" \
    -parentID "$node_id" \
    -spiffeID "spiffe://polyedge.local/conduit/$lane" \
    -selector "unix:user:polyedge-identity-$lane" \
    -selector unix:path:/opt/spire/bin/spire-agent \
    -jwtSVIDTTL 300
done
sudo systemctl enable --now spire-oidc-discovery-provider.service
sudo systemctl enable --now polyedge-federated-token@research.timer
```

The provider listens only on `/run/spire-oidc/provider.sock`; Caddy publishes
only `/.well-known/openid-configuration` and `/keys`. The Workload API remains
on `/run/spire/agent.sock`. Enable only the research token timer first. Before
creating its FIC, decode claims locally without printing the token and verify
`alg=RS256`, the exact HTTPS issuer and SPIFFE subject, the sole audience
`api://AzureADTokenExchange`, and a lifetime no greater than six minutes.

```sh
az deployment group create --resource-group rg-polyedge-dev \
  --template-file infra/conduit-federated-identity.bicep \
  --parameters lane=research issuer=https://oidc.jupiterlabs.dev \
  --parameters tags='{"owner":"polyedge","migration":"oci-compute-plane"}'
```

The research UAMI first received only a temporary Blob Data Reader role on the
`bot-events` container. Its bounded workload-federation `getProperties` proof
passed, after which the temporary role was replaced with the reviewed exact
container Contributor role. The remaining FICs were created sequentially and
only the positive scopes in `identity-rbac-plan.json` were assigned. Live IDs,
role counts, positive reads, and cross-lane 403 checks are captured without
credentials in `identity-rbac-proof.json`.

The isolated promotion controller is a fifth UAMI,
`id-polyedge-conduit-promotion-controller`, with only the custom `PolyEdge OCI
Promotion Controller` role assigned directly to `polyedge-dev` and
`polyedge-hourly-quality-job`. Its local `promotion` lane maps exactly to
`spiffe://polyedge.local/conduit/promotion-controller`; its live UAMI, FIC,
two assignments, and isolated SDK 200/403 proof are recorded in
`identity-rbac-proof.json`. A resource-group resources-list response can be
200 while returning only those two individually readable resources; it is Azure
filtered-list behavior, not resource-group read access. The controller remains
disabled.

## Promotion controller runtime

`polyedge-azure-promotion.service` uses the checksum-verified isolated Node
v24 LTS runtime at `/opt/polyedge-node`. The installed runtime is `v24.19.0`
Linux ARM64, verified from the release archive SHA-256
`01443c1e1a29e531ccad5a46fefa6df490d2189c49f7955904aecdbb0fe86fdc`.
Extract it with `sudo tar --no-same-owner -xJf ... -C /opt`, or explicitly fix
ownership, then require this to print nothing before a root unit uses it:

```sh
find -L /opt/polyedge-node -xdev \( ! -user root -o ! -group root -o -perm /022 \) -print
```

Do not substitute system `/usr/bin/node` 18: it is EOL and unsupported by the
installed Azure SDK.

The funded signer is a separate, no-ingress, read-only container. Only it gets
the Podman wallet secrets and its dedicated Azure funded identity; the API and
research jobs get neither. It has no install target and remains disabled until
the funded identity, exact non-secret environment, origin check, queue repair,
and `FUNDED_EVIDENCE_TRUST_BOUNDARY_READY` review all pass. Root remains the
single-host trust ceiling and can administer both containers.

The continuous funded v10 cutover is not yet live; the local OCI funded signer
is also still disabled. Guarded run `31865237346` passed safety/history,
zero-write preflight, exact controls, Service Bus, qset isolation, and warmup.
Three intents then failed closed before submission at the final volatile,
post-only-cross, and WebSocket gates. One more lifecycle completed within the
seven-second send bound and attempted submission, but did not prove an accepted
order or reach terminal reservation verification, so the workflow refused the
cutover and rolled back. Azure revision `polyedge-funded-direct-cl--0000201` is
healthy in the dormant state with minimum replicas zero, dry-run true, and all
live/trading controls false. The queue is `SendDisabled` with zero active or
scheduled messages and 860 quarantined messages. No matching application
dead-letter event identifies the newest quarantine, so nothing is replayed or
purged without classification.

Primary research jobs share one serialized workspace so daily normalization,
replay, prospective validation, and backfills reuse local artifacts. The qset
workspace remains separate. The recorder writes one fsynced JSONL segment per
ten-minute UTC bucket. The ring timer leaves that local job input untouched,
creates a deterministic gzip sidecar outside the job input tree, seals both
hashes in a v2 manifest, uploads the compressed payload as an immutable Azure
Hot-tier blob without any remote listing, and verifies retry collisions byte
for byte. V1 uncompressed receipts remain valid. Local source, sidecar, and
manifest files are retained for 48 hours to leave job-workspace headroom on the
260-GB volume, then removed only after the immutable remote manifest is re-read
successfully. A separate health timer checks upload age and projected capacity
and stops the API before free space falls below 32 GiB. Azure tiers only the
future `events-oci-hot7-v1/` prefix to Cool after seven days and Archive after
30 days.

After the approved authentication and separate-volume gates, create the marker
and enable only the intended timers:

```sh
sudo touch /etc/polyedge/ENABLE_AZURE_JOBS
sudo systemctl enable --now polyedge-api.service polyedge-frontend.service caddy.service
sudo systemctl enable --now polyedge-ring-sync.timer
sudo systemctl enable --now polyedge-ring-health.timer
sudo systemctl enable --now polyedge-boot-disk-guard.timer
sudo systemctl enable --now polyedge-parity-hourly.timer
sudo systemctl enable --now polyedge-freshness.timer polyedge-hourly.timer
# Enable daily/replay/qset individually only after their validation.
# sudo systemctl enable --now polyedge-daily.timer polyedge-replay.timer
# sudo systemctl enable --now polyedge-shadow-qset.timer
```

## Digest deployment

After the API/frontend are healthy, update one reviewed GHCR ARM64 digest at a
time. The helper accepts only these three units, updates only their installed
`Image=` line, makes a timestamped rollback copy, pulls and checks
`linux/arm64`, then restarts and verifies the running container's exact digest.
Any restart or verification failure restores the prior Quadlet and restarts it.

```sh
sudo /usr/local/sbin/polyedge-quadlet-deploy polyedge-api \
  ghcr.io/OWNER/polyedge-rust-backend@sha256:LOWERCASE_64_HEX_DIGEST
sudo /usr/local/sbin/polyedge-quadlet-deploy polyedge-frontend \
  ghcr.io/OWNER/polyedge-frontend@sha256:LOWERCASE_64_HEX_DIGEST
sudo /usr/local/sbin/polyedge-quadlet-deploy polyedge-funded-signer \
  ghcr.io/OWNER/polyedge-venue-probe@sha256:LOWERCASE_64_HEX_DIGEST
```

Rollback copies live in `/etc/polyedge/rollback/`. The helper never accepts a
tag, a non-GHCR registry, a mismatched image repository, or any unit other than
API, frontend, or funded signer.

Schedules are UTC: freshness every five minutes; hourly quality at `:12`;
primary daily at 03:10; replay at 03:15; the disabled qset shadow timer remains
configured for 02:15. Data-producing jobs use one
`flock -w 129600 /run/polyedge/research.lock` (36 hours); bounded audits bypass
it. Daily, replay, and qset are each capped at 1.5 CPU. During parity, qset and
funded are disabled, so API/frontend (1 CPU), one writer (1.5), freshness (0.5),
ordered hourly or parity (0.5), and the ring uploader (0.5) total 4 OCPUs.
Origin check is manual and unscheduled; re-budget before enabling funded.

## Verify, reboot, rollback

```sh
sudo systemd-analyze verify /etc/systemd/system/polyedge-job@.service
systemd-analyze calendar '*-*-* *:03/5:00 UTC' '*-*-* *:12:00 UTC' '*-*-* 03:10:00 UTC' '*-*-* 03:15:00 UTC'
systemctl list-timers 'polyedge-*'
podman ps --format '{{.Names}} {{.Status}}'
curl -fsS https://YOUR_DOMAIN/api/backend/health | jq -e '.ok and .execution_mode == "paper" and .kill_switch == false'
curl -fsS https://YOUR_DOMAIN/api/backend/status | jq -e '.task_health.api == "ok" and .task_health.runtime_loop == "running"'
journalctl -u polyedge-api.service -u polyedge-frontend.service -u caddy.service -b
sudo jq . /srv/polyedge-ring/status.json
```

For a no-data lock test:

```sh
sudo systemd-run --unit=polyedge-lock-a --collect /usr/bin/flock /run/polyedge/research.lock /bin/sleep 20
sudo systemd-run --unit=polyedge-lock-b --collect /usr/bin/flock /run/polyedge/research.lock /usr/bin/date
journalctl -u polyedge-lock-a -u polyedge-lock-b --since '1 minute ago'
```

Reboot only after health passes, then repeat the health, status, `podman ps`,
and `systemctl list-timers` checks. For rollback, restore the prior saved
`polyedge-api.container` and `polyedge-frontend.container` (both immutable
digests), run `sudo systemctl daemon-reload`, restart the two services, and
repeat the health checks. Retain the prior images until the rollback window
closes.
