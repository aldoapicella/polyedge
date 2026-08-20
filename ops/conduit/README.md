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
that path is the 310-GB block-volume mount (not a directory on `/`) before every
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
sudo install -m 0755 ops/conduit/bin/polyedge-ring-quarantine /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-ring-health /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-boot-disk-guard /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-parity-hourly /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-parity-record-daily /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-github-deploy /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-quadlet-deploy /usr/local/sbin/
sudo install -m 0755 ops/conduit/bin/polyedge-funded-secret-bootstrap /usr/local/sbin/
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
freshness, hourly, parity, and ring-upload utilities share a separate utility
lock, so a long daily cycle cannot create a monitoring or parity gap and only
one utility consumes its 0.5-CPU allocation at a time. Revisit the offsets only
after Azure compute deletion, not during parity.
The extra freshness minute lets the local ring upload finish before the Azure
blob-age query runs.

The funded-active ledger `/srv/polyedge-ring/parity/20260820T180000Z-funded-active.json`
failed closed with zero credit at 19:26: its audit outer Git provenance was `6b`
while OCI API runtime provenance was `103d`. It remains immutable, with Azure
authoritative and `azureDeletionAllowed:false`.

The replacement 21:00 funded-active ledger is staged at zero credit, but its
collector timer is paused: two recovered pre-order warmup retries left
invocation-cumulative `failed_messages=2`, and the strict zero-failure gate
remains. Neither gate weakening nor a signer restart has explicit approval.

The scheduled 17:18 collector run failed closed because `/etc/polyedge/parity-hourly.env`
still pinned superseded funded digest `sha256:218e4e20d5d8372fec8ae7262b370fd5507b3125815073b00ddbb5a97a01c637`
while the fresh ledger and live signer pinned `sha256:912b5e345d14f3abbe666b5dd462208271f582a98ea83ef338f4fc391a41c1ee`;
the ledger remained zero. At 17:20:15, that single non-secret image pin was
atomically corrected with rollback
`/etc/polyedge/rollback/20260820T171900Z-parity-funded-image/parity-hourly.env.before`.
The clean 17:22:17 retry succeeded, created only valid excluded `16:00` evidence,
and left the ledger at zero with deletion false.

The superseded `20260816T100000Z.json` ledger retained three accepted hours at
`10:00`, `11:00`, and `12:00`. None is carried forward because source
`6b567ac` and its new image fix primary daily decision-grade applicability,
stable runtime provenance, and bounded executable-markout evidence. The new
window begins at a clean UTC-day boundary so its first daily cycle cannot mix
old and new binaries.

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

The OCI API and all seven primary research-job environments now run the `f644`
image with `103d` runtime provenance from build run `31984694797`. Freshness at
19:48 and hourly quality at 20:12 succeeded; the ring is healthy and boot free
space is 24 GiB.

Azure controller transaction `f646e7d4-194e-4aa9-b1ad-ed2fab1a8a54` promoted
primary revision `polyedge-dev--0000139` and the hourly image/generator to ACR
`f644`; proof execution `4xejmi6` succeeded at 20:17:31. Protected app hashes
are unchanged. Its marker is archived under
`/etc/polyedge/rollback/20260820T194400Z-primary-103d-parity-2100`; the journal
is retained and the controller is disabled and inactive. This promotion receives
no parity credit. The rollback controller restored the exact prior ACR `93a`
app, hourly, and generator at 20:27; its journal reached `rolled_back`, protected
hashes remained unchanged, and the archived rollback-journal SHA-256 begins
`4c09f80d`. Fresh transaction `60724954-fed1-44bf-aacd-ff97be066418` then
re-promoted exact `f644`/`103d`; proof execution
`polyedge-hourly-quality-job-ttm273a` succeeded. The current live journal is
retained, its used marker is archived, protected hashes remain unchanged, and
this re-promotion also receives no parity credit.

The August 16 pre-boundary daily container completed every research stage, but
its superseded bundle was correctly rejected by the parity recorder because it
predated the approved source and corrected primary decision-grade evidence. It
received zero credit. The installed runner, recorder, image revision, and
formal bindings now match the approved source; the first eligible daily cycle
is the run after the formal boundary.

The hot ring was expanded online from 210 GB to 260 GB on August 14 and from
260 GB to 310 GB on August 20 after the measured 48-hour projection again
exceeded the capacity gate. OCI reports 310 GB at 0 VPU/GB; the mounted
filesystem exposes 326,492,274,688 bytes, with 131,765,489,664 bytes free. The
conservative projection is 257,210,376,768 bytes, and the 32-GiB reserve,
sealing, quarantine, and upload gates are all green. The boot filesystem remains
separate with 25,461,936,128 bytes free at 75% used, above the 15-GiB hard
deployment floor. The five-minute disk guard and capped journald growth remain
active; image pulls are not paused.

For v4 recorder segments, a discontinuous sequence proof is never repaired or
sealed. The sync process writes a content-addressed receipt under
`/srv/polyedge-ring/quarantine/recorder-sequence-proof-v1`, preserves the exact
source without an archive or manifest, continues sealing and uploading later
valid segments, and still exits nonzero so ring health and parity remain red.
Receipt/source changes, missing sources, sidecars, malformed entries, and path
escapes fail before upload. Recorder write failures retain and reconcile the
exact staged JSONL bytes before later sequence-bound events are admitted; they
are not re-recorded as a new batch.

An explicitly approved segment that ends on or before the formal evidence
boundary can be preserved without sealing or parity credit. Run the root-only
operator command with the content-addressed receipt ID, exact boundary epoch,
and the real approval reference (do not substitute the placeholder):

```sh
sudo /usr/local/libexec/polyedge-ring-quarantine \
  RECEIPT_ID FORMAL_BOUNDARY_EPOCH APPROVAL_REFERENCE
```

The command serializes with ring sync, requires the host's Azure Arc managed
identity, and is bounded by a three-hour outer timeout. It uses create-only
uploads under
`events-oci-quarantine-v1/invalid-recorder-sequence-proof/<receipt-id>/`,
uploads `resolution.json` last, and downloads every object for hash
verification. Only then does it atomically publish the four-file local bundle
under `quarantine/resolved-recorder-sequence-proof-v1/<receipt-id>/`. The
original source and receipt deliberately remain in place; the resolved bundle
is a content-addressed local copy, not a move. Its resolution declares
`active_ring:false`, `parity_eligible:false`, and indefinite retention outside
the normal lifecycle, so neither copy gains an archive or normal manifest.
Idempotent reruns still perform create-if-absent and remote read-back
verification for all three objects before succeeding. Missing, partial,
tampered, post-boundary, or orphan resolution state keeps ring health red.

The approved production resolution completed on `2026-08-17` for receipt
`29b09463cc8554f1a950ea7c5860be1573a102bc1c07d6f64877239972f48958`.
It preserves the 399,238,014-byte source with SHA-256
`723290d5c5a569220cf2d21e58db43cc7bc07b0021e66bb0c2000c3ac5ea716b`
under the fixed remote quarantine prefix and binds it before the exact
`2026-08-17T12:00:00Z` boundary. The first run created all three objects; the
second returned `already_resolved:true` with zero new objects after bounded
read-back verification. Catch-up then uploaded and verified 22 segments, and
removed 17 local segments only after their existing remote proofs and the
48-hour retention check. Ring health at `2026-08-17T06:48:13Z` reported zero
unresolved or malformed quarantine entries, one resolved entry, zero unsealed
closed segments, zero unuploaded segments, fresh uploads, all capacity/free
gates green, and 116 GiB free on the ring volume. The API stayed healthy and
the enabled sync timer was restored.

Rollback state is retained under `/etc/polyedge/rollback`, including
`20260814T134524Z-azure-primary-e504c51`,
`20260814T134805Z-azure-hourly-e504c51`,
`20260814T135152Z-polyedge-api.container`,
`20260814T135500Z-unified-e504c51-parity-rebind`, and
`20260814T142500Z-parity-1500-reset`. The current guarded deployment added
`20260816T143153Z-polyedge-api.container`,
`20260816T143244Z-research-image-6b567ac`,
`20260816T152556Z-azure-promotion-retry-6b567ac`, and
`20260816T153012Z-parity-20260817-reset`. The funded-handoff controller update
retains its prior runtime source in
`20260816T160010Z-funded-handoff-controller`. The old image, Azure revision,
superseded ledgers, controller journals, and evidence data remain present.

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
disabled and no Azure compute or network resource is
deletion-eligible yet.

The live Azure reconciliation on `2026-08-20` is captured in
`compute-plane-mapping.json`. All four Container Apps remain provisioned, all
three managed environments are occupied, and both NAT gateways and managed load
balancers are attached to active environment networks; the current network
deletion-candidate count is therefore zero. Cost Management usage for August
1-19 totals $278.51 pretax, about $440/month when normalized. The
evidence-backed removable compute/network opportunity is about $300-330/month
after mapping, parity, reboot, rollback, funded, and qset gates pass. Immediate
safe deletion savings remain $0.

The canonical legacy correction `shadow-2026-07-23-through-2026-07-23` pointer
and state hash match immutable state, but it remains `in_progress`: `completed_at`
is absent and `daily/2026-07-23/latest.json` is absent. It must resume through
the existing legacy shadow recovery gates, not be force-completed. Two exhaustive
stable listings found 1,440 blobs / 7,473,225,576 bytes on `2026-07-23` and 782
blobs / 4,187,252,980 bytes on D+1 `2026-07-24`; all are unsealed Append Blobs.
Recovery is NO-GO: no job was started and no source was sealed or changed. This is
independent of frozen `campaign-2026-07-28-qset-v1`; qset and Azure deletion
remain blocked.

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
An active-funded window additionally requires the signer and token-rotation
timer to be reboot-enabled and active, the exact image and isolated UID, a JWT
rotated within four minutes, at least 50 healthy per-minute heartbeats, ready
market/user channels and safety cache, zero gaps/unparsed frames, zero alerts,
zero failed messages, and zero reconciliation or latency blockers. The evidence
stores only this bounded status summary, never a token or wallet secret.
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

The funded signer alone uses fixed container IP `10.89.0.250`. The required
`polyedge-funded-egress.service` SNATs only that address to secondary private IP
`10.0.0.81`, whose reserved OCI public IP is `149.130.186.60`; it does not
change the host default route or SSH traffic. Before activation, require the
unit to be active and the exact rule to pass `iptables -t nat -C`, then verify
the signer container observes the reserved public IP.

After the funded-signer identity has read access to the existing Key Vault
secrets, and while `polyedge-funded-signer.service` is stopped, bootstrap its
five rootful Podman secrets exactly once:

```sh
sudo env \
  AZURE_TENANT_ID=9767f0dc-e83f-4cc1-94e1-0d5f9d287d32 \
  AZURE_CLIENT_ID=d9ce9154-66a6-4bdb-839f-0da7b02b38da \
  /usr/local/sbin/polyedge-funded-secret-bootstrap
```

The helper accepts only those identity bindings, the five funded Key Vault
names, and the five Quadlet secret names. It refuses to overwrite or delete a
pre-existing Podman secret. If a later create fails, it exits nonzero and lists
only the names created before the failure. Keep the service stopped, inspect
those names with `sudo podman secret inspect NAME`, manually remove that exact
partial set with `sudo podman secret rm NAME...`, and rerun; the helper never
performs that destructive recovery itself.

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
research jobs get neither. It was enabled only after
the funded identity, exact non-secret environment, origin check, queue repair,
and `FUNDED_EVIDENCE_TRUST_BOUNDARY_READY` review passed. Root remains the
single-host trust ceiling and can administer both containers.

The continuous funded v10 OCI signer is active on
`ghcr.io/aldoapicella/polyedge-venue-probe@sha256:912b5e345d14f3abbe666b5dd462208271f582a98ea83ef338f4fc391a41c1ee`
as UID:GID `986:982`; the Azure signer is stopped with zero
replicas and its controls disabled. The producer and funded queue handoff remain
enabled. The queue is `Active` with zero active or scheduled messages and 932
historical quarantined messages; nothing was replayed or purged. The
loss-tolerant v3 profile preserves at least `$2` or 10% of fully reconciled
current equity and resizes later orders to 5% of current equity after losses
without martingale.

An intermittent Polygon RPC response, `no nodes available for platform polygon-bor`,
previously failed closed during redemption reads. The guarded replacement deploys
Tenderly with explicitly configured HTTPS-only dRPC and PublicNode fallbacks through
the maintained `viem` fallback transport; a future private primary receives no
silent public fallback. The former 350-second window let synchronous automatic
redemption take about 38 seconds while next-market non-executable warmup sequence
18720, with a 30-second TTL, arrived and expired. The maximum is now 300 seconds
inside the final-360-second safety boundary; the next 17:15 warmup was consumed,
with gaps, reconciliation, cache, and unresolved state clean and the DLQ stable at
932. After deploying `sha256:912b5e345d14f3abbe666b5dd462208271f582a98ea83ef338f4fc391a41c1ee`,
the natural 17:30 UTC warmup was consumed with `processed=1`, `failed=0`, both
channels ready, zero gaps, reconciliation false, cache error null,
`startup_unresolved:0`, and DLQ stable at 932.

On `2026-08-20`, exact reservation
`e3ced30000241eaec55b59c437b00af5fd642c4de080afaa04807951eaf72e91` was
reconciled after an ambiguous post-submission RPC failure. The reviewed recovery
bound the immutable reservation and completion hashes, waited more than 24
hours, authenticated against the production CLOB, proved zero open orders,
post-reservation trades, unresolved positions, and exact positions, then used
the reservation ETag for a single conditional finalization. Read-back reports
`finalized_no_fill`, `order_submitted:true`, matched notional zero, complete
reconciliation, zero-open proof, and an unresolved campaign index count of zero.

The post-promotion audit found that a later primary template no longer carried
the four non-secret operator-direct Service Bus producer bindings. They were
restored on the unchanged image in one healthy Single-mode revision with paper
execution, `ALLOW_LIVE=false`, and taker orders still disabled. The next two
eligible intents submitted successfully in 4,442 ms and 5,118 ms against a
7,000 ms bound. The service then held new exposure while one position and its
risk reservation were unresolved, automatically returned to zero blockers at
fully reconciled equity of `$21.084301`, recomputed its protected reserve to
`$2.10843`, and submitted the next `$2.30` eligible order in 4,041 ms. The
active Bicep profile now owns those bindings, and the promotion controller
refuses to run when any is absent.

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
it. Daily, replay, and qset are each capped at 1.5 CPU. Freshness, hourly,
parity, and ring upload share `/run/polyedge/utility.lock`, so only one 0.5-CPU
utility runs at a time. With qset disabled, API/frontend (1 CPU), funded signer
(0.5), one writer (1.5), and one utility (0.5) allocate at most 3.5 OCPUs to
heavy workloads, leaving 0.5 OCPU for the OS, Caddy, SPIRE, and token refresh.
Origin check is manual and unscheduled.

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
