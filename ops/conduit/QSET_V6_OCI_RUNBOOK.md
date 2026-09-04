# qset-v6 OCI operations runbook

This bundle is additive. It never stops, reconfigures, or removes RBAC from qset-v1/v2/v3/v4/v5 or funded lanes. Qset-v2/v3/v4/v5 remain retained; qset-v5 stays active because its terminal receipt does not authorize writer retirement. Qset-v6 has two new zero-cost identities and two distinct SPIFFE/token lanes:

- writer: `id-polyedge-conduit-shadow-qset-v6-writer`, `spiffe://polyedge.local/conduit/shadow-qset-v6-writer`
- processor: `id-polyedge-conduit-shadow-qset-v6-processor`, `spiffe://polyedge.local/conduit/shadow-qset-v6-processor`
- issuer: `https://oidc.jupiterlabs.dev`
- audience: `api://AzureADTokenExchange`

There is no Azure Container Apps qset-v6 job. The initial profitability template contains only seven Storage resources and nine exact role assignments (16 resources total). Processing runs later on OCI under the processor lane.

## Hard gates
- The immutable qset-v5 terminal ineligibility receipt remains root-owned mode 0640 at `/srv/polyedge-ring/migration/qset-v5/processor-handoff-ineligible-20260828T155929Z-74d816e962994b139423cb410b1529ff/ineligibility.json` with SHA-256 `d8b270dcc317d84792290d09b2fbb172e8e43f993b0c5c24464253316dc811da`; it requires fresh campaign evidence and does not authorize writer retirement.
- Qset-v4 and qset-v5 writers remain active and healthy and are never stopped or reconfigured by this bundle.

- Qset-v3 is active and healthy with its five writer and three processor assignments unchanged. Its old boundary and first-seal timers remain disabled.
- Qset-v2 is active and healthy; `polyedge-qset-v2-first-seal.timer` is already disabled/inactive. The v6 boundary guard never changes either workload or any timer.
- Both v6 UAMIs/FICs exist with the exact issuer, subject, and sole audience above, and both v6 principals have zero role assignments before initial apply.
- The final source-freeze manifest binds `research_image`, `source_commit`, and `git_tree`. Its hash-selected upload receipt is root-owned mode 0640 under `/srv/polyedge-ring/migration/qset-v6/source-freeze/source-HASH.json` and binds `manifest`, `researchImage`, `sourceCommit`, and `gitTree`.
- Writer, sealer, and later processor use the receipt's same immutable `researchImage`; every OCI revision label equals `sourceCommit`.
- The writer remains paper-only. Funded execution and Service Bus remain disabled.

## Provision the two independent identities

An authorized operator deploys each lane once. These deployments create only one UAMI and one FIC each; they create no RBAC or compute.

```sh
az deployment group what-if --resource-group rg-polyedge-dev \
  --template-file infra/conduit-federated-identity.bicep \
  --parameters lane=shadow-qset-v6-writer issuer=https://oidc.jupiterlabs.dev
az deployment group create --resource-group rg-polyedge-dev \
  --name conduit-shadow-qset-v6-writer-identity \
  --template-file infra/conduit-federated-identity.bicep \
  --parameters lane=shadow-qset-v6-writer issuer=https://oidc.jupiterlabs.dev

az deployment group what-if --resource-group rg-polyedge-dev \
  --template-file infra/conduit-federated-identity.bicep \
  --parameters lane=shadow-qset-v6-processor issuer=https://oidc.jupiterlabs.dev
az deployment group create --resource-group rg-polyedge-dev \
  --name conduit-shadow-qset-v6-processor-identity \
  --template-file infra/conduit-federated-identity.bicep \
  --parameters lane=shadow-qset-v6-processor issuer=https://oidc.jupiterlabs.dev
```

Provision two dedicated local service identities: writer UID/GID `974:974` and processor UID/GID `973:973`. Before creating them, prove both numeric IDs are still unallocated; after creation, prove each name resolves to its exact pair and that the pairs are distinct from every existing lane. Never reuse the v3 writer (`983:979`) or promotion (`985:981`) identity. Register each lane with the exact SPIRE agent path plus its dedicated username. Fetching explicitly by SPIFFE ID keeps the two SVIDs distinct.

```bash
! getent passwd 974
! getent group 974
! getent passwd 973
! getent group 973
sudo groupadd --system --gid 974 polyedge-qset-v6-writer
sudo useradd --system --uid 974 --gid 974 --home-dir /nonexistent --shell /usr/sbin/nologin polyedge-qset-v6-writer
sudo groupadd --system --gid 973 polyedge-qset-v6-processor
sudo useradd --system --uid 973 --gid 973 --home-dir /nonexistent --shell /usr/sbin/nologin polyedge-qset-v6-processor
test "$(id -u polyedge-qset-v6-writer):$(id -g polyedge-qset-v6-writer)" = 974:974
test "$(id -u polyedge-qset-v6-processor):$(id -g polyedge-qset-v6-processor)" = 973:973
```

```sh
sudo /opt/spire/bin/spire-server entry create -socketPath /run/spire-server/api.sock \
  -parentID spiffe://polyedge.local/conduit-dev \
  -spiffeID spiffe://polyedge.local/conduit/shadow-qset-v6-writer \
  -selector unix:path:/opt/spire/bin/spire-agent \
  -selector unix:user:polyedge-qset-v6-writer \
  -jwtSVIDTTL 300
sudo /opt/spire/bin/spire-server entry create -socketPath /run/spire-server/api.sock \
  -parentID spiffe://polyedge.local/conduit-dev \
  -spiffeID spiffe://polyedge.local/conduit/shadow-qset-v6-processor \
  -selector unix:path:/opt/spire/bin/spire-agent \
  -selector unix:user:polyedge-qset-v6-processor \
  -jwtSVIDTTL 300
```

## Install the two token lanes and writer bundle

The template unit creates separate directories and files:

- `/run/polyedge-federated-shadow-qset-v6-writer/azure-federated-token`
- `/run/polyedge-federated-shadow-qset-v6-processor/azure-federated-token`

The writer Quadlet and sealer bind only the writer directory. The OCI processor must later bind only the processor directory.

```sh
sudo install -m 0755 ops/conduit/bin/polyedge-federated-token-refresh /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-qset-v6-source-freeze /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-qset-v6-rbac-handoff /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-qset-v6-boundary-guard /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-qset-v6-seal-days /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-qset-v6-retire-writer /usr/local/libexec/
sudo install -D -m 0644 ops/conduit/systemd/polyedge-federated-token@shadow-qset-v6-writer.service.d/override.conf /etc/systemd/system/polyedge-federated-token@shadow-qset-v6-writer.service.d/override.conf
sudo install -D -m 0644 ops/conduit/systemd/polyedge-federated-token@shadow-qset-v6-processor.service.d/override.conf /etc/systemd/system/polyedge-federated-token@shadow-qset-v6-processor.service.d/override.conf
sudo install -m 0644 ops/conduit/quadlets/polyedge-shadow-qset-v6.container /etc/containers/systemd/
sudo install -m 0644 ops/conduit/systemd/polyedge-qset-v6-seal-days.service ops/conduit/systemd/polyedge-qset-v6-first-seal.timer /etc/systemd/system/
sudo install -m 0644 ops/conduit/systemd/polyedge-qset-v6-boundary@.service ops/conduit/systemd/polyedge-qset-v6-boundary-pre.timer ops/conduit/systemd/polyedge-qset-v6-boundary-post.timer /etc/systemd/system/
sudo install -m 0600 ops/conduit/env/shadow-qset-v6.env.example /etc/polyedge/shadow-qset-v6.env
sudo install -m 0640 ops/conduit/env/qset-v6-sealer.env.example /etc/polyedge/qset-v6-sealer.env
sudo chown root:root /etc/polyedge/shadow-qset-v6.env /etc/polyedge/qset-v6-sealer.env
sudo systemctl daemon-reload
sudo systemctl enable --now polyedge-federated-token@shadow-qset-v6-writer.timer polyedge-federated-token@shadow-qset-v6-processor.timer
```

Decode and verify each token locally without printing it. Require RS256, its exact v6 subject, the issuer above, the sole Azure Token Exchange audience, and a lifetime no longer than six minutes. Confirm the two files have different paths and inodes.

## Additive Storage/RBAC apply

The check is read-only. It requires qset-v5 active/healthy with its exact five writer and three processor assignments unchanged, both v6 principals at zero assignments, exact FICs, no Azure processor job, and a compiled template of exactly 16 resources/9 assignments/zero compute.

```sh
export AZURE_RESOURCE_GROUP=rg-polyedge-dev
export AZURE_STORAGE_ACCOUNT_NAME=stpolyedge6urdjr5nmwx7w
export AZURE_TENANT_ID=9767f0dc-e83f-4cc1-94e1-0d5f9d287d32
ops/conduit/bin/polyedge-qset-v6-rbac-handoff check
az deployment group what-if --resource-group "$AZURE_RESOURCE_GROUP" \
  --template-file infra/shadow-profitability-qset-v6.bicep
```

Accept only 16 creates: three containers, one immutability policy, three tables, and nine role assignments. Reject every modify/delete and every `Microsoft.App` or compute resource. Then run `apply`. It is additive and interruption-safe: a root-owned before receipt proves the initial zero-assignment state; rerunning reconciles only an expected partial v6 assignment set. It never starts the writer.

```sh
ops/conduit/bin/polyedge-qset-v6-rbac-handoff apply
```

Apply proves exactly five writer assignments, three processor assignments, and one API research reader. It rejects every unexpected full-principal assignment and performs negative data-plane probes against v1/v2/v3/v4/v5/funded Storage, Key Vault, and Service Bus with both v6 tokens.

Rollback requires the v6 writer and local processor units quiescent. It removes only the exact nine v6 assignments. It leaves both UAMIs/FICs, all containers/tables/evidence, and every qset-v5 and older role/service untouched.

```sh
ops/conduit/bin/polyedge-qset-v6-rbac-handoff rollback
```

## Final freeze and boundary

From a clean committed checkout, build with `FREEZE_RESEARCH_IMAGE` set to the reviewed multi-architecture digest. An explicitly authorized operator may then lock/upload. The upload receipt is root:root 0640 and includes the exact manifest URI/hash/ETag/bytes plus `researchImage`, `sourceCommit`, and `gitTree`.

```sh
FREEZE_RESEARCH_IMAGE=ghcr.io/OWNER/polyedge-rust-backend@sha256:DIGEST \
  ops/conduit/bin/polyedge-qset-v6-source-freeze build /secure/path/qset-v6-source-freeze.json
sudo -E ops/conduit/bin/polyedge-qset-v6-source-freeze lock-and-upload /secure/path/qset-v6-source-freeze.json
```

Populate both installed env files from that exact receipt. Writer and sealer image values must equal `researchImage`; all writer/sealer revision values must equal `sourceCommit`; `EXECUTION_FREEZE_SHA256` selects the local receipt filename and exact immutable Azure manifest.

Start qset-v6 without stopping qset-v2/v3/v4/v5. The v6 Quadlet has no `Conflicts=` edge to any predecessor.

```sh
sudo systemctl start polyedge-shadow-qset-v6.service
sudo podman healthcheck run polyedge-shadow-qset-v6
sudo systemctl enable --now polyedge-qset-v6-boundary-pre.timer polyedge-qset-v6-boundary-post.timer
```

The boundary guard is read-only except for its local root-owned receipts. It requires qset-v2/v3/v4/v5 active and healthy, the exact immutable qset-v5 terminal receipt hash, v2's unsafe first-seal timer disabled, all old v3 timers disabled, and the same v3/v4/v5 PID, invocation, and container across pre/post. It performs no `start`, `stop`, `disable`, or Azure evidence mutation.

## Closed-day seal

Keep `polyedge-qset-v6-first-seal.timer` disabled until both complete UTC days exist. At 2026-09-03 02:15 UTC it validates September 1 and 2 before fencing only the v6 writer, seals both days under the writer identity, writes deterministic root-owned receipts under `/srv/polyedge-ring/migration/qset-v6-seal/`, and restarts/health-checks only v6. Qset-v2, qset-v3, qset-v4, and qset-v5 are never stopped. Enable the one-shot timer only after the final freeze and boundary receipts pass.

```sh
sudo systemctl enable --now polyedge-qset-v6-first-seal.timer
sudo systemctl list-timers polyedge-qset-v6-first-seal.timer
```

## OCI-local processor binding

Do not deploy an Azure job. After both sealed-day receipts exist, the separately managed OCI local processor must use:

- client ID from `id-polyedge-conduit-shadow-qset-v6-processor`
- only `/run/polyedge-federated-shadow-qset-v6-processor/azure-federated-token`
- raw `polyedge-shadow-qset-v6-events` (reader)
- control `polyedge-qset-v6-control` (reader)
- research `polyedge-research-qset-v6` (custom no-delete writer)
- exact `researchImage`, `sourceCommit`, `gitTree`, manifest URI/path, and hash from the selected root:root 0640 freeze receipt

The local deployment guard must reject any image other than `researchImage`, any OCI revision other than `sourceCommit`, any different receipt hash/path, any Azure Container Apps qset-v6 job, and any funded/v1/v2/v3/v4/v5/KV/Service Bus access. Install the maintained local processor components without a timer or enablement:

```sh
sudo install -m 0755 ops/conduit/bin/polyedge-run-job ops/conduit/bin/polyedge-qset-v6-processor-preflight ops/conduit/bin/polyedge-qset-v6-processor-handoff /usr/local/libexec/
sudo install -D -m 0640 ops/conduit/env/qset-v6-processor.env.example /etc/polyedge/jobs/qset-v6-processor.env
sudo chown root:root /etc/polyedge/jobs/qset-v6-processor.env
sudo install -m 0644 ops/conduit/systemd/polyedge-qset-v6-processor.service /etc/systemd/system/
sudo systemctl daemon-reload
```

Leave the service disabled. Only after the exact source-freeze receipt v2 and both 2026-09-01/2026-09-02 sealed receipts exist, run the root-only no-argument handoff. It first runs the maintained read-only `polyedge-qset-v6-rbac-handoff verify-live` gate, which proves exact v6 5/3/1 assignments, denied v1/v2/v3/v4/v5/funded/Key Vault/Service Bus access, unchanged qset-v5, and no Azure qset-v6 processor job. The root caller uses the authenticated `ubuntu` Azure CLI context while retaining root-only token-file access. The handoff derives the four receipt/inventory hashes, preflights the candidate, fsyncs a blank-environment snapshot and `attempt.json`, installs the bound environment and gate, then fsyncs `dispatched.json` immediately before asking systemd to start; `ExecStartPre` removes the gate and fsyncs `/etc/polyedge` before job code executes, so it cannot admit a second invocation. A durable attempt without dispatch may resume; a dispatch without a recoverable invocation is fail-closed and never replayed. After observing the invocation it binds `started.json`; on success it repeats the disk guard and records terminal `completed.json`, which remains valid across reboot after the full marker chain is revalidated.

```sh
sudo /usr/local/libexec/polyedge-qset-v6-processor-handoff
```

Do not add recurrence. Require successful output hash/readback and the processor negative-access proof first.

## Retire the qset-v6 writer

Only after the campaign is complete and the local processor has independently proven successful output hash/readback and zero queued or unrecovered work, run the maintained host command:

```sh
sudo /usr/local/libexec/polyedge-qset-v6-retire-writer
```

It signals the current writer with `SIGUSR1`, accepts only the exact retirement receipt emitted after the recorder waterline is fully durable, and binds the captured journal message to the current systemd `InvocationID`, full container ID, pinned image digest, and immutable image revision. It atomically persists root:root mode 0640 evidence at `/srv/polyedge-ring/migration/qset-v6/retirement/campaign-2026-09-01-qset-v6-writer.json`, fsyncs the file and directory, rechecks the same live identity, and only then stops the service. If interrupted after evidence persistence, rerun the same command; it validates the existing evidence and stops the same writer without sending a second prepare signal. Any invocation, container, image, receipt, ownership, or durability mismatch fails closed and leaves the writer running.
