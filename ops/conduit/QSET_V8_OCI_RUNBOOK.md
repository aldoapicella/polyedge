# qset-v8 OCI operations runbook

Qset-v8 is the only active qualification runtime. Qset-v1 through qset-v7 are
historical, promotion-ineligible, inactive, and masked. Their Azure containers,
tables, local receipts, source freezes, and processor markers remain immutable;
do not replay, purge, overwrite, or mix them into qset-v8.

Qset-v8 is paper-only and uses isolated namespaces:

- writer: UID/GID `970:970`, `spiffe://polyedge.local/conduit/shadow-qset-v8-writer`
- processor: UID/GID `969:969`, `spiffe://polyedge.local/conduit/shadow-qset-v8-processor`
- raw/control/research: `polyedge-shadow-qset-v8-events`, `polyedge-qset-v8-control`, `polyedge-research-qset-v8`
- tables: `ShadowQsetV8EventIndex`, `ShadowQsetV8ChartSeries`, `ShadowQsetV8MarketCatalog`

## Admission

First prove all historical services are inactive and masked, their containers
are absent, the qset-v4/v5/v7 retirement receipts validate, the qset-v5/v6
ineligibility receipts retain their exact hashes, and qset-v7 has the immutable
attempt/dispatched/started marker chain with no `completed.json`. The maintained
boundary guard performs the same checks before accepting qset-v8.

Create the two isolated Azure identities only after a clean what-if:

```sh
az deployment group what-if --resource-group rg-polyedge-dev --template-file infra/conduit-federated-identity.bicep --parameters lane=shadow-qset-v8-writer issuer=https://oidc.jupiterlabs.dev
az deployment group create --resource-group rg-polyedge-dev --name conduit-shadow-qset-v8-writer-identity --template-file infra/conduit-federated-identity.bicep --parameters lane=shadow-qset-v8-writer issuer=https://oidc.jupiterlabs.dev
az deployment group what-if --resource-group rg-polyedge-dev --template-file infra/conduit-federated-identity.bicep --parameters lane=shadow-qset-v8-processor issuer=https://oidc.jupiterlabs.dev
az deployment group create --resource-group rg-polyedge-dev --name conduit-shadow-qset-v8-processor-identity --template-file infra/conduit-federated-identity.bicep --parameters lane=shadow-qset-v8-processor issuer=https://oidc.jupiterlabs.dev
```

Create the dedicated local identities only if both numeric IDs are free:

```sh
! getent passwd 970; ! getent group 970; ! getent passwd 969; ! getent group 969
sudo groupadd --system --gid 970 polyedge-qset-v8-writer
sudo useradd --system --uid 970 --gid 970 --home-dir /nonexistent --shell /usr/sbin/nologin polyedge-qset-v8-writer
sudo groupadd --system --gid 969 polyedge-qset-v8-processor
sudo useradd --system --uid 969 --gid 969 --home-dir /nonexistent --shell /usr/sbin/nologin polyedge-qset-v8-processor
test "$(id -u polyedge-qset-v8-writer):$(id -g polyedge-qset-v8-writer)" = 970:970
test "$(id -u polyedge-qset-v8-processor):$(id -g polyedge-qset-v8-processor)" = 969:969
```

Register exact SPIRE entries:

```sh
sudo /opt/spire/bin/spire-server entry create -socketPath /run/spire-server/api.sock -parentID spiffe://polyedge.local/conduit-dev -spiffeID spiffe://polyedge.local/conduit/shadow-qset-v8-writer -selector unix:path:/opt/spire/bin/spire-agent -selector unix:user:polyedge-qset-v8-writer -jwtSVIDTTL 300
sudo /opt/spire/bin/spire-server entry create -socketPath /run/spire-server/api.sock -parentID spiffe://polyedge.local/conduit-dev -spiffeID spiffe://polyedge.local/conduit/shadow-qset-v8-processor -selector unix:path:/opt/spire/bin/spire-agent -selector unix:user:polyedge-qset-v8-processor -jwtSVIDTTL 300
```

Install the maintained host surface, but leave the writer and processor stopped:

```sh
sudo install -m 0755 ops/conduit/bin/polyedge-federated-token-refresh ops/conduit/bin/polyedge-qset-v8-source-freeze ops/conduit/bin/polyedge-qset-v8-rbac-handoff ops/conduit/bin/polyedge-qset-v8-boundary-guard ops/conduit/bin/polyedge-qset-v8-seal-days ops/conduit/bin/polyedge-qset-v8-retire-writer ops/conduit/bin/polyedge-qset-v8-processor-preflight ops/conduit/bin/polyedge-qset-v8-processor-handoff ops/conduit/bin/polyedge-run-job /usr/local/libexec/
sudo install -D -m 0644 ops/conduit/systemd/polyedge-federated-token@shadow-qset-v8-writer.service.d/override.conf /etc/systemd/system/polyedge-federated-token@shadow-qset-v8-writer.service.d/override.conf
sudo install -D -m 0644 ops/conduit/systemd/polyedge-federated-token@shadow-qset-v8-processor.service.d/override.conf /etc/systemd/system/polyedge-federated-token@shadow-qset-v8-processor.service.d/override.conf
sudo install -m 0644 ops/conduit/quadlets/polyedge-shadow-qset-v8.container /etc/containers/systemd/
sudo install -m 0644 ops/conduit/systemd/polyedge-qset-v8-*.service ops/conduit/systemd/polyedge-qset-v8-*.timer /etc/systemd/system/
sudo install -m 0600 ops/conduit/env/shadow-qset-v8.env.example /etc/polyedge/shadow-qset-v8.env
sudo install -m 0640 ops/conduit/env/qset-v8-sealer.env.example /etc/polyedge/qset-v8-sealer.env
sudo install -D -m 0640 ops/conduit/env/qset-v8-processor.env.example /etc/polyedge/jobs/qset-v8-processor.env
sudo chown root:root /etc/polyedge/shadow-qset-v8.env /etc/polyedge/qset-v8-sealer.env /etc/polyedge/jobs/qset-v8-processor.env
sudo systemctl daemon-reload
sudo systemctl enable --now polyedge-federated-token@shadow-qset-v8-writer.timer polyedge-federated-token@shadow-qset-v8-processor.timer
```

Populate only reviewed non-secret values, exact Azure client IDs, and the final
immutable image/source bindings. Never bind funded Storage, Service Bus, Key
Vault, client secrets, or another qset's token directory.

## Isolated storage, RBAC, and freeze

```sh
export AZURE_RESOURCE_GROUP=rg-polyedge-dev AZURE_STORAGE_ACCOUNT_NAME=stpolyedge6urdjr5nmwx7w
ops/conduit/bin/polyedge-qset-v8-rbac-handoff check
az deployment group what-if --resource-group "$AZURE_RESOURCE_GROUP" --template-file infra/shadow-profitability-qset-v8.bicep
ops/conduit/bin/polyedge-qset-v8-rbac-handoff apply
```

Accept only 16 creates: three containers, one immutability policy, three v8
tables, and nine assignments. Reject modifications, deletions, compute, or
`Microsoft.App`. Apply must prove exact 5/3/1 assignments, positive qset-v8
read/write probes, and denial against qset-v1 through v7, funded Storage, Key
Vault, and Service Bus.

Build, upload, lock, and bind the final source manifest only after the reviewed
commit's ARM64 image is available:

```sh
FREEZE_RESEARCH_IMAGE=ghcr.io/OWNER/polyedge-rust-backend@sha256:DIGEST ops/conduit/bin/polyedge-qset-v8-source-freeze build /secure/path/qset-v8-source-freeze.json
sudo -E ops/conduit/bin/polyedge-qset-v8-source-freeze lock-and-upload /secure/path/qset-v8-source-freeze.json
```

Validate both token files' RS256 issuer/subject/audience/expiry locally without
printing either JWT, then start the writer and run the guard:

```sh
sudo systemctl start polyedge-shadow-qset-v8.service
sudo podman healthcheck run polyedge-shadow-qset-v8
sudo /usr/local/libexec/polyedge-qset-v8-boundary-guard check
sudo systemctl enable --now polyedge-qset-v8-boundary-pre.timer polyedge-qset-v8-boundary-post.timer
```

The writer starts on the preflight prefix and switches in-process at
`2026-09-09T00:00:00Z`. Require a nonzero persisted recorder waterline, zero
queue/failure/unrecovered counters, local ring segments, and Azure blob readback.

## Seal and process

Leave `polyedge-qset-v8-first-seal.timer` disabled until the pre/post boundary
receipts exist. Its one-shot time is `2026-09-11T02:15:00Z`; it validates and
seals the complete UTC days 2026-09-09 and 2026-09-10, briefly fencing only the
qset-v8 writer and restoring the same frozen image healthy.

Leave `polyedge-qset-v8-processor.service` disabled. After both deterministic
seal receipts exist, populate their exact hashes and run the root-only one-shot
`polyedge-qset-v8-processor-handoff`. Do not add recurrence or replay a durable
dispatch without a recoverable invocation.

After campaign completion and verified processor output/readback, retire the
writer with `sudo /usr/local/libexec/polyedge-qset-v8-retire-writer`; it stops the
writer only after an exact durable recorder waterline receipt is atomically saved.
