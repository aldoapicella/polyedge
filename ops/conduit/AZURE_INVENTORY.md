# PolyEdge Azure compute inventory

Live read-only snapshot: 2026-08-06 00:10 UTC. Resource group:
`rg-polyedge-dev`. Secret names are listed; secret values are not.

The retired `polyedge-adx-ingestion-job` was deleted before this snapshot. The
remaining deletion target is therefore four apps and nineteen jobs, not twenty.

## Environments and network dependencies

| Environment | Region | Apps | Jobs | Attached network |
| --- | --- | ---: | ---: | --- |
| `polyedge-dev-env` | East US | 1 | 8 | app environment network |
| `polyedge-venue-neu-env` | North Europe | 2 | 8 | managed load balancer, public IP, `nat-polyedge-venue-neu`, egress IP, VNet/subnet |
| `polyedge-execution-cl-env` | Chile Central | 1 | 3 | managed load balancer, public IP, `nat-polyedge-execution-cl`, egress IP, VNet/subnet |

Every environment is occupied. Both NAT gateways have one attached subnet and
one egress public IP. Both managed load balancers have active Container Apps
frontends/backends. None is an unused standalone deletion candidate.

## Apps

| App | Environment | Container image | CPU / memory | Identity | Secret references |
| --- | --- | --- | --- | --- | --- |
| `polyedge-dev` | East US | `polyedge-rust-backend@sha256:e31dbc1b47eed665a6c5bb5b03418e6b0a7bae6632cd82da5079a078d975d670`; `polyedge-frontend@sha256:32bed43c6b23bbe02b46e9949f78365067254d7f82598b6112c2c325c9f96d8e` | 0.5 / 1 GiB each | `polyedge-dev-id` | `api-bearer-token`, `dashboard-auth-password`, `dashboard-session-secret` |
| `polyedge-shadow-neu` | North Europe | `polyedge-rust-backend@sha256:2507ebbbd0efd408a25a7c6cfeb7bec57e57973777009a41a3428cb61af65969` | 0.5 / 1 GiB | `polyedge-shadow-neu-id` | none |
| `polyedge-shadow-qset-neu` | North Europe | `polyedge-rust-backend@sha256:e31dbc1b47eed665a6c5bb5b03418e6b0a7bae6632cd82da5079a078d975d670` | 0.5 / 1 GiB | `polyedge-shadow-qset-neu-id` | none |
| `polyedge-funded-direct-cl` | Chile Central | `polyedge-venue-probe@sha256:a48a4f96042a6c31a9ed1dc01d4d25e92f5f8c4053d1583f6c66b3657d71979f` | 0.5 / 1 GiB | `polyedge-execution-cl-id` | four Polymarket API/wallet secrets plus relayer key |

Only `polyedge-dev` has external ingress. The live Azure templates have no
container health probes; health is currently inferred through Log Analytics
and metric alerts.

## Jobs

| Job | Region | Trigger | CPU / memory | Image | Identity |
| --- | --- | --- | --- | --- | --- |
| `polyedge-venue-probe-neu-job` | North Europe | manual | 0.5 / 1 GiB | `polyedge-venue-probe:9c1131d9639eeed83234ff3a9fccbedffafcfa0c` | `polyedge-venue-neu-id` |
| `polyedge-redeem-neu-job` | North Europe | manual | 0.5 / 1 GiB | same venue image | `polyedge-venue-neu-id` |
| `polyedge-strategy-canary-neu-job` | North Europe | manual | 0.5 / 1 GiB | same venue image | `polyedge-venue-neu-id` |
| `polyedge-funded-ladder-neu-job` | North Europe | manual | 0.5 / 1 GiB | same venue image | `polyedge-venue-neu-id` |
| `polyedge-shadow-daily-neu-job` | North Europe | `15 2 * * *` | 4 / 8 GiB | `polyedge-rust-backend:9c1131d9639eeed83234ff3a9fccbedffafcfa0c` | `polyedge-shadow-research-neu-id` |
| `polyedge-promotion-neu-job` | North Europe | manual | 0.5 / 1 GiB | same Rust image | `polyedge-promotion-transition-neu-id` |
| `polyedge-shadow-val-neu-job` | North Europe | manual | 4 / 8 GiB | `polyedge-rust-research-validation@sha256:bc15f0be0f790f45469268db82e17f4d18f3e69434437714e357ab2668452325` | `polyedge-shadow-validation-neu-id` |
| `polyedge-shadow-qset-neu-job` | North Europe | manual | 4 / 8 GiB | `polyedge-rust-backend@sha256:e31dbc1b47eed665a6c5bb5b03418e6b0a7bae6632cd82da5079a078d975d670` | `polyedge-qset-research-neu-id` |
| `polyedge-daily-research-job` | East US | `30 0 * * *` | 2 / 4 GiB | `polyedge-rust-research@sha256:c77f000689d7733b692be714b8b18ba019a5fdf98943c5aff724fa14a34f4b96` | `polyedge-dev-id` |
| `polyedge-prospective-job` | East US | manual | 0.5 / 1 GiB | `polyedge-rust-backend:d07fc1dc5ed31a5a1558909411a614f9e5f098f1` | `polyedge-dev-id` |
| `polyedge-hourly-quality-job` | East US | `10 * * * *` | 0.5 / 1 GiB | same research image | `polyedge-dev-id` |
| `polyedge-data-freshness-job` | East US | `*/5 * * * *` | 0.5 / 1 GiB | same research image | `polyedge-dev-id` |
| `polyedge-replay-index-job` | East US | `0 3 * * *` | 2 / 4 GiB | same research image | `polyedge-dev-id` |
| `polyedge-backfill-job` | East US | manual | 0.5 / 1 GiB | same backend image as prospective | `polyedge-dev-id` |
| `polyedge-chart-backfill-job` | East US | manual | 0.5 / 1 GiB | same backend image as prospective | `polyedge-dev-id` |
| `polyedge-venue-model-job` | East US | manual | 0.5 / 1 GiB | `polyedge-venue-probe:d07fc1dc5ed31a5a1558909411a614f9e5f098f1` | `polyedge-dev-venue-model-id` |
| `polyedge-origin-check-cl-job` | Chile Central | manual | 0.25 / 0.5 GiB | `polyedge-venue-probe@sha256:a48a4f96042a6c31a9ed1dc01d4d25e92f5f8c4053d1583f6c66b3657d71979f` | `polyedge-execution-cl-id` |
| `polyedge-funded-direct-cl-job` | Chile Central | manual | 0.5 / 1 GiB | same digest-pinned venue image | `polyedge-execution-cl-id` |
| `polyedge-funded-warmup-cl` | Chile Central | manual | 0.25 / 0.5 GiB | same digest-pinned venue image | `polyedge-shadow-neu-id` |

Research jobs reference `api-bearer-token`. Venue/funded jobs reference the
four Polymarket API/wallet secrets; the persistent funded app additionally
references the relayer key. All nineteen jobs use user-assigned identities.

Azure Arc machine `conduit-dev` is connected in East US with system identity
`19d0cc08-c6be-4b5b-85a8-05211f19428a`. Arc extensions, guest configuration,
and incoming connections are disabled locally. Its custom no-delete blob role
is scoped only to `bot-events`; the funded evidence container and Service Bus
queue are excluded. A host-side token challenge succeeded on 2026-08-06. The
Rust uploader then created an immutable Cool-tier segment and manifest, re-read
the manifest, and wrote its verified local receipt from both the host binary and
the rootful ARM64 container. The published digest must repeat that proof before
the 72-hour clock starts.

## OCI schedule and coverage

| OCI unit | UTC schedule | Azure counterpart |
| --- | --- | --- |
| `polyedge-freshness.timer` | every five minutes | `polyedge-data-freshness-job` |
| `polyedge-hourly.timer` | hourly at `:10` | `polyedge-hourly-quality-job` |
| `polyedge-daily.timer` | 00:30 | `polyedge-daily-research-job` |
| `polyedge-replay.timer` | 03:00 | `polyedge-replay-index-job` |
| `polyedge-shadow-qset.timer` | 02:15, disabled pending approval | `polyedge-shadow-qset-neu-job` |
| `polyedge-ring-sync.timer` | every five minutes at `:02` | new local seal/upload plane |
| `polyedge-ring-health.timer` | every five minutes at `:04` | capacity/backlog/upload guard |
| `systemctl start polyedge-job@prospective` | manual | `polyedge-prospective-job` |
| `systemctl start polyedge-job@chart-backfill` | manual | `polyedge-chart-backfill-job` |
| `systemctl start polyedge-job@backfill` | manual | `polyedge-backfill-job` |

The single `/run/polyedge/research.lock` serializes every research writer.
Recurring daily/replay work is capped at 2 CPU / 4 GiB and qset at 3 CPU /
8 GiB, leaving capacity for the 0.5-CPU API and frontend. Manual Azure jobs not
listed in this table still require an explicit local command mapping before
their Azure definitions can be deleted.

## Capacity and cost evidence

For 2026-07-25 through 2026-08-05, Cost Management reported $168.79 of
PolyEdge spend. A straight 30-day projection is $421.96: Container Apps
$125.45, NAT gateways $113.27, Storage $95.78, managed load balancers $28.41,
Azure Monitor $18.68, Log Analytics $15.19, public IP/VNet meters $12.26, ACR
$9.72, Service Bus $2.71, and $0.50 of bandwidth, Key Vault, and transient
Container Instances. Deleting the accepted compute plane, environments, NATs,
managed load balancers, and public IP/VNet resources therefore removes a
current projected $279.39/month before the later ACR, compute-monitoring, and
storage optimizations.

The four Azure apps averaged 0.33 CPU cores and 2.40 GB of working set in the
24 hours ending 2026-08-06T01:00Z. The sum of their individual observed peaks
was 1.02 cores and 2.87 GB. `conduit-dev` has four ARM64 cores, 25.14 GB RAM,
and 154.7 GB currently available on the dedicated ring volume. A closed
ten-minute local segment contained 340,675 events (about 568 events/second)
while the local collector reported about 0.16 core and 131 MB. The 48-hour
ring projection was 76.4 GB. This proves steady collector capacity with broad
headroom; the two daily ARM64 cycles remain the required batch-efficiency gate.
The free single VM does not provide Azure's failure-domain redundancy.

## Azure data and evidence surfaces retained

Storage account: `stpolyedge6urdjr5nmwx7w`, Standard LRS, StorageV2.

Blob containers:

- `bot-events`: canonical raw events, indexes, normalized control, and the OCI ring prefix.
- `polyedge-shadow-events`, `polyedge-shadow-qset-events`: isolated shadow raw evidence.
- `polyedge-research`, `polyedge-research-qset`, `polyedge-research-validation`: separated research and validation output domains.
- `polyedge-funded-evidence`: funded manifests, reservations, execution, settlement, and recovery evidence.
- `polyedge-models`: immutable model artifacts.
- `polyedge-qset-control`: frozen qset campaign control.

Tables:

`BotChartSeries`, `BotEventIndex`, `BotMarketCatalog`, `PolyEdgeDataFreshness`,
`PolyEdgeExclusionWindows`, `PolyEdgeJobStatus`, `PolyEdgeProspectiveResults`,
`PolyEdgeResearchArtifacts`, `PolyEdgeResearchRuns`, `ShadowBotChartSeries`,
`ShadowBotEventIndex`, `ShadowBotMarketCatalog`, `ShadowQsetChartSeries`,
`ShadowQsetEventIndex`, and `ShadowQsetMarketCatalog`.

Blob and container soft delete remain enabled for 14 days. Change Feed is
disabled. The active lifecycle rule deletes only block blobs under
`bot-events/data/research/normalized/v1/` seven days after modification. It
does not tier or move the existing raw corpus.

Key Vault `kvpolyedge6urdjr5nmwx7w` retains six named secrets: dashboard auth,
four Polymarket API/wallet values, and the relayer API key.

## Queue and monitoring

Service Bus namespace `sb-polyedge-funded-cl-6urdjr5nmwx7w` is Standard and
active. Queue `funded-dynamic-quote-intents` had zero active, 633 dead-letter,
and zero scheduled messages at snapshot time. The growing dead-letter count is
an unresolved funded cutover gate; do not delete or purge it as unused.

Storage metric alerts cover zero ingress/transactions. The compute workspace
contains Container Apps console/system logs and scheduled-query alerts for
recorder failure/drop/backlog, runtime restart/health, job failure/duration,
tiny/missing blobs, and funded safety. Delete only compute-specific queries and
the workspace after the OCI acceptance window; retain storage metric alerts.

## Deletion gates

Delete no app, job, environment, load balancer, public IP, NAT, or VNet until:

1. dedicated OCI identities and container/table/queue role assignments exist;
2. the ring upload and remote hash verification smoke test passes;
3. all recurring and manual workload mappings are executable on OCI;
4. 72 healthy Azure-authoritative hours and two daily cycles pass;
5. count/hash/output/funded-evidence parity, reboot recovery, and rollback pass;
6. the frozen qset guard is explicitly resolved.
