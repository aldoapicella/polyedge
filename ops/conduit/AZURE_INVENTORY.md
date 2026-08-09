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

The Chile app is not idle. At `2026-08-06T02:27Z` it reported the funded
worker and automatic redemption enabled, Chile static egress verified, zero
open orders and unresolved reservations at that snapshot, and recent order
submission attempts. Do not stop or delete it until a Chile-egress replacement
passes the funded safety gates.

Azure Arc machine `conduit-dev` is connected in East US with system identity
`19d0cc08-c6be-4b5b-85a8-05211f19428a`. Arc extensions, guest configuration,
and incoming connections are disabled locally. Its custom no-delete blob role
is scoped only to `bot-events`; the funded evidence container and Service Bus
queue are excluded. A host-side token challenge succeeded on 2026-08-06. The
Rust uploader then created immutable Hot-tier segments and manifests, re-read
each manifest, and wrote verified local receipts from the rootful ARM64
container. Digest `d3a6ed43c8b7cb02c5cbe5e6d00bbbca60544ab034e7120cbf30dca67848ce1b`
accepted all prior receipts and uploaded the clean 05:50-06:00 UTC interval.
Its local source and manifest hashes matched their receipts; an authenticated
remote HEAD returned `200`, `BlockBlob`, `Hot`, and the exact 188,157,190-byte
manifest length.

The four dedicated UAMIs and exact FICs now exist without any Entra directory
role. Each isolated lane what-if using `https://oidc.jupiterlabs.dev` produced
exactly two creates and no updates, deletes, or role assignments; deployments
ran sequentially. SPIRE 1.15.2 Server, Agent, OIDC Provider, and Caddy are
installed, active, and boot-enabled on `conduit-dev`; the provider and Workload
API remain on protected Unix sockets. Every lane JWT-SVID is RS256, has a
300-second lifetime, and exactly matches its issuer, subject, and sole
`api://AzureADTokenExchange` audience. Token files are owner-only and isolated
per lane. All four two-minute refresh timers are enabled; enabling token
rotation does not enable the shadow-qset or funded workloads, which remain
disabled behind their independent gates.

`jupiterlabs.dev` is user-owned and delegated through GoDaddy; no DNS API
credential is present on the host. `oidc.jupiterlabs.dev` resolves to reserved
OCI public IP `149.130.186.60`, assigned to secondary private IP `10.0.0.81` on
the existing VNIC. The primary ephemeral IP remains assigned and was not
disrupted. OCI and UFW TCP/80 and TCP/443 ingress are live; Caddy obtained a
valid public certificate after multi-network ACME validation. HTTPS discovery
and JWKS return the exact issuer and public RS256 keys, while all other Caddy
paths return 404. The four UAMIs have only the exact container, table, and queue
roles in `identity-rbac-plan.json`; live positive and cross-lane negative proof
is in `identity-rbac-proof.json`. This clears the identity gate but not the
two-daily-cycle, qset, funded, reboot, rollback, or parity gates.

## OCI schedule and coverage

| OCI unit | UTC schedule | Azure counterpart |
| --- | --- | --- |
| `polyedge-freshness.timer` | every five minutes at `:03/:08/...` | `polyedge-data-freshness-job` at `:00/:05/...` |
| `polyedge-hourly.timer` | hourly at `:12` | `polyedge-hourly-quality-job` at `:10` |
| `polyedge-daily.timer` | 02:20 | `polyedge-daily-research-job` at 00:30 |
| `polyedge-replay.timer` | 03:05 | `polyedge-replay-index-job` at 03:00 |
| `polyedge-shadow-qset.timer` | 02:15, disabled pending approval | `polyedge-shadow-qset-neu-job` |
| `polyedge-ring-sync.timer` | every five minutes at `:02` | new local seal/upload plane |
| `polyedge-ring-health.timer` | every five minutes at `:04` | capacity/backlog/upload guard |
| `systemctl start polyedge-job@prospective` | manual | `polyedge-prospective-job` |
| `systemctl start polyedge-job@chart-backfill` | manual | `polyedge-chart-backfill-job` |
| `systemctl start polyedge-job@backfill` | manual | `polyedge-backfill-job` |
| `systemctl start polyedge-job@origin-check` | manual, credential-free | `polyedge-origin-check-cl-job` |

The single `/run/polyedge/research.lock` serializes every research writer.
The offsets keep Azure first during parity: freshness runs one minute after the
local ring sync and three minutes after Azure. The latest observed Azure
freshness, hourly, and replay runs finished in 32, 47-52, and 47-58 seconds;
the last five daily runs finished between 01:36 and 02:15 UTC. OCI daily therefore
starts at 02:20, and replay waits on the same host lock if that run is active.
Daily, replay, and qset are each capped at 1.5 CPU. API/frontend, funded signer,
ring upload, and the optional origin check consume at most the remaining 2.5
CPU, so container quotas cannot exceed the 4-OCPU host. `origin-check` uses
only the immutable venue image, no credentials or mounts, and exits non-zero
unless Polymarket reports exactly `country=CO` and the configured exact OCI
IPv4. Set that address from a trusted out-of-band host record in
`/etc/polyedge/jobs/origin-check.env`; do not obtain it inside the job.

### Remaining Azure job disposition

The live inventory has 19 jobs (not the earlier 20-job planning estimate). The
following definitions are **not** evidence that their capability has migrated:

| Azure job(s) | Disposition before Azure deletion | Evidence |
| --- | --- | --- |
| `polyedge-venue-probe-neu-job`, `polyedge-redeem-neu-job` | Remain until the dedicated funded identity, wallet/API secrets, egress, and evidence-trust review exist locally. | Both jobs are disabled/dry-run/trust-false in the deployed templates. |
| `polyedge-strategy-canary-neu-job`, `polyedge-funded-ladder-neu-job`, `polyedge-promotion-neu-job` | Remain gated; they need an explicit human grant/manifest or promotion bindings, not a generic local command. | All controller/promotion enable values and required bindings are false or blank. |
| `polyedge-shadow-daily-neu-job`, `polyedge-shadow-val-neu-job` | Remain until their independent shadow/validation identities and existing correction are resolved. | The legacy shadow correction is `in_progress`; the validation job has its own identity and output container. |
| `polyedge-shadow-qset-neu-job` | Existing `shadow-qset` mapping remains disabled pending the frozen campaign's controlled seal/approval. | Qset evidence and credentials are intentionally isolated. |
| `polyedge-venue-model-job` | Remain until its model identity and explicit checkpoint/hash approval are supplied. | `QUEUE_MODEL_TRAINING_ENABLED=false`; checkpoint and hash are blank. |
| `polyedge-funded-warmup-cl` | Remain until the producer Service Bus identity and Chile-egress replacement are proved. | It is a no-sign rehearsal but last succeeded on 2026-08-05. |
| `polyedge-funded-direct-cl-job` | Explicitly retired as replaced by the continuous funded service; delete only with the Chile service after the funded cutover gates. | Template tag `retiredReason=replaced-by-continuous-service`. |

The 2026-08-06 execution-history check found zero executions for backfill,
chart-backfill, promotion, strategy-canary, and the frozen qset manual job.
The scheduled North Europe shadow daily failed at 02:15 because the prior
`shadow-2026-07-23-through-2026-07-23` correction remains `in_progress`; this
is an evidence blocker, not proof that the workload is unused.

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

The approved ring block volume was expanded online from 150 GB to 200 GB at
zero VPUs/GB. The ext4 filesystem has about 126 GiB available after live ring
growth, and the production `polyedge-ring-health.service` capacity/upload check
passes. The
cheapest full-processing migration needs no paid relay. On 2026-08-06 the
Polymarket geoblock endpoint observed the default OCI egress as `CO`, ephemeral
IP `157.137.237.247`, with `blocked=false`. Binding the host request to secondary
private IP `10.0.0.81` instead produced reserved public IP `149.130.186.60`, also
`CO` and `blocked=false`, without changing SSH. The signer must use that exact
reserved route and `oci_bogota_static_egress`, then repeat origin and funded
dry-run parity before moving credentials; it remains disabled today.
The previously budgeted $9-12/month Chile relay is now only a fallback if the
OCI origin ceases to be eligible or the existing NAT IP cannot be retained.

Direct OCI egress leaves the immediate post-compute Azure projection at
$142.57/month. Before the approved OCI storage excess, it restores the later
storage-optimized Azure target to about $38-49/month. Do not move funded
credentials without explicit approval.

Maintaining the full 48-hour local ring under the observed worst segment needed
the completed 50-GB data-volume expansion. The boot volume was separately
expanded online from 47 GB to 97 GB to enforce the 15-GiB deployment reserve.
The resulting 297 GB of combined boot and block storage is 97 GB above Oracle's
documented 200-GB Always Free allowance. At the May 2026 public storage rate of
$0.0255 per GB-month, the paid excess is at least about $2.47/month. OCI requires
boot volumes to stay at the 10-VPU/GB Balanced tier; if the paid allocation falls
entirely on the boot volume, its performance units make about $4.12/month a
conservative upper bound. This changes the defensible post-optimization range
to roughly $41-54/month, a projected $368-381/month (87-90%) reduction from the
current $421.96 projection. Reducing retention or boot headroom to save those
few dollars would weaken explicit acceptance gates and is not recommended.

OCI metadata reports 4 A1 OCPUs and 24 GB. The user reports this existing VM is
free, so the cost model treats its compute and included public IP as $0. Oracle's
current public Free Tier pages are inconsistent about whether the A1 allowance
is 2 OCPUs/12 GB or 4 OCPUs/24 GB. The instance identity is not authorized to
read the tenancy usage API, so the first OCI invoice remains a mandatory cost
acceptance check; do not present the saving as durable before that readback.

The storage bill has both a fixed retained-data floor and avoidable transaction
pressure. The 2026-07-25 through 2026-08-05 meter query returned $39.54: $14.93
Hot LRS write operations, $8.56 list/create-container operations, $12.84 Hot
LRS stored data, $1.73 reads, and $1.48 other storage meters. Writes plus
list/create are 59% of the storage cost. The retained 2,047,214,559,460 bytes
alone project the stored-data meter near $32/month, so a $15 total is not
compatible with the requirement to leave that corpus untouched.

The no-list ring uploader, removal of repeated container creation calls, and
run-scoped funded risk snapshot target the dominant transaction cost. A real
302,050,848-byte closed segment compressed deterministically to 36,970,396
bytes in 1.97 seconds, an 87.8% reduction. At the observed 44 GB/day raw rate,
future archive growth falls to about 5.4 GB/day without changing the local job
input. At the 2026-08-06 East US public retail rates of $0.0208/GB-month Hot and
$0.0152/GB-month Cool, the rolling 30-day raw window falls from about $21.79 to
$2.67, roughly $19/month avoided before agreement discounts and transaction
charges. Each older retained month then costs about $0.16 instead of $1.31 at
the $0.00099/GB-month Archive rate. Only the future
`events-oci-hot7-v1/` prefix moves from Hot to Cool after seven days and Archive
after 30; the existing corpus and evidence/control prefixes do not move.
Archive reads require rehydration. If transaction meters fall by 85-95%, gzip
stays representative, and compute monitoring is removed, the upper edge of the
$15-35 target becomes plausible; keep $38-49 as the defensible range until the
first post-cutover bill proves otherwise. Prices are estimates from the
[Azure Blob Storage pricing page](https://azure.microsoft.com/pricing/details/storage/blobs/),
not a bill quote.

The four Azure apps averaged 0.33 CPU cores and 2.40 GB of working set in the
24 hours ending 2026-08-06T01:00Z. The sum of their individual observed peaks
was 1.02 cores and 2.87 GB. `conduit-dev` has four ARM64 cores, 25.14 GB RAM,
and broad free space on the dedicated ring volume. A closed
ten-minute local segment contained 340,675 events (about 568 events/second)
while the local collector reported about 0.16 core and 131 MB. The 48-hour
ring projection was 76.4 GB. This proves steady collector capacity with broad
headroom; the two daily ARM64 cycles remain the required batch-efficiency gate.
The OCI boot volume is 97 GB with a 15-GiB minimum-free cutover gate. A
five-minute guard warns at 75%, performs only safe cache/dangling-image cleanup
at 80%, and blocks image pulls at 85% or below the minimum-free floor. Container
logs are routed through the size-capped system journal.
The later live ring check found 125 segments with a combined raw-plus-gzip
average of 287,727,134 bytes and a peak of 541,986,617 bytes. A 48-hour
projection is 82.9 GB at the observed average but 156.1 GB at the peak, above
the old volume's roughly 123-GB budget after its 32-GiB safety reserve. The
completed expansion raised the data volume to 200 GB; the same conservative
projection now fits, `capacity_ok`, `free_ok`, and `upload_fresh` are green,
and no sealed segment is waiting for upload. The free single VM still does not
provide Azure's failure-domain redundancy.

## Acceptance candidate

The candidate window started at the clean recorder boundary
`2026-08-06T02:20:00Z`. The rootful API is running
`sha256:08e0c7e4b563208d12fd507ea254ab6afdadba667b173bb428d046a532ce14ed`
and the frontend is running
`sha256:eeb2d570a8dfc9842e2725e823653ba3eb77f3060821e672adc0ca92a1b1d9f7`.
Both native health checks were healthy; the API reported eight books, one
tradeable market, healthy discovery, and 29,841 persisted recorder events with
zero failures or unrecovered durable events. The first rootful transition
segment was hash-sealed and remotely verified at `2026-08-06T02:21:36Z`.
The first full ten-minute rootful segment was 217,483,672 bytes, hash-sealed,
uploaded, and receipted by `2026-08-06T02:32:26Z`; the API then reported
487,932 persisted events with zero recorder failures.

The older rootless image could not drain within 30 seconds. Its interrupted
pre-acceptance segment was preserved, not deleted, under
`/srv/polyedge-ring/quarantine`; it is excluded from the upload prefix. Azure
remained authoritative through the handoff. Do not count time before the clean
02:20 segment, and do not delete Azure compute until every deletion gate below
passes.

This candidate failed at `2026-08-06T02:30:36Z`: the non-adaptive primary path
panicked while building final-decision lineage. Raw recording continued, but
strategy, fair-value, and decision processing stopped in the unsupervised Tokio
task. The 72-hour clock is invalid and must restart from a clean segment after
the corrected digest is deployed and its full processing path is verified.

The corrected API candidate started at `2026-08-06T04:02:05Z` on source
`af69993b24dea731322a068aaa9dfda5b59dbb50`, pinned to ARM64 digest
`sha256:ead54f43b3dd25367f4b79a0e463d513ef332ea42986da440764ade6dea58e65`.
At the `04:15` venue rollover it produced 12 fresh final decisions without a
panic. At `04:15:14Z` the required feed and runtime tasks were still running,
the service had zero restarts, and the recorder reported 504,431 persisted
events with zero failures or unrecovered durable events. The `04:00` segment
contains both API digests and is excluded; `04:10:00Z` is the first clean
processing-observation boundary for the corrected digest. This is not the
formal 72-hour acceptance start: the dedicated job identities and primary
research timers must exist and run before that clock can start. Azure remains
authoritative.

The archive-capable API was deployed at `2026-08-06T07:27:55Z` from source
`69402f0ddbbb8effedda30a5297e85b643134435`, pinned to ARM64 digest
`sha256:4e2f32d34d3ac8768a656f6481728e643afaae588510763dddbdf739dfa7f02d`.
The first production schema-v2 segment retained its 277,381,051-byte local
JSONL source and uploaded a 34,635,062-byte deterministic gzip sidecar, an
87.5% reduction. At `07:32:19Z` the runtime Arc identity created and receipted
both immutable objects under `events-oci-hot7-v1/`; an independent Arc-authenticated
download returned Hot `application/gzip` content whose 34,635,062-byte length
and `sha256:4bffcd8e019ffb5487776c9c51b062f651912dafb995961397e35aef6a43ea9f`
matched the sealed manifest exactly. Ring health then reported zero unuploaded
or unsealed closed segments with the recurring timer enabled. A conflicting,
unuploaded `events-oci-test` sidecar from preflight was moved intact to the
rollback directory; its original source and already-verified production v1
receipt were not changed.

The first provisional Azure-authoritative parity window started at the clean
final digest segment boundary, `2026-08-09T07:50:00Z`, and cannot complete before
`2026-08-12T07:50:00Z` or before two OCI daily cycles pass. The API is pinned to
ARM64 digest
`sha256:d6e9545f18d7d53da42880749b57beee6c9477d6f1e3621eead74b80f6192334`
from source `e451bf77317d55060ecbbd6aef6d6ea544c78fcf`; the frontend remains pinned to
`sha256:dd59509d917855a345ab7b3eb9b33d44506fa75f9a6ba96b4d65100c39ca78c1`.
An actual rollback to API digest
`sha256:4e2f32d34d3ac8768a656f6481728e643afaae588510763dddbdf739dfa7f02d`
passed at `07:41:17Z`, and the final UAMI configuration/digest redeployed and
passed at `07:41:41Z`. The first timer-fired freshness run at `07:47:02Z` was
healthy against the gzip ring prefix, with zero warnings or critical findings;
the manual hourly smoke finished in 77 seconds with no critical finding. The
four primary timers are boot-enabled; shadow-qset and funded remain disabled.
At window start, root had about 56 GiB free and the ring about 127 GiB free.
Machine-readable live state is preserved at
`/srv/polyedge-ring/parity/20260809T075000Z.json`, with Azure deletion set to
false. The first clean in-window segment (`07:50-08:00Z`) contained 165,980
records and 139,862,191 source bytes; its 20,588,439-byte gzip object and
manifest were remotely verified at `08:02:21Z`. The immediately following
`08:03:02Z` freshness run was healthy with zero warnings or critical findings.
That window was invalidated at `08:15:39Z`: the shared report writer still
auto-published OCI outputs to the same Azure comparison paths. All four OCI
research timers were disabled immediately. Azure execution
`polyedge-hourly-quality-job-0ly3tr8` then regenerated the affected hour and
succeeded at `08:20:35Z`, restoring Azure as the last writer. No deletion gate
was advanced; a new 72-hour window starts only after the local-only guard is
published and proven.

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
disabled. One lifecycle rule deletes only block blobs under
`bot-events/data/research/normalized/v1/` seven days after modification. A
second rule tiers only the new `bot-events/events-oci-hot7-v1/` block-blob
prefix to Cool after seven days and Archive after 30 days. That future-only
prefix leaves the existing raw corpus and all evidence/control prefixes
untouched. The exact two-rule policy was updated and read back from Azure at
`2026-08-06T06:17:06Z`.

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
