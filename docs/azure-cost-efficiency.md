# Azure Cost Efficiency Rollout

This update lowers recurring Azure spend without stopping the continuous paper
recorder, changing funded-execution controls, or changing the frozen qset
campaign. It is intentionally split into independently reviewable deployments.

## Baseline and target

The Cost Management snapshot queried on 2026-07-31 attributes approximately
$387.94 of July usage to PolyEdge, including the Container Apps managed resource
groups. The directly tagged resource group accounts for $373.61. Its largest
categories are Container Apps ($151.23), NAT Gateway ($102.44), bandwidth
($48.39), Storage ($31.24), Azure Monitor ($17.58), Log Analytics ($12.27), and
Container Registry ($7.83). Managed load balancers and their public IPs add
roughly $14 outside the application resource group.

The latest complete six cost days, 2026-07-25 through 2026-07-30, total $70.04,
or a pre-change run rate near $350 per 30 days. The implemented changes target
approximately $255-$285 for the first full 30-day period after rollout. This is
an evidence-based range, not a guaranteed bill; Cost Management ingestion lags,
job duration varies, and Azure meter prices apply at billing time. Budgets alert
at $206.25, $247.50, and $275 of actual PolyEdge spend and at a forecast of
$275.

### OCI migration live cost floor

A second live Cost Management query on 2026-08-09 covers August 1 through
August 8. Storage cost $27.1352: $9.7256 of Hot LRS capacity, $9.6638 of Hot
writes, and $6.1772 of list/create-class operations. Storage metrics at
13:00 UTC reported 2,204,118,414,920 bytes in the retained Standard LRS Hot
account. Daily metrics also showed hundreds of thousands of OAuth table writes
and append operations while the Azure API remained authoritative. Those
transaction meters should fall after the gated compute cutover, but that claim
must be verified from the post-cutover bill rather than assumed.

The retained capacity alone is currently about a $38-$41 monthly floor; the
retained Service Bus, Key Vault, and storage monitoring keep the honest
steady-state estimate above the requested $15-$35 range. The account already
uses Standard LRS, and the deployed lifecycle rule tiers only future compressed
OCI block blobs. Moving, deleting, or tiering the existing roughly 2 TB is not
authorized, so this runbook does not claim the lower target. Revisit it only
after the 72-hour cutover evidence quantifies the transaction collapse or a new
explicit decision changes the existing-data constraint.

### Live utilization evidence

| Driver | 2026-07-31 observation | Decision |
|---|---|---|
| Public app | 42 ingress requests in seven days; average 0.20 cores and 0.44 GiB working set across the current bot/frontend replica. | Keep the bot/API at one replica and move the rarely used frontend to HTTP scale-to-zero. |
| North Europe NAT | Approximately 55 GiB/day processed on the latest stable days: about 40 GiB inbound and 15 GiB outbound. The processed-data meter cost $12.01 in the last six billed days. | Keep static egress and the region, but route Azure Storage traffic over a service endpoint so storage writes do not consume NAT processed bytes. |
| Research logs | 2.04 GiB billable in seven days. Daily research emitted 1.22 GiB, hourly quality 0.26 GiB, replay 0.05 GiB, and freshness 0.04 GiB, mostly pretty-printed reports already persisted as artifacts. | Emit compact success summaries and retain capped output only on failure. |
| Research jobs | Daily research currently runs about 116 minutes and replay-index about 75 minutes. | Publish one verified normalized snapshot and let replay restore it instead of normalizing the same day again. |
| Storage | Used capacity reached 1.81 TB and grew about 266 GB from July 25 through July 31, roughly 44 GB/day. | Do not delete raw events. Cache normalized data for duplicate daily work now; design verified lossless raw compaction before imposing raw retention. |
| ACR | Basic registry storage is 41.0 GB across 264 manifests; the registry cost $7.83 in July. | Preserve current and rollback digests. Pruning is a later guarded change because Basic SKU has no native retention policy. |

The estimated first-month savings are $25-$35 from splitting the frontend,
$15-$20 from bypassing NAT for regional storage traffic, $10-$16 from compact
logs, $5-$8 from snapshot reuse, and about $5 from exact duplicate/orphan
cleanup. The ranges overlap Azure billing behaviors and must not be added as a
guarantee.

## Implemented changes

| Cost driver | Change | Functionality guardrail |
|---|---|---|
| Always-on public sidecar | `polyedge-dev-api` keeps the paper bot/API at one internal replica. `polyedge-dev` becomes a frontend-only app that scales from zero to one on HTTP traffic. | The bot remains continuous. Public HTTP, WebSocket, dashboard authentication, and API proxy routes remain at the existing public app. |
| Repeated normalization | The daily job normalizes once and publishes an immutable, content-addressed snapshot. The replay job restores and verifies that snapshot before building its index. | A changed or unavailable raw inventory fails snapshot validation and falls back to a fresh normalization. Both jobs share an Azure lease, so they cannot process the same day concurrently. |
| Raw archive size and lifecycle | Closed OCI segments keep their local JSONL source but upload a deterministic gzip sidecar; a measured 302,050,848-byte segment compressed to 36,970,396 bytes (87.8% smaller). One rule deletes only `bot-events/data/research/normalized/v1/` block blobs after seven days. A second tiers only future `bot-events/events-oci-hot7-v1/` block blobs from Hot to Cool after seven days and Archive after 30 days; the existing 14-day soft-delete recovery window still applies. | The original local segment is hashed separately and remains the job input. V1 receipts stay compatible. The existing raw corpus, daily reports, replay outputs, qset evidence, and funded evidence are outside the new prefix and are retained. Archive retrieval requires rehydration, so only future raw objects use it. |
| Verbose successful job logs | Daily/replay scripts emit compact stage records. A shared wrapper reduces the hourly-audit and five-minute-freshness reports to status, warnings, critical findings, and duration. Captured command output is emitted only on failure and capped at 64 KiB. | The full JSON artifacts are still written to storage; exit codes and the exact warning/failure phrases consumed by Azure Monitor remain in console logs. |
| Job-failure alert drift | The retained rule uses `ContainerJobName_s` for console rows, `JobName_s` for system rows, and explicit system/exit failure reasons. It deliberately excludes generic `error` and `failed` text, which appears in benign report fields. | Cleanup refuses to remove the legacy rule until the retained rule is enabled and contains all required explicit failure fragments. |
| Duplicate/orphan resources | A preflighted cleanup script targets only two named legacy alerts and the exact unattached North Europe public IP. | Default mode is read-only. `--apply` validates every target before deleting any target and refuses an attached IP or non-redundant alert. |
| Regional storage egress | The North Europe shadow and Chile funded Container Apps subnets enable the `Microsoft.Storage.Global` service endpoint through an isolated two-subnet template. | NAT gateways, static IPs, subnet prefixes, delegations, runtimes, minimum replicas, credentials, risk gates, and execution commands are unchanged. |
| Cost visibility | Subscription-scope Bicep adds a $275 PolyEdge budget covering the application and both Container Apps managed resource groups, plus a $350 subscription budget, with actual and forecast notifications. | The isolated workflow proves its what-if contains only budget resources. |

Azure documents `Microsoft.Storage.Global` as generally available in all Azure
regions and routes service traffic directly over the Azure backbone; enabling a
service endpoint can temporarily interrupt service traffic while the route and
source identity change. The isolated endpoint workflow requires an explicit
maintenance-window acknowledgement and proves that its what-if contains only
the two named subnets. Review the
[service endpoint guidance](https://learn.microsoft.com/en-us/azure/virtual-network/virtual-network-service-endpoints-overview)
before the Chile maintenance window. The budget template follows the
[Microsoft.Consumption budget schema](https://learn.microsoft.com/en-us/azure/templates/microsoft.consumption/budgets).

## Rollout order

No source change in this update should be treated as permission to deploy it.
Use this sequence and keep the full runtime topology change behind the active
qset campaign gate.

1. Deploy budgets. This has no runtime effect. The GitHub deployment identity is
   resource-group scoped, so a subscription operator must run this isolated
   subscription-scope deployment unless that identity is deliberately granted
   separate subscription-level budget and deployment permissions.

   ```bash
   expected_subscription=73783c0c-5a53-4f9b-b244-6f64e813814c
   test "$(az account show --query id -o tsv)" = "$expected_subscription"
   budget_start=$(date -u +%Y-%m-01T00:00:00Z)
   az deployment sub what-if \
     --name polyedge-cost-governance \
     --location eastus \
     --template-file infra/cost-governance.bicep \
     --parameters budgetStartDate="$budget_start" \
     --result-format ResourceIdOnly
   az deployment sub create \
     --name polyedge-cost-governance \
     --location eastus \
     --template-file infra/cost-governance.bicep \
     --parameters budgetStartDate="$budget_start"
   ```

2. Deploy the isolated monitoring template. It may safely mention
   `polyedge-dev-api` before that app exists and snapshots only Container Apps
   that currently exist.

   ```bash
   gh workflow run deploy-polyedge-active.yml \
     --ref <reviewed-sha> \
     -f research_job_target=monitoring-only \
     -f authorize_shadow_runtime_change=false
   ```

3. Preflight and then apply exact cleanup. Review the dry-run output before the
   second command.

   ```bash
   bash scripts/azure-cost-cleanup.sh
   bash scripts/azure-cost-cleanup.sh --apply
   ```

4. Deploy the compact freshness/hourly logs and the primary daily/replay jobs
   separately, outside their schedule exclusion windows. Each deployment
   updates only the selected job image and exact command. It does not start a
   job or touch the frozen shadow recorder.

   ```bash
   gh workflow run deploy-polyedge-research-jobs.yml \
     --ref <reviewed-sha> \
     -f target_job=polyedge-data-freshness-job

   gh workflow run deploy-polyedge-research-jobs.yml \
     --ref <reviewed-sha> \
     -f target_job=polyedge-hourly-quality-job

   gh workflow run deploy-polyedge-research-jobs.yml \
     --ref <reviewed-sha> \
     -f target_job=polyedge-daily-research-job

   gh workflow run deploy-polyedge-research-jobs.yml \
     --ref <reviewed-sha> \
     -f target_job=polyedge-replay-index-job
   ```

5. Deploy the two storage endpoints only in an approved maintenance window.
   The workflow snapshots protected runtime definitions, restricts what-if to
   the exact North Europe and Chile subnets, and verifies that both NAT gateway
   attachments and Container Apps delegations survive.

   ```bash
   gh workflow run deploy-polyedge-storage-endpoints.yml \
     --ref <reviewed-sha> \
     -f confirm_maintenance_window=true
   ```

6. Do not deploy `infra/main.bicep` or the frontend-only workflow until the qset
   campaign gate allows an active-runtime topology change. At that point, run a
   reviewed group-scope what-if and use the active workflow. The expected plan
   creates `polyedge-dev-api`, changes `polyedge-dev` to frontend-only, and
   changes the daily/replay job commands. It must not change the frozen qset
   configuration or funded execution resources.

## Validation after each phase

- Budgets: both budgets are monthly, enabled, and contain actual 75/90/100 and
  forecast 100 notifications.
- Monitoring: all six isolated rules are enabled and protected Container App and
  funded-job definitions are byte-for-byte unchanged.
- Cleanup: both legacy alert lookups and the public-IP lookup return not found;
  the retained alerts remain enabled.
- Daily/replay jobs: identities, schedules, paper-only environment, absence of
  funded credentials, immutable image digest, shared lease blob, and exact
  script command all match source.
- Split runtime: `polyedge-dev-api` is internal with one `bot` replica;
  `polyedge-dev` is external, frontend-only, and configured for zero-to-one HTTP
  scaling. Public health, authenticated API, Labs pages, SSE, and WebSocket
  checks pass after a scale-from-zero request. Exactly one paper writer emits
  new durable events.
- Storage endpoints: each exact subnet retains its prefix, NAT gateway, and
  `Microsoft.App/environments` delegation; `Microsoft.Storage.Global` exists;
  protected North Europe/qset/funded runtime definitions compare byte-for-byte;
  storage upload/download and recorder heartbeat checks pass.

## Applied state on 2026-07-31

- Subscription deployment `polyedge-cost-governance-3814514` created and
  verified the $275 PolyEdge and $350 subscription budgets.
- GitHub Actions run `30659858265` deployed the six retained monitoring rules
  and proved protected runtime definitions unchanged. The guarded cleanup then
  removed only `polyedge-dev-missing-latest-blob`,
  `polyedge-dev-research-job-failure`, and the unattached
  `pip-polyedge-venue-neu-egress` public IP.
- Research snapshot `e7ca3ecb46c52230f007c302a7e4aac671f9d9ca` passed the
  source-freeze guard and complete CI validation step. OIDC then rejected the
  temporary branch before any Azure mutation, so the authenticated subscription
  operator ran ACR build `ca22`, which published
  `polyedge-rust-research@sha256:c77f000689d7733b692be714b8b18ba019a5fdf98943c5aff724fa14a34f4b96`,
  which is deployed to freshness, hourly quality, daily research, and replay
  index. Freshness executions at 20:05 and 20:10 UTC succeeded with compact
  two-line output.
- Resource-group deployment `polyedge-storage-service-endpoints-3814514`
  enabled `Microsoft.Storage.Global` on the two exact subnets. The qset
  recorder saw one connection reset during route propagation and recovered all
  three queued events on retry 2; later health rows showed zero unrecovered
  durable events. Protected shadow, qset, and funded definitions were unchanged
  and funded heartbeats continued.
- The `polyedge-dev` frontend/API split is not deployed. The qset disposition
  still marks `campaign-2026-07-28-qset-v1` active through 2026-09-25, so the
  broad topology cutover remains blocked. Live `polyedge-dev--0000123` retains
  its existing bot and frontend containers at one replica until that gate is
  formally closed or a separately reviewed exactly-one-writer cutover is
  approved.

## Rollback

- Budgets and alerts: redeploy the prior isolated template, or recreate a
  removed legacy rule only if the retained rule is demonstrably insufficient.
- Research jobs: dispatch the research-only workflow at the last known-good SHA.
  The raw-input fallback retains existing output behavior if no valid snapshot
  is available.
- Split runtime: first scale `polyedge-dev-api` to zero, then reactivate the
  previous `polyedge-dev` sidecar revision and prove exactly one writer. Never
  run both bot revisions simultaneously. Roll forward with a reviewed template
  once the issue is fixed.
- Storage endpoints: remove only the added service endpoint through the isolated
  subnet template, then re-run storage and recorder checks. Do not detach either
  NAT gateway or delegation.

## Storage growth follow-up

Raw event growth is the remaining unbounded cost driver. The current update does
not delete or archive it because the source is append-blob evidence used by
audit, replay, qset, and settlement-carry paths. A safe compaction phase must:

1. deterministically gzip each sealed minute append blob into an immutable block
   blob while preserving every JSONL byte and the original ETag/SHA-256 binding;
2. teach all Azure raw readers and inventories to accept mixed `.jsonl` and
   `.jsonl.gz` days without changing event order or canonical hashes;
3. verify download, decompression, row count, byte hash, replay output, and a
   complete day inventory before deleting the source append blob; and
4. keep the 14-day soft-delete recovery window and prove restoration before a
   lifecycle rule is enabled.

Until that proof exists, raw-event deletion is explicitly out of scope. Review
realized daily cost after 72 hours and the monthly forecast after seven days. If
the forecast remains above $285, compare NAT processed bytes, Log Analytics
ingestion, job duration, and storage growth against the baseline above before
making a second change.
