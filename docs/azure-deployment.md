# Azure Deployment

PolyEdge runs as a Rust backend container plus a Next.js frontend sidecar in Azure Container Apps.

```text
subscription: Visual Studio Professional Subscription
resource group: rg-polyedge-dev
region: eastus
Container App: polyedge-dev
Container App identity: polyedge-dev-id
FQDN: polyedge-dev.graypond-7f5d8417.eastus.azurecontainerapps.io
Storage account: stpolyedge6urdjr5nmwx7w
Blob container: bot-events
Table: BotEventIndex
ACR: crpolyedge6urdjr5nmwx7w.azurecr.io
GitHub deploy identity: id-github-polyedge-dev
```

## Safety Defaults

The deployed backend remains paper-only:

```text
EXECUTION_MODE=paper
ALLOW_LIVE=false
RUN_BOT_ON_STARTUP=true
ENABLE_TAKER_ORDERS=false
PAPER_MAKER_FILL_POLICY=touch_after_quote_was_live
PAPER_ORDER_LIVE_AFTER_MS=250
ALLOW_EMERGENCY_ACCOUNT_CANCEL=false
ENABLE_LIVE_HEARTBEAT=true
```

The deployed paper runtime's execution-quality metrics remain research-only.
See [`execution-quality-limitations.md`](execution-quality-limitations.md) for
the public-data boundary and the separately gated authenticated venue-probe
path needed for real order acknowledgements, fills, and cancellation timing.

## Deployment Pipeline

Deployments are driven by GitHub Actions. The active workflow is `.github/workflows/deploy-polyedge-active.yml`. It runs Rust checks, frontend typecheck/build, builds `Dockerfile.rust` and `Dockerfile.frontend`, pushes images to ACR, and deploys both images with `infra/main.bicep`.

Trigger from GitHub Actions or with:

```bash
gh workflow run deploy-polyedge-active.yml --ref <branch-or-sha>
```

The backend image produced by the workflow is:

```text
crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-rust-backend:<git-sha>
```

Do not deploy by local `az acr build` or `az containerapp update`; use the workflow so validation and deployment evidence stay attached to the commit.

After deployment the workflow logs into the dashboard without exposing the
password, verifies authenticated health/status/snapshot/market/order/fill/
decision/report routes, asserts paper-only recorder health, runs the
deterministic execution-quality probe, and rejects any fresh replica restart.

Azure Monitor alerts cover storage ingress, job failures, recorder failures or
drops, recorder queue depth above 1,000 events, runtime restart/backoff, and a
Container Apps working set above 750 MiB. The backend emits a compact
`runtime_health` record every minute so the recorder alerts test numeric values
instead of matching harmless fields whose value is zero.

## Frontend Wiring

The frontend container proxies backend traffic to the Rust sidecar:

```text
BACKEND_API_BASE_URL=http://127.0.0.1:8081/api/v1
BACKEND_WS_URL=ws://127.0.0.1:8081/api/v1/ws/live
```

The public browser path remains stable:

```text
/api/backend/*
/api/realtime
```

## Data Layout

Events are recorded as minute-segmented JSONL append blobs:

```text
bot-events/events/YYYY/MM/DD/HH/mm.jsonl
```

The JSONL envelope is:

```json
{
  "recorded_ts": "2026-06-02T00:00:00+00:00",
  "event_type": "reference",
  "payload": {}
}
```

Table index partitions use:

```text
<event_type>:<YYYYMMDD>
```

Indexed event types:

```text
market
market_start_price
paper_settlement
fair_value
decision
execution_report
feed_error
reference
live_heartbeat
```

## Replay Validation

Generate a short-lived read/list SAS without printing it, then run replay benchmarks:

```bash
key="$(az storage account keys list \
  --resource-group rg-polyedge-dev \
  --account-name stpolyedge6urdjr5nmwx7w \
  --query '[0].value' -o tsv)"
az storage container generate-sas \
  --account-name stpolyedge6urdjr5nmwx7w \
  --account-key "$key" \
  --name bot-events \
  --permissions rl \
  --expiry 2026-06-11T23:59Z \
  --https-only -o tsv > /tmp/polyedge_azure_storage_sas.txt
unset key
```

```bash
AZURE_STORAGE_SAS="$(cat /tmp/polyedge_azure_storage_sas.txt)" \
  target/release/polyedge-rs bench-azure-replay \
  --account stpolyedge6urdjr5nmwx7w \
  --container bot-events \
  --prefix events/ \
  --prefetch-blobs 8
```

Latest full replay artifact:

```text
docs/reports/rust-azure-full-replay-20260611T1540Z.json
```

## Production Validation

Post-deploy checks validate these fields on the latest active revision:

```text
traffic: 100%
health: Healthy
backend_impl: rust
shadow_only: false
execution_mode: paper
runtime_loop: running
reference.stale: false
RUN_BOT_ON_STARTUP: true
BACKEND_API_BASE_URL: http://127.0.0.1:8081/api/v1
```

Validated public paths:

```text
/api/backend/health      200
/api/backend/status      200
/api/backend/snapshot    200
/api/backend/markets/current 200
/api/backend/orders      200
/api/backend/fills       200
/api/backend/decisions   200
/dashboard               200
```

## North Europe authenticated evidence worker

The paper strategy runtime and research control plane remain in East US. The
manual authenticated evidence worker is a separate Azure Container Apps Job in
North Europe because the East US origin is geoblocked by the venue.

Provision or update it with the exact probe image:

```bash
az deployment group create \
  --resource-group rg-polyedge-dev \
  --template-file infra/venue-probe.bicep \
  --parameters venueProbeImage="$VENUE_PROBE_IMAGE"
```

`infra/venue-probe.bicep` creates a dedicated North Europe managed environment,
VNet, delegated infrastructure subnet, NAT Gateway, zone-redundant static public
IP, and user-assigned managed identity. The job has no inbound endpoint. It
reads credential references from the existing Key Vault and writes redacted,
per-run evidence to the existing `bot-events` container. Redaction covers the
signing key, API key identifier, API secret, passphrase, authorization fields,
and authenticated lifecycle `owner`/`order_owner` fields. Blob WORM/versioning
is not enabled, so evidence is append-only by application convention rather
than storage-enforced immutability.

The declarative state is manual and dry-run. Before any order-enabled override,
verify the dry-run artifact proves all of the following:

```text
blocked: false
country: IE
observed egress IP: configured NAT public IP
open orders before run: 0
clock drift: <= 5000 ms
authenticated user channel: ready
public market channel: ready
```

The worker independently repeats the origin and clock checks immediately before
every submission. Live evidence collection remains maker-only, one open order
at a time, at most $1.25 per order for the current limited-funding campaign
(`$2` remains the hard code ceiling), and at most $5 of conservative filled
notional per UTC day. The East US probe job must remain deleted; deploying `main.bicep`
does not recreate it.

Dry-run preflight remains available after the daily risk budget is exhausted so
operators can still verify origin, authentication, WebSocket readiness, clock
drift, and zero open orders. This does not relax submission safety: the risk
gate reports `submission_allowed: false`, and no order is signed or sent while
`VENUE_PROBE_DRY_RUN=true`.

For an authorized campaign, use the checked launcher from Azure Cloud Shell or
the deployment runner:

```bash
./scripts/start-venue-probe-campaign.sh
```

The launcher refuses concurrent executions, requires the persisted manual job
to begin in dry-run, temporarily arms it, starts exactly one execution, and
restores dry-run under a shell trap. It then proves that the execution snapshot
is `false` while the persisted job is back to `true`. This update/start/restore
sequence is necessary because the installed Azure CLI accepted
`job start --env-vars VENUE_PROBE_DRY_RUN=false` but silently retained the
stored `true` value in the execution template. Never infer live authorization
from the command exit code; inspect both templates.
Restoration is retried five times with bounded backoff and read-back after every
attempt. The EXIT trap is not removed until persisted `true` is observed; a
failed restoration is a critical launcher failure, never a suppressed warning.

The worker itself also holds a renewable Blob lease for the funded account, so
overlapping executions fail closed even if they bypass the launcher. Evidence
protocol v3 writes a durable full-notional reservation before every send and
persists the venue order ID immediately after acknowledgement. A reservation
is reduced to matched notional only after terminal REST/user-channel agreement
and strict zero-open-order confirmation. An ambiguous or interrupted probe
therefore consumes its full reservation and blocks later live runs until it is
explicitly reconciled. Live orders are short-lived GTD orders; process signals
trigger cancel-all recovery, while venue expiry remains the crash backstop.
Lease calls are bounded to ten seconds and lease freshness is independently
checked at 45 seconds on a monotonic clock, including immediately after the
pre-send reservation. Live startup scans unresolved reservations across every
UTC date, not only the current daily-risk partition.

Post-cancel accounting is not finalized on the first empty REST response. The
worker observes REST order/trade state, authenticated user events, and all open
orders for at least ten seconds and requires five seconds of unchanged terminal
state. Missing probe evidence is reconstructed as an ineligible audit row from
its durable reservation, preventing a crashed execution from disappearing from
the quality gate.

Protocol v3 deliberately resets the promotion cohort. Older protocol-v2
observations remain available in Azure and the dashboard as legacy history,
but they do not count toward the 100-probe, 10-fill, 10-non-fill gate. Every
distinct authenticated trade ID now requires an independent timely 1/5/30
markout triplet, and REST/user-channel quantities and trade-ID sets must agree.

A refreshed book that no longer supports a non-marketable order above the
configured evidence floor is a normal market transition. The worker now
reconciles zero open orders and reports `campaign_stopped_safely` with
`no_safe_order_after_prewarm` rather than misclassifying that transition as a
failed execution. Completed probes and their conservative risk remain preserved
in the immutable run summary.

## Gasless resolved-position redemption

`infra/venue-probe.bicep` also provisions
`polyedge-redeem-neu-job` in the same isolated North Europe
environment. Its persistent state is manual, `VENUE_REDEMPTION_ENABLED=false`,
and `VENUE_REDEMPTION_DRY_RUN=true`. It shares the funded-wallet Blob lease with
the probe, so redemption and order evidence collection cannot overlap.

First create a Relayer API key in Polymarket **Settings > API Keys**. Store only
its key value in Key Vault; the associated public owner address is a Bicep
parameter:

```bash
az keyvault secret set \
  --vault-name kvpolyedge6urdjr5nmwx7w \
  --name polymarket-relayer-api-key \
  --value '<relayer-api-key>' \
  --output none

az deployment group create \
  --resource-group rg-polyedge-dev \
  --template-file infra/venue-probe.bicep \
  --parameters venueProbeImage="$VENUE_PROBE_IMAGE" \
               relayerApiKeySecretConfigured=true \
               relayerApiKeyAddress='<address shown for that key>'
```

Do not reuse the CLOB API key. The live launcher refuses to run until the Key
Vault secret and job secret reference both exist:

```bash
./scripts/start-venue-redemption.sh
```

The launcher passes the enable and dry-run overrides only to one execution and
verifies that the persisted job stays disabled/dry-run before and after start.
It then validates the execution snapshot. A successful redemption does not
start a probe; the $5 UTC trading-risk cap remains independent.

Every dry-run and live artifact also reads the venue's recent public redemption
activity. A transaction is attributed to `azure_redemption_worker` only when
its hash matches the durable worker control record; otherwise the dashboard
labels it `external_or_manual`. This prevents a wallet/UI redemption from being
misreported as an Azure worker submission.
