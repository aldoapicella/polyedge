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
