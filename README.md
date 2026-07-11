# PolyEdge

PolyEdge is a Rust-first, paper-default control plane for crypto Up/Down Polymarket markets. The active target is BTC 15-minute Up/Down, with configurable discovery, reference feeds, strategy, risk, execution, recording, replay, and reporting.

The deployed backend is Rust. Live trading remains disabled by default and is guarded by configuration, location confirmation, wallet credentials, risk checks, and exact source checks.

## Quick Start

```bash
cargo test --workspace --all-features
cargo run -p polyedge-cli -- api --bind 127.0.0.1:8081
```

Run one market discovery pass:

```bash
cargo run -p polyedge-cli -- discover
```

Confirm the configured Polymarket/Chainlink source:

```bash
cargo run -p polyedge-cli -- confirm-source
```

Replay collected JSONL events:

```bash
cargo run -p polyedge-cli -- backtest --path data/events.jsonl
```

Benchmark replay over Azure Blob data with a short-lived read/list SAS in `AZURE_STORAGE_SAS`:

```bash
cargo run --release -p polyedge-cli -- bench-azure-replay \
  --account stpolyedge6urdjr5nmwx7w \
  --container bot-events \
  --prefix events/ \
  --prefetch-blobs 8
```

## Deployment

Deploy through GitHub Actions with `.github/workflows/deploy-polyedge-active.yml`. The workflow runs Rust and frontend validations, builds `Dockerfile.rust` and `Dockerfile.frontend`, pushes images to ACR, and applies `infra/main.bicep` to `polyedge-dev`.

```bash
gh workflow run deploy-polyedge-active.yml --ref <branch-or-sha>
```

## Frontend

The Next.js frontend remains the public control plane. Browser calls use `/api/backend/*`; the server-side proxy forwards them to the Rust backend `/api/v1/*` with the bearer token kept server-side.

```text
Public frontend/API: https://polyedge-dev.graypond-7f5d8417.eastus.azurecontainerapps.io
Backend sidecar:     http://127.0.0.1:8081/api/v1
WebSocket sidecar:   ws://127.0.0.1:8081/api/v1/ws/live
```

## Live Trading Gates

The safe defaults are:

```text
EXECUTION_MODE=paper
ALLOW_LIVE=false
ENABLE_TAKER_ORDERS=false
ALLOW_EMERGENCY_ACCOUNT_CANCEL=false
PAPER_MAKER_FILL_POLICY=touch_after_quote_was_live
PAPER_ORDER_LIVE_AFTER_MS=250
```

Live mode must not be enabled unless all live gates are intentionally satisfied:

```text
EXECUTION_MODE=live
ALLOW_LIVE=true
CONFIRM_NON_RESTRICTED_LOCATION=true
POLYMARKET_PRIVATE_KEY is set
REQUIRE_EXACT_RESOLUTION_SOURCE_FOR_LIVE is satisfied
all risk checks pass
kill switch is clear
```

## Project Layout

```text
crates/polyedge-domain      shared event, market, order, and report models
crates/polyedge-config      environment and safety-gate configuration
crates/polyedge-feeds       Polymarket discovery, CLOB books, RTDS, reference feeds
crates/polyedge-engine      fair value, strategy, risk, order manager, paper fills
crates/polyedge-execution   execution traits and paper execution client
crates/polyedge-storage     JSONL recorder and native Azure Blob client/recorder
crates/polyedge-reporting   streaming replay, backtesting, PnL reports
crates/polyedge-api         Rust HTTP/WebSocket API and runtime controller
crates/polyedge-cli         local API, replay, discovery, benchmarks
frontend/                   Next.js control plane
infra/                      Azure Container Apps, ACR, Storage, identity
```

Operational docs:

```text
docs/azure-deployment.md
docs/api-access.md
docs/backtesting.md
docs/chainlink-source.md
docs/execution-quality-limitations.md
docs/rust-migration-status.md
```
