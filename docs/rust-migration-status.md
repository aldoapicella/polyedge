# Rust Migration Status

Status: complete and active.

The production Container App is running the Rust backend as the active paper-mode backend. The previous backend source, tests, scripts, Dockerfile, and package metadata have been removed from the repository.

## Implemented Crates

```text
polyedge-domain      shared models and event contracts
polyedge-config      env config, live gates, paper defaults
polyedge-feeds       market discovery, CLOB books, RTDS reference feeds
polyedge-engine      fair value, strategy, risk, order manager, paper fills
polyedge-execution   execution traits and paper execution client
polyedge-storage     JSONL recorder, Azure Append Blob recorder, Azure Blob replay client
polyedge-reporting   streaming ReplayBacktester, PnL reports, market statistics
polyedge-api         Rust API, WebSocket, runtime controller, frontend contract routes
polyedge-cli         API server, discovery, source confirmation, replay, benchmarks
```

## Cutover Evidence

Deployment is performed by `.github/workflows/deploy-polyedge-active.yml`. The workflow validates Rust and frontend code, builds `Dockerfile.rust` and `Dockerfile.frontend`, pushes images to ACR, and applies `infra/main.bicep`.

```text
Container App: polyedge-dev
Traffic: 100% to the latest active revision
Backend image pattern: crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-rust-backend:<git-sha>
Frontend image pattern: crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-frontend:<git-sha>
RUN_BOT_ON_STARTUP: true
BACKEND_API_BASE_URL: http://127.0.0.1:8081/api/v1
```

Public proxy validation returned HTTP 200 for:

```text
/api/backend/health
/api/backend/status
/api/backend/snapshot
/api/backend/markets/current
/api/backend/orders
/api/backend/fills
/api/backend/decisions
/dashboard
```

Key status fields at validation:

```text
backend_impl: rust
shadow_only: false
execution_mode: paper
runtime_loop: running
markets: 4
tradeable_markets: 1
books: 8
reference.stale: false
kill_switch: false
paused: false
```

## Replay Benchmarks

134 MB capped Azure sample:

```text
prefix: events/2026/06/10/
transport: native_ureq_persistent_prefetch
prefetch_blobs: 4
listed_blobs: 13
replayed_bytes: 122,755,898
events: 275,940
elapsed_ms: 7,991.881066
events_per_second: 34,527.54085317115
mib_per_second: 14.64850967415352
memory_rss_mb: 44.1640625
filled_orders: 1
net_pnl: 0
```

Full Azure prefix:

```text
artifact: docs/reports/rust-azure-full-replay-20260611T1540Z.json
prefix: events/
transport: native_ureq_persistent_prefetch
prefetch_blobs: 8
listed_blobs: 12,728
replayed_bytes: 132,262,143,189
replayed_gib: 123.17871971894056
events: 297,025,142
elapsed_ms: 4,513,692.556698
events_per_second: 65,805.35520950264
mib_per_second: 27.944971308473306
memory_rss_mb: 372.11328125
filled_orders: 2,028
net_pnl: -516.155
```

## Remaining Operational Notes

- Live trading is still disabled and must remain gated.
- Binance direct WebSocket can return HTTP 451 from this environment; Polymarket RTDS Binance cross-check remains the usable proxy path.
- The Azure replay client retries initial GET/list failures and full response-body read timeouts.
- Future optimization: stream large blobs in ranged chunks instead of downloading each prefetched blob into memory.
