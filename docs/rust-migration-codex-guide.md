# Rust Migration Guide

This migration is complete. PolyEdge is now Rust-only for backend/runtime code, and the active Azure deployment uses the Rust backend behind the existing frontend proxy contract.

Use this document only as a completion checklist for future audits.

## Required Invariants

```text
Do not enable live trading by default.
Preserve EXECUTION_MODE=paper and ALLOW_LIVE=false.
Preserve ENABLE_TAKER_ORDERS=false unless explicitly changed in a reviewed live-mode task.
Keep browser-facing secrets server-side in the Next.js proxy/session layer.
Keep /api/backend/* and /api/realtime frontend contracts stable.
Keep /api/v1/* backend contracts stable for the sidecar API.
```

## Current Backend

```text
binary: polyedge-rs
API bind: 0.0.0.0:8081 in container, 127.0.0.1:8081 locally
Dockerfile: Dockerfile.rust
Azure image repo: crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-rust-backend
```

## Validation Checklist

```bash
cargo fmt --all
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --release -p polyedge-cli
```

Frontend checks, when Node is available:

```bash
cd frontend
npm run typecheck
npm run build
```

Production checks:

```bash
curl https://polyedge-dev.graypond-7f5d8417.eastus.azurecontainerapps.io/api/backend/health
curl https://polyedge-dev.graypond-7f5d8417.eastus.azurecontainerapps.io/api/backend/status
curl https://polyedge-dev.graypond-7f5d8417.eastus.azurecontainerapps.io/api/backend/snapshot
curl https://polyedge-dev.graypond-7f5d8417.eastus.azurecontainerapps.io/dashboard
```

Replay checks:

```bash
target/release/polyedge-rs bench-replay --path tests/fixtures/events_pnl_sample.jsonl
AZURE_STORAGE_SAS=<read-list-sas> target/release/polyedge-rs bench-azure-replay \
  --account stpolyedge6urdjr5nmwx7w \
  --container bot-events \
  --prefix events/ \
  --prefetch-blobs 8
```

Latest status and metrics are recorded in `docs/rust-migration-status.md`.
