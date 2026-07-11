# Replay Backtesting

The Rust replay engine streams JSONL event envelopes and reconstructs markets, captured RTDS Chainlink start prices, settlement prices, paper decisions, fills, and estimated PnL.

Run a local replay:

```bash
cargo run -p polyedge-cli -- backtest --path data/events.jsonl
```

Run the fixture benchmark:

```bash
cargo run --release -p polyedge-cli -- bench-replay --path tests/fixtures/events_pnl_sample.jsonl
```

Run an Azure replay using the native Rust Azure Blob client:

```bash
AZURE_STORAGE_SAS=<read-list-sas> target/release/polyedge-rs bench-azure-replay \
  --account stpolyedge6urdjr5nmwx7w \
  --container bot-events \
  --prefix events/2026/06/10/ \
  --max-bytes 134217728 \
  --prefetch-blobs 4
```

## Fill Model

This is a conservative research replay, not a queue-accurate exchange simulator.

```text
FAK/FOK decisions:
  fill immediately at decision price
  interpret size as shares
  pay crypto taker fee

Post-only maker decisions:
  rest as open replay orders
  interpret size as shares
  fill only if a later book ask is less than or equal to the bid price
  pay no taker fee
```

The replay enforces quote-live delay, TTL, market active window, final no-trade window, stale-book guard, and cancellation state.

Recorded `book` events are complete snapshots, not deltas. Replay replaces the
prior reconstructed book on every snapshot; retaining absent historical levels
can manufacture crossed books and false fills. A crossed or incomplete top of
book now fails closed before fill evaluation and is emitted as a report warning.
Daily PnL produced before this invariant was enforced must be regenerated before
it is used for strategy conclusions.

## Runtime Paper vs Replay

Reports separate two ledgers:

```text
actual_paper      runtime paper execution reports with positive filled_size
replay_estimate   offline cancellation-aware replay over recorded events
runtime_vs_replay comparison of fills and PnL between both ledgers
```

The default runtime paper maker-fill policy is `touch_after_quote_was_live`.

Queue position, partial-fill, cancellation-latency, trade-through, and markout
measurement boundaries are defined in
[`execution-quality-limitations.md`](execution-quality-limitations.md). Public
level-2 data provides a visible-size-ahead estimate, not a true FIFO rank.

Generate the daily execution-quality evidence report with:

```bash
polyedge-rs research execution-quality \
  --input data/research/normalized \
  --out reports/research/execution_quality.json \
  --markdown reports/research/execution_quality.md
```

The evidence gate requires at least 95% queue-snapshot coverage and 95%
completion for each observed 1/5/30-second markout horizon. A lack of paper
orders is `COLLECTING`, not a false data-quality failure.

## Settlement Model

For each market:

```text
start_price = captured market_start_price from RTDS Chainlink btc/usd
final_price = first RTDS Chainlink btc/usd tick at or after market end
```

Default settlement window:

```text
settlement_window_seconds = 15
```

Outcome:

```text
Up wins   if final_price >= start_price
Down wins otherwise
```

## Latest Full Azure Replay

Validated artifact:

```text
docs/reports/rust-azure-full-replay-20260611T1540Z.json
```

Result:

```text
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
