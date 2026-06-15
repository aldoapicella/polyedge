# PolyEdge Rust Research Lab + Regime Profiles — Codex Guide

Use this guide with the short Codex goal. This is for the Rust-migrated `aldoapicella/polyedge` repo.

## 0. Mission

Build an offline research lab for the existing 120GB / 5-day dataset and implement deterministic regime-conditioned strategy profiles for evaluation.

The goal is not to “make it profitable” by overfitting. The goal is to determine, with reproducible evidence, whether the current static maker-first strategy or an adaptive regime policy has positive expected value under realistic fill assumptions.

## 1. Safety Rules

Non-negotiable:

- Do not enable live trading.
- Do not place real orders.
- Do not call live order-placement endpoints.
- Do not set `EXECUTION_MODE=live`.
- Do not set `ALLOW_LIVE=true`.
- Do not weaken live gates.
- Do not print, expose, or commit secrets.
- Do not log bearer tokens, private keys, Azure credentials, Chainlink credentials, or wallet keys.
- Do not mutate raw event data.
- Do not use final settlement price in decision-time features.
- Do not use LLMs for trade decisions.
- Do not deploy adaptive profiles to live.
- All adaptive behavior must be research-only or paper-only and disabled by default.

If a code path could place live orders, stop and report it.

## 2. Strategy Source of Truth

The strategy is a short-horizon probability, execution, and risk-management system, not an LLM prediction strategy.

Baseline assumptions:

- Default target: BTC 15-minute Polymarket crypto Up/Down.
- Primary reference: Polymarket RTDS Chainlink `btc/usd`.
- Cross-checks: Polymarket RTDS Binance, Binance, Coinbase.
- Fair value:

```text
q_up = Phi((log(S_now / S_start) + mu * tau) / (sigma * sqrt(tau)))
q_down = 1 - q_up
```

- `mu = 0` by default.
- Sigma comes from fresh Chainlink RTDS ticks only.
- Maker-first strategy.
- Takers disabled by default.
- Maker fee = 0.
- Crypto taker fee:

```text
taker_fee = shares * 0.07 * price * (1 - price)
```

- Maker quote defaults:

```text
maker_margin = 0.015
maker_min_edge = 0.01
order_ttl_seconds = 10
```

- No midpoint-fill backtests.
- No future leakage.
- No assuming all maker orders fill.
- No ignoring cancels, stale feeds, or taker fees.

## 3. Rust Repo Context

Expected workspace:

```text
crates/polyedge-domain
crates/polyedge-config
crates/polyedge-feeds
crates/polyedge-engine
crates/polyedge-execution
crates/polyedge-storage
crates/polyedge-reporting
crates/polyedge-api
crates/polyedge-cli
```

Existing binary:

```text
polyedge-rs
```

Existing commands may include:

```text
polyedge-rs api
polyedge-rs run
polyedge-rs discover
polyedge-rs confirm-source
polyedge-rs backtest --path
polyedge-rs report --prefix
polyedge-rs bench-ingest
polyedge-rs bench-replay
polyedge-rs bench-azure-replay
polyedge-rs bench-api-snapshot
```

Add research commands without breaking existing commands.

## 4. Engineering Standards

- Use typed structs and enums.
- Keep domain logic pure where possible.
- Use streaming/lazy processing for 120GB data.
- Do not load full data into memory.
- Use market-level net PnL as the primary statistical unit.
- Every experiment output must include:
  - command
  - input path/prefix
  - generated timestamp
  - git SHA if available
  - backend = rust
  - data window
  - config
  - fill model
  - split method
  - duration
  - warnings
- Keep old tests passing.
- Add tests for all new logic.
- Use `tracing` for diagnostics.
- Do not use `unwrap()`/`expect()` in production paths unless justified at startup/config boundaries.

## 5. Output Paths

```text
data/research/normalized/
data/research/parquet/              # optional, if implemented
data/research/markets.json
reports/research/data_audit.json
reports/research/data_audit.md
reports/research/markets_summary.json
reports/research/markets_summary.md
reports/research/baseline_static_all_fill_models.json
reports/research/baseline_static_all_fill_models.md
reports/research/regime_profiles.json
reports/research/regime_profiles.md
reports/research/parameter_sweep.json
reports/research/parameter_sweep.md
reports/research/calibration.json
reports/research/calibration.md
reports/research/sample_size.json
reports/research/sample_size.md
reports/research/final_strategy_research_report.json
reports/research/final_strategy_research_report.md
```

## 6. CLI Commands to Add

```text
polyedge-rs research audit
polyedge-rs research normalize
polyedge-rs research build-markets
polyedge-rs research replay
polyedge-rs research baseline
polyedge-rs research regimes
polyedge-rs research sweep
polyedge-rs research calibration
polyedge-rs research sample-size
polyedge-rs research report
polyedge-rs research ml-calibrate       # optional, only after core research works
```

## 7. Data Audit

Command:

```bash
polyedge-rs research audit \
  --input <data-root> \
  --out reports/research/data_audit.json \
  --markdown reports/research/data_audit.md
```

Support file or recursive directory input.

Report:

- total events
- event count by type/day/hour
- first and last event timestamp
- markets seen
- markets with start price
- markets settled
- start-price capture rate
- settlement rate
- missing start/final markets
- decision count
- execution report count
- paper resting/cancelled/filled/filled_maker
- cancel decisions
- paper settlements
- feed errors
- stale reference/book counts
- malformed lines
- missing payloads
- missing market IDs
- out-of-order timestamps
- duplicate estimate
- largest time gaps
- warnings and fatal data quality issues

Acceptance:

- Runs on fixtures.
- Runs on full dataset.
- Bounded memory.
- JSON and Markdown generated.

## 8. Normalize / Index

Command:

```bash
polyedge-rs research normalize \
  --input <data-root> \
  --out data/research/normalized \
  --format jsonl-indexed \
  --overwrite false
```

Optional Parquet if feasible.

Minimum outputs:

```text
events_manifest.json
markets.jsonl
references.jsonl
books.jsonl
fair_values.jsonl
decisions.jsonl
execution_reports.jsonl
paper_settlements.jsonl
feed_errors.jsonl
```

Each row should include:

- event type
- recorded timestamp
- source timestamp if present
- market ID
- token ID
- flattened important fields
- raw payload JSON fallback

## 9. Market Truth Table

Command:

```bash
polyedge-rs research build-markets \
  --input data/research/normalized \
  --out data/research/markets.json \
  --markdown reports/research/markets_summary.md
```

One row per market:

- market ID, condition ID, slug, question
- asset, horizon
- up/down token IDs
- start/end timestamps
- start price
- final price
- winning outcome
- completeness flags
- start/final source
- reference tick count
- book update counts
- fair value count
- decisions, reports, fills, cancels, feed errors
- data quality flags

Incomplete markets must be visible but excluded from profitability simulation by default.

## 10. Fill Models

Implement:

```rust
enum FillModel {
    NoMakerFills,
    Touch,
    TouchAfter250Ms,
    TouchAfter1000Ms,
    TradeThrough,
    QueueProxy,
    AdverseSelectionPenalized,
}
```

Definitions:

- `NoMakerFills`: maker orders never fill.
- `Touch`: maker buy fills if later best ask <= quote price while open.
- `TouchAfter250Ms`: touch but order live >= 250ms.
- `TouchAfter1000Ms`: touch but order live >= 1000ms.
- `TradeThrough`: best ask strictly below quote by at least one tick.
- `QueueProxy`: estimate size ahead and fill only if enough depletion/trade evidence exists. If infeasible, warn and skip.
- `AdverseSelectionPenalized`: touch-after-250ms plus penalty for unfavorable q/reference movement after fill.

All models must:

- replay event time
- handle cancel_all
- prevent fills after cancel
- prevent fills after close
- prevent fills inside final no-trade window unless explicitly configured and flagged
- track fills-after-cancel prevented
- track open orders remaining
- maker fees = 0
- taker fees only when taker simulation enabled

## 11. Replay

Command:

```bash
polyedge-rs research replay \
  --input data/research/normalized \
  --markets data/research/markets.json \
  --strategy-config research/configs/baseline.yaml \
  --fill-model touch_after_250ms \
  --out reports/research/replay_touch_after_250ms.json \
  --markdown reports/research/replay_touch_after_250ms.md
```

Replay must simulate:

- market state
- reference updates
- start-price capture
- fair value
- decisions
- order manager cancel/replace
- fill model fills
- close/settlement
- PnL

Outputs:

- events, markets, decisions, orders, fills, cancels
- gross PnL, fees, net PnL, notional cost, ROI
- market-level PnL
- daily/hourly PnL
- time-to-expiry bucket PnL
- q bucket PnL
- max drawdown
- cancel/fill ratio
- warnings

Time buckets:

```text
15-12m
12-9m
9-6m
6-3m
3-1m
final_60s
inside_final_no_trade_window
```

## 12. Baseline Static Evaluation

Command:

```bash
polyedge-rs research baseline \
  --input data/research/normalized \
  --markets data/research/markets.json \
  --out reports/research/baseline_static_all_fill_models.json \
  --markdown reports/research/baseline_static_all_fill_models.md
```

Run baseline across:

- no_maker_fills
- touch
- touch_after_250ms
- touch_after_1000ms
- trade_through
- queue_proxy if feasible
- adverse_selection_penalized

Report per fill model:

- markets settled
- orders
- fills
- fill rate
- net PnL
- ROI
- mean/median/std market PnL
- standard error
- 95% CI
- max drawdown
- profitable/losing markets
- cancel/fill ratio
- warnings

Do not claim profitability if only optimistic fill models win.

## 13. Regime-Conditioned Profiles

Implement in `polyedge-engine` and use in reporting experiments.

Core types:

```text
RegimeFeatures
RegimeLabel
RegimeProfile
RegimeClassifier
RegimePolicy
ProfiledStrategyConfig
AdaptiveStrategyResult
QuoteStyle
```

Regime labels and priority:

1. FeedRisk
2. MarketInactive
3. FinalWindow
4. VolatilityShock
5. NearStrike
6. WideOrThinBook
7. CalmLiquid
8. Normal

Safety regimes override immediately.

### Features

Compute only from data available at decision time:

- seconds since start / to expiry
- distance bps from start
- Chainlink returns 5s/10s/30s/120s
- realized vol 30s/120s
- shock z-score
- q_up/q_down/sigma
- up/down bid/ask/spread/top size/depth
- book update rates
- reference/book age
- feed divergence bps
- recent feed errors
- current/open positions/orders
- recent fill/cancel stats
- adverse move after fill if available

Missing values must be null with quality flags.

Every adaptive decision must log:

- regime
- profile
- features summary
- original params
- effective params
- reason

### Classifier Defaults

FeedRisk:

- stale reference or book
- feed divergence > 15 bps
- recent feed error

MarketInactive:

- inactive market
- missing start price
- missing books
- observe only

FinalWindow:

- seconds_to_expiry <= final_no_trade_seconds

VolatilityShock:

- abs Chainlink 10s return > 5 bps
- shock z >= 3
- high realized vol

NearStrike:

- abs distance <= 5 bps and <= 180s remaining
- or abs distance <= 2 bps anytime

WideOrThinBook:

- spread >= 3 ticks
- top size below threshold
- missing bid/ask

CalmLiquid:

- spread <= 1 tick both sides
- low vol
- not near strike
- no safety regime

Normal:

- fallback

Hysteresis:

- switch confirm = 3s
- min dwell = 5s
- safety overrides immediately

### Profiles

Quote styles:

```text
ImproveOneTick
JoinBestBid
FairMinusMarginOnly
NoQuote
```

Bounds:

```text
maker_margin 0.005..0.080
maker_min_edge 0.005..0.080
model_error_buffer 0.005..0.080
adverse_selection_buffer 0.000..0.080
order_ttl_seconds 1..30
size_multiplier 0..1
final_no_trade_seconds 30..300
```

No profile may increase size above base in v1.

Profiles:

FeedRisk / MarketInactive / FinalWindow:

```text
no_trade = true
cancel_existing = true
size_multiplier = 0
```

VolatilityShock:

```text
maker_margin >= max(base*2, 0.030)
maker_min_edge >= max(base*2, 0.020)
model_error_buffer >= max(base*2, 0.020)
adverse_selection_buffer >= max(base*3, 0.020)
ttl <= 3s
size_multiplier = 0.25
quote_style = JoinBestBid or NoQuote
```

NearStrike:

```text
maker_margin >= max(base*1.5, 0.025)
maker_min_edge >= max(base*2, 0.020)
model_error_buffer >= max(base*2, 0.020)
adverse_selection_buffer >= max(base*2, 0.015)
ttl <= 3s
size_multiplier 0.25..0.50
quote_style = FairMinusMarginOnly
optional no_trade when late and very near strike
```

WideOrThinBook:

```text
maker_margin >= max(base*1.5, 0.025)
maker_min_edge >= max(base*1.5, 0.015)
model_error_buffer >= max(base, 0.015)
adverse_selection_buffer >= max(base*2, 0.010)
ttl <= 5s
size_multiplier = 0.50
quote_style = JoinBestBid or FairMinusMarginOnly
```

CalmLiquid:

```text
maker_margin = max(0.010, base*0.75)
maker_min_edge = base
model_error_buffer = base
adverse_selection_buffer = base
ttl = base
size_multiplier = 1.0
quote_style = ImproveOneTick
```

Normal:

- current static params.

Runtime config:

```text
adaptive_regime_enabled = false
adaptive_regime_mode = paper_only
```

Live must reject adaptive regimes.

## 14. Regime Experiments

Command:

```bash
polyedge-rs research regimes \
  --input data/research/normalized \
  --markets data/research/markets.json \
  --fill-model touch_after_250ms \
  --profile-config research/configs/regime_profiles.yaml \
  --out reports/research/regime_profiles.json \
  --markdown reports/research/regime_profiles.md
```

Compare:

- static baseline
- dynamic safety-only
- dynamic quote-style
- full deterministic profile

Output:

- regime frequency/time share
- switches
- fills/cancels/PnL by regime
- PnL by day/market
- drawdown
- cancel/fill ratio
- static-vs-adaptive delta
- fill-model sensitivity
- warnings

## 15. Parameter Sweep

Command:

```bash
polyedge-rs research sweep \
  --input data/research/normalized \
  --markets data/research/markets.json \
  --search research/search_space.yaml \
  --split walk_forward \
  --max-experiments 500 \
  --out reports/research/parameter_sweep.json \
  --markdown reports/research/parameter_sweep.md
```

Search static and adaptive parameters.

Static ranges:

```text
maker_margin: 0.005,0.010,0.015,0.020,0.030,0.040
maker_min_edge: 0.005,0.010,0.015,0.020,0.030,0.050
model_error_buffer: 0.005,0.010,0.015,0.020,0.030,0.050
adverse_selection_buffer: 0,0.005,0.010,0.020,0.030
ttl: 1,2,5,10,20,30
final_no_trade: 30,60,90,120,180
sigma_floor: 0.20,0.40,0.60,0.80
ewma_lambda: 0.90,0.94,0.97,0.99
quote_style: ImproveOneTick,JoinBestBid,FairMinusMarginOnly
min_spread_ticks: 1,2,3,5
no_trade_distance_bps: 0,2,5,10,20
min_time_remaining: 60,120,180
max_time_remaining: 600,840,900
```

Use random/coarse search. Always include baseline. Default max experiments = 500.

Splits:

- days 1-3 train
- day 4 validation
- day 5 test
- also support leave-one-day-out

Never rank by test set.

Robust candidate requires:

- validation positive under at least two non-optimistic fill models
- test does not collapse
- drawdown acceptable
- cancel/fill ratio acceptable
- not concentrated in one day/hour
- no leakage flags

## 16. Calibration

Command:

```bash
polyedge-rs research calibration \
  --input data/research/normalized \
  --markets data/research/markets.json \
  --out reports/research/calibration.json \
  --markdown reports/research/calibration.md
```

q_up buckets:

```text
0.00-0.40
0.40-0.45
0.45-0.50
0.50-0.55
0.55-0.60
0.60-0.70
0.70-1.00
```

Report:

- avg q_up
- observed Up frequency
- calibration error
- Brier score
- log loss
- market/decision count
- fill/cancel count
- PnL if attributable

Group also by:

- time-to-expiry
- distance bps
- volatility regime
- spread bucket
- regime label

## 17. Optional Simple ML

Only after core research works.

Command:

```bash
polyedge-rs research ml-calibrate ...
```

Rules:

- no deep learning
- no LLM
- research-only
- no runtime deployment

Allowed:

- logistic regression
- isotonic calibration
- gradient boosted trees if dependency is acceptable

Targets:

- final Up/Down
- maker fill probability
- toxic fill / adverse selection

Evaluate out-of-sample only.

## 18. Sample Size

Command:

```bash
polyedge-rs research sample-size \
  --results reports/research/<result>.json \
  --out reports/research/sample_size.json \
  --markdown reports/research/sample_size.md
```

Use market-level net PnL.

Compute:

- n
- mean/median/std
- standard error
- 95% CI
- profitable/losing counts
- required N for +/- $0.05 precision
- required N for +/- $0.10 precision
- required N to detect observed mean:

```text
n ≈ 7.84 * (std_pnl / abs(mean_pnl))^2
```

Do not claim profitability if CI lower bound <= 0.

## 19. Final Report

Command:

```bash
polyedge-rs research report ...
```

Create:

```text
reports/research/final_strategy_research_report.json
reports/research/final_strategy_research_report.md
```

Sections:

1. Executive Summary
2. Data Coverage
3. Baseline Static Strategy
4. Fill Model Sensitivity
5. Regime-Conditioned Profiles
6. Parameter Sweep
7. Calibration
8. ML Experiments, if any
9. Statistical Evidence
10. Risks and Measurement Weaknesses
11. Recommendation
12. Next 10 Actions

Recommendation must choose exactly one:

1. Reject adaptive profiles
2. Keep adaptive profiles research-only
3. Enable adaptive profiles in paper mode only
4. Continue collecting data unchanged
5. Pause strategy
6. Consider tiny live maker-only later, if all live gates pass

## 20. Tests

Add tests for:

- audit fixtures
- malformed line handling
- market table correctness
- missing start/final prices
- no fill after cancel
- no fill after close
- no fill inside final window
- touch vs trade-through
- maker fee zero
- taker fee formula
- no future leakage
- regime priority
- hysteresis
- bounded effective params
- no size increase
- split logic
- calibration math
- report generation
- no secrets in reports

Required validation:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

If frontend touched:

```bash
npm --prefix frontend run typecheck
npm --prefix frontend run build
```

## 21. Final Codex Response

When done, report:

- modules/crates changed
- commands added
- report files generated
- tests run and results
- data audit summary
- baseline static result
- adaptive regime result
- fill model sensitivity
- calibration result
- sample-size conclusion
- known gaps
- exact next action

Do not include secrets.
Do not claim profitability unless supported.
Do not enable live trading.
