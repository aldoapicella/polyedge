# PolyEdge Remaining Work: Post-Workbench Implementation Plan

**Scope:** This document replaces earlier broad roadmaps. It assumes the Rust backend, research CLI, Workbench frontend, Labs pages, Data Explorer, Azure jobs, alerts, and dashboard shell already exist. It keeps only the remaining work and next implementation steps.

**Hard safety rule:** no live trading work is allowed in this milestone. Everything here is research-only, paper-only, or observability.

---

## 1. Current State: Already Implemented / Verified

Based on the current repository structure and latest workbench commit, these are no longer roadmap items; they already exist and should be hardened rather than reimplemented.

### 1.1 Rust backend and research CLI

Implemented or present:

- Rust workspace.
- `polyedge-rs` CLI.
- Research subcommands:
  - `audit`
  - `normalize`
  - `build-markets`
  - `replay`
  - `baseline`
  - `regimes`
  - `sweep`
  - `calibration`
  - `sample-size`
  - `report`
  - `ml-calibrate`
  - `azure-freshness`
  - `validate-prospective`
  - `build-replay-index`
  - `backfill`
- Fill models:
  - `no_maker_fills`
  - `touch`
  - `touch_after_250ms`
  - `touch_after_1000ms`
  - `trade_through`
  - `queue_proxy` placeholder/gated model
  - `adverse_selection_penalized`
- Exclusion-window support.
- Frozen-candidate/prospective-validation support.
- Backfill and replay-index commands.

### 1.2 API / Workbench backend

Implemented or present:

- `/api/v1/labs/*`
- `/api/v1/query/schema`
- `/api/v1/query/run`
- `/api/v1/query/templates`
- `/api/v1/data-quality/timeline`
- `/api/v1/jobs`
- `/api/v1/jobs/{job_id}`
- `/api/v1/jobs/{job_id}/logs`
- `/api/v1/artifacts/{artifact_id}/preview`
- Report-backed query backend with safe structured query model.
- Labs endpoints for reports, candidates, artifacts, data quality, prospective validation, regimes, calibration, sample size, fill models, and jobs.

### 1.3 Frontend Workbench

Implemented or present:

- Login page.
- Dashboard page.
- Markets page.
- Reports page.
- Research/Labs pages.
- Data Quality page.
- Jobs page.
- Settings/config page.
- Explore/Data Explorer page.
- Dashboard health cards.
- Event Timeline with tabs and raw JSON drawer.
- Market charts.
- Query builder.
- Query templates.
- Artifact browser/preview support.
- CSV export helper.
- Virtual table component.

### 1.4 Azure infrastructure

Implemented or present in IaC:

- Container App.
- Frontend sidecar/container support.
- Storage account, Blob container, Azure Tables.
- Managed identity.
- Research Container Apps Jobs:
  - `freshness-check`
  - `hourly-quality-audit`
  - `daily-research-report`
  - `prospective-validation`
  - `compact-replay-index`
  - `chart-backfill`
  - `adx-ingestion`
  - `manual-backfill`
- Azure Monitor action group.
- Metric alerts for blob ingress/transactions.
- Log alerts for:
  - no new blob
  - tiny blob anomaly
  - missing minute blobs
  - recorder failure
  - recorder drops
  - job failure
  - long job duration
  - ADX ingestion failure

---

## 2. What Is NOT Fully Implemented Yet

These are the remaining implementation priorities.

---

# P0 — QueueProxy Evidence Pipeline and Shadow Fill Models

## Why this is P0

`queue_proxy` exists as a fill model label, but it is not yet decision-grade. The existing live feed converts CLOB market-channel events into `BookState`, but it does **not** preserve enough raw event evidence for true queue simulation:

- raw `price_change`
- raw `last_trade_price`
- trade size
- trade side semantics
- level depletion
- size at quote price before placement
- cancellation/depletion vs trade distinction

Without this, QueueProxy can only be a placeholder or skipped model.

## Required outcome

Implement **QueueProxy research/shadow activation**.

It must never be used for live decisions. It must only be used in research reports and Labs.

## Tasks

### 2.1 Preserve raw market-channel events

Add raw market-channel event support.

Suggested domain type:

```rust
pub enum MarketChannelEvent {
    BookSnapshot { raw: serde_json::Value, ... },
    PriceChange { raw: serde_json::Value, ... },
    LastTradePrice { raw: serde_json::Value, ... },
    BestBidAsk { raw: serde_json::Value, ... },
    TickSizeChange { raw: serde_json::Value, ... },
    MarketResolved { raw: serde_json::Value, ... },
    Unknown { raw: serde_json::Value, ... },
}
```

Extend feed events:

```rust
FeedEvent::RawMarketEvent(MarketChannelEvent)
```

Recorder must persist both:

```text
1. raw_market_event
2. normalized_book/book_update_summary
```

Do not replace existing book recording. Add raw evidence in parallel.

### 2.2 Normalize queue evidence

Add normalized outputs:

```text
raw_market_events.jsonl.gz
price_changes.jsonl.gz
last_trades.jsonl.gz
book_snapshots.jsonl.gz
level_changes.jsonl.gz
```

Required fields:

```text
event_type
recorded_ts
source_ts/exchange_ts
market_id if known
condition_id if known
token_id/asset_id
side
price
size
best_bid
best_ask
book_hash
raw_payload
```

### 2.3 Queue evidence audit

Add command:

```bash
polyedge-rs research queue-audit \
  --input data/research/normalized \
  --markets data/research/markets.json \
  --out reports/research/queue_evidence_audit.json \
  --markdown reports/research/queue_evidence_audit.md
```

Report:

```text
total_markets
queue_proxy_eligible_markets
queue_proxy_ineligible_markets
eligibility_rate
book_snapshot_count
price_change_count
last_trade_price_count
best_bid_ask_count
market_resolved_count
events_by_day
events_by_market
events_by_token
markets_with_trade_events
markets_with_price_change_events
markets_with_full_book_snapshots
markets_with_usable_order_lifecycle
ineligible_reasons
coverage_warnings
```

Eligibility rules:

```text
Eligible only if market has:
- start/final truth
- book snapshots
- price_change or full level updates
- last_trade_price/trade-size evidence
- simulated order lifecycle timestamps
```

If not eligible, QueueProxy must explicitly skip the market with a reason.

### 2.4 Implement QueueProxy models

Add:

```rust
FillModel::QueueProxyConservative
FillModel::QueueProxyBalanced
```

#### Conservative

For maker BUY at price `P`:

```text
size_ahead = visible bid size at P at order_live_ts
own_remaining = order_size
```

Only trade prints can consume queue:

```text
if trade.token == order.token
and trade.price <= P
and trade.ts between order_live_ts and cancel_ts:
    trade_size first reduces size_ahead
    any leftover fills own order
```

Cancellations do **not** reduce `size_ahead`.

#### Balanced

Same as conservative, but non-trade level-size decreases may reduce `size_ahead`.

Still, only actual trade prints can fill your order.

### 2.5 QueueProxy report fields

Add to replay/baseline/regime reports:

```text
queue_proxy_enabled
queue_proxy_mode
queue_proxy_eligible_markets
queue_proxy_ineligible_markets
queue_proxy_eligibility_rate
queue_proxy_fills
queue_proxy_partial_fills
queue_proxy_fill_rate
avg_size_ahead
p50_size_ahead
p95_size_ahead
queue_vs_touch_fill_delta
queue_vs_trade_through_fill_delta
queue_proxy_net_pnl
ineligible_reasons
```

### 2.6 Labs UI

Add Labs tab:

```text
QueueProxy / Fill Realism
```

Show:

```text
eligibility rate
eligible markets
ineligible reasons
touch vs trade_through vs queue_proxy
PnL by fill model
size-ahead distribution
queue fill rate
warnings
```

---

# P1 — Make Azure Jobs Operational, Not Merely Defined

The Bicep defines jobs, and API can list/start them, but some jobs are still placeholders or not fully integrated.

## Remaining tasks

### 3.1 Real job logs

Current `job_logs` endpoint returns identity and artifact hints, but not real logs.

Add:

```text
GET /api/v1/jobs/{job_id}/executions
GET /api/v1/jobs/{job_id}/executions/{execution_id}/logs
```

Use Azure Container Apps Job executions and Log Analytics.

Show in UI:

```text
execution_id
status
start/end
duration
exit_code
stderr/stdout snippets
artifact paths
error summary
```

### 3.2 Chart backfill

Currently `chart-backfill` is defined as pending CLI.

Implement actual command:

```bash
polyedge-rs research chart-backfill \
  --input <event prefix or normalized path> \
  --out <chart store/report>
```

### 3.3 ADX ingestion

Currently `adx-ingestion` is defined as pending pipeline.

Implement one of:

- ADX ingestion path, or
- mark ADX as not configured and hide “run” control unless config exists.

If ADX is implemented, add:

```text
ADX cluster/database config
KQL table schemas
ingestion mappings
incremental ingestion state
```

### 3.4 Job success verification

Each job must write a structured artifact:

```json
{
  "job_id": "...",
  "status": "completed|failed",
  "started_ts": "...",
  "finished_ts": "...",
  "duration_seconds": 123,
  "input_window": "...",
  "artifacts": [],
  "warnings": [],
  "errors": [],
  "live_trading_enabled": false,
  "raw_data_mutated": false
}
```

The UI should show job artifact status, not just job definition status.

---

# P1 — Prospective Validation Hardening

The prospective-validation UI and API exist, but the process must be made stricter.

## Tasks

### 4.1 Frozen candidates are immutable during validation

Add or enforce:

```text
research/configs/frozen_candidates.yaml
```

Track:

```text
candidate name
strategy profile
hash of config
created_at
frozen_since
reason
```

If candidate config changes, validation must start a new candidate version.

### 4.2 Paired comparison metrics

For each settled market:

```text
D_i = PnL_candidate_i - PnL_static_i
```

Report:

```text
mean_D
std_D
SE_D
95% CI for D
required_n_to_detect_mean_D
daily paired delta
paired drawdown
```

### 4.3 Decision gates

Add clear gating:

```text
REJECT
RESEARCH_ONLY
PAPER_SHADOW_OK
PAPER_RUNTIME_OK
TINY_LIVE_LATER
```

Rules:

```text
PAPER_SHADOW_OK only if:
- clean data
- positive out-of-sample candidate PnL
- positive paired improvement
- non-optimistic fill model support
- no single day dominates
- CI lower bound improving
```

No live gate should be added.

---

# P1 — Query/Data Explorer Hardening

The Data Explorer exists, but it is report-backed and not yet a true large-scale query layer.

## Tasks

### 5.1 Persist saved queries

`POST /api/v1/query/templates` currently returns `persisted: false`.

Implement saved query persistence:

```text
reports/query_templates/*.json
or Azure Table PolyEdgeQueryTemplates
```

Fields:

```text
id
name
description
request
created_ts
updated_ts
owner
tags
```

### 5.2 Query audit log

Every query run should write:

```text
query_id
ts
dataset
filters
group_by
metrics
limit
duration_ms
returned_rows
source
error
```

No secrets.

### 5.3 Real analytical backend

Current query backend is `ReportBackedQueryBackend`.

Add pluggable backends:

```rust
enum QueryBackendKind {
    ReportBacked,
    ParquetDuckDb,
    AzureDataExplorer,
}
```

Minimum next step:

- implement Parquet/DuckDB or normalized JSONL-backed historical query for full historical market/fill/decision datasets.
- leave ADX adapter behind config if not ready.

### 5.4 Better datasets

Add curated datasets:

```text
market_truth
decision_features
fill_candidates
queue_evidence
queue_proxy_results
prospective_daily
candidate_market_pnl
regime_market_pnl
calibration_buckets
```

---

# P1 — Workbench UX: Make It a Decision System

The UI now has pages, but the next step is making them decision-oriented.

## Tasks

### 6.1 Dashboard “Current Verdict”

Add a strong top banner:

```text
System: Healthy / Degraded / Broken
Trading: Paper / Observing / Paused / No Edge
Data: Clean / Stale / Contaminated
Research: Continue collecting / Candidate under validation / Reject static
Next action: ...
```

### 6.2 Labs evidence pages

Improve pages to answer:

```text
Why is this recommendation shown?
What changed vs yesterday?
Which fill models agree?
Which candidate is improving?
What is blocking live?
How much more data is needed?
```

### 6.3 Jobs page

Add:

```text
job execution drilldown
logs
artifacts
rerun button
copy command
source input window
quality status
```

### 6.4 Data Quality page

Add:

```text
freshness timeline
tiny-blob anomaly timeline
minute-blob completeness heatmap
exclusion window validation
latest good hour/day
```

### 6.5 Markets page

Add drilldowns:

```text
market truth
q path
book path
decision timeline
fills
regime state
settlement
data quality
```

---

# P2 — Research Performance and Incremental Indexing

The research lab works, but it must get faster.

## Tasks

### 7.1 Compact replay index

Ensure `build-replay-index` is fully used by:

```text
baseline
regimes
sweep
calibration
prospective validation
```

Index should include:

```text
market_truth
decision_time_features
book_touch_events
trade_events
order_lifecycle
settlement_labels
regime_features
```

### 7.2 Incremental daily processing

Do not rescan historical data daily.

Daily job should only process yesterday’s data, update cumulative indices, and produce daily + cumulative reports.

### 7.3 Performance targets

```text
hourly audit < 5 minutes
daily report < 45 minutes
single fill-model replay < 10 minutes per day
prospective validation < 10 minutes
query page response < 2 seconds for curated reports
```

---

# P2 — ML / Calibration Research Only

Do not implement ML into runtime.

Next ML work should be limited to:

```text
probability calibration
fill probability
toxic fill / adverse selection gate
```

Outputs:

```text
Brier score
log loss
calibration curve
feature importance
out-of-sample PnL if used as gate
```

No deep learning, no LLM trading decisions.

---

# 3. Multi-Agent Implementation System

Use multi-agent execution if Codex supports it. If not, emulate these agents as sequential passes in separate worktrees/branches.

## Agent 1 — Safety and Release Coordinator

Responsibilities:

- Enforce no live trading.
- Check live gates unchanged.
- Check no secrets printed.
- Review every PR before merge.
- Maintain acceptance checklist.
- Run final tests.
- Produce final release summary.

Must run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
npm --prefix frontend run typecheck
npm --prefix frontend run build
```

## Agent 2 — QueueProxy / Market Evidence Agent

Responsibilities:

- Raw market-channel event recording.
- Queue evidence audit.
- QueueProxy conservative/balanced models.
- QueueProxy tests.
- Fill Realism Labs UI.

Research tasks:

- Re-read Polymarket CLOB market-channel docs.
- Validate exact semantics of `last_trade_price.side`.
- Verify whether historical raw blobs contain enough queue evidence.

## Agent 3 — Azure Automation Agent

Responsibilities:

- Confirm deployed jobs match IaC.
- Implement missing job commands.
- Add actual job logs.
- Harden alerts.
- Validate artifacts.

Research tasks:

- Re-check Azure Container Apps Jobs docs.
- Re-check Azure Monitor alert/log query docs.
- Confirm scheduled cron behavior is UTC.

## Agent 4 — Prospective Validation / Quant Agent

Responsibilities:

- Frozen candidate versioning.
- Paired improvement math.
- Gate recommendations.
- Sample-size updates.
- Report interpretation.

Research tasks:

- Re-validate statistical formulas.
- Review whether current sample is enough for each candidate.
- Confirm no future leakage.

## Agent 5 — Query/Data Explorer Agent

Responsibilities:

- Saved query persistence.
- Query audit logs.
- Query backend plugin interface.
- Historical/Parquet/ADX-backed datasets.
- Data Explorer UI improvements.

Research tasks:

- Evaluate ADX vs DuckDB/Parquet tradeoff.
- Ensure query API cannot execute arbitrary SQL/KQL from the browser.

## Agent 6 — Workbench UI/UX Agent

Responsibilities:

- Current Verdict banner.
- Labs evidence pages.
- Data Quality timeline.
- Jobs detail/logs.
- Markets drilldowns.
- Better empty states and copy.

Research tasks:

- Review screenshots after each change.
- Conduct “operator task” walkthrough:
  - Is data healthy?
  - What is the bot doing?
  - Which candidate is best?
  - What failed?
  - What should I do next?

## Agent 7 — Validator / Red Team Agent

Responsibilities:

- Try to break assumptions.
- Check QueueProxy for fake fills.
- Check query API safety.
- Check no leaked secrets.
- Check no live activation path.
- Check reports for overstated profitability.

This agent must disagree with the implementation if evidence is weak.

## Agent 8 — Documentation Agent

Responsibilities:

- Update docs only after implementation.
- Maintain:
  - architecture docs
  - operator runbooks
  - research methodology
  - QueueProxy assumptions
  - query schema docs
  - Azure jobs docs

---

# 4. Iterative Process

Each agent must follow this loop:

```text
1. Inspect current code and docs.
2. Write a short implementation plan.
3. Identify assumptions.
4. Implement the smallest safe slice.
5. Add tests.
6. Run local validation.
7. Self-review.
8. Ask another agent/pass to review.
9. Fix findings.
10. Update docs.
11. Produce a concise status report.
```

Every status report must include:

```text
done
not done
tests run
risks
evidence
next action
```

---

# 5. Updated Codex Goal Prompt

Use this short prompt and attach/reference this document:

```text
You are Codex in the PolyEdge repo. Read docs/polyedge-remaining-implementation-plan.md and implement only the remaining items. Do not reimplement already-completed Workbench, Labs, Query API, Azure jobs, or Rust research CLI unless hardening is explicitly required.

Goal: finish the next PolyEdge milestone: QueueProxy evidence/shadow activation, operational Azure job/log validation, prospective-validation hardening, query backend persistence/scalability, and Workbench decision UX improvements.

Hard rules: do not enable live trading, do not place orders, do not weaken live gates, do not expose secrets, do not mutate raw data, and keep QueueProxy/adaptive profiles research-only or paper-only.

Use multi-agent/worktree execution if available:
1. Safety/release coordinator
2. QueueProxy evidence agent
3. Azure automation agent
4. Prospective validation/quant agent
5. Query/Data Explorer agent
6. Workbench UI/UX agent
7. Validator/red-team agent
8. Documentation agent

Each agent must iteratively inspect, research, implement, test, self-review, and cross-validate. If multi-agent support is unavailable, emulate the agents as sequential passes.

Acceptance:
- cargo fmt/clippy/test pass
- frontend typecheck/build pass
- QueueProxy refuses ineligible markets
- QueueProxy conservative/balanced models are tested
- Azure job logs/artifacts are visible
- prospective validation uses frozen candidate versions and paired improvement
- query templates persist and query runs are audited
- UI shows current verdict, evidence, job details, data-quality timeline, and fill realism
- no live trading path is added

When done, report implemented files, endpoints, commands, tests, screenshots/descriptions, remaining risks, and next action.
```

---

# 6. Research and Source Links

Useful source links to keep in docs:

- Polymarket CLOB market WebSocket docs: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
- Polymarket fees docs: https://docs.polymarket.com/trading/fees
- Azure Container Apps Jobs: https://learn.microsoft.com/en-us/azure/container-apps/jobs
- Azure Monitor alerts overview: https://learn.microsoft.com/en-us/azure/azure-monitor/alerts/alerts-overview
- Azure Blob Storage monitoring reference: https://learn.microsoft.com/en-us/azure/storage/blobs/monitor-blob-storage-reference
- Azure Data Explorer overview: https://learn.microsoft.com/en-us/azure/data-explorer/data-explorer-overview
- Polymarket microstructure paper: https://arxiv.org/abs/2604.24366
- Prediction market dataset paper: https://arxiv.org/abs/2604.20421
