# PolyEdge Workbench Overhaul Implementation Plan

**Purpose:** turn the current PolyEdge dashboard into a fast, queryable, decision-oriented operations and research platform.

**Primary outcome:** a UI that lets an operator understand live state in seconds, investigate market/research behavior without one-off scripts, and manage Azure-native research automations safely.

**Non-negotiable safety rule:** this is a UI/data/research-platform overhaul only. Do not enable live trading, do not place real orders, do not weaken live gates, do not expose secrets, and keep adaptive strategies research-only or paper-only unless a separate live-readiness process explicitly approves them.

---

## 1. Current state summary

The current product has the right foundations:

- Rust PolyEdge backend with modular API and research lab.
- Next.js frontend with Dashboard, Markets, Reports, Labs, Data Quality, Jobs, Settings.
- Snapshot, chart, report, lab, data-quality, job, config, and control API wrappers.
- WebSocket/realtime event stream.
- Runtime paper fills, reports, lab outputs, prospective validation, and data-quality artifacts.

The current screenshots show an improved dashboard with health cards, an operator readiness panel, control panel, active market summary, large market probability chart, event timeline, decisions, and execution reports. The Reports and Labs pages now surface research outputs such as recommendation, static net PnL, dynamic quote result, confidence interval, data quality, fill-model sensitivity, prospective validation, and artifacts.

However, the product still feels like a set of panels rather than a complete command center. The main missing layer is a queryable analytical workbench.

---

## 2. Problems to solve

### 2.1 UX problems

1. **No top-level verdict.** The dashboard shows many values but does not clearly say whether the system is healthy, degraded, broken, collecting data, validating a strategy, or blocked.
2. **Research pages are too shallow.** Labs pages show candidate values and tables, but they do not explain why a recommendation exists or how evidence changed over time.
3. **Weak drilldowns.** A user cannot easily click from a report to the markets, fills, regimes, decisions, or data-quality events that produced it.
4. **Charts are informative but not explanatory.** They show time series but not decision markers, regime switches, quote events, fills, cancels, or toxicity annotations.
5. **Tables lack workbench functionality.** Research tables need sorting, filtering, column tooltips, row detail, export, and deep links.
6. **Too much blank space.** Sparse pages create the feeling that the system is incomplete, even when the data is available.
7. **`n/a` is overused.** Empty data should explain what is missing, why, and what action to take.

### 2.2 Data engineering problems

1. **No unified analytical query layer.** Data exists in raw blobs, reports, labs, and charts, but users cannot query it consistently.
2. **Expensive repeated scans.** Full research scans over hundreds of millions of events are too slow for interactive exploration.
3. **Insufficient query contracts.** Existing API wrappers expose many endpoints but not a safe, generic query interface over curated datasets.
4. **No first-class saved views.** Common queries like toxic fills, calibration failures, losing regimes, or data gaps should be reusable.
5. **Data quality is not integrated into every report.** Research outputs should be visually marked clean, partial, contaminated, excluded, or unknown.

### 2.3 Operations problems

1. **Jobs need better observability.** Job list should show state, duration, input window, output artifacts, logs, warnings, and rerun controls.
2. **Azure automation status should be visible.** Freshness checks, quality audits, daily reports, prospective validation, and replay index builds should be visible and linked to artifacts.
3. **Data quality alerts need UI presence.** Missing blobs, tiny blobs, stale latest blob, recorder drops, and job failures should become visible UI conditions.

---

## 3. Target product: PolyEdge Workbench

The product should become four coordinated surfaces:

1. **Operations Cockpit** — live safety, market, execution, and recorder state.
2. **Research Lab** — strategy evidence, candidate comparison, calibration, fill-model sensitivity, sample size, and artifacts.
3. **Data Explorer** — query and inspect curated datasets across markets, decisions, fills, regimes, reports, jobs, and data quality.
4. **Automation & Governance** — Azure job status, data freshness, exclusion registry, report generation, audit trail, and settings.

---

## 4. Information architecture

### 4.1 Top navigation

Recommended top nav:

```text
Dashboard | Markets | Research | Explore | Data Quality | Jobs | Settings
```

Notes:

- Rename **Labs** to **Research** or keep Labs but introduce a top-level **Explore** page.
- Keep **Reports** if it remains an artifact/report viewer, but avoid overlap with Research.
- Data Quality should be a dedicated operational page, not hidden inside Labs.

### 4.2 Operations Dashboard structure

```text
Current Verdict Banner
System Health Cards
Operator Readiness
Control Panel
Active Market + Main Chart
Secondary Charts
Decision Timeline + Event Timeline
Execution Reports
```

#### Current Verdict Banner

Full-width top banner:

```text
Mode: PAPER
System: HEALTHY
Trading: OBSERVING / NO EDGE / QUOTING / PAUSED
Data: CLEAN / DEGRADED / STALE / CONTAMINATED
Research: VALIDATING_DYNAMIC_QUOTE_STYLE
Next action: Continue collecting clean data
```

Severity levels:

```text
healthy | warning | danger | unknown
```

#### System Health Cards

Group cards into four logical clusters:

```text
Safety
  mode
  kill switch
  live gates
  pause state

Data
  recorder health
  latest blob age
  dropped/error count
  data quality status

Market
  active market
  reference price
  reference age
  distance from start

Execution
  open orders
  latest decision
  runtime paper PnL
  paper fills
```

#### Active Market

Left panel should show only current facts:

```text
Market title
start/end
time remaining
start price
current Chainlink price
reference age
bps distance from start
q_up/q_down
market status
bot action / latest reason
```

#### Market chart

Main chart should include toggles:

```text
q_up/q_down
UP bid/ask
DOWN bid/ask
fills
decisions
regime switches
reference distance
```

Add vertical markers:

```text
market start
market end
quote placed
fill
cancel
regime switch
paper settlement
feed error
```

#### Event Timeline

Default should be decision-oriented, not raw-event-oriented.

Tabs:

```text
Highlights | Orders | Market Data | Errors | Raw
```

Features:

```text
pause/resume
clear local buffer
search
severity filter
expand raw JSON
copy event
open related market
```

---

## 5. Research Lab 2.0

### 5.1 Research overview page

Primary element: **Candidate Evidence Matrix**.

Columns:

```text
candidate
status
latest test PnL
CI low/high
max drawdown
fill model agreement
data quality
recommendation
last updated
```

Candidate rows:

```text
static_baseline
dynamic_quote_style
full_deterministic_profile
dynamic_safety_only
```

Each row opens a candidate detail drawer:

```text
why this status
PnL by day
PnL by fill model
PnL by regime
market count
known warnings
linked artifacts
```

### 5.2 Prospective validation page

Must show:

```text
cumulative PnL by candidate
daily PnL bars
market count by day
drawdown trend
confidence interval trend
fill model selector
quality flags
recommendation history
```

### 5.3 Regime profiles page

Must answer:

```text
Which regimes lose money?
Which regimes are skipped?
Which regimes produce fills?
Which regimes trigger cancels?
Which regime switches are too frequent?
```

Charts:

```text
PnL by regime
fills by regime
skipped orders by regime
cancel/fill ratio by regime
regime time share
regime transition matrix
```

### 5.4 Calibration page

Must answer:

```text
Is q_up calibrated?
Where is the model overconfident?
Does calibration differ near expiry?
Does calibration differ by distance from start?
```

Charts:

```text
q bucket observed vs predicted
Brier score by day
log-loss by day
calibration by time-to-expiry
calibration by distance bucket
```

### 5.5 Fill models page

Must answer:

```text
Does the strategy only win under optimistic fill assumptions?
```

Charts:

```text
net PnL by fill model
fill count by model
drawdown by model
cancel/fill by model
static vs dynamic by fill model
```

### 5.6 Sample size page

Must answer:

```text
Do we have enough evidence?
How many more settled markets are needed?
```

Display:

```text
n markets
mean market PnL
std dev
standard error
95% CI
required N for ±$0.05
required N for ±$0.10
required N to detect observed mean
```

### 5.7 Artifacts page

Artifact table:

```text
path
kind
date
size
modified
quality
source job
preview
download
copy path
```

Add preview support for:

```text
json
markdown
csv
small parquet metadata
```

---

## 6. Data Explorer

Add a new page:

```text
/explore
```

### 6.1 Datasets

Curated datasets:

```text
markets
decisions
fills
regimes
reports
data_quality
jobs
artifacts
calibration
fill_models
sample_size
```

### 6.2 Query builder

Filters:

```text
date range
market_id
asset
horizon
event_type
candidate
fill_model
regime
outcome
quality flag
PnL range
q_up range
time-to-expiry bucket
```

Group-by:

```text
date
hour
candidate
fill_model
regime
outcome
time bucket
q bucket
```

Metrics:

```text
count
net_pnl
avg_pnl
fill_count
cancel_count
fill_rate
cancel_per_fill
mean_q_up
observed_up_rate
brier_score
max_drawdown
```

Output modes:

```text
table
bar chart
line chart
scatter
CSV export
JSON export
saved query
```

### 6.3 Query templates

Default saved templates:

```text
Toxic fills
Losing regimes
High q_up but Down won
Final-window activity
Data-quality exclusions
Dynamic beats static
Calibration failures
Markets with missing start price
Markets with large drawdown
Regime switch storms
```

### 6.4 Query backend

Implement adapter interface:

```rust
trait QueryBackend {
    fn schema(&self) -> QuerySchema;
    async fn run(&self, request: QueryRequest) -> QueryResult;
}
```

Backends:

```text
ReportBackedQueryBackend   // initial fast implementation from JSON artifacts
DuckDbParquetQueryBackend  // local/offline
AdxQueryBackend            // Azure Data Explorer/Kusto, optional/pro path
```

The frontend must only send structured query requests. Do not expose arbitrary KQL/SQL execution to the UI in v1.

---

## 7. Azure-native automations

Use Azure Container Apps Jobs for finite batch tasks. Jobs should be scheduled or manual depending on task type.

### 7.1 Required jobs

```text
polyedge-freshness-check
polyedge-hourly-quality-audit
polyedge-daily-research-report
polyedge-prospective-validation
polyedge-compact-replay-index
polyedge-chart-backfill
polyedge-adx-ingestion
polyedge-manual-backfill
```

### 7.2 Job schedule recommendations

```text
freshness-check: every 5 minutes
hourly-quality-audit: every hour + 10 minutes
daily-research-report: daily 00:30 UTC
prospective-validation: daily 01:15 UTC
compact-replay-index: daily 02:00 UTC
chart-backfill: manual
adx-ingestion: hourly or daily depending cost
manual-backfill: manual
```

### 7.3 Job output contracts

Each job must write:

```text
reports/jobs/{job_id}.json
reports/jobs/{job_id}.md if useful
reports/jobs/latest/{job_type}.json
logs/job summaries if supported
```

Job JSON fields:

```json
{
  "job_id": "...",
  "job_type": "daily-research-report",
  "status": "completed",
  "started_ts": "...",
  "finished_ts": "...",
  "duration_seconds": 123,
  "input_window": { "start": "...", "end": "..." },
  "artifacts": [],
  "warnings": [],
  "errors": [],
  "data_quality": "healthy"
}
```

### 7.4 Azure Monitor alerts

Alerts:

```text
no_new_blob_for_3_minutes
tiny_blob_anomaly
hour_missing_minute_blobs
recorder_unrecovered_durable_events_gt_0
recorder_flush_unrecovered_true
recorder_dropped_count_gt_0
job_failed
job_duration_too_long
adx_ingestion_failed
```

---

## 8. Backend API additions

Add or verify:

```text
GET  /api/v1/query/schema
POST /api/v1/query/run
GET  /api/v1/query/templates
POST /api/v1/query/templates
GET  /api/v1/labs/summary
GET  /api/v1/labs/candidates
GET  /api/v1/labs/candidates/{candidate}
GET  /api/v1/data-quality/timeline
GET  /api/v1/jobs
GET  /api/v1/jobs/{job_id}
GET  /api/v1/jobs/{job_id}/logs
GET  /api/v1/artifacts/{artifact_id}/preview
```

Existing API wrappers should stay compatible.

---

## 9. Frontend architecture

Refactor into feature modules:

```text
frontend/src/features/operations/
frontend/src/features/research/
frontend/src/features/data-explorer/
frontend/src/features/jobs/
frontend/src/features/settings/
frontend/src/shared/ui/
frontend/src/shared/charts/
frontend/src/shared/api/
frontend/src/shared/query/
```

### 9.1 State

Use focused stores/hooks:

```text
useOperationStore
useResearchStore
useQueryStore
useJobStore
useUiPreferencesStore
```

### 9.2 Deep links

Support URLs like:

```text
/labs?date=2026-06-15&candidate=dynamic_quote_style&fill=touch_after_250ms
/explore?dataset=fills&regime=near_strike&from=...&to=...
/markets/{market_id}?tab=fills
/jobs/{job_id}
```

### 9.3 Tables

All important tables need:

```text
sort
filter
search
sticky header
pagination or virtualization
CSV export
row detail drawer
column help tooltips
```

### 9.4 Performance rules

```text
Do not render every websocket event.
Coalesce high-frequency events.
Use chart downsampling.
Use paginated/virtualized tables.
Cache query results.
Keep charts animation-free for live data.
Use no-store only when truly needed.
```

---

## 10. Design system rules

### 10.1 Visual hierarchy

```text
Level 1: current verdict / alert banner
Level 2: KPI cards
Level 3: primary chart/table
Level 4: supporting details
Level 5: raw/debug
```

### 10.2 Color semantics

```text
green = healthy / positive
red = danger / negative
amber = warning / inconclusive
blue = informational
gray = inactive / unavailable
```

### 10.3 Formatting

```text
Money: -$13.35
PnL: +$16.20
Probability: 51.0%
Share price: 0.48
Bps: +3.5 bps
CI: [-0.14, +0.18]
UTC time: 04:15 UTC
```

### 10.4 Empty states

Bad:

```text
n/a
```

Good:

```text
No calibration report for this date.
Run the daily research report or select another date.
[Build Report]
```

---

## 11. Implementation phases

### Phase 1 — UX cleanup

- Current Verdict banner.
- Better health groups.
- Standard number formatting.
- Better empty states.
- Chart toggles.
- Market event markers.
- Labs candidate matrix.

### Phase 2 — Data Explorer MVP

- Query schema endpoint.
- Query run endpoint.
- Query builder UI.
- Curated datasets.
- Saved templates.
- CSV export.

### Phase 3 — Jobs/Data Quality console

- Jobs table.
- Job detail page.
- Job logs/summary.
- Data quality timeline.
- Freshness/tiny-blob panels.
- Rerun controls.

### Phase 4 — Labs 2.0

- Candidate detail pages.
- Fill-model sensitivity drilldowns.
- Regime analysis.
- Calibration explorer.
- Sample-size evidence dashboard.
- Artifact previewer.

### Phase 5 — Azure automation/IaC

- Container Apps Jobs definitions.
- Schedules.
- Job environment variables.
- Azure Monitor alert definitions.
- Dashboard links to job outputs.

### Phase 6 — Performance and polish

- Virtualized tables.
- Cached queries.
- Downsampled charts.
- Improved WebSocket throttling.
- Frontend bundle/performance audit.

---

## 12. Acceptance criteria

### Safety

- Live trading remains disabled.
- No live gates weakened.
- No secrets exposed.
- UI cannot enable live.
- Backend rejects unsafe control actions.

### UX

- Dashboard communicates system verdict in under 10 seconds.
- Research pages explain recommendation and evidence.
- Data Explorer answers common research questions without custom scripts.
- Jobs and data-quality status are visible and actionable.
- Raw JSON is never the default view.

### Data

- Query API supports markets, decisions, fills, regimes, reports, data quality, jobs.
- Curated datasets are documented.
- Query results are paginated/limited.
- Reports link to data quality and artifacts.

### Engineering

- Rust tests pass if backend touched.
- Frontend typecheck/build pass.
- New UI model/query functions have tests.
- Existing API contracts remain compatible.
- Generated artifacts are ignored by git.

---

## 13. Codex implementation prompt

```text
You are Codex in the PolyEdge repo. Read and follow docs/polyedge-workbench-overhaul-plan.md exactly.

Goal: implement the next-generation PolyEdge Workbench: a faster, clearer, queryable operations and research UI with Azure-native job/data-quality visibility.

Hard rules: do not enable live trading, do not place orders, do not weaken live gates, do not expose secrets, do not change trading strategy logic, and keep live gates backend-only.

Implement in phases:
1. Operations dashboard overhaul: Current Verdict banner, grouped health cards, improved empty states, standardized formatting, chart toggles, market event markers.
2. Research Labs 2.0: candidate evidence matrix, drilldowns for prospective validation, regimes, calibration, fill models, sample size, artifacts, and explicit recommendation explanations.
3. Data Explorer: /api/v1/query/schema, /api/v1/query/run, query templates, frontend query builder, curated datasets, filters, group-by, metrics, table/chart output, CSV export.
4. Jobs/Data Quality console: job list, job detail/log views, data-quality timeline, freshness/tiny-blob panels, rerun controls, and artifact links.
5. Azure automations/IaC: Container Apps Jobs for freshness checks, hourly quality audits, daily reports, prospective validation, replay index builds, chart backfills, and alert definitions.
6. Performance: virtualized tables, cached queries, chart downsampling, event coalescing.

Acceptance: frontend typecheck/build passes, Rust tests pass if backend touched, dashboard is understandable without raw JSON, Research pages explain evidence, Data Explorer can query markets/fills/decisions/reports, jobs are visible/actionable, and no live trading capability is added.

When done, report implemented files, commands/endpoints added, screenshots or descriptions of changed pages, test results, known gaps, and next recommended action.
```
