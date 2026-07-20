# PolyEdge Azure Automation + Research Dashboard Implementation Plan

**Purpose:** implement Azure-native automations and frontend dashboard updates so PolyEdge can continuously collect clean paper-mode data, detect corrupted data quickly, run daily offline research, and display research/lab results in the control plane.

**Target repo:** `https://github.com/aldoapicella/polyedge/tree/main`

**Status from current investigation:**  
The recorder is alive and paper-mode data collection should continue unchanged. The static maker-first strategy is currently negative across executable maker fill assumptions, while `dynamic_quote_style` is promising but unproven and must stay research-only/paper-only until future clean data confirms it. The contaminated PUT-bug window must remain excluded from clean evaluation.

---

## 1. Non-negotiable safety boundaries

These are requirements for every automation, backend endpoint, and frontend control:

- Keep `EXECUTION_MODE=paper`.
- Keep `ALLOW_LIVE=false`.
- Keep taker trading disabled unless an offline research command explicitly simulates takers.
- Do not weaken live gates.
- Do not expose secrets in logs, reports, Azure tables, blobs, dashboards, or API responses.
- Do not mutate raw event blobs.
- Do not let frontend controls enable live trading, adaptive live mode, emergency account cancel, wallet configuration, private keys, bearer tokens, or Azure credentials.
- Adaptive profiles remain research-only or paper-only and disabled by default.
- QueueProxy remains skipped until queue depletion and trade-print evidence are recorded and validated.
- The June 11/12 PUT-bug exclusion window must be enforced by default in clean research runs.

---

## 2. Why this implementation is needed

The investigation found:

- Azure Portal capacity display lag made the storage account look stuck, but direct blob writes, blob-service ingress, transactions, blob counts, and recorder metrics showed the recorder was alive.
- The window `2026-06-11T10:00:00Z..2026-06-12T22:00:00Z` is contaminated by incomplete tiny blobs and must be excluded.
- After excluding that window, the clean sample had 793 settled markets.
- Static maker-first lost across executable maker fill models.
- `dynamic_quote_style` was positive in research, but it was selected after observing the dataset and therefore requires future out-of-sample validation.
- The independent sample is market-level, not event-level. Hundreds of millions of book events do not equal hundreds of millions of independent strategy observations.

Therefore the immediate goal is not more tuning. The goal is a reliable daily research factory.

---

## 3. Azure automation architecture

Use Azure Container Apps Jobs for finite-duration batch tasks. Jobs can be manual, scheduled, or event-driven; scheduled jobs use cron expressions evaluated in UTC. Use Azure Monitor alerts for liveness/data-quality alerts and action groups for notifications or workflows. Blob Storage exposes metrics such as `Ingress`, `Transactions`, `BlobCount`, `BlobCapacity`, and `UsedCapacity`, but direct latest-blob checks must remain the primary freshness signal.

### 3.1 Azure resources

Recommended resources:

```text
Container App:
  polyedge-dev                    # existing long-running paper recorder/API

Container Apps Jobs:
  polyedge-data-freshness-job      # scheduled every 5 minutes
  polyedge-hourly-quality-job      # scheduled hourly
  polyedge-daily-research-job      # scheduled daily after UTC day completion
  polyedge-prospective-job         # scheduled daily, frozen candidates only
  polyedge-replay-index-job        # scheduled daily or manual
  polyedge-backfill-job            # manual repair/backfill only
  polyedge-report-prune-job        # scheduled weekly/monthly retention

Storage account:
  bot-events raw event blobs
  reports/research output blobs
  data_quality exclusion manifests and freshness snapshots
  data/research normalized/indexed data
  research/index compact replay index

Azure Tables:
  PolyEdgeDataFreshness
  PolyEdgeResearchRuns
  PolyEdgeProspectiveResults
  PolyEdgeExclusionWindows
  PolyEdgeResearchArtifacts
  PolyEdgeJobStatus

Azure Monitor:
  Metric alerts
  Log alerts
  Action group to email/Teams/webhook
```

### 3.2 Required Container Apps Jobs

#### Job A — Data freshness monitor

**Schedule:** every 5 minutes.

**Command:**

```bash
polyedge-rs research azure-freshness \
  --account "$AZURE_STORAGE_ACCOUNT" \
  --container bot-events \
  --prefix "events/" \
  --out "data_quality/freshness/latest.json"
```

**Checks:**

- Latest blob `LastModified`.
- Latest blob size.
- Blob count in current UTC hour.
- Expected minute blobs: approximately 60 per complete hour.
- New blobs in last 5 minutes.
- Median minute-blob size for current hour.
- Tiny-blob ratio: blobs `< 5 KB`.
- Very tiny blobs: blobs `<= 600 bytes`.
- Blob-service metrics if accessible:
  - `Ingress`
  - `Transactions`
  - `BlobCount`
  - `BlobCapacity`
  - `UsedCapacity`
- Runtime `/status` recorder metrics:
  - queued
  - enqueued_total
  - persisted_total
  - failed_total
  - dropped_count
  - error_count
  - last_error
  - worker_alive

**Alert thresholds:**

```text
critical:
  no new blob for > 5 minutes
  recorder failed_total > 0
  recorder dropped_count > 0
  worker_alive=false
  failed heartbeat to API/status for > 3 checks

warning:
  no new blob for > 3 minutes
  current hour has < expected minute blobs after grace period
  tiny blob ratio > 20% in a 30-minute window
  median minute-blob size drops > 80% vs trailing baseline
  recorder queue grows for 3 consecutive checks
  feed_error spike
```

**Outputs:**

```text
data_quality/freshness/YYYY/MM/DD/HH/mm.json
data_quality/freshness/latest.json
Azure Table: PolyEdgeDataFreshness
```

#### Job B — Hourly quality job

**Schedule:** hourly, at minute 10.

The audit targets the fully closed preceding UTC hour. The job derives both
the date and hour from one `1 hour ago` timestamp, so the midnight rollover is
correct. Auditing the current hour is unsafe because the recorder can still
replace the active minute blob, which correctly makes the sealed inventory's
conditional read fail with Azure HTTP 412.

**Command:**

```bash
polyedge-rs research audit \
  --input "azure://$ACCOUNT/bot-events/events/YYYY/MM/DD/HH/" \
  --out "reports/research/hourly/YYYY/MM/DD/HH/audit.json" \
  --markdown "reports/research/hourly/YYYY/MM/DD/HH/audit.md" \
  --exclusion-file "data_quality/exclusion_windows.yaml"
```

**Checks:**

- Event counts by type.
- Malformed lines.
- Missing payloads.
- Missing market IDs.
- Tiny blob counts.
- Book/reference/fair-value presence.
- Feed errors.
- Recorder errors.
- Time gaps.
- Start/settlement capture if markets close in that hour.

Primary prospective validation starts on 2026-07-13, the first complete
protocol-v3 daily chain. Older reports remain retained for exploratory and
historical analysis, but cannot enter promotion evidence unless they are
backfilled into verified atomic daily bundles.

**Outputs:**

```text
reports/research/hourly/YYYY/MM/DD/HH/audit.json
reports/research/hourly/YYYY/MM/DD/HH/audit.md
Azure Table: PolyEdgeResearchRuns
```

#### Job C — Daily research job

**Schedule:** daily after the previous UTC day is complete, for example `30 1 * * *`.

**Command sequence:**

```bash
polyedge-rs research audit \
  --input "azure://$ACCOUNT/bot-events/events/YYYY/MM/DD/" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out reports/research/daily/YYYY-MM-DD/data_audit.json \
  --markdown reports/research/daily/YYYY-MM-DD/data_audit.md

polyedge-rs research build-markets \
  --input data/research/normalized_or_azure/YYYY-MM-DD \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out data/research/daily/YYYY-MM-DD/markets.json \
  --markdown reports/research/daily/YYYY-MM-DD/markets_summary.md

polyedge-rs research baseline \
  --input data/research/daily/YYYY-MM-DD \
  --markets data/research/daily/YYYY-MM-DD/markets.json \
  --out reports/research/daily/YYYY-MM-DD/baseline.json \
  --markdown reports/research/daily/YYYY-MM-DD/baseline.md

polyedge-rs research regimes \
  --input data/research/daily/YYYY-MM-DD \
  --markets data/research/daily/YYYY-MM-DD/markets.json \
  --profile-config research/configs/frozen_candidates.yaml \
  --out reports/research/daily/YYYY-MM-DD/regimes.json \
  --markdown reports/research/daily/YYYY-MM-DD/regimes.md

polyedge-rs research calibration \
  --input data/research/daily/YYYY-MM-DD \
  --markets data/research/daily/YYYY-MM-DD/markets.json \
  --out reports/research/daily/YYYY-MM-DD/calibration.json \
  --markdown reports/research/daily/YYYY-MM-DD/calibration.md

polyedge-rs research sample-size \
  --results reports/research/daily/YYYY-MM-DD/regimes.json \
  --out reports/research/daily/YYYY-MM-DD/sample_size.json \
  --markdown reports/research/daily/YYYY-MM-DD/sample_size.md

polyedge-rs research report \
  --audit reports/research/daily/YYYY-MM-DD/data_audit.json \
  --baseline reports/research/daily/YYYY-MM-DD/baseline.json \
  --regimes reports/research/daily/YYYY-MM-DD/regimes.json \
  --calibration reports/research/daily/YYYY-MM-DD/calibration.json \
  --sample-size reports/research/daily/YYYY-MM-DD/sample_size.json \
  --out reports/research/daily/YYYY-MM-DD/final_report.json \
  --markdown reports/research/daily/YYYY-MM-DD/final_report.md
```

**Required comparison candidates:**

```text
static_baseline
dynamic_quote_style
full_deterministic_profile
dynamic_safety_only
```

**Outputs:**

```text
reports/research/daily/YYYY-MM-DD/*
reports/research/latest_daily_report.json
reports/research/latest_daily_report.md
Azure Table: PolyEdgeResearchRuns
Azure Table: PolyEdgeProspectiveResults
```

#### Job D — Prospective validation job

**Schedule:** daily after daily research job.

**Purpose:** evaluate frozen strategies only on new post-bug data. No tuning.

**Command:**

```bash
polyedge-rs research validate-prospective \
  --since 2026-06-14T00:00:00Z \
  --candidates research/configs/frozen_candidates.yaml \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out reports/research/prospective/prospective_validation.json \
  --markdown reports/research/prospective/prospective_validation.md
```

**Rules:**

- No new parameter search.
- No test-day re-ranking.
- No ML training unless explicitly marked research-only.
- Track day-by-day result of frozen candidates.
- Highlight if `dynamic_quote_style` loses future stability.

**Outputs:**

```text
reports/research/prospective/prospective_validation.json
reports/research/prospective/prospective_validation.md
Azure Table: PolyEdgeProspectiveResults
```

#### Job E — Compact replay index builder

**Schedule:** daily or manual.

**Purpose:** reduce research runtime.

Current research times are high. Build a compact index so sweeps do not rescan hundreds of millions of events.

**Command:**

```bash
polyedge-rs research build-replay-index \
  --input "azure://$ACCOUNT/bot-events/events/YYYY/MM/DD/" \
  --exclude-file data_quality/exclusion_windows.yaml \
  --out "data/research/replay-index/YYYY-MM-DD/"
```

**Index contents:**

```text
market_truth_table
decision_time_features
book_touch_events_by_market_token
reference_series_by_market
order_lifecycle_events
settlement_labels
fair_value_series_by_market
regime_features_by_decision
```

**Success target:**

```text
daily report runtime < 30 minutes
single fill-model replay < 10 minutes
regime comparison < 30 minutes
```

#### Job F — Manual backfill/repair job

**Trigger:** manual only.

**Use cases:**

- Rebuild normalized data.
- Rebuild chart series.
- Reprocess a day after a bug fix.
- Recompute reports with updated exclusion registry.

**Command pattern:**

```bash
polyedge-rs research backfill \
  --start YYYY-MM-DD \
  --end YYYY-MM-DD \
  --task normalize|markets|reports|replay-index|all \
  --exclude-file data_quality/exclusion_windows.yaml
```

---

## 4. Exclusion window registry

Create and maintain:

```text
data_quality/exclusion_windows.yaml
```

Initial content:

```yaml
version: 1
updated_at: "2026-06-14T00:00:00Z"
windows:
  - id: "azure-put-bug-2026-06-11"
    start: "2026-06-11T10:00:00Z"
    end: "2026-06-12T22:00:00Z"
    reason: "Azure PUT bug: tiny/incomplete blobs"
    evidence:
      - "events/2026/06/11/11 had mostly tiny blobs"
      - "events/2026/06/12/08-21 had many <=600B or <=5000B blobs"
      - "recovered at 2026-06-12T22:00:00Z"
    default_exclude: true
```

All clean research commands must load this by default.

Frontend must show these windows on the Data Quality page.

---

## 5. Frontend dashboard changes for labs and reports

Add a new primary navigation section:

```text
Dashboard
Markets
Orders/Fills
Reports
Labs
Data Quality
Settings
```

### 5.1 Reports page

Purpose: view generated daily/offline research reports.

**Route:**

```text
/frontend/src/app/reports/page.tsx
```

**Sections:**

1. Latest report summary.
2. Report artifact browser.
3. Daily reports table.
4. Fill-model sensitivity chart.
5. Static vs adaptive comparison.
6. Market-level confidence interval panel.
7. Download JSON/Markdown links.

**Cards:**

```text
Current recommendation
Static net PnL
Dynamic quote-style net PnL
Clean settled markets
95% CI low/high
Data quality verdict
Last successful job
```

**Charts:**

```text
Net PnL by fill model
Max drawdown by fill model
Daily PnL static vs dynamic_quote_style
Profitable vs losing markets
Required market sample size
```

### 5.2 Labs page

Purpose: research experiment control center.

**Route:**

```text
/frontend/src/app/labs/page.tsx
```

**Tabs:**

```text
Overview
Prospective Validation
Parameter Sweeps
Regime Profiles
Calibration
Fill Models
Sample Size
Artifacts
```

**Prospective Validation tab:**

Show:

```text
date
settled markets
static net PnL
dynamic_quote_style net PnL
full_deterministic_profile net PnL
fill model
drawdown
cancel/fill ratio
data quality flags
recommendation
```

**Regime Profiles tab:**

Show:

```text
regime frequency
regime time share
PnL by regime
fills by regime
cancels by regime
orders skipped by profile
static vs adaptive delta
```

**Fill Models tab:**

Show:

```text
touch
touch_after_250ms
touch_after_1000ms
trade_through
adverse_selection_penalized
queue_proxy skipped reason
no_maker_fills
```

### 5.3 Data Quality page

Purpose: prove data is clean before research results are trusted.

**Route:**

```text
/frontend/src/app/data-quality/page.tsx
```

**Cards:**

```text
Latest blob age
Latest blob size
Current hour blob count
Tiny blob ratio
Recorder worker status
Recorder dropped/errors
Blob ingress/transactions
Feed errors
Active exclusion windows
```

**Tables:**

```text
Hourly data-quality audits
Exclusion windows
Anomaly detections
Blob freshness history
Recorder metric history
```

**Charts:**

```text
Minute blob size over time
Blob count by hour
Ingress/transactions by minute
Recorder queued/enqueued/persisted/failed
```

### 5.4 Job Monitor page or panel

Show Azure job executions and internal report jobs:

```text
job_name
last_start
last_finish
status
duration
exit_code
output_artifact
error
```

Actions:

```text
Run daily report manually
Run freshness check
Run prospective validation
Run replay-index build
Run backfill
```

All actions require confirmation and must not enable live trading.

---

## 6. Backend API endpoints for frontend

Implement versioned API endpoints.

### Data quality

```text
GET /api/v1/labs/data-quality/latest
GET /api/v1/labs/data-quality/hourly?date=YYYY-MM-DD
GET /api/v1/labs/data-quality/exclusions
POST /api/v1/labs/data-quality/exclusions/validate
```

Only allow modifying exclusions from backend/admin context if explicitly implemented later. First version read-only.

### Research reports

```text
GET /api/v1/labs/reports/latest
GET /api/v1/labs/reports/daily/{date}
GET /api/v1/labs/reports/artifacts?prefix=
GET /api/v1/labs/reports/artifacts/{artifact_id}
```

### Research jobs

```text
POST /api/v1/labs/jobs/freshness-check
POST /api/v1/labs/jobs/daily-report
POST /api/v1/labs/jobs/prospective-validation
POST /api/v1/labs/jobs/replay-index
POST /api/v1/labs/jobs/backfill
GET  /api/v1/labs/jobs
GET  /api/v1/labs/jobs/{job_id}
```

Job-start endpoints should either:

1. Start an Azure Container Apps Job execution, or
2. Queue an internal job record and return clear "not implemented" until Azure job wiring exists.

Do not run long research jobs inside the API process.

### Lab summaries

```text
GET /api/v1/labs/prospective
GET /api/v1/labs/regimes/latest
GET /api/v1/labs/calibration/latest
GET /api/v1/labs/sample-size/latest
GET /api/v1/labs/fill-models/latest
```

---

## 7. Azure alerting plan

Use Azure Monitor alerts and action groups.

### Metric/log alerts

Create alerts for:

```text
Blob ingress zero for 10 minutes
Transactions zero for 10 minutes
No new latest blob for > 5 minutes
Blob count for complete hour < 55
Tiny blob ratio > 20%
Recorder failed_total > 0
Recorder dropped_count > 0
Container App revision unhealthy
Container App restarts spike
Daily research job failed
Freshness job failed
Prospective validation job failed
```

### Alert routing

Action group:

```text
Email
Teams webhook or Slack webhook
Optional GitHub issue/automation webhook
```

### Alert payload must include

```text
environment
storage account
container
latest blob
latest blob last modified
latest blob size
job name
job execution id
recommended action
```

---

## 8. Data contracts

Define typed JSON schemas in docs or code.

### Data freshness summary

```json
{
  "generated_ts": "...",
  "status": "healthy|warning|critical",
  "latest_blob": "events/YYYY/MM/DD/HH/mm.jsonl",
  "latest_blob_last_modified": "...",
  "latest_blob_size": 2119714,
  "current_hour_blob_count": 60,
  "tiny_blob_ratio": 0.0,
  "ingress_bytes_5m": 21954108,
  "transactions_5m": 5397,
  "recorder": {
    "queued": 0,
    "enqueued_total": 213772,
    "persisted_total": 213772,
    "failed_total": 0,
    "dropped_count": 0,
    "error_count": 0
  },
  "warnings": [],
  "critical": []
}
```

### Prospective validation row

```json
{
  "date": "YYYY-MM-DD",
  "settled_markets": 96,
  "fill_model": "touch_after_250ms",
  "static_net_pnl": "-12.50",
  "dynamic_quote_style_net_pnl": "4.25",
  "full_deterministic_profile_net_pnl": "-1.50",
  "max_drawdown": "22.00",
  "cancel_per_fill": "0.12",
  "ci_95_low": "-0.40",
  "ci_95_high": "0.55",
  "data_quality_status": "healthy",
  "recommendation": "continue_collecting"
}
```

---

## 9. Implementation phases

### Phase 1 — Documentation and config

- Add this document to `docs/azure-research-automation-and-labs-dashboard.md`.
- Add `data_quality/exclusion_windows.yaml`.
- Add `research/configs/frozen_candidates.yaml`.
- Add docs for daily run procedure.

### Phase 2 — Azure jobs

- Add Bicep/Terraform or existing IaC updates for Container Apps Jobs.
- Add job definitions for freshness, hourly audit, daily research, prospective validation, replay index, backfill.
- Add managed identity/RBAC for storage read/write and job execution.
- Add Azure Monitor alerts and action group.

### Phase 3 — Rust commands

Add or verify:

```text
polyedge-rs research azure-freshness
polyedge-rs research validate-prospective
polyedge-rs research build-replay-index
polyedge-rs research backfill
```

Ensure existing commands accept:

```text
--exclude-file data_quality/exclusion_windows.yaml
--exclude-window ...
--out
--markdown
```

### Phase 4 — Backend APIs

Implement `/api/v1/labs/*` endpoints.

- Read summaries from Azure Blob/Table.
- Do not run long jobs inside API.
- Provide job status.
- Validate and expose exclusion windows.
- Preserve existing `/api/v1` endpoints.

### Phase 5 — Frontend dashboard

Implement:

```text
Reports page
Labs page
Data Quality page
Job Monitor
```

Add cards, charts, artifact browser, and daily/prospective validation tables.

### Phase 6 — CI and smoke tests

Add tests for:

```text
exclusion file parsing
freshness summary parsing
prospective validation JSON schema
lab endpoint serialization
frontend component rendering
no secrets in reports
```

Add smoke commands:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
npm --prefix frontend run typecheck
npm --prefix frontend run build
```

---

## 10. Acceptance criteria

### Azure automation

- Freshness job runs every 5 minutes.
- Daily research job runs automatically after UTC day completion.
- Prospective validation job updates tracker daily.
- Contaminated exclusion windows are enforced.
- Alerts fire on missing blobs/tiny blobs/recorder failures.
- Job artifacts are written to `reports/research/daily/YYYY-MM-DD/`.
- No automation enables live trading.

### Data quality

- PUT-bug window is excluded by default.
- New corrupted windows are detected automatically.
- Data-quality status appears in frontend.
- Daily reports show whether data is clean.

### Frontend

- Reports page shows latest daily report and historical reports.
- Labs page shows static vs dynamic vs full deterministic comparison.
- Data Quality page shows freshness, blob counts, tiny blob ratio, recorder metrics, and exclusions.
- Job Monitor shows scheduled/manual job status.
- UI never exposes secrets.
- UI cannot enable live trading.

### Research workflow

- Frozen candidates are tracked prospectively.
- `dynamic_quote_style` is not promoted based on historical data alone.
- Sample-size and confidence intervals are visible.
- Reports clearly say when evidence is inconclusive.

---

## 11. Codex implementation prompt

Use this prompt after adding this document to the repo:

```text
You are Codex in the Rust PolyEdge repo. Read and follow docs/azure-research-automation-and-labs-dashboard.md exactly.

Goal: implement Azure-native automations for PolyEdge research/data quality and update the frontend dashboard to manage and visualize research labs, daily reports, data freshness, prospective validation, and job status.

Hard rules: do not enable live trading, do not place real orders, do not weaken live gates, do not expose secrets, do not mutate raw data, and keep adaptive profiles research-only or paper-only and disabled by default.

Implement Azure Container Apps Jobs for freshness checks, hourly data-quality audits, daily research reports, prospective validation, compact replay index builds, and manual backfills. Add Azure Monitor alert wiring for missing blobs, tiny blob anomalies, recorder failures, and job failures.

Add backend /api/v1/labs/* endpoints and frontend pages for Reports, Labs, Data Quality, and Job Monitor. Enforce the exclusion registry, especially 2026-06-11T10:00:00Z..2026-06-12T22:00:00Z. Track frozen candidates: static_baseline, dynamic_quote_style, full_deterministic_profile, and dynamic_safety_only.

Acceptance: all Rust tests pass, frontend typecheck/build passes, no secrets in artifacts, reports are written under reports/research/, Azure jobs are defined in IaC, and the UI shows latest data quality, daily reports, fill-model sensitivity, sample-size confidence, and prospective validation status.
```

---

## 12. External references

- Azure Container Apps Jobs: scheduled/manual/event-driven finite jobs, schedule cron in UTC.
- Azure Monitor alerts: alert rules monitor metrics/logs and trigger action groups.
- Azure Blob Storage metrics: `Ingress`, `Transactions`, `BlobCount`, `BlobCapacity`, `UsedCapacity`.
