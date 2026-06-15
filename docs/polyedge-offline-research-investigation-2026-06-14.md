# PolyEdge Offline Research Investigation Report

Generated: 2026-06-14
Scope: Azure blob freshness, Azure PUT bug impact, Rust offline research lab, static maker-first strategy evaluation, deterministic adaptive regime profile evaluation, calibration, sample size, and final recommendation.

## Executive Summary

The PolyEdge Azure recorder was not stopped. The Azure Portal capacity tile that appeared stuck near `120GB` was stale because of Azure capacity metric/display lag. Independent evidence showed active blob writes, active blob-service ingress/transactions, increasing blob counts, and healthy recorder metrics.

However, the event data from `events/2026/06/11/10/` through `events/2026/06/12/21/` is incomplete because of a blob PUT bug. Most hourly blobs in that interval are tiny, often roughly 500 bytes or smaller. The evaluation therefore excludes the event-time window:

`2026-06-11T10:00:00Z..2026-06-12T22:00:00Z`

After excluding that contaminated window, the Rust offline research lab evaluated 793 complete settled markets. The current static maker-first strategy is negative under realistic maker fill assumptions:

| Fill model | Settled markets | Orders | Fills | Net PnL | Max drawdown | Fill rate | Cancel/fill ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| touch | 793 | 3,670 | 3,400 | -131.755 | 265.150 | 92.64% | 7.79% |
| touch_after_250ms | 793 | 3,670 | 3,351 | -162.605 | 275.000 | 91.31% | 9.13% |
| touch_after_1000ms | 793 | 3,670 | 3,351 | -162.605 | 275.000 | 91.31% | 9.13% |
| trade_through | 793 | 3,670 | 3,255 | -141.640 | 245.850 | 88.69% | 11.46% |
| adverse_selection_penalized | 793 | 3,670 | 3,351 | -214.105 | 303.150 | 91.31% | 9.13% |
| queue_proxy | 793 | 3,670 | 0 | 0 | 0 | 0.00% | n/a |
| no_maker_fills | 793 | 3,670 | 0 | 0 | 0 | 0.00% | n/a |

The best deterministic adaptive regime profile was `dynamic_quote_style`, with net PnL `+29.75` and max drawdown `172.55`, but it skipped 2,156 orders and is not robust enough for live deployment. Adaptive profiles remain research-only or paper-only and disabled by default. Live mode rejects adaptive profiles.

Final recommendation: continue collecting data unchanged, keep adaptive profiles research-only, do not enable live trading, and do not activate adaptive profiles for live. The current static strategy should not be promoted. A paper-only shadow of deterministic adaptive behavior can be considered only after more clean, post-bug data is collected and the result remains stable out-of-sample.

## Safety Boundaries Observed

No live trading was enabled.

No real orders were placed.

Live gates were not weakened.

No secrets were printed into reports.

Raw blob data was not mutated.

The replay is event-time based.

Decision-time features do not use final settlement data.

Adaptive profiles are research-only or paper-only and disabled by default.

Live mode rejects adaptive regime profiles.

## Azure Blob Freshness Investigation

Storage target:

| Field | Value |
|---|---|
| Subscription | `Visual Studio Professional Subscription` (`73783c0c-5a53-4f9b-b244-6f64e813814c`) |
| Resource group | `rg-polyedge-dev` |
| Storage account | `stpolyedge6urdjr5nmwx7w` |
| Container | `bot-events` |
| Container App | `polyedge-dev` |
| Active revision | `polyedge-dev--0000045` |
| Runtime mode | `paper` |
| Live gates observed | `ALLOW_LIVE=false`, `ENABLE_TAKER_ORDERS=false` |

### Finding

Blob writes were active. The stale-looking `120GB` Azure Portal capacity value was a capacity metric/display lag, not evidence that the recorder stopped.

Direct write evidence:

| Metric | Value |
|---|---|
| Check window | `2026-06-13T02:37:58Z` to `2026-06-13T02:45:13Z` |
| June 13 prefix count at check | 160 blobs |
| June 13 prefix bytes at check | 1,221,967,046 bytes |
| Latest observed blob | `events/2026/06/13/02/39.jsonl` |
| Latest observed blob modified | `2026-06-13T02:39:21Z` |
| Latest observed blob size | 2,119,714 bytes |
| June 12 completed day prefix | 1,154 blobs, 968,026,180 bytes, ending at `events/2026/06/12/23/59.jsonl` |

Azure metrics evidence:

| Metric timestamp | Metric | Value |
|---|---|---:|
| `2026-06-10T02:37:00Z` | UsedCapacity | 116,840,957,820 bytes |
| `2026-06-12T23:37:00Z` | UsedCapacity | 141,874,203,521 bytes |
| `2026-06-13T01:37:00Z` | UsedCapacity | 142,530,840,876 bytes |
| `2026-06-13T00:40:00Z` | BlobCapacity | 133,437,860,809 bytes |
| `2026-06-13T01:40:00Z` | BlobCapacity | 134,094,498,164 bytes |
| `2026-06-13T00:40:00Z` | BlobCount | 14,038 |
| `2026-06-13T01:40:00Z` | BlobCount | 14,119 |
| `2026-06-13T02:39:00Z` | Ingress | 21,954,108 bytes |
| `2026-06-13T02:39:00Z` | Transactions | 5,397 |

Recorder evidence:

| Runtime metric | Value |
|---|---:|
| execution mode | `paper` |
| recorder count | 2 |
| recorder.error_count | 0 |
| recorder.dropped_count | 0 |
| recorder.last_error | `null` |
| recorder_metrics.queued | 0 |
| recorder_metrics.enqueued_total | 213,772 |
| recorder_metrics.persisted_total | 213,772 |
| recorder_metrics.failed_total | 0 |

Code path verification:

| Component | Verified behavior |
|---|---|
| `crates/polyedge-api/src/runtime/recorder.rs` | Runtime recorder creates local JSONL and Azure append blob recorders. |
| `crates/polyedge-storage/src/lib.rs` | Azure append blob recorder batches by minute. |
| Blob layout | `events/YYYY/MM/DD/HH/mm.jsonl` |

Conclusion: no recorder rewrite is needed. The correct freshness signals are latest blob `LastModified`, blob-service ingress and transactions, blob count, and runtime recorder metrics. The Portal capacity tile alone should not be used as a liveness signal.

## Azure PUT Bug Window

The user reported incomplete blobs from `events/2026/06/11/10/` through `events/2026/06/12/21/`. The blob-size inventory confirms that report.

The clean exclusion window used for research was:

`2026-06-11T10:00:00Z..2026-06-12T22:00:00Z`

This is end-exclusive, so replay resumes at `2026-06-12T22:00:00Z`.

Selected hourly evidence:

| Hour prefix | Blobs | Min bytes | Max bytes | <=600 bytes | <=5000 bytes | Total bytes |
|---|---:|---:|---:|---:|---:|---:|
| `events/2026/06/11/09` | 60 | 2,568,642 | 12,063,879 | 0 | 0 | 428,459,936 |
| `events/2026/06/11/10` | 60 | 404 | 13,751,237 | 24 | 24 | 300,673,753 |
| `events/2026/06/11/11` | 32 | 0 | 3,470 | 29 | 32 | 18,180 |
| `events/2026/06/12/08` | 60 | 187 | 3,481 | 58 | 60 | 27,882 |
| `events/2026/06/12/09` | 60 | 242 | 409 | 60 | 60 | 24,149 |
| `events/2026/06/12/18` | 49 | 0 | 409 | 49 | 49 | 18,971 |
| `events/2026/06/12/21` | 60 | 324 | 12,571,260 | 10 | 48 | 61,064,835 |
| `events/2026/06/12/22` | 60 | 341,166 | 14,314,022 | 0 | 0 | 455,749,991 |
| `events/2026/06/12/23` | 60 | 2,616,961 | 13,606,653 | 0 | 0 | 450,697,501 |

Interpretation:

The data is healthy before the bug window, becomes tiny and incomplete during the bug window, and recovers at `2026-06-12T22:00:00Z`. Therefore, the evaluation must not use that corrupted interval.

## Research Lab Implementation

Implemented Rust CLI commands under `polyedge-rs research`:

| Command | Purpose |
|---|---|
| `audit` | Event coverage and data quality audit. |
| `normalize` | Convert Azure/local JSONL events into normalized sharded gzip files. |
| `build-markets` | Build market truth table and completeness flags. |
| `replay` | Event-time replay for one fill model. |
| `baseline` | Static strategy across multiple fill models. |
| `regimes` | Deterministic regime-conditioned profile evaluation. |
| `sweep` | Bounded deterministic parameter sweep with walk-forward split metadata. |
| `calibration` | q-bucket and grouped calibration metrics. |
| `sample-size` | Market-level statistical confidence and required sample size. |
| `report` | Combined JSON and Markdown report. |
| `ml-calibrate` | Optional ML calibration stub, skipped by default. |

Implemented safety and data-quality features:

| Feature | Behavior |
|---|---|
| `--exclude-window` | Skips contaminated event-time intervals without mutating raw data. |
| Sharded gzip event-time merge | Replays normalized shards in event-time order. |
| Reorder buffer | Handles local shard timestamp inversions deterministically. |
| Settlement isolation | Final settlement is used for evaluation labels, not decision-time features. |
| Fill models | `touch`, `touch_after_250ms`, `touch_after_1000ms`, `trade_through`, `queue_proxy`, `adverse_selection_penalized`, `no_maker_fills`. |
| QueueProxy gate | Skipped unless queue depletion and trade evidence exists. |
| Adaptive gate | Adaptive profiles disabled by default and rejected in live mode. |

## Data Normalization and Audit

Input:

`azure://stpolyedge6urdjr5nmwx7w/bot-events/events/2026/06/?prefetch_blobs=32`

Normalized output:

`data/research/normalized_full_june_sharded_gzip`

Normalization summary:

| Metric | Value |
|---|---:|
| Total normalized events | 301,496,858 |
| Malformed lines | 0 |
| First recorded timestamp | `2026-06-02T15:50:14Z` |
| Last recorded timestamp | `2026-06-13T02:03:59Z` |

Event counts:

| Event type | Rows |
|---|---:|
| book | 290,967,057 |
| reference | 8,958,128 |
| decision | 649,382 |
| fair_value | 649,277 |
| market | 264,030 |
| execution_report | 7,398 |
| market_start_price | 798 |
| paper_settlement | 700 |
| feed_error | 88 |

Full audit before the research exclusion:

| Metric | Value |
|---|---:|
| Total events | 301,496,858 |
| Malformed lines | 0 |
| Out-of-order timestamps | 0 |
| Markets seen | 896 |
| Markets with start price | 797 |
| Markets settled | 856 |
| Start price capture rate | 88.95% |
| Settlement rate | 95.54% |
| Decisions | 649,382 |
| Execution reports | 7,398 |
| Paper fills | 2,004 |
| Maker fills | 2,004 |
| Paper cancels | 1,664 |

Largest observed gaps included:

| From | To | Gap |
|---|---|---:|
| `2026-06-02T16:14:17Z` | `2026-06-02T17:00:00Z` | 2,743 seconds |
| `2026-06-12T18:47:59Z` | `2026-06-12T19:32:59Z` | 2,700 seconds |
| `2026-06-11T20:54:31Z` | `2026-06-11T21:10:57Z` | 986 seconds |

The June 11/12 gaps align with the PUT bug window and are the reason the contaminated interval is excluded from clean evaluation.

## Clean Research Dataset After Excluding PUT-Bug Window

Market table built with:

`--exclude-window 2026-06-11T10:00:00Z..2026-06-12T22:00:00Z`

Clean market summary:

| Metric | Value |
|---|---:|
| Markets | 869 |
| Complete for simulation | 793 |
| Missing start price | 73 |
| Missing final price | 17 |
| Total decisions | 649,364 |
| Total fills in market table | 2,004 |
| Excluded market-truth/calibration events | 14,989 |

Replay and baseline commands skipped 799,263 events from the excluded event-time window.

## Baseline Static Maker-First Strategy

Primary static strategy result:

| Metric | Value |
|---|---:|
| Fill model | `touch_after_250ms` |
| Events processed | 300,697,595 |
| Events excluded | 799,263 |
| Settled markets | 793 |
| Orders | 3,670 |
| Fills | 3,351 |
| Taker fills | 0 |
| Net PnL | -162.605 |
| Max drawdown | 275.000 |
| Fill rate | 91.31% |
| Cancel/fill ratio | 9.13% |
| 95% CI, market PnL | [-0.7563, 0.3462] |

Sensitivity across fill models:

| Fill model | Settled markets | Orders | Fills | Net PnL | Max drawdown | 95% CI, market PnL | Interpretation |
|---|---:|---:|---:|---:|---:|---|---|
| `touch` | 793 | 3,670 | 3,400 | -131.755 | 265.150 | [-0.7216, 0.3893] | Optimistic maker fill; still negative. |
| `touch_after_250ms` | 793 | 3,670 | 3,351 | -162.605 | 275.000 | [-0.7563, 0.3462] | Primary maker fill model; negative. |
| `touch_after_1000ms` | 793 | 3,670 | 3,351 | -162.605 | 275.000 | [-0.7563, 0.3462] | Same realized fills in this data. |
| `trade_through` | 793 | 3,670 | 3,255 | -141.640 | 245.850 | [-0.7267, 0.3695] | More conservative fill; negative. |
| `adverse_selection_penalized` | 793 | 3,670 | 3,351 | -214.105 | 303.150 | [-0.8216, 0.2816] | Penalized for adverse selection; worse. |
| `queue_proxy` | 793 | 3,670 | 0 | 0 | 0 | [0, 0] | Skipped; missing queue evidence. |
| `no_maker_fills` | 793 | 3,670 | 0 | 0 | 0 | [0, 0] | Null lower-bound model. |

Conclusion:

The current static maker-first strategy should not be promoted. Every executable maker fill model is negative. The most optimistic fill model is still negative, and adverse selection makes the result materially worse.

## QueueProxy Gap

`QueueProxy` remains warning/skipped because the dataset does not contain validated queue depletion and trade evidence.

Required evidence before enabling QueueProxy:

1. Resting queue position or size-ahead estimate.
2. Trade prints or equivalent executed volume by token, price, and time.
3. Book level depletion evidence after order placement.
4. Own order lifecycle timestamps: place, ack, cancel, cancel ack, and fill.
5. A validation pass proving the queue model does not create midpoint fills or future leakage.

Without this evidence, QueueProxy would be pretending to know whether our maker order was filled. That would be a false precision problem, not a better backtest.

## Deterministic Adaptive Regime Profiles

Adaptive profiles were evaluated in research mode only. They remain disabled by default and are not live-deployable.

| Profile | Settled markets | Orders | Fills | Net PnL | Max drawdown | Fill rate | Cancel/fill ratio | Orders skipped by profile |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| static | 793 | 3,670 | 3,351 | -162.605 | 275.000 | 91.31% | 9.13% | 0 |
| dynamic_safety_only | 793 | 1,514 | 1,377 | -18.40 | 203.00 | 90.95% | 9.66% | 2,156 |
| dynamic_quote_style | 793 | 1,514 | 1,356 | 29.75 | 172.55 | 89.56% | 11.21% | 2,156 |
| full_deterministic_profile | 793 | 1,336 | 1,199 | -10.4250 | 89.0750 | 89.75% | 10.93% | 2,334 |

Interpretation:

`dynamic_quote_style` is the only positive profile. Its result is promising enough for continued research, but not enough for deployment. It makes far fewer trades than static, skips 2,156 orders, and was selected after seeing the same broad research dataset. The result must survive clean future days before being trusted.

## Parameter Sweep

The bounded sweep evaluated one candidate plus baseline under walk-forward split metadata.

| Candidate | Fill model | Fills | Net PnL | Max drawdown | Fill rate | Cancel/fill ratio | Robust |
|---|---|---:|---:|---:|---:|---:|---|
| baseline | touch_after_250ms | 3,351 | -162.605 | 275.000 | 91.31% | 9.13% | false |
| baseline | trade_through | 3,255 | -141.640 | 245.850 | 88.69% | 11.46% | false |
| edge_0.005_ttl_1_final_30_style_improveonetick | touch_after_250ms | 0 | 0 | 0 | 0.00% | n/a | false |
| edge_0.005_ttl_1_final_30_style_improveonetick | trade_through | 2,942 | 6.910 | 180.40 | 80.16% | 19.24% | false |

Conclusion:

The sweep candidate is not robust. A result that has zero fills under one primary maker fill model and only small positive PnL under another cannot justify deployment.

## Calibration

Top-level q-up calibration buckets:

| q bucket | Decisions | Avg q_up | Observed up frequency | Calibration error | Brier score | Log loss |
|---|---:|---:|---:|---:|---:|---:|
| 0.00-0.40 | 291,398 | 0.1294 | 0.1935 | 0.0641 | 0.1771 | 0.1507 |
| 0.40-0.45 | 25,171 | 0.4250 | 0.4224 | -0.0026 | 0.2442 | 0.5538 |
| 0.45-0.50 | 26,729 | 0.4759 | 0.4516 | -0.0243 | 0.2485 | 0.6465 |
| 0.50-0.55 | 29,824 | 0.5234 | 0.4909 | -0.0325 | 0.2512 | 0.7416 |
| 0.55-0.60 | 25,202 | 0.5748 | 0.5419 | -0.0329 | 0.2495 | 0.5541 |
| 0.60-0.70 | 44,111 | 0.6476 | 0.5965 | -0.0511 | 0.2441 | 0.4355 |
| 0.70-1.00 | 205,614 | 0.9110 | 0.8613 | -0.0498 | 0.1313 | 0.0992 |

Interpretation:

The model is close in the 0.40-0.45 bucket, underconfident in the lowest bucket, and overconfident by roughly 2.4 to 5.1 percentage points in the higher buckets. This does not directly say the strategy is bad, but it does say the decision probabilities are not perfectly calibrated. Bad calibration can make quote prices too aggressive or too passive.

## Statistical Confidence and Why More Data Is Needed

The key point: the effective sample size is the number of resolved markets, not the number of book updates.

The normalized dataset contains 301,496,858 events. That sounds huge, but most of those are book updates inside the same 15-minute markets. They are highly correlated. A strategy does not get 301 million independent chances to prove profitability. It gets one PnL outcome per market.

For the clean evaluation, the independent market-level sample is:

| Metric | Value |
|---|---:|
| Settled market PnL samples | 793 |
| Profitable markets | 364 |
| Losing markets | 289 |
| Mean PnL per settled market | -0.2051 |
| Median PnL per settled market | 0 |
| Standard deviation | 7.9195 |
| Standard error | 0.2812 |
| 95% CI | [-0.7563, 0.3462] |
| Profitability claim allowed | false |
| Required markets for +/-0.10 precision | 24,095 |
| Required markets for +/-0.05 precision | 96,377 |
| Required markets to detect observed mean magnitude | 11,695 |

Plain explanation:

If a market can randomly swing the PnL by several dollars, and the average edge we are trying to measure is only a few tenths of a dollar per market, then 793 markets is not enough to separate real edge from noise.

The standard deviation is `7.9195`, while the mean is only `-0.2051`. That means the typical market-to-market variability is about 39 times larger than the average loss we are trying to measure. Because noise is much larger than the measured average, the confidence interval is wide: from `-0.7563` to `+0.3462` per market.

That interval includes both a materially losing strategy and a mildly profitable strategy. Therefore, the data proves that the current static strategy looked negative in this run, but it does not prove the exact long-run average with tight precision.

More data is needed for five reasons:

1. The independent unit is the settled market, not the tick. We have 793 useful settled markets, not 301 million independent samples.
2. Market PnL is noisy. Individual market outcomes vary much more than the average measured edge.
3. The Azure PUT bug removed a long continuous interval from June 11 to June 12.
4. Adaptive profiles trade fewer markets. `dynamic_quote_style` only filled 1,356 orders and skipped 2,156 orders, so its apparent profit is based on a narrower subset.
5. Testing multiple fill models and candidate profiles creates selection risk. One candidate can look good by chance unless it also works on future data that was not used while designing it.

This is why the recommendation is not "adaptive is live-ready." The correct conclusion is: static is currently negative; adaptive is interesting but unproven; collect more clean data before activating even paper-only adaptive behavior beyond controlled research.

## Runtime and Resource Notes

| Task | Elapsed seconds | Max RSS KB | Notes |
|---|---:|---:|---|
| Normalize Azure to sharded gzip | 19,612.37 | 1,391,580 | Full Azure prefix normalization. |
| Full event-time audit | 4,268.30 | 2,117,564 | Required reorder buffer for sharded gzip. |
| Clean market build | 180.01 | 500,480 | Excluded PUT-bug window. |
| Clean replay | 4,284.84 | 2,128,324 | `touch_after_250ms`. |
| Clean baseline | 9,317.81 | 2,174,056 | All fill models. |
| Clean calibration | 325.75 | 445,504 | q buckets and grouped calibration. |
| Clean regimes | 6,220.55 | 2,156,584 | Static plus deterministic adaptive profiles. |
| Clean capped sweep | 6,998.83 | 2,157,728 | One candidate plus baseline. |

Future performance improvement:

The next research iteration should cache market-level replay features or create a compact replay index. Full-stream sweeps are correct but expensive.

## Verification

Validation commands completed successfully:

| Command | Result |
|---|---|
| `cargo fmt --check` | passed |
| `cargo test -p polyedge-reporting --all-features` | passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | passed |
| `cargo test --workspace --all-features` | passed |
| `cargo build --release -p polyedge-cli` | passed |

Additional safety check:

Generated research reports were scanned for common SAS, storage key, bearer token, access token, refresh token, and API key patterns. No matches were found.

## Artifact Index

Primary report artifacts:

| Artifact | Purpose |
|---|---|
| `reports/research/azure_blob_health_2026-06-13.md` | Azure capacity and recorder liveness investigation. |
| `reports/research/azure_put_bug_window_2026-06-13.md` | Blob-size evidence for the PUT-bug window. |
| `reports/research/full_june_normalize_sharded_gzip.json` | Full Azure normalization result. |
| `reports/research/full_june_data_audit_sharded_gzip.json` | Full event-time audit. |
| `reports/research/full_june_excluding_put_bug_markets_sharded_gzip.json` | Clean market truth table. |
| `reports/research/full_june_excluding_put_bug_replay_sharded_gzip.json` | Primary static replay. |
| `reports/research/full_june_excluding_put_bug_baseline_sharded_gzip.json` | Static strategy across fill models. |
| `reports/research/full_june_excluding_put_bug_regimes_sharded_gzip.json` | Deterministic adaptive regime profiles. |
| `reports/research/full_june_excluding_put_bug_sweep_sharded_gzip.json` | Bounded sweep with walk-forward metadata. |
| `reports/research/full_june_excluding_put_bug_calibration_sharded_gzip.json` | Calibration metrics. |
| `reports/research/full_june_excluding_put_bug_sample_size_sharded_gzip.json` | Statistical confidence and sample-size conclusion. |
| `reports/research/full_june_excluding_put_bug_final_strategy_research_report.md` | Generated combined Markdown summary. |
| `reports/research/full_june_excluding_put_bug_ml_calibrate.md` | Optional ML calibration stub. |

## Known Gaps

QueueProxy:

QueueProxy cannot be trusted until queue depletion and trade-print evidence is recorded and validated. It should remain skipped.

Adaptive profiles:

Adaptive profiles are implemented but must remain research-only or paper-only and disabled by default. Live mode rejects adaptive profiles. The positive `dynamic_quote_style` result is not sufficient for live.

PUT-bug contamination:

The June 11 10:00 UTC through June 12 22:00 UTC window must stay excluded from clean evaluation.

Sample size:

The clean market-level sample is too small for a profitability claim. More clean settled markets are needed.

## Final Recommendation

Continue collecting data unchanged.

Do not enable live trading.

Do not deploy adaptive profiles to live.

Do not rely on QueueProxy until queue depletion and trade evidence exists.

Do not promote the static maker-first strategy; it is negative across executable maker fill models.

Keep adaptive profiles research-only. A controlled paper-only shadow can be reconsidered later if the same adaptive profile remains positive on clean future data with stronger statistical confidence.

Exact next action:

Keep the recorder running in paper mode with current safety gates unchanged. Collect a clean post-bug sample, then rerun the same report bundle with the PUT-bug exclusion still applied and compare static, `dynamic_quote_style`, and `full_deterministic_profile` out-of-sample.
