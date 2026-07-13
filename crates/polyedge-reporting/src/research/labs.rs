use super::run_bundle::quality_from_audit;
use super::*;
use chrono::NaiveDate;
use sha2::{Digest, Sha256};

mod config;
pub use config::{
    load_default_exclusions, load_exclusion_registry, load_frozen_candidate_registry,
    ExclusionRegistry, ExclusionWindowRecord, FrozenCandidateRecord, FrozenCandidateRegistry,
    DEFAULT_EXCLUSION_FILE, DEFAULT_FROZEN_CANDIDATES_FILE, DEFAULT_PROSPECTIVE_SINCE,
    FROZEN_CANDIDATE_NAMES,
};

/// First UTC research day for which the immutable run-manifest protocol is
/// mandatory. Flat daily artifacts are read only for genuinely historical
/// dates before this cutoff and only when no atomic marker exists.
pub const ATOMIC_DAILY_PROTOCOL_CUTOFF: &str = "2026-07-12";
pub const WALLET_CAMPAIGN_START: &str = "2026-07-12";
pub const CUMULATIVE_WALLET_SCOPE: &str = "cumulative_since_2026-07-12";

pub fn legacy_daily_fallback_allowed(report_date: NaiveDate, atomic_marker_present: bool) -> bool {
    !atomic_marker_present
        && report_date
            < NaiveDate::parse_from_str(ATOMIC_DAILY_PROTOCOL_CUTOFF, "%Y-%m-%d")
                .expect("atomic daily protocol cutoff is a valid date")
}

#[derive(Clone, Debug)]
pub struct AzureFreshnessOptions {
    pub account: String,
    pub container: String,
    pub prefix: String,
    pub out: PathBuf,
    pub sas_env: Option<String>,
    pub client_id: Option<String>,
    pub generated_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct ProspectiveValidationOptions {
    pub since: DateTime<Utc>,
    pub reports_dir: PathBuf,
    pub candidates: PathBuf,
    pub out: PathBuf,
    pub markdown: PathBuf,
    /// When set, validation is dependency-aware and leaves the prior output
    /// untouched until this UTC day's atomic bundle is COMPLETE and verified.
    pub expected_daily_date: Option<NaiveDate>,
}

#[derive(Clone, Debug)]
pub struct ProfitabilityEvaluationOptions {
    pub daily_root: PathBuf,
    pub prospective: PathBuf,
    pub gate_config: PathBuf,
    pub execution_model: PathBuf,
    pub out: PathBuf,
    pub generated_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct CumulativeWalletSnapshotOptions {
    pub regimes: PathBuf,
    pub normalized_manifest: PathBuf,
    pub snapshot_date: NaiveDate,
    pub out: PathBuf,
}

/// Binds the wallet ledger produced by the cumulative replay to the exact
/// normalized input manifest. The resulting file is included in the day's
/// immutable bundle; profitability refuses daily reset wallet metrics.
pub fn run_build_cumulative_wallet_snapshot(
    options: CumulativeWalletSnapshotOptions,
) -> Result<Value, ResearchError> {
    let regimes_bytes = fs::read(&options.regimes)?;
    let regimes: Value = serde_json::from_slice(&regimes_bytes)?;
    let normalized_bytes = fs::read(&options.normalized_manifest)?;
    let normalized: Value = serde_json::from_slice(&normalized_bytes)?;
    let normalized_events = normalized["events"].as_u64().ok_or_else(|| {
        ResearchError::InvalidInput("normalized cumulative manifest is missing events".to_owned())
    })?;
    let profile = find_regime_profile(&regimes, "dynamic_quote_style").ok_or_else(|| {
        ResearchError::InvalidInput(
            "cumulative replay is missing dynamic_quote_style profile".to_owned(),
        )
    })?;
    if profile["wallet_constrained"].as_bool() != Some(true) {
        return Err(ResearchError::InvalidInput(
            "cumulative replay is not wallet constrained".to_owned(),
        ));
    }
    let cumulative_events = profile["events"].as_u64().ok_or_else(|| {
        ResearchError::InvalidInput("cumulative replay profile is missing events".to_owned())
    })?;
    if cumulative_events == 0 || cumulative_events > normalized_events {
        return Err(ResearchError::InvalidInput(
            "cumulative replay event count is outside its normalized input".to_owned(),
        ));
    }
    for field in [
        "wallet_constrained_net_pnl",
        "wallet_constrained_ending_equity",
        "wallet_constrained_max_drawdown",
        "wallet_constrained_unresolved_orders",
    ] {
        if (field == "wallet_constrained_unresolved_orders" && profile[field].as_u64().is_none())
            || (field != "wallet_constrained_unresolved_orders"
                && decimal_from_value(&profile[field]).is_none())
        {
            return Err(ResearchError::InvalidInput(format!(
                "cumulative replay is missing valid {field}"
            )));
        }
    }
    let snapshot = json!({
        "schema_version": 1,
        "wallet_scope": CUMULATIVE_WALLET_SCOPE,
        "campaign_start": WALLET_CAMPAIGN_START,
        "snapshot_date": options.snapshot_date.format("%Y-%m-%d").to_string(),
        "cumulative_input_sha256": format!("sha256:{}", sha256_hex(&normalized_bytes)),
        "cumulative_state_sha256": format!("sha256:{}", sha256_hex(&regimes_bytes)),
        "cumulative_events": cumulative_events,
        "candidate": "dynamic_quote_style",
        "fill_model": regimes.pointer("/result/fill_model").cloned().unwrap_or(Value::Null),
        "wallet_constrained": true,
        "wallet_constrained_net_pnl": profile["wallet_constrained_net_pnl"].clone(),
        "wallet_constrained_ending_equity": profile["wallet_constrained_ending_equity"].clone(),
        "wallet_constrained_max_drawdown": profile["wallet_constrained_max_drawdown"].clone(),
        "wallet_constrained_unresolved_orders": profile["wallet_constrained_unresolved_orders"].clone(),
        "research_only": true,
        "funded_execution_allowed": false
    });
    write_json_file(&options.out, &snapshot)?;
    Ok(snapshot)
}

#[derive(Clone, Debug)]
pub struct ReplayIndexOptions {
    pub input: PathBuf,
    pub out: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct ChartBackfillOptions {
    pub input: PathBuf,
    pub out: PathBuf,
    pub markdown: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct BackfillOptions {
    pub start: String,
    pub end: String,
    pub task: String,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
    pub out: PathBuf,
    pub markdown: PathBuf,
}

pub fn run_azure_freshness(options: AzureFreshnessOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let generated_at = options.generated_at.unwrap_or_else(Utc::now);
    let mut client = match options.sas_env.as_deref() {
        Some(sas_env) => {
            let sas = std::env::var(sas_env).map_err(|_| {
                ResearchError::Azure(format!(
                    "{sas_env} must contain a read/list SAS token for azure freshness"
                ))
            })?;
            AzureBlobClient::new(&options.account, &options.container, sas)
        }
        None => AzureBlobClient::with_managed_identity(
            &options.account,
            &options.container,
            options.client_id.clone(),
        ),
    };
    let normalized_prefix = ensure_trailing_slash(&options.prefix);
    let current_prefix = hour_blob_prefix(&normalized_prefix, generated_at);
    let previous_prefix = hour_blob_prefix(&normalized_prefix, generated_at - Duration::hours(1));
    let mut blobs = Vec::new();
    for prefix in [&previous_prefix, &current_prefix] {
        let listed = client
            .list_blobs(prefix, None, None)
            .map_err(|error| ResearchError::Azure(error.to_string()))?;
        blobs.extend(listed);
    }
    blobs.sort_by(|left, right| {
        left.last_modified
            .cmp(&right.last_modified)
            .then_with(|| left.name.cmp(&right.name))
    });
    blobs.dedup_by(|left, right| left.name == right.name);
    let latest = blobs.last();
    let current_hour_blobs = blobs
        .iter()
        .filter(|blob| blob.name.starts_with(&current_prefix))
        .collect::<Vec<_>>();
    let latest_age_seconds = latest
        .and_then(|blob| blob.last_modified)
        .map(|modified| (generated_at - modified).num_seconds().max(0));
    let tiny_blob_count = current_hour_blobs
        .iter()
        .filter(|blob| blob.content_length < 5_000)
        .count();
    let very_tiny_blob_count = current_hour_blobs
        .iter()
        .filter(|blob| blob.content_length <= 600)
        .count();
    let tiny_blob_ratio = if current_hour_blobs.is_empty() {
        0.0
    } else {
        tiny_blob_count as f64 / current_hour_blobs.len() as f64
    };
    let median_minute_blob_size = median_u64(
        current_hour_blobs
            .iter()
            .map(|blob| blob.content_length)
            .collect(),
    );
    let expected_current_hour_blobs = usize::try_from(generated_at.minute() + 1).unwrap_or(60);
    let mut warnings = Vec::new();
    let mut critical = Vec::new();
    if latest.is_none() {
        critical.push("no blobs found in current or previous UTC hour".to_owned());
    }
    if latest_age_seconds.is_some_and(|age| age > 300) {
        critical.push("no new blob for more than 5 minutes".to_owned());
    } else if latest_age_seconds.is_some_and(|age| age > 180) {
        warnings.push("no new blob for more than 3 minutes".to_owned());
    }
    if tiny_blob_ratio > 0.20 {
        warnings.push("tiny blob ratio above 20% in current hour".to_owned());
    }
    if current_hour_blobs.len() + 1 < expected_current_hour_blobs && generated_at.minute() > 10 {
        warnings.push("current hour blob count is below minute expectation".to_owned());
    }
    let status = if !critical.is_empty() {
        "critical"
    } else if !warnings.is_empty() {
        "warning"
    } else {
        "healthy"
    };
    let result = json!({
        "generated_ts": ts(generated_at),
        "status": status,
        "storage_account": options.account,
        "container": options.container,
        "prefix": normalized_prefix,
        "latest_blob": latest.map(|blob| blob.name.clone()),
        "latest_blob_last_modified": latest.and_then(|blob| blob.last_modified).map(ts),
        "latest_blob_size": latest.map(|blob| blob.content_length),
        "latest_blob_age_seconds": latest_age_seconds,
        "current_hour_prefix": current_prefix,
        "current_hour_blob_count": current_hour_blobs.len(),
        "expected_current_hour_blob_count": expected_current_hour_blobs,
        "tiny_blob_count": tiny_blob_count,
        "very_tiny_blob_count": very_tiny_blob_count,
        "tiny_blob_ratio": tiny_blob_ratio,
        "median_minute_blob_size": median_minute_blob_size,
        "recorder": Value::Null,
        "metrics": {
            "ingress_bytes_5m": Value::Null,
            "transactions_5m": Value::Null,
            "blob_count": Value::Null,
            "blob_capacity": Value::Null,
            "used_capacity": Value::Null
        },
        "warnings": warnings,
        "critical": critical,
        "research_only": true,
        "live_trading_enabled": false
    });
    let report = envelope(
        "polyedge-rs research azure-freshness",
        Path::new("azure"),
        "none",
        "freshness",
        start.elapsed(),
        result["warnings"].as_array().cloned().unwrap_or_default(),
        result,
    );
    write_json_file(&options.out, &report)?;
    write_freshness_snapshot_copy(&options.out, generated_at, &report)?;
    Ok(report)
}

pub fn run_validate_prospective(
    options: ProspectiveValidationOptions,
) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let candidates = load_frozen_candidate_registry(&options.candidates)?;
    if let Some(expected_date) = options.expected_daily_date {
        let local_dependency = inspect_daily_dependency(&options.reports_dir, expected_date)?;
        let dependency = if matches!(
            local_dependency,
            DailyDependency::WaitingForDependency { .. }
        ) {
            inspect_azure_daily_dependency(&options.reports_dir, expected_date)?
                .unwrap_or(local_dependency)
        } else {
            local_dependency
        };
        if let DailyDependency::WaitingForDependency { reason, .. } = &dependency {
            return Ok(envelope(
                "polyedge-rs research validate-prospective",
                &options.reports_dir,
                "queue_proxy_conservative",
                "frozen_candidates",
                start.elapsed(),
                vec![json!(format!("waiting for daily dependency: {reason}"))],
                json!({
                    "status": "waiting_for_dependency",
                    "expected_daily_date": expected_date,
                    "dependency": dependency,
                    "previous_latest_preserved": true,
                    "output_written": false,
                    "frozen_candidates": candidates.as_json(),
                    "research_only": true,
                    "paper_only": true,
                    "live_deployment_allowed": false
                }),
            ));
        }
    }
    let rows = load_daily_prospective_rows(&options.reports_dir, options.since)?;
    let paired_improvement = paired_improvement_summary(&rows);
    let status = if rows.is_empty() {
        "collecting"
    } else {
        "tracking"
    };
    let result = json!({
        "status": status,
        "since": ts(options.since),
        "rows": rows,
        "paired_improvement": paired_improvement,
        "frozen_candidates": candidates.as_json(),
        "rules": [
            "No new parameter search.",
            "No test-day re-ranking.",
            "No ML training unless explicitly marked research-only.",
            "dynamic_quote_style must remain research-only until future clean data confirms stability."
        ],
        "research_only": true,
        "paper_only": true,
        "live_deployment_allowed": false
    });
    let warnings = if result["rows"].as_array().is_some_and(Vec::is_empty) {
        vec![json!("no daily reports found for prospective window yet")]
    } else {
        Vec::new()
    };
    let report = envelope(
        "polyedge-rs research validate-prospective",
        &options.reports_dir,
        "touch_after_250ms",
        "frozen_candidates",
        start.elapsed(),
        warnings,
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &prospective_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_evaluate_profitability(
    options: ProfitabilityEvaluationOptions,
) -> Result<PromotionManifestV1, ResearchError> {
    // `stopped_no_go` is an absorbing terminal state. Once the canonical
    // manifest reaches it, later data or model recomputation cannot silently
    // resurrect the candidate. A new candidate/version must use a new state.
    let existing_manifest = read_local_or_azure_json(&options.out)?
        .map(serde_json::from_value::<PromotionManifestV1>)
        .transpose()?;
    if let Some(existing) = &existing_manifest {
        if existing.phase == PromotionPhase::StoppedNoGo {
            return Ok(existing.clone());
        }
        if existing
            .funded_ladder
            .as_ref()
            .is_some_and(|ladder| ladder.terminal)
        {
            return Ok(existing.clone());
        }
    }
    let config = load_profitability_gate(&options.gate_config)?;
    let prospective = read_local_or_azure_json(&options.prospective)?.unwrap_or(Value::Null);
    let rows = load_daily_prospective_rows(
        &options.daily_root,
        DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch is valid"),
    )?;
    let (execution_model, execution_model_binding) =
        load_exact_execution_model(&options.execution_model)?;
    let expected_prior_sha = if config.shadow_prior_sha256.starts_with("sha256:") {
        config.shadow_prior_sha256.to_ascii_lowercase()
    } else {
        format!("sha256:{}", config.shadow_prior_sha256.to_ascii_lowercase())
    };
    if execution_model_binding.model_version != config.shadow_prior_model_version
        || execution_model_binding.sha256 != expected_prior_sha
        || execution_model["status"].as_str() != Some("frozen_conservative_prior")
        || execution_model["prediction_policy"].as_str()
            != Some("zero_fill_probability_until_authenticated_calibration")
        || execution_model["sample_size"].as_u64() != Some(0)
        || execution_model["promotion_ready"].as_bool() != Some(false)
        || execution_model["promotion_allowed"].as_bool() != Some(false)
        || execution_model["funded_execution_allowed"].as_bool() != Some(false)
    {
        return Err(ResearchError::InvalidInput(
            "shadow profitability requires the exact pinned non-executable conservative execution prior"
                .to_owned(),
        ));
    }
    let metrics =
        aggregate_profitability_metrics(&rows, &prospective, &execution_model, &config.thresholds);
    let evaluation =
        PromotionEvaluation::evaluate_shadow_with_thresholds(metrics, &config.thresholds);
    let generated_at = options.generated_at.unwrap_or_else(Utc::now);
    let mut manifest = PromotionManifestV1::new(
        config.candidate,
        evaluation,
        BTreeMap::from([
            (
                "shadow_daily_root".to_owned(),
                options.daily_root.to_string_lossy().into_owned(),
            ),
            (
                "prospective_result".to_owned(),
                options.prospective.to_string_lossy().into_owned(),
            ),
            (
                "profitability_gate".to_owned(),
                options.gate_config.to_string_lossy().into_owned(),
            ),
            (
                "effective_queue_model".to_owned(),
                execution_model_binding.blob_uri.clone(),
            ),
        ]),
        execution_model_binding,
        generated_at,
        generated_at + Duration::hours(24),
    )?;
    if let Some(existing) = existing_manifest {
        if existing.candidate == manifest.candidate && existing.funded_ladder.is_some() {
            manifest.funded_ladder = existing.funded_ladder;
            manifest.phase = manifest
                .funded_ladder
                .as_ref()
                .expect("preserved funded ladder exists")
                .phase;
        }
    }
    // PromotionManifestV1 is intentionally fail-closed. This research command
    // can report passing gates, but it can never arm funded execution.
    write_promotion_manifest(&options.out, &manifest)?;
    Ok(manifest)
}

pub fn run_build_replay_index(options: ReplayIndexOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    fs::create_dir_all(&options.out)?;
    let input_files = collect_replay_index_inputs(&options.input)?;
    let result = json!({
        "status": "manifest_built",
        "input": options.input.to_string_lossy(),
        "out": options.out.to_string_lossy(),
        "input_files": input_files,
        "index_contents": [
            "market_truth_table",
            "decision_time_features",
            "book_touch_events_by_market_token",
            "reference_series_by_market",
            "order_lifecycle_events",
            "settlement_labels",
            "fair_value_series_by_market",
            "regime_features_by_decision"
        ],
        "success_targets": {
            "daily_report_runtime_minutes": 30,
            "single_fill_model_replay_minutes": 10,
            "regime_comparison_minutes": 30
        },
        "excluded_time_windows": exclusion_windows_json(&options.exclude_windows),
        "research_only": true,
        "raw_data_mutated": false,
        "live_trading_enabled": false
    });
    let report = envelope(
        "polyedge-rs research build-replay-index",
        &options.input,
        "none",
        "compact_index_manifest",
        start.elapsed(),
        Vec::new(),
        result,
    );
    write_json_file(&options.out.join("index_manifest.json"), &report)?;
    Ok(report)
}

pub fn run_chart_backfill(options: ChartBackfillOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let started_ts = Utc::now();
    let mut accumulator = ChartBackfillAccumulator::default();
    let stats = stream_events(
        &options.input,
        EventPathMode::ChartBackfill,
        &options.exclude_windows,
        |event| accumulator.observe(event),
    )?;
    let mut warnings = stats
        .warnings
        .into_iter()
        .map(Value::String)
        .collect::<Vec<_>>();
    let truncated_markets = accumulator.truncated_market_count();
    if truncated_markets > 0 {
        warnings.push(json!(format!(
            "chart samples were downsampled for {} markets",
            truncated_markets
        )));
    }
    let finished_ts = Utc::now();
    let first_ts = accumulator.first_ts;
    let last_ts = accumulator.last_ts;
    let markets = accumulator.market_rows();
    let point_count = markets
        .iter()
        .filter_map(|market| market["points"].as_array().map(Vec::len))
        .sum::<usize>();
    let decision_marker_count = markets
        .iter()
        .filter_map(|market| market["decisions"].as_array().map(Vec::len))
        .sum::<usize>();
    let fill_marker_count = markets
        .iter()
        .filter_map(|market| market["fills"].as_array().map(Vec::len))
        .sum::<usize>();
    let result = json!({
        "job_id": "chart-backfill",
        "job_type": "chart-backfill",
        "status": "completed",
        "started_ts": ts(started_ts),
        "finished_ts": ts(finished_ts),
        "duration_seconds": start.elapsed().as_secs_f64(),
        "input": options.input.to_string_lossy(),
        "input_window": {
            "first_recorded_ts": first_ts.map(ts),
            "last_recorded_ts": last_ts.map(ts)
        },
        "chart_store": {
            "market_count": markets.len(),
            "point_count": point_count,
            "decision_marker_count": decision_marker_count,
            "fill_marker_count": fill_marker_count,
            "max_points_per_market": MAX_CHART_BACKFILL_POINTS_PER_MARKET
        },
        "markets": markets,
        "artifacts": [
            {
                "path": options.out.to_string_lossy(),
                "kind": "chart_backfill_report"
            },
            {
                "path": options.markdown.to_string_lossy(),
                "kind": "markdown"
            }
        ],
        "warnings": warnings.clone(),
        "errors": [],
        "excluded_event_count": stats.excluded_events,
        "excluded_time_windows": exclusion_windows_json(&options.exclude_windows),
        "research_only": true,
        "raw_data_mutated": false,
        "live_trading_enabled": false
    });
    let report = envelope(
        "polyedge-rs research chart-backfill",
        &options.input,
        "none",
        "chart_backfill",
        start.elapsed(),
        warnings,
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &chart_backfill_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_backfill(options: BackfillOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    validate_backfill_task(&options.task)?;
    validate_date(&options.start, "start")?;
    validate_date(&options.end, "end")?;
    let result = json!({
        "status": "planned",
        "start": options.start,
        "end": options.end,
        "task": options.task,
        "allowed_tasks": ["normalize", "markets", "reports", "replay-index", "all"],
        "excluded_time_windows": exclusion_windows_json(&options.exclude_windows),
        "research_only": true,
        "manual_only": true,
        "raw_data_mutated": false,
        "live_trading_enabled": false,
        "note": "Manual backfill planning only; raw event blobs are never mutated."
    });
    let report = envelope(
        "polyedge-rs research backfill",
        Path::new("reports/research"),
        "none",
        "manual_backfill",
        start.elapsed(),
        Vec::new(),
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &backfill_markdown(&report),
    )?;
    Ok(report)
}

const MAX_CHART_BACKFILL_POINTS_PER_MARKET: usize = 2_000;
const MAX_CHART_BACKFILL_MARKERS_PER_MARKET: usize = 500;

#[derive(Default)]
struct ChartBackfillAccumulator {
    markets: BTreeMap<String, ChartMarketBackfill>,
    token_to_market: BTreeMap<String, String>,
    first_ts: Option<DateTime<Utc>>,
    last_ts: Option<DateTime<Utc>>,
}

impl ChartBackfillAccumulator {
    fn observe(&mut self, event: &EventLine) {
        self.first_ts = min_ts(self.first_ts, Some(event.recorded_ts));
        self.last_ts = max_ts(self.last_ts, Some(event.recorded_ts));
        match event.event_type.as_str() {
            "market" => self.observe_market(event),
            "fair_value" => self.observe_fair_value(event),
            "book" => self.observe_book(event),
            "decision" => self.observe_decision(event),
            "execution_report" => self.observe_execution_report(event),
            _ => {}
        }
    }

    fn observe_market(&mut self, event: &EventLine) {
        let payload = &event.payload;
        let market_id = text(payload, "market_id");
        if market_id.is_empty() {
            return;
        }
        if let Some(token) = optional_text(payload, "up_token_id") {
            self.token_to_market.insert(token, market_id.clone());
        }
        if let Some(token) = optional_text(payload, "down_token_id") {
            self.token_to_market.insert(token, market_id.clone());
        }
        let market = self.market_mut(&market_id);
        market.question = optional_text(payload, "question").or(market.question.take());
        market.start_ts = parse_datetime(payload.get("start_ts")).or(market.start_ts);
        market.end_ts = parse_datetime(payload.get("end_ts")).or(market.end_ts);
        market.condition_id = optional_text(payload, "condition_id").or(market.condition_id.take());
        market.slug = optional_text(payload, "market_slug").or(market.slug.take());
    }

    fn observe_fair_value(&mut self, event: &EventLine) {
        let payload = &event.payload;
        let market_id = text(payload, "market_id");
        if market_id.is_empty() {
            return;
        }
        let point_ts = chart_event_ts(event, payload);
        let point = json!({
            "time": ts(point_ts),
            "bucket": point_ts.timestamp_millis(),
            "qUp": decimal(payload.get("q_up")).and_then(|value| value.to_f64()),
            "qDown": decimal(payload.get("q_down")).and_then(|value| value.to_f64()),
            "eventType": "fair_value"
        });
        self.market_mut(&market_id).push_point(point);
    }

    fn observe_book(&mut self, event: &EventLine) {
        let payload = &event.payload;
        let Some(market_id) = self.market_id_for_payload(payload) else {
            return;
        };
        let point_ts = chart_event_ts(event, payload);
        let point = json!({
            "time": ts(point_ts),
            "bucket": point_ts.timestamp_millis(),
            "token_id": text(payload, "token_id"),
            "bestBid": best_level_price(payload.get("bids"), true).and_then(|value| value.to_f64()),
            "bestAsk": best_level_price(payload.get("asks"), false).and_then(|value| value.to_f64()),
            "bookHash": optional_text(payload, "book_hash"),
            "eventType": "book"
        });
        self.market_mut(&market_id).push_point(point);
    }

    fn observe_decision(&mut self, event: &EventLine) {
        let payload = &event.payload;
        let market_id = text(payload, "market_id");
        if market_id.is_empty() {
            return;
        }
        let marker_ts = chart_event_ts(event, payload);
        let marker = json!({
            "time": ts(marker_ts),
            "bucket": marker_ts.timestamp_millis(),
            "action": text(payload, "action"),
            "outcome": text(payload, "outcome"),
            "price": decimal(payload.get("price")).and_then(|value| value.to_f64()),
            "size": decimal(payload.get("size")).and_then(|value| value.to_f64()),
            "reason": text(payload, "reason")
        });
        self.market_mut(&market_id).push_decision(marker);
    }

    fn observe_execution_report(&mut self, event: &EventLine) {
        let payload = &event.payload;
        let market_id = text(payload, "market_id");
        if market_id.is_empty() {
            return;
        }
        let marker_ts = chart_event_ts(event, payload);
        let marker = json!({
            "time": ts(marker_ts),
            "bucket": marker_ts.timestamp_millis(),
            "status": text(payload, "status"),
            "token_id": text(payload, "token_id"),
            "fillPrice": decimal(payload.get("avg_price")).and_then(|value| value.to_f64()),
            "filledSize": decimal(payload.get("filled_size")).and_then(|value| value.to_f64())
        });
        self.market_mut(&market_id).push_fill(marker);
    }

    fn market_id_for_payload(&self, payload: &Value) -> Option<String> {
        optional_text(payload, "market_id")
            .filter(|value| !value.is_empty())
            .or_else(|| {
                optional_text(payload, "token_id").and_then(|token| {
                    self.token_to_market
                        .get(&token)
                        .filter(|value| !value.is_empty())
                        .cloned()
                })
            })
    }

    fn market_mut(&mut self, market_id: &str) -> &mut ChartMarketBackfill {
        self.markets
            .entry(market_id.to_owned())
            .or_insert_with(|| ChartMarketBackfill::new(market_id))
    }

    fn truncated_market_count(&self) -> usize {
        self.markets
            .values()
            .filter(|market| market.truncated_points)
            .count()
    }

    fn market_rows(self) -> Vec<Value> {
        self.markets
            .into_values()
            .map(ChartMarketBackfill::into_json)
            .collect()
    }
}

struct ChartMarketBackfill {
    market_id: String,
    question: Option<String>,
    condition_id: Option<String>,
    slug: Option<String>,
    start_ts: Option<DateTime<Utc>>,
    end_ts: Option<DateTime<Utc>>,
    total_points_seen: usize,
    points: Vec<Value>,
    decisions: Vec<Value>,
    fills: Vec<Value>,
    truncated_points: bool,
    truncated_decisions: bool,
    truncated_fills: bool,
}

impl ChartMarketBackfill {
    fn new(market_id: &str) -> Self {
        Self {
            market_id: market_id.to_owned(),
            question: None,
            condition_id: None,
            slug: None,
            start_ts: None,
            end_ts: None,
            total_points_seen: 0,
            points: Vec::new(),
            decisions: Vec::new(),
            fills: Vec::new(),
            truncated_points: false,
            truncated_decisions: false,
            truncated_fills: false,
        }
    }

    fn push_point(&mut self, point: Value) {
        self.total_points_seen += 1;
        if self.points.len() < MAX_CHART_BACKFILL_POINTS_PER_MARKET {
            self.points.push(point);
        } else {
            self.truncated_points = true;
        }
    }

    fn push_decision(&mut self, marker: Value) {
        if self.decisions.len() < MAX_CHART_BACKFILL_MARKERS_PER_MARKET {
            self.decisions.push(marker);
        } else {
            self.truncated_decisions = true;
        }
    }

    fn push_fill(&mut self, marker: Value) {
        if self.fills.len() < MAX_CHART_BACKFILL_MARKERS_PER_MARKET {
            self.fills.push(marker);
        } else {
            self.truncated_fills = true;
        }
    }

    fn into_json(self) -> Value {
        json!({
            "market_id": self.market_id,
            "question": self.question,
            "condition_id": self.condition_id,
            "market_slug": self.slug,
            "start_ts": self.start_ts.map(ts),
            "end_ts": self.end_ts.map(ts),
            "point_count": self.points.len(),
            "total_points_seen": self.total_points_seen,
            "decision_count": self.decisions.len(),
            "fill_count": self.fills.len(),
            "truncated_points": self.truncated_points,
            "truncated_decisions": self.truncated_decisions,
            "truncated_fills": self.truncated_fills,
            "points": self.points,
            "decisions": self.decisions,
            "fills": self.fills
        })
    }
}

fn chart_event_ts(event: &EventLine, payload: &Value) -> DateTime<Utc> {
    parse_datetime(payload.get("computed_ts"))
        .or_else(|| parse_datetime(payload.get("source_ts")))
        .or_else(|| parse_datetime(payload.get("exchange_ts")))
        .or_else(|| parse_datetime(payload.get("local_ts")))
        .unwrap_or(event.recorded_ts)
}

fn ensure_trailing_slash(value: &str) -> String {
    let trimmed = value.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

fn hour_blob_prefix(base_prefix: &str, timestamp: DateTime<Utc>) -> String {
    format!(
        "{}{:04}/{:02}/{:02}/{:02}/",
        base_prefix,
        timestamp.year(),
        timestamp.month(),
        timestamp.day(),
        timestamp.hour()
    )
}

fn median_u64(mut values: Vec<u64>) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(values[values.len() / 2])
}

fn write_freshness_snapshot_copy(
    out: &Path,
    timestamp: DateTime<Utc>,
    report: &Value,
) -> Result<(), ResearchError> {
    if out.file_name().and_then(|name| name.to_str()) != Some("latest.json") {
        return Ok(());
    }
    let Some(root) = out.parent() else {
        return Ok(());
    };
    let snapshot = root
        .join(format!("{:04}", timestamp.year()))
        .join(format!("{:02}", timestamp.month()))
        .join(format!("{:02}", timestamp.day()))
        .join(format!("{:02}", timestamp.hour()))
        .join(format!("{:02}.json", timestamp.minute()));
    write_json_file(&snapshot, report)
}

fn load_daily_prospective_rows(
    reports_dir: &Path,
    since: DateTime<Utc>,
) -> Result<Vec<Value>, ResearchError> {
    let mut rows = load_local_daily_prospective_rows(reports_dir, since)?;
    if rows.is_empty() {
        rows = load_azure_daily_prospective_rows(reports_dir, since)?;
    }
    rows.sort_by(|left, right| left["date"].as_str().cmp(&right["date"].as_str()));
    Ok(rows)
}

fn load_local_daily_prospective_rows(
    reports_dir: &Path,
    since: DateTime<Utc>,
) -> Result<Vec<Value>, ResearchError> {
    if !reports_dir.exists() {
        return Ok(Vec::new());
    }
    let since_date = since.date_naive();
    let mut rows = Vec::new();
    for entry in fs::read_dir(reports_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let date = entry.file_name().to_string_lossy().into_owned();
        let Ok(report_date) = NaiveDate::parse_from_str(&date, "%Y-%m-%d") else {
            continue;
        };
        if report_date < since_date {
            continue;
        }
        let date_dir = entry.path();
        let atomic_marker_present =
            date_dir.join("latest.json").is_file() || date_dir.join("runs").is_dir();
        let source_dir = if atomic_marker_present {
            match inspect_daily_dependency(reports_dir, report_date)? {
                DailyDependency::Ready { bundle_dir, .. } => bundle_dir,
                DailyDependency::WaitingForDependency { reason, .. } => {
                    return Err(ResearchError::InvalidInput(format!(
                        "atomic daily bundle {date} is not verified: {reason}"
                    )))
                }
            }
        } else if legacy_daily_fallback_allowed(report_date, false) {
            date_dir
        } else {
            return Err(ResearchError::InvalidInput(format!(
                "atomic daily bundle is required on or after {ATOMIC_DAILY_PROTOCOL_CUTOFF}: {date}"
            )));
        };
        rows.push(daily_prospective_row(&date, &source_dir)?);
    }
    Ok(rows)
}

fn daily_prospective_row(date: &str, dir: &Path) -> Result<Value, ResearchError> {
    let final_report = read_optional_json(&dir.join("final_report.json"))?;
    let regimes = read_optional_json(&dir.join("regimes.json"))?
        .or(read_optional_json(&dir.join("regime_profiles.json"))?);
    let baseline = read_optional_json(&dir.join("baseline.json"))?.or(read_optional_json(
        &dir.join("baseline_static_all_fill_models.json"),
    )?);
    let sample_size = read_optional_json(&dir.join("sample_size.json"))?;
    let audit = read_optional_json(&dir.join("data_audit.json"))?;
    let execution_quality = read_optional_json(&dir.join("execution_quality.json"))?;
    let cumulative_wallet = read_optional_json(&dir.join("cumulative_wallet.json"))?;
    daily_prospective_row_from_reports(
        date,
        DailyReportDocuments {
            final_report,
            regimes,
            baseline,
            sample_size,
            audit,
            execution_quality,
            cumulative_wallet,
        },
    )
}

fn load_azure_daily_prospective_rows(
    reports_dir: &Path,
    since: DateTime<Utc>,
) -> Result<Vec<Value>, ResearchError> {
    let Some(mut client) = research_blob_client() else {
        return Ok(Vec::new());
    };
    let prefix = report_blob_prefix(reports_dir);
    let blobs = client
        .list_blobs_by_suffixes(
            &prefix,
            &["latest.json", "run_manifest.json", "final_report.json"],
            Some(3000),
            None,
        )
        .map_err(|error| {
            ResearchError::Azure(format!("listing prospective daily reports: {error}"))
        })?;
    let since_date = since.date_naive();
    let mut dates = blobs
        .into_iter()
        .filter_map(|blob| {
            let relative = blob.name.strip_prefix(&prefix)?;
            let date = relative.split('/').next()?.to_owned();
            let report_date = NaiveDate::parse_from_str(&date, "%Y-%m-%d").ok()?;
            (report_date >= since_date).then_some(date)
        })
        .collect::<Vec<_>>();
    dates.sort();
    dates.dedup();

    let mut rows = Vec::new();
    for date in dates {
        let daily_prefix = format!("{prefix}{date}/");
        match load_azure_complete_bundle(&mut client, &prefix, &date)? {
            AzureDailyBundleState::Ready {
                run_prefix,
                manifest,
            } => rows.push(daily_prospective_row_from_reports(
                &date,
                DailyReportDocuments {
                    final_report: read_manifest_artifact(
                        &mut client,
                        &run_prefix,
                        &manifest,
                        &["final_report.json", "final_strategy_research_report.json"],
                    )?,
                    regimes: read_manifest_artifact(
                        &mut client,
                        &run_prefix,
                        &manifest,
                        &["regimes.json", "regime_profiles.json"],
                    )?,
                    baseline: read_manifest_artifact(
                        &mut client,
                        &run_prefix,
                        &manifest,
                        &["baseline.json", "baseline_static_all_fill_models.json"],
                    )?,
                    sample_size: read_manifest_artifact(
                        &mut client,
                        &run_prefix,
                        &manifest,
                        &["sample_size.json"],
                    )?,
                    audit: read_manifest_artifact(
                        &mut client,
                        &run_prefix,
                        &manifest,
                        &["data_audit.json"],
                    )?,
                    execution_quality: read_manifest_artifact(
                        &mut client,
                        &run_prefix,
                        &manifest,
                        &["execution_quality.json"],
                    )?,
                    cumulative_wallet: read_manifest_artifact(
                        &mut client,
                        &run_prefix,
                        &manifest,
                        &["cumulative_wallet.json"],
                    )?,
                },
            )?),
            AzureDailyBundleState::Invalid { reason } => {
                return Err(ResearchError::InvalidInput(format!(
                    "Azure atomic daily bundle {date} is not verified: {reason}"
                )))
            }
            AzureDailyBundleState::Absent => {
                let report_date = NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                    .expect("date was validated while discovering daily blobs");
                if !legacy_daily_fallback_allowed(report_date, false) {
                    return Err(ResearchError::InvalidInput(format!(
                        "Azure atomic daily bundle is required on or after {ATOMIC_DAILY_PROTOCOL_CUTOFF}: {date}"
                    )));
                }
                rows.push(daily_prospective_row_from_reports(
                    &date,
                    DailyReportDocuments {
                        final_report: read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}final_report.json"),
                        )?,
                        regimes: read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}regimes.json"),
                        )?
                        .or(read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}regime_profiles.json"),
                        )?),
                        baseline: read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}baseline.json"),
                        )?
                        .or(read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}baseline_static_all_fill_models.json"),
                        )?),
                        sample_size: read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}sample_size.json"),
                        )?,
                        audit: read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}data_audit.json"),
                        )?,
                        execution_quality: read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}execution_quality.json"),
                        )?,
                        cumulative_wallet: read_blob_json(
                            &mut client,
                            &format!("{daily_prefix}cumulative_wallet.json"),
                        )?,
                    },
                )?);
            }
        }
    }
    Ok(rows)
}

fn inspect_azure_daily_dependency(
    reports_dir: &Path,
    expected_date: NaiveDate,
) -> Result<Option<DailyDependency>, ResearchError> {
    let Some(mut client) = research_blob_client() else {
        return Ok(None);
    };
    let prefix = report_blob_prefix(reports_dir);
    let date = expected_date.format("%Y-%m-%d").to_string();
    Ok(Some(
        match load_azure_complete_bundle(&mut client, &prefix, &date)? {
            AzureDailyBundleState::Ready {
                run_prefix,
                manifest,
            } => DailyDependency::Ready {
                date: expected_date,
                run_id: manifest.run_id.clone(),
                bundle_dir: PathBuf::from(format!("azure://{run_prefix}")),
                manifest,
            },
            AzureDailyBundleState::Absent => DailyDependency::WaitingForDependency {
                date: expected_date,
                reason: "azure_latest_pointer_absent".to_owned(),
            },
            AzureDailyBundleState::Invalid { reason } => DailyDependency::WaitingForDependency {
                date: expected_date,
                reason: format!("azure_atomic_bundle_invalid:{reason}"),
            },
        },
    ))
}

enum AzureDailyBundleState {
    Absent,
    Ready {
        run_prefix: String,
        manifest: Box<DailyRunManifest>,
    },
    Invalid {
        reason: String,
    },
}

fn load_azure_complete_bundle(
    client: &mut AzureBlobClient,
    prefix: &str,
    date: &str,
) -> Result<AzureDailyBundleState, ResearchError> {
    let pointer_blob = format!("{prefix}{date}/latest.json");
    let pointer_bytes = match client.download_blob_bytes(&pointer_blob) {
        Ok(bytes) => bytes,
        Err(AzureBlobError::HttpStatus(404)) => {
            let run_prefix = format!("{prefix}{date}/runs/");
            let manifests = client
                .list_blobs_by_suffixes(&run_prefix, &["run_manifest.json"], Some(1), None)
                .map_err(|error| {
                    ResearchError::Azure(format!(
                        "checking atomic manifests without a latest pointer under {run_prefix}: {error}"
                    ))
                })?;
            return Ok(if manifests.is_empty() {
                AzureDailyBundleState::Absent
            } else {
                AzureDailyBundleState::Invalid {
                    reason: "manifest_present_without_latest_pointer".to_owned(),
                }
            });
        }
        Err(error) => {
            return Err(ResearchError::Azure(format!(
                "reading daily latest pointer {pointer_blob}: {error}"
            )))
        }
    };
    let pointer: LatestRunPointer = serde_json::from_slice(&pointer_bytes)?;
    if pointer.date.format("%Y-%m-%d").to_string() != date
        || !safe_blob_relative_path(&pointer.manifest_path)
    {
        return Ok(AzureDailyBundleState::Invalid {
            reason: "latest_pointer_identity_or_path_invalid".to_owned(),
        });
    }
    let manifest_blob = format!("{prefix}{date}/{}", pointer.manifest_path);
    let manifest_bytes = match client.download_blob_bytes(&manifest_blob) {
        Ok(bytes) => bytes,
        Err(AzureBlobError::HttpStatus(404)) => {
            return Ok(AzureDailyBundleState::Invalid {
                reason: "manifest_absent".to_owned(),
            })
        }
        Err(error) => {
            return Err(ResearchError::Azure(format!(
                "reading daily manifest {manifest_blob}: {error}"
            )))
        }
    };
    if sha256_hex(&manifest_bytes) != pointer.manifest_sha256 {
        return Ok(AzureDailyBundleState::Invalid {
            reason: "manifest_hash_mismatch".to_owned(),
        });
    }
    let manifest: DailyRunManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.status != RunStatus::Complete
        || manifest.run_id != pointer.run_id
        || manifest.date != pointer.date
    {
        return Ok(AzureDailyBundleState::Invalid {
            reason: "manifest_incomplete_or_identity_mismatch".to_owned(),
        });
    }
    let run_prefix = manifest_blob
        .strip_suffix("run_manifest.json")
        .unwrap_or(&manifest_blob)
        .to_owned();
    for artifact in manifest.artifacts.values() {
        if !safe_blob_relative_path(&artifact.relative_path) {
            return Ok(AzureDailyBundleState::Invalid {
                reason: "artifact_path_invalid".to_owned(),
            });
        }
        let blob_name = format!("{run_prefix}{}", artifact.relative_path);
        let bytes = match client.download_blob_bytes(&blob_name) {
            Ok(bytes) => bytes,
            Err(AzureBlobError::HttpStatus(404)) => {
                return Ok(AzureDailyBundleState::Invalid {
                    reason: format!("artifact_absent:{}", artifact.relative_path),
                })
            }
            Err(error) => {
                return Err(ResearchError::Azure(format!(
                    "verifying daily artifact {blob_name}: {error}"
                )))
            }
        };
        if bytes.len() as u64 != artifact.bytes || sha256_hex(&bytes) != artifact.sha256 {
            return Ok(AzureDailyBundleState::Invalid {
                reason: format!("artifact_hash_or_size_mismatch:{}", artifact.relative_path),
            });
        }
    }
    Ok(AzureDailyBundleState::Ready {
        run_prefix,
        manifest: Box::new(manifest),
    })
}

fn read_manifest_artifact(
    client: &mut AzureBlobClient,
    run_prefix: &str,
    manifest: &DailyRunManifest,
    candidates: &[&str],
) -> Result<Option<Value>, ResearchError> {
    let Some(artifact) = candidates.iter().find_map(|candidate| {
        manifest
            .artifacts
            .values()
            .find(|artifact| artifact.relative_path == *candidate)
    }) else {
        return Ok(None);
    };
    read_blob_json(client, &format!("{run_prefix}{}", artifact.relative_path))
}

fn safe_blob_relative_path(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.starts_with('\\')
        && !value.split(['/', '\\']).any(|part| part == "..")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

struct DailyReportDocuments {
    final_report: Option<Value>,
    regimes: Option<Value>,
    baseline: Option<Value>,
    sample_size: Option<Value>,
    audit: Option<Value>,
    execution_quality: Option<Value>,
    cumulative_wallet: Option<Value>,
}

fn daily_prospective_row_from_reports(
    date: &str,
    documents: DailyReportDocuments,
) -> Result<Value, ResearchError> {
    json_row(
        date,
        DailyReportSources {
            final_report: documents.final_report.as_ref(),
            regimes: documents.regimes.as_ref(),
            baseline: documents.baseline.as_ref(),
        },
        documents.sample_size.as_ref(),
        documents.audit.as_ref(),
        documents.execution_quality.as_ref(),
        documents.cumulative_wallet.as_ref(),
    )
}

fn research_blob_client() -> Option<AzureBlobClient> {
    let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())?;
    let container = std::env::var("AZURE_STORAGE_CONTAINER_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "bot-events".to_owned());
    let client_id = std::env::var("AZURE_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());
    Some(AzureBlobClient::with_managed_identity(
        account, container, client_id,
    ))
}

fn report_blob_prefix(path: &Path) -> String {
    let mut prefix = path
        .to_string_lossy()
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_matches('/')
        .to_owned();
    if !prefix.ends_with('/') {
        prefix.push('/');
    }
    prefix
}

fn read_blob_json(
    client: &mut AzureBlobClient,
    blob_name: &str,
) -> Result<Option<Value>, ResearchError> {
    match client.download_blob_bytes(blob_name) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(ResearchError::Json),
        Err(AzureBlobError::HttpStatus(404)) => Ok(None),
        Err(error) => Err(ResearchError::Azure(format!(
            "reading research artifact {blob_name}: {error}"
        ))),
    }
}

fn merge_optional_reports(values: [Option<&Value>; 3]) -> Value {
    let mut merged = Map::new();
    for value in values.into_iter().flatten() {
        if let Some(object) = value.as_object() {
            for (key, child) in object {
                merged.insert(key.clone(), child.clone());
            }
        }
        if let Some(result) = value.get("result").and_then(Value::as_object) {
            for (key, child) in result {
                merged.insert(format!("result.{key}"), child.clone());
            }
        }
    }
    Value::Object(merged)
}

struct DailyReportSources<'a> {
    final_report: Option<&'a Value>,
    regimes: Option<&'a Value>,
    baseline: Option<&'a Value>,
}

fn json_row(
    date: &str,
    reports: DailyReportSources<'_>,
    sample: Option<&Value>,
    audit: Option<&Value>,
    execution_quality: Option<&Value>,
    cumulative_wallet: Option<&Value>,
) -> Result<Value, ResearchError> {
    let source = merge_optional_reports([reports.final_report, reports.regimes, reports.baseline]);
    let sample = sample.unwrap_or(&source);
    let fill_model = text_at(&source, &["/result/fill_model"]).unwrap_or("touch_after_250ms");
    let static_net = select_regime_profile_net(reports.regimes, "static")
        .or_else(|| select_regime_profile_net(reports.regimes, "static_baseline"))
        .or_else(|| select_regime_profile_net(reports.final_report, "static"))
        .or_else(|| select_regime_profile_net(reports.final_report, "static_baseline"))
        .or_else(|| select_fill_model_net(reports.baseline, fill_model))
        .or_else(|| select_fill_model_net(reports.final_report, fill_model));
    let dynamic_net = select_regime_profile_net(reports.regimes, "dynamic_quote_style")
        .or_else(|| select_regime_profile_net(reports.final_report, "dynamic_quote_style"));
    // Wallet fields are accepted only from the separately generated
    // cumulative campaign replay. Per-day regime reports reset capital and
    // therefore cannot support a promotion decision.
    let dynamic_wallet_net =
        cumulative_wallet.and_then(|wallet| value_to_string(&wallet["wallet_constrained_net_pnl"]));
    let dynamic_wallet_constrained =
        cumulative_wallet.and_then(|wallet| wallet["wallet_constrained"].as_bool());
    let full_net = select_regime_profile_net(reports.regimes, "full_deterministic_profile")
        .or_else(|| select_regime_profile_net(reports.final_report, "full_deterministic_profile"));
    let safety_net = select_regime_profile_net(reports.regimes, "dynamic_safety_only")
        .or_else(|| select_regime_profile_net(reports.final_report, "dynamic_safety_only"));
    let dynamic_delta = paired_delta(dynamic_net.as_deref(), static_net.as_deref());
    let full_delta = paired_delta(full_net.as_deref(), static_net.as_deref());
    let safety_delta = paired_delta(safety_net.as_deref(), static_net.as_deref());
    let best_delta = [dynamic_delta, full_delta, safety_delta]
        .into_iter()
        .flatten()
        .max();
    let ci_low = text_at(sample, &["/result/statistics/ci_low", "/statistics/ci_low"]);
    let ci_high = text_at(
        sample,
        &["/result/statistics/ci_high", "/statistics/ci_high"],
    );
    let settled_markets = number_at(
        &source,
        &[
            "/result.market_truth_table/result/summary/complete_for_simulation",
            "/result.summary/complete_for_simulation",
            "/summary/complete_for_simulation",
            "/result/summary/complete_for_simulation",
            "/result/statistics/sample_size",
        ],
    )
    .or_else(|| number_at(sample, &["/result/statistics/n", "/statistics/n"]));
    let quality = data_quality_status(audit);
    let quality_reasons = data_quality_reasons(audit);
    let quality_summary = audit.map(quality_from_audit);
    let execution_quality_gate = execution_quality
        .and_then(|report| report.pointer("/result/evidence_gate"))
        .cloned()
        .unwrap_or_else(|| json!("NOT_AVAILABLE"));
    let recommendation = prospective_recommendation(ci_low, ci_high, dynamic_net.as_deref());
    let dynamic_gate =
        prospective_decision_gate(quality, dynamic_net.as_deref(), dynamic_delta, ci_low);
    let full_gate = prospective_decision_gate(quality, full_net.as_deref(), full_delta, ci_low);
    let safety_gate =
        prospective_decision_gate(quality, safety_net.as_deref(), safety_delta, ci_low);
    Ok(json!({
        "date": date,
        "settled_markets": settled_markets,
        "fill_model": fill_model,
        "static_net_pnl": static_net,
        "dynamic_quote_style_net_pnl": dynamic_net,
        "wallet_constrained_net_pnl": dynamic_wallet_net,
        "wallet_constrained_ending_equity": cumulative_wallet.and_then(|wallet| value_to_string(&wallet["wallet_constrained_ending_equity"])),
        "wallet_constrained_max_drawdown": cumulative_wallet.and_then(|wallet| value_to_string(&wallet["wallet_constrained_max_drawdown"])),
        "wallet_constrained_unresolved_orders": cumulative_wallet.and_then(|wallet| wallet["wallet_constrained_unresolved_orders"].as_u64()),
        "wallet_scope": cumulative_wallet.and_then(|wallet| wallet["wallet_scope"].as_str()),
        "wallet_campaign_start": cumulative_wallet.and_then(|wallet| wallet["campaign_start"].as_str()),
        "wallet_snapshot_date": cumulative_wallet.and_then(|wallet| wallet["snapshot_date"].as_str()),
        "cumulative_input_sha256": cumulative_wallet.and_then(|wallet| wallet["cumulative_input_sha256"].as_str()),
        "cumulative_state_sha256": cumulative_wallet.and_then(|wallet| wallet["cumulative_state_sha256"].as_str()),
        "cumulative_events": cumulative_wallet.and_then(|wallet| wallet["cumulative_events"].as_u64()),
        "full_deterministic_profile_net_pnl": full_net,
        "dynamic_safety_only_net_pnl": safety_net,
        "dynamic_quote_style_paired_delta": dynamic_delta.map(|value| value.to_string()),
        "full_deterministic_profile_paired_delta": full_delta.map(|value| value.to_string()),
        "dynamic_safety_only_paired_delta": safety_delta.map(|value| value.to_string()),
        "best_candidate_paired_delta": best_delta.map(|value| value.to_string()),
        "max_drawdown": find_any_text(&source, "max_drawdown"),
        "cancel_per_fill": find_any_text(&source, "cancel_fill_ratio"),
        "ci_95_low": ci_low,
        "ci_95_high": ci_high,
        "data_quality_status": quality,
        "data_quality_reasons": quality_reasons,
        "data_quality": quality_summary,
        "wallet_constrained": dynamic_wallet_constrained,
        "decision_parity_rate": number_at(&source, &["/result/decision_parity_rate", "/decision_parity_rate"]),
        "markout_30s_ci_low": execution_quality.and_then(|report| {
            report.pointer("/result/markouts/30/executable/ci_95_low")
                .or_else(|| report.pointer("/result/markouts/30/executable_markout_ci_95_low"))
        }).cloned(),
        "execution_quality_gate": execution_quality_gate,
        "queue_snapshot_coverage": execution_quality.and_then(|report| report.pointer("/result/queue_snapshot_coverage")).cloned(),
        "markout_1s_completion": execution_quality.and_then(|report| report.pointer("/result/markouts/1/completion_rate")).cloned(),
        "markout_5s_completion": execution_quality.and_then(|report| report.pointer("/result/markouts/5/completion_rate")).cloned(),
        "markout_30s_completion": execution_quality.and_then(|report| report.pointer("/result/markouts/30/completion_rate")).cloned(),
        "recommendation": recommendation,
        "decision_gate": dynamic_gate,
        "dynamic_quote_style_decision_gate": dynamic_gate,
        "full_deterministic_profile_decision_gate": full_gate,
        "dynamic_safety_only_decision_gate": safety_gate,
        "research_only": true,
        "live_deployment_allowed": false
    }))
}

struct LoadedProfitabilityGate {
    candidate: CandidateIdentity,
    thresholds: PromotionThresholds,
    shadow_prior_model_version: String,
    shadow_prior_sha256: String,
}

fn load_profitability_gate(path: &Path) -> Result<LoadedProfitabilityGate, ResearchError> {
    let text = fs::read_to_string(path)?;
    let values = flatten_simple_yaml(&text);
    let required = |key: &str| {
        values.get(key).cloned().ok_or_else(|| {
            ResearchError::InvalidInput(format!("profitability gate is missing {key}"))
        })
    };
    let parse_u32 = |key: &str| -> Result<u32, ResearchError> {
        required(key)?.parse().map_err(|_| {
            ResearchError::InvalidInput(format!("profitability gate has invalid {key}"))
        })
    };
    let parse_u64 = |key: &str| -> Result<u64, ResearchError> {
        required(key)?.parse().map_err(|_| {
            ResearchError::InvalidInput(format!("profitability gate has invalid {key}"))
        })
    };
    let parse_decimal = |key: &str| -> Result<Decimal, ResearchError> {
        required(key)?.parse().map_err(|_| {
            ResearchError::InvalidInput(format!("profitability gate has invalid {key}"))
        })
    };
    Ok(LoadedProfitabilityGate {
        candidate: CandidateIdentity {
            name: required("candidate.name")?,
            candidate_version: required("candidate.version")?,
            config_hash: required("candidate.config_hash")?,
        },
        thresholds: PromotionThresholds {
            required_clean_days: parse_u32("shadow.required_clean_days")?,
            maximum_extension_days: parse_u32("shadow.maximum_extension_days")?,
            required_settled_markets: parse_u64("shadow.required_settled_markets")?,
            maximum_extension_markets: parse_u64("shadow.maximum_extension_markets")?,
            required_positive_weekly_blocks: parse_u32("shadow.required_positive_weekly_blocks")?,
            minimum_decision_parity_rate: parse_decimal("shadow.minimum_decision_parity_rate")?,
            minimum_decision_grade_coverage: parse_decimal(
                "shadow.minimum_decision_grade_coverage",
            )?,
            maximum_modeled_drawdown: parse_decimal("shadow.maximum_modeled_drawdown")?,
            maximum_out_of_order_event_rate: parse_decimal(
                "shadow.maximum_out_of_order_event_rate",
            )?,
            execution_model_protocol_version: parse_u32(
                "execution_model.evidence_protocol_version",
            )?,
            minimum_execution_model_eligible_orders: parse_u64(
                "execution_model.minimum_eligible_orders",
            )?,
            minimum_execution_model_filled_orders: parse_u64(
                "execution_model.minimum_filled_orders",
            )?,
            minimum_execution_model_non_filled_orders: parse_u64(
                "execution_model.minimum_non_filled_orders",
            )?,
            minimum_brier_improvement_over_base_rate: parse_decimal(
                "execution_model.minimum_brier_improvement_over_base_rate",
            )?,
            maximum_expected_calibration_error: parse_decimal(
                "execution_model.maximum_expected_calibration_error",
            )?,
        },
        shadow_prior_model_version: required("execution_model.shadow_prior_model_version")?,
        shadow_prior_sha256: required("execution_model.shadow_prior_sha256")?,
    })
}

fn flatten_simple_yaml(text: &str) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    let mut parents: Vec<(usize, String)> = Vec::new();
    for raw in text.lines() {
        let line = raw.split('#').next().unwrap_or_default();
        if line.trim().is_empty() || line.trim_start().starts_with('-') {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        let Some((key, raw_value)) = line.trim().split_once(':') else {
            continue;
        };
        while parents.last().is_some_and(|(level, _)| *level >= indent) {
            parents.pop();
        }
        let raw_value = raw_value.trim();
        if raw_value.is_empty() {
            parents.push((indent, key.trim().to_owned()));
            continue;
        }
        let full_key = parents
            .iter()
            .map(|(_, parent)| parent.as_str())
            .chain(std::iter::once(key.trim()))
            .collect::<Vec<_>>()
            .join(".");
        values.insert(full_key, raw_value.trim_matches(['\"', '\'']).to_owned());
    }
    values
}

fn read_local_or_azure_json(path: &Path) -> Result<Option<Value>, ResearchError> {
    if let Some(value) = read_optional_json(path)? {
        return Ok(Some(value));
    }
    let Some(mut client) = research_blob_client() else {
        return Ok(None);
    };
    read_blob_json(&mut client, report_blob_prefix(path).trim_end_matches('/'))
}

fn load_exact_execution_model(
    path: &Path,
) -> Result<(Value, ExecutionModelBinding), ResearchError> {
    let (bytes, blob_uri) = if path.is_file() {
        let absolute = fs::canonicalize(path)?;
        (
            fs::read(&absolute)?,
            format!("file://{}", absolute.display()),
        )
    } else {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|_| {
            ResearchError::InvalidInput(
                "effective queue model is missing locally and Azure storage is not configured"
                    .to_owned(),
            )
        })?;
        let container = std::env::var("AZURE_STORAGE_CONTAINER_NAME")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "bot-events".to_owned());
        let blob_name = path.to_string_lossy().replace('\\', "/");
        let mut client = research_blob_client().ok_or_else(|| {
            ResearchError::InvalidInput("Azure storage is not configured".to_owned())
        })?;
        let bytes = client.download_blob_bytes(&blob_name).map_err(|error| {
            ResearchError::Azure(format!(
                "reading exact execution model {blob_name}: {error}"
            ))
        })?;
        (bytes, format!("azure://{account}/{container}/{blob_name}"))
    };
    let value: Value = serde_json::from_slice(&bytes)?;
    let model_version = value
        .get("model_version")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ResearchError::InvalidInput("effective queue model is missing model_version".to_owned())
        })?
        .to_owned();
    Ok((
        value,
        ExecutionModelBinding {
            blob_uri,
            sha256: format!("sha256:{}", sha256_hex(&bytes)),
            model_version,
        },
    ))
}

fn aggregate_profitability_metrics(
    rows: &[Value],
    prospective: &Value,
    execution_model: &Value,
    thresholds: &PromotionThresholds,
) -> ProfitabilityMetrics {
    let mut missing = Vec::new();
    if rows.is_empty() {
        missing.push("complete_daily_rows".to_owned());
    }
    let qualities = rows
        .iter()
        .filter_map(|row| {
            serde_json::from_value::<DataQualitySummary>(row["data_quality"].clone()).ok()
        })
        .collect::<Vec<_>>();
    if qualities.len() != rows.len() || qualities.is_empty() {
        missing.push("daily_data_quality".to_owned());
    }
    let total_events = qualities.iter().map(|quality| quality.total_events).sum();
    let coverage = qualities
        .iter()
        .map(|quality| quality.decision_grade_coverage)
        .min()
        .unwrap_or(Decimal::ZERO);
    let fatal_issues = qualities
        .iter()
        .flat_map(|quality| quality.fatal_issues.clone())
        .collect();
    let warnings = qualities
        .iter()
        .flat_map(|quality| quality.warnings.clone())
        .collect();
    let quality = DataQualitySummary {
        registry_version: WARNING_REGISTRY_VERSION.to_owned(),
        total_events,
        decision_grade_coverage: coverage,
        fatal_issues,
        warnings,
        out_of_order_events: qualities
            .iter()
            .map(|quality| quality.out_of_order_events)
            .sum(),
        event_time_ordering_restored: qualities
            .iter()
            .all(|quality| quality.event_time_ordering_restored),
    };
    let clean_days = consecutive_clean_day_streak(rows);

    let settled = rows
        .iter()
        .map(|row| row["settled_markets"].as_u64())
        .collect::<Option<Vec<_>>>();
    if settled.is_none() {
        missing.push("settled_markets".to_owned());
    }
    let settled_markets = settled.unwrap_or_default().into_iter().sum();

    let daily_pnl = rows
        .iter()
        .map(|row| decimal_from_value(&row["dynamic_quote_style_net_pnl"]))
        .collect::<Option<Vec<_>>>();
    if daily_pnl.is_none() {
        missing.push("queue_conservative_net_pnl".to_owned());
    }
    let pnl_values = daily_pnl.unwrap_or_default();
    let queue_conservative = !rows.is_empty()
        && rows
            .iter()
            .all(|row| row["fill_model"] == "queue_proxy_conservative");
    let queue_pnl: Decimal = pnl_values.iter().copied().sum();
    let cumulative_wallet = validated_cumulative_wallet_snapshots(rows);
    if cumulative_wallet.is_none() {
        missing.push("valid_cumulative_wallet_ledger".to_owned());
    }
    let wallet_snapshots = cumulative_wallet.unwrap_or_default();
    let latest_wallet = wallet_snapshots.last();
    let wallet_constrained = latest_wallet.is_some_and(|snapshot| snapshot.unresolved_orders == 0);
    if latest_wallet.is_some_and(|snapshot| snapshot.unresolved_orders > 0) {
        missing.push("cumulative_wallet_positions_resolved".to_owned());
    }
    let wallet_pnl = latest_wallet
        .map(|snapshot| snapshot.net_pnl)
        .unwrap_or_default();
    let wallet_ending_equity = latest_wallet
        .map(|snapshot| snapshot.ending_equity)
        .unwrap_or_default();
    let wallet_max_drawdown = latest_wallet
        .map(|snapshot| snapshot.max_drawdown)
        .unwrap_or_default();

    let pnl_ci = decimal_at(
        prospective,
        &[
            "/result/paired_improvement/dynamic_quote_style/ci_95_low",
            "/result/pnl_ci_95_low",
        ],
    );
    if pnl_ci.is_none() {
        missing.push("pnl_ci_95_low".to_owned());
    }
    let markout_ci = decimal_at(prospective, &["/result/markout_30s_ci_low"]).or_else(|| {
        rows.iter()
            .filter_map(|row| decimal_from_value(&row["markout_30s_ci_low"]))
            .min()
    });
    if markout_ci.is_none() {
        missing.push("markout_30s_ci_low".to_owned());
    }
    let parity_rate = decimal_at(prospective, &["/result/decision_parity_rate"]).or_else(|| {
        rows.iter()
            .filter_map(|row| decimal_from_value(&row["decision_parity_rate"]))
            .min()
    });
    if parity_rate.is_none() {
        missing.push("decision_parity_rate".to_owned());
    }
    let parity_rate = parity_rate.unwrap_or(Decimal::ZERO);

    let execution_model_protocol_version = execution_model["evidence_protocol_version"]
        .as_u64()
        .and_then(|value| u32::try_from(value).ok());
    let execution_model_eligible_orders = execution_model["sample_size"].as_u64();
    let execution_model_filled_orders = execution_model["positive_fills"].as_u64();
    let execution_model_non_filled_orders = execution_model["negative_non_fills"].as_u64();
    let execution_model_brier_improvement =
        decimal_from_value(&execution_model["brier_improvement_fraction"]);
    let execution_model_expected_calibration_error =
        decimal_from_value(&execution_model["expected_calibration_error"]);
    let execution_model_promotion_ready = execution_model["promotion_ready"].as_bool();
    let execution_model_markout_30s_lower_95 = decimal_from_value(
        &execution_model["net_executable_markout_30s_lower_confidence_bound_95"],
    );
    let mut weekly = BTreeMap::<(i32, u32), Decimal>::new();
    let mut previous_cumulative_pnl = Decimal::ZERO;
    for snapshot in &wallet_snapshots {
        let week = snapshot.date.iso_week();
        let delta = snapshot.net_pnl - previous_cumulative_pnl;
        *weekly.entry((week.year(), week.week())).or_default() += delta;
        previous_cumulative_pnl = snapshot.net_pnl;
    }
    let mut consecutive = 0_u32;
    let mut best_consecutive = 0_u32;
    for pnl in weekly.values() {
        if *pnl > Decimal::ZERO {
            consecutive += 1;
            best_consecutive = best_consecutive.max(consecutive);
        } else {
            consecutive = 0;
        }
    }
    if weekly.is_empty() {
        missing.push("weekly_blocks".to_owned());
    }
    missing.sort();
    missing.dedup();
    ProfitabilityMetrics {
        observed_calendar_days: observed_campaign_days(rows),
        clean_days,
        settled_markets,
        wallet_constrained,
        queue_conservative,
        wallet_constrained_net_pnl: wallet_pnl,
        wallet_constrained_ending_equity: wallet_ending_equity,
        queue_conservative_net_pnl: queue_pnl,
        pnl_ci_95_low: pnl_ci.unwrap_or(Decimal::ZERO),
        consecutive_positive_weekly_blocks: best_consecutive,
        max_drawdown: wallet_max_drawdown,
        drawdown_limit: thresholds.maximum_modeled_drawdown,
        markout_30s_ci_low: markout_ci.unwrap_or(Decimal::ZERO),
        replay_runtime_parity: parity_rate >= thresholds.minimum_decision_parity_rate,
        decision_parity_rate: parity_rate,
        execution_model_protocol_version: execution_model_protocol_version.unwrap_or_default(),
        execution_model_eligible_orders: execution_model_eligible_orders.unwrap_or_default(),
        execution_model_filled_orders: execution_model_filled_orders.unwrap_or_default(),
        execution_model_non_filled_orders: execution_model_non_filled_orders.unwrap_or_default(),
        execution_model_brier_improvement: execution_model_brier_improvement.unwrap_or_default(),
        execution_model_expected_calibration_error: execution_model_expected_calibration_error
            .unwrap_or(Decimal::ONE),
        execution_model_promotion_ready: execution_model_promotion_ready.unwrap_or(false),
        execution_model_markout_30s_lower_95: execution_model_markout_30s_lower_95
            .unwrap_or_default(),
        data_quality: quality,
        missing_metrics: missing,
    }
}

#[derive(Clone, Debug)]
struct CumulativeWalletSnapshot {
    date: NaiveDate,
    events: u64,
    net_pnl: Decimal,
    ending_equity: Decimal,
    max_drawdown: Decimal,
    unresolved_orders: u64,
}

fn validated_cumulative_wallet_snapshots(rows: &[Value]) -> Option<Vec<CumulativeWalletSnapshot>> {
    if rows.is_empty() {
        return None;
    }
    let campaign_start = NaiveDate::parse_from_str(WALLET_CAMPAIGN_START, "%Y-%m-%d").ok()?;
    let mut snapshots: Vec<CumulativeWalletSnapshot> = Vec::with_capacity(rows.len());
    for row in rows {
        let date_text = row["date"].as_str()?;
        let date = NaiveDate::parse_from_str(date_text, "%Y-%m-%d").ok()?;
        let input_hash = row["cumulative_input_sha256"].as_str()?;
        let state_hash = row["cumulative_state_sha256"].as_str()?;
        let snapshot = CumulativeWalletSnapshot {
            date,
            events: row["cumulative_events"].as_u64()?,
            net_pnl: decimal_from_value(&row["wallet_constrained_net_pnl"])?,
            ending_equity: decimal_from_value(&row["wallet_constrained_ending_equity"])?,
            max_drawdown: decimal_from_value(&row["wallet_constrained_max_drawdown"])?,
            unresolved_orders: row["wallet_constrained_unresolved_orders"].as_u64()?,
        };
        if row["wallet_scope"].as_str() != Some(CUMULATIVE_WALLET_SCOPE)
            || row["wallet_campaign_start"].as_str() != Some(WALLET_CAMPAIGN_START)
            || row["wallet_snapshot_date"].as_str() != Some(date_text)
            || row["wallet_constrained"].as_bool() != Some(true)
            || date < campaign_start
            || !valid_sha256(input_hash)
            || !valid_sha256(state_hash)
            || snapshot.events == 0
            || snapshot.ending_equity != WALLET_CAMPAIGN_BASELINE + snapshot.net_pnl
            || snapshot.max_drawdown < Decimal::ZERO
        {
            return None;
        }
        if let Some(previous) = snapshots.last() {
            if snapshot.date <= previous.date
                || snapshot.events < previous.events
                || snapshot.max_drawdown < previous.max_drawdown
            {
                return None;
            }
        }
        snapshots.push(snapshot);
    }
    Some(snapshots)
}

fn valid_sha256(value: &str) -> bool {
    let hex = value.strip_prefix("sha256:").unwrap_or(value);
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn consecutive_clean_day_streak(rows: &[Value]) -> u32 {
    let mut streak = 0_u32;
    let mut previous: Option<NaiveDate> = None;
    for row in rows {
        let date = row["date"]
            .as_str()
            .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
        let clean = serde_json::from_value::<DataQualitySummary>(row["data_quality"].clone())
            .ok()
            .is_some_and(|quality| quality.promotion_allowed());
        if !clean || date.is_none() || previous.is_some_and(|value| date != value.succ_opt()) {
            streak = 0;
        }
        if clean && date.is_some() {
            streak += 1;
        }
        previous = date;
    }
    streak
}

fn observed_campaign_days(rows: &[Value]) -> u32 {
    let Some(latest) = rows
        .iter()
        .filter_map(|row| row["date"].as_str())
        .filter_map(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
        .max()
    else {
        return 0;
    };
    let start = NaiveDate::parse_from_str(WALLET_CAMPAIGN_START, "%Y-%m-%d")
        .expect("wallet campaign start is valid");
    u32::try_from((latest - start).num_days().max(0) + 1).unwrap_or(u32::MAX)
}

fn decimal_at(value: &Value, pointers: &[&str]) -> Option<Decimal> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(decimal_from_value))
}

fn paired_delta(candidate_net: Option<&str>, static_net: Option<&str>) -> Option<Decimal> {
    let candidate = candidate_net.map(decimal_from_str)?;
    let baseline = static_net.map(decimal_from_str)?;
    Some(candidate - baseline)
}

fn paired_improvement_summary(rows: &[Value]) -> Value {
    let candidates = [
        (
            "dynamic_quote_style",
            "dynamic_quote_style_paired_delta",
            "dynamic_quote_style_net_pnl",
        ),
        (
            "full_deterministic_profile",
            "full_deterministic_profile_paired_delta",
            "full_deterministic_profile_net_pnl",
        ),
        (
            "dynamic_safety_only",
            "dynamic_safety_only_paired_delta",
            "dynamic_safety_only_net_pnl",
        ),
    ];
    Value::Object(
        candidates
            .into_iter()
            .map(|(candidate, delta_field, pnl_field)| {
                (
                    candidate.to_owned(),
                    paired_candidate_summary(rows, candidate, delta_field, pnl_field),
                )
            })
            .collect(),
    )
}

fn paired_candidate_summary(
    rows: &[Value],
    candidate: &str,
    delta_field: &str,
    pnl_field: &str,
) -> Value {
    let daily = rows
        .iter()
        .filter_map(|row| {
            let date = row["date"].as_str()?.to_owned();
            let delta = decimal_from_value(&row[delta_field])?;
            Some(json!({
                "date": date,
                "D": delta.to_string(),
                "candidate_net_pnl": row[pnl_field].clone(),
                "static_net_pnl": row["static_net_pnl"].clone(),
                "decision_gate": row["decision_gate"].clone()
            }))
        })
        .collect::<Vec<_>>();
    let values = daily
        .iter()
        .filter_map(|row| decimal_from_value(&row["D"]))
        .collect::<Vec<_>>();
    let n = values.len();
    let mean = mean_decimal(&values);
    let std = std_decimal(&values, mean);
    let se = std.and_then(|value| Decimal::from_f64_retain(value.to_f64()? / (n as f64).sqrt()));
    let ci_low = mean
        .zip(se)
        .map(|(mean, se)| mean - Decimal::new(196, 2) * se);
    let ci_high = mean
        .zip(se)
        .map(|(mean, se)| mean + Decimal::new(196, 2) * se);
    let required_n = match (std, mean) {
        (Some(std), Some(mean)) if mean != Decimal::ZERO => {
            let effect = mean.abs();
            (Decimal::new(196, 2) * std / effect)
                .to_f64()
                .and_then(|value| Decimal::from_f64_retain(value.powi(2)))
                .and_then(|value| value.ceil().to_u64())
        }
        _ => None,
    };
    json!({
        "candidate": candidate,
        "sample_size": n,
        "mean_D": mean.map(|value| value.to_string()),
        "std_D": std.map(|value| value.to_string()),
        "SE_D": se.map(|value| value.to_string()),
        "ci_95_low": ci_low.map(|value| value.to_string()),
        "ci_95_high": ci_high.map(|value| value.to_string()),
        "required_n_to_detect_mean_D": required_n,
        "daily_paired_delta": daily,
        "paired_drawdown": paired_drawdown(&values).map(|value| value.to_string()),
        "recommendation": paired_summary_recommendation(ci_low, mean),
        "research_only": true,
        "paper_only": true,
        "live_deployment_allowed": false
    })
}

fn decimal_from_value(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str_exact(text).ok(),
        Value::Number(number) => Decimal::from_str_exact(&number.to_string()).ok(),
        _ => None,
    }
}

fn paired_drawdown(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let mut cumulative = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut drawdown = Decimal::ZERO;
    for value in values {
        cumulative += *value;
        peak = peak.max(cumulative);
        drawdown = drawdown.max(peak - cumulative);
    }
    Some(drawdown)
}

fn paired_summary_recommendation(ci_low: Option<Decimal>, mean: Option<Decimal>) -> &'static str {
    if ci_low.is_some_and(|value| value > Decimal::ZERO)
        && mean.is_some_and(|value| value > Decimal::ZERO)
    {
        "paper_shadow_ok"
    } else if mean.is_some_and(|value| value < Decimal::ZERO) {
        "reject_candidate"
    } else {
        "continue_collecting"
    }
}

fn data_quality_status(audit: Option<&Value>) -> &'static str {
    let Some(audit) = audit else {
        return "unknown";
    };
    let result = &audit["result"];
    let fatal = result["fatal_data_quality_issues"]
        .as_array()
        .is_some_and(|issues| !issues.is_empty());
    let total_events = decimal_from_value(&result["total_events"]).unwrap_or(Decimal::ZERO);
    let malformed = decimal_from_value(&result["malformed_lines"]).unwrap_or(Decimal::ZERO);
    if fatal || total_events <= Decimal::ZERO || malformed > Decimal::ZERO {
        return "critical";
    }
    let duplicate = decimal_from_value(&result["duplicate_estimate"]).unwrap_or(Decimal::ZERO);
    let out_of_order =
        decimal_from_value(&result["out_of_order_timestamps"]).unwrap_or(Decimal::ZERO);
    let stale_references =
        decimal_from_value(&result["stale_reference_count"]).unwrap_or(Decimal::ZERO);
    let missing_market_ids =
        decimal_from_value(&result["missing_market_ids"]).unwrap_or(Decimal::ZERO);
    let start_capture =
        decimal_from_value(&result["start_price_capture_rate"]).unwrap_or(Decimal::ZERO);
    let settlement = decimal_from_value(&result["settlement_rate"]).unwrap_or(Decimal::ZERO);
    let out_of_order_rate = out_of_order / total_events;
    let stale_reference_rate = stale_references / total_events;
    let missing_market_rate = missing_market_ids / total_events;
    let unexpected_warning = result["warnings"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|warning| !is_informational_audit_message(warning));
    if duplicate > Decimal::ZERO
        || out_of_order_rate > Decimal::new(1, 5)
        || stale_reference_rate > Decimal::new(1, 3)
        || missing_market_rate > Decimal::new(1, 3)
        || start_capture < Decimal::new(95, 2)
        || settlement < Decimal::new(95, 2)
        || unexpected_warning
    {
        "warning"
    } else {
        "healthy"
    }
}

fn is_informational_audit_message(message: &str) -> bool {
    message.ends_with("out-of-order timestamps")
        || message.starts_with("out-of-order timestamp in ")
        || message.starts_with("azure input listed ")
        || (message.starts_with("0 events skipped by ")
            && message.ends_with("excluded event-time window(s)"))
}

fn data_quality_reasons(audit: Option<&Value>) -> Vec<Value> {
    let Some(result) = audit.map(|audit| &audit["result"]) else {
        return vec![json!("audit_not_available")];
    };
    let mut reasons = Vec::new();
    if result["fatal_data_quality_issues"]
        .as_array()
        .is_some_and(|issues| !issues.is_empty())
    {
        reasons.push(json!("fatal_data_quality_issue"));
    }
    if decimal_from_value(&result["malformed_lines"]).unwrap_or(Decimal::ZERO) > Decimal::ZERO {
        reasons.push(json!("malformed_lines"));
    }
    if decimal_from_value(&result["duplicate_estimate"]).unwrap_or(Decimal::ZERO) > Decimal::ZERO {
        reasons.push(json!("duplicate_events"));
    }
    if decimal_from_value(&result["start_price_capture_rate"])
        .is_some_and(|rate| rate < Decimal::new(95, 2))
    {
        reasons.push(json!("start_price_capture_below_95pct"));
    }
    if decimal_from_value(&result["settlement_rate"]).is_some_and(|rate| rate < Decimal::new(95, 2))
    {
        reasons.push(json!("settlement_coverage_below_95pct"));
    }
    for warning in result["warnings"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|warning| !is_informational_audit_message(warning))
    {
        reasons.push(json!(warning));
    }
    reasons.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    reasons.dedup();
    reasons
}

fn prospective_recommendation(
    ci_low: Option<&str>,
    ci_high: Option<&str>,
    dynamic_net: Option<&str>,
) -> &'static str {
    let lower = ci_low.map(decimal_from_str);
    let upper = ci_high.map(decimal_from_str);
    let dynamic = dynamic_net.map(decimal_from_str);
    if lower.is_some_and(|value| value > Decimal::ZERO)
        && dynamic.is_some_and(|value| value > Decimal::ZERO)
    {
        "continue_paper_validation"
    } else if upper.is_some_and(|value| value < Decimal::ZERO) {
        "candidate_unstable"
    } else {
        "continue_collecting"
    }
}

fn prospective_decision_gate(
    data_quality: &str,
    candidate_net: Option<&str>,
    paired_delta: Option<Decimal>,
    ci_low: Option<&str>,
) -> &'static str {
    if !matches!(data_quality, "healthy") {
        return "RESEARCH_ONLY";
    }
    if candidate_net
        .map(decimal_from_str)
        .is_some_and(|value| value < Decimal::ZERO)
        || paired_delta.is_some_and(|value| value < Decimal::ZERO)
    {
        return "REJECT";
    }
    if candidate_net
        .map(decimal_from_str)
        .is_some_and(|value| value > Decimal::ZERO)
        && paired_delta.is_some_and(|value| value > Decimal::ZERO)
        && ci_low
            .map(decimal_from_str)
            .is_some_and(|value| value > Decimal::ZERO)
    {
        return "PAPER_SHADOW_OK";
    }
    "RESEARCH_ONLY"
}

fn text_at<'a>(value: &'a Value, pointers: &[&str]) -> Option<&'a str> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
}

fn number_at(value: &Value, pointers: &[&str]) -> Option<Value> {
    pointers.iter().find_map(|pointer| {
        let value = value.pointer(pointer)?;
        if value.is_number() || value.is_string() {
            Some(value.clone())
        } else {
            None
        }
    })
}

fn select_regime_profile_net(report: Option<&Value>, profile: &str) -> Option<String> {
    let report = report?;
    [
        "/result/comparisons",
        "/result/profiles",
        "/result/regime_conditioned_profiles/result/comparisons",
        "/result/regime_conditioned_profiles/result/profiles",
    ]
    .into_iter()
    .find_map(|pointer| profile_net_in_rows(report.pointer(pointer), profile))
}

fn find_regime_profile<'a>(report: &'a Value, profile: &str) -> Option<&'a Value> {
    [
        "/result/comparisons",
        "/result/profiles",
        "/result/regime_conditioned_profiles/result/comparisons",
        "/result/regime_conditioned_profiles/result/profiles",
    ]
    .into_iter()
    .find_map(|pointer| {
        report
            .pointer(pointer)?
            .as_array()?
            .iter()
            .find(|row| row.get("profile").and_then(Value::as_str) == Some(profile))
    })
}

fn profile_net_in_rows(rows: Option<&Value>, profile: &str) -> Option<String> {
    rows?.as_array()?.iter().find_map(|row| {
        let map = row.as_object()?;
        if map.get("profile").and_then(Value::as_str) != Some(profile) {
            return None;
        }
        map.get("net_pnl")
            .and_then(value_to_string)
            .or_else(|| map.get("delta_vs_static").and_then(value_to_string))
    })
}

fn select_fill_model_net(report: Option<&Value>, fill_model: &str) -> Option<String> {
    let report = report?;
    [
        "/result/fill_models",
        "/result/fill_model_sensitivity",
        "/result/baseline_static_strategy/result/fill_models",
    ]
    .into_iter()
    .find_map(|pointer| fill_model_net_in_rows(report.pointer(pointer), fill_model))
}

fn fill_model_net_in_rows(rows: Option<&Value>, fill_model: &str) -> Option<String> {
    rows?.as_array()?.iter().find_map(|row| {
        let map = row.as_object()?;
        if map.get("fill_model").and_then(Value::as_str) != Some(fill_model) {
            return None;
        }
        map.get("net_pnl").and_then(value_to_string)
    })
}

fn find_any_text(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Object(map) => {
            if let Some(found) = map.get(key).and_then(value_to_string) {
                return Some(found);
            }
            map.values().find_map(|child| find_any_text(child, key))
        }
        Value::Array(values) => values.iter().find_map(|child| find_any_text(child, key)),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod wallet_metric_tests {
    use super::*;

    #[test]
    fn daily_row_and_profitability_evaluator_use_dynamic_wallet_metrics() {
        let regimes = json!({
            "result": {
                "fill_model": "queue_proxy_conservative",
                "comparisons": [{
                    "profile": "dynamic_quote_style",
                    "net_pnl": "100",
                    "wallet_constrained": true,
                    "wallet_constrained_net_pnl": "0.25"
                }]
            }
        });
        let cumulative_wallet = json!({
            "wallet_scope": CUMULATIVE_WALLET_SCOPE,
            "campaign_start": WALLET_CAMPAIGN_START,
            "snapshot_date": "2026-07-12",
            "cumulative_input_sha256": format!("sha256:{}", "a".repeat(64)),
            "cumulative_state_sha256": format!("sha256:{}", "c".repeat(64)),
            "cumulative_events": 10,
            "wallet_constrained": true,
            "wallet_constrained_net_pnl": "0.25",
            "wallet_constrained_ending_equity": "5.280521",
            "wallet_constrained_max_drawdown": "0",
            "wallet_constrained_unresolved_orders": 0
        });
        let row = json_row(
            "2026-07-12",
            DailyReportSources {
                final_report: None,
                regimes: Some(&regimes),
                baseline: None,
            },
            None,
            None,
            None,
            Some(&cumulative_wallet),
        )
        .unwrap();

        assert_eq!(row["dynamic_quote_style_net_pnl"], "100");
        assert_eq!(row["wallet_constrained"], true);
        assert_eq!(row["wallet_constrained_net_pnl"], "0.25");

        let metrics = aggregate_profitability_metrics(
            &[row],
            &json!({
                "result": {
                    "pnl_ci_95_low": "0.01",
                    "markout_30s_ci_low": "0.01",
                    "decision_parity_rate": "1"
                }
            }),
            &json!({
                "evidence_protocol_version": 3,
                "sample_size": 100,
                "positive_fills": 10,
                "negative_non_fills": 90,
                "brier_improvement_fraction": "0.05",
                "expected_calibration_error": "0.10",
                "promotion_ready": true,
                "net_executable_markout_30s_lower_confidence_bound_95": "0.01"
            }),
            &PromotionThresholds::default(),
        );
        assert_eq!(metrics.queue_conservative_net_pnl, d("100"));
        assert_eq!(metrics.wallet_constrained_net_pnl, d("0.25"));
        assert_eq!(metrics.wallet_constrained_ending_equity, d("5.280521"));
    }

    #[test]
    fn cumulative_wallet_never_sums_reset_daily_profit_and_blocks_capital_lock() {
        fn row(
            date: &str,
            pnl: &str,
            equity: &str,
            drawdown: &str,
            events: u64,
            unresolved: u64,
        ) -> Value {
            json!({
                "date": date,
                "settled_markets": 10,
                "fill_model": "queue_proxy_conservative",
                // A reset-per-day implementation would incorrectly sum these to +2.
                "dynamic_quote_style_net_pnl": "1",
                "wallet_scope": CUMULATIVE_WALLET_SCOPE,
                "wallet_campaign_start": WALLET_CAMPAIGN_START,
                "wallet_snapshot_date": date,
                "cumulative_input_sha256": format!("sha256:{}", if date.ends_with("12") { "a".repeat(64) } else { "b".repeat(64) }),
                "cumulative_state_sha256": format!("sha256:{}", if date.ends_with("12") { "c".repeat(64) } else { "d".repeat(64) }),
                "cumulative_events": events,
                "wallet_constrained": true,
                "wallet_constrained_net_pnl": pnl,
                "wallet_constrained_ending_equity": equity,
                "wallet_constrained_max_drawdown": drawdown,
                "wallet_constrained_unresolved_orders": unresolved,
                "data_quality": DataQualitySummary::new(100, Decimal::ONE, Vec::new(), Vec::<String>::new())
            })
        }
        let rows = vec![
            row("2026-07-12", "1", "6.030521", "0", 100, 0),
            row("2026-07-13", "-0.5", "4.530521", "1.5", 200, 1),
        ];
        let metrics = aggregate_profitability_metrics(
            &rows,
            &json!({"result":{"pnl_ci_95_low":"0.01","markout_30s_ci_low":"0.01","decision_parity_rate":"1"}}),
            &json!({
                "evidence_protocol_version": 3,
                "sample_size": 100,
                "positive_fills": 10,
                "negative_non_fills": 90,
                "brier_improvement_fraction": "0.05",
                "expected_calibration_error": "0.10",
                "promotion_ready": true,
                "net_executable_markout_30s_lower_confidence_bound_95": "0.01"
            }),
            &PromotionThresholds::default(),
        );
        assert_eq!(metrics.queue_conservative_net_pnl, d("2"));
        assert_eq!(metrics.wallet_constrained_net_pnl, d("-0.5"));
        assert_eq!(metrics.wallet_constrained_ending_equity, d("4.530521"));
        assert_eq!(metrics.max_drawdown, d("1.5"));
        assert!(!metrics.wallet_constrained);
        assert!(metrics
            .missing_metrics
            .contains(&"cumulative_wallet_positions_resolved".to_owned()));

        let mut missing_state = rows;
        missing_state[1]
            .as_object_mut()
            .unwrap()
            .remove("cumulative_state_sha256");
        let invalid = aggregate_profitability_metrics(
            &missing_state,
            &json!({}),
            &json!({}),
            &PromotionThresholds::default(),
        );
        assert!(!invalid.wallet_constrained);
        assert!(invalid
            .missing_metrics
            .contains(&"valid_cumulative_wallet_ledger".to_owned()));
    }

    #[test]
    fn clean_day_streak_resets_on_date_gap_and_dirty_day() {
        let clean = DataQualitySummary::new(100, Decimal::ONE, Vec::new(), Vec::<String>::new());
        let dirty =
            DataQualitySummary::new(100, Decimal::new(90, 2), Vec::new(), Vec::<String>::new());
        let row = |date: &str, quality: &DataQualitySummary| json!({"date": date, "data_quality": quality});
        assert_eq!(
            consecutive_clean_day_streak(&[row("2026-07-12", &clean), row("2026-07-13", &clean)]),
            2
        );
        assert_eq!(
            consecutive_clean_day_streak(&[row("2026-07-12", &clean), row("2026-07-14", &clean)]),
            1
        );
        assert_eq!(
            consecutive_clean_day_streak(&[
                row("2026-07-12", &clean),
                row("2026-07-13", &dirty),
                row("2026-07-14", &clean)
            ]),
            1
        );
    }

    #[test]
    fn clean_day_streak_resets_on_gap_or_dirty_day() {
        fn row(date: &str, clean: bool) -> Value {
            let quality = if clean {
                DataQualitySummary::new(100, Decimal::ONE, Vec::new(), Vec::<String>::new())
            } else {
                DataQualitySummary::new(
                    100,
                    Decimal::ONE,
                    vec!["fatal_test_gap".to_owned()],
                    Vec::<String>::new(),
                )
            };
            json!({"date": date, "data_quality": quality})
        }

        assert_eq!(
            consecutive_clean_day_streak(&[
                row("2026-07-12", true),
                row("2026-07-13", true),
                row("2026-07-15", true),
            ]),
            1
        );
        assert_eq!(
            consecutive_clean_day_streak(&[
                row("2026-07-12", true),
                row("2026-07-13", false),
                row("2026-07-14", true),
                row("2026-07-15", true),
            ]),
            2
        );
    }
}

fn collect_replay_index_inputs(input: &Path) -> Result<Value, ResearchError> {
    if input.to_string_lossy().starts_with("azure://") {
        return Ok(json!({
            "source": input.to_string_lossy(),
            "listed_locally": false,
            "files": []
        }));
    }
    if !input.exists() {
        return Ok(json!({
            "source": input.to_string_lossy(),
            "listed_locally": false,
            "files": [],
            "warning": "input path does not exist"
        }));
    }
    let mut files = Vec::new();
    collect_event_files(input, &mut files)?;
    files.sort();
    let total_bytes = files
        .iter()
        .filter_map(|path| fs::metadata(path).ok().map(|metadata| metadata.len()))
        .sum::<u64>();
    Ok(json!({
        "source": input.to_string_lossy(),
        "listed_locally": true,
        "file_count": files.len(),
        "total_bytes": total_bytes,
        "files": files.into_iter().take(500).map(|path| path.to_string_lossy().into_owned()).collect::<Vec<_>>()
    }))
}

fn collect_event_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), ResearchError> {
    if path.is_file() {
        if is_event_data_path(path) {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_event_files(&path, files)?;
        } else if is_event_data_path(&path) {
            files.push(path);
        }
    }
    Ok(())
}

fn is_event_data_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".jsonl") || name.ends_with(".jsonl.gz"))
}

fn validate_backfill_task(task: &str) -> Result<(), ResearchError> {
    match task {
        "normalize" | "markets" | "reports" | "replay-index" | "all" => Ok(()),
        other => Err(ResearchError::InvalidInput(format!(
            "unsupported backfill task: {other}"
        ))),
    }
}

fn validate_date(value: &str, name: &str) -> Result<(), ResearchError> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map(|_| ())
        .map_err(|error| ResearchError::InvalidInput(format!("invalid {name} date: {error}")))
}

fn prospective_markdown(report: &Value) -> String {
    let rows = report["result"]["rows"].as_array().map_or(0, Vec::len);
    format!(
        "# Prospective Validation\n\n- Status: `{}`\n- Since: `{}`\n- Daily rows: {}\n- Frozen candidates: `{}`\n\nNo parameter search, test-day re-ranking, live promotion, or live trading is allowed from this report.\n",
        report["result"]["status"].as_str().unwrap_or("collecting"),
        report["result"]["since"].as_str().unwrap_or("unknown"),
        rows,
        FROZEN_CANDIDATE_NAMES.join("`, `")
    )
}

fn backfill_markdown(report: &Value) -> String {
    format!(
        "# Manual Backfill Plan\n\n- Status: `{}`\n- Date range: `{}` to `{}`\n- Task: `{}`\n\nRaw event blobs are not mutated. This plan is manual-only and research-only.\n",
        report["result"]["status"].as_str().unwrap_or("planned"),
        report["result"]["start"].as_str().unwrap_or("unknown"),
        report["result"]["end"].as_str().unwrap_or("unknown"),
        report["result"]["task"].as_str().unwrap_or("unknown")
    )
}

fn chart_backfill_markdown(report: &Value) -> String {
    format!(
        "# Chart Backfill\n\n- Status: `{}`\n- Markets: {}\n- Chart points: {}\n- Decision markers: {}\n- Fill markers: {}\n- Output: `{}`\n\nThis is a derived research/observability artifact. Raw event blobs are not mutated and live trading remains disabled.\n",
        report["result"]["status"].as_str().unwrap_or("unknown"),
        report["result"]["chart_store"]["market_count"]
            .as_u64()
            .unwrap_or(0),
        report["result"]["chart_store"]["point_count"]
            .as_u64()
            .unwrap_or(0),
        report["result"]["chart_store"]["decision_marker_count"]
            .as_u64()
            .unwrap_or(0),
        report["result"]["chart_store"]["fill_marker_count"]
            .as_u64()
            .unwrap_or(0),
        report["result"]["artifacts"][0]["path"]
            .as_str()
            .unwrap_or("unknown")
    )
}

#[cfg(test)]
mod data_quality_tests {
    use super::*;

    #[test]
    fn informational_inventory_and_negligible_timestamp_disorder_are_healthy() {
        let audit = json!({
            "result": {
                "fatal_data_quality_issues": [],
                "total_events": 100_000_000,
                "malformed_lines": 0,
                "duplicate_estimate": 0,
                "out_of_order_timestamps": 8,
                "stale_reference_count": 0,
                "missing_market_ids": 0,
                "start_price_capture_rate": "0.99",
                "settlement_rate": "0.99",
                "warnings": [
                    "azure input listed 1440 blobs / 1 bytes from azure://example",
                    "0 events skipped by 1 excluded event-time window(s)",
                    "out-of-order timestamp in events/2026/06/15/04/02.jsonl",
                    "8 out-of-order timestamps"
                ],
                "notices": ["azure blob inventory loaded"]
            }
        });
        assert_eq!(data_quality_status(Some(&audit)), "healthy");
        assert!(data_quality_reasons(Some(&audit)).is_empty());
    }

    #[test]
    fn material_capture_gaps_remain_warnings() {
        let audit = json!({
            "result": {
                "fatal_data_quality_issues": [],
                "total_events": 100_000,
                "malformed_lines": 0,
                "duplicate_estimate": 0,
                "out_of_order_timestamps": 0,
                "stale_reference_count": 0,
                "missing_market_ids": 0,
                "start_price_capture_rate": "0.82",
                "settlement_rate": "0.91",
                "warnings": []
            }
        });
        assert_eq!(data_quality_status(Some(&audit)), "warning");
        assert_eq!(
            data_quality_reasons(Some(&audit)),
            vec![
                json!("settlement_coverage_below_95pct"),
                json!("start_price_capture_below_95pct")
            ]
        );
    }
}
