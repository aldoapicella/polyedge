use chrono::{DateTime, Datelike, Duration, SecondsFormat, Timelike, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use polyedge_config::RuntimeSettings;
use polyedge_engine::{
    crypto_taker_fee_per_share, QuoteStyle, RegimeClassifier, RegimeFeatures, RegimePolicy,
};
use polyedge_storage::{AzureBlobClient, AzureBlobItem};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Instant;
use thiserror::Error;

const SETTLEMENT_WINDOW_SECONDS: i64 = 15;
const MAX_DUPLICATE_HASHES: usize = 100_000;
const MAX_STREAM_WARNINGS: usize = 1_000;
const DEFAULT_AZURE_PREFETCH_BLOBS: usize = 4;
const MAX_AZURE_PREFETCH_BLOBS: usize = 32;
const DEFAULT_EVENT_TIME_REORDER_BUFFER_EVENTS: usize = 8_192;
const MAX_EVENT_TIME_REORDER_BUFFER_EVENTS: usize = 1_048_576;
const NORMALIZE_PROGRESS_INTERVAL_EVENTS: usize = 100_000;
const ADAPTIVE_LOG_LIMIT: usize = 100;
const REFERENCE_HISTORY_SECONDS: i64 = 130;
const QUEUE_EVIDENCE_KEYS: &[&str] = &[
    "queue_position",
    "queue_ahead",
    "queue_size_ahead",
    "size_ahead",
    "order_queue",
    "queue_depth",
];
const TRADE_EVIDENCE_KEYS: &[&str] = &[
    "trade_id",
    "trade_price",
    "trade_size",
    "last_trade_price",
    "last_trade_size",
    "fill_size",
    "filled_size",
];
const DEPLETION_EVIDENCE_KEYS: &[&str] = &[
    "depleted_size",
    "size_depleted",
    "ask_depletion",
    "bid_depletion",
    "level_depletion",
    "previous_size",
];
const REDACTED: &str = "[redacted]";
const SECRET_KEY_FRAGMENTS: &[&str] = &[
    "secret",
    "password",
    "credential",
    "authorization",
    "private_key",
    "api_key",
    "account_key",
    "connection_string",
    "access_token",
    "refresh_token",
    "sas_token",
];

#[derive(Debug, Error)]
pub enum ResearchError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid fill model: {0}")]
    InvalidFillModel(String),
    #[error("azure input error: {0}")]
    Azure(String),
    #[error("{0}")]
    InvalidInput(String),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FillModel {
    NoMakerFills,
    Touch,
    TouchAfter250Ms,
    TouchAfter1000Ms,
    TradeThrough,
    QueueProxy,
    AdverseSelectionPenalized,
}

impl FillModel {
    pub fn all_baseline() -> Vec<Self> {
        vec![
            Self::NoMakerFills,
            Self::Touch,
            Self::TouchAfter250Ms,
            Self::TouchAfter1000Ms,
            Self::TradeThrough,
            Self::QueueProxy,
            Self::AdverseSelectionPenalized,
        ]
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoMakerFills => "no_maker_fills",
            Self::Touch => "touch",
            Self::TouchAfter250Ms => "touch_after_250ms",
            Self::TouchAfter1000Ms => "touch_after_1000ms",
            Self::TradeThrough => "trade_through",
            Self::QueueProxy => "queue_proxy",
            Self::AdverseSelectionPenalized => "adverse_selection_penalized",
        }
    }

    fn live_after_ms(self) -> i64 {
        match self {
            Self::TouchAfter250Ms | Self::AdverseSelectionPenalized => 250,
            Self::TouchAfter1000Ms => 1000,
            _ => 0,
        }
    }
}

impl fmt::Display for FillModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for FillModel {
    type Err = ResearchError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "no_maker_fills" | "none" => Ok(Self::NoMakerFills),
            "touch" => Ok(Self::Touch),
            "touch_after_250ms" | "touch-after-250ms" => Ok(Self::TouchAfter250Ms),
            "touch_after_1000ms" | "touch-after-1000ms" => Ok(Self::TouchAfter1000Ms),
            "trade_through" | "trade-through" => Ok(Self::TradeThrough),
            "queue_proxy" | "queue-proxy" => Ok(Self::QueueProxy),
            "adverse_selection_penalized" | "adverse-selection-penalized" => {
                Ok(Self::AdverseSelectionPenalized)
            }
            other => Err(ResearchError::InvalidFillModel(other.to_owned())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExcludedTimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl ExcludedTimeWindow {
    pub fn parse(value: &str) -> Result<Self, ResearchError> {
        let Some((start, end)) = value.split_once("..") else {
            return Err(ResearchError::InvalidInput(format!(
                "exclude window must be START..END, got {value}"
            )));
        };
        let start = parse_rfc3339_utc(start.trim()).ok_or_else(|| {
            ResearchError::InvalidInput(format!("invalid exclude window start: {start}"))
        })?;
        let end = parse_rfc3339_utc(end.trim()).ok_or_else(|| {
            ResearchError::InvalidInput(format!("invalid exclude window end: {end}"))
        })?;
        if start >= end {
            return Err(ResearchError::InvalidInput(format!(
                "exclude window start must be before end: {value}"
            )));
        }
        Ok(Self { start, end })
    }

    fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        timestamp >= self.start && timestamp < self.end
    }

    fn as_json(&self) -> Value {
        json!({
            "start": ts(self.start),
            "end_exclusive": ts(self.end)
        })
    }
}

#[derive(Clone, Debug)]
pub struct AuditOptions {
    pub input: PathBuf,
    pub out: PathBuf,
    pub markdown: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct NormalizeOptions {
    pub input: PathBuf,
    pub out: PathBuf,
    pub format: String,
    pub overwrite: bool,
}

#[derive(Clone, Debug)]
pub struct BuildMarketsOptions {
    pub input: PathBuf,
    pub out: PathBuf,
    pub markdown: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct ReplayOptions {
    pub input: PathBuf,
    pub markets: Option<PathBuf>,
    pub strategy_config: Option<PathBuf>,
    pub fill_model: FillModel,
    pub out: PathBuf,
    pub markdown: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct BaselineOptions {
    pub input: PathBuf,
    pub markets: Option<PathBuf>,
    pub out: PathBuf,
    pub markdown: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct RegimesOptions {
    pub input: PathBuf,
    pub markets: Option<PathBuf>,
    pub fill_model: FillModel,
    pub profile_config: Option<PathBuf>,
    pub out: PathBuf,
    pub markdown: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct SweepOptions {
    pub input: PathBuf,
    pub markets: Option<PathBuf>,
    pub search: Option<PathBuf>,
    pub split: String,
    pub max_experiments: usize,
    pub out: PathBuf,
    pub markdown: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct CalibrationOptions {
    pub input: PathBuf,
    pub markets: Option<PathBuf>,
    pub out: PathBuf,
    pub markdown: PathBuf,
    pub exclude_windows: Vec<ExcludedTimeWindow>,
}

#[derive(Clone, Debug)]
pub struct SampleSizeOptions {
    pub results: PathBuf,
    pub out: PathBuf,
    pub markdown: PathBuf,
}

#[derive(Clone, Debug)]
pub struct FinalReportOptions {
    pub reports_dir: PathBuf,
    pub out: PathBuf,
    pub markdown: PathBuf,
}

#[derive(Clone, Debug)]
pub struct MlCalibrateOptions {
    pub out: PathBuf,
    pub markdown: PathBuf,
}

pub fn run_audit(options: AuditOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let mut audit = AuditAccumulator::default();
    let stream = stream_events(
        &options.input,
        EventPathMode::PreferEventsJsonl,
        &options.exclude_windows,
        |event| {
            audit.observe(event);
        },
    )?;
    audit.malformed_lines = stream.malformed_lines;
    audit.duplicate_estimate = stream.duplicate_estimate;
    let stream_warnings = stream
        .warnings
        .iter()
        .cloned()
        .map(Value::String)
        .collect::<Vec<_>>();
    let mut result = audit.finish();
    let mut warnings = result
        .get("warnings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    warnings.extend(stream_warnings.clone());
    warnings.extend(exclusion_warnings(&stream, &options.exclude_windows));
    if let Some(object) = result.as_object_mut() {
        object.insert("warnings".to_owned(), Value::Array(warnings.clone()));
        object.insert("stream_warnings".to_owned(), Value::Array(stream_warnings));
        insert_exclusion_metadata(object, &stream, &options.exclude_windows);
    }
    let report = envelope(
        "polyedge-rs research audit",
        &options.input,
        "none",
        "none",
        start.elapsed(),
        warnings,
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &audit_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_normalize(options: NormalizeOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let file_format = NormalizedFileFormat::parse(&options.format)?;
    if options.out.exists() && !options.overwrite {
        return Err(ResearchError::InvalidInput(format!(
            "{} exists; pass --overwrite to replace generated research files",
            options.out.display()
        )));
    }
    if options.out.exists() {
        fs::remove_dir_all(&options.out)?;
    }
    fs::create_dir_all(&options.out)?;
    let mut writers = NormalizedWriters::new(&options.out, file_format)?;
    let mut counts = BTreeMap::<String, usize>::new();
    let mut first_ts = None;
    let mut last_ts = None;
    let mut processed_events = 0_usize;
    let mut write_error = None::<String>;
    let stream = stream_events(&options.input, EventPathMode::AllJsonl, &[], |event| {
        first_ts = min_ts(first_ts, Some(event.recorded_ts));
        last_ts = max_ts(last_ts, Some(event.recorded_ts));
        processed_events += 1;
        *counts.entry(event.event_type.clone()).or_insert(0) += 1;
        if write_error.is_none() {
            if let Err(error) = writers.write(event) {
                write_error = Some(error.to_string());
            }
        }
        if write_error.is_none()
            && is_multiple_of(processed_events, NORMALIZE_PROGRESS_INTERVAL_EVENTS)
        {
            if let Err(error) = write_json_file(
                &options.out.join("normalize_progress.json"),
                &normalize_progress(
                    "running",
                    file_format,
                    processed_events,
                    &counts,
                    first_ts,
                    last_ts,
                ),
            ) {
                write_error = Some(error.to_string());
            }
        }
    })?;
    if let Some(error) = write_error {
        return Err(ResearchError::InvalidInput(error));
    }
    writers.flush()?;
    write_json_file(
        &options.out.join("normalize_progress.json"),
        &normalize_progress(
            "completed",
            file_format,
            stream.events,
            &counts,
            first_ts,
            last_ts,
        ),
    )?;
    let manifest = json!({
        "format": file_format.as_str(),
        "compression": file_format.compression(),
        "event_log_written": file_format.writes_event_log(),
        "input": options.input.to_string_lossy(),
        "generated_at": now_ts(),
        "backend": "rust",
        "files": writers.manifest(),
        "events": stream.events,
        "malformed_lines": stream.malformed_lines,
        "event_counts": counts,
        "first_recorded_ts": first_ts.map(ts),
        "last_recorded_ts": last_ts.map(ts),
        "warnings": stream.warnings
    });
    write_json_file(&options.out.join("events_manifest.json"), &manifest)?;
    let report = envelope(
        "polyedge-rs research normalize",
        &options.input,
        "none",
        "none",
        start.elapsed(),
        stream.warnings.into_iter().map(Value::String).collect(),
        manifest,
    );
    Ok(report)
}

pub fn run_build_markets(options: BuildMarketsOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let market_rows = build_market_rows(&options.input, &options.exclude_windows)?;
    let rows = market_rows.rows;
    let summary = market_summary(&rows);
    let result = json!({
        "markets": rows.iter().map(MarketTruth::as_json).collect::<Vec<_>>(),
        "summary": summary,
        "excluded_event_count": market_rows.stream.excluded_events,
        "excluded_time_windows": exclusion_windows_json(&options.exclude_windows),
    });
    let mut warnings = result["summary"]["warnings"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    warnings.extend(exclusion_warnings(
        &market_rows.stream,
        &options.exclude_windows,
    ));
    let report = envelope(
        "polyedge-rs research build-markets",
        &options.input,
        "none",
        "none",
        start.elapsed(),
        warnings,
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &markets_markdown(&report),
    )?;
    if let Some(parent) = options.markdown.parent() {
        let summary_json = parent.join("markets_summary.json");
        if summary_json != options.out {
            write_json_file(&summary_json, &report)?;
        }
    }
    Ok(report)
}

pub fn run_replay(options: ReplayOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let markets = load_market_truth(options.markets.as_deref())?;
    let settings = RuntimeSettings::default();
    let request = ReplayRequest {
        name: options.fill_model.as_str().to_owned(),
        fill_model: options.fill_model,
        mode: StrategyProfileMode::Static,
        settings,
    };
    let mut results = run_replay_requests(
        &options.input,
        &markets,
        vec![request],
        &options.exclude_windows,
    )?;
    let result = results.pop().unwrap_or_else(empty_replay_result);
    let report = envelope(
        "polyedge-rs research replay",
        &options.input,
        options.fill_model.as_str(),
        "none",
        start.elapsed(),
        result["warnings"].as_array().cloned().unwrap_or_default(),
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &replay_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_baseline(options: BaselineOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let markets = load_market_truth(options.markets.as_deref())?;
    let settings = RuntimeSettings::default();
    let requests = FillModel::all_baseline()
        .into_iter()
        .map(|fill_model| ReplayRequest {
            name: fill_model.as_str().to_owned(),
            fill_model,
            mode: StrategyProfileMode::Static,
            settings: settings.clone(),
        })
        .collect::<Vec<_>>();
    let results =
        run_replay_requests(&options.input, &markets, requests, &options.exclude_windows)?;
    let result = json!({
        "fill_models": results,
        "primary_unit": "settled_market_net_pnl",
        "selection_warning": "Do not claim profitability if only optimistic fill models win."
    });
    let report = envelope(
        "polyedge-rs research baseline",
        &options.input,
        "all",
        "none",
        start.elapsed(),
        collect_child_warnings(&result["fill_models"]),
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &baseline_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_regimes(options: RegimesOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let markets = load_market_truth(options.markets.as_deref())?;
    let settings = RuntimeSettings::default();
    let modes = [
        StrategyProfileMode::Static,
        StrategyProfileMode::DynamicSafetyOnly,
        StrategyProfileMode::DynamicQuoteStyle,
        StrategyProfileMode::FullDeterministic,
    ];
    let requests = modes
        .into_iter()
        .map(|mode| ReplayRequest {
            name: mode.as_str().to_owned(),
            fill_model: options.fill_model,
            mode,
            settings: settings.clone(),
        })
        .collect::<Vec<_>>();
    let results =
        run_replay_requests(&options.input, &markets, requests, &options.exclude_windows)?;
    let static_net = results
        .iter()
        .find(|row| row["profile"].as_str() == Some("static"))
        .and_then(|row| row["net_pnl"].as_str())
        .map(decimal_from_str)
        .unwrap_or(Decimal::ZERO);
    let comparisons = results
        .iter()
        .map(|row| {
            let net = row["net_pnl"]
                .as_str()
                .map(decimal_from_str)
                .unwrap_or(Decimal::ZERO);
            json!({
                "profile": row["profile"],
                "net_pnl": net.to_string(),
                "delta_vs_static": (net - static_net).to_string(),
                "regime_frequency": row["regime_frequency"],
                "regime_time_share": row["regime_time_share"],
                "warnings": row["warnings"]
            })
        })
        .collect::<Vec<_>>();
    let result = json!({
        "fill_model": options.fill_model.as_str(),
        "profiles": results,
        "comparisons": comparisons,
        "profile_config": options.profile_config.map(|path| path.to_string_lossy().into_owned()),
        "research_only": true,
        "live_deployment_allowed": false
    });
    let report = envelope(
        "polyedge-rs research regimes",
        &options.input,
        options.fill_model.as_str(),
        "none",
        start.elapsed(),
        collect_child_warnings(&result["profiles"]),
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &regimes_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_sweep(options: SweepOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let markets = load_market_truth(options.markets.as_deref())?;
    let settings = RuntimeSettings::default();
    let candidates = sweep_candidates(options.max_experiments.max(1));
    let requests = candidates
        .iter()
        .flat_map(|candidate| {
            [
                ReplayRequest {
                    name: format!("{}__touch_after_250ms", candidate.name),
                    fill_model: FillModel::TouchAfter250Ms,
                    mode: StrategyProfileMode::StaticSweep(candidate.clone()),
                    settings: settings.clone(),
                },
                ReplayRequest {
                    name: format!("{}__trade_through", candidate.name),
                    fill_model: FillModel::TradeThrough,
                    mode: StrategyProfileMode::StaticSweep(candidate.clone()),
                    settings: settings.clone(),
                },
            ]
        })
        .collect::<Vec<_>>();
    let results =
        run_replay_requests(&options.input, &markets, requests, &options.exclude_windows)?;
    let (plan, mut split_warnings) = split_plan(&results, &options.split);
    let mut grouped = group_sweep_results(results);
    add_split_metrics(&mut grouped, &plan);
    split_warnings.push(json!(
        "coarse deterministic search over logged decisions; no live deployment"
    ));
    let result = json!({
        "split_method": options.split,
        "split_plan": plan,
        "search": options.search.map(|path| path.to_string_lossy().into_owned()),
        "max_experiments": options.max_experiments,
        "candidates": grouped,
        "selection_rule": "Rank on validation only; test is reported, not optimized.",
        "robust_candidate_rule": "Validation positive under touch_after_250ms and trade_through, test does not collapse, and CI lower bound remains above zero.",
        "warnings": split_warnings.clone()
    });
    let report = envelope(
        "polyedge-rs research sweep",
        &options.input,
        "touch_after_250ms,trade_through",
        &options.split,
        start.elapsed(),
        split_warnings,
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &sweep_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_calibration(options: CalibrationOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let mut calibration =
        CalibrationAccumulator::new(load_market_truth(options.markets.as_deref())?);
    let stream = stream_events(
        &options.input,
        EventPathMode::Calibration,
        &options.exclude_windows,
        |event| {
            calibration.observe(event);
        },
    )?;
    calibration.add_stream_warnings(stream.warnings.clone());
    calibration.add_stream_warnings(
        exclusion_warnings(&stream, &options.exclude_windows)
            .into_iter()
            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
            .collect(),
    );
    let result = calibration.finish();
    let mut result = result;
    if let Some(object) = result.as_object_mut() {
        insert_exclusion_metadata(object, &stream, &options.exclude_windows);
    }
    let report = envelope(
        "polyedge-rs research calibration",
        &options.input,
        "none",
        "none",
        start.elapsed(),
        result["warnings"].as_array().cloned().unwrap_or_default(),
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &calibration_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_sample_size(options: SampleSizeOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let source = read_json_file(&options.results)?;
    let pnls = extract_market_pnls(&source);
    let stats = sample_size_stats(&pnls);
    let result = json!({
        "results": options.results.to_string_lossy(),
        "sample_unit": "settled_market_net_pnl",
        "statistics": stats,
        "profitability_claim_allowed": stats["ci_low"].as_str().is_some_and(|value| decimal_from_str(value) > Decimal::ZERO)
    });
    let report = envelope(
        "polyedge-rs research sample-size",
        &options.results,
        "none",
        "none",
        start.elapsed(),
        Vec::new(),
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &sample_size_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_final_report(options: FinalReportOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    fs::create_dir_all(&options.reports_dir)?;
    let audit = read_optional_json(&options.reports_dir.join("data_audit.json"))?;
    let markets = read_optional_json(&options.reports_dir.join("markets_summary.json"))?;
    let baseline = read_optional_json(
        &options
            .reports_dir
            .join("baseline_static_all_fill_models.json"),
    )?;
    let regimes = read_optional_json(&options.reports_dir.join("regime_profiles.json"))?;
    let sweep = read_optional_json(&options.reports_dir.join("parameter_sweep.json"))?;
    let calibration = read_optional_json(&options.reports_dir.join("calibration.json"))?;
    let sample_size = read_optional_json(&options.reports_dir.join("sample_size.json"))?;
    let recommendation = choose_recommendation(&baseline, &regimes, &sample_size);
    let result = json!({
        "executive_summary": {
            "backend": "rust",
            "research_only": true,
            "live_trading_enabled": false,
            "adaptive_profiles_live_deployment_allowed": false,
            "recommendation": recommendation
        },
        "data_coverage": audit,
        "market_truth_table": markets,
        "baseline_static_strategy": baseline,
        "fill_model_sensitivity": baseline.as_ref().and_then(|value| value.pointer("/result/fill_models")).cloned(),
        "regime_conditioned_profiles": regimes,
        "parameter_sweep": sweep,
        "calibration": calibration,
        "ml_experiments": Value::Null,
        "statistical_evidence": sample_size,
        "risks_and_measurement_weaknesses": [
            "No adaptive profile is enabled for live trading.",
            "Full 120GB/five-day dataset must be mounted or normalized before relying on final conclusions.",
            "QueueProxy is reported as infeasible unless order-book depletion evidence is present.",
            "Evaluation uses event-time replay and excludes final settlement from decision-time features."
        ],
        "next_10_actions": [
            "Run audit on the complete raw dataset.",
            "Normalize the complete raw dataset into data/research/normalized.",
            "Build the complete market truth table.",
            "Run baseline across all fill models.",
            "Run regime profiles on touch_after_250ms and trade_through.",
            "Run the coarse sweep with walk-forward splits.",
            "Review calibration by q bucket and time-to-expiry bucket.",
            "Check sample-size CI before making any strategy change.",
            "Keep adaptive profiles disabled outside research until evidence is conclusive.",
            "Only consider paper-only activation after deterministic reports are green."
        ]
    });
    let report = envelope(
        "polyedge-rs research report",
        &options.reports_dir,
        "none",
        "combined",
        start.elapsed(),
        Vec::new(),
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &final_report_markdown(&report),
    )?;
    Ok(report)
}

pub fn run_ml_calibrate(options: MlCalibrateOptions) -> Result<Value, ResearchError> {
    let start = Instant::now();
    let result = json!({
        "status": "skipped",
        "reason": "optional ML calibration was not run; core deterministic research lab is the required path",
        "allowed_models": ["logistic_regression", "isotonic_calibration"],
        "forbidden_models": ["llm_trade_decisions", "deep_learning_runtime_policy"],
        "research_only": true,
        "live_deployment_allowed": false
    });
    let report = envelope(
        "polyedge-rs research ml-calibrate",
        Path::new("reports/research"),
        "none",
        "none",
        start.elapsed(),
        vec![json!("optional ML calibration skipped")],
        result,
    );
    write_json_and_markdown(
        &options.out,
        &options.markdown,
        &report,
        &ml_calibrate_markdown(&report),
    )?;
    Ok(report)
}

#[derive(Clone, Copy)]
enum EventPathMode {
    PreferEventsJsonl,
    AllJsonl,
    MarketTruth,
    Calibration,
}

#[derive(Clone, Debug)]
struct EventLine {
    event_type: String,
    recorded_ts: DateTime<Utc>,
    payload: Value,
    raw: Value,
}

#[derive(Default)]
struct StreamStats {
    events: usize,
    excluded_events: usize,
    malformed_lines: usize,
    duplicate_estimate: usize,
    warnings: Vec<String>,
}

fn stream_events<F>(
    input: &Path,
    mode: EventPathMode,
    exclude_windows: &[ExcludedTimeWindow],
    mut visitor: F,
) -> Result<StreamStats, ResearchError>
where
    F: FnMut(&EventLine),
{
    if let Some(source) = AzureEventSource::parse(&input.to_string_lossy())? {
        return stream_azure_events(&source, exclude_windows, &mut visitor);
    }
    let path_set = collect_jsonl_path_set(input, mode)?;
    let mut stats = StreamStats::default();
    let mut seen_hashes = BTreeSet::new();
    if path_set.merge_by_event_time {
        stream_merged_event_paths(
            path_set.paths,
            exclude_windows,
            &mut stats,
            &mut seen_hashes,
            &mut visitor,
        )?;
        return Ok(stats);
    }
    for path in path_set.paths {
        let reader = open_event_reader(&path)?;
        let mut previous_ts = None;
        for line in reader.lines() {
            let line = line?;
            process_event_line(
                &line,
                &path.display().to_string(),
                &mut previous_ts,
                exclude_windows,
                &mut stats,
                &mut seen_hashes,
                &mut visitor,
            );
        }
    }
    Ok(stats)
}

#[derive(Clone, Debug)]
struct EventPathSet {
    paths: Vec<PathBuf>,
    merge_by_event_time: bool,
}

fn open_event_reader(path: &Path) -> Result<Box<dyn BufRead>, ResearchError> {
    let file = File::open(path)?;
    if is_gzip_jsonl_path(path) {
        let decoder = GzDecoder::new(file);
        Ok(Box::new(BufReader::with_capacity(
            super::REPLAY_BUFFER_BYTES,
            decoder,
        )))
    } else {
        Ok(Box::new(BufReader::with_capacity(
            super::REPLAY_BUFFER_BYTES,
            file,
        )))
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PendingEventKey {
    recorded_ts: DateTime<Utc>,
    sequence: u64,
    reader_index: usize,
    line_index: u64,
}

#[derive(Debug)]
struct PendingEventLine {
    key: PendingEventKey,
    event: EventLine,
}

struct EventReaderState {
    source: String,
    reader: Box<dyn BufRead>,
    line_index: u64,
    pending: BTreeMap<PendingEventKey, PendingEventLine>,
    exhausted: bool,
}

fn stream_merged_event_paths<F>(
    paths: Vec<PathBuf>,
    exclude_windows: &[ExcludedTimeWindow],
    stats: &mut StreamStats,
    seen_hashes: &mut BTreeSet<u64>,
    visitor: &mut F,
) -> Result<(), ResearchError>
where
    F: FnMut(&EventLine),
{
    let mut readers = paths
        .iter()
        .map(|path| {
            Ok(EventReaderState {
                source: path.display().to_string(),
                reader: open_event_reader(path)?,
                line_index: 0,
                pending: BTreeMap::new(),
                exhausted: false,
            })
        })
        .collect::<Result<Vec<_>, ResearchError>>()?;
    let reorder_window = event_time_reorder_buffer_events();
    for (reader_index, reader) in readers.iter_mut().enumerate() {
        fill_reader_pending_window(
            reader_index,
            reader,
            exclude_windows,
            stats,
            seen_hashes,
            reorder_window,
        )?;
    }
    let mut previous_ts = None;
    while let Some(reader_index) = next_reader_with_earliest_event(&readers) {
        let (_, line) = readers[reader_index]
            .pending
            .pop_first()
            .expect("reader selected with a pending event");
        let source = readers[reader_index].source.as_str();
        if previous_ts.is_some_and(|prior| line.event.recorded_ts < prior) {
            push_stream_warning(stats, format!("out-of-order timestamp in {source}"));
        }
        previous_ts = Some(line.event.recorded_ts);
        stats.events += 1;
        visitor(&line.event);
        fill_reader_pending_window(
            reader_index,
            &mut readers[reader_index],
            exclude_windows,
            stats,
            seen_hashes,
            reorder_window,
        )?;
    }
    Ok(())
}

fn event_time_reorder_buffer_events() -> usize {
    std::env::var("POLYEDGE_RESEARCH_REORDER_BUFFER_EVENTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_EVENT_TIME_REORDER_BUFFER_EVENTS)
        .clamp(1, MAX_EVENT_TIME_REORDER_BUFFER_EVENTS)
}

fn next_reader_with_earliest_event(readers: &[EventReaderState]) -> Option<usize> {
    readers
        .iter()
        .enumerate()
        .filter_map(|(index, reader)| {
            reader
                .pending
                .first_key_value()
                .map(|(key, _)| (index, key))
        })
        .min_by(|(_, left), (_, right)| left.cmp(right))
        .map(|(index, _)| index)
}

fn fill_reader_pending_window(
    reader_index: usize,
    state: &mut EventReaderState,
    exclude_windows: &[ExcludedTimeWindow],
    stats: &mut StreamStats,
    seen_hashes: &mut BTreeSet<u64>,
    reorder_window: usize,
) -> Result<(), ResearchError> {
    while state.pending.len() < reorder_window && !state.exhausted {
        let Some(line) =
            read_next_pending_event(reader_index, state, exclude_windows, stats, seen_hashes)?
        else {
            break;
        };
        state.pending.insert(line.key.clone(), line);
    }
    Ok(())
}

fn read_next_pending_event(
    reader_index: usize,
    state: &mut EventReaderState,
    exclude_windows: &[ExcludedTimeWindow],
    stats: &mut StreamStats,
    seen_hashes: &mut BTreeSet<u64>,
) -> Result<Option<PendingEventLine>, ResearchError> {
    if state.exhausted {
        return Ok(None);
    }
    loop {
        let mut line = String::new();
        let bytes = state.reader.read_line(&mut line)?;
        if bytes == 0 {
            state.exhausted = true;
            return Ok(None);
        }
        state.line_index += 1;
        if line.trim().is_empty() {
            continue;
        }
        if seen_hashes.len() < MAX_DUPLICATE_HASHES {
            let hash = stable_hash(line.as_bytes());
            if !seen_hashes.insert(hash) {
                stats.duplicate_estimate += 1;
            }
        }
        let raw = match serde_json::from_str::<Value>(&line) {
            Ok(value) => value,
            Err(_) => {
                stats.malformed_lines += 1;
                continue;
            }
        };
        let event_type = raw
            .get("event_type")
            .or_else(|| raw.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let recorded_ts = parse_datetime(raw.get("recorded_ts"))
            .or_else(|| parse_datetime(raw.get("ts")))
            .unwrap_or_else(Utc::now);
        if is_excluded_ts(recorded_ts, exclude_windows) {
            stats.excluded_events += 1;
            continue;
        }
        let sequence = raw
            .get("sequence")
            .and_then(Value::as_u64)
            .unwrap_or(state.line_index);
        let payload = raw
            .get("payload")
            .or_else(|| raw.get("raw_payload"))
            .cloned()
            .unwrap_or(Value::Null);
        return Ok(Some(PendingEventLine {
            key: PendingEventKey {
                recorded_ts,
                sequence,
                reader_index,
                line_index: state.line_index,
            },
            event: EventLine {
                event_type,
                recorded_ts,
                payload,
                raw: Value::Null,
            },
        }));
    }
}

fn process_event_line<F>(
    line: &str,
    source: &str,
    previous_ts: &mut Option<DateTime<Utc>>,
    exclude_windows: &[ExcludedTimeWindow],
    stats: &mut StreamStats,
    seen_hashes: &mut BTreeSet<u64>,
    visitor: &mut F,
) where
    F: FnMut(&EventLine),
{
    if line.trim().is_empty() {
        return;
    }
    if seen_hashes.len() < MAX_DUPLICATE_HASHES {
        let hash = stable_hash(line.as_bytes());
        if !seen_hashes.insert(hash) {
            stats.duplicate_estimate += 1;
        }
    }
    let raw = match serde_json::from_str::<Value>(line) {
        Ok(value) => value,
        Err(_) => {
            stats.malformed_lines += 1;
            return;
        }
    };
    let event_type = raw
        .get("event_type")
        .or_else(|| raw.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let recorded_ts = parse_datetime(raw.get("recorded_ts"))
        .or_else(|| parse_datetime(raw.get("ts")))
        .unwrap_or_else(Utc::now);
    if is_excluded_ts(recorded_ts, exclude_windows) {
        stats.excluded_events += 1;
        return;
    }
    if previous_ts.is_some_and(|prior| recorded_ts < prior) {
        push_stream_warning(stats, format!("out-of-order timestamp in {source}"));
    }
    *previous_ts = Some(recorded_ts);
    let payload = raw
        .get("payload")
        .or_else(|| raw.get("raw_payload"))
        .cloned()
        .unwrap_or(Value::Null);
    stats.events += 1;
    visitor(&EventLine {
        event_type,
        recorded_ts,
        payload,
        raw,
    });
}

fn push_stream_warning(stats: &mut StreamStats, warning: String) {
    match stats.warnings.len().cmp(&MAX_STREAM_WARNINGS) {
        std::cmp::Ordering::Less => stats.warnings.push(warning),
        std::cmp::Ordering::Equal => stats.warnings.push(format!(
            "additional stream warnings suppressed after {MAX_STREAM_WARNINGS} warnings"
        )),
        std::cmp::Ordering::Greater => {}
    }
}

fn is_excluded_ts(timestamp: DateTime<Utc>, windows: &[ExcludedTimeWindow]) -> bool {
    windows.iter().any(|window| window.contains(timestamp))
}

fn exclusion_windows_json(windows: &[ExcludedTimeWindow]) -> Value {
    Value::Array(windows.iter().map(ExcludedTimeWindow::as_json).collect())
}

fn insert_exclusion_metadata(
    object: &mut Map<String, Value>,
    stream: &StreamStats,
    windows: &[ExcludedTimeWindow],
) {
    object.insert(
        "excluded_event_count".to_owned(),
        json!(stream.excluded_events),
    );
    object.insert(
        "excluded_time_windows".to_owned(),
        exclusion_windows_json(windows),
    );
}

fn exclusion_warnings(stream: &StreamStats, windows: &[ExcludedTimeWindow]) -> Vec<Value> {
    if windows.is_empty() {
        return Vec::new();
    }
    vec![json!(format!(
        "{} events skipped by {} excluded event-time window(s)",
        stream.excluded_events,
        windows.len()
    ))]
}

fn stream_azure_events<F>(
    source: &AzureEventSource,
    exclude_windows: &[ExcludedTimeWindow],
    visitor: &mut F,
) -> Result<StreamStats, ResearchError>
where
    F: FnMut(&EventLine),
{
    let sas = std::env::var(&source.sas_env).map_err(|_| {
        ResearchError::Azure(format!(
            "{} must contain a read/list SAS token for azure input",
            source.sas_env
        ))
    })?;
    let client = AzureBlobClient::new(&source.account, &source.container, sas);
    let blobs = client
        .list_blobs(&source.prefix, source.max_blobs, source.max_bytes)
        .map_err(|error| ResearchError::Azure(error.to_string()))?;
    let listed_bytes = blobs.iter().map(|blob| blob.content_length).sum::<u64>();
    let mut stats = StreamStats::default();
    stats.warnings.push(format!(
        "azure input listed {} blobs / {} bytes from azure://{}/{}/{} with prefetch_blobs={}",
        blobs.len(),
        listed_bytes,
        source.account,
        source.container,
        source.prefix,
        source.worker_count(blobs.len())
    ));
    let mut seen_hashes = BTreeSet::new();
    stream_ordered_azure_blobs(client, blobs, source.prefetch_blobs, |prefetched| {
        let reader =
            BufReader::with_capacity(super::REPLAY_BUFFER_BYTES, prefetched.bytes.as_slice());
        let mut previous_ts = None;
        for line in reader.lines() {
            let line = line?;
            process_event_line(
                &line,
                &prefetched.blob.name,
                &mut previous_ts,
                exclude_windows,
                &mut stats,
                &mut seen_hashes,
                visitor,
            );
        }
        Ok(())
    })?;
    Ok(stats)
}

struct PrefetchedAzureBlob {
    index: usize,
    blob: AzureBlobItem,
    bytes: Vec<u8>,
}

fn stream_ordered_azure_blobs<F>(
    client: AzureBlobClient,
    blobs: Vec<AzureBlobItem>,
    prefetch_blobs: usize,
    mut handle_azure_blob: F,
) -> Result<(), ResearchError>
where
    F: FnMut(PrefetchedAzureBlob) -> Result<(), ResearchError>,
{
    if blobs.is_empty() {
        return Ok(());
    }
    let total_blobs = blobs.len();
    let worker_count = prefetch_blobs.max(1).min(blobs.len());
    let (job_tx, job_rx) = mpsc::channel::<(usize, AzureBlobItem)>();
    let (result_tx, result_rx) =
        mpsc::sync_channel::<Result<PrefetchedAzureBlob, ResearchError>>(worker_count);
    let job_rx = Arc::new(Mutex::new(job_rx));
    let mut handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let worker_client = client.clone();
        let worker_job_rx = Arc::clone(&job_rx);
        let worker_result_tx = result_tx.clone();
        handles.push(thread::spawn(move || {
            while let Ok((index, blob)) = worker_job_rx
                .lock()
                .map_err(|_| ())
                .and_then(|receiver| receiver.recv().map_err(|_| ()))
            {
                let result = worker_client
                    .download_blob_bytes(&blob.name)
                    .map(|bytes| PrefetchedAzureBlob { index, blob, bytes })
                    .map_err(|error| ResearchError::Azure(error.to_string()));
                if worker_result_tx.send(result).is_err() {
                    break;
                }
            }
        }));
    }
    drop(result_tx);

    let mut blob_iter = blobs.into_iter().enumerate();
    let mut pending = BTreeMap::new();
    let mut next_index = 0_usize;
    let mut in_flight = 0_usize;
    fill_azure_prefetch_window(
        &job_tx,
        &mut blob_iter,
        &pending,
        &mut in_flight,
        worker_count,
    )?;
    while next_index < total_blobs {
        let prefetched = match result_rx.recv() {
            Ok(Ok(prefetched)) => prefetched,
            Ok(Err(error)) => {
                drop(job_tx);
                drop(result_rx);
                join_azure_workers(handles);
                return Err(error);
            }
            Err(_) => {
                drop(job_tx);
                join_azure_workers(handles);
                return Err(ResearchError::Azure(
                    "Azure blob download workers stopped early".to_owned(),
                ));
            }
        };
        in_flight = in_flight.saturating_sub(1);
        pending.insert(prefetched.index, prefetched);
        while let Some(prefetched) = pending.remove(&next_index) {
            if let Err(error) = handle_azure_blob(prefetched) {
                drop(job_tx);
                drop(result_rx);
                join_azure_workers(handles);
                return Err(error);
            }
            next_index += 1;
        }
        fill_azure_prefetch_window(
            &job_tx,
            &mut blob_iter,
            &pending,
            &mut in_flight,
            worker_count,
        )?;
    }
    drop(job_tx);
    for handle in handles {
        handle
            .join()
            .map_err(|_| ResearchError::Azure("Azure blob download worker panicked".to_owned()))?;
    }
    if !pending.is_empty() {
        return Err(ResearchError::Azure(
            "Azure blob prefetch completed with unreplayed out-of-order blobs".to_owned(),
        ));
    }
    Ok(())
}

fn join_azure_workers(handles: Vec<thread::JoinHandle<()>>) {
    for handle in handles {
        let _ = handle.join();
    }
}

fn fill_azure_prefetch_window<I>(
    job_tx: &mpsc::Sender<(usize, AzureBlobItem)>,
    blob_iter: &mut I,
    pending: &BTreeMap<usize, PrefetchedAzureBlob>,
    in_flight: &mut usize,
    worker_count: usize,
) -> Result<(), ResearchError>
where
    I: Iterator<Item = (usize, AzureBlobItem)>,
{
    while *in_flight + pending.len() < worker_count {
        let Some((index, blob)) = blob_iter.next() else {
            break;
        };
        job_tx.send((index, blob)).map_err(|_| {
            ResearchError::Azure("queueing Azure blob download job failed".to_owned())
        })?;
        *in_flight += 1;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
struct AzureEventSource {
    account: String,
    container: String,
    prefix: String,
    sas_env: String,
    max_blobs: Option<usize>,
    max_bytes: Option<u64>,
    prefetch_blobs: usize,
}

impl AzureEventSource {
    fn parse(input: &str) -> Result<Option<Self>, ResearchError> {
        let Some(rest) = input.strip_prefix("azure://") else {
            return Ok(None);
        };
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        let mut parts = path.splitn(3, '/');
        let account = parts.next().unwrap_or_default();
        let container = parts.next().unwrap_or_default();
        let prefix = parts.next().unwrap_or_default();
        if account.is_empty() || container.is_empty() || prefix.is_empty() {
            return Err(ResearchError::InvalidInput(
                "azure input must be azure://<account>/<container>/<prefix>".to_owned(),
            ));
        }
        let mut source = Self {
            account: account.to_owned(),
            container: container.to_owned(),
            prefix: prefix.to_owned(),
            sas_env: "AZURE_STORAGE_SAS".to_owned(),
            max_blobs: None,
            max_bytes: None,
            prefetch_blobs: DEFAULT_AZURE_PREFETCH_BLOBS,
        };
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            match key {
                "sas_env" if !value.is_empty() => source.sas_env = value.to_owned(),
                "max_blobs" if !value.is_empty() => {
                    source.max_blobs = Some(value.parse::<usize>().map_err(|_| {
                        ResearchError::InvalidInput(format!("invalid max_blobs in {input}"))
                    })?);
                }
                "max_bytes" if !value.is_empty() => {
                    source.max_bytes = Some(value.parse::<u64>().map_err(|_| {
                        ResearchError::InvalidInput(format!("invalid max_bytes in {input}"))
                    })?);
                }
                "prefetch_blobs" if !value.is_empty() => {
                    let prefetch_blobs = value.parse::<usize>().map_err(|_| {
                        ResearchError::InvalidInput(format!("invalid prefetch_blobs in {input}"))
                    })?;
                    source.prefetch_blobs = prefetch_blobs.clamp(1, MAX_AZURE_PREFETCH_BLOBS);
                }
                _ => {}
            }
        }
        Ok(Some(source))
    }

    fn worker_count(&self, blob_count: usize) -> usize {
        self.prefetch_blobs.max(1).min(blob_count.max(1))
    }
}

fn collect_jsonl_path_set(
    input: &Path,
    mode: EventPathMode,
) -> Result<EventPathSet, ResearchError> {
    if input.is_file() {
        return Ok(EventPathSet {
            paths: vec![input.to_path_buf()],
            merge_by_event_time: false,
        });
    }
    if !input.exists() {
        return Err(ResearchError::InvalidInput(format!(
            "{} does not exist",
            input.display()
        )));
    }
    let preferred = input.join("events.jsonl");
    if matches!(mode, EventPathMode::PreferEventsJsonl) && preferred.is_file() {
        return Ok(EventPathSet {
            paths: vec![preferred],
            merge_by_event_time: false,
        });
    }
    let preferred_gzip = input.join("events.jsonl.gz");
    if matches!(mode, EventPathMode::PreferEventsJsonl) && preferred_gzip.is_file() {
        return Ok(EventPathSet {
            paths: vec![preferred_gzip],
            merge_by_event_time: false,
        });
    }
    let mut paths = Vec::new();
    collect_jsonl_recursive(input, &mut paths)?;
    paths.sort();
    if input.join("events_manifest.json").is_file() {
        let filtered = filtered_normalized_event_paths(&paths, mode);
        if !filtered.is_empty() {
            paths = filtered;
        }
    }
    Ok(EventPathSet {
        paths,
        merge_by_event_time: input.join("events_manifest.json").is_file(),
    })
}

fn filtered_normalized_event_paths(paths: &[PathBuf], mode: EventPathMode) -> Vec<PathBuf> {
    let allowed = match mode {
        EventPathMode::PreferEventsJsonl | EventPathMode::AllJsonl => return paths.to_vec(),
        EventPathMode::MarketTruth => &[
            "markets.jsonl",
            "references.jsonl",
            "fair_values.jsonl",
            "decisions.jsonl",
            "execution_reports.jsonl",
            "feed_errors.jsonl",
            "other.jsonl",
        ][..],
        EventPathMode::Calibration => &[
            "markets.jsonl",
            "references.jsonl",
            "fair_values.jsonl",
            "other.jsonl",
        ][..],
    };
    paths
        .iter()
        .filter(|path| normalized_path_matches(path, allowed))
        .cloned()
        .collect()
}

fn normalized_path_matches(path: &Path, allowed_bases: &[&str]) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    allowed_bases
        .iter()
        .any(|base| name == *base || name == format!("{base}.gz"))
}

fn collect_jsonl_recursive(dir: &Path, paths: &mut Vec<PathBuf>) -> Result<(), ResearchError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        if path.is_dir() {
            if matches!(name, ".git" | "target" | "node_modules" | ".next") {
                continue;
            }
            collect_jsonl_recursive(&path, paths)?;
        } else if is_jsonl_path(&path) {
            paths.push(path);
        }
    }
    Ok(())
}

fn is_jsonl_path(path: &Path) -> bool {
    path.extension().and_then(|value| value.to_str()) == Some("jsonl") || is_gzip_jsonl_path(path)
}

fn is_gzip_jsonl_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| name.ends_with(".jsonl.gz"))
}

#[derive(Default)]
struct AuditAccumulator {
    total_events: usize,
    event_count_by_type: BTreeMap<String, usize>,
    event_count_by_day: BTreeMap<String, usize>,
    event_count_by_hour: BTreeMap<String, usize>,
    first_ts: Option<DateTime<Utc>>,
    last_ts: Option<DateTime<Utc>>,
    markets: BTreeMap<String, MarketTruth>,
    token_to_market: BTreeMap<String, String>,
    decisions: usize,
    execution_reports: usize,
    paper_resting: usize,
    paper_cancelled: usize,
    paper_filled: usize,
    paper_filled_maker: usize,
    cancel_decisions: usize,
    paper_settlements: usize,
    feed_errors: usize,
    stale_reference_count: usize,
    stale_book_count: usize,
    malformed_lines: usize,
    missing_payloads: usize,
    missing_market_ids: usize,
    out_of_order_timestamps: usize,
    duplicate_estimate: usize,
    previous_ts: Option<DateTime<Utc>>,
    largest_gaps: Vec<(i64, DateTime<Utc>, DateTime<Utc>)>,
}

impl AuditAccumulator {
    fn observe(&mut self, event: &EventLine) {
        self.total_events += 1;
        *self
            .event_count_by_type
            .entry(event.event_type.clone())
            .or_insert(0) += 1;
        *self
            .event_count_by_day
            .entry(day_key(event.recorded_ts))
            .or_insert(0) += 1;
        *self
            .event_count_by_hour
            .entry(hour_key(event.recorded_ts))
            .or_insert(0) += 1;
        self.first_ts = min_ts(self.first_ts, Some(event.recorded_ts));
        self.last_ts = max_ts(self.last_ts, Some(event.recorded_ts));
        if self
            .previous_ts
            .is_some_and(|previous| event.recorded_ts < previous)
        {
            self.out_of_order_timestamps += 1;
        }
        if let Some(previous) = self.previous_ts {
            let gap = event
                .recorded_ts
                .signed_duration_since(previous)
                .num_milliseconds();
            if gap > 0 {
                self.largest_gaps.push((gap, previous, event.recorded_ts));
                self.largest_gaps
                    .sort_by_key(|entry| std::cmp::Reverse(entry.0));
                self.largest_gaps.truncate(10);
            }
        }
        self.previous_ts = Some(event.recorded_ts);
        if event.payload.is_null() {
            self.missing_payloads += 1;
        }
        match event.event_type.as_str() {
            "market" => self.observe_market(&event.payload),
            "market_start_price" => self.observe_market_start(&event.payload),
            "reference" => self.observe_reference(&event.payload),
            "book" => self.observe_book(&event.payload),
            "decision" => self.observe_decision(&event.payload),
            "execution_report" => self.observe_execution_report(&event.payload),
            "paper_settlement" => self.paper_settlements += 1,
            "feed_error" => self.feed_errors += 1,
            "fair_value" => self.observe_market_count(&event.payload, |market| {
                market.fair_value_count += 1;
            }),
            _ => {}
        }
    }

    fn observe_market(&mut self, payload: &Value) {
        let market = market_from_payload(payload);
        if market.market_id.is_empty() {
            self.missing_market_ids += 1;
            return;
        }
        if !market.up_token_id.is_empty() {
            self.token_to_market
                .insert(market.up_token_id.clone(), market.market_id.clone());
        }
        if !market.down_token_id.is_empty() {
            self.token_to_market
                .insert(market.down_token_id.clone(), market.market_id.clone());
        }
        self.markets
            .entry(market.market_id.clone())
            .and_modify(|existing| existing.merge(market.clone()))
            .or_insert(market);
    }

    fn observe_market_start(&mut self, payload: &Value) {
        let market_id = text(payload, "market_id");
        if market_id.is_empty() {
            self.missing_market_ids += 1;
            return;
        }
        let market = self
            .markets
            .entry(market_id.clone())
            .or_insert_with(|| MarketTruth {
                market_id: market_id.clone(),
                ..MarketTruth::default()
            });
        market.start_price = decimal(payload.get("start_price")).or(market.start_price);
        market.start_source = Some("market_start_price".to_owned());
    }

    fn observe_reference(&mut self, payload: &Value) {
        if bool_value(payload, "stale") {
            self.stale_reference_count += 1;
        }
        let Some(price) = decimal(payload.get("price")) else {
            return;
        };
        let Some(source_ts) = parse_datetime(payload.get("source_ts")) else {
            return;
        };
        for market in self.markets.values_mut() {
            market.reference_tick_count += 1;
            market.observe_settlement_reference(source_ts, price);
        }
    }

    fn observe_book(&mut self, payload: &Value) {
        let token_id = text(payload, "token_id");
        let Some(market_id) = self.token_to_market.get(&token_id).cloned() else {
            self.missing_market_ids += 1;
            return;
        };
        if bool_value(payload, "stale") {
            self.stale_book_count += 1;
        }
        if let Some(market) = self.markets.get_mut(&market_id) {
            market
                .book_update_counts
                .entry(token_id)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }

    fn observe_decision(&mut self, payload: &Value) {
        self.decisions += 1;
        let action = text(payload, "action");
        if action == "cancel_all" {
            self.cancel_decisions += 1;
        }
        self.observe_market_count(payload, |market| {
            market.decisions += 1;
            if action == "cancel_all" {
                market.cancels += 1;
            }
        });
    }

    fn observe_execution_report(&mut self, payload: &Value) {
        self.execution_reports += 1;
        let status = text(payload, "status");
        match status.as_str() {
            "paper_resting" => self.paper_resting += 1,
            "paper_cancelled" => self.paper_cancelled += 1,
            "paper_filled" => self.paper_filled += 1,
            "paper_filled_maker" => {
                self.paper_filled += 1;
                self.paper_filled_maker += 1;
            }
            _ => {}
        }
        self.observe_market_count(payload, |market| {
            market.reports += 1;
            if decimal(payload.get("filled_size")).unwrap_or(Decimal::ZERO) > Decimal::ZERO {
                market.fills += 1;
            }
        });
    }

    fn observe_market_count<F>(&mut self, payload: &Value, mut update: F)
    where
        F: FnMut(&mut MarketTruth),
    {
        let market_id = text(payload, "market_id");
        if market_id.is_empty() {
            self.missing_market_ids += 1;
            return;
        }
        let market = self
            .markets
            .entry(market_id.clone())
            .or_insert_with(|| MarketTruth {
                market_id,
                ..MarketTruth::default()
            });
        update(market);
    }

    fn finish(mut self) -> Value {
        let markets_with_start = self
            .markets
            .values()
            .filter(|market| market.start_price.is_some())
            .count();
        for market in self.markets.values_mut() {
            market.finalize_flags();
        }
        let markets_settled = self
            .markets
            .values()
            .filter(|market| market.final_price.is_some())
            .count();
        let mut warnings = Vec::new();
        if self.malformed_lines > 0 {
            warnings.push(json!(format!(
                "{} malformed JSONL lines",
                self.malformed_lines
            )));
        }
        if self.missing_payloads > 0 {
            warnings.push(json!(format!(
                "{} events missing payload",
                self.missing_payloads
            )));
        }
        if self.out_of_order_timestamps > 0 {
            warnings.push(json!(format!(
                "{} out-of-order timestamps",
                self.out_of_order_timestamps
            )));
        }
        if markets_settled == 0 {
            warnings.push(json!(
                "no settled markets found; profitability simulation will be incomplete"
            ));
        }
        json!({
            "total_events": self.total_events,
            "event_count_by_type": self.event_count_by_type,
            "event_count_by_day": self.event_count_by_day,
            "event_count_by_hour": self.event_count_by_hour,
            "first_event_timestamp": self.first_ts.map(ts),
            "last_event_timestamp": self.last_ts.map(ts),
            "markets_seen": self.markets.len(),
            "markets_with_start_price": markets_with_start,
            "markets_settled": markets_settled,
            "start_price_capture_rate": ratio_usize(markets_with_start, self.markets.len()),
            "settlement_rate": ratio_usize(markets_settled, self.markets.len()),
            "missing_start_markets": self.markets.values().filter(|market| market.start_price.is_none()).map(|market| market.market_id.clone()).collect::<Vec<_>>(),
            "missing_final_markets": self.markets.values().filter(|market| market.final_price.is_none()).map(|market| market.market_id.clone()).collect::<Vec<_>>(),
            "decision_count": self.decisions,
            "execution_report_count": self.execution_reports,
            "paper_resting": self.paper_resting,
            "paper_cancelled": self.paper_cancelled,
            "paper_filled": self.paper_filled,
            "paper_filled_maker": self.paper_filled_maker,
            "cancel_decisions": self.cancel_decisions,
            "paper_settlements": self.paper_settlements,
            "feed_errors": self.feed_errors,
            "stale_reference_count": self.stale_reference_count,
            "stale_book_count": self.stale_book_count,
            "malformed_lines": self.malformed_lines,
            "missing_payloads": self.missing_payloads,
            "missing_market_ids": self.missing_market_ids,
            "out_of_order_timestamps": self.out_of_order_timestamps,
            "duplicate_estimate": self.duplicate_estimate,
            "largest_time_gaps": self.largest_gaps.iter().map(|(gap, from, to)| json!({
                "gap_ms": gap,
                "from": ts(*from),
                "to": ts(*to)
            })).collect::<Vec<_>>(),
            "warnings": warnings,
            "fatal_data_quality_issues": if self.total_events == 0 { vec!["no events found"] } else { Vec::<&str>::new() }
        })
    }
}

struct NormalizedWriters {
    root: PathBuf,
    format: NormalizedFileFormat,
    events: Option<JsonlLineWriter>,
    by_type: BTreeMap<String, JsonlLineWriter>,
    counts: BTreeMap<String, usize>,
    sequence: u64,
}

impl NormalizedWriters {
    fn new(root: &Path, format: NormalizedFileFormat) -> Result<Self, ResearchError> {
        fs::create_dir_all(root)?;
        let events = format
            .event_file_name()
            .map(|file_name| JsonlLineWriter::new(&root.join(file_name), format))
            .transpose()?;
        let mut by_type = BTreeMap::new();
        for (event_type, file_name) in normalized_files(format) {
            by_type.insert(
                event_type.to_owned(),
                JsonlLineWriter::new(&root.join(file_name), format)?,
            );
        }
        Ok(Self {
            root: root.to_path_buf(),
            format,
            events,
            by_type,
            counts: BTreeMap::new(),
            sequence: 0,
        })
    }

    fn write(&mut self, event: &EventLine) -> Result<(), ResearchError> {
        let row = normalized_row(event, self.sequence);
        self.sequence += 1;
        if let Some(writer) = &mut self.events {
            writer.write_row(&row)?;
        }
        let target = match event.event_type.as_str() {
            "market" => "market",
            "reference" => "reference",
            "book" => "book",
            "fair_value" => "fair_value",
            "decision" => "decision",
            "execution_report" => "execution_report",
            "paper_settlement" => "paper_settlement",
            "feed_error" => "feed_error",
            _ => "other",
        };
        if let Some(writer) = self.by_type.get_mut(target) {
            writer.write_row(&row)?;
            *self.counts.entry(target.to_owned()).or_insert(0) += 1;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), ResearchError> {
        if let Some(writer) = &mut self.events {
            writer.flush()?;
        }
        for writer in self.by_type.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    fn manifest(&self) -> Value {
        let mut files = Map::new();
        if let Some(file_name) = self.format.event_file_name() {
            files.insert(
                "events".to_owned(),
                json!(self.root.join(file_name).to_string_lossy()),
            );
        } else {
            files.insert("events".to_owned(), Value::Null);
        }
        for (event_type, file_name) in normalized_files(self.format) {
            files.insert(
                event_type.to_owned(),
                json!({
                    "path": self.root.join(file_name).to_string_lossy(),
                    "rows": self.counts.get(event_type).copied().unwrap_or(0)
                }),
            );
        }
        Value::Object(files)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NormalizedFileFormat {
    Indexed,
    Gzip,
    GzipSharded,
}

impl NormalizedFileFormat {
    fn parse(value: &str) -> Result<Self, ResearchError> {
        match value {
            "jsonl-indexed" => Ok(Self::Indexed),
            "jsonl-indexed-gzip" | "jsonl-indexed-gz" => Ok(Self::Gzip),
            "jsonl-indexed-gzip-sharded" | "jsonl-indexed-gz-sharded" => Ok(Self::GzipSharded),
            _ => Err(ResearchError::InvalidInput(format!(
                "unsupported normalize format {value}; expected jsonl-indexed, jsonl-indexed-gzip, or jsonl-indexed-gzip-sharded"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Indexed => "jsonl-indexed",
            Self::Gzip => "jsonl-indexed-gzip",
            Self::GzipSharded => "jsonl-indexed-gzip-sharded",
        }
    }

    fn compression(self) -> &'static str {
        match self {
            Self::Indexed => "none",
            Self::Gzip | Self::GzipSharded => "gzip",
        }
    }

    fn event_file_name(self) -> Option<&'static str> {
        match self {
            Self::Indexed => Some("events.jsonl"),
            Self::Gzip => Some("events.jsonl.gz"),
            Self::GzipSharded => None,
        }
    }

    fn file_name(self, base: &str) -> String {
        match self {
            Self::Indexed => base.to_owned(),
            Self::Gzip | Self::GzipSharded => format!("{base}.gz"),
        }
    }

    fn writes_event_log(self) -> bool {
        self.event_file_name().is_some()
    }
}

enum JsonlLineWriter {
    Plain(BufWriter<File>),
    Gzip(GzEncoder<BufWriter<File>>),
}

impl JsonlLineWriter {
    fn new(path: &Path, format: NormalizedFileFormat) -> Result<Self, ResearchError> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        match format {
            NormalizedFileFormat::Indexed => Ok(Self::Plain(writer)),
            NormalizedFileFormat::Gzip | NormalizedFileFormat::GzipSharded => {
                Ok(Self::Gzip(GzEncoder::new(writer, Compression::default())))
            }
        }
    }

    fn write_row(&mut self, row: &Value) -> Result<(), ResearchError> {
        match self {
            Self::Plain(writer) => {
                serde_json::to_writer(&mut *writer, row)?;
                writer.write_all(b"\n")?;
            }
            Self::Gzip(writer) => {
                serde_json::to_writer(&mut *writer, row)?;
                writer.write_all(b"\n")?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), ResearchError> {
        match self {
            Self::Plain(writer) => writer.flush()?,
            Self::Gzip(writer) => writer.try_finish()?,
        }
        Ok(())
    }
}

fn normalized_files(format: NormalizedFileFormat) -> Vec<(&'static str, String)> {
    vec![
        ("market", format.file_name("markets.jsonl")),
        ("reference", format.file_name("references.jsonl")),
        ("book", format.file_name("books.jsonl")),
        ("fair_value", format.file_name("fair_values.jsonl")),
        ("decision", format.file_name("decisions.jsonl")),
        (
            "execution_report",
            format.file_name("execution_reports.jsonl"),
        ),
        (
            "paper_settlement",
            format.file_name("paper_settlements.jsonl"),
        ),
        ("feed_error", format.file_name("feed_errors.jsonl")),
        ("other", format.file_name("other.jsonl")),
    ]
}

fn normalize_progress(
    status: &str,
    format: NormalizedFileFormat,
    events: usize,
    counts: &BTreeMap<String, usize>,
    first_ts: Option<DateTime<Utc>>,
    last_ts: Option<DateTime<Utc>>,
) -> Value {
    json!({
        "status": status,
        "format": format.as_str(),
        "compression": format.compression(),
        "event_log_written": format.writes_event_log(),
        "events": events,
        "event_counts": counts,
        "first_recorded_ts": first_ts.map(ts),
        "last_recorded_ts": last_ts.map(ts),
        "updated_at": now_ts()
    })
}

fn normalized_row(event: &EventLine, sequence: u64) -> Value {
    let payload = &event.payload;
    json!({
        "sequence": sequence,
        "event_type": event.event_type,
        "recorded_ts": ts(event.recorded_ts),
        "source_ts": parse_datetime(payload.get("source_ts")).map(ts)
            .or_else(|| parse_datetime(payload.get("exchange_ts")).map(ts))
            .or_else(|| parse_datetime(payload.get("local_ts")).map(ts)),
        "market_id": text(payload, "market_id"),
        "token_id": text(payload, "token_id"),
        "condition_id": text(payload, "condition_id"),
        "outcome": text(payload, "outcome"),
        "action": text(payload, "action"),
        "status": text(payload, "status"),
        "price": decimal(payload.get("price")).map(|value| value.to_string())
            .or_else(|| decimal(payload.get("start_price")).map(|value| value.to_string())),
        "size": decimal(payload.get("size")).map(|value| value.to_string())
            .or_else(|| decimal(payload.get("filled_size")).map(|value| value.to_string())),
        "raw_payload": redact_json(payload),
        "raw_event": redact_json(&event.raw)
    })
}

#[derive(Clone, Debug, Default)]
struct MarketTruth {
    market_id: String,
    condition_id: Option<String>,
    slug: Option<String>,
    question: Option<String>,
    asset: Option<String>,
    horizon: Option<String>,
    up_token_id: String,
    down_token_id: String,
    start_ts: Option<DateTime<Utc>>,
    end_ts: Option<DateTime<Utc>>,
    start_price: Option<Decimal>,
    final_price: Option<Decimal>,
    final_distance_ms: Option<i64>,
    winning_outcome: Option<String>,
    start_source: Option<String>,
    final_source: Option<String>,
    reference_tick_count: usize,
    book_update_counts: BTreeMap<String, usize>,
    fair_value_count: usize,
    decisions: usize,
    reports: usize,
    fills: usize,
    cancels: usize,
    feed_errors: usize,
    flags: Vec<String>,
}

impl MarketTruth {
    fn merge(&mut self, other: Self) {
        self.condition_id = self.condition_id.clone().or(other.condition_id);
        self.slug = self.slug.clone().or(other.slug);
        self.question = self.question.clone().or(other.question);
        self.asset = self.asset.clone().or(other.asset);
        self.horizon = self.horizon.clone().or(other.horizon);
        if self.up_token_id.is_empty() {
            self.up_token_id = other.up_token_id;
        }
        if self.down_token_id.is_empty() {
            self.down_token_id = other.down_token_id;
        }
        self.start_ts = self.start_ts.or(other.start_ts);
        self.end_ts = self.end_ts.or(other.end_ts);
        self.start_price = self.start_price.or(other.start_price);
        self.start_source = self.start_source.clone().or(other.start_source);
    }

    fn observe_settlement_reference(&mut self, source_ts: DateTime<Utc>, price: Decimal) {
        let Some(end_ts) = self.end_ts else {
            return;
        };
        let distance_ms = source_ts
            .signed_duration_since(end_ts)
            .num_milliseconds()
            .abs();
        if distance_ms > SETTLEMENT_WINDOW_SECONDS * 1000 {
            return;
        }
        if self
            .final_distance_ms
            .is_none_or(|existing| distance_ms < existing)
        {
            self.final_distance_ms = Some(distance_ms);
            self.final_price = Some(price);
            self.final_source = Some("chainlink_reference_settlement_window".to_owned());
        }
    }

    fn finalize_flags(&mut self) {
        self.flags.clear();
        if self.start_price.is_none() {
            self.flags.push("missing_start_price".to_owned());
        }
        if self.final_price.is_none() {
            self.flags.push("missing_final_price".to_owned());
        }
        if self.up_token_id.is_empty() || self.down_token_id.is_empty() {
            self.flags.push("missing_token_ids".to_owned());
        }
        self.winning_outcome = match (self.start_price, self.final_price) {
            (Some(start), Some(final_price)) if final_price >= start => Some("up".to_owned()),
            (Some(_), Some(_)) => Some("down".to_owned()),
            _ => None,
        };
    }

    fn complete_for_simulation(&self) -> bool {
        self.start_price.is_some()
            && self.final_price.is_some()
            && !self.up_token_id.is_empty()
            && !self.down_token_id.is_empty()
    }

    fn as_json(&self) -> Value {
        let mut row = self.clone();
        row.finalize_flags();
        json!({
            "market_id": row.market_id,
            "condition_id": row.condition_id,
            "slug": row.slug,
            "question": row.question,
            "asset": row.asset,
            "horizon": row.horizon,
            "up_token_id": row.up_token_id,
            "down_token_id": row.down_token_id,
            "start_ts": row.start_ts.map(ts),
            "end_ts": row.end_ts.map(ts),
            "start_price": row.start_price.map(|value| value.to_string()),
            "final_price": row.final_price.map(|value| value.to_string()),
            "winning_outcome": row.winning_outcome,
            "complete_for_simulation": row.complete_for_simulation(),
            "start_source": row.start_source,
            "final_source": row.final_source,
            "reference_tick_count": row.reference_tick_count,
            "book_update_counts": row.book_update_counts,
            "fair_value_count": row.fair_value_count,
            "decisions": row.decisions,
            "reports": row.reports,
            "fills": row.fills,
            "cancels": row.cancels,
            "feed_errors": row.feed_errors,
            "data_quality_flags": row.flags
        })
    }
}

struct MarketRowsResult {
    rows: Vec<MarketTruth>,
    stream: StreamStats,
}

fn build_market_rows(
    input: &Path,
    exclude_windows: &[ExcludedTimeWindow],
) -> Result<MarketRowsResult, ResearchError> {
    let mut audit = AuditAccumulator::default();
    let stream = stream_events(
        input,
        EventPathMode::MarketTruth,
        exclude_windows,
        |event| {
            audit.observe(event);
        },
    )?;
    audit.malformed_lines = stream.malformed_lines;
    let mut rows = audit.markets.into_values().collect::<Vec<_>>();
    for row in &mut rows {
        row.finalize_flags();
    }
    rows.sort_by(|left, right| {
        left.start_ts
            .cmp(&right.start_ts)
            .then(left.market_id.cmp(&right.market_id))
    });
    Ok(MarketRowsResult { rows, stream })
}

fn market_summary(rows: &[MarketTruth]) -> Value {
    let complete = rows
        .iter()
        .filter(|row| row.complete_for_simulation())
        .count();
    let warnings = if complete == 0 {
        vec![json!("no markets complete for profitability simulation")]
    } else {
        Vec::new()
    };
    json!({
        "markets": rows.len(),
        "complete_for_simulation": complete,
        "missing_start_price": rows.iter().filter(|row| row.start_price.is_none()).count(),
        "missing_final_price": rows.iter().filter(|row| row.final_price.is_none()).count(),
        "total_decisions": rows.iter().map(|row| row.decisions).sum::<usize>(),
        "total_fills": rows.iter().map(|row| row.fills).sum::<usize>(),
        "warnings": warnings
    })
}

fn market_from_payload(payload: &Value) -> MarketTruth {
    let market_id = text(payload, "market_id");
    MarketTruth {
        market_id,
        condition_id: optional_text(payload, "condition_id"),
        slug: optional_text(payload, "market_slug").or_else(|| optional_text(payload, "slug")),
        question: optional_text(payload, "question"),
        asset: optional_text(payload, "asset"),
        horizon: optional_text(payload, "horizon"),
        up_token_id: text(payload, "up_token_id"),
        down_token_id: text(payload, "down_token_id"),
        start_ts: parse_datetime(payload.get("start_ts")),
        end_ts: parse_datetime(payload.get("end_ts")),
        start_price: decimal(payload.get("start_price")),
        final_price: None,
        final_distance_ms: None,
        winning_outcome: None,
        start_source: if payload.get("start_price").is_some() {
            Some("market_payload".to_owned())
        } else {
            None
        },
        final_source: None,
        reference_tick_count: 0,
        book_update_counts: BTreeMap::new(),
        fair_value_count: 0,
        decisions: 0,
        reports: 0,
        fills: 0,
        cancels: 0,
        feed_errors: 0,
        flags: Vec::new(),
    }
}

fn load_market_truth(path: Option<&Path>) -> Result<Vec<MarketTruth>, ResearchError> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let value = read_json_file(path)?;
    let markets = value
        .pointer("/result/markets")
        .or_else(|| value.get("markets"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(markets.iter().map(market_from_json).collect())
}

fn market_from_json(value: &Value) -> MarketTruth {
    let mut row = MarketTruth {
        market_id: text(value, "market_id"),
        condition_id: optional_text(value, "condition_id"),
        slug: optional_text(value, "slug").or_else(|| optional_text(value, "market_slug")),
        question: optional_text(value, "question"),
        asset: optional_text(value, "asset"),
        horizon: optional_text(value, "horizon"),
        up_token_id: text(value, "up_token_id"),
        down_token_id: text(value, "down_token_id"),
        start_ts: parse_datetime(value.get("start_ts")),
        end_ts: parse_datetime(value.get("end_ts")),
        start_price: decimal(value.get("start_price")),
        final_price: decimal(value.get("final_price")),
        final_distance_ms: None,
        winning_outcome: optional_text(value, "winning_outcome"),
        start_source: optional_text(value, "start_source"),
        final_source: optional_text(value, "final_source"),
        reference_tick_count: value
            .get("reference_tick_count")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize,
        book_update_counts: BTreeMap::new(),
        fair_value_count: value
            .get("fair_value_count")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize,
        decisions: value.get("decisions").and_then(Value::as_u64).unwrap_or(0) as usize,
        reports: value.get("reports").and_then(Value::as_u64).unwrap_or(0) as usize,
        fills: value.get("fills").and_then(Value::as_u64).unwrap_or(0) as usize,
        cancels: value.get("cancels").and_then(Value::as_u64).unwrap_or(0) as usize,
        feed_errors: value
            .get("feed_errors")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize,
        flags: Vec::new(),
    };
    row.finalize_flags();
    row
}

#[derive(Clone, Debug)]
struct ReplayRequest {
    name: String,
    fill_model: FillModel,
    mode: StrategyProfileMode,
    settings: RuntimeSettings,
}

#[derive(Clone, Debug)]
enum StrategyProfileMode {
    Static,
    DynamicSafetyOnly,
    DynamicQuoteStyle,
    FullDeterministic,
    StaticSweep(SweepCandidate),
}

impl StrategyProfileMode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Static => "static",
            Self::DynamicSafetyOnly => "dynamic_safety_only",
            Self::DynamicQuoteStyle => "dynamic_quote_style",
            Self::FullDeterministic => "full_deterministic_profile",
            Self::StaticSweep(_) => "static_sweep",
        }
    }
}

#[derive(Clone, Debug)]
struct SweepCandidate {
    name: String,
    maker_min_edge: Decimal,
    ttl_seconds: i64,
    final_no_trade_seconds: i64,
    quote_style: QuoteStyle,
}

#[derive(Clone, Debug)]
struct ReferencePoint {
    ts: DateTime<Utc>,
    price: Decimal,
    stale: bool,
}

#[derive(Clone, Debug, Default)]
struct OrderBookState {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    local_ts: Option<DateTime<Utc>>,
    updates: usize,
}

impl OrderBookState {
    fn apply(&mut self, payload: &Value, recorded_ts: DateTime<Utc>) {
        apply_levels(&mut self.bids, payload.get("bids"));
        apply_levels(&mut self.asks, payload.get("asks"));
        self.local_ts = parse_datetime(payload.get("local_ts")).or(Some(recorded_ts));
        self.updates += 1;
    }

    fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, size)| (*price, *size))
    }

    fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(price, size)| (*price, *size))
    }

    fn spread_ticks(&self, tick_size: Decimal) -> Option<f64> {
        let (bid, _) = self.best_bid()?;
        let (ask, _) = self.best_ask()?;
        if tick_size <= Decimal::ZERO {
            return None;
        }
        ((ask - bid) / tick_size).to_f64()
    }

    fn top_size(&self) -> Option<f64> {
        let bid = self
            .best_bid()
            .map(|(_, size)| size)
            .unwrap_or(Decimal::ZERO);
        let ask = self
            .best_ask()
            .map(|(_, size)| size)
            .unwrap_or(Decimal::ZERO);
        bid.min(ask).to_f64()
    }

    fn age_ms(&self, now: DateTime<Utc>) -> Option<f64> {
        self.local_ts.map(|local_ts| {
            now.signed_duration_since(local_ts)
                .num_microseconds()
                .map_or(0.0, |micros| (micros.max(0) as f64) / 1000.0)
        })
    }
}

fn apply_levels(book: &mut BTreeMap<Decimal, Decimal>, levels: Option<&Value>) {
    let Some(levels) = levels.and_then(Value::as_array) else {
        return;
    };
    for level in levels {
        let Some(price) = decimal(level.get("price")) else {
            continue;
        };
        let size = decimal(level.get("size")).unwrap_or(Decimal::ZERO);
        if size <= Decimal::ZERO {
            book.remove(&price);
        } else {
            book.insert(price, size);
        }
    }
}

#[derive(Clone, Debug)]
struct ReplayOrder {
    market_id: String,
    token_id: String,
    outcome: String,
    side: String,
    price: Decimal,
    size: Decimal,
    order_kind: String,
    decision_ts: DateTime<Utc>,
    ttl_ms: Option<i64>,
    tick_size: Decimal,
    q_at_decision: Option<Decimal>,
    filled_size: Decimal,
    avg_price: Option<Decimal>,
    fee: Decimal,
    adverse_penalty: Decimal,
    fill_ts: Option<DateTime<Utc>>,
    fill_ref_price: Option<Decimal>,
    adverse_checked: bool,
    cancel_ts: Option<DateTime<Utc>>,
}

impl ReplayOrder {
    fn is_filled(&self) -> bool {
        self.filled_size > Decimal::ZERO
    }

    fn is_maker(&self) -> bool {
        self.order_kind.starts_with("post_only")
    }
}

struct ResearchReplayEngine {
    request: ReplayRequest,
    markets: BTreeMap<String, MarketTruth>,
    token_to_market: BTreeMap<String, (String, String)>,
    books: BTreeMap<String, OrderBookState>,
    fair_values: BTreeMap<String, Value>,
    reference_history: VecDeque<ReferencePoint>,
    last_reference: Option<ReferencePoint>,
    feed_error_times: VecDeque<DateTime<Utc>>,
    orders: Vec<ReplayOrder>,
    open_orders: BTreeSet<usize>,
    classifiers: BTreeMap<String, RegimeClassifier>,
    policy: RegimePolicy,
    event_count: usize,
    decisions_seen: usize,
    orders_seen: usize,
    fills: usize,
    maker_fills: usize,
    taker_fills: usize,
    cancels: usize,
    skipped_by_profile: usize,
    fills_after_cancel_prevented: usize,
    fills_prevented_not_live: usize,
    fills_prevented_final_window: usize,
    fills_prevented_market_inactive: usize,
    fills_prevented_expired: usize,
    fills_prevented_close: usize,
    queue_evidence_events: usize,
    trade_evidence_events: usize,
    depletion_evidence_events: usize,
    regime_frequency: BTreeMap<String, usize>,
    regime_time_share: BTreeMap<String, usize>,
    adaptive_logs: Vec<Value>,
    warnings: BTreeSet<String>,
    settlement_from_stream: bool,
}

impl ResearchReplayEngine {
    fn new(request: ReplayRequest, initial_markets: &[MarketTruth]) -> Self {
        let mut markets = BTreeMap::new();
        let mut token_to_market = BTreeMap::new();
        for market in initial_markets {
            markets.insert(market.market_id.clone(), market.clone());
            if !market.up_token_id.is_empty() {
                token_to_market.insert(
                    market.up_token_id.clone(),
                    (market.market_id.clone(), "up".to_owned()),
                );
            }
            if !market.down_token_id.is_empty() {
                token_to_market.insert(
                    market.down_token_id.clone(),
                    (market.market_id.clone(), "down".to_owned()),
                );
            }
        }
        let settlement_from_stream = initial_markets.is_empty();
        if request.fill_model == FillModel::QueueProxy {
            let mut warnings = BTreeSet::new();
            warnings.insert(
                "queue_proxy fill model skipped maker fills because queue depletion/trade evidence is not available in the normalized schema"
                    .to_owned(),
            );
            Self {
                policy: RegimePolicy::new(request.settings.strategy.clone()),
                request,
                markets,
                token_to_market,
                books: BTreeMap::new(),
                fair_values: BTreeMap::new(),
                reference_history: VecDeque::new(),
                last_reference: None,
                feed_error_times: VecDeque::new(),
                orders: Vec::new(),
                open_orders: BTreeSet::new(),
                classifiers: BTreeMap::new(),
                event_count: 0,
                decisions_seen: 0,
                orders_seen: 0,
                fills: 0,
                maker_fills: 0,
                taker_fills: 0,
                cancels: 0,
                skipped_by_profile: 0,
                fills_after_cancel_prevented: 0,
                fills_prevented_not_live: 0,
                fills_prevented_final_window: 0,
                fills_prevented_market_inactive: 0,
                fills_prevented_expired: 0,
                fills_prevented_close: 0,
                queue_evidence_events: 0,
                trade_evidence_events: 0,
                depletion_evidence_events: 0,
                regime_frequency: BTreeMap::new(),
                regime_time_share: BTreeMap::new(),
                adaptive_logs: Vec::new(),
                warnings,
                settlement_from_stream,
            }
        } else {
            Self {
                policy: RegimePolicy::new(request.settings.strategy.clone()),
                request,
                markets,
                token_to_market,
                books: BTreeMap::new(),
                fair_values: BTreeMap::new(),
                reference_history: VecDeque::new(),
                last_reference: None,
                feed_error_times: VecDeque::new(),
                orders: Vec::new(),
                open_orders: BTreeSet::new(),
                classifiers: BTreeMap::new(),
                event_count: 0,
                decisions_seen: 0,
                orders_seen: 0,
                fills: 0,
                maker_fills: 0,
                taker_fills: 0,
                cancels: 0,
                skipped_by_profile: 0,
                fills_after_cancel_prevented: 0,
                fills_prevented_not_live: 0,
                fills_prevented_final_window: 0,
                fills_prevented_market_inactive: 0,
                fills_prevented_expired: 0,
                fills_prevented_close: 0,
                queue_evidence_events: 0,
                trade_evidence_events: 0,
                depletion_evidence_events: 0,
                regime_frequency: BTreeMap::new(),
                regime_time_share: BTreeMap::new(),
                adaptive_logs: Vec::new(),
                warnings: BTreeSet::new(),
                settlement_from_stream,
            }
        }
    }

    fn observe(&mut self, event: &EventLine) {
        self.event_count += 1;
        if self.request.fill_model == FillModel::QueueProxy {
            self.observe_queue_proxy_evidence(event);
        }
        self.expire_reference_history(event.recorded_ts);
        match event.event_type.as_str() {
            "market" => self.handle_market(&event.payload),
            "market_start_price" => self.handle_market_start(&event.payload),
            "reference" => self.handle_reference(&event.payload, event.recorded_ts),
            "book" => self.handle_book(&event.payload, event.recorded_ts),
            "fair_value" => self.handle_fair_value(&event.payload),
            "decision" => self.handle_decision(&event.payload, event.recorded_ts),
            "execution_report" => self.handle_execution_report(&event.payload, event.recorded_ts),
            "feed_error" => self.feed_error_times.push_back(event.recorded_ts),
            _ => {}
        }
    }

    fn observe_queue_proxy_evidence(&mut self, event: &EventLine) {
        let event_type = event.event_type.to_ascii_lowercase();
        if event_type.contains("queue") || has_any_key(&event.payload, QUEUE_EVIDENCE_KEYS) {
            self.queue_evidence_events += 1;
        }
        if event_type.contains("trade") || has_any_key(&event.payload, TRADE_EVIDENCE_KEYS) {
            self.trade_evidence_events += 1;
        }
        if event_type.contains("deplet") || has_any_key(&event.payload, DEPLETION_EVIDENCE_KEYS) {
            self.depletion_evidence_events += 1;
        }
    }

    fn handle_market(&mut self, payload: &Value) {
        let market = market_from_payload(payload);
        if market.market_id.is_empty() {
            return;
        }
        if !market.up_token_id.is_empty() {
            self.token_to_market.insert(
                market.up_token_id.clone(),
                (market.market_id.clone(), "up".to_owned()),
            );
        }
        if !market.down_token_id.is_empty() {
            self.token_to_market.insert(
                market.down_token_id.clone(),
                (market.market_id.clone(), "down".to_owned()),
            );
        }
        self.markets
            .entry(market.market_id.clone())
            .and_modify(|existing| existing.merge(market.clone()))
            .or_insert(market);
    }

    fn handle_market_start(&mut self, payload: &Value) {
        let market_id = text(payload, "market_id");
        if market_id.is_empty() {
            return;
        }
        let market = self
            .markets
            .entry(market_id.clone())
            .or_insert_with(|| MarketTruth {
                market_id,
                ..MarketTruth::default()
            });
        market.start_price = decimal(payload.get("start_price")).or(market.start_price);
        market.start_source = Some("market_start_price".to_owned());
    }

    fn handle_reference(&mut self, payload: &Value, recorded_ts: DateTime<Utc>) {
        let Some(price) = decimal(payload.get("price")) else {
            return;
        };
        if !self.settlement_from_stream
            && matches!(self.request.mode, StrategyProfileMode::Static)
            && matches!(
                self.request.fill_model,
                FillModel::NoMakerFills | FillModel::QueueProxy
            )
        {
            return;
        }
        let point = ReferencePoint {
            ts: parse_datetime(payload.get("source_ts")).unwrap_or(recorded_ts),
            price,
            stale: bool_value(payload, "stale"),
        };
        if self.settlement_from_stream {
            for market in self.markets.values_mut() {
                market.reference_tick_count += 1;
                if !point.stale {
                    market.observe_settlement_reference(point.ts, point.price);
                }
            }
        }
        self.apply_adverse_penalties(&point);
        self.last_reference = Some(point.clone());
        self.reference_history.push_back(point);
    }

    fn handle_book(&mut self, payload: &Value, recorded_ts: DateTime<Utc>) {
        if matches!(
            self.request.fill_model,
            FillModel::NoMakerFills | FillModel::QueueProxy
        ) {
            return;
        }
        let token_id = text(payload, "token_id");
        if token_id.is_empty() {
            return;
        }
        let book = self.books.entry(token_id.clone()).or_default();
        book.apply(payload, recorded_ts);
        if let Some((market_id, _)) = self.token_to_market.get(&token_id).cloned() {
            if let Some(market) = self.markets.get_mut(&market_id) {
                market
                    .book_update_counts
                    .entry(token_id.clone())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }
        self.fill_open_orders(&token_id, recorded_ts);
    }

    fn handle_fair_value(&mut self, payload: &Value) {
        let market_id = text(payload, "market_id");
        if market_id.is_empty() {
            return;
        }
        self.fair_values.insert(market_id.clone(), payload.clone());
        if let Some(market) = self.markets.get_mut(&market_id) {
            market.fair_value_count += 1;
        }
    }

    fn handle_decision(&mut self, payload: &Value, recorded_ts: DateTime<Utc>) {
        self.decisions_seen += 1;
        let action = text(payload, "action");
        if action == "cancel_all" {
            self.cancel_market(&text(payload, "market_id"), recorded_ts);
            return;
        }
        if action != "place" {
            return;
        }
        let mut order = match self.order_from_decision(payload, recorded_ts) {
            Some(order) => order,
            None => return,
        };
        if !self.apply_strategy_mode(&mut order, payload, recorded_ts) {
            self.skipped_by_profile += 1;
            return;
        }
        if let StrategyProfileMode::StaticSweep(candidate) = &self.request.mode {
            if order.q_at_decision.is_some()
                && decimal(payload.get("expected_edge")).unwrap_or(Decimal::ZERO)
                    < candidate.maker_min_edge
            {
                self.skipped_by_profile += 1;
                return;
            }
            order.ttl_ms = Some(candidate.ttl_seconds * 1000);
            if self
                .market(&order.market_id)
                .and_then(|market| market.end_ts)
                .is_some_and(|end_ts| {
                    end_ts.signed_duration_since(recorded_ts).num_seconds()
                        <= candidate.final_no_trade_seconds
                })
            {
                self.skipped_by_profile += 1;
                return;
            }
            if candidate.quote_style == QuoteStyle::FairMinusMarginOnly {
                order.price = (order.price - order.tick_size).max(order.tick_size);
            }
        }
        self.orders.push(order);
        let index = self.orders.len() - 1;
        self.orders_seen += 1;
        if self.orders[index].is_maker() {
            self.open_orders.insert(index);
        } else if matches!(self.orders[index].order_kind.as_str(), "fak" | "fok") {
            if self.request.settings.strategy.enable_taker_orders {
                self.fill_order(index, self.orders[index].price, recorded_ts, false);
            } else {
                self.warnings.insert(
                    "taker decision observed but taker simulation is disabled by default"
                        .to_owned(),
                );
            }
        }
    }

    fn handle_execution_report(&mut self, payload: &Value, recorded_ts: DateTime<Utc>) {
        let status = text(payload, "status");
        if status == "paper_cancelled" || status == "live_cancel_all_submitted" {
            self.cancel_market(&text(payload, "market_id"), recorded_ts);
        }
    }

    fn order_from_decision(
        &self,
        payload: &Value,
        recorded_ts: DateTime<Utc>,
    ) -> Option<ReplayOrder> {
        let market_id = text(payload, "market_id");
        let token_id = text(payload, "token_id");
        let price = decimal(payload.get("price"))?;
        let size = decimal(payload.get("size"))?;
        if market_id.is_empty() || token_id.is_empty() || size <= Decimal::ZERO {
            return None;
        }
        let tick_size = self
            .markets
            .get(&market_id)
            .and_then(|_| decimal(payload.get("tick_size")))
            .unwrap_or_else(|| Decimal::new(1, 2));
        let q_at_decision = self.fair_values.get(&market_id).and_then(|fair| {
            match text(payload, "outcome").as_str() {
                "up" => decimal(fair.get("q_up")),
                "down" => decimal(fair.get("q_down")),
                _ => None,
            }
        });
        Some(ReplayOrder {
            market_id,
            token_id,
            outcome: text(payload, "outcome"),
            side: text(payload, "side"),
            price,
            size,
            order_kind: text(payload, "order_kind"),
            decision_ts: recorded_ts,
            ttl_ms: payload.get("ttl_ms").and_then(Value::as_i64),
            tick_size,
            q_at_decision,
            filled_size: Decimal::ZERO,
            avg_price: None,
            fee: Decimal::ZERO,
            adverse_penalty: Decimal::ZERO,
            fill_ts: None,
            fill_ref_price: self
                .latest_reference_at(recorded_ts)
                .map(|reference| reference.price),
            adverse_checked: false,
            cancel_ts: None,
        })
    }

    fn apply_strategy_mode(
        &mut self,
        order: &mut ReplayOrder,
        payload: &Value,
        recorded_ts: DateTime<Utc>,
    ) -> bool {
        if matches!(
            self.request.mode,
            StrategyProfileMode::Static | StrategyProfileMode::StaticSweep(_)
        ) {
            return true;
        }
        let features = self.features_for_order(order, recorded_ts);
        let classifier = self.classifiers.entry(order.market_id.clone()).or_default();
        let regime = classifier.classify(&features, recorded_ts);
        *self
            .regime_frequency
            .entry(regime.as_str().to_owned())
            .or_insert(0) += 1;
        let adaptive = self.policy.apply(regime, &features);
        if self.adaptive_logs.len() < ADAPTIVE_LOG_LIMIT {
            self.adaptive_logs.push(json!({
                "recorded_ts": ts(recorded_ts),
                "market_id": order.market_id,
                "regime": regime.as_str(),
                "profile": adaptive.profile.name,
                "features_summary": adaptive.features_summary,
                "original_params": adaptive.original_params,
                "effective_params": adaptive.effective_params,
                "reason": adaptive.reason
            }));
        }
        if adaptive.effective_params.cancel_existing {
            self.cancel_market(&order.market_id, recorded_ts);
        }
        if adaptive.effective_params.no_trade {
            return false;
        }
        match self.request.mode {
            StrategyProfileMode::DynamicSafetyOnly => true,
            StrategyProfileMode::DynamicQuoteStyle => {
                self.apply_quote_style(order, adaptive.effective_params.quote_style);
                true
            }
            StrategyProfileMode::FullDeterministic => {
                self.apply_quote_style(order, adaptive.effective_params.quote_style);
                order.size *= adaptive.effective_params.size_multiplier;
                if order.size <= Decimal::ZERO {
                    return false;
                }
                order.ttl_ms = Some(
                    order
                        .ttl_ms
                        .unwrap_or(adaptive.effective_params.order_ttl_seconds * 1000)
                        .min(adaptive.effective_params.order_ttl_seconds * 1000),
                );
                let original_edge = decimal(payload.get("expected_edge")).unwrap_or(Decimal::ZERO);
                original_edge >= adaptive.effective_params.maker_min_edge
            }
            _ => true,
        }
    }

    fn apply_quote_style(&self, order: &mut ReplayOrder, style: QuoteStyle) {
        match style {
            QuoteStyle::ImproveOneTick => {}
            QuoteStyle::JoinBestBid => {
                if let Some((best_bid, _)) = self
                    .books
                    .get(&order.token_id)
                    .and_then(OrderBookState::best_bid)
                {
                    order.price = order.price.min(best_bid);
                }
            }
            QuoteStyle::FairMinusMarginOnly => {
                order.price = (order.price - order.tick_size).max(order.tick_size);
            }
            QuoteStyle::NoQuote => {
                order.size = Decimal::ZERO;
            }
        }
    }

    fn features_for_order(&self, order: &ReplayOrder, now: DateTime<Utc>) -> RegimeFeatures {
        let market = self.market(&order.market_id);
        let up_book = market.and_then(|market| self.books.get(&market.up_token_id));
        let down_book = market.and_then(|market| self.books.get(&market.down_token_id));
        let tick_size = Decimal::new(1, 2);
        let start_price = market.and_then(|market| market.start_price);
        let reference = self.latest_reference_at(now);
        let distance_bps = match (reference, start_price) {
            (Some(reference), Some(start)) if start > Decimal::ZERO => {
                ((reference.price - start) / start * Decimal::from(10_000)).to_f64()
            }
            _ => None,
        };
        let seconds_to_expiry = market
            .and_then(|market| market.end_ts)
            .map(|end| end.signed_duration_since(now).num_milliseconds() as f64 / 1000.0);
        let seconds_since_start = market
            .and_then(|market| market.start_ts)
            .map(|start| now.signed_duration_since(start).num_milliseconds() as f64 / 1000.0);
        let max_book_age_ms = self.request.settings.risk.max_book_age_ms as f64;
        let up_age = up_book.and_then(|book| book.age_ms(now));
        let down_age = down_book.and_then(|book| book.age_ms(now));
        let book_age_ms = [up_age, down_age]
            .into_iter()
            .flatten()
            .max_by(f64::total_cmp);
        RegimeFeatures {
            seconds_since_start,
            seconds_to_expiry,
            distance_bps,
            chainlink_return_5s_bps: self.reference_return_bps(now, 5),
            chainlink_return_10s_bps: self.reference_return_bps(now, 10),
            chainlink_return_30s_bps: self.reference_return_bps(now, 30),
            chainlink_return_120s_bps: self.reference_return_bps(now, 120),
            realized_vol_30s_bps: self.realized_vol_bps(now, 30),
            realized_vol_120s_bps: self.realized_vol_bps(now, 120),
            shock_z: self.shock_z(now),
            q_up: self
                .fair_values
                .get(&order.market_id)
                .and_then(|fair| decimal(fair.get("q_up")))
                .and_then(|value| value.to_f64()),
            q_down: self
                .fair_values
                .get(&order.market_id)
                .and_then(|fair| decimal(fair.get("q_down")))
                .and_then(|value| value.to_f64()),
            sigma: self
                .fair_values
                .get(&order.market_id)
                .and_then(|fair| fair.get("sigma"))
                .and_then(Value::as_f64),
            up_bid: up_book
                .and_then(OrderBookState::best_bid)
                .and_then(|(price, _)| price.to_f64()),
            up_ask: up_book
                .and_then(OrderBookState::best_ask)
                .and_then(|(price, _)| price.to_f64()),
            up_spread_ticks: up_book.and_then(|book| book.spread_ticks(tick_size)),
            up_top_size: up_book.and_then(OrderBookState::top_size),
            down_bid: down_book
                .and_then(OrderBookState::best_bid)
                .and_then(|(price, _)| price.to_f64()),
            down_ask: down_book
                .and_then(OrderBookState::best_ask)
                .and_then(|(price, _)| price.to_f64()),
            down_spread_ticks: down_book.and_then(|book| book.spread_ticks(tick_size)),
            down_top_size: down_book.and_then(OrderBookState::top_size),
            book_update_rate_10s: None,
            reference_age_ms: reference.map(|reference| {
                now.signed_duration_since(reference.ts)
                    .num_microseconds()
                    .map_or(0.0, |micros| (micros.max(0) as f64) / 1000.0)
            }),
            book_age_ms,
            feed_divergence_bps: None,
            recent_feed_errors: self
                .feed_error_times
                .iter()
                .filter(|ts| now.signed_duration_since(**ts) <= Duration::seconds(30))
                .count() as u32,
            open_positions: None,
            open_orders: self.open_orders.len(),
            recent_fill_count: 0,
            recent_cancel_count: self.cancels as u32,
            adverse_move_after_fill_bps: None,
            market_active: market.is_some_and(|market| {
                market
                    .start_ts
                    .zip(market.end_ts)
                    .is_some_and(|(start, end)| start <= now && now < end)
            }),
            has_start_price: start_price.is_some(),
            has_books: up_book.and_then(OrderBookState::best_bid).is_some()
                && up_book.and_then(OrderBookState::best_ask).is_some()
                && down_book.and_then(OrderBookState::best_bid).is_some()
                && down_book.and_then(OrderBookState::best_ask).is_some(),
            reference_stale: reference.is_none_or(|reference| reference.stale)
                || reference.is_some_and(|reference| {
                    now.signed_duration_since(reference.ts).num_milliseconds()
                        > self.request.settings.risk.max_reference_age_ms
                }),
            book_stale: book_age_ms.is_none_or(|age| age > max_book_age_ms),
            final_no_trade_seconds: self.request.settings.strategy.final_no_trade_seconds,
            quality_flags: Vec::new(),
        }
    }

    fn reference_return_bps(&self, now: DateTime<Utc>, seconds: i64) -> Option<f64> {
        let current = self.latest_reference_at(now)?;
        let target = now - Duration::seconds(seconds);
        let prior = self
            .reference_history
            .iter()
            .rev()
            .find(|point| point.ts <= target)?;
        if prior.price <= Decimal::ZERO {
            return None;
        }
        ((current.price - prior.price) / prior.price * Decimal::from(10_000)).to_f64()
    }

    fn realized_vol_bps(&self, now: DateTime<Utc>, seconds: i64) -> Option<f64> {
        let lower = now - Duration::seconds(seconds);
        let points = self
            .reference_history
            .iter()
            .filter(|point| point.ts >= lower && point.ts <= now && point.price > Decimal::ZERO)
            .collect::<Vec<_>>();
        if points.len() < 3 {
            return None;
        }
        let mut returns = Vec::new();
        for pair in points.windows(2) {
            let Some(prev) = pair.first() else {
                continue;
            };
            let Some(next) = pair.get(1) else {
                continue;
            };
            let Some(prev_price) = prev.price.to_f64() else {
                continue;
            };
            let Some(next_price) = next.price.to_f64() else {
                continue;
            };
            if prev_price > 0.0 {
                returns.push((next_price / prev_price).ln() * 10_000.0);
            }
        }
        sample_std_f64(&returns)
    }

    fn shock_z(&self, now: DateTime<Utc>) -> Option<f64> {
        let ret = self.reference_return_bps(now, 10)?;
        let vol = self.realized_vol_bps(now, 120)?;
        if vol <= 0.0 {
            None
        } else {
            Some(ret / vol)
        }
    }

    fn fill_open_orders(&mut self, token_id: &str, recorded_ts: DateTime<Utc>) {
        let Some((best_ask, _)) = self.books.get(token_id).and_then(OrderBookState::best_ask)
        else {
            return;
        };
        let open = self.open_orders.iter().copied().collect::<Vec<_>>();
        for index in open {
            if self.orders[index].token_id != token_id {
                continue;
            }
            if self.orders[index].cancel_ts.is_some() {
                if self.would_fill(index, best_ask) {
                    self.fills_after_cancel_prevented += 1;
                }
                self.open_orders.remove(&index);
                continue;
            }
            if !self.order_can_fill(index, recorded_ts) {
                continue;
            }
            if self.would_fill(index, best_ask) {
                self.fill_order(index, self.orders[index].price, recorded_ts, true);
                self.open_orders.remove(&index);
            }
        }
    }

    fn order_can_fill(&mut self, index: usize, now: DateTime<Utc>) -> bool {
        if self.request.fill_model == FillModel::NoMakerFills
            || self.request.fill_model == FillModel::QueueProxy
        {
            return false;
        }
        let order = &self.orders[index];
        let Some(market) = self.market(&order.market_id) else {
            self.fills_prevented_market_inactive += 1;
            return false;
        };
        let Some((start_ts, end_ts)) = market.start_ts.zip(market.end_ts) else {
            self.fills_prevented_market_inactive += 1;
            return false;
        };
        if now < start_ts {
            self.fills_prevented_market_inactive += 1;
            return false;
        }
        if now >= end_ts {
            self.fills_prevented_close += 1;
            return false;
        }
        if end_ts.signed_duration_since(now).num_seconds()
            <= self.request.settings.strategy.final_no_trade_seconds
        {
            self.fills_prevented_final_window += 1;
            return false;
        }
        if now < order.decision_ts + Duration::milliseconds(self.request.fill_model.live_after_ms())
        {
            self.fills_prevented_not_live += 1;
            return false;
        }
        if order
            .ttl_ms
            .is_some_and(|ttl| now >= order.decision_ts + Duration::milliseconds(ttl))
        {
            self.fills_prevented_expired += 1;
            return false;
        }
        true
    }

    fn would_fill(&self, index: usize, best_ask: Decimal) -> bool {
        let order = &self.orders[index];
        if order.side != "buy" {
            return false;
        }
        match self.request.fill_model {
            FillModel::NoMakerFills | FillModel::QueueProxy => false,
            FillModel::TradeThrough => best_ask <= order.price - order.tick_size,
            _ => best_ask <= order.price,
        }
    }

    fn fill_order(&mut self, index: usize, price: Decimal, fill_ts: DateTime<Utc>, maker: bool) {
        if self.orders[index].is_filled() {
            return;
        }
        let fill_ref_price = self
            .latest_reference_at(fill_ts)
            .map(|reference| reference.price);
        let order = &mut self.orders[index];
        order.filled_size = order.size;
        order.avg_price = Some(price);
        order.fill_ts = Some(fill_ts);
        order.fill_ref_price = fill_ref_price;
        if maker {
            self.maker_fills += 1;
        } else {
            order.fee = crypto_taker_fee_per_share(price).unwrap_or(Decimal::ZERO) * order.size;
            self.taker_fills += 1;
        }
        self.fills += 1;
    }

    fn cancel_market(&mut self, market_id: &str, recorded_ts: DateTime<Utc>) {
        let open = self.open_orders.iter().copied().collect::<Vec<_>>();
        for index in open {
            if market_id.is_empty() || self.orders[index].market_id == market_id {
                self.orders[index].cancel_ts = Some(recorded_ts);
                self.open_orders.remove(&index);
                self.cancels += 1;
            }
        }
    }

    fn apply_adverse_penalties(&mut self, reference: &ReferencePoint) {
        if self.request.fill_model != FillModel::AdverseSelectionPenalized {
            return;
        }
        for order in &mut self.orders {
            if !order.is_filled() || order.adverse_checked {
                continue;
            }
            let Some(fill_ts) = order.fill_ts else {
                continue;
            };
            if reference.ts < fill_ts || reference.ts > fill_ts + Duration::seconds(5) {
                continue;
            }
            let Some(fill_ref) = order.fill_ref_price else {
                order.adverse_checked = true;
                continue;
            };
            let adverse = (order.outcome == "up" && reference.price < fill_ref)
                || (order.outcome == "down" && reference.price > fill_ref);
            if adverse {
                order.adverse_penalty = order.filled_size * Decimal::new(5, 3);
            }
            order.adverse_checked = true;
        }
    }

    fn expire_reference_history(&mut self, now: DateTime<Utc>) {
        while self.reference_history.front().is_some_and(|point| {
            now.signed_duration_since(point.ts) > Duration::seconds(REFERENCE_HISTORY_SECONDS)
        }) {
            self.reference_history.pop_front();
        }
        while self
            .feed_error_times
            .front()
            .is_some_and(|ts| now.signed_duration_since(*ts) > Duration::seconds(60))
        {
            self.feed_error_times.pop_front();
        }
    }

    fn market(&self, market_id: &str) -> Option<&MarketTruth> {
        self.markets.get(market_id)
    }

    fn latest_reference_at(&self, now: DateTime<Utc>) -> Option<&ReferencePoint> {
        self.reference_history
            .iter()
            .rev()
            .find(|reference| reference.ts <= now)
    }

    fn finish(mut self) -> Value {
        for market in self.markets.values_mut() {
            market.finalize_flags();
        }
        let mut market_results = Vec::new();
        let mut gross = Decimal::ZERO;
        let mut fees = Decimal::ZERO;
        let mut adverse_penalties = Decimal::ZERO;
        let mut notional_cost = Decimal::ZERO;
        let mut time_bucket_pnl = BTreeMap::<String, Decimal>::new();
        let mut q_bucket_pnl = BTreeMap::<String, Decimal>::new();
        for market in self.markets.values() {
            let winning = market.winning_outcome.clone();
            let market_orders = self
                .orders
                .iter()
                .filter(|order| order.market_id == market.market_id && order.is_filled())
                .collect::<Vec<_>>();
            let mut market_gross = Decimal::ZERO;
            let mut market_fees = Decimal::ZERO;
            let mut market_penalty = Decimal::ZERO;
            let mut market_cost = Decimal::ZERO;
            if let Some(winning_outcome) = winning.as_deref() {
                for order in market_orders {
                    let cost = order.avg_price.unwrap_or(order.price) * order.filled_size;
                    let payout = if order.outcome == winning_outcome {
                        order.filled_size
                    } else {
                        Decimal::ZERO
                    };
                    let pnl = payout - cost - order.fee - order.adverse_penalty;
                    market_gross += payout - cost;
                    market_fees += order.fee;
                    market_penalty += order.adverse_penalty;
                    market_cost += cost;
                    if let Some(bucket) = market.end_ts.zip(order.fill_ts).map(|(end, fill)| {
                        time_to_expiry_bucket(end.signed_duration_since(fill).num_seconds())
                    }) {
                        *time_bucket_pnl.entry(bucket).or_insert(Decimal::ZERO) += pnl;
                    }
                    if let Some(q) = order.q_at_decision {
                        *q_bucket_pnl.entry(q_bucket(q)).or_insert(Decimal::ZERO) += pnl;
                    }
                }
            }
            gross += market_gross;
            fees += market_fees;
            adverse_penalties += market_penalty;
            notional_cost += market_cost;
            market_results.push(json!({
                "market_id": market.market_id,
                "market_slug": market.slug,
                "start_ts": market.start_ts.map(ts),
                "end_ts": market.end_ts.map(ts),
                "start_price": market.start_price.map(|value| value.to_string()),
                "final_price": market.final_price.map(|value| value.to_string()),
                "winning_outcome": winning,
                "filled_orders": self.orders.iter().filter(|order| order.market_id == market.market_id && order.is_filled()).count(),
                "gross_pnl": market_gross.to_string(),
                "fees": market_fees.to_string(),
                "adverse_penalty": market_penalty.to_string(),
                "net_pnl": (market_gross - market_fees - market_penalty).to_string(),
                "notional_cost": market_cost.to_string(),
                "complete_for_simulation": market.complete_for_simulation()
            }));
        }
        let net = gross - fees - adverse_penalties;
        let stats = market_level_statistics_json(&market_results);
        let drawdown = max_drawdown(&market_results);
        let queue_proxy = queue_proxy_report(
            self.request.fill_model,
            self.queue_evidence_events,
            self.trade_evidence_events,
            self.depletion_evidence_events,
        );
        if self.request.fill_model == FillModel::QueueProxy {
            if queue_proxy["evidence_complete"].as_bool() == Some(true) {
                self.warnings.insert(
                    "queue_proxy evidence is present, but maker fills remain disabled until the depletion semantics are validated"
                        .to_owned(),
                );
            } else {
                self.warnings.insert(
                    "queue_proxy skipped maker fills because queue depletion/trade evidence is incomplete"
                        .to_owned(),
                );
            }
        }
        let warnings = self
            .warnings
            .into_iter()
            .map(Value::String)
            .collect::<Vec<_>>();
        json!({
            "name": self.request.name,
            "profile": self.request.mode.as_str(),
            "fill_model": self.request.fill_model.as_str(),
            "events": self.event_count,
            "markets_seen": self.markets.len(),
            "markets_settled": self.markets.values().filter(|market| market.complete_for_simulation()).count(),
            "decisions": self.decisions_seen,
            "orders": self.orders_seen,
            "fills": self.fills,
            "maker_fills": self.maker_fills,
            "taker_fills": self.taker_fills,
            "fill_rate": ratio_usize(self.fills, self.orders_seen),
            "cancels": self.cancels,
            "cancel_fill_ratio": ratio_usize(self.cancels, self.fills),
            "skipped_by_profile": self.skipped_by_profile,
            "gross_pnl": gross.to_string(),
            "fees": fees.to_string(),
            "adverse_penalty": adverse_penalties.to_string(),
            "net_pnl": net.to_string(),
            "notional_cost": notional_cost.to_string(),
            "roi": decimal_ratio(net, notional_cost),
            "market_level_statistics": stats,
            "max_drawdown": drawdown.to_string(),
            "profitable_markets": market_results.iter().filter(|row| row["net_pnl"].as_str().is_some_and(|value| decimal_from_str(value) > Decimal::ZERO)).count(),
            "losing_markets": market_results.iter().filter(|row| row["net_pnl"].as_str().is_some_and(|value| decimal_from_str(value) < Decimal::ZERO)).count(),
            "time_to_expiry_bucket_pnl": decimal_map_json(&time_bucket_pnl),
            "q_bucket_pnl": decimal_map_json(&q_bucket_pnl),
            "replay_metrics": {
                "fills_after_cancel_prevented": self.fills_after_cancel_prevented,
                "fills_prevented_not_live": self.fills_prevented_not_live,
                "fills_prevented_final_window": self.fills_prevented_final_window,
                "fills_prevented_market_inactive": self.fills_prevented_market_inactive,
                "fills_prevented_expired": self.fills_prevented_expired,
                "fills_prevented_close": self.fills_prevented_close,
                "queue_proxy": queue_proxy,
                "open_orders_remaining": self.open_orders.len(),
                "maker_fee_model": "zero",
                "taker_fee_model": "shares * 0.07 * price * (1 - price)"
            },
            "regime_frequency": self.regime_frequency,
            "regime_time_share": self.regime_time_share,
            "adaptive_decision_log_sample": self.adaptive_logs,
            "warnings": warnings,
            "market_results": market_results
        })
    }
}

fn run_replay_requests(
    input: &Path,
    markets: &[MarketTruth],
    requests: Vec<ReplayRequest>,
    exclude_windows: &[ExcludedTimeWindow],
) -> Result<Vec<Value>, ResearchError> {
    let mut engines = requests
        .into_iter()
        .map(|request| ResearchReplayEngine::new(request, markets))
        .collect::<Vec<_>>();
    let stream = stream_events(
        input,
        EventPathMode::PreferEventsJsonl,
        exclude_windows,
        |event| {
            for engine in &mut engines {
                engine.observe(event);
            }
        },
    )?;
    let mut results = Vec::new();
    for mut engine in engines {
        if stream.malformed_lines > 0 {
            engine.warnings.insert(format!(
                "{} malformed lines skipped",
                stream.malformed_lines
            ));
        }
        for warning in &stream.warnings {
            engine.warnings.insert(warning.clone());
        }
        for warning in exclusion_warnings(&stream, exclude_windows) {
            if let Some(text) = warning.as_str() {
                engine.warnings.insert(text.to_owned());
            }
        }
        let mut result = engine.finish();
        if let Some(object) = result.as_object_mut() {
            insert_exclusion_metadata(object, &stream, exclude_windows);
        }
        results.push(result);
    }
    Ok(results)
}

fn empty_replay_result() -> Value {
    json!({
        "events": 0,
        "markets_seen": 0,
        "markets_settled": 0,
        "orders": 0,
        "fills": 0,
        "net_pnl": "0",
        "warnings": ["no replay result produced"],
        "market_results": []
    })
}

#[derive(Default)]
struct CalibrationAccumulator {
    markets: BTreeMap<String, MarketTruth>,
    pending: BTreeMap<String, BTreeMap<String, CalibrationBucket>>,
    time_buckets: BTreeMap<String, BTreeMap<String, CalibrationBucket>>,
    warnings: Vec<Value>,
}

#[derive(Clone, Debug, Default)]
struct CalibrationBucket {
    count: usize,
    sum_q: f64,
    sum_q2: f64,
    sum_log_q: f64,
    sum_log_one_minus_q: f64,
    observed_up: usize,
}

impl CalibrationBucket {
    fn add_pending(&mut self, q: f64) {
        self.count += 1;
        self.sum_q += q;
        self.sum_q2 += q * q;
        self.sum_log_q += q.clamp(1e-6, 1.0 - 1e-6).ln();
        self.sum_log_one_minus_q += (1.0 - q).clamp(1e-6, 1.0 - 1e-6).ln();
    }

    fn settle(&mut self, observed_up: bool) {
        if observed_up {
            self.observed_up += self.count;
        }
    }

    fn as_json(&self) -> Value {
        if self.count == 0 {
            return json!({
                "decision_count": 0,
                "avg_q_up": null,
                "observed_up_frequency": null,
                "calibration_error": null,
                "brier_score": null,
                "log_loss": null
            });
        }
        let n = self.count as f64;
        let observed = self.observed_up as f64 / n;
        let avg_q = self.sum_q / n;
        let brier = if self.observed_up == self.count {
            (self.sum_q2 - 2.0 * self.sum_q + n) / n
        } else if self.observed_up == 0 {
            self.sum_q2 / n
        } else {
            // Mixed observed values should not happen inside one market-level settle pass, but
            // this keeps merged buckets numerically meaningful.
            let y = observed;
            (self.sum_q2 - 2.0 * y * self.sum_q + n * y) / n
        };
        let log_loss = if observed >= 0.5 {
            -self.sum_log_q / n
        } else {
            -self.sum_log_one_minus_q / n
        };
        json!({
            "decision_count": self.count,
            "avg_q_up": avg_q,
            "observed_up_frequency": observed,
            "calibration_error": observed - avg_q,
            "brier_score": brier,
            "log_loss": log_loss
        })
    }
}

impl CalibrationAccumulator {
    fn new(markets: Vec<MarketTruth>) -> Self {
        Self {
            markets: markets
                .into_iter()
                .map(|market| (market.market_id.clone(), market))
                .collect(),
            pending: BTreeMap::new(),
            time_buckets: BTreeMap::new(),
            warnings: Vec::new(),
        }
    }

    fn observe(&mut self, event: &EventLine) {
        match event.event_type.as_str() {
            "market" => {
                let market = market_from_payload(&event.payload);
                if !market.market_id.is_empty() {
                    self.markets
                        .entry(market.market_id.clone())
                        .and_modify(|existing| existing.merge(market.clone()))
                        .or_insert(market);
                }
            }
            "market_start_price" => {
                let market_id = text(&event.payload, "market_id");
                if let Some(market) = self.markets.get_mut(&market_id) {
                    market.start_price =
                        decimal(event.payload.get("start_price")).or(market.start_price);
                }
            }
            "reference" => {
                let Some(price) = decimal(event.payload.get("price")) else {
                    return;
                };
                let Some(source_ts) = parse_datetime(event.payload.get("source_ts")) else {
                    return;
                };
                for market in self.markets.values_mut() {
                    market.observe_settlement_reference(source_ts, price);
                }
            }
            "fair_value" => {
                let market_id = text(&event.payload, "market_id");
                let Some(q) = decimal(event.payload.get("q_up")).and_then(|value| value.to_f64())
                else {
                    return;
                };
                let bucket = q_bucket_f64(q);
                self.pending
                    .entry(market_id.clone())
                    .or_default()
                    .entry(bucket)
                    .or_default()
                    .add_pending(q);
                if let Some(market) = self.markets.get(&market_id) {
                    if let Some(end_ts) = market.end_ts {
                        let time_bucket = time_to_expiry_bucket(
                            end_ts
                                .signed_duration_since(event.recorded_ts)
                                .num_seconds(),
                        );
                        self.time_buckets
                            .entry(time_bucket)
                            .or_default()
                            .entry(q_bucket_f64(q))
                            .or_default()
                            .add_pending(q);
                    }
                }
            }
            _ => {}
        }
    }

    fn add_stream_warnings(&mut self, warnings: Vec<String>) {
        self.warnings
            .extend(warnings.into_iter().map(Value::String));
    }

    fn finish(mut self) -> Value {
        for market in self.markets.values_mut() {
            market.finalize_flags();
        }
        let mut merged = BTreeMap::<String, CalibrationBucket>::new();
        for (market_id, buckets) in &self.pending {
            let observed_up = self
                .markets
                .get(market_id)
                .and_then(|market| market.winning_outcome.as_deref())
                == Some("up");
            if !self
                .markets
                .get(market_id)
                .is_some_and(MarketTruth::complete_for_simulation)
            {
                continue;
            }
            for (bucket_name, bucket) in buckets {
                let mut bucket = bucket.clone();
                bucket.settle(observed_up);
                merge_calibration_bucket(merged.entry(bucket_name.clone()).or_default(), &bucket);
            }
        }
        let by_q_bucket = merged
            .iter()
            .map(|(bucket, stats)| (bucket.clone(), stats.as_json()))
            .collect::<Map<_, _>>();
        if by_q_bucket.is_empty() {
            self.warnings.push(json!(
                "no settled fair_value rows available for calibration; run on the full normalized dataset"
            ));
        }
        json!({
            "q_up_buckets": Value::Object(by_q_bucket),
            "grouped_by_time_to_expiry": calibration_group_json(&self.time_buckets),
            "grouped_by_distance_bps": Value::Object(Map::new()),
            "grouped_by_volatility_regime": Value::Object(Map::new()),
            "grouped_by_spread_bucket": Value::Object(Map::new()),
            "grouped_by_regime_label": Value::Object(Map::new()),
            "warnings": self.warnings
        })
    }
}

fn merge_calibration_bucket(target: &mut CalibrationBucket, source: &CalibrationBucket) {
    target.count += source.count;
    target.sum_q += source.sum_q;
    target.sum_q2 += source.sum_q2;
    target.sum_log_q += source.sum_log_q;
    target.sum_log_one_minus_q += source.sum_log_one_minus_q;
    target.observed_up += source.observed_up;
}

fn calibration_group_json(groups: &BTreeMap<String, BTreeMap<String, CalibrationBucket>>) -> Value {
    let mut output = Map::new();
    for (group, buckets) in groups {
        let mut bucket_map = Map::new();
        for (bucket, stats) in buckets {
            bucket_map.insert(bucket.clone(), stats.as_json());
        }
        output.insert(group.clone(), Value::Object(bucket_map));
    }
    Value::Object(output)
}

fn group_sweep_results(results: Vec<Value>) -> Vec<Value> {
    let mut by_candidate = BTreeMap::<String, Vec<Value>>::new();
    for result in results {
        let name = result["name"].as_str().unwrap_or("unknown");
        let candidate = name
            .split("__")
            .next()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| name.to_owned());
        by_candidate.entry(candidate).or_default().push(result);
    }
    by_candidate
        .into_iter()
        .map(|(candidate, rows)| {
            let robust = rows.iter().all(|row| {
                row["net_pnl"]
                    .as_str()
                    .is_some_and(|value| decimal_from_str(value) > Decimal::ZERO)
            }) && rows.len() >= 2;
            json!({
                "candidate": candidate,
                "robust_candidate": robust,
                "fill_model_results": rows
            })
        })
        .collect()
}

fn split_plan(results: &[Value], split_method: &str) -> (Value, Vec<Value>) {
    let days = sweep_market_days(results);
    let mut warnings = Vec::new();
    if days.len() < 3 {
        warnings.push(json!(
            "fewer than three market days available; split metrics are informational only"
        ));
    }
    let walk_forward_folds = if days.len() >= 3 {
        (1..days.len() - 1)
            .map(|validation_index| {
                json!({
                    "train_days": days[..validation_index].to_vec(),
                    "validation_day": days[validation_index],
                    "test_day": days[validation_index + 1]
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let leave_one_day_out_folds = days
        .iter()
        .map(|test_day| {
            json!({
                "train_days": days.iter().filter(|day| *day != test_day).cloned().collect::<Vec<_>>(),
                "test_day": test_day
            })
        })
        .collect::<Vec<_>>();
    let latest_walk_forward = walk_forward_folds.last().cloned().unwrap_or_else(|| {
        json!({
            "train_days": days.iter().take(days.len().saturating_sub(2)).cloned().collect::<Vec<_>>(),
            "validation_day": days.get(days.len().saturating_sub(2)).cloned(),
            "test_day": days.last().cloned()
        })
    });
    let selected = if split_method.eq_ignore_ascii_case("leave_one_day_out") {
        json!({
            "method": "leave_one_day_out",
            "folds": leave_one_day_out_folds,
            "selection_rule": "summarize held-out day stability; do not tune on held-out days"
        })
    } else {
        json!({
            "method": "walk_forward",
            "folds": walk_forward_folds,
            "selection_rule": "rank on validation only; report next day as test"
        })
    };
    (
        json!({
            "requested_method": split_method,
            "market_days": days,
            "latest_walk_forward": latest_walk_forward,
            "walk_forward": selected,
            "leave_one_day_out": {
                "folds": leave_one_day_out_folds,
                "selection_rule": "summarize held-out day stability; do not tune on held-out days"
            },
            "no_future_leakage_rule": "training days must be strictly earlier than validation/test days"
        }),
        warnings,
    )
}

fn add_split_metrics(candidates: &mut [Value], plan: &Value) {
    let latest = &plan["latest_walk_forward"];
    let train_days = json_string_array(&latest["train_days"]);
    let validation_days = latest["validation_day"]
        .as_str()
        .map(|day| vec![day.to_owned()])
        .unwrap_or_default();
    let test_days = latest["test_day"]
        .as_str()
        .map(|day| vec![day.to_owned()])
        .unwrap_or_default();
    for candidate in candidates {
        let Some(rows) = candidate
            .get_mut("fill_model_results")
            .and_then(Value::as_array_mut)
        else {
            continue;
        };
        for row in rows {
            let split = json!({
                "train": market_split_stats(row, &train_days),
                "validation": market_split_stats(row, &validation_days),
                "test": market_split_stats(row, &test_days)
            });
            if let Some(object) = row.as_object_mut() {
                object.insert("split_performance".to_owned(), split);
            }
        }
    }
}

fn sweep_market_days(results: &[Value]) -> Vec<String> {
    let mut days = BTreeSet::new();
    for result in results {
        if let Some(markets) = result.get("market_results").and_then(Value::as_array) {
            for market in markets {
                if let Some(day) = market_day(market) {
                    days.insert(day);
                }
            }
        }
    }
    days.into_iter().collect()
}

fn json_string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

fn market_split_stats(result: &Value, days: &[String]) -> Value {
    if days.is_empty() {
        return json!({
            "days": [],
            "markets": 0,
            "net_pnl": "0",
            "profitable_markets": 0,
            "losing_markets": 0
        });
    }
    let mut markets = 0usize;
    let mut net = Decimal::ZERO;
    let mut profitable = 0usize;
    let mut losing = 0usize;
    if let Some(rows) = result.get("market_results").and_then(Value::as_array) {
        for row in rows {
            if market_day(row).is_some_and(|day| days.contains(&day)) {
                markets += 1;
                let pnl = row
                    .get("net_pnl")
                    .and_then(Value::as_str)
                    .map(decimal_from_str)
                    .unwrap_or(Decimal::ZERO);
                net += pnl;
                match pnl.cmp(&Decimal::ZERO) {
                    std::cmp::Ordering::Greater => profitable += 1,
                    std::cmp::Ordering::Less => losing += 1,
                    std::cmp::Ordering::Equal => {}
                }
            }
        }
    }
    json!({
        "days": days,
        "markets": markets,
        "net_pnl": net.to_string(),
        "profitable_markets": profitable,
        "losing_markets": losing
    })
}

fn market_day(row: &Value) -> Option<String> {
    row.get("end_ts")
        .and_then(Value::as_str)
        .or_else(|| row.get("start_ts").and_then(Value::as_str))
        .and_then(|value| value.get(0..10))
        .map(ToOwned::to_owned)
}

fn sweep_candidates(max: usize) -> Vec<SweepCandidate> {
    let edges = [d("0.005"), d("0.010"), d("0.015"), d("0.020"), d("0.030")];
    let ttls = [1, 2, 5, 10, 20, 30];
    let final_windows = [30, 60, 90, 120, 180];
    let styles = [
        QuoteStyle::ImproveOneTick,
        QuoteStyle::JoinBestBid,
        QuoteStyle::FairMinusMarginOnly,
    ];
    let mut candidates = Vec::new();
    candidates.push(SweepCandidate {
        name: "baseline".to_owned(),
        maker_min_edge: d("0.010"),
        ttl_seconds: 10,
        final_no_trade_seconds: 30,
        quote_style: QuoteStyle::ImproveOneTick,
    });
    'outer: for edge in edges {
        for ttl in ttls {
            for final_window in final_windows {
                for style in styles {
                    candidates.push(SweepCandidate {
                        name: format!(
                            "edge_{}_ttl_{}_final_{}_style_{:?}",
                            edge, ttl, final_window, style
                        )
                        .to_ascii_lowercase(),
                        maker_min_edge: edge,
                        ttl_seconds: ttl,
                        final_no_trade_seconds: final_window,
                        quote_style: style,
                    });
                    if candidates.len() >= max {
                        break 'outer;
                    }
                }
            }
        }
    }
    candidates
}

fn sample_size_stats(values: &[Decimal]) -> Value {
    let n = values.len();
    let mean = mean_decimal(values);
    let median = median_decimal(values);
    let std = std_decimal(values, mean);
    let se = std.and_then(|value| Decimal::from_f64_retain(value.to_f64()? / (n as f64).sqrt()));
    let ci_low = mean
        .zip(se)
        .map(|(mean, se)| mean - Decimal::new(196, 2) * se);
    let ci_high = mean
        .zip(se)
        .map(|(mean, se)| mean + Decimal::new(196, 2) * se);
    let required_005 = std.and_then(|std| required_n_for_precision(std, Decimal::new(5, 2)));
    let required_010 = std.and_then(|std| required_n_for_precision(std, Decimal::new(10, 2)));
    let required_detect = mean
        .zip(std)
        .and_then(|(mean, std)| required_n_to_detect_mean(mean, std));
    json!({
        "n": n,
        "mean": mean.map(|value| value.to_string()),
        "median": median.map(|value| value.to_string()),
        "std": std.map(|value| value.to_string()),
        "standard_error": se.map(|value| value.to_string()),
        "ci_low": ci_low.map(|value| value.to_string()),
        "ci_high": ci_high.map(|value| value.to_string()),
        "profitable_count": values.iter().filter(|value| **value > Decimal::ZERO).count(),
        "losing_count": values.iter().filter(|value| **value < Decimal::ZERO).count(),
        "required_n_for_plus_minus_0_05": required_005,
        "required_n_for_plus_minus_0_10": required_010,
        "required_n_to_detect_observed_mean": required_detect,
        "profitability_claim_allowed": ci_low.is_some_and(|value| value > Decimal::ZERO)
    })
}

fn required_n_for_precision(std: Decimal, precision: Decimal) -> Option<u64> {
    if precision <= Decimal::ZERO {
        return None;
    }
    let value = (Decimal::new(196, 2) * std / precision).to_f64()?.powi(2);
    Some(value.ceil() as u64)
}

fn required_n_to_detect_mean(mean: Decimal, std: Decimal) -> Option<u64> {
    if mean == Decimal::ZERO {
        return None;
    }
    let ratio = (std / mean.abs()).to_f64()?;
    Some((7.84 * ratio * ratio).ceil() as u64)
}

fn extract_market_pnls(source: &Value) -> Vec<Decimal> {
    if let Some(markets) = source
        .pointer("/result/market_results")
        .and_then(Value::as_array)
    {
        return markets
            .iter()
            .filter(|row| row["winning_outcome"].is_string())
            .filter_map(|row| {
                row.get("net_pnl")
                    .and_then(Value::as_str)
                    .map(decimal_from_str)
            })
            .collect();
    }
    if let Some(models) = source
        .pointer("/result/fill_models")
        .and_then(Value::as_array)
    {
        if let Some(primary) = models
            .iter()
            .find(|row| row["fill_model"].as_str() == Some("touch_after_250ms"))
            .or_else(|| models.first())
        {
            return primary["market_results"]
                .as_array()
                .map(|markets| {
                    markets
                        .iter()
                        .filter(|row| row["winning_outcome"].is_string())
                        .filter_map(|row| {
                            row.get("net_pnl")
                                .and_then(Value::as_str)
                                .map(decimal_from_str)
                        })
                        .collect()
                })
                .unwrap_or_default();
        }
    }
    Vec::new()
}

fn choose_recommendation(
    baseline: &Option<Value>,
    regimes: &Option<Value>,
    sample_size: &Option<Value>,
) -> &'static str {
    let sample_allows = sample_size
        .as_ref()
        .and_then(|value| value.pointer("/result/statistics/profitability_claim_allowed"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !sample_allows {
        return "Continue collecting data unchanged";
    }
    let baseline_primary = baseline
        .as_ref()
        .and_then(|value| value.pointer("/result/fill_models"))
        .and_then(Value::as_array)
        .and_then(|models| {
            models
                .iter()
                .find(|row| row["fill_model"].as_str() == Some("touch_after_250ms"))
        })
        .and_then(|row| row["net_pnl"].as_str())
        .map(decimal_from_str)
        .unwrap_or(Decimal::ZERO);
    let best_adaptive = regimes
        .as_ref()
        .and_then(|value| value.pointer("/result/profiles"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| row["profile"].as_str() != Some("static"))
        .filter_map(|row| row["net_pnl"].as_str().map(decimal_from_str))
        .max()
        .unwrap_or(Decimal::ZERO);
    if best_adaptive > baseline_primary {
        "Keep adaptive profiles research-only"
    } else {
        "Reject adaptive profiles"
    }
}

fn market_level_statistics_json(market_results: &[Value]) -> Value {
    let pnls = market_results
        .iter()
        .filter(|row| row["winning_outcome"].is_string())
        .filter_map(|row| row["net_pnl"].as_str().map(decimal_from_str))
        .collect::<Vec<_>>();
    sample_size_stats(&pnls)
}

fn queue_proxy_report(
    fill_model: FillModel,
    queue_events: usize,
    trade_events: usize,
    depletion_events: usize,
) -> Value {
    let evidence_complete = queue_events > 0 && trade_events > 0 && depletion_events > 0;
    let status = if fill_model != FillModel::QueueProxy {
        "not_requested"
    } else if evidence_complete {
        "evidence_present_fill_simulation_disabled_until_validated"
    } else {
        "skipped_missing_queue_depletion_trade_evidence"
    };
    json!({
        "status": status,
        "skipped": fill_model == FillModel::QueueProxy,
        "evidence_complete": evidence_complete,
        "queue_evidence_events": queue_events,
        "trade_evidence_events": trade_events,
        "depletion_evidence_events": depletion_events,
        "required_before_enabling": [
            "resting queue position or size-ahead estimate",
            "trade prints or equivalent executed volume by token/price/time",
            "book level depletion evidence after order placement"
        ]
    })
}

fn max_drawdown(market_results: &[Value]) -> Decimal {
    let mut rows = market_results.to_vec();
    rows.sort_by(|left, right| left["end_ts"].as_str().cmp(&right["end_ts"].as_str()));
    let mut cumulative = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut max_drawdown = Decimal::ZERO;
    for row in rows {
        let pnl = row["net_pnl"]
            .as_str()
            .map(decimal_from_str)
            .unwrap_or(Decimal::ZERO);
        cumulative += pnl;
        peak = peak.max(cumulative);
        max_drawdown = max_drawdown.max(peak - cumulative);
    }
    max_drawdown
}

fn q_bucket(q: Decimal) -> String {
    q.to_f64()
        .map(q_bucket_f64)
        .unwrap_or_else(|| "unknown".to_owned())
}

fn q_bucket_f64(q: f64) -> String {
    match q {
        value if value < 0.40 => "0.00-0.40",
        value if value < 0.45 => "0.40-0.45",
        value if value < 0.50 => "0.45-0.50",
        value if value < 0.55 => "0.50-0.55",
        value if value < 0.60 => "0.55-0.60",
        value if value < 0.70 => "0.60-0.70",
        _ => "0.70-1.00",
    }
    .to_owned()
}

fn time_to_expiry_bucket(seconds: i64) -> String {
    match seconds {
        value if value > 12 * 60 => "15-12m",
        value if value > 9 * 60 => "12-9m",
        value if value > 6 * 60 => "9-6m",
        value if value > 3 * 60 => "6-3m",
        value if value > 60 => "3-1m",
        value if value >= 0 => "final_60s",
        _ => "inside_final_no_trade_window",
    }
    .to_owned()
}

fn envelope(
    command: &str,
    input: &Path,
    fill_model: &str,
    split_method: &str,
    duration: std::time::Duration,
    warnings: Vec<Value>,
    result: Value,
) -> Value {
    let result = redact_json(&result);
    json!({
        "command": command,
        "input_path": input.to_string_lossy(),
        "generated_at": now_ts(),
        "git_sha": git_sha(),
        "backend": "rust",
        "data_window": data_window(&result),
        "config": {
            "adaptive_regime_enabled": false,
            "adaptive_regime_mode": "research_only_or_paper_only",
            "live_trading_enabled": false
        },
        "fill_model": fill_model,
        "split_method": split_method,
        "duration_ms": duration.as_secs_f64() * 1000.0,
        "warnings": warnings,
        "result": result
    })
}

fn redact_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    if is_secret_key(key) {
                        (key.clone(), Value::String(REDACTED.to_owned()))
                    } else {
                        (key.clone(), redact_json(value))
                    }
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact_json).collect()),
        _ => value.clone(),
    }
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    SECRET_KEY_FRAGMENTS
        .iter()
        .any(|fragment| key.contains(fragment))
}

fn data_window(result: &Value) -> Value {
    if let Some(markets) = result
        .get("markets")
        .and_then(Value::as_array)
        .or_else(|| result.get("market_results").and_then(Value::as_array))
    {
        let first = markets
            .iter()
            .filter_map(|row| row.get("start_ts").and_then(Value::as_str))
            .min()
            .map(ToOwned::to_owned);
        let last = markets
            .iter()
            .filter_map(|row| row.get("end_ts").and_then(Value::as_str))
            .max()
            .map(ToOwned::to_owned);
        return json!({"start": first, "end": last});
    }
    json!({"start": null, "end": null})
}

fn git_sha() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn write_json_and_markdown(
    json_path: &Path,
    markdown_path: &Path,
    value: &Value,
    markdown: &str,
) -> Result<(), ResearchError> {
    write_json_file(json_path, value)?;
    write_text_file(markdown_path, markdown)?;
    Ok(())
}

fn write_json_file(path: &Path, value: &Value) -> Result<(), ResearchError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    serde_json::to_writer_pretty(BufWriter::new(file), value)?;
    Ok(())
}

fn write_text_file(path: &Path, text: &str) -> Result<(), ResearchError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(text.as_bytes())?;
    Ok(())
}

fn read_json_file(path: &Path) -> Result<Value, ResearchError> {
    let file = File::open(path)?;
    serde_json::from_reader(BufReader::new(file)).map_err(ResearchError::Json)
}

fn read_optional_json(path: &Path) -> Result<Option<Value>, ResearchError> {
    match File::open(path) {
        Ok(file) => serde_json::from_reader(BufReader::new(file))
            .map(Some)
            .map_err(ResearchError::Json),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ResearchError::Io(error)),
    }
}

fn audit_markdown(report: &Value) -> String {
    let result = &report["result"];
    format!(
        "# Data Audit\n\n- Events: {}\n- Markets seen: {}\n- Markets with start price: {}\n- Markets settled: {}\n- Decisions: {}\n- Execution reports: {}\n- Malformed lines: {}\n- Warnings: {}\n",
        result["total_events"],
        result["markets_seen"],
        result["markets_with_start_price"],
        result["markets_settled"],
        result["decision_count"],
        result["execution_report_count"],
        result["malformed_lines"],
        markdown_list(&report["warnings"])
    )
}

fn markets_markdown(report: &Value) -> String {
    let summary = &report["result"]["summary"];
    format!(
        "# Markets Summary\n\n- Markets: {}\n- Complete for simulation: {}\n- Missing start price: {}\n- Missing final price: {}\n- Total decisions: {}\n- Total fills: {}\n",
        summary["markets"],
        summary["complete_for_simulation"],
        summary["missing_start_price"],
        summary["missing_final_price"],
        summary["total_decisions"],
        summary["total_fills"]
    )
}

fn replay_markdown(report: &Value) -> String {
    let result = &report["result"];
    format!(
        "# Replay\n\n- Fill model: {}\n- Profile: {}\n- Markets settled: {}\n- Orders: {}\n- Fills: {}\n- Net PnL: {}\n- Max drawdown: {}\n- Cancel/fill ratio: {}\n- Warnings: {}\n",
        result["fill_model"],
        result["profile"],
        result["markets_settled"],
        result["orders"],
        result["fills"],
        result["net_pnl"],
        result["max_drawdown"],
        result["cancel_fill_ratio"],
        markdown_list(&result["warnings"])
    )
}

fn baseline_markdown(report: &Value) -> String {
    let mut text = "# Baseline Static Strategy\n\n".to_owned();
    if let Some(models) = report["result"]["fill_models"].as_array() {
        for model in models {
            text.push_str(&format!(
                "- `{}`: net PnL {}, fills {}, markets {}, CI [{}, {}]\n",
                model["fill_model"].as_str().unwrap_or("unknown"),
                model["net_pnl"].as_str().unwrap_or("0"),
                model["fills"],
                model["markets_settled"],
                model["market_level_statistics"]["ci_low"]
                    .as_str()
                    .unwrap_or("null"),
                model["market_level_statistics"]["ci_high"]
                    .as_str()
                    .unwrap_or("null")
            ));
        }
    }
    text
}

fn regimes_markdown(report: &Value) -> String {
    let mut text = "# Regime Profiles\n\n".to_owned();
    if let Some(comparisons) = report["result"]["comparisons"].as_array() {
        for row in comparisons {
            text.push_str(&format!(
                "- `{}`: net PnL {}, delta vs static {}\n",
                row["profile"].as_str().unwrap_or("unknown"),
                row["net_pnl"].as_str().unwrap_or("0"),
                row["delta_vs_static"].as_str().unwrap_or("0")
            ));
        }
    }
    text.push_str("\nAdaptive profiles remain research-only and are not live-deployable.\n");
    text
}

fn sweep_markdown(report: &Value) -> String {
    let count = report["result"]["candidates"]
        .as_array()
        .map_or(0, Vec::len);
    format!(
        "# Parameter Sweep\n\n- Candidates evaluated: {}\n- Split method: {}\n- Warning: {}\n",
        count,
        report["result"]["split_method"]
            .as_str()
            .unwrap_or("walk_forward"),
        markdown_list(&report["warnings"])
    )
}

fn calibration_markdown(report: &Value) -> String {
    let mut text = "# Calibration\n\n".to_owned();
    if let Some(buckets) = report["result"]["q_up_buckets"].as_object() {
        for (bucket, stats) in buckets {
            text.push_str(&format!(
                "- `{}`: count {}, avg q {}, observed up {}, error {}\n",
                bucket,
                stats["decision_count"],
                stats["avg_q_up"],
                stats["observed_up_frequency"],
                stats["calibration_error"]
            ));
        }
    }
    text
}

fn sample_size_markdown(report: &Value) -> String {
    let stats = &report["result"]["statistics"];
    format!(
        "# Sample Size\n\n- N: {}\n- Mean: {}\n- Std: {}\n- 95% CI: [{}, {}]\n- Required N for +/- $0.05: {}\n- Required N for +/- $0.10: {}\n- Required N to detect observed mean: {}\n- Profitability claim allowed: {}\n",
        stats["n"],
        stats["mean"],
        stats["std"],
        stats["ci_low"],
        stats["ci_high"],
        stats["required_n_for_plus_minus_0_05"],
        stats["required_n_for_plus_minus_0_10"],
        stats["required_n_to_detect_observed_mean"],
        stats["profitability_claim_allowed"]
    )
}

fn final_report_markdown(report: &Value) -> String {
    let result = &report["result"];
    format!(
        "# Final Strategy Research Report\n\n## Executive Summary\n\nRecommendation: **{}**\n\nAdaptive profiles are research-only, disabled by default, and not allowed for live deployment.\n\n## Risks and Measurement Weaknesses\n\n{}\n\n## Next 10 Actions\n\n{}\n",
        result["executive_summary"]["recommendation"]
            .as_str()
            .unwrap_or("Continue collecting data unchanged"),
        markdown_list(&result["risks_and_measurement_weaknesses"]),
        markdown_list(&result["next_10_actions"])
    )
}

fn ml_calibrate_markdown(report: &Value) -> String {
    format!(
        "# ML Calibration\n\nStatus: `{}`\n\nReason: {}\n",
        report["result"]["status"].as_str().unwrap_or("skipped"),
        report["result"]["reason"].as_str().unwrap_or("")
    )
}

fn markdown_list(value: &Value) -> String {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    let text = item
                        .as_str()
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| item.to_string());
                    format!("\n- {text}")
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_else(|| "\n- none".to_owned())
}

fn collect_child_warnings(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|row| {
            row.get("warnings")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        })
        .collect()
}

fn parse_datetime(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let text = value?.as_str()?;
    parse_rfc3339_utc(text)
}

fn parse_rfc3339_utc(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn decimal(value: Option<&Value>) -> Option<Decimal> {
    match value? {
        Value::String(text) => Decimal::from_str_exact(text).ok(),
        Value::Number(number) => Decimal::from_str_exact(&number.to_string()).ok(),
        _ => None,
    }
}

fn decimal_from_str(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap_or(Decimal::ZERO)
}

fn d(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap_or(Decimal::ZERO)
}

fn text(payload: &Value, key: &str) -> String {
    payload
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn optional_text(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn has_any_key(value: &Value, keys: &[&str]) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, child)| {
            (keys.contains(&key.as_str()) && value_has_data(child)) || has_any_key(child, keys)
        }),
        Value::Array(values) => values.iter().any(|child| has_any_key(child, keys)),
        _ => false,
    }
}

fn value_has_data(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(text) => !text.is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(map) => !map.is_empty(),
        Value::Bool(_) | Value::Number(_) => true,
    }
}

fn bool_value(payload: &Value, key: &str) -> bool {
    payload.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn ts(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn now_ts() -> String {
    ts(Utc::now())
}

fn min_ts(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn max_ts(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn day_key(ts: DateTime<Utc>) -> String {
    format!("{:04}-{:02}-{:02}", ts.year(), ts.month(), ts.day())
}

fn hour_key(ts: DateTime<Utc>) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}",
        ts.year(),
        ts.month(),
        ts.day(),
        ts.hour()
    )
}

fn ratio_usize(numerator: usize, denominator: usize) -> Value {
    if denominator == 0 {
        Value::Null
    } else {
        json!(numerator as f64 / denominator as f64)
    }
}

fn decimal_ratio(numerator: Decimal, denominator: Decimal) -> Value {
    if denominator == Decimal::ZERO {
        Value::Null
    } else {
        json!((numerator / denominator).to_string())
    }
}

fn decimal_map_json(map: &BTreeMap<String, Decimal>) -> Value {
    Value::Object(
        map.iter()
            .map(|(key, value)| (key.clone(), json!(value.to_string())))
            .collect(),
    )
}

fn mean_decimal(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().copied().sum::<Decimal>() / Decimal::from(values.len()))
    }
}

fn median_decimal(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let mid = sorted.len() / 2;
    if is_multiple_of(sorted.len(), 2) {
        Some((sorted[mid - 1] + sorted[mid]) / Decimal::from(2))
    } else {
        sorted.get(mid).copied()
    }
}

#[allow(unknown_lints, clippy::manual_is_multiple_of)]
fn is_multiple_of(value: usize, divisor: usize) -> bool {
    divisor != 0 && value % divisor == 0
}

fn std_decimal(values: &[Decimal], mean: Option<Decimal>) -> Option<Decimal> {
    let mean = mean?;
    if values.len() < 2 {
        return None;
    }
    let variance = values
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<Decimal>()
        / Decimal::from(values.len() - 1);
    Decimal::from_f64_retain(variance.to_f64()?.sqrt())
}

fn sample_std_f64(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (values.len() - 1) as f64;
    Some(variance.sqrt())
}

fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = 14_695_981_039_346_656_037_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_azure_input_without_credentials_in_uri() {
        let source = AzureEventSource::parse(
            "azure://acct/container/events/2026/06/12/?sas_env=POLYEDGE_SAS&max_blobs=7&max_bytes=12345&prefetch_blobs=8",
        )
        .unwrap()
        .unwrap();

        assert_eq!(source.account, "acct");
        assert_eq!(source.container, "container");
        assert_eq!(source.prefix, "events/2026/06/12/");
        assert_eq!(source.sas_env, "POLYEDGE_SAS");
        assert_eq!(source.max_blobs, Some(7));
        assert_eq!(source.max_bytes, Some(12345));
        assert_eq!(source.prefetch_blobs, 8);
    }

    #[test]
    fn rejects_incomplete_azure_input() {
        assert!(AzureEventSource::parse("azure://acct/container")
            .unwrap_err()
            .to_string()
            .contains("azure://<account>/<container>/<prefix>"));
    }

    #[test]
    fn local_input_is_not_azure() {
        assert_eq!(AzureEventSource::parse("data/events.jsonl").unwrap(), None);
    }

    #[test]
    fn azure_prefetch_is_clamped_to_bounded_window() {
        let source = AzureEventSource::parse("azure://acct/container/events/?prefetch_blobs=1000")
            .unwrap()
            .unwrap();

        assert_eq!(source.prefetch_blobs, MAX_AZURE_PREFETCH_BLOBS);
        assert_eq!(source.worker_count(3), 3);
    }

    #[test]
    fn normalized_filters_skip_book_shards_for_market_truth_and_calibration() {
        let paths = [
            "books.jsonl.gz",
            "markets.jsonl.gz",
            "references.jsonl.gz",
            "fair_values.jsonl.gz",
            "decisions.jsonl.gz",
            "execution_reports.jsonl.gz",
            "feed_errors.jsonl.gz",
            "paper_settlements.jsonl.gz",
            "other.jsonl.gz",
        ]
        .into_iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();

        let market_truth = filtered_normalized_event_paths(&paths, EventPathMode::MarketTruth)
            .into_iter()
            .map(|path| path.display().to_string())
            .collect::<BTreeSet<_>>();
        assert!(!market_truth.contains("books.jsonl.gz"));
        assert!(!market_truth.contains("paper_settlements.jsonl.gz"));
        assert!(market_truth.contains("other.jsonl.gz"));
        assert!(market_truth.contains("decisions.jsonl.gz"));

        let calibration = filtered_normalized_event_paths(&paths, EventPathMode::Calibration)
            .into_iter()
            .map(|path| path.display().to_string())
            .collect::<BTreeSet<_>>();
        assert!(!calibration.contains("books.jsonl.gz"));
        assert!(!calibration.contains("decisions.jsonl.gz"));
        assert!(calibration.contains("fair_values.jsonl.gz"));
        assert!(calibration.contains("other.jsonl.gz"));
    }

    #[test]
    fn stream_warning_storage_is_bounded() {
        let mut stats = StreamStats::default();
        for index in 0..(MAX_STREAM_WARNINGS + 10) {
            push_stream_warning(&mut stats, format!("warning {index}"));
        }

        assert_eq!(stats.warnings.len(), MAX_STREAM_WARNINGS + 1);
        assert!(stats
            .warnings
            .last()
            .is_some_and(|warning| warning.contains("additional stream warnings suppressed")));
    }
}
