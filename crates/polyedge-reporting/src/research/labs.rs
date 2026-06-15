use super::*;
use chrono::NaiveDate;

mod config;
pub use config::{
    load_default_exclusions, load_exclusion_registry, load_frozen_candidate_registry,
    ExclusionRegistry, ExclusionWindowRecord, FrozenCandidateRecord, FrozenCandidateRegistry,
    DEFAULT_EXCLUSION_FILE, DEFAULT_FROZEN_CANDIDATES_FILE, DEFAULT_PROSPECTIVE_SINCE,
    FROZEN_CANDIDATE_NAMES,
};

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
}

#[derive(Clone, Debug)]
pub struct ReplayIndexOptions {
    pub input: PathBuf,
    pub out: PathBuf,
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
    let rows = load_daily_prospective_rows(&options.reports_dir, options.since)?;
    let status = if rows.is_empty() {
        "collecting"
    } else {
        "tracking"
    };
    let result = json!({
        "status": status,
        "since": ts(options.since),
        "rows": rows,
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
        rows.push(daily_prospective_row(&date, &entry.path())?);
    }
    rows.sort_by(|left, right| left["date"].as_str().cmp(&right["date"].as_str()));
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
    let source =
        merge_optional_reports([final_report.as_ref(), regimes.as_ref(), baseline.as_ref()]);
    let fill_model = text_at(&source, &["/result/fill_model"]).unwrap_or("touch_after_250ms");
    let sample = sample_size.as_ref().unwrap_or(&source);
    json_row(date, fill_model, &source, sample, audit.as_ref())
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

fn json_row(
    date: &str,
    fill_model: &str,
    source: &Value,
    sample: &Value,
    audit: Option<&Value>,
) -> Result<Value, ResearchError> {
    let static_net = find_profile_net(source, "static")
        .or_else(|| find_profile_net(source, "static_baseline"))
        .or_else(|| find_fill_model_net(source, fill_model));
    let dynamic_net = find_profile_net(source, "dynamic_quote_style");
    let full_net = find_profile_net(source, "full_deterministic_profile");
    let safety_net = find_profile_net(source, "dynamic_safety_only");
    let ci_low = text_at(sample, &["/result/statistics/ci_low", "/statistics/ci_low"]);
    let ci_high = text_at(
        sample,
        &["/result/statistics/ci_high", "/statistics/ci_high"],
    );
    let settled_markets = number_at(
        source,
        &[
            "/result.market_truth_table/result/summary/complete_for_simulation",
            "/result.summary/complete_for_simulation",
            "/summary/complete_for_simulation",
            "/result/summary/complete_for_simulation",
            "/result/statistics/sample_size",
        ],
    );
    Ok(json!({
        "date": date,
        "settled_markets": settled_markets,
        "fill_model": fill_model,
        "static_net_pnl": static_net,
        "dynamic_quote_style_net_pnl": dynamic_net,
        "full_deterministic_profile_net_pnl": full_net,
        "dynamic_safety_only_net_pnl": safety_net,
        "max_drawdown": find_any_text(source, "max_drawdown"),
        "cancel_per_fill": find_any_text(source, "cancel_fill_ratio"),
        "ci_95_low": ci_low,
        "ci_95_high": ci_high,
        "data_quality_status": data_quality_status(audit),
        "recommendation": prospective_recommendation(ci_low, ci_high, dynamic_net),
        "research_only": true,
        "live_deployment_allowed": false
    }))
}

fn data_quality_status(audit: Option<&Value>) -> &'static str {
    let Some(audit) = audit else {
        return "unknown";
    };
    if audit
        .pointer("/result/warnings")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        "healthy"
    } else {
        "warning"
    }
}

fn prospective_recommendation(
    ci_low: Option<&str>,
    ci_high: Option<&str>,
    dynamic_net: Option<String>,
) -> &'static str {
    let lower = ci_low.map(decimal_from_str);
    let upper = ci_high.map(decimal_from_str);
    let dynamic = dynamic_net.as_deref().map(decimal_from_str);
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

fn find_profile_net(value: &Value, profile: &str) -> Option<String> {
    match value {
        Value::Object(map) => {
            if map.get("profile").and_then(Value::as_str) == Some(profile) {
                return map
                    .get("net_pnl")
                    .and_then(value_to_string)
                    .or_else(|| map.get("delta_vs_static").and_then(value_to_string));
            }
            map.values()
                .find_map(|child| find_profile_net(child, profile))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|child| find_profile_net(child, profile)),
        _ => None,
    }
}

fn find_fill_model_net(value: &Value, fill_model: &str) -> Option<String> {
    match value {
        Value::Object(map) => {
            if map.get("fill_model").and_then(Value::as_str) == Some(fill_model) {
                return map.get("net_pnl").and_then(value_to_string);
            }
            map.values()
                .find_map(|child| find_fill_model_net(child, fill_model))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|child| find_fill_model_net(child, fill_model)),
        _ => None,
    }
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
