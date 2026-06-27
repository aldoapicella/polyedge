use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{SecondsFormat, Utc};
use polyedge_storage::{AzureBlobClient, AzureBlobError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

mod catalog;

use crate::history::{max_historical_markets, merge_market_lists, MarketHistoryStore};
use crate::labs;
use crate::ApiState;
use catalog::{datasets, templates};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1_000;
const REPORT_ROOT: &str = "reports/research";
const FRESHNESS_LATEST: &str = "data_quality/freshness/latest.json";
const DEFAULT_QUERY_TEMPLATE_DIR: &str = "reports/query_templates";
const DEFAULT_QUERY_AUDIT_DIR: &str = "reports/query_audit";
const DEFAULT_QUERY_DATA_ROOT: &str = "data/research/normalized";

pub(crate) fn router() -> Router<ApiState> {
    Router::new()
        .route("/schema", get(schema))
        .route("/run", post(run_query))
        .route("/templates", get(query_templates).post(save_query_template))
}

trait QueryBackend {
    fn schema(&self) -> Value;
    async fn run(&self, request: QueryRequest) -> Result<QueryResult, QueryError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum QueryBackendKind {
    ReportBacked,
    NormalizedJsonl,
    AzureDataExplorer,
}

impl QueryBackendKind {
    fn configured() -> Self {
        match env::var("POLYEDGE_QUERY_BACKEND")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "normalized_jsonl" | "jsonl" | "historical_jsonl" => Self::NormalizedJsonl,
            "adx" | "azure_data_explorer" => Self::AzureDataExplorer,
            _ => Self::ReportBacked,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ReportBacked => "report_backed",
            Self::NormalizedJsonl => "normalized_jsonl",
            Self::AzureDataExplorer => "azure_data_explorer",
        }
    }
}

struct ReportBackedQueryBackend {
    state: ApiState,
    kind: QueryBackendKind,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct QueryRequest {
    dataset: String,
    #[serde(default)]
    filters: Vec<QueryFilter>,
    #[serde(default)]
    group_by: Vec<String>,
    #[serde(default)]
    metrics: Vec<String>,
    #[serde(default)]
    sort: Vec<QuerySort>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct QueryFilter {
    field: String,
    #[serde(default = "default_filter_op")]
    op: String,
    value: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct QuerySort {
    field: String,
    #[serde(default = "default_sort_direction")]
    direction: String,
}

#[derive(Debug, Serialize)]
struct QueryResult {
    dataset: String,
    columns: Vec<QueryColumn>,
    rows: Vec<Value>,
    total_rows: usize,
    returned_rows: usize,
    offset: usize,
    limit: usize,
    truncated: bool,
    warnings: Vec<String>,
    source: Value,
}

#[derive(Debug, Serialize)]
struct QueryColumn {
    field: String,
    label: String,
    kind: String,
    help: String,
}

#[derive(Debug, Deserialize)]
struct SaveQueryTemplateRequest {
    id: Option<String>,
    name: String,
    description: Option<String>,
    request: QueryRequest,
    owner: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueryTemplateRecord {
    id: String,
    name: String,
    description: String,
    request: QueryRequest,
    created_ts: String,
    updated_ts: String,
    owner: String,
    tags: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QueryAuditEntry {
    query_id: String,
    ts: String,
    dataset: String,
    filters: Value,
    group_by: Vec<String>,
    metrics: Vec<String>,
    limit: usize,
    duration_ms: u128,
    returned_rows: Option<usize>,
    source: String,
    error: Option<String>,
}

#[derive(Debug)]
struct QueryError {
    status: StatusCode,
    detail: String,
}

impl QueryError {
    fn bad_request(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            detail: detail.into(),
        }
    }
}

async fn schema(State(state): State<ApiState>) -> Json<Value> {
    Json(ReportBackedQueryBackend::new(state).schema())
}

async fn query_templates() -> Json<Value> {
    let mut all_templates = templates();
    all_templates.extend(
        saved_query_templates()
            .into_iter()
            .filter_map(|template| serde_json::to_value(template).ok()),
    );
    Json(json!({
        "templates": all_templates,
        "persisted": true,
        "template_store": query_template_dir().to_string_lossy()
    }))
}

async fn save_query_template(Json(request): Json<SaveQueryTemplateRequest>) -> impl IntoResponse {
    match persist_query_template(request) {
        Ok(template) => (
            StatusCode::CREATED,
            Json(json!({
                "persisted": true,
                "template": template,
                "template_store": query_template_dir().to_string_lossy(),
                "detail": "Query template persisted server-side as structured JSON."
            })),
        )
            .into_response(),
        Err(error) => (
            error.status,
            Json(json!({
                "persisted": false,
                "detail": error.detail
            })),
        )
            .into_response(),
    }
}

async fn run_query(
    State(state): State<ApiState>,
    Json(request): Json<QueryRequest>,
) -> impl IntoResponse {
    let backend = ReportBackedQueryBackend::new(state);
    let audit_request = request.clone();
    let started = Instant::now();
    match backend.run(request).await {
        Ok(result) => {
            append_query_audit(QueryAuditEntry::from_success(
                &audit_request,
                &result,
                backend.kind,
                started.elapsed().as_millis(),
            ));
            (StatusCode::OK, Json(json!(result))).into_response()
        }
        Err(error) => {
            append_query_audit(QueryAuditEntry::from_error(
                &audit_request,
                backend.kind,
                started.elapsed().as_millis(),
                &error.detail,
            ));
            (error.status, Json(json!({ "detail": error.detail }))).into_response()
        }
    }
}

impl QueryBackend for ReportBackedQueryBackend {
    fn schema(&self) -> Value {
        json!({
            "backend": "ReportBackedQueryBackend",
            "backend_kind": self.kind.as_str(),
            "available_backends": [
                QueryBackendKind::ReportBacked.as_str(),
                QueryBackendKind::NormalizedJsonl.as_str(),
                QueryBackendKind::AzureDataExplorer.as_str()
            ],
            "structured_only": true,
            "generated_ts": now_ts(),
            "datasets": datasets(),
            "operators": ["eq", "ne", "contains", "gt", "gte", "lt", "lte", "in"],
            "output_modes": ["table", "bar", "line", "scatter", "csv", "json"],
            "safety": {
                "live_trading_enabled": false,
                "arbitrary_sql_or_kql": false,
                "secrets_exposed": false,
                "query_audit_enabled": true,
                "saved_templates_persisted": true
            }
        })
    }

    async fn run(&self, request: QueryRequest) -> Result<QueryResult, QueryError> {
        let dataset = canonical_dataset(&request.dataset)?;
        let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
        let offset = request.offset.unwrap_or(0);
        let mut warnings = Vec::new();
        let mut rows = self.load_dataset(dataset, &mut warnings).await?;
        rows.extend(self.normalized_jsonl_rows(dataset, &mut warnings));
        rows.retain(|row| {
            request
                .filters
                .iter()
                .all(|filter| filter_matches(row, filter))
        });

        if !request.group_by.is_empty() {
            rows = grouped_rows(&rows, &request.group_by, &request.metrics);
        }

        sort_rows(&mut rows, &request.sort);
        let total_rows = rows.len();
        let rows = rows
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        let returned_rows = rows.len();
        let columns = columns_for_rows(dataset, &rows, &request.group_by, &request.metrics);
        Ok(QueryResult {
            dataset: dataset.to_owned(),
            columns,
            rows,
            total_rows,
            returned_rows,
            offset,
            limit,
            truncated: offset + returned_rows < total_rows,
            warnings,
            source: json!({
                "backend": "ReportBackedQueryBackend",
                "backend_kind": self.kind.as_str(),
                "runtime": "rust_runtime_memory",
                "reports_root": REPORT_ROOT,
                "normalized_jsonl_root": normalized_query_data_root().to_string_lossy(),
                "read_only": true,
                "live_trading_enabled": false
            }),
        })
    }
}

impl ReportBackedQueryBackend {
    fn new(state: ApiState) -> Self {
        Self {
            state,
            kind: QueryBackendKind::configured(),
        }
    }

    async fn load_dataset(
        &self,
        dataset: &str,
        warnings: &mut Vec<String>,
    ) -> Result<Vec<Value>, QueryError> {
        match dataset {
            "markets" => self.market_rows(warnings).await,
            "decisions" => Ok(tag_rows(
                "runtime",
                self.state
                    .runtime
                    .decisions()
                    .await
                    .into_iter()
                    .filter_map(|row| serde_json::to_value(row).ok())
                    .collect(),
            )),
            "fills" => Ok(tag_rows(
                "runtime",
                self.state
                    .runtime
                    .fills()
                    .await
                    .into_iter()
                    .filter_map(|row| serde_json::to_value(row).ok())
                    .collect(),
            )),
            "jobs" => Ok(job_rows_from_labs()),
            "artifacts" => Ok(artifact_rows()),
            "data_quality" => Ok(data_quality_rows(&self.state).await),
            "reports" => Ok(report_rows(&[
                "final_report.json",
                "latest_daily_report.json",
            ])),
            "regimes" => Ok(report_rows(&["regimes.json", "regime_profiles.json"])),
            "calibration" => Ok(report_rows(&["calibration.json"])),
            "fill_models" => Ok(report_rows(&[
                "baseline.json",
                "baseline_static_all_fill_models.json",
            ])),
            "sample_size" => Ok(report_rows(&["sample_size.json"])),
            "market_truth" => Ok(report_rows(&["markets_summary.json"])),
            "decision_features" => Ok(Vec::new()),
            "fill_candidates" => Ok(Vec::new()),
            "queue_evidence" => Ok(Vec::new()),
            "queue_proxy_results" => Ok(report_rows(&[
                "baseline.json",
                "baseline_static_all_fill_models.json",
                "regime_profiles.json",
            ])),
            "prospective_daily" => Ok(report_rows(&["prospective_validation.json"])),
            "candidate_market_pnl" => Ok(report_rows(&[
                "prospective_validation.json",
                "regimes.json",
                "regime_profiles.json",
            ])),
            "regime_market_pnl" => Ok(report_rows(&["regimes.json", "regime_profiles.json"])),
            "calibration_buckets" => Ok(report_rows(&["calibration.json"])),
            _ => Err(QueryError::bad_request(format!(
                "Unsupported dataset {dataset}."
            ))),
        }
    }

    fn normalized_jsonl_rows(&self, dataset: &str, warnings: &mut Vec<String>) -> Vec<Value> {
        let files = match dataset {
            "markets" | "market_truth" => &["markets.jsonl"][..],
            "decisions" | "decision_features" => &["decisions.jsonl"][..],
            "fills" | "fill_candidates" => &["execution_reports.jsonl"][..],
            "queue_evidence" => &[
                "raw_market_events.jsonl",
                "price_changes.jsonl",
                "last_trades.jsonl",
                "book_snapshots.jsonl",
                "level_changes.jsonl",
            ],
            _ => return Vec::new(),
        };
        let root = normalized_query_data_root();
        let mut rows = Vec::new();
        for file in files {
            let path = root.join(file);
            if !path.exists() {
                warnings.push(format!(
                    "normalized_jsonl_missing:{}",
                    path.to_string_lossy()
                ));
                continue;
            }
            let before = rows.len();
            rows.extend(read_jsonl_rows(&path, MAX_LIMIT.saturating_sub(rows.len())));
            for row in rows.iter_mut().skip(before) {
                if let Some(object) = row.as_object_mut() {
                    object.insert("source".to_owned(), json!("normalized_jsonl"));
                    object.insert("path".to_owned(), json!(path.to_string_lossy()));
                    if dataset == "fills" || dataset == "fill_candidates" {
                        object
                            .entry("fill_model".to_owned())
                            .or_insert_with(|| json!("paper_runtime"));
                    }
                }
            }
            if rows.len() >= MAX_LIMIT {
                warnings.push("normalized_jsonl_truncated_at_max_limit".to_owned());
                break;
            }
        }
        rows
    }

    async fn market_rows(&self, warnings: &mut Vec<String>) -> Result<Vec<Value>, QueryError> {
        let store = MarketHistoryStore::new(&self.state.settings);
        let live_markets = self.state.runtime.markets().await;
        let live_count = live_markets.len();
        let historical_markets = match store.markets(max_historical_markets()).await {
            Ok(markets) => markets,
            Err(error) => {
                warnings.push(format!("historical_market_read_failed: {error}"));
                Vec::new()
            }
        };
        let historical_count = historical_markets.len();
        let rows = merge_market_lists(live_markets, historical_markets, MAX_LIMIT)
            .into_iter()
            .map(|mut row| {
                if let Some(object) = row.as_object_mut() {
                    object.insert("source".to_owned(), json!("runtime+catalog"));
                    object.insert("live_market_count".to_owned(), json!(live_count));
                    object.insert(
                        "historical_market_count".to_owned(),
                        json!(historical_count),
                    );
                }
                row
            })
            .collect();
        Ok(rows)
    }
}

fn canonical_dataset(dataset: &str) -> Result<&'static str, QueryError> {
    match dataset.trim() {
        "markets" | "market" => Ok("markets"),
        "decisions" | "decision" => Ok("decisions"),
        "fills" | "execution" | "executions" => Ok("fills"),
        "reports" | "report" => Ok("reports"),
        "data_quality" | "quality" => Ok("data_quality"),
        "jobs" | "job" => Ok("jobs"),
        "artifacts" | "artifact" => Ok("artifacts"),
        "calibration" => Ok("calibration"),
        "fill_models" | "fill-models" => Ok("fill_models"),
        "sample_size" | "sample-size" => Ok("sample_size"),
        "regimes" | "regime" => Ok("regimes"),
        "market_truth" | "market-truth" => Ok("market_truth"),
        "decision_features" | "decision-features" => Ok("decision_features"),
        "fill_candidates" | "fill-candidates" => Ok("fill_candidates"),
        "queue_evidence" | "queue-evidence" => Ok("queue_evidence"),
        "queue_proxy_results" | "queue-proxy-results" => Ok("queue_proxy_results"),
        "prospective_daily" | "prospective-daily" => Ok("prospective_daily"),
        "candidate_market_pnl" | "candidate-market-pnl" => Ok("candidate_market_pnl"),
        "regime_market_pnl" | "regime-market-pnl" => Ok("regime_market_pnl"),
        "calibration_buckets" | "calibration-buckets" => Ok("calibration_buckets"),
        other => Err(QueryError::bad_request(format!(
            "Unknown dataset {other}. Use /api/v1/query/schema for available datasets."
        ))),
    }
}

fn tag_rows(source: &str, rows: Vec<Value>) -> Vec<Value> {
    rows.into_iter()
        .map(|mut row| {
            if let Some(object) = row.as_object_mut() {
                object.insert("source".to_owned(), json!(source));
            }
            row
        })
        .collect()
}

fn job_rows_from_labs() -> Vec<Value> {
    labs::job_definitions()
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn data_quality_rows(state: &ApiState) -> Vec<Value> {
    let mut rows = Vec::new();
    let freshness = read_json_or_null(FRESHNESS_LATEST);
    if !freshness.is_null() {
        rows.push(with_dataset_fields(
            "freshness",
            FRESHNESS_LATEST,
            freshness,
        ));
    } else {
        rows.push(json!({
            "kind": "freshness",
            "path": FRESHNESS_LATEST,
            "status": "missing",
            "quality_flag": "unknown",
            "action": "Run the freshness check job or select another date."
        }));
    }
    let status = state.runtime.status().await;
    rows.push(json!({
        "kind": "recorder",
        "path": "runtime/status/recorder",
        "status": recorder_status(&status["recorder"]),
        "quality_flag": recorder_status(&status["recorder"]),
        "recorder": status["recorder"].clone()
    }));
    for window in exclusion_rows() {
        rows.push(window);
    }
    rows
}

fn recorder_status(recorder: &Value) -> &'static str {
    if recorder.is_null() {
        return "unknown";
    }
    let failed = number_at(recorder, "failed_total").unwrap_or(0.0)
        + number_at(recorder, "error_count").unwrap_or(0.0)
        + number_at(recorder, "dropped_count").unwrap_or(0.0);
    if failed > 0.0 || field_value(recorder, "worker_alive").and_then(Value::as_bool) == Some(false)
    {
        "warning"
    } else {
        "healthy"
    }
}

fn exclusion_rows() -> Vec<Value> {
    let mut rows = Vec::new();
    let Ok(text) = fs::read_to_string("data_quality/exclusion_windows.yaml") else {
        return rows;
    };
    for block in text.split("\n- ").skip(1) {
        let mut row = serde_json::Map::new();
        row.insert("kind".to_owned(), json!("exclusion_window"));
        row.insert(
            "path".to_owned(),
            json!("data_quality/exclusion_windows.yaml"),
        );
        for line in block.lines() {
            let Some((key, value)) = line.trim().split_once(':') else {
                continue;
            };
            let value = value.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                row.insert(key.trim().to_owned(), parse_scalar(value));
            }
        }
        rows.push(Value::Object(row));
    }
    rows
}

fn report_rows(file_names: &[&str]) -> Vec<Value> {
    if let Some(rows) = blob_report_rows(file_names) {
        return rows;
    }
    let mut rows = Vec::new();
    collect_report_rows(Path::new(REPORT_ROOT), file_names, &mut rows, MAX_LIMIT);
    rows
}

fn blob_report_rows(file_names: &[&str]) -> Option<Vec<Value>> {
    let mut client = artifact_blob_client()?;
    let blobs = client
        .list_blobs_by_suffixes(
            &format!("{REPORT_ROOT}/"),
            file_names,
            Some(MAX_LIMIT),
            Some(64 * 1024 * 1024),
        )
        .ok()?;
    let mut rows = Vec::new();
    for blob in blobs {
        if rows.len() >= MAX_LIMIT {
            break;
        }
        let Ok(bytes) = client.download_blob_bytes(&blob.name) else {
            continue;
        };
        let Ok(payload) = serde_json::from_slice::<Value>(&bytes) else {
            continue;
        };
        if payload.is_null() {
            continue;
        }
        let file_name = Path::new(&blob.name)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("report.json");
        let relative = blob
            .name
            .strip_prefix(&format!("{REPORT_ROOT}/"))
            .unwrap_or(&blob.name)
            .to_owned();
        let mut extracted = extract_analytical_rows(&payload);
        if extracted.is_empty() {
            extracted.push(payload);
        }
        for row in extracted.into_iter().take(100) {
            rows.push(with_dataset_fields(file_name, &relative, row));
            if rows.len() >= MAX_LIMIT {
                return Some(rows);
            }
        }
    }
    Some(rows)
}

fn collect_report_rows(root: &Path, file_names: &[&str], rows: &mut Vec<Value>, limit: usize) {
    if rows.len() >= limit || !root.exists() {
        return;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if rows.len() >= limit {
            return;
        }
        let path = entry.path();
        if path.is_dir() {
            collect_report_rows(&path, file_names, rows, limit);
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !file_names.contains(&file_name) {
            continue;
        }
        let payload = read_json_or_null(&path);
        if payload.is_null() {
            continue;
        }
        let relative = relative_report_path(&path);
        let mut extracted = extract_analytical_rows(&payload);
        if extracted.is_empty() {
            extracted.push(payload);
        }
        for row in extracted.into_iter().take(100) {
            rows.push(with_dataset_fields(file_name, &relative, row));
            if rows.len() >= limit {
                return;
            }
        }
    }
}

fn extract_analytical_rows(payload: &Value) -> Vec<Value> {
    let mut rows = Vec::new();
    visit_records(payload, &mut |record| {
        let keys = [
            "candidate",
            "profile",
            "regime",
            "q_bucket",
            "fill_model",
            "net_pnl",
            "max_drawdown",
            "mean",
            "required_n_to_detect_observed_mean",
            "observed_up_frequency",
            "calibration_error",
        ];
        if keys.iter().any(|key| record.get(*key).is_some()) {
            rows.push(Value::Object(record.clone()));
        }
    });
    rows
}

fn visit_records(value: &Value, visit: &mut impl FnMut(&serde_json::Map<String, Value>)) {
    match value {
        Value::Array(items) => {
            for item in items {
                visit_records(item, visit);
            }
        }
        Value::Object(record) => {
            visit(record);
            for child in record.values() {
                visit_records(child, visit);
            }
        }
        _ => {}
    }
}

fn artifact_rows() -> Vec<Value> {
    if let Some(rows) = blob_artifact_rows() {
        return rows;
    }
    let mut rows = Vec::new();
    collect_artifacts(Path::new(REPORT_ROOT), &mut rows);
    rows.sort_by_key(|row| string_at(row, "path"));
    rows.truncate(MAX_LIMIT);
    rows
}

fn blob_artifact_rows() -> Option<Vec<Value>> {
    let mut client = artifact_blob_client()?;
    let blobs = client
        .list_blobs_by_suffixes(
            &format!("{REPORT_ROOT}/"),
            &[".json", ".md", ".csv", ".parquet"],
            Some(MAX_LIMIT),
            None,
        )
        .ok()?;
    let mut rows = blobs
        .into_iter()
        .filter_map(|blob| {
            let relative = blob.name.strip_prefix(&format!("{REPORT_ROOT}/"))?.to_owned();
            let extension = Path::new(&relative)
                .extension()
                .and_then(|value| value.to_str())?;
            Some(json!({
                "artifact_id": relative.replace('/', "~"),
                "path": relative,
                "kind": extension,
                "date": date_from_path(&relative),
                "size_bytes": blob.content_length,
                "modified_ts": blob.last_modified.map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)),
                "quality": quality_from_path(&relative),
                "source_job": source_job_from_path(&relative)
            }))
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| string_at(row, "path"));
    Some(rows)
}

fn collect_artifacts(root: &Path, rows: &mut Vec<Value>) {
    if rows.len() >= MAX_LIMIT || !root.exists() {
        return;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if rows.len() >= MAX_LIMIT {
            return;
        }
        let path = entry.path();
        if path.is_dir() {
            collect_artifacts(&path, rows);
            continue;
        }
        let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
            continue;
        };
        if !matches!(extension, "json" | "md" | "csv" | "parquet") {
            continue;
        }
        let relative = relative_report_path(&path);
        let metadata = fs::metadata(&path).ok();
        let modified_ts = metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok())
            .map(chrono::DateTime::<Utc>::from)
            .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Secs, true));
        rows.push(json!({
            "artifact_id": relative.replace('/', "~"),
            "path": relative,
            "kind": extension,
            "date": date_from_path(&relative),
            "size_bytes": metadata.map(|metadata| metadata.len()),
            "modified_ts": modified_ts,
            "quality": quality_from_path(&relative),
            "source_job": source_job_from_path(&relative)
        }));
    }
}

fn with_dataset_fields(kind: &str, path: &str, mut row: Value) -> Value {
    if let Some(object) = row.as_object_mut() {
        object.insert("kind".to_owned(), json!(kind));
        object.insert("path".to_owned(), json!(path));
        if !object.contains_key("date") {
            if let Some(date) = date_from_path(path) {
                object.insert("date".to_owned(), json!(date));
            }
        }
        if !object.contains_key("quality") {
            object.insert("quality".to_owned(), json!(quality_from_path(path)));
        }
    }
    row
}

fn grouped_rows(rows: &[Value], group_by: &[String], metrics: &[String]) -> Vec<Value> {
    let metric_names = if metrics.is_empty() {
        vec!["count".to_owned()]
    } else {
        metrics.to_vec()
    };
    let mut groups: BTreeMap<Vec<String>, Vec<&Value>> = BTreeMap::new();
    for row in rows {
        let key = group_by
            .iter()
            .map(|field| {
                field_value(row, field)
                    .map(compact_json)
                    .unwrap_or_else(|| "unknown".to_owned())
            })
            .collect::<Vec<_>>();
        groups.entry(key).or_default().push(row);
    }
    groups
        .into_iter()
        .map(|(key, group_rows)| {
            let mut output = serde_json::Map::new();
            for (index, field) in group_by.iter().enumerate() {
                output.insert(
                    field.to_owned(),
                    json!(key.get(index).cloned().unwrap_or_default()),
                );
            }
            for metric in &metric_names {
                output.insert(metric.to_owned(), metric_value(metric, &group_rows));
            }
            Value::Object(output)
        })
        .collect()
}

fn metric_value(metric: &str, rows: &[&Value]) -> Value {
    match metric {
        "count" => json!(rows.len()),
        "net_pnl" => json!(sum_fields(
            rows,
            &[
                "net_pnl",
                "actual_paper_net_pnl",
                "dynamic_quote_style_net_pnl"
            ]
        )),
        "avg_pnl" => avg_fields(rows, &["net_pnl", "pnl", "market_pnl"]),
        "fill_count" => json!(sum_fields(rows, &["fill_count", "fills", "filled_size"])),
        "cancel_count" => json!(rows
            .iter()
            .filter(|row| row_contains(row, "cancel"))
            .count()),
        "fill_rate" => ratio(
            sum_fields(rows, &["fill_count", "fills", "filled_size"]),
            rows.len() as f64,
        ),
        "cancel_per_fill" => ratio(
            rows.iter()
                .filter(|row| row_contains(row, "cancel"))
                .count() as f64,
            sum_fields(rows, &["fill_count", "fills", "filled_size"]),
        ),
        "mean_q_up" => avg_fields(rows, &["q_up", "qUp", "avg_q_up", "mean_q_up"]),
        "observed_up_rate" => avg_fields(rows, &["observed_up_rate", "observed_up_frequency"]),
        "brier_score" => avg_fields(rows, &["brier_score"]),
        "max_drawdown" => min_fields(rows, &["max_drawdown"]),
        other => avg_fields(rows, &[other]),
    }
}

fn sum_fields(rows: &[&Value], fields: &[&str]) -> f64 {
    rows.iter()
        .filter_map(|row| fields.iter().find_map(|field| number_at(row, field)))
        .sum()
}

fn avg_fields(rows: &[&Value], fields: &[&str]) -> Value {
    let values = rows
        .iter()
        .filter_map(|row| fields.iter().find_map(|field| number_at(row, field)))
        .collect::<Vec<_>>();
    if values.is_empty() {
        Value::Null
    } else {
        json!(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn min_fields(rows: &[&Value], fields: &[&str]) -> Value {
    rows.iter()
        .filter_map(|row| fields.iter().find_map(|field| number_at(row, field)))
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal))
        .map(|value| json!(value))
        .unwrap_or(Value::Null)
}

fn ratio(numerator: f64, denominator: f64) -> Value {
    if denominator == 0.0 {
        Value::Null
    } else {
        json!(numerator / denominator)
    }
}

fn sort_rows(rows: &mut [Value], sort: &[QuerySort]) {
    for sort_key in sort.iter().rev() {
        let descending = sort_key.direction.eq_ignore_ascii_case("desc");
        rows.sort_by(|left, right| {
            let ordering = compare_values(
                field_value(left, &sort_key.field),
                field_value(right, &sort_key.field),
            );
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        });
    }
}

fn columns_for_rows(
    dataset: &str,
    rows: &[Value],
    group_by: &[String],
    metrics: &[String],
) -> Vec<QueryColumn> {
    let mut fields = BTreeSet::new();
    if !group_by.is_empty() {
        for field in group_by {
            fields.insert(field.to_owned());
        }
        for metric in metrics {
            fields.insert(metric.to_owned());
        }
    }
    if fields.is_empty() {
        for row in rows.iter().take(50) {
            if let Some(object) = row.as_object() {
                for key in object.keys().take(40) {
                    fields.insert(key.to_owned());
                }
            }
        }
    }
    fields
        .into_iter()
        .map(|field| QueryColumn {
            label: field.replace('_', " "),
            kind: inferred_kind(rows, &field),
            help: column_help(dataset, &field),
            field,
        })
        .collect()
}

fn inferred_kind(rows: &[Value], field: &str) -> String {
    let sample = rows.iter().find_map(|row| field_value(row, field));
    if sample.and_then(Value::as_f64).is_some() {
        "number".to_owned()
    } else if sample.and_then(Value::as_bool).is_some() {
        "boolean".to_owned()
    } else if field.contains("ts") || field.contains("date") || field.contains("time") {
        "datetime".to_owned()
    } else {
        "text".to_owned()
    }
}

fn column_help(dataset: &str, field: &str) -> String {
    match field {
        "count" => "Number of rows in the current group.".to_owned(),
        "net_pnl" => "Summed net paper/research PnL where present.".to_owned(),
        "fill_rate" => "Fill count divided by row count for the current group.".to_owned(),
        "brier_score" => "Average Brier score where calibration reports include it.".to_owned(),
        "quality" | "quality_flag" => {
            "Data quality label derived from report path or data-quality artifacts.".to_owned()
        }
        "live_trading_enabled" => {
            "Safety field; query datasets expose false for research jobs.".to_owned()
        }
        _ => format!("{field} from the curated {dataset} dataset."),
    }
}

fn filter_matches(row: &Value, filter: &QueryFilter) -> bool {
    let left = field_value(row, &filter.field);
    let right = if let Some(field_ref) = filter.value.get("field_ref").and_then(Value::as_str) {
        field_value(row, field_ref).cloned().unwrap_or(Value::Null)
    } else {
        filter.value.clone()
    };
    match filter.op.as_str() {
        "eq" => values_equal(left, Some(&right)),
        "ne" => !values_equal(left, Some(&right)),
        "contains" => left
            .map(compact_json)
            .unwrap_or_default()
            .to_lowercase()
            .contains(&compact_json(&right).to_lowercase()),
        "gt" => compare_values(left, Some(&right)) == Ordering::Greater,
        "gte" => matches!(
            compare_values(left, Some(&right)),
            Ordering::Greater | Ordering::Equal
        ),
        "lt" => compare_values(left, Some(&right)) == Ordering::Less,
        "lte" => matches!(
            compare_values(left, Some(&right)),
            Ordering::Less | Ordering::Equal
        ),
        "in" => right
            .as_array()
            .is_some_and(|items| items.iter().any(|item| values_equal(left, Some(item)))),
        _ => false,
    }
}

fn values_equal(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (left, right) {
        (None, Some(Value::Null)) | (Some(Value::Null), Some(Value::Null)) => true,
        (Some(left), Some(right)) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                (left - right).abs() < f64::EPSILON
            } else {
                compact_json(left).eq_ignore_ascii_case(&compact_json(right))
            }
        }
        _ => false,
    }
}

fn compare_values(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left.and_then(Value::as_f64), right.and_then(Value::as_f64)) {
        (Some(left), Some(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        _ => compact_option(left).cmp(&compact_option(right)),
    }
}

fn field_value<'a>(row: &'a Value, field: &str) -> Option<&'a Value> {
    if let Some(value) = row.get(field) {
        return Some(value);
    }
    field
        .split('.')
        .try_fold(row, |current, part| current.get(part))
}

fn number_at(row: &Value, field: &str) -> Option<f64> {
    field_value(row, field).and_then(|value| match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    })
}

fn string_at(row: &Value, field: &str) -> String {
    field_value(row, field)
        .map(compact_json)
        .unwrap_or_default()
}

fn compact_option(value: Option<&Value>) -> String {
    value.map(compact_json).unwrap_or_default()
}

fn compact_json(value: &Value) -> String {
    match value {
        Value::Null => "n/a".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .take(5)
            .map(compact_json)
            .collect::<Vec<_>>()
            .join(", "),
        Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn read_jsonl_rows(path: &Path, limit: usize) -> Vec<Value> {
    if limit == 0 {
        return Vec::new();
    }
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str::<Value>(&line).ok())
        .take(limit)
        .collect()
}

fn query_template_dir() -> PathBuf {
    env::var("POLYEDGE_QUERY_TEMPLATE_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_QUERY_TEMPLATE_DIR))
}

fn query_audit_dir() -> PathBuf {
    env::var("POLYEDGE_QUERY_AUDIT_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_QUERY_AUDIT_DIR))
}

fn normalized_query_data_root() -> PathBuf {
    env::var("POLYEDGE_QUERY_DATA_ROOT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_QUERY_DATA_ROOT))
}

fn saved_query_templates() -> Vec<QueryTemplateRecord> {
    let dir = query_template_dir();
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut records = entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                return None;
            }
            serde_json::from_value::<QueryTemplateRecord>(read_json_or_null(&path)).ok()
        })
        .collect::<Vec<_>>();
    records.sort_by(|left, right| left.name.cmp(&right.name));
    records
}

fn persist_query_template(
    request: SaveQueryTemplateRequest,
) -> Result<QueryTemplateRecord, QueryError> {
    if request.name.trim().is_empty() {
        return Err(QueryError::bad_request("Query template name is required."));
    }
    let dataset = canonical_dataset(&request.request.dataset)?;
    if contains_disallowed_secret_field(&request.request) {
        return Err(QueryError::bad_request(
            "Query templates cannot persist filters or sorts that look like secret fields.",
        ));
    }
    let now = now_ts();
    let id = request
        .id
        .as_deref()
        .map(safe_id)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("{}-{}", safe_id(&request.name), compact_timestamp(&now)));
    let mut query_request = request.request;
    query_request.dataset = dataset.to_owned();
    let record = QueryTemplateRecord {
        id,
        name: request.name.trim().to_owned(),
        description: request
            .description
            .unwrap_or_else(|| "Saved structured query template.".to_owned()),
        request: query_request,
        created_ts: now.clone(),
        updated_ts: now,
        owner: request.owner.unwrap_or_else(|| "local".to_owned()),
        tags: request
            .tags
            .into_iter()
            .map(|tag| tag.trim().to_owned())
            .filter(|tag| !tag.is_empty())
            .take(12)
            .collect(),
    };
    persist_query_template_to_dir(&query_template_dir(), &record)?;
    Ok(record)
}

fn persist_query_template_to_dir(
    dir: &Path,
    record: &QueryTemplateRecord,
) -> Result<(), QueryError> {
    fs::create_dir_all(dir).map_err(|error| {
        QueryError::bad_request(format!(
            "Could not create query template directory: {error}"
        ))
    })?;
    let path = dir.join(format!("{}.json", safe_id(&record.id)));
    let bytes = serde_json::to_vec_pretty(record)
        .map_err(|error| QueryError::bad_request(format!("Template was not JSON: {error}")))?;
    fs::write(path, bytes)
        .map_err(|error| QueryError::bad_request(format!("Could not persist template: {error}")))
}

fn append_query_audit(entry: QueryAuditEntry) {
    let dir = query_audit_dir();
    if let Err(error) = append_query_audit_to_dir(&dir, &entry) {
        tracing::warn!("query audit append failed: {error}");
    }
}

fn append_query_audit_to_dir(dir: &Path, entry: &QueryAuditEntry) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|error| error.to_string())?;
    let day = entry
        .ts
        .get(0..10)
        .unwrap_or("unknown-date")
        .replace(':', "");
    let path = dir.join(format!("{day}.jsonl"));
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    let line = serde_json::to_string(entry).map_err(|error| error.to_string())?;
    file.write_all(line.as_bytes())
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|error| error.to_string())
}

impl QueryAuditEntry {
    fn from_success(
        request: &QueryRequest,
        result: &QueryResult,
        backend: QueryBackendKind,
        duration_ms: u128,
    ) -> Self {
        Self {
            query_id: query_id(),
            ts: now_ts(),
            dataset: sanitize_audit_text(&result.dataset),
            filters: sanitized_filters(request),
            group_by: sanitized_audit_list(&request.group_by),
            metrics: sanitized_audit_list(&request.metrics),
            limit: result.limit,
            duration_ms,
            returned_rows: Some(result.returned_rows),
            source: sanitize_audit_text(backend.as_str()),
            error: None,
        }
    }

    fn from_error(
        request: &QueryRequest,
        backend: QueryBackendKind,
        duration_ms: u128,
        error: &str,
    ) -> Self {
        Self {
            query_id: query_id(),
            ts: now_ts(),
            dataset: sanitize_audit_text(&request.dataset),
            filters: sanitized_filters(request),
            group_by: sanitized_audit_list(&request.group_by),
            metrics: sanitized_audit_list(&request.metrics),
            limit: request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT),
            duration_ms,
            returned_rows: None,
            source: sanitize_audit_text(backend.as_str()),
            error: Some(sanitize_audit_text(error)),
        }
    }
}

fn sanitized_filters(request: &QueryRequest) -> Value {
    Value::Array(
        request
            .filters
            .iter()
            .map(|filter| {
                json!({
                    "field": sanitize_audit_text(&filter.field),
                    "op": sanitize_audit_text(&filter.op),
                    "value": if is_secret_like_field(&filter.field) {
                        json!("[redacted]")
                    } else {
                        bounded_value(&filter.value)
                    }
                })
            })
            .collect(),
    )
}

fn sanitized_audit_list(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| sanitize_audit_text(value))
        .collect()
}

fn bounded_value(value: &Value) -> Value {
    match value {
        Value::String(text) => json!(sanitize_audit_text(text)),
        Value::Array(items) => Value::Array(items.iter().take(20).map(bounded_value).collect()),
        Value::Object(object) => Value::Object(
            object
                .iter()
                .take(20)
                .map(|(key, value)| {
                    (
                        sanitize_audit_text(key),
                        if is_secret_like_field(key) {
                            json!("[redacted]")
                        } else {
                            bounded_value(value)
                        },
                    )
                })
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn sanitize_audit_text(text: &str) -> String {
    let mut output = Vec::new();
    let mut redact_next = false;
    for part in text.split_whitespace() {
        let lowered = part.to_ascii_lowercase();
        let redact = redact_next
            || is_secret_like_field(&lowered)
            || lowered == "bearer"
            || lowered.starts_with("bearer=")
            || lowered.starts_with("sharedaccesssignature")
            || lowered.contains("sig=")
            || lowered.contains("se=") && lowered.contains("sp=")
            || lowered.contains("accountkey=")
            || lowered.contains("accesskey=")
            || lowered.contains("password=")
            || lowered.contains("authorization:")
            || lowered.contains("private_key")
            || lowered.contains("-----begin");
        let mark_next = lowered == "bearer" || lowered.ends_with("bearer");
        if redact {
            redact_next = mark_next;
            output.push("[redacted]".to_owned());
        } else if part.len() > 128 {
            redact_next = mark_next;
            output.push(format!(
                "{}...[truncated]",
                part.chars().take(128).collect::<String>()
            ));
        } else {
            redact_next = mark_next;
            output.push(part.to_owned());
        }
    }
    if output.is_empty() && text.len() > 128 {
        return format!(
            "{}...[truncated]",
            text.chars().take(128).collect::<String>()
        );
    }
    output.join(" ")
}

fn contains_disallowed_secret_field(request: &QueryRequest) -> bool {
    request
        .filters
        .iter()
        .any(|filter| is_secret_like_field(&filter.field))
        || request
            .sort
            .iter()
            .any(|sort| is_secret_like_field(&sort.field))
}

fn is_secret_like_field(field: &str) -> bool {
    let lower = field.to_ascii_lowercase();
    lower.contains("secret")
        || lower.contains("password")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("bearer")
        || lower == "authorization"
        || lower.ends_with("_authorization")
}

fn query_id() -> String {
    format!("query-{}", compact_timestamp(&now_ts()))
}

fn compact_timestamp(ts: &str) -> String {
    ts.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
}

fn safe_id(value: &str) -> String {
    let mut id = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else if matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    while id.contains("--") {
        id = id.replace("--", "-");
    }
    id.trim_matches('-').chars().take(96).collect()
}

fn row_contains(row: &Value, needle: &str) -> bool {
    serde_json::to_string(row)
        .unwrap_or_default()
        .to_lowercase()
        .contains(needle)
}

fn read_json_or_null(path: impl AsRef<Path>) -> Value {
    let path = path.as_ref();
    if let Some(bytes) = read_blob_bytes_for_path(path) {
        return serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    }
    let Ok(text) = fs::read_to_string(path) else {
        return Value::Null;
    };
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

fn artifact_blob_client() -> Option<AzureBlobClient> {
    let account = env::var("AZURE_STORAGE_ACCOUNT_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())?;
    let container = env::var("AZURE_STORAGE_CONTAINER_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "bot-events".to_owned());
    let client_id = env::var("AZURE_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());
    Some(AzureBlobClient::with_managed_identity(
        account, container, client_id,
    ))
}

fn read_blob_bytes_for_path(path: &Path) -> Option<Vec<u8>> {
    let blob_name = blob_name_for_path(path)?;
    let mut client = artifact_blob_client()?;
    match client.download_blob_bytes(&blob_name) {
        Ok(bytes) => Some(bytes),
        Err(AzureBlobError::HttpStatus(404)) => None,
        Err(_) => None,
    }
}

fn blob_name_for_path(path: &Path) -> Option<String> {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let trimmed = normalized.trim_start_matches("./").trim_start_matches('/');
    let allowed = ["reports/research/", "data_quality/freshness/"];
    if allowed.iter().any(|prefix| trimmed.starts_with(prefix)) {
        return Some(trimmed.to_owned());
    }
    None
}

fn parse_scalar(value: &str) -> Value {
    if value.eq_ignore_ascii_case("true") {
        return json!(true);
    }
    if value.eq_ignore_ascii_case("false") {
        return json!(false);
    }
    value
        .parse::<f64>()
        .map(|number| json!(number))
        .unwrap_or_else(|_| json!(value))
}

fn date_from_path(path: &str) -> Option<String> {
    for part in path.split('/') {
        if part.len() == 10
            && part.as_bytes().get(4) == Some(&b'-')
            && part.as_bytes().get(7) == Some(&b'-')
        {
            return Some(part.to_owned());
        }
    }
    None
}

fn quality_from_path(path: &str) -> &'static str {
    if path.contains("exclusion") || path.contains("put_bug") {
        "excluded"
    } else if path.contains("audit") || path.contains("quality") || path.contains("freshness") {
        "quality_checked"
    } else {
        "unknown"
    }
}

fn source_job_from_path(path: &str) -> &'static str {
    if path.contains("prospective") {
        "prospective-validation"
    } else if path.contains("hourly") || path.contains("audit") {
        "hourly-quality-audit"
    } else if path.contains("daily") || path.contains("final_report") {
        "daily-research-report"
    } else if path.contains("backfill") {
        "manual-backfill"
    } else {
        "research-artifact"
    }
}

fn relative_report_path(path: &Path) -> String {
    path.strip_prefix(REPORT_ROOT)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_owned()
}

fn default_filter_op() -> String {
    "eq".to_owned()
}

fn default_sort_direction() -> String {
    "asc".to_owned()
}

fn now_ts() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_template_writes_structured_json() {
        let dir = test_dir("query-template");
        let request = QueryRequest {
            dataset: "queue-evidence".to_owned(),
            filters: vec![QueryFilter {
                field: "event_type".to_owned(),
                op: "eq".to_owned(),
                value: json!("last_trade_price"),
            }],
            group_by: vec!["market_id".to_owned()],
            metrics: vec!["count".to_owned()],
            sort: Vec::new(),
            limit: Some(50),
            offset: None,
        };
        let record = QueryTemplateRecord {
            id: "queue-coverage".to_owned(),
            name: "Queue coverage".to_owned(),
            description: "Queue evidence coverage.".to_owned(),
            request,
            created_ts: "2026-06-23T00:00:00Z".to_owned(),
            updated_ts: "2026-06-23T00:00:00Z".to_owned(),
            owner: "test".to_owned(),
            tags: vec!["queue".to_owned()],
        };

        persist_query_template_to_dir(&dir, &record).expect("persist template");
        let payload = read_json_or_null(dir.join("queue-coverage.json"));

        assert_eq!(payload["id"], "queue-coverage");
        assert_eq!(payload["request"]["dataset"], "queue-evidence");
        assert_eq!(
            payload["request"]["filters"][0]["value"],
            "last_trade_price"
        );
    }

    #[test]
    fn audit_append_redacts_secret_like_filters() {
        let dir = test_dir("query-audit");
        let request = QueryRequest {
            dataset: "markets password=dataset-secret".to_owned(),
            filters: vec![QueryFilter {
                field: "api_key".to_owned(),
                op: "eq".to_owned(),
                value: json!("do-not-write"),
            }],
            group_by: vec!["authorization".to_owned()],
            metrics: vec!["bearer token-value".to_owned()],
            sort: Vec::new(),
            limit: Some(10),
            offset: None,
        };
        let entry = QueryAuditEntry::from_error(
            &request,
            QueryBackendKind::ReportBacked,
            7,
            "bad query sig=hidden password=also-hidden",
        );

        append_query_audit_to_dir(&dir, &entry).expect("audit append");
        let audit_text = fs::read_to_string(dir.join(format!("{}.jsonl", &entry.ts[0..10])))
            .expect("audit text");

        assert!(audit_text.contains("\"value\":\"[redacted]\""));
        assert!(!audit_text.contains("do-not-write"));
        assert!(!audit_text.contains("dataset-secret"));
        assert!(!audit_text.contains("token-value"));
        assert!(!audit_text.contains("also-hidden"));
        assert!(!audit_text.contains("sig=hidden"));
        assert!(audit_text.contains("\"source\":\"report_backed\""));
    }

    #[test]
    fn new_curated_datasets_are_canonical() {
        for dataset in [
            "market_truth",
            "decision-features",
            "fill_candidates",
            "queue-evidence",
            "queue_proxy_results",
            "prospective_daily",
            "candidate-market-pnl",
            "regime_market_pnl",
            "calibration_buckets",
        ] {
            assert!(canonical_dataset(dataset).is_ok(), "{dataset}");
        }
    }

    #[test]
    fn safe_id_removes_path_and_shell_characters() {
        assert_eq!(safe_id("../Queue Coverage && Run"), "queue-coverage-run");
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-{name}-{}-{}",
            std::process::id(),
            compact_timestamp(&now_ts())
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("test dir");
        dir
    }
}
