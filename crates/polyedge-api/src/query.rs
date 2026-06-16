use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

mod catalog;

use crate::history::{max_historical_markets, merge_market_lists, MarketHistoryStore};
use crate::labs;
use crate::ApiState;
use catalog::{datasets, templates};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1_000;
const REPORT_ROOT: &str = "reports/research";
const FRESHNESS_LATEST: &str = "data_quality/freshness/latest.json";

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

struct ReportBackedQueryBackend {
    state: ApiState,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
struct QueryFilter {
    field: String,
    #[serde(default = "default_filter_op")]
    op: String,
    value: Value,
}

#[derive(Debug, Deserialize)]
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
    Json(ReportBackedQueryBackend { state }.schema())
}

async fn query_templates() -> Json<Value> {
    Json(json!({ "templates": templates() }))
}

async fn save_query_template(Json(request): Json<Value>) -> impl IntoResponse {
    (
        StatusCode::CREATED,
        Json(json!({
            "persisted": false,
            "template": request,
            "detail": "Server-side saved views are not persisted in this build. The structured query can be reused by the frontend."
        })),
    )
}

async fn run_query(
    State(state): State<ApiState>,
    Json(request): Json<QueryRequest>,
) -> impl IntoResponse {
    let backend = ReportBackedQueryBackend { state };
    match backend.run(request).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(error) => (error.status, Json(json!({ "detail": error.detail }))).into_response(),
    }
}

impl QueryBackend for ReportBackedQueryBackend {
    fn schema(&self) -> Value {
        json!({
            "backend": "ReportBackedQueryBackend",
            "structured_only": true,
            "generated_ts": now_ts(),
            "datasets": datasets(),
            "operators": ["eq", "ne", "contains", "gt", "gte", "lt", "lte", "in"],
            "output_modes": ["table", "bar", "line", "scatter", "csv", "json"],
            "safety": {
                "live_trading_enabled": false,
                "arbitrary_sql_or_kql": false,
                "secrets_exposed": false
            }
        })
    }

    async fn run(&self, request: QueryRequest) -> Result<QueryResult, QueryError> {
        let dataset = canonical_dataset(&request.dataset)?;
        let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
        let offset = request.offset.unwrap_or(0);
        let mut warnings = Vec::new();
        let mut rows = self.load_dataset(dataset, &mut warnings).await?;
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
                "runtime": "rust_runtime_memory",
                "reports_root": REPORT_ROOT,
                "read_only": true,
                "live_trading_enabled": false
            }),
        })
    }
}

impl ReportBackedQueryBackend {
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
            _ => Err(QueryError::bad_request(format!(
                "Unsupported dataset {dataset}."
            ))),
        }
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
    let mut rows = Vec::new();
    collect_report_rows(Path::new(REPORT_ROOT), file_names, &mut rows, MAX_LIMIT);
    rows
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
    let mut rows = Vec::new();
    collect_artifacts(Path::new(REPORT_ROOT), &mut rows);
    rows.sort_by_key(|row| string_at(row, "path"));
    rows.truncate(MAX_LIMIT);
    rows
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

fn row_contains(row: &Value, needle: &str) -> bool {
    serde_json::to_string(row)
        .unwrap_or_default()
        .to_lowercase()
        .contains(needle)
}

fn read_json_or_null(path: impl AsRef<Path>) -> Value {
    let Ok(text) = fs::read_to_string(path) else {
        return Value::Null;
    };
    serde_json::from_str(&text).unwrap_or(Value::Null)
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
