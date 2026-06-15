use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{SecondsFormat, Utc};
use polyedge_reporting::research::{
    load_exclusion_registry, load_frozen_candidate_registry, DEFAULT_EXCLUSION_FILE,
    DEFAULT_FROZEN_CANDIDATES_FILE, FROZEN_CANDIDATE_NAMES,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path as FsPath, PathBuf};

use crate::ApiState;

const REPORT_ROOT: &str = "reports/research";
const FRESHNESS_LATEST: &str = "data_quality/freshness/latest.json";

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/data-quality/latest", get(data_quality_latest))
        .route("/data-quality/hourly", get(data_quality_hourly))
        .route("/data-quality/exclusions", get(data_quality_exclusions))
        .route(
            "/data-quality/exclusions/validate",
            post(validate_exclusions),
        )
        .route("/reports/latest", get(reports_latest))
        .route("/reports/daily/:date", get(reports_daily))
        .route("/reports/artifacts", get(report_artifacts))
        .route("/reports/artifacts/:artifact_id", get(report_artifact))
        .route("/jobs/freshness-check", post(start_freshness_check))
        .route("/jobs/daily-report", post(start_daily_report))
        .route(
            "/jobs/prospective-validation",
            post(start_prospective_validation),
        )
        .route("/jobs/replay-index", post(start_replay_index))
        .route("/jobs/backfill", post(start_backfill))
        .route("/jobs", get(jobs))
        .route("/jobs/:job_id", get(job_detail))
        .route("/prospective", get(prospective))
        .route("/regimes/latest", get(regimes_latest))
        .route("/calibration/latest", get(calibration_latest))
        .route("/sample-size/latest", get(sample_size_latest))
        .route("/fill-models/latest", get(fill_models_latest))
}

async fn data_quality_latest(State(state): State<ApiState>) -> impl IntoResponse {
    let freshness = read_json_or_null(FRESHNESS_LATEST);
    let exclusions = exclusion_payload();
    let recorder = state.runtime.status().await["recorder"].clone();
    Json(json!({
        "generated_ts": now_ts(),
        "freshness": freshness,
        "recorder": recorder,
        "exclusions": exclusions,
        "source": {
            "freshness_path": FRESHNESS_LATEST,
            "exclusion_file": DEFAULT_EXCLUSION_FILE
        }
    }))
}

#[derive(Deserialize)]
struct HourlyQuery {
    date: Option<String>,
}

async fn data_quality_hourly(Query(query): Query<HourlyQuery>) -> impl IntoResponse {
    let date = query.date.unwrap_or_else(today);
    let prefix = PathBuf::from(REPORT_ROOT)
        .join("hourly")
        .join(date.replace('-', "/"));
    Json(json!({
        "date": date,
        "audits": list_json_files(&prefix, "audit.json", 96)
    }))
}

async fn data_quality_exclusions() -> impl IntoResponse {
    Json(exclusion_payload())
}

async fn validate_exclusions() -> impl IntoResponse {
    let payload = exclusion_payload();
    let has_put_bug_window = payload["windows"].as_array().is_some_and(|windows| {
        windows.iter().any(|window| {
            window["start"].as_str() == Some("2026-06-11T10:00:00Z")
                && window["end"].as_str() == Some("2026-06-12T22:00:00Z")
                && window["default_exclude"].as_bool() == Some(true)
        })
    });
    Json(json!({
        "valid": has_put_bug_window,
        "issues": if has_put_bug_window { Vec::<String>::new() } else { vec!["Required June 11/12 PUT-bug exclusion window is missing or not defaulted.".to_owned()] },
        "registry": payload
    }))
}

async fn reports_latest() -> impl IntoResponse {
    Json(read_latest_report_payload())
}

async fn reports_daily(Path(date): Path<String>) -> impl IntoResponse {
    if !valid_date(&date) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "date must be YYYY-MM-DD" })),
        )
            .into_response();
    }
    let dir = PathBuf::from(REPORT_ROOT).join("daily").join(&date);
    Json(json!({
        "date": date,
        "report": read_json_or_null(dir.join("final_report.json")),
        "audit": read_json_or_null(dir.join("data_audit.json")),
        "baseline": read_json_or_null(dir.join("baseline.json")),
        "regimes": read_json_or_null(dir.join("regimes.json")),
        "calibration": read_json_or_null(dir.join("calibration.json")),
        "sample_size": read_json_or_null(dir.join("sample_size.json")),
        "artifacts": artifacts_for_prefix(&format!("daily/{date}"))
    }))
    .into_response()
}

#[derive(Deserialize)]
struct ArtifactsQuery {
    prefix: Option<String>,
}

async fn report_artifacts(Query(query): Query<ArtifactsQuery>) -> impl IntoResponse {
    let prefix = sanitize_prefix(query.prefix.as_deref().unwrap_or_default());
    match prefix {
        Ok(prefix) => Json(json!({ "prefix": prefix, "artifacts": artifacts_for_prefix(&prefix) }))
            .into_response(),
        Err(detail) => (StatusCode::BAD_REQUEST, Json(json!({ "detail": detail }))).into_response(),
    }
}

async fn report_artifact(Path(artifact_id): Path<String>) -> impl IntoResponse {
    let relative = artifact_id.replace('~', "/");
    let Ok(relative) = sanitize_prefix(&relative) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "invalid artifact id" })),
        )
            .into_response();
    };
    let path = PathBuf::from(REPORT_ROOT).join(relative);
    if !path.is_file() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": "artifact not found" })),
        )
            .into_response();
    }
    match artifact_payload(&path) {
        Ok(payload) => Json(payload).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "detail": error })),
        )
            .into_response(),
    }
}

async fn jobs() -> impl IntoResponse {
    Json(json!({
        "jobs": job_definitions(),
        "source": "iac_defined_jobs",
        "note": "API does not run long research jobs in-process."
    }))
}

async fn job_detail(Path(job_id): Path<String>) -> impl IntoResponse {
    let jobs = job_definitions();
    let found = jobs
        .as_array()
        .into_iter()
        .flatten()
        .find(|job| {
            job["job_id"].as_str() == Some(&job_id) || job["job_name"].as_str() == Some(&job_id)
        })
        .cloned();
    match found {
        Some(job) => (StatusCode::OK, Json(json!({ "job": job }))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("Job {job_id} was not found.") })),
        )
            .into_response(),
    }
}

async fn start_freshness_check() -> impl IntoResponse {
    job_start_response("freshness-check", "polyedge-data-freshness-job")
}

async fn start_daily_report() -> impl IntoResponse {
    job_start_response("daily-report", "polyedge-daily-research-job")
}

async fn start_prospective_validation() -> impl IntoResponse {
    job_start_response("prospective-validation", "polyedge-prospective-job")
}

async fn start_replay_index() -> impl IntoResponse {
    job_start_response("replay-index", "polyedge-replay-index-job")
}

async fn start_backfill() -> impl IntoResponse {
    job_start_response("backfill", "polyedge-backfill-job")
}

async fn prospective() -> impl IntoResponse {
    let payload = read_json_or_null(
        PathBuf::from(REPORT_ROOT)
            .join("prospective")
            .join("prospective_validation.json"),
    );
    if !payload.is_null() {
        return Json(payload);
    }
    Json(json!({
        "generated_ts": now_ts(),
        "result": {
            "status": "collecting",
            "rows": [],
            "frozen_candidates": frozen_candidates_payload(),
            "research_only": true,
            "live_deployment_allowed": false
        }
    }))
}

async fn regimes_latest() -> impl IntoResponse {
    Json(latest_named_report("regimes.json", "regime_profiles.json"))
}

async fn calibration_latest() -> impl IntoResponse {
    Json(latest_named_report("calibration.json", "calibration.json"))
}

async fn sample_size_latest() -> impl IntoResponse {
    Json(latest_named_report("sample_size.json", "sample_size.json"))
}

async fn fill_models_latest() -> impl IntoResponse {
    Json(latest_named_report(
        "baseline.json",
        "baseline_static_all_fill_models.json",
    ))
}

fn job_start_response(job_id: &str, job_name: &str) -> impl IntoResponse {
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "job_id": job_id,
            "job_name": job_name,
            "status": "defined_in_iac_not_started_by_api",
            "created_ts": now_ts(),
            "research_only": true,
            "live_trading_enabled": false,
            "raw_data_mutated": false,
            "detail": "Azure Container Apps Job execution is defined in IaC. Start it through Azure job execution wiring; the API does not run long research jobs in-process."
        })),
    )
}

fn read_latest_report_payload() -> Value {
    let latest = read_json_or_null(PathBuf::from(REPORT_ROOT).join("latest_daily_report.json"));
    if !latest.is_null() {
        return json!({
            "report": latest,
            "latest": true,
            "artifacts": artifacts_for_prefix("")
        });
    }
    if let Some(date) = latest_daily_date() {
        let dir = PathBuf::from(REPORT_ROOT).join("daily").join(&date);
        return json!({
            "date": date,
            "report": read_json_or_null(dir.join("final_report.json")),
            "audit": read_json_or_null(dir.join("data_audit.json")),
            "baseline": read_json_or_null(dir.join("baseline.json")),
            "regimes": read_json_or_null(dir.join("regimes.json")),
            "calibration": read_json_or_null(dir.join("calibration.json")),
            "sample_size": read_json_or_null(dir.join("sample_size.json")),
            "artifacts": artifacts_for_prefix(&format!("daily/{date}"))
        });
    }
    json!({
        "report": Value::Null,
        "detail": "No research daily report exists yet.",
        "artifacts": artifacts_for_prefix("")
    })
}

fn latest_named_report(primary: &str, fallback: &str) -> Value {
    let Some(date) = latest_daily_date() else {
        return json!({ "report": Value::Null, "detail": "No daily report exists yet." });
    };
    let dir = PathBuf::from(REPORT_ROOT).join("daily").join(&date);
    let report = read_json_or_null(dir.join(primary));
    let report = if report.is_null() {
        read_json_or_null(dir.join(fallback))
    } else {
        report
    };
    json!({ "date": date, "report": report })
}

fn latest_daily_date() -> Option<String> {
    let root = PathBuf::from(REPORT_ROOT).join("daily");
    let mut dates = fs::read_dir(root)
        .ok()?
        .flatten()
        .filter(|entry| entry.file_type().ok().is_some_and(|kind| kind.is_dir()))
        .filter_map(|entry| {
            let value = entry.file_name().to_string_lossy().into_owned();
            valid_date(&value).then_some(value)
        })
        .collect::<Vec<_>>();
    dates.sort();
    dates.pop()
}

fn exclusion_payload() -> Value {
    load_exclusion_registry(FsPath::new(DEFAULT_EXCLUSION_FILE))
        .map(|registry| registry.as_json())
        .unwrap_or_else(|error| {
            json!({
                "version": 0,
                "updated_at": Value::Null,
                "windows": [],
                "error": error.to_string()
            })
        })
}

fn frozen_candidates_payload() -> Value {
    load_frozen_candidate_registry(FsPath::new(DEFAULT_FROZEN_CANDIDATES_FILE))
        .map(|registry| registry.as_json())
        .unwrap_or_else(|error| {
            json!({
                "version": 0,
                "candidates": FROZEN_CANDIDATE_NAMES,
                "error": error.to_string(),
                "research_only": true,
                "enabled_by_default": false
            })
        })
}

fn list_json_files(root: &FsPath, file_name: &str, limit: usize) -> Vec<Value> {
    let mut values = Vec::new();
    collect_named_files(root, file_name, &mut values, limit);
    values
}

fn collect_named_files(root: &FsPath, file_name: &str, values: &mut Vec<Value>, limit: usize) {
    if values.len() >= limit || !root.exists() {
        return;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if values.len() >= limit {
            return;
        }
        let path = entry.path();
        if path.is_dir() {
            collect_named_files(&path, file_name, values, limit);
        } else if path.file_name().and_then(|name| name.to_str()) == Some(file_name) {
            values.push(read_json_or_null(path));
        }
    }
}

fn artifacts_for_prefix(prefix: &str) -> Vec<Value> {
    let root = PathBuf::from(REPORT_ROOT).join(prefix);
    let mut artifacts = Vec::new();
    collect_artifacts(&root, &mut artifacts);
    artifacts.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));
    artifacts
}

fn collect_artifacts(root: &FsPath, artifacts: &mut Vec<Value>) {
    if !root.exists() {
        return;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_artifacts(&path, artifacts);
            continue;
        }
        let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
            continue;
        };
        if !matches!(extension, "json" | "md") {
            continue;
        }
        let relative = path
            .strip_prefix(REPORT_ROOT)
            .unwrap_or(&path)
            .to_string_lossy()
            .trim_start_matches('/')
            .to_owned();
        let modified_ts = fs::metadata(&path)
            .and_then(|metadata| metadata.modified())
            .ok()
            .map(chrono::DateTime::<Utc>::from)
            .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Secs, true));
        artifacts.push(json!({
            "artifact_id": relative.replace('/', "~"),
            "path": relative,
            "kind": extension,
            "size_bytes": fs::metadata(&path).ok().map(|metadata| metadata.len()),
            "modified_ts": modified_ts
        }));
    }
}

fn artifact_payload(path: &FsPath) -> Result<Value, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let relative = path
        .strip_prefix(REPORT_ROOT)
        .unwrap_or(path)
        .to_string_lossy()
        .trim_start_matches('/')
        .to_owned();
    if path.extension().and_then(|value| value.to_str()) == Some("json") {
        let json: Value = serde_json::from_str(&text).map_err(|error| error.to_string())?;
        Ok(json!({ "path": relative, "kind": "json", "content": json }))
    } else {
        Ok(json!({ "path": relative, "kind": "markdown", "content": text }))
    }
}

fn read_json_or_null(path: impl AsRef<FsPath>) -> Value {
    let path = path.as_ref();
    let Ok(text) = fs::read_to_string(path) else {
        return Value::Null;
    };
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

fn job_definitions() -> Value {
    json!([
        job_definition(
            "freshness-check",
            "polyedge-data-freshness-job",
            "Schedule",
            "*/5 * * * *"
        ),
        job_definition(
            "hourly-quality",
            "polyedge-hourly-quality-job",
            "Schedule",
            "10 * * * *"
        ),
        job_definition(
            "daily-report",
            "polyedge-daily-research-job",
            "Schedule",
            "30 1 * * *"
        ),
        job_definition(
            "prospective-validation",
            "polyedge-prospective-job",
            "Schedule",
            "30 2 * * *"
        ),
        job_definition(
            "replay-index",
            "polyedge-replay-index-job",
            "Schedule",
            "0 3 * * *"
        ),
        job_definition("backfill", "polyedge-backfill-job", "Manual", Value::Null),
    ])
}

fn job_definition(job_id: &str, job_name: &str, trigger: &str, cron: impl Into<Value>) -> Value {
    json!({
        "job_id": job_id,
        "job_name": job_name,
        "status": "defined_in_iac",
        "trigger": trigger,
        "cron": cron.into(),
        "last_start": Value::Null,
        "last_finish": Value::Null,
        "duration": Value::Null,
        "exit_code": Value::Null,
        "output_artifact": Value::Null,
        "error": Value::Null,
        "research_only": true,
        "live_trading_enabled": false
    })
}

fn sanitize_prefix(value: &str) -> Result<String, &'static str> {
    let trimmed = value.trim().trim_start_matches('/').trim_end_matches('/');
    if trimmed.contains("..") || trimmed.contains('\\') {
        return Err("prefix must stay under reports/research");
    }
    Ok(trimmed.to_owned())
}

fn valid_date(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-')
        && value
            .chars()
            .enumerate()
            .all(|(index, c)| matches!(index, 4 | 7) || c.is_ascii_digit())
}

fn today() -> String {
    Utc::now().date_naive().to_string()
}

fn now_ts() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}
