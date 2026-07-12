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
use polyedge_storage::{AzureBlobClient, AzureBlobError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::path::{Path as FsPath, PathBuf};

use crate::azure_jobs::{
    execution_summary, latest_execution_summary, AzureJobClient, AzureLogAnalyticsClient,
    JobStartOptions,
};
use crate::ApiState;

const REPORT_ROOT: &str = "reports/research";
const FRESHNESS_LATEST: &str = "data_quality/freshness/latest.json";

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/summary", get(summary))
        .route("/candidates", get(candidates))
        .route("/candidates/:candidate", get(candidate_detail))
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
        .route("/jobs/:job_id/start", post(start_job))
        .route("/jobs/:job_id/executions", get(job_executions))
        .route(
            "/jobs/:job_id/executions/:execution_id/logs",
            get(job_execution_logs),
        )
        .route("/jobs/:job_id", get(job_detail))
        .route("/prospective", get(prospective))
        .route("/regimes/latest", get(regimes_latest))
        .route("/calibration/latest", get(calibration_latest))
        .route("/sample-size/latest", get(sample_size_latest))
        .route("/fill-models/latest", get(fill_models_latest))
        .route("/venue-execution", get(venue_execution))
}

async fn venue_execution() -> impl IntoResponse {
    Json(json!({
        "generated_ts": now_ts(),
        "latest": read_json_or_null("reports/research/venue-probe/latest.json"),
        "latest_attempt": read_json_or_null("reports/research/venue-probe/latest_attempt.json"),
        "preflight": read_json_or_null("reports/research/venue-probe/latest_authenticated_dry_run.json"),
        "model": read_json_or_null("reports/research/venue-probe/effective_queue_model.json"),
        "queue_position_source": "authenticated_lifecycle_plus_public_l2",
        "queue_position_metric": "inferred_size_ahead",
        "literal_fifo_rank_available": false,
        "practical_target": "probability_of_fill_within_1_5_30_60_seconds",
        "remaining_limitation": "Polymarket does not expose exact matching rank, per-order public priority, hidden liquidity, or venue-internal priority changes.",
        "research_only": true,
        "strategy_promotion_allowed": false
    }))
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

async fn summary() -> impl IntoResponse {
    let latest = read_latest_report_payload();
    let prospective = prospective_payload();
    let sample_size = latest.get("sample_size").cloned().unwrap_or_else(|| {
        latest_named_report("sample_size.json", "sample_size.json")["report"].clone()
    });
    let sample_stats = sample_size
        .pointer("/result/statistics")
        .cloned()
        .unwrap_or(Value::Null);
    let recommendation = latest
        .pointer("/report/result/executive_summary/recommendation")
        .or_else(|| latest.pointer("/report/result/recommendation"))
        .cloned()
        .unwrap_or_else(|| json!("Continue collecting data unchanged"));
    Json(json!({
        "generated_ts": now_ts(),
        "date": latest["date"].clone(),
        "status": prospective["result"]["status"].clone(),
        "recommendation": recommendation,
        "sample_size": sample_stats,
        "data_quality": latest["audit"]["result"]["status"].as_str().unwrap_or("unknown"),
        "candidate_count": candidate_evidence_rows(&prospective).len(),
        "prospective_rows": prospective["result"]["rows"].as_array().map(Vec::len).unwrap_or(0),
        "research_only": true,
        "live_deployment_allowed": false
    }))
}

async fn candidates() -> impl IntoResponse {
    let prospective = prospective_payload();
    Json(json!({
        "generated_ts": now_ts(),
        "status": prospective["result"]["status"].clone(),
        "candidates": candidate_evidence_rows(&prospective),
        "research_only": true,
        "live_deployment_allowed": false
    }))
}

async fn candidate_detail(Path(candidate): Path<String>) -> impl IntoResponse {
    let prospective = prospective_payload();
    let candidates = candidate_evidence_rows(&prospective);
    let Some(summary) = candidates.iter().find(|row| {
        row["candidate"].as_str() == Some(candidate.as_str())
            || row["profile"].as_str() == Some(candidate.as_str())
    }) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("Candidate {candidate} was not found.") })),
        )
            .into_response();
    };
    let history = prospective["result"]["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| {
            json!({
                "date": row["date"].clone(),
                "pnl": candidate_pnl(row, &candidate),
                "settled_markets": row["settled_markets"].clone(),
                "max_drawdown": row["max_drawdown"].clone(),
                "data_quality_status": row["data_quality_status"].clone(),
                "recommendation": row["recommendation"].clone()
            })
        })
        .collect::<Vec<_>>();
    Json(json!({
        "generated_ts": now_ts(),
        "candidate": summary,
        "history": history,
        "artifacts": candidate_artifacts(),
        "research_only": true,
        "live_deployment_allowed": false
    }))
    .into_response()
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
    Json(daily_report_payload(&date)).into_response()
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

pub(crate) async fn report_artifact(Path(artifact_id): Path<String>) -> impl IntoResponse {
    let relative = artifact_id.replace('~', "/");
    let Ok(relative) = sanitize_prefix(&relative) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "invalid artifact id" })),
        )
            .into_response();
    };
    let path = PathBuf::from(REPORT_ROOT).join(relative);
    match artifact_payload(&path) {
        Ok(Some(payload)) => Json(payload).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": "artifact not found" })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "detail": error })),
        )
            .into_response(),
    }
}

pub(crate) async fn jobs() -> impl IntoResponse {
    let mut jobs = job_definitions();
    let mut source = "iac_defined_jobs".to_owned();
    let mut note = "API does not run long research jobs in-process.".to_owned();
    match AzureJobClient::from_env() {
        Ok(Some(client)) => {
            source = "iac_defined_jobs+azure_arm_executions".to_owned();
            jobs = match tokio::task::spawn_blocking(move || {
                enrich_jobs_with_azure(&mut jobs, &client);
                jobs
            })
            .await
            {
                Ok(jobs) => jobs,
                Err(error) => {
                    note.push_str(&format!(" Azure execution enrichment failed: {error}"));
                    job_definitions()
                }
            };
        }
        Ok(None) => {
            note.push_str(" Azure ARM env is not configured, so execution history is unavailable.");
        }
        Err(error) => {
            note.push_str(&format!(" Azure ARM client initialization failed: {error}"));
        }
    }
    Json(json!({
        "jobs": jobs,
        "source": source,
        "note": note
    }))
}

pub(crate) async fn job_detail(Path(job_id): Path<String>) -> impl IntoResponse {
    match job_definition_by_id(&job_id) {
        Some(job) => (StatusCode::OK, Json(json!({ "job": job }))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("Job {job_id} was not found.") })),
        )
            .into_response(),
    }
}

pub(crate) async fn job_logs(Path(job_id): Path<String>) -> impl IntoResponse {
    let Some(job) = job_definition_by_id(&job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("Job {job_id} was not found.") })),
        )
            .into_response();
    };
    Json(json!({
        "job_id": job["job_id"],
        "job_name": job["job_name"],
        "logs": [],
        "artifacts": [
            job["output_artifact"].clone(),
            format!("reports/jobs/latest/{}.json", job["job_id"].as_str().unwrap_or("job"))
        ],
        "detail": "Live Container Apps execution logs are read from Azure Monitor. This endpoint exposes the job identity and artifact paths without exposing credentials."
    }))
    .into_response()
}

pub(crate) async fn job_executions(Path(job_id): Path<String>) -> impl IntoResponse {
    let Some(job) = job_definition_by_id(&job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("Job {job_id} was not found.") })),
        )
            .into_response();
    };
    let Some(job_name) = job["job_name"].as_str().map(str::to_owned) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "detail": format!("Job {job_id} has no job_name.") })),
        )
            .into_response();
    };
    let client = match AzureJobClient::from_env() {
        Ok(Some(client)) => client,
        Ok(None) => {
            return Json(json!({
                "job_id": job_id,
                "job_name": job_name,
                "executions": [],
                "source": "azure_arm_not_configured",
                "artifacts": job_artifact_paths(&job),
                "detail": "Azure ARM env is not configured. Set AZURE_SUBSCRIPTION_ID and AZURE_RESOURCE_GROUP to list Container Apps Job executions."
            }))
            .into_response();
        }
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "job_id": job_id,
                    "job_name": job_name,
                    "executions": [],
                    "source": "azure_arm_unavailable",
                    "artifacts": job_artifact_paths(&job),
                    "detail": error
                })),
            )
                .into_response();
        }
    };
    let result = tokio::task::spawn_blocking(move || client.list_executions(&job_name))
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result);
    match result {
        Ok(executions) => Json(json!({
            "job_id": job_id,
            "job_name": job["job_name"],
            "executions": executions.iter().map(execution_summary).collect::<Vec<_>>(),
            "source": "azure_arm_executions",
            "artifacts": job_artifact_paths(&job),
            "live_trading_enabled": false,
            "raw_data_mutated": false
        }))
        .into_response(),
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "job_id": job_id,
                "job_name": job["job_name"],
                "executions": [],
                "source": "azure_arm_error",
                "artifacts": job_artifact_paths(&job),
                "detail": error,
                "live_trading_enabled": false,
                "raw_data_mutated": false
            })),
        )
            .into_response(),
    }
}

pub(crate) async fn job_execution_logs(
    Path((job_id, execution_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(job) = job_definition_by_id(&job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("Job {job_id} was not found.") })),
        )
            .into_response();
    };
    let Some(job_name) = job["job_name"].as_str().map(str::to_owned) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "detail": format!("Job {job_id} has no job_name.") })),
        )
            .into_response();
    };
    let client = match AzureLogAnalyticsClient::from_env() {
        Ok(Some(client)) => client,
        Ok(None) => {
            return Json(json!({
                "job_id": job_id,
                "job_name": job_name,
                "execution_id": execution_id,
                "logs": [],
                "log_rows": [],
                "source": "log_analytics_not_configured",
                "artifacts": job_artifact_paths(&job),
                "detail": "Log Analytics workspace is not configured. Set AZURE_LOG_ANALYTICS_WORKSPACE_ID to retrieve Container Apps Job logs."
            }))
            .into_response();
        }
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "job_id": job_id,
                    "job_name": job_name,
                    "execution_id": execution_id,
                    "logs": [],
                    "log_rows": [],
                    "source": "log_analytics_unavailable",
                    "artifacts": job_artifact_paths(&job),
                    "detail": error
                })),
            )
                .into_response();
        }
    };
    let lookup_execution = execution_id.clone();
    let result =
        tokio::task::spawn_blocking(move || client.execution_logs(&job_name, &lookup_execution))
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result);
    match result {
        Ok(payload) => {
            let safe_payload = redact_sensitive_json(&payload);
            let rows = log_rows_from_analytics(&safe_payload);
            Json(json!({
                "job_id": job_id,
                "job_name": job["job_name"],
                "execution_id": execution_id,
                "logs": rows.iter().filter_map(|row| row["message"].as_str().map(str::to_owned)).collect::<Vec<_>>(),
                "log_rows": rows,
                "source": "azure_log_analytics",
                "artifacts": job_artifact_paths(&job),
                "raw": safe_payload,
                "live_trading_enabled": false,
                "raw_data_mutated": false
            }))
            .into_response()
        }
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "job_id": job_id,
                "job_name": job["job_name"],
                "execution_id": execution_id,
                "logs": [],
                "log_rows": [],
                "source": "azure_log_analytics_error",
                "artifacts": job_artifact_paths(&job),
                "detail": error,
                "live_trading_enabled": false,
                "raw_data_mutated": false
            })),
        )
            .into_response(),
    }
}

pub(crate) async fn data_quality_timeline() -> impl IntoResponse {
    let latest = read_json_or_null(FRESHNESS_LATEST);
    let mut events = Vec::new();
    if !latest.is_null() {
        let ts = latest["generated_at"]
            .as_str()
            .or_else(|| latest["generated_ts"].as_str())
            .map(str::to_owned)
            .unwrap_or_else(now_ts);
        events.push(json!({
            "ts": ts,
            "kind": "freshness",
            "status": latest["result"]["status"].as_str().or_else(|| latest["status"].as_str()).unwrap_or("unknown"),
            "title": "Latest freshness check",
            "detail": latest
        }));
    }
    for audit in list_json_files(&PathBuf::from(REPORT_ROOT), "data_audit.json", 200) {
        let ts = audit["generated_at"]
            .as_str()
            .or_else(|| audit["generated_ts"].as_str())
            .map(str::to_owned)
            .unwrap_or_else(now_ts);
        events.push(json!({
            "ts": ts,
            "kind": "quality_audit",
            "status": audit["result"]["status"].as_str().or_else(|| audit["status"].as_str()).unwrap_or("unknown"),
            "title": "Data quality audit",
            "detail": audit
        }));
    }
    events.sort_by(|left, right| right["ts"].as_str().cmp(&left["ts"].as_str()));
    Json(json!({ "events": events, "generated_ts": now_ts() })).into_response()
}

async fn start_freshness_check() -> impl IntoResponse {
    start_research_job_by_id("freshness-check", None).await
}

async fn start_daily_report() -> impl IntoResponse {
    start_research_job_by_id("daily-research-report", None).await
}

async fn start_prospective_validation() -> impl IntoResponse {
    start_research_job_by_id("prospective-validation", None).await
}

async fn start_replay_index() -> impl IntoResponse {
    start_research_job_by_id("compact-replay-index", None).await
}

async fn start_backfill(body: Option<Json<StartJobRequest>>) -> impl IntoResponse {
    start_research_job_by_id("manual-backfill", body.map(|body| body.0)).await
}

async fn start_job(
    Path(job_id): Path<String>,
    body: Option<Json<StartJobRequest>>,
) -> impl IntoResponse {
    start_research_job_by_id(&job_id, body.map(|body| body.0)).await
}

async fn prospective() -> impl IntoResponse {
    Json(prospective_payload())
}

fn prospective_payload() -> Value {
    let payload = read_json_or_null(
        PathBuf::from(REPORT_ROOT)
            .join("prospective")
            .join("prospective_validation.json"),
    );
    if !payload.is_null() {
        return payload;
    }
    json!({
        "generated_ts": now_ts(),
        "result": {
            "status": "collecting",
            "rows": [],
            "frozen_candidates": frozen_candidates_payload(),
            "research_only": true,
            "live_deployment_allowed": false
        }
    })
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

#[derive(Deserialize, Clone, Debug, Default)]
struct StartJobRequest {
    start: Option<String>,
    end: Option<String>,
    task: Option<String>,
}

async fn start_research_job_by_id(
    job_id: &str,
    body: Option<StartJobRequest>,
) -> axum::response::Response {
    let Some(job) = job_definition_by_id(job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("Job {job_id} was not found.") })),
        )
            .into_response();
    };
    let Some(job_name) = job["job_name"].as_str().map(str::to_owned) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "detail": format!("Job {job_id} has no job_name.") })),
        )
            .into_response();
    };
    if job["runnable"].as_bool() == Some(false) {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "job_id": job_id,
                "job_name": job_name,
                "status": "not_configured",
                "research_only": true,
                "live_trading_enabled": false,
                "raw_data_mutated": false,
                "detail": job["detail"].as_str().unwrap_or("Job is not configured to run.")
            })),
        )
            .into_response();
    }
    let options = match start_options(job_id, body.as_ref()) {
        Ok(options) => options,
        Err(detail) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "detail": detail }))).into_response()
        }
    };
    let client = match AzureJobClient::from_env() {
        Ok(Some(client)) => client,
        Ok(None) => {
            return (
                StatusCode::ACCEPTED,
                Json(json!({
                    "job_id": job_id,
                    "job_name": job_name,
                    "status": "defined_in_iac_not_started_by_api",
                    "created_ts": now_ts(),
                    "research_only": true,
                    "live_trading_enabled": false,
                    "raw_data_mutated": false,
                    "detail": "Azure ARM env is not configured. Start the Container Apps Job from Azure or configure AZURE_SUBSCRIPTION_ID and AZURE_RESOURCE_GROUP."
                })),
            )
                .into_response();
        }
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(
                    json!({ "detail": format!("Azure job client initialization failed: {error}") }),
                ),
            )
                .into_response();
        }
    };
    let result = tokio::task::spawn_blocking(move || client.start_job(&job_name, options))
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result);
    match result {
        Ok(result) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "job_id": job_id,
                "job_name": job["job_name"].as_str(),
                "status": "start_requested",
                "created_ts": now_ts(),
                "research_only": true,
                "live_trading_enabled": false,
                "raw_data_mutated": false,
                "azure": result
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "job_id": job_id,
                "job_name": job["job_name"].as_str(),
                "status": "start_failed",
                "error": error,
                "research_only": true,
                "live_trading_enabled": false,
                "raw_data_mutated": false
            })),
        )
            .into_response(),
    }
}

fn read_latest_report_payload() -> Value {
    if let Some(date) = latest_daily_date() {
        return daily_report_payload(&date);
    }
    let root = PathBuf::from(REPORT_ROOT);
    let root_report = json!({
        "report": read_json_or_null(root.join("final_report.json")),
        "audit": read_json_or_null(root.join("data_audit.json")),
        "baseline": read_json_or_null(root.join("baseline.json")),
        "regimes": read_json_or_null(root.join("regimes.json")),
        "calibration": read_json_or_null(root.join("calibration.json")),
        "sample_size": read_json_or_null(root.join("sample_size.json")),
        "execution_quality": read_json_or_null(root.join("execution_quality.json")),
        "artifacts": artifacts_for_prefix("")
    });
    if root_report["report"].is_object()
        || root_report["audit"].is_object()
        || root_report["baseline"].is_object()
        || root_report["regimes"].is_object()
        || root_report["calibration"].is_object()
        || root_report["sample_size"].is_object()
    {
        return root_report;
    }
    let latest = read_json_or_null(PathBuf::from(REPORT_ROOT).join("latest_daily_report.json"));
    if !latest.is_null() {
        return json!({
            "report": latest,
            "latest": true,
            "artifacts": artifacts_for_prefix("")
        });
    }
    json!({
        "report": Value::Null,
        "detail": "No research daily report exists yet.",
        "artifacts": artifacts_for_prefix("")
    })
}

pub(crate) fn daily_report_payload(date: &str) -> Value {
    let dir = PathBuf::from(REPORT_ROOT).join("daily").join(date);
    json!({
        "date": date,
        "report": read_json_or_null(dir.join("final_report.json")),
        "audit": read_json_or_null(dir.join("data_audit.json")),
        "baseline": read_json_or_null(dir.join("baseline.json")),
        "regimes": read_json_or_null(dir.join("regimes.json")),
        "calibration": read_json_or_null(dir.join("calibration.json")),
        "sample_size": read_json_or_null(dir.join("sample_size.json")),
        "execution_quality": read_json_or_null(dir.join("execution_quality.json")),
        "artifacts": artifacts_for_prefix(&format!("daily/{date}"))
    })
}

fn latest_named_report(primary: &str, fallback: &str) -> Value {
    let Some(date) = latest_daily_date() else {
        let root = PathBuf::from(REPORT_ROOT);
        let report = read_json_or_null(root.join(primary));
        let report = if report.is_null() {
            read_json_or_null(root.join(fallback))
        } else {
            report
        };
        return if report.is_null() {
            json!({ "report": Value::Null, "detail": "No daily report exists yet." })
        } else {
            json!({
                "report": report,
                "source": format!("{REPORT_ROOT}/{primary}")
            })
        };
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
    if let Some(date) = latest_blob_daily_date() {
        return Some(date);
    }
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

fn candidate_evidence_rows(prospective: &Value) -> Vec<Value> {
    let candidates = candidate_config_rows();
    let latest = prospective["result"]["rows"]
        .as_array()
        .and_then(|rows| rows.last());
    candidates
        .into_iter()
        .map(|candidate| candidate_evidence_row(candidate, latest))
        .collect()
}

fn candidate_config_rows() -> Vec<Value> {
    let payload = frozen_candidates_payload();
    let Some(candidates) = payload["candidates"].as_array() else {
        return FROZEN_CANDIDATE_NAMES
            .iter()
            .map(|name| json!({ "name": name, "profile": name }))
            .collect();
    };
    candidates
        .iter()
        .map(|candidate| {
            if candidate.is_object() {
                candidate.clone()
            } else {
                json!({ "name": candidate.as_str().unwrap_or("candidate"), "profile": candidate.as_str().unwrap_or("candidate") })
            }
        })
        .collect()
}

fn candidate_evidence_row(candidate: Value, latest: Option<&Value>) -> Value {
    let name = candidate["name"]
        .as_str()
        .or_else(|| candidate["profile"].as_str())
        .unwrap_or("candidate");
    let pnl = latest
        .map(|row| candidate_pnl(row, name))
        .unwrap_or_else(|| json!("collecting"));
    let paired_delta = latest
        .map(|row| candidate_paired_delta(row, name))
        .unwrap_or(Value::Null);
    let status = candidate_status(latest, name, &pnl);
    let recommendation = latest
        .and_then(|row| row["recommendation"].as_str())
        .unwrap_or_else(|| recommendation_for_latest(latest))
        .to_owned();
    json!({
        "candidate": name,
        "profile": candidate["profile"].clone(),
        "candidate_version": candidate["candidate_version"].clone(),
        "config_hash": candidate["config_hash"].clone(),
        "frozen_since": candidate["frozen_since"].clone(),
        "reason": candidate["reason"].clone(),
        "status": status,
        "latest_test_pnl": pnl,
        "paired_delta": paired_delta,
        "decision_gate": latest.map(|row| candidate_decision_gate(row, name)).unwrap_or(Value::Null),
        "ci_95_low": latest.map(|row| row["ci_95_low"].clone()).unwrap_or(Value::Null),
        "ci_95_high": latest.map(|row| row["ci_95_high"].clone()).unwrap_or(Value::Null),
        "max_drawdown": latest.map(|row| row["max_drawdown"].clone()).unwrap_or(Value::Null),
        "fill_model_agreement": latest.and_then(|row| row["fill_model"].as_str()).unwrap_or("pending sensitivity"),
        "data_quality": latest.and_then(|row| row["data_quality_status"].as_str()).unwrap_or("unknown"),
        "recommendation": recommendation,
        "last_updated": latest.and_then(|row| row["date"].as_str()).unwrap_or("not run"),
        "explanation": candidate_explanation(name, &status, &recommendation, latest),
        "notes": candidate["notes"].clone(),
        "research_only": true,
        "enabled_by_default": false,
        "deployment_allowed": false,
        "live_deployment_allowed": false
    })
}

fn candidate_pnl(row: &Value, candidate: &str) -> Value {
    if candidate.contains("dynamic_quote_style") {
        row["dynamic_quote_style_net_pnl"].clone()
    } else if candidate.contains("full_deterministic_profile") {
        row["full_deterministic_profile_net_pnl"].clone()
    } else if candidate.contains("dynamic_safety_only") {
        row["dynamic_safety_only_net_pnl"].clone()
    } else {
        row["static_net_pnl"].clone()
    }
}

fn candidate_paired_delta(row: &Value, candidate: &str) -> Value {
    if candidate.contains("dynamic_quote_style") {
        row["dynamic_quote_style_paired_delta"].clone()
    } else if candidate.contains("full_deterministic_profile") {
        row["full_deterministic_profile_paired_delta"].clone()
    } else if candidate.contains("dynamic_safety_only") {
        row["dynamic_safety_only_paired_delta"].clone()
    } else {
        json!("baseline")
    }
}

fn candidate_decision_gate(row: &Value, candidate: &str) -> Value {
    if candidate.contains("dynamic_quote_style") {
        row["dynamic_quote_style_decision_gate"].clone()
    } else if candidate.contains("full_deterministic_profile") {
        row["full_deterministic_profile_decision_gate"].clone()
    } else if candidate.contains("dynamic_safety_only") {
        row["dynamic_safety_only_decision_gate"].clone()
    } else {
        json!("BASELINE_CONTROL")
    }
}

fn candidate_status(latest: Option<&Value>, candidate: &str, pnl: &Value) -> String {
    let Some(row) = latest else {
        return "collecting".to_owned();
    };
    if candidate_decision_gate(row, candidate).as_str() == Some("REJECT")
        && !candidate.contains("static")
        && !candidate.contains("baseline")
    {
        return "rejected_by_paired_evidence".to_owned();
    }
    if row["data_quality_status"]
        .as_str()
        .is_some_and(|status| status != "healthy")
    {
        return "blocked".to_owned();
    }
    let best = [
        "static_net_pnl",
        "dynamic_quote_style_net_pnl",
        "full_deterministic_profile_net_pnl",
        "dynamic_safety_only_net_pnl",
    ]
    .iter()
    .filter_map(|field| number_value(&row[*field]))
    .fold(f64::NEG_INFINITY, f64::max);
    if best.is_finite()
        && number_value(pnl).is_some_and(|value| (value - best).abs() < f64::EPSILON)
    {
        "candidate_leader".to_owned()
    } else if candidate.contains("static") {
        "baseline".to_owned()
    } else {
        "needs_more_evidence".to_owned()
    }
}

fn recommendation_for_latest(latest: Option<&Value>) -> &'static str {
    let Some(row) = latest else {
        return "collect more settled markets";
    };
    let low = number_value(&row["ci_95_low"]);
    let high = number_value(&row["ci_95_high"]);
    match (low, high) {
        (Some(low), Some(high)) if low > 0.0 && high > 0.0 => {
            "candidate positive under current evidence"
        }
        (Some(low), Some(high)) if low < 0.0 && high < 0.0 => {
            "candidate negative under current evidence"
        }
        _ => "evidence inconclusive; continue paper validation",
    }
}

fn candidate_explanation(
    name: &str,
    status: &str,
    recommendation: &str,
    latest: Option<&Value>,
) -> String {
    let Some(row) = latest else {
        return format!("{name} has no prospective validation row yet. Run prospective validation before using it for research conclusions.");
    };
    if status == "blocked" {
        return format!(
            "{name} is blocked by {} data quality. Do not trust this candidate until quality is resolved.",
            row["data_quality_status"].as_str().unwrap_or("unknown")
        );
    }
    format!(
        "{name} is {status}. Recommendation: {recommendation}. Evidence uses {} settled markets, drawdown {}, and CI [{}, {}].",
        compact_value(&row["settled_markets"]),
        compact_value(&row["max_drawdown"]),
        compact_value(&row["ci_95_low"]),
        compact_value(&row["ci_95_high"])
    )
}

fn candidate_artifacts() -> Vec<Value> {
    artifacts_for_prefix("")
        .into_iter()
        .filter(|artifact| {
            artifact["path"].as_str().is_some_and(|path| {
                path.contains("prospective")
                    || path.contains("sample_size")
                    || path.contains("final_report")
                    || path.contains("baseline")
                    || path.contains("regimes")
            })
        })
        .take(50)
        .collect()
}

fn number_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
}

fn compact_value(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn list_json_files(root: &FsPath, file_name: &str, limit: usize) -> Vec<Value> {
    if let Some(values) = list_blob_json_files(root, file_name, limit) {
        return values;
    }
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
    if let Some(artifacts) = blob_artifacts_for_prefix(prefix) {
        return artifacts;
    }
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
        if !matches!(extension, "json" | "md" | "csv" | "parquet") {
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

fn artifact_payload(path: &FsPath) -> Result<Option<Value>, String> {
    let relative = report_relative_path(path);
    let text = if let Some(bytes) = read_blob_bytes_for_path(path) {
        String::from_utf8(bytes).map_err(|error| error.to_string())?
    } else {
        match fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.to_string()),
        }
    };
    match FsPath::new(&relative)
        .extension()
        .and_then(|value| value.to_str())
    {
        Some("json") => {
            let json: Value = serde_json::from_str(&text).map_err(|error| error.to_string())?;
            Ok(Some(
                json!({ "path": relative, "kind": "json", "content": json }),
            ))
        }
        Some("csv") => Ok(Some(json!({
            "path": relative,
            "kind": "csv",
            "content": text.lines().take(200).collect::<Vec<_>>().join("\n"),
            "truncated": text.lines().count() > 200
        }))),
        Some("parquet") => Ok(Some(json!({
            "path": relative,
            "kind": "parquet_metadata",
            "content": {
                "size_bytes": fs::metadata(path).ok().map(|metadata| metadata.len()),
                "preview": "Parquet binary preview is metadata-only in the API."
            }
        }))),
        _ => Ok(Some(
            json!({ "path": relative, "kind": "markdown", "content": text }),
        )),
    }
}

fn read_json_or_null(path: impl AsRef<FsPath>) -> Value {
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

fn read_blob_bytes_for_path(path: &FsPath) -> Option<Vec<u8>> {
    let blob_name = blob_name_for_path(path)?;
    read_blob_bytes(&blob_name)
}

fn read_blob_bytes(blob_name: &str) -> Option<Vec<u8>> {
    let mut client = artifact_blob_client()?;
    match client.download_blob_bytes(blob_name) {
        Ok(bytes) => Some(bytes),
        Err(AzureBlobError::HttpStatus(404)) => None,
        Err(_) => None,
    }
}

fn list_blob_json_files(root: &FsPath, file_name: &str, limit: usize) -> Option<Vec<Value>> {
    let mut client = artifact_blob_client()?;
    let mut prefix = blob_name_for_path(root)?;
    if !prefix.ends_with('/') {
        prefix.push('/');
    }
    let blobs = client
        .list_blobs_by_suffixes(&prefix, &[file_name], Some(limit), Some(32 * 1024 * 1024))
        .ok()?;
    let mut values = Vec::new();
    for blob in blobs {
        if values.len() >= limit {
            break;
        }
        let bytes = read_blob_bytes(&blob.name)?;
        values.push(serde_json::from_slice(&bytes).unwrap_or(Value::Null));
    }
    Some(values)
}

fn blob_artifacts_for_prefix(prefix: &str) -> Option<Vec<Value>> {
    let mut client = artifact_blob_client()?;
    let mut blob_prefix = REPORT_ROOT.to_owned();
    let clean_prefix = prefix.trim().trim_start_matches('/').trim_end_matches('/');
    if !clean_prefix.is_empty() {
        blob_prefix.push('/');
        blob_prefix.push_str(clean_prefix);
    }
    if !blob_prefix.ends_with('/') {
        blob_prefix.push('/');
    }
    let blobs = client
        .list_blobs_by_suffixes(&blob_prefix, &[".json", ".md"], Some(1000), None)
        .ok()?;
    let mut artifacts = blobs
        .into_iter()
        .filter_map(|blob| {
            let relative = blob.name.strip_prefix(&format!("{REPORT_ROOT}/"))?.to_owned();
            let extension = FsPath::new(&relative)
                .extension()
                .and_then(|value| value.to_str())?;
            Some(json!({
                "artifact_id": relative.replace('/', "~"),
                "path": relative,
                "kind": extension,
                "size_bytes": blob.content_length,
                "modified_ts": blob.last_modified.map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Secs, true))
            }))
        })
        .collect::<Vec<_>>();
    artifacts.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));
    Some(artifacts)
}

fn latest_blob_daily_date() -> Option<String> {
    let mut client = artifact_blob_client()?;
    let blobs = client
        .list_blobs_by_suffixes(
            "reports/research/daily/",
            &["final_report.json"],
            Some(1000),
            None,
        )
        .ok()?;
    let mut dates = blobs
        .into_iter()
        .filter_map(|blob| {
            let relative = blob.name.strip_prefix("reports/research/daily/")?;
            let date = relative.split('/').next()?.to_owned();
            valid_date(&date).then_some(date)
        })
        .collect::<Vec<_>>();
    dates.sort();
    dates.pop()
}

fn blob_name_for_path(path: &FsPath) -> Option<String> {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let trimmed = normalized.trim_start_matches("./").trim_start_matches('/');
    let allowed = ["reports/research/", "data_quality/freshness/"];
    if allowed.iter().any(|prefix| trimmed.starts_with(prefix)) {
        return Some(trimmed.to_owned());
    }
    None
}

fn report_relative_path(path: &FsPath) -> String {
    path.strip_prefix(REPORT_ROOT)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_owned()
}

fn job_artifact_paths(job: &Value) -> Vec<Value> {
    vec![
        job["output_artifact"].clone(),
        json!(format!(
            "reports/jobs/latest/{}.json",
            job["job_id"].as_str().unwrap_or("job")
        )),
    ]
    .into_iter()
    .filter(|value| !value.is_null())
    .collect()
}

fn log_rows_from_analytics(payload: &Value) -> Vec<Value> {
    let Some(table) = payload["tables"]
        .as_array()
        .and_then(|tables| tables.first())
    else {
        return Vec::new();
    };
    let columns = table["columns"]
        .as_array()
        .map(|columns| {
            columns
                .iter()
                .filter_map(|column| column["name"].as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    table["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let values = row.as_array()?;
            let mut record = serde_json::Map::new();
            for (index, value) in values.iter().enumerate() {
                let key = columns
                    .get(index)
                    .map(|value| value.to_ascii_lowercase())
                    .unwrap_or_else(|| format!("column_{index}"));
                record.insert(key, redact_sensitive_json(value));
            }
            let ts = record
                .get("timegenerated")
                .cloned()
                .or_else(|| record.get("timestamp").cloned())
                .unwrap_or(Value::Null);
            let message = record
                .get("message")
                .cloned()
                .or_else(|| record.get("log").cloned())
                .unwrap_or(Value::Null);
            Some(json!({
                "ts": ts,
                "level": record.get("level").cloned().unwrap_or(Value::Null),
                "message": message,
                "replica": record.get("replica").cloned().unwrap_or(Value::Null),
                "container": record.get("container").cloned().unwrap_or(Value::Null),
                "raw": Value::Object(record)
            }))
        })
        .collect()
}

fn redact_sensitive_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| {
                    if is_secret_like_text(key) {
                        (key.clone(), json!("[redacted]"))
                    } else {
                        (key.clone(), redact_sensitive_json(value))
                    }
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact_sensitive_json).collect()),
        Value::String(text) => json!(redact_sensitive_text(text)),
        _ => value.clone(),
    }
}

fn redact_sensitive_text(text: &str) -> String {
    let mut redact_next = false;
    text.split_whitespace()
        .map(|part| {
            let lowered = part.to_ascii_lowercase();
            let redact = redact_next
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
                || lowered.contains("-----begin")
                || is_secret_like_text(&lowered);
            let mark_next = lowered == "bearer" || lowered.ends_with("bearer");
            if redact {
                redact_next = mark_next;
                "[redacted]"
            } else {
                redact_next = mark_next;
                part
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_secret_like_text(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("secret")
        || lower.contains("password")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("bearer")
        || lower.contains("authorization")
        || lower.contains("private_key")
        || lower.contains("account_key")
        || lower.contains("connection_string")
        || lower.contains("access_token")
        || lower.contains("refresh_token")
        || lower.contains("sas_token")
}

pub(crate) fn job_definitions() -> Value {
    json!([
        job_definition(
            "freshness-check",
            "polyedge-data-freshness-job",
            "Schedule",
            "*/5 * * * *"
        ),
        job_definition(
            "hourly-quality-audit",
            "polyedge-hourly-quality-job",
            "Schedule",
            "10 * * * *"
        ),
        job_definition(
            "daily-research-report",
            "polyedge-daily-research-job",
            "Schedule",
            "30 0 * * *"
        ),
        job_definition(
            "prospective-validation",
            "polyedge-prospective-job",
            "Schedule",
            "15 1 * * *"
        ),
        job_definition(
            "compact-replay-index",
            "polyedge-replay-index-job",
            "Schedule",
            "0 2 * * *"
        ),
        job_definition(
            "chart-backfill",
            "polyedge-chart-backfill-job",
            "Manual",
            Value::Null
        ),
        job_definition(
            "adx-ingestion",
            "polyedge-adx-ingestion-job",
            "Schedule",
            "15 * * * *"
        ),
        job_definition(
            "manual-backfill",
            "polyedge-backfill-job",
            "Manual",
            Value::Null
        ),
    ])
}

fn job_definition_by_id(job_id: &str) -> Option<Value> {
    let canonical = match job_id {
        "daily-report" => "daily-research-report",
        "hourly-quality" => "hourly-quality-audit",
        "replay-index" => "compact-replay-index",
        "backfill" => "manual-backfill",
        other => other,
    };
    job_definitions()
        .as_array()
        .into_iter()
        .flatten()
        .find(|job| {
            job["job_id"].as_str() == Some(canonical) || job["job_name"].as_str() == Some(canonical)
        })
        .cloned()
}

fn enrich_jobs_with_azure(jobs: &mut Value, client: &AzureJobClient) {
    let Some(jobs) = jobs.as_array_mut() else {
        return;
    };
    for job in jobs {
        let Some(job_name) = job["job_name"].as_str().map(str::to_owned) else {
            continue;
        };
        let latest = client
            .list_executions(&job_name)
            .ok()
            .and_then(|executions| latest_execution_summary(&executions));
        let Some(latest) = latest else {
            continue;
        };
        let Some(object) = job.as_object_mut() else {
            continue;
        };
        object.insert(
            "status".to_owned(),
            latest["status"].as_str().unwrap_or("unknown").into(),
        );
        object.insert("last_start".to_owned(), latest["last_start"].clone());
        object.insert("last_finish".to_owned(), latest["last_finish"].clone());
        object.insert("duration".to_owned(), latest["duration"].clone());
        object.insert("exit_code".to_owned(), latest["exit_code"].clone());
        object.insert("error".to_owned(), latest["error"].clone());
        object.insert("running".to_owned(), latest["running"].clone());
        object.insert(
            "execution_name".to_owned(),
            latest["execution_name"].clone(),
        );
        object.insert("execution_id".to_owned(), latest["execution_id"].clone());
    }
}

fn start_options(
    job_id: &str,
    request: Option<&StartJobRequest>,
) -> Result<Option<JobStartOptions>, String> {
    if job_id != "manual-backfill" && job_id != "backfill" {
        return Ok(None);
    }
    let request = request.ok_or_else(|| "Backfill requires start, end, and task.".to_owned())?;
    let start = request
        .start
        .as_deref()
        .filter(|value| valid_date(value))
        .ok_or_else(|| "Backfill start must be YYYY-MM-DD.".to_owned())?;
    let end = request
        .end
        .as_deref()
        .filter(|value| valid_date(value))
        .ok_or_else(|| "Backfill end must be YYYY-MM-DD.".to_owned())?;
    let task = request
        .task
        .as_deref()
        .filter(|value| {
            matches!(
                *value,
                "all" | "audit" | "daily-report" | "replay-index" | "prospective"
            )
        })
        .ok_or_else(|| {
            "Backfill task must be all, audit, daily-report, replay-index, or prospective."
                .to_owned()
        })?;
    if end < start {
        return Err("Backfill end must be on or after start.".to_owned());
    }
    Ok(Some(JobStartOptions {
        env: vec![
            ("BACKFILL_START".to_owned(), start.to_owned()),
            ("BACKFILL_END".to_owned(), end.to_owned()),
            ("BACKFILL_TASK".to_owned(), task.to_owned()),
        ],
    }))
}

fn job_definition(job_id: &str, job_name: &str, trigger: &str, cron: impl Into<Value>) -> Value {
    let runnable = job_id != "adx-ingestion" || adx_ingestion_configured();
    let detail = if job_id == "adx-ingestion" && !runnable {
        Value::String(
            "ADX ingestion is hidden from run controls until ADX_CLUSTER_URI and ADX_DATABASE are configured."
                .to_owned(),
        )
    } else {
        Value::Null
    };
    json!({
        "job_id": job_id,
        "job_type": job_id,
        "job_name": job_name,
        "status": "defined_in_iac",
        "trigger": trigger,
        "cron": cron.into(),
        "last_start": Value::Null,
        "last_finish": Value::Null,
        "duration": Value::Null,
        "exit_code": Value::Null,
        "output_artifact": job_output_artifact(job_id),
        "error": Value::Null,
        "running": false,
        "runnable": runnable,
        "detail": detail,
        "research_only": true,
        "live_trading_enabled": false,
        "data_quality": "unknown"
    })
}

fn adx_ingestion_configured() -> bool {
    env::var("ADX_CLUSTER_URI")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .is_some()
        && env::var("ADX_DATABASE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .is_some()
}

fn job_output_artifact(job_id: &str) -> &'static str {
    match job_id {
        "freshness-check" => FRESHNESS_LATEST,
        "daily-research-report" => "reports/research/latest_daily_report.json",
        "prospective-validation" => "reports/research/prospective/prospective_validation.json",
        "compact-replay-index" => "data/research/replay-index/<date>/index_manifest.json",
        "manual-backfill" => "reports/research/backfill/<start>-<end>-<task>.json",
        "chart-backfill" => "reports/jobs/latest/chart-backfill.json",
        "adx-ingestion" => "reports/jobs/latest/adx-ingestion.json",
        "hourly-quality-audit" => "reports/research/hourly/<yyyy/mm/dd/hh>/audit.json",
        _ => "reports/jobs/latest/unknown.json",
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daily_report_payload_reads_reports_research_daily_contract() {
        let date = "2099-12-31";
        let dir = PathBuf::from(REPORT_ROOT).join("daily").join(date);
        let _guard = CleanupPath(dir.clone());
        fs::create_dir_all(&dir).expect("daily report dir");
        fs::write(
            dir.join("sample_size.json"),
            r#"{"result":{"statistics":{"n":67}}}"#,
        )
        .expect("sample size");
        fs::write(
            dir.join("final_report.json"),
            r#"{"result":{"executive_summary":{"recommendation":"collect"}}}"#,
        )
        .expect("final report");

        let payload = daily_report_payload(date);

        assert_eq!(payload["date"], date);
        assert_eq!(payload["sample_size"]["result"]["statistics"]["n"], 67);
        assert_eq!(
            payload["report"]["result"]["executive_summary"]["recommendation"],
            "collect"
        );
    }

    #[test]
    fn log_analytics_rows_redact_secret_like_content() {
        let payload = json!({
            "tables": [{
                "columns": [
                    {"name": "TimeGenerated"},
                    {"name": "Message"},
                    {"name": "Authorization"},
                    {"name": "ConnectionString"}
                ],
                "rows": [[
                    "2026-06-24T00:00:00Z",
                    "starting Bearer token-value https://example.blob.core.windows.net/c?sp=rl&se=2026&sig=hidden",
                    "Bearer other-token",
                    "AccountKey=hidden-key"
                ]]
            }]
        });

        let safe = redact_sensitive_json(&payload);
        let text = serde_json::to_string(&safe).unwrap();

        assert!(!text.contains("token-value"));
        assert!(!text.contains("other-token"));
        assert!(!text.contains("sig=hidden"));
        assert!(!text.contains("hidden-key"));
        assert!(text.contains("[redacted]"));
    }

    #[test]
    fn adx_job_is_not_runnable_without_config() {
        std::env::remove_var("ADX_CLUSTER_URI");
        std::env::remove_var("ADX_DATABASE");
        let job = job_definition_by_id("adx-ingestion").expect("adx job");

        assert_eq!(job["runnable"], false);
        assert!(job["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("not configured") || detail.contains("hidden")));
    }

    struct CleanupPath(PathBuf);

    impl Drop for CleanupPath {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}
