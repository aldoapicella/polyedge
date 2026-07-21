use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use polyedge_reporting::research::{
    daily_provenance_required, load_exclusion_registry, load_frozen_candidate_registry,
    load_shadow_campaign_contract, read_shadow_correction_state, DailyRunManifest,
    LatestRunPointer, PromotionManifestV1, PromotionPhase, RunStatus,
    ShadowCampaignContractBinding, ShadowCorrectionState, DEFAULT_EXCLUSION_FILE,
    DEFAULT_FROZEN_CANDIDATES_FILE, FROZEN_CANDIDATE_NAMES,
};
use polyedge_storage::{AzureBlobClient, AzureBlobError};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path as FsPath, PathBuf};

use crate::azure_jobs::{
    execution_summary, latest_execution_summary, AzureJobClient, AzureLogAnalyticsClient,
    JobStartOptions,
};
use crate::ApiState;

const REPORT_ROOT: &str = "reports/research";
#[cfg(test)]
const PRIMARY_DAILY_ROOT: &str = "reports/research/daily";
const SHADOW_DAILY_ROOT: &str = "reports/research/shadow/daily";
const ACTIVE_SHADOW_CAMPAIGN_ID: &str = "campaign-2026-07-22";
const LEGACY_SHADOW_CAMPAIGN_ID: &str = "campaign-2026-07-12";
const ACTIVE_SHADOW_CAMPAIGN_START: &str = "2026-07-22";
const ACTIVE_SHADOW_CAMPAIGN_TERMINAL: &str = "2026-09-19";
const LEGACY_SHADOW_FIRST_DATE: &str = "2026-07-13";
const LEGACY_SHADOW_LAST_DATE: &str = "2026-07-20";
const ACTIVE_SHADOW_DAILY_ROOT: &str =
    "reports/research/shadow/campaigns/campaign-2026-07-22/daily";
const ACTIVE_SHADOW_PROSPECTIVE_PATH: &str =
    "reports/research/shadow/campaigns/campaign-2026-07-22/prospective/prospective_validation.json";
const ACTIVE_SHADOW_PROFITABILITY_LATEST: &str =
    "reports/research/shadow/campaigns/campaign-2026-07-22/profitability/latest.json";
const ACTIVE_SHADOW_CORRECTION_PATH: &str =
    "reports/research/shadow/campaigns/campaign-2026-07-22/corrections/active.json";
const ACTIVE_SHADOW_CAMPAIGN_CONTRACT_PATH: &str =
    "research/configs/profitability_gate_v3_2026-07-22.yaml";
const FRESHNESS_LATEST: &str = "data_quality/freshness/latest.json";
const PROFITABILITY_LATEST: &str = "reports/research/profitability/latest.json";
const PROMOTION_MANIFEST_SCHEMA: &str = "promotion_manifest_v1";
const EVIDENCE_PROTOCOL_VERSION: u64 = 3;
const ARTIFACT_FRESHNESS_SECONDS: i64 = 24 * 60 * 60;

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
    let now = Utc::now();
    let correction = load_shadow_correction_gate();
    let profitability_path = FsPath::new(PROFITABILITY_LATEST);
    let mut profitability_selection = select_profitability_artifact(
        ProfitabilityArtifact::new(
            read_json_from_container_or_null(
                profitability_path,
                "AZURE_FUNDED_STORAGE_CONTAINER_NAME",
            ),
            ProfitabilitySource::Funded,
            now,
        ),
        ProfitabilityArtifact::new(
            read_json_from_container_or_null(
                FsPath::new(ACTIVE_SHADOW_PROFITABILITY_LATEST),
                "AZURE_RESEARCH_STORAGE_CONTAINER_NAME",
            ),
            ProfitabilitySource::Shadow,
            now,
        ),
        now,
    );
    apply_shadow_correction_to_profitability(&mut profitability_selection.value, &correction);
    let latest_path = FsPath::new("reports/research/venue-probe/latest.json");
    let latest = normalize_venue_execution_summary(read_json_from_container_or_null(
        latest_path,
        "AZURE_FUNDED_STORAGE_CONTAINER_NAME",
    ));
    let latest_attempt_path = FsPath::new("reports/research/venue-probe/latest_attempt.json");
    let latest_attempt = read_json_from_container_or_null(
        latest_attempt_path,
        "AZURE_FUNDED_STORAGE_CONTAINER_NAME",
    );
    let preflight_path =
        FsPath::new("reports/research/venue-probe/latest_authenticated_dry_run.json");
    let preflight =
        read_json_from_container_or_null(preflight_path, "AZURE_FUNDED_STORAGE_CONTAINER_NAME");
    let redemption_path = FsPath::new("reports/research/venue-probe/latest_redemption.json");
    let redemption =
        read_json_from_container_or_null(redemption_path, "AZURE_FUNDED_STORAGE_CONTAINER_NAME");
    let trained_model_path = FsPath::new("reports/research/venue-probe/effective_queue_model.json");
    let trained_model =
        read_json_from_container_or_null(trained_model_path, "AZURE_MODEL_STORAGE_CONTAINER_NAME");
    let prior_model_path = FsPath::new(
        "reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json",
    );
    let (execution_model, execution_model_source, execution_model_path) = if trained_model.is_null()
    {
        (
            read_json_from_container_or_null(
                prior_model_path,
                "AZURE_RESEARCH_STORAGE_CONTAINER_NAME",
            ),
            "research_conservative_prior",
            prior_model_path,
        )
    } else {
        (trained_model, "trained_model_storage", trained_model_path)
    };
    let artifact_provenance = json!({
        "profitability": profitability_selection.provenance,
        "latest": artifact_provenance(
            &latest,
            latest_path,
            "funded_execution_evidence",
            now,
            Some(venue_legacy_eligibility(&latest)),
        ),
        "latest_attempt": artifact_provenance(
            &latest_attempt,
            latest_attempt_path,
            "funded_execution_evidence",
            now,
            Some(venue_legacy_eligibility(&latest_attempt)),
        ),
        "preflight": artifact_provenance(
            &preflight,
            preflight_path,
            "funded_execution_evidence",
            now,
            Some("not_applicable"),
        ),
        "redemption": artifact_provenance(
            &redemption,
            redemption_path,
            "funded_execution_evidence",
            now,
            Some("not_applicable"),
        ),
        "model": artifact_provenance(
            &execution_model,
            execution_model_path,
            execution_model_source,
            now,
            Some(model_legacy_eligibility(&execution_model)),
        )
    });
    Json(json!({
        "generated_ts": now.to_rfc3339_opts(SecondsFormat::Secs, true),
        "latest": latest,
        "latest_attempt": latest_attempt,
        "preflight": preflight,
        "redemption": redemption,
        "model": execution_model,
        "profitability": profitability_selection.value,
        "correction": correction.as_json(),
        "promotion_decision": correction.decision(),
        "promotion_blocker": correction.blocker,
        "artifact_provenance": artifact_provenance,
        "queue_position_source": "authenticated_lifecycle_plus_public_l2",
        "queue_position_metric": "inferred_size_ahead",
        "literal_fifo_rank_available": false,
        "practical_target": "probability_of_fill_within_1_5_30_60_seconds",
        "remaining_limitation": "Polymarket does not expose exact matching rank, per-order public priority, hidden liquidity, or venue-internal priority changes.",
        "research_only": true,
        "strategy_promotion_allowed": false
    }))
}

#[derive(Clone, Debug)]
struct ShadowCorrectionGate {
    state: Option<ShadowCorrectionState>,
    status: String,
    blocks_promotion: bool,
    blocker: Option<String>,
    validation_error: bool,
}

impl ShadowCorrectionGate {
    fn decision(&self) -> &'static str {
        if self.blocks_promotion {
            "NO-GO"
        } else {
            "ELIGIBILITY_UNCHANGED"
        }
    }

    fn as_json(&self) -> Value {
        json!({
            "journal_path": ACTIVE_SHADOW_CORRECTION_PATH,
            "available": self.state.is_some(),
            "status": self.status,
            "blocks_promotion": self.blocks_promotion,
            "decision": self.decision(),
            "blocker": self.blocker,
            "validation_error": self.validation_error,
            "state": self.state
        })
    }
}

fn load_shadow_correction_gate() -> ShadowCorrectionGate {
    let result = read_shadow_correction_state()
        .map_err(|error| error.to_string())
        .and_then(|state| match state {
            Some(state) if state.campaign_id != ACTIVE_SHADOW_CAMPAIGN_ID => Err(format!(
                "correction journal belongs to {}, not the active campaign",
                state.campaign_id
            )),
            state => Ok(state),
        });
    correction_gate_from_result(result)
}

fn correction_gate_from_result(
    result: Result<Option<ShadowCorrectionState>, String>,
) -> ShadowCorrectionGate {
    match result {
        Ok(Some(state)) if matches!(state.status.as_str(), "in_progress" | "failed") => {
            let blocker = format!(
                "Shadow correction {} is {} for {} through {}. Profitability and promotion are NO-GO until corrected artifacts are atomically republished and the correction journal is complete.",
                state.correction_id, state.status, state.from, state.through
            );
            ShadowCorrectionGate {
                status: state.status.clone(),
                state: Some(state),
                blocks_promotion: true,
                blocker: Some(blocker),
                validation_error: false,
            }
        }
        Ok(Some(state)) if state.status == "complete" => ShadowCorrectionGate {
            status: state.status.clone(),
            state: Some(state),
            blocks_promotion: false,
            blocker: None,
            validation_error: false,
        },
        Ok(Some(state)) => ShadowCorrectionGate {
            status: "invalid".to_owned(),
            state: Some(state),
            blocks_promotion: true,
            blocker: Some(
                "Shadow correction journal has an unsupported status. Profitability and promotion are NO-GO until the journal is repaired and verified."
                    .to_owned(),
            ),
            validation_error: true,
        },
        Ok(None) => ShadowCorrectionGate {
            state: None,
            status: "none".to_owned(),
            blocks_promotion: false,
            blocker: None,
            validation_error: false,
        },
        Err(_) => ShadowCorrectionGate {
            state: None,
            status: "unavailable".to_owned(),
            blocks_promotion: true,
            blocker: Some(
                "Shadow correction journal could not be verified. Profitability and promotion are NO-GO until correction state is readable and valid."
                    .to_owned(),
            ),
            validation_error: true,
        },
    }
}

fn apply_shadow_correction_to_profitability(
    profitability: &mut Value,
    correction: &ShadowCorrectionGate,
) {
    if !correction.blocks_promotion {
        return;
    }
    if !profitability.is_object() {
        *profitability = default_profitability_manifest();
    }
    let Some(object) = profitability.as_object_mut() else {
        return;
    };
    if let Some(phase) = object.get("phase").cloned() {
        object.insert("pre_correction_phase".to_owned(), phase);
    }
    if let Some(status) = object.get("status").cloned() {
        object.insert("pre_correction_status".to_owned(), status);
    }
    object.insert("phase".to_owned(), json!("risk_repair"));
    object.insert("status".to_owned(), json!("correction_blocked_no_go"));
    object.insert("effective_decision".to_owned(), json!("NO-GO"));
    object.insert("promotion_allowed".to_owned(), json!(false));
    object.insert("human_authorization_required".to_owned(), json!(true));
    object.insert(
        "blocking_reason".to_owned(),
        json!(correction
            .blocker
            .as_deref()
            .unwrap_or("Shadow correction state blocks profitability promotion.")),
    );
    if let Some(gate_metrics) = object
        .get_mut("gate_metrics")
        .and_then(Value::as_object_mut)
    {
        gate_metrics.insert("promotion_allowed".to_owned(), json!(false));
        gate_metrics.insert("effective_decision".to_owned(), json!("NO-GO"));
    }
    if let Some(funded_ladder) = object
        .get_mut("funded_ladder")
        .and_then(Value::as_object_mut)
    {
        funded_ladder.insert("promotion_allowed".to_owned(), json!(false));
        funded_ladder.insert("stage_authorized".to_owned(), json!(false));
        funded_ladder.insert("human_grant_required".to_owned(), json!(true));
        funded_ladder.insert("effective_decision".to_owned(), json!("NO-GO"));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProfitabilitySource {
    Funded,
    Shadow,
}

impl ProfitabilitySource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Funded => "funded_evidence",
            Self::Shadow => "profitability_shadow",
        }
    }

    fn trust_scope(self) -> &'static str {
        match self {
            Self::Funded => "funded_control",
            Self::Shadow => "shadow_research",
        }
    }
}

#[derive(Clone, Debug)]
struct ProfitabilityArtifact {
    value: Value,
    source: ProfitabilitySource,
    manifest: Option<PromotionManifestV1>,
    validation_error: Option<String>,
    authoritative_ts: Option<DateTime<Utc>>,
    authoritative_ts_field: Option<&'static str>,
    control_valid: bool,
    fresh: bool,
    canonical_funded_state: bool,
    path: &'static str,
}

impl ProfitabilityArtifact {
    fn new(value: Value, source: ProfitabilitySource, now: DateTime<Utc>) -> Self {
        let (manifest, validation_error) = match validate_profitability_manifest(&value, source) {
            Ok(manifest) => (Some(manifest), None),
            Err(error) => (None, Some(error)),
        };
        let canonical_funded_state = source == ProfitabilitySource::Funded
            && manifest
                .as_ref()
                .is_some_and(|manifest| manifest.funded_ladder.is_some());
        let (authoritative_ts, authoritative_ts_field) = manifest
            .as_ref()
            .map(|manifest| {
                if canonical_funded_state {
                    (
                        manifest
                            .funded_ladder
                            .as_ref()
                            .map(|ladder| ladder.updated_at),
                        Some("funded_ladder.updated_at"),
                    )
                } else {
                    (Some(manifest.created_at), Some("created_at"))
                }
            })
            .unwrap_or_else(|| artifact_timestamp(&value));
        let control_valid = manifest
            .as_ref()
            .is_some_and(|manifest| manifest.expires_at > now && manifest.created_at <= now);
        let fresh = control_valid
            && authoritative_ts.is_some_and(|timestamp| {
                timestamp <= now
                    && now.signed_duration_since(timestamp).num_seconds()
                        <= ARTIFACT_FRESHNESS_SECONDS
            });
        Self {
            value,
            source,
            manifest,
            validation_error,
            authoritative_ts,
            authoritative_ts_field,
            control_valid,
            fresh,
            canonical_funded_state,
            path: match source {
                ProfitabilitySource::Funded => PROFITABILITY_LATEST,
                ProfitabilitySource::Shadow => ACTIVE_SHADOW_PROFITABILITY_LATEST,
            },
        }
    }

    fn available(&self) -> bool {
        self.value.is_object()
    }

    fn valid_current_schema(&self) -> bool {
        self.manifest.is_some()
    }

    fn metadata(&self, now: DateTime<Utc>) -> Value {
        let expires_at = self.manifest.as_ref().map(|manifest| {
            manifest
                .expires_at
                .to_rfc3339_opts(SecondsFormat::Secs, true)
        });
        let age_seconds = self
            .authoritative_ts
            .map(|timestamp| now.signed_duration_since(timestamp).num_seconds().max(0));
        let legacy_eligibility = if self.valid_current_schema() {
            "current_schema"
        } else if self.available() {
            "display_only_legacy"
        } else {
            "unavailable"
        };
        json!({
            "path": self.path,
            "source": self.source.as_str(),
            "trust_scope": self.source.trust_scope(),
            "available": self.available(),
            "schema_version": self.value.get("schema_version").cloned().unwrap_or(Value::Null),
            "valid_current_schema": self.valid_current_schema(),
            "legacy_eligibility": legacy_eligibility,
            "authoritative_ts": self.authoritative_ts.map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)),
            "authoritative_ts_field": self.authoritative_ts_field,
            "age_seconds": age_seconds,
            "freshness_window_seconds": ARTIFACT_FRESHNESS_SECONDS,
            "freshness": if !self.available() { "unavailable" } else if self.authoritative_ts.is_none() { "unknown" } else if self.fresh { "fresh" } else { "stale" },
            "control_valid": self.control_valid,
            "expires_at": expires_at,
            "fresh": self.fresh,
            "expired": self.manifest.as_ref().is_some_and(|manifest| manifest.expires_at <= now),
            "canonical_funded_state": self.canonical_funded_state,
            "promotion_ready": false,
            "validation_error": self.validation_error
        })
    }
}

#[derive(Clone, Debug)]
struct ProfitabilitySelection {
    value: Value,
    provenance: Value,
}

fn select_profitability_artifact(
    funded: ProfitabilityArtifact,
    shadow: ProfitabilityArtifact,
    now: DateTime<Utc>,
) -> ProfitabilitySelection {
    let candidates = [funded, shadow];
    let (selected_index, selection_reason) = if candidates[0].canonical_funded_state {
        (Some(0), "canonical_funded_state")
    } else if candidates[1].available() {
        (Some(1), "active_campaign_shadow")
    } else {
        (None, "awaiting_active_campaign_profitability")
    };
    let candidate_metadata = candidates
        .iter()
        .map(|candidate| candidate.metadata(now))
        .collect::<Vec<_>>();
    let (value, selected_source, canonical_funded_state) = selected_index.map_or_else(
        || (default_profitability_manifest(), "api_fallback", false),
        |index| {
            let selected = &candidates[index];
            (
                fail_closed_profitability_value(
                    selected.value.clone(),
                    selected.canonical_funded_state,
                    selected.valid_current_schema(),
                ),
                selected.source.as_str(),
                selected.canonical_funded_state,
            )
        },
    );
    let selected_metadata = selected_index
        .map(|index| candidates[index].metadata(now))
        .unwrap_or_else(|| {
            json!({
                "path": ACTIVE_SHADOW_PROFITABILITY_LATEST,
                "source": "api_fallback",
                "trust_scope": "none",
                "available": false,
                "valid_current_schema": false,
                "legacy_eligibility": "display_only_fallback",
                "fresh": false,
                "freshness": "unavailable",
                "control_valid": false,
                "canonical_funded_state": false,
                "promotion_ready": false
            })
        });
    ProfitabilitySelection {
        value,
        provenance: json!({
            "selected_source": selected_source,
            "selection_reason": selection_reason,
            "canonical_funded_state": canonical_funded_state,
            "promotion_ready": false,
            "selected": selected_metadata,
            "candidates": candidate_metadata
        }),
    }
}

fn validate_profitability_manifest(
    value: &Value,
    source: ProfitabilitySource,
) -> Result<PromotionManifestV1, String> {
    if !value.is_object() {
        return Err("artifact is unavailable or is not a JSON object".to_owned());
    }
    let manifest: PromotionManifestV1 = serde_json::from_value(value.clone())
        .map_err(|error| format!("artifact does not match {PROMOTION_MANIFEST_SCHEMA}: {error}"))?;
    if manifest.schema_version != PROMOTION_MANIFEST_SCHEMA {
        return Err("unsupported promotion manifest schema".to_owned());
    }
    if source == ProfitabilitySource::Shadow {
        validate_active_shadow_profitability_binding(&manifest.artifact_uris)?;
    }
    if manifest.expires_at <= manifest.created_at {
        return Err("promotion manifest validity window is invalid".to_owned());
    }
    if !manifest.human_authorization_required || manifest.promotion_allowed {
        return Err("profitability artifacts must remain non-executable".to_owned());
    }
    if manifest.candidate.name.trim().is_empty()
        || manifest.candidate.candidate_version.trim().is_empty()
        || !valid_prefixed_sha256(&manifest.candidate.config_hash)
    {
        return Err("candidate identity is incomplete or invalid".to_owned());
    }
    if let Some(ladder) = &manifest.funded_ladder {
        if source != ProfitabilitySource::Funded {
            return Err("shadow storage cannot publish canonical funded ladder state".to_owned());
        }
        ladder
            .validate()
            .map_err(|error| format!("funded ladder is invalid: {error}"))?;
        if ladder.candidate != manifest.candidate
            || ladder.phase != manifest.phase
            || manifest.gate_metrics.phase != PromotionPhase::ShadowPassed
            || !manifest.gate_metrics.promotion_allowed
        {
            return Err("funded ladder identity or phase is inconsistent".to_owned());
        }
    } else if manifest.phase != manifest.gate_metrics.phase
        || !matches!(
            manifest.phase,
            PromotionPhase::Frozen
                | PromotionPhase::RiskRepair
                | PromotionPhase::ShadowCollecting
                | PromotionPhase::ShadowPassed
        )
    {
        return Err("pre-funded artifact claims a funded-only phase".to_owned());
    }
    Ok(manifest)
}

fn validate_active_shadow_profitability_binding(
    artifact_uris: &BTreeMap<String, String>,
) -> Result<(), String> {
    let binding = active_shadow_campaign_contract()?;
    let required = [
        ("campaign_contract", ACTIVE_SHADOW_CAMPAIGN_CONTRACT_PATH),
        ("shadow_campaign_id", binding.contract.campaign_id.as_str()),
        ("shadow_daily_root", binding.contract.daily_root.as_str()),
        (
            "shadow_prospective_result",
            binding.contract.prospective_path.as_str(),
        ),
        (
            "shadow_profitability_result",
            binding.contract.profitability_path.as_str(),
        ),
    ];
    for (key, expected) in required {
        let value = artifact_uris
            .get(key)
            .ok_or_else(|| format!("active campaign binding is missing {key}"))?;
        if value != expected && !value.ends_with(&format!("/{expected}")) {
            return Err(format!("active campaign binding has wrong {key}"));
        }
    }
    let contract_sha = artifact_uris
        .get("campaign_contract_sha256")
        .ok_or_else(|| "active campaign binding is missing campaign_contract_sha256".to_owned())?;
    if contract_sha != &binding.sha256 {
        return Err("active campaign binding has wrong campaign_contract_sha256".to_owned());
    }
    Ok(())
}

fn active_shadow_campaign_contract() -> Result<ShadowCampaignContractBinding, String> {
    let relative = PathBuf::from(ACTIVE_SHADOW_CAMPAIGN_CONTRACT_PATH);
    let path = if relative.is_file() {
        relative
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(ACTIVE_SHADOW_CAMPAIGN_CONTRACT_PATH)
    };
    let binding = load_shadow_campaign_contract(&path)
        .map_err(|error| format!("active campaign contract is unavailable or invalid: {error}"))?;
    if binding.contract.campaign_id != ACTIVE_SHADOW_CAMPAIGN_ID
        || binding.contract.start_date.to_string() != ACTIVE_SHADOW_CAMPAIGN_START
        || binding.contract.terminal_date.to_string() != ACTIVE_SHADOW_CAMPAIGN_TERMINAL
        || binding.contract.daily_root != ACTIVE_SHADOW_DAILY_ROOT
        || binding.contract.prospective_path != ACTIVE_SHADOW_PROSPECTIVE_PATH
        || binding.contract.profitability_path != ACTIVE_SHADOW_PROFITABILITY_LATEST
    {
        return Err("active campaign contract does not match the Labs API campaign".to_owned());
    }
    Ok(binding)
}

fn fail_closed_profitability_value(
    mut value: Value,
    canonical_funded_state: bool,
    valid_current_schema: bool,
) -> Value {
    let Some(object) = value.as_object_mut() else {
        return default_profitability_manifest();
    };
    object.insert("promotion_allowed".to_owned(), json!(false));
    object.insert("human_authorization_required".to_owned(), json!(true));
    if !canonical_funded_state {
        object.remove("funded_ladder");
    }
    if !valid_current_schema {
        object.insert("phase".to_owned(), json!("risk_repair"));
        object.insert("status".to_owned(), json!("legacy_evidence_display_only"));
        object.insert(
            "blocking_reason".to_owned(),
            json!("Legacy or invalid profitability evidence is display-only and cannot authorize promotion."),
        );
    }
    value
}

fn default_profitability_manifest() -> Value {
    json!({
        "campaign_id": ACTIVE_SHADOW_CAMPAIGN_ID,
        "campaign_start": ACTIVE_SHADOW_CAMPAIGN_START,
        "campaign_terminal_date": ACTIVE_SHADOW_CAMPAIGN_TERMINAL,
        "phase": "risk_repair",
        "status": "awaiting_first_sealed_day",
        "candidate": {
            "name": "dynamic_quote_style",
            "version": "dynamic_quote_style@2026-06-14",
            "config_hash": "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c"
        },
        "blocking_reason": "No campaign-bound sealed profitability evidence exists for the active campaign yet.",
        "capital": {
            "original_starting_capital": 9.23,
            "campaign_starting_equity": 5.030521,
            "equity_floor": 4.03,
            "max_campaign_drawdown": 1.0
        },
        "shadow": {
            "clean_days": 0,
            "required_clean_days": 30,
            "settled_markets": 0,
            "required_settled_markets": 1000,
            "required_positive_weekly_blocks": 4
        },
        "data_quality": {
            "status": "collecting",
            "minimum_coverage": 0.95,
            "fatal_warnings": 0,
            "blocking_warnings": 0,
            "unclassified_warnings": 0
        },
        "promotion_allowed": false,
        "human_authorization_required": true
    })
}

fn normalize_venue_execution_summary(mut value: Value) -> Value {
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    let nested_probe = object
        .get("probes")
        .and_then(Value::as_array)
        .and_then(|probes| probes.last())
        .and_then(Value::as_object)
        .cloned();
    if let Some(probe) = nested_probe {
        for key in [
            "evidence_protocol_version",
            "started_ts",
            "finished_ts",
            "status",
            "order_submitted",
            "order",
            "lifecycle",
            "markouts",
            "portfolio",
        ] {
            if !object.contains_key(key) {
                if let Some(field) = probe.get(key) {
                    object.insert(key.to_owned(), field.clone());
                }
            }
        }
    }
    let order_submitted = object
        .get("order_submitted")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    object
        .entry("order_submission_attempted".to_owned())
        .or_insert_with(|| json!(order_submitted));
    object
        .entry("submitted_order_count".to_owned())
        .or_insert_with(|| json!(u8::from(order_submitted)));
    let completed = object
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| matches!(status, "completed" | "campaign_completed"));
    object
        .entry("completed_probe_count".to_owned())
        .or_insert_with(|| json!(u8::from(completed && order_submitted)));
    let eligibility = venue_evidence_eligibility(&value);
    if let Some(object) = value.as_object_mut() {
        object.insert("evidence_eligibility".to_owned(), eligibility);
    }
    value
}

fn venue_evidence_eligibility(value: &Value) -> Value {
    if !value.is_object() {
        return Value::Null;
    }
    let protocol_version = value.get("evidence_protocol_version").and_then(json_u64);
    let exact_protocol = protocol_version == Some(EVIDENCE_PROTOCOL_VERSION);
    json!({
        "required_protocol_version": EVIDENCE_PROTOCOL_VERSION,
        "observed_protocol_version": protocol_version,
        "exact_protocol_version": exact_protocol,
        "legacy": !exact_protocol,
        "legacy_eligibility": if exact_protocol { "requires_full_validator" } else { "display_only_legacy" },
        "counts_toward_protocol_evidence": false,
        "aggregate_promotion_ready": false,
        "validation_status": if exact_protocol { "terminal_binding_and_full_protocol_v3_validation_required" } else { "ineligible_protocol_version" },
        "reasons": if exact_protocol {
            vec!["labs_api_does_not_assert_protocol_eligibility_without_terminal_binding"]
        } else {
            vec!["evidence_protocol_version_must_equal_3"]
        }
    })
}

fn artifact_provenance(
    value: &Value,
    path: &FsPath,
    source: &str,
    now: DateTime<Utc>,
    legacy_eligibility: Option<&str>,
) -> Value {
    let (timestamp, timestamp_field) = artifact_timestamp(value);
    let age_seconds =
        timestamp.map(|timestamp| now.signed_duration_since(timestamp).num_seconds().max(0));
    let fresh = age_seconds.is_some_and(|age| age <= ARTIFACT_FRESHNESS_SECONDS)
        && timestamp.is_some_and(|timestamp| timestamp <= now);
    json!({
        "path": path.to_string_lossy(),
        "source": source,
        "available": value.is_object(),
        "schema_version": value.get("schema_version").cloned().unwrap_or(Value::Null),
        "authoritative_ts": timestamp.map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)),
        "authoritative_ts_field": timestamp_field,
        "age_seconds": age_seconds,
        "freshness_window_seconds": ARTIFACT_FRESHNESS_SECONDS,
        "fresh": fresh,
        "freshness": if !value.is_object() { "unavailable" } else if timestamp.is_none() { "unknown" } else if fresh { "fresh" } else { "stale" },
        "legacy_eligibility": legacy_eligibility.unwrap_or("not_applicable")
    })
}

fn artifact_timestamp(value: &Value) -> (Option<DateTime<Utc>>, Option<&'static str>) {
    for (pointer, field) in [
        ("/funded_ladder/updated_at", "funded_ladder.updated_at"),
        ("/updated_at", "updated_at"),
        ("/finished_ts", "finished_ts"),
        ("/generated_at", "generated_at"),
        ("/generated_ts", "generated_ts"),
        ("/created_at", "created_at"),
        ("/captured_ts", "captured_ts"),
    ] {
        if let Some(timestamp) = value
            .pointer(pointer)
            .and_then(Value::as_str)
            .and_then(parse_utc_timestamp)
        {
            return (Some(timestamp), Some(field));
        }
    }
    (None, None)
}

fn venue_legacy_eligibility(value: &Value) -> &'static str {
    match value.get("evidence_protocol_version").and_then(json_u64) {
        Some(EVIDENCE_PROTOCOL_VERSION) => "current_protocol",
        Some(_) => "display_only_legacy",
        None if value.is_object() => "unknown_display_only",
        None => "unavailable",
    }
}

fn model_legacy_eligibility(value: &Value) -> &'static str {
    match value.get("evidence_protocol_version").and_then(json_u64) {
        Some(EVIDENCE_PROTOCOL_VERSION) => "current_protocol",
        Some(_) => "display_only_legacy",
        None if value.is_object() => "conservative_prior_only",
        None => "unavailable",
    }
}

fn json_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.trim().parse().ok())
}

fn parse_utc_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn valid_prefixed_sha256(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
    let active = read_json_or_null(FsPath::new(ACTIVE_SHADOW_PROSPECTIVE_PATH));
    let correction = load_shadow_correction_gate();
    apply_shadow_correction_to_prospective(active_campaign_prospective_payload(active), &correction)
}

fn awaiting_active_campaign_prospective(status: &str, detail: &str) -> Value {
    json!({
        "generated_ts": now_ts(),
        "campaign_id": ACTIVE_SHADOW_CAMPAIGN_ID,
        "campaign_start": ACTIVE_SHADOW_CAMPAIGN_START,
        "campaign_terminal_date": ACTIVE_SHADOW_CAMPAIGN_TERMINAL,
        "promotion_allowed": false,
        "result": {
            "status": status,
            "detail": detail,
            "rows": [],
            "frozen_candidates": frozen_candidates_payload(),
            "research_only": true,
            "live_deployment_allowed": false
        }
    })
}

fn active_campaign_prospective_payload(mut payload: Value) -> Value {
    let binding = match active_shadow_campaign_contract() {
        Ok(binding) => binding,
        Err(error) => {
            return awaiting_active_campaign_prospective("active_campaign_binding_invalid", &error);
        }
    };
    if payload.is_null() {
        return awaiting_active_campaign_prospective(
            "awaiting_first_sealed_day",
            "No sealed prospective evidence exists for the active campaign yet.",
        );
    }
    let Some(rows) = payload.pointer("/result/rows").and_then(Value::as_array) else {
        return awaiting_active_campaign_prospective(
            "active_campaign_binding_invalid",
            "Active prospective evidence has no rows array.",
        );
    };
    if rows.is_empty() {
        return awaiting_active_campaign_prospective(
            "awaiting_first_sealed_day",
            "No sealed prospective evidence exists for the active campaign yet.",
        );
    }
    let bound = rows.iter().all(|row| {
        row["wallet_campaign_id"].as_str() == Some(ACTIVE_SHADOW_CAMPAIGN_ID)
            && row["wallet_campaign_start"].as_str() == Some(ACTIVE_SHADOW_CAMPAIGN_START)
            && row["wallet_campaign_first_eligible_date"].as_str()
                == Some(ACTIVE_SHADOW_CAMPAIGN_START)
            && row["wallet_campaign_terminal_date"].as_str()
                == Some(ACTIVE_SHADOW_CAMPAIGN_TERMINAL)
            && row["wallet_campaign_contract_sha256"]
                .as_str()
                .is_some_and(|sha256| sha256 == binding.sha256.as_str())
            && row["date"]
                .as_str()
                .is_some_and(|date| date >= ACTIVE_SHADOW_CAMPAIGN_START)
    });
    if !bound {
        return awaiting_active_campaign_prospective(
            "active_campaign_binding_invalid",
            "Prospective evidence is missing or has the wrong active campaign binding.",
        );
    }
    let Some(object) = payload.as_object_mut() else {
        return awaiting_active_campaign_prospective(
            "active_campaign_binding_invalid",
            "Active prospective evidence is not a JSON object.",
        );
    };
    object.insert("campaign_id".to_owned(), json!(ACTIVE_SHADOW_CAMPAIGN_ID));
    object.insert(
        "campaign_start".to_owned(),
        json!(ACTIVE_SHADOW_CAMPAIGN_START),
    );
    object.insert("promotion_allowed".to_owned(), json!(false));
    if let Some(result) = object.get_mut("result").and_then(Value::as_object_mut) {
        result.insert("research_only".to_owned(), json!(true));
        result.insert("live_deployment_allowed".to_owned(), json!(false));
    }
    payload
}

fn apply_shadow_correction_to_prospective(
    mut payload: Value,
    correction: &ShadowCorrectionGate,
) -> Value {
    if !payload.is_object() {
        payload = json!({
            "generated_ts": now_ts(),
            "result": {
                "status": "collecting",
                "rows": [],
                "frozen_candidates": frozen_candidates_payload(),
                "research_only": true,
                "live_deployment_allowed": false
            }
        });
    }
    let correction_json = correction.as_json();
    let Some(object) = payload.as_object_mut() else {
        return payload;
    };
    object.insert("correction".to_owned(), correction_json);
    object.insert(
        "promotion_decision".to_owned(),
        json!(correction.decision()),
    );
    object.insert(
        "promotion_blocker".to_owned(),
        correction
            .blocker
            .clone()
            .map_or(Value::Null, Value::String),
    );
    if !correction.blocks_promotion {
        return payload;
    }
    object.insert("promotion_allowed".to_owned(), json!(false));
    let result = object
        .entry("result".to_owned())
        .or_insert_with(|| json!({}))
        .as_object_mut();
    let Some(result) = result else {
        object.insert(
            "result".to_owned(),
            json!({
                "status": "correction_blocked_no_go",
                "decision": "NO-GO",
                "promotion_allowed": false,
                "live_deployment_allowed": false,
                "research_only": true,
                "blocker": correction.blocker
            }),
        );
        return payload;
    };
    if let Some(status) = result.get("status").cloned() {
        result.insert("pre_correction_status".to_owned(), status);
    }
    result.insert("status".to_owned(), json!("correction_blocked_no_go"));
    result.insert("decision".to_owned(), json!("NO-GO"));
    result.insert("promotion_allowed".to_owned(), json!(false));
    result.insert("live_deployment_allowed".to_owned(), json!(false));
    result.insert("research_only".to_owned(), json!(true));
    result.insert(
        "blocker".to_owned(),
        correction
            .blocker
            .clone()
            .map_or(Value::Null, Value::String),
    );
    payload
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

struct VerifiedDailyBundle {
    date: String,
    daily_root: String,
    source: String,
    manifest: DailyRunManifest,
    artifact_bytes: BTreeMap<String, Vec<u8>>,
}

fn resolve_verified_daily_bundle(
    requested_date: Option<&str>,
) -> Result<Option<VerifiedDailyBundle>, String> {
    let daily_root = match requested_date {
        None => ACTIVE_SHADOW_DAILY_ROOT,
        Some(date) if date >= ACTIVE_SHADOW_CAMPAIGN_START => ACTIVE_SHADOW_DAILY_ROOT,
        Some(date) if (LEGACY_SHADOW_FIRST_DATE..=LEGACY_SHADOW_LAST_DATE).contains(&date) => {
            SHADOW_DAILY_ROOT
        }
        Some(_) => return Ok(None),
    };
    let expected_runtime_role = polyedge_config::RuntimeRole::ProfitabilityShadow;
    if let Some(mut client) = artifact_blob_client("AZURE_RESEARCH_STORAGE_CONTAINER_NAME") {
        if let Some(bundle) = resolve_azure_verified_daily_bundle(
            &mut client,
            daily_root,
            &expected_runtime_role,
            requested_date,
        )? {
            return Ok(Some(bundle));
        }
    }
    if let Some(bundle) =
        resolve_local_verified_daily_bundle(daily_root, &expected_runtime_role, requested_date)?
    {
        return Ok(Some(bundle));
    }
    Ok(None)
}

fn resolve_azure_verified_daily_bundle(
    client: &mut AzureBlobClient,
    daily_root: &str,
    expected_runtime_role: &polyedge_config::RuntimeRole,
    requested_date: Option<&str>,
) -> Result<Option<VerifiedDailyBundle>, String> {
    let pointer_base = requested_date
        .map(|date| format!("{daily_root}/{date}"))
        .unwrap_or_else(|| daily_root.to_owned());
    let pointer_name = format!("{pointer_base}/latest.json");
    let pointer_bytes = match client.download_blob_bytes(&pointer_name) {
        Ok(bytes) => bytes,
        Err(AzureBlobError::HttpStatus(404)) => return Ok(None),
        Err(error) => return Err(format!("Unable to read atomic latest pointer: {error}")),
    };
    let pointer: LatestRunPointer = serde_json::from_slice(&pointer_bytes)
        .map_err(|error| format!("Atomic latest pointer is invalid JSON: {error}"))?;
    validate_daily_root_date(daily_root, pointer.date)?;
    validate_pointer(&pointer, requested_date)?;
    let manifest_name = format!("{pointer_base}/{}", pointer.manifest_path);
    let manifest_bytes = client
        .download_blob_bytes(&manifest_name)
        .map_err(|error| format!("Atomic run manifest is unavailable: {error}"))?;
    let manifest = validate_manifest(
        &pointer,
        &manifest_bytes,
        expected_runtime_role,
        requested_date,
    )?;
    let run_prefix = manifest_name
        .strip_suffix("run_manifest.json")
        .ok_or_else(|| "Atomic manifest path must end in run_manifest.json".to_owned())?;
    let mut artifact_bytes = BTreeMap::new();
    for artifact in manifest.artifacts.values() {
        validate_relative_artifact_path(&artifact.relative_path)?;
        let blob_name = format!("{run_prefix}{}", artifact.relative_path);
        let bytes = client.download_blob_bytes(&blob_name).map_err(|error| {
            format!(
                "Atomic artifact {} is unavailable: {error}",
                artifact.relative_path
            )
        })?;
        verify_artifact(artifact, &bytes)?;
        artifact_bytes.insert(artifact.relative_path.clone(), bytes);
    }
    Ok(Some(VerifiedDailyBundle {
        date: pointer.date.to_string(),
        daily_root: daily_root.to_owned(),
        source: format!("azure://{run_prefix}"),
        manifest,
        artifact_bytes,
    }))
}

fn resolve_local_verified_daily_bundle(
    daily_root: &str,
    expected_runtime_role: &polyedge_config::RuntimeRole,
    requested_date: Option<&str>,
) -> Result<Option<VerifiedDailyBundle>, String> {
    let daily_root = PathBuf::from(daily_root);
    let pointer_base = requested_date
        .map(|date| daily_root.join(date))
        .unwrap_or_else(|| daily_root.clone());
    let pointer_path = pointer_base.join("latest.json");
    let pointer_bytes = match fs::read(&pointer_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("Unable to read atomic latest pointer: {error}")),
    };
    let pointer: LatestRunPointer = serde_json::from_slice(&pointer_bytes)
        .map_err(|error| format!("Atomic latest pointer is invalid JSON: {error}"))?;
    validate_daily_root_date(&daily_root.to_string_lossy(), pointer.date)?;
    validate_pointer(&pointer, requested_date)?;
    let manifest_path = pointer_base.join(&pointer.manifest_path);
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|error| format!("Atomic run manifest is unavailable: {error}"))?;
    let manifest = validate_manifest(
        &pointer,
        &manifest_bytes,
        expected_runtime_role,
        requested_date,
    )?;
    let run_dir = manifest_path
        .parent()
        .ok_or_else(|| "Atomic run manifest has no parent directory".to_owned())?;
    let mut artifact_bytes = BTreeMap::new();
    for artifact in manifest.artifacts.values() {
        validate_relative_artifact_path(&artifact.relative_path)?;
        let bytes = fs::read(run_dir.join(&artifact.relative_path)).map_err(|error| {
            format!(
                "Atomic artifact {} is unavailable: {error}",
                artifact.relative_path
            )
        })?;
        verify_artifact(artifact, &bytes)?;
        artifact_bytes.insert(artifact.relative_path.clone(), bytes);
    }
    Ok(Some(VerifiedDailyBundle {
        date: pointer.date.to_string(),
        daily_root: daily_root.to_string_lossy().replace('\\', "/"),
        source: run_dir.to_string_lossy().replace('\\', "/"),
        manifest,
        artifact_bytes,
    }))
}

fn validate_daily_root_date(daily_root: &str, date: NaiveDate) -> Result<(), String> {
    let date = date.to_string();
    if daily_root == ACTIVE_SHADOW_DAILY_ROOT && date.as_str() < ACTIVE_SHADOW_CAMPAIGN_START {
        return Err("Active campaign daily root points before the campaign start".to_owned());
    }
    if daily_root == SHADOW_DAILY_ROOT
        && !(LEGACY_SHADOW_FIRST_DATE..=LEGACY_SHADOW_LAST_DATE).contains(&date.as_str())
    {
        return Err("Legacy shadow daily root points outside the display-only window".to_owned());
    }
    Ok(())
}

fn validate_pointer(
    pointer: &LatestRunPointer,
    requested_date: Option<&str>,
) -> Result<(), String> {
    validate_relative_artifact_path(&pointer.manifest_path)?;
    if !pointer.manifest_path.ends_with("run_manifest.json") {
        return Err("Atomic latest pointer does not reference run_manifest.json".to_owned());
    }
    if requested_date.is_some_and(|date| pointer.date.to_string() != date) {
        return Err("Atomic latest pointer date does not match the requested date".to_owned());
    }
    let expected_manifest_path = if requested_date.is_some() {
        format!("runs/{}/run_manifest.json", pointer.run_id)
    } else {
        format!("{}/runs/{}/run_manifest.json", pointer.date, pointer.run_id)
    };
    if pointer.manifest_path != expected_manifest_path {
        return Err("Atomic latest pointer does not use the canonical run path".to_owned());
    }
    Ok(())
}

fn validate_manifest(
    pointer: &LatestRunPointer,
    bytes: &[u8],
    expected_runtime_role: &polyedge_config::RuntimeRole,
    requested_date: Option<&str>,
) -> Result<DailyRunManifest, String> {
    if sha256_hex(bytes) != pointer.manifest_sha256 {
        return Err("Atomic run manifest hash does not match latest.json".to_owned());
    }
    let manifest: DailyRunManifest = serde_json::from_slice(bytes)
        .map_err(|error| format!("Atomic run manifest is invalid JSON: {error}"))?;
    if manifest.status != RunStatus::Complete {
        return Err("Atomic run manifest is not COMPLETE".to_owned());
    }
    if daily_provenance_required(pointer.date) && manifest.schema_version != 2 {
        return Err("Atomic run manifest uses a downgraded schema".to_owned());
    }
    if manifest.schema_version == 2
        && !manifest
            .git_sha
            .as_deref()
            .is_some_and(polyedge_config::is_full_git_sha)
    {
        return Err("Atomic run manifest has invalid Git provenance".to_owned());
    }
    if manifest.schema_version == 2 {
        match manifest.runtime_role.as_ref() {
            None => {
                return Err("Atomic run manifest has no runtime role provenance".to_owned());
            }
            Some(runtime_role) if runtime_role != expected_runtime_role => {
                return Err(
                    "Atomic run manifest runtime role does not match its daily root".to_owned(),
                );
            }
            Some(_) => {}
        }
    }
    if manifest.date != pointer.date || manifest.run_id != pointer.run_id {
        return Err("Atomic run manifest identity does not match latest.json".to_owned());
    }
    if requested_date.is_some_and(|date| manifest.date.to_string() != date) {
        return Err("Atomic run manifest date does not match the requested date".to_owned());
    }
    if manifest.artifacts.is_empty() {
        return Err("Atomic run manifest contains no artifacts".to_owned());
    }
    Ok(manifest)
}

fn validate_relative_artifact_path(path: &str) -> Result<(), String> {
    if path.is_empty()
        || path.starts_with('/')
        || path.starts_with('\\')
        || path.split(['/', '\\']).any(|part| part == "..")
    {
        return Err("Atomic bundle contains an unsafe relative path".to_owned());
    }
    Ok(())
}

fn verify_artifact(
    artifact: &polyedge_reporting::research::RunArtifact,
    bytes: &[u8],
) -> Result<(), String> {
    if artifact.bytes != bytes.len() as u64 || artifact.sha256 != sha256_hex(bytes) {
        return Err(format!(
            "Atomic artifact hash or size mismatch: {}",
            artifact.relative_path
        ));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn bundle_json(bundle: &VerifiedDailyBundle, candidates: &[&str]) -> Value {
    candidates
        .iter()
        .find_map(|candidate| bundle.artifact_bytes.get(*candidate))
        .and_then(|bytes| serde_json::from_slice(bytes).ok())
        .unwrap_or(Value::Null)
}

fn daily_payload_from_bundle(bundle: &VerifiedDailyBundle) -> Value {
    let daily_root = bundle
        .daily_root
        .strip_prefix(&format!("{REPORT_ROOT}/"))
        .unwrap_or(&bundle.daily_root);
    let artifacts = bundle
        .manifest
        .artifacts
        .values()
        .map(|artifact| {
            json!({
                "artifact_id": format!("{}~{}~runs~{}~{}", daily_root.replace('/', "~"), bundle.date, bundle.manifest.run_id, artifact.relative_path.replace('/', "~")),
                "path": artifact.relative_path,
                "kind": FsPath::new(&artifact.relative_path).extension().and_then(|value| value.to_str()),
                "size_bytes": artifact.bytes,
                "sha256": artifact.sha256
            })
        })
        .collect::<Vec<_>>();
    let historical = bundle.daily_root == SHADOW_DAILY_ROOT;
    json!({
        "date": bundle.date,
        "campaign_id": if historical { LEGACY_SHADOW_CAMPAIGN_ID } else { ACTIVE_SHADOW_CAMPAIGN_ID },
        "active_campaign_id": ACTIVE_SHADOW_CAMPAIGN_ID,
        "campaign_start": ACTIVE_SHADOW_CAMPAIGN_START,
        "evidence_eligibility": if historical { "historical_ineligible" } else { "active_campaign_eligible" },
        "run_id": bundle.manifest.run_id,
        "status": if historical { "historical_ineligible" } else { "complete" },
        "bundle_status": "complete",
        "source": bundle.source,
        "report": bundle_json(bundle, &["final_report.json", "final_strategy_research_report.json"]),
        "audit": bundle_json(bundle, &["data_audit.json"]),
        "baseline": bundle_json(bundle, &["baseline.json", "baseline_static_all_fill_models.json"]),
        "regimes": bundle_json(bundle, &["regimes.json", "regime_profiles.json"]),
        "calibration": bundle_json(bundle, &["calibration.json"]),
        "sample_size": bundle_json(bundle, &["sample_size.json"]),
        "execution_quality": bundle_json(bundle, &["execution_quality.json"]),
        "artifacts": artifacts
    })
}

fn read_latest_report_payload() -> Value {
    match resolve_verified_daily_bundle(None) {
        Ok(Some(bundle)) => daily_payload_from_bundle(&bundle),
        Ok(None) => json!({
            "campaign_id": ACTIVE_SHADOW_CAMPAIGN_ID,
            "campaign_start": ACTIVE_SHADOW_CAMPAIGN_START,
            "report": Value::Null,
            "detail": "No sealed daily report exists for the active campaign yet.",
            "status": "awaiting_first_sealed_day",
            "artifacts": []
        }),
        Err(detail) => json!({
            "report": Value::Null,
            "detail": detail,
            "status": "atomic_bundle_invalid",
            "artifacts": []
        }),
    }
}

pub(crate) fn daily_report_payload(date: &str) -> Value {
    if NaiveDate::parse_from_str(date, "%Y-%m-%d").is_err() {
        return json!({
            "date": date,
            "report": Value::Null,
            "detail": "Date must be a real YYYY-MM-DD calendar date.",
            "status": "invalid_date",
            "artifacts": []
        });
    }
    match resolve_verified_daily_bundle(Some(date)) {
        Ok(Some(bundle)) => daily_payload_from_bundle(&bundle),
        Ok(None) if date < ACTIVE_SHADOW_CAMPAIGN_START => json!({
            "date": date,
            "campaign_id": ACTIVE_SHADOW_CAMPAIGN_ID,
            "campaign_start": ACTIVE_SHADOW_CAMPAIGN_START,
            "evidence_eligibility": "historical_ineligible",
            "report": Value::Null,
            "detail": if date == "2026-07-21" { "July 21 is the campaign reset boundary and is ineligible." } else { "No eligible legacy shadow report exists for this historical date." },
            "status": "historical_ineligible",
            "artifacts": []
        }),
        Ok(None) => json!({
            "date": date,
            "campaign_id": ACTIVE_SHADOW_CAMPAIGN_ID,
            "campaign_start": ACTIVE_SHADOW_CAMPAIGN_START,
            "report": Value::Null,
            "detail": "No sealed daily report exists for this active campaign date.",
            "status": "awaiting_sealed_day",
            "artifacts": []
        }),
        Err(detail) => json!({
            "date": date,
            "report": Value::Null,
            "detail": detail,
            "status": "atomic_bundle_invalid",
            "artifacts": []
        }),
    }
}

fn latest_named_report(primary: &str, fallback: &str) -> Value {
    match resolve_verified_daily_bundle(None) {
        Ok(Some(bundle)) => json!({
            "date": bundle.date,
            "run_id": bundle.manifest.run_id,
            "report": bundle_json(&bundle, &[primary, fallback]),
            "source": bundle.source
        }),
        Ok(None) => json!({
            "campaign_id": ACTIVE_SHADOW_CAMPAIGN_ID,
            "campaign_start": ACTIVE_SHADOW_CAMPAIGN_START,
            "report": Value::Null,
            "detail": "No sealed daily report exists for the active campaign yet.",
            "status": "awaiting_first_sealed_day"
        }),
        Err(detail) => json!({
            "report": Value::Null,
            "detail": detail,
            "status": "atomic_bundle_invalid"
        }),
    }
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

fn artifact_blob_client(container_env: &str) -> Option<AzureBlobClient> {
    let account = env::var("AZURE_STORAGE_ACCOUNT_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())?;
    let container = match env::var(container_env)
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        Some(value) => value,
        None if container_env == "AZURE_STORAGE_CONTAINER_NAME" => "bot-events".to_owned(),
        None => return None,
    };
    let client_id = env::var("AZURE_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());
    Some(AzureBlobClient::with_managed_identity(
        account, container, client_id,
    ))
}

fn artifact_container_env(blob_name: &str) -> &'static str {
    if blob_name == "reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json"
    {
        "AZURE_RESEARCH_STORAGE_CONTAINER_NAME"
    } else if blob_name == "reports/research/venue-probe/effective_queue_model.json"
        || blob_name.starts_with("reports/research/venue-probe/models/")
    {
        "AZURE_MODEL_STORAGE_CONTAINER_NAME"
    } else if blob_name.starts_with("reports/research/venue-probe/") {
        "AZURE_FUNDED_STORAGE_CONTAINER_NAME"
    } else if blob_name.starts_with("reports/research/shadow/")
        || blob_name.starts_with("reports/research/profitability/")
    {
        "AZURE_RESEARCH_STORAGE_CONTAINER_NAME"
    } else {
        "AZURE_STORAGE_CONTAINER_NAME"
    }
}

fn read_json_from_container_or_null(path: &FsPath, container_env: &str) -> Value {
    let Some(blob_name) = blob_name_for_path(path) else {
        return Value::Null;
    };
    let Some(mut client) = artifact_blob_client(container_env) else {
        return Value::Null;
    };
    match client.download_blob_bytes(&blob_name) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        Err(AzureBlobError::HttpStatus(404)) => Value::Null,
        Err(_) => Value::Null,
    }
}

fn read_blob_bytes_for_path(path: &FsPath) -> Option<Vec<u8>> {
    let blob_name = blob_name_for_path(path)?;
    read_blob_bytes(&blob_name)
}

fn read_blob_bytes(blob_name: &str) -> Option<Vec<u8>> {
    let container_envs: &[&str] = if blob_name == "reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json"
    {
        &["AZURE_RESEARCH_STORAGE_CONTAINER_NAME"]
    } else if blob_name.starts_with("reports/research/profitability/") {
        &[
            "AZURE_FUNDED_STORAGE_CONTAINER_NAME",
            "AZURE_RESEARCH_STORAGE_CONTAINER_NAME",
        ]
    } else if blob_name == "reports/research/venue-probe/effective_queue_model.json"
        || blob_name.starts_with("reports/research/venue-probe/models/")
    {
        &[
            "AZURE_MODEL_STORAGE_CONTAINER_NAME",
            "AZURE_FUNDED_STORAGE_CONTAINER_NAME",
            "AZURE_RESEARCH_STORAGE_CONTAINER_NAME",
        ]
    } else {
        &[artifact_container_env(blob_name)]
    };
    for container_env in container_envs {
        let Some(mut client) = artifact_blob_client(container_env) else {
            continue;
        };
        match client.download_blob_bytes(blob_name) {
            Ok(bytes) => return Some(bytes),
            Err(AzureBlobError::HttpStatus(404)) => continue,
            Err(_) => return None,
        }
    }
    None
}

fn list_blob_json_files(root: &FsPath, file_name: &str, limit: usize) -> Option<Vec<Value>> {
    let mut prefix = blob_name_for_path(root)?;
    if !prefix.ends_with('/') {
        prefix.push('/');
    }
    let mut client = artifact_blob_client(artifact_container_env(&prefix))?;
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
    let mut blob_prefix = REPORT_ROOT.to_owned();
    let clean_prefix = prefix.trim().trim_start_matches('/').trim_end_matches('/');
    if !clean_prefix.is_empty() {
        blob_prefix.push('/');
        blob_prefix.push_str(clean_prefix);
    }
    if !blob_prefix.ends_with('/') {
        blob_prefix.push('/');
    }
    let container_envs: &[&str] = if clean_prefix.is_empty() {
        &[
            "AZURE_STORAGE_CONTAINER_NAME",
            "AZURE_RESEARCH_STORAGE_CONTAINER_NAME",
            "AZURE_MODEL_STORAGE_CONTAINER_NAME",
            "AZURE_FUNDED_STORAGE_CONTAINER_NAME",
        ]
    } else if clean_prefix == "profitability" || clean_prefix.starts_with("profitability/") {
        &[
            "AZURE_RESEARCH_STORAGE_CONTAINER_NAME",
            "AZURE_FUNDED_STORAGE_CONTAINER_NAME",
        ]
    } else if clean_prefix == "venue-probe/models"
        || clean_prefix.starts_with("venue-probe/models/")
    {
        &[
            "AZURE_RESEARCH_STORAGE_CONTAINER_NAME",
            "AZURE_MODEL_STORAGE_CONTAINER_NAME",
        ]
    } else {
        &[artifact_container_env(&blob_prefix)]
    };
    let mut artifacts = BTreeMap::<String, Value>::new();
    let mut any_client = false;
    for container_env in container_envs {
        let Some(mut client) = artifact_blob_client(container_env) else {
            continue;
        };
        any_client = true;
        let blobs = client
            .list_blobs_by_suffixes(&blob_prefix, &[".json", ".md"], Some(1000), None)
            .ok()?;
        for blob in blobs {
            let Some(relative) = blob.name.strip_prefix(&format!("{REPORT_ROOT}/")) else {
                continue;
            };
            let Some(extension) = FsPath::new(relative)
                .extension()
                .and_then(|value| value.to_str())
            else {
                continue;
            };
            artifacts.insert(
                relative.to_owned(),
                json!({
                    "artifact_id": relative.replace('/', "~"),
                    "path": relative,
                    "kind": extension,
                    "size_bytes": blob.content_length,
                    "modified_ts": blob.last_modified.map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)),
                    "storage_role": container_env
                }),
            );
        }
    }
    any_client.then(|| artifacts.into_values().collect())
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
    use chrono::{Duration, TimeZone};
    use polyedge_reporting::research::{
        CandidateIdentity, DataQualitySummary, ExecutionModelBinding, FundedLadderStateV1,
        ProfitabilityMetrics, PromotionEvaluation,
    };
    use rust_decimal::Decimal;

    #[test]
    fn prospective_accepts_only_active_campaign_bound_rows() {
        let contract_sha256 = active_shadow_campaign_contract().unwrap().sha256;
        let active = json!({
            "result": {
                "status": "tracking",
                "rows": [{
                    "date": "2026-07-22",
                    "wallet_schema_version": 3,
                    "wallet_campaign_id": ACTIVE_SHADOW_CAMPAIGN_ID,
                    "wallet_campaign_contract_sha256": contract_sha256,
                    "wallet_campaign_start": ACTIVE_SHADOW_CAMPAIGN_START,
                    "wallet_campaign_first_eligible_date": ACTIVE_SHADOW_CAMPAIGN_START,
                    "wallet_campaign_terminal_date": ACTIVE_SHADOW_CAMPAIGN_TERMINAL
                }]
            }
        });
        let stale = json!({
            "result": {
                "status": "tracking",
                "rows": [{"date": "2026-07-20"}]
            }
        });
        let selected = active_campaign_prospective_payload(active);
        assert_eq!(selected["campaign_id"], ACTIVE_SHADOW_CAMPAIGN_ID);
        assert_eq!(selected["result"]["live_deployment_allowed"], false);
        assert_eq!(
            active_campaign_prospective_payload(stale)["result"]["status"],
            "active_campaign_binding_invalid"
        );
        assert_eq!(
            active_campaign_prospective_payload(Value::Null)["result"]["status"],
            "awaiting_first_sealed_day"
        );
    }

    #[test]
    fn in_progress_correction_forces_prospective_no_go_without_hiding_rows() {
        let correction =
            correction_gate_from_result(Ok(Some(test_correction_state("in_progress"))));
        let payload = apply_shadow_correction_to_prospective(
            json!({
                "result": {
                    "status": "passed",
                    "promotion_allowed": true,
                    "live_deployment_allowed": true,
                    "rows": [{"date": "2026-07-13", "decision_gate": "GO"}]
                }
            }),
            &correction,
        );

        assert_eq!(payload["correction"]["status"], "in_progress");
        assert_eq!(payload["correction"]["blocks_promotion"], true);
        assert_eq!(payload["promotion_decision"], "NO-GO");
        assert_eq!(payload["promotion_allowed"], false);
        assert_eq!(payload["result"]["status"], "correction_blocked_no_go");
        assert_eq!(payload["result"]["pre_correction_status"], "passed");
        assert_eq!(payload["result"]["promotion_allowed"], false);
        assert_eq!(payload["result"]["live_deployment_allowed"], false);
        assert_eq!(payload["result"]["rows"][0]["decision_gate"], "GO");
        assert!(payload["result"]["blocker"]
            .as_str()
            .is_some_and(|blocker| blocker.contains("corr-2026-07-13")));
    }

    #[test]
    fn failed_correction_overrides_stale_profitability_and_nested_promotion_flags() {
        let correction = correction_gate_from_result(Ok(Some(test_correction_state("failed"))));
        let mut profitability = json!({
            "phase": "profitable_go",
            "status": "passed",
            "promotion_allowed": true,
            "human_authorization_required": false,
            "gate_metrics": {"promotion_allowed": true},
            "funded_ladder": {
                "phase": "profitable_go",
                "promotion_allowed": true,
                "stage_authorized": true,
                "human_grant_required": false
            }
        });

        apply_shadow_correction_to_profitability(&mut profitability, &correction);

        assert_eq!(profitability["phase"], "risk_repair");
        assert_eq!(profitability["pre_correction_phase"], "profitable_go");
        assert_eq!(profitability["status"], "correction_blocked_no_go");
        assert_eq!(profitability["effective_decision"], "NO-GO");
        assert_eq!(profitability["promotion_allowed"], false);
        assert_eq!(profitability["human_authorization_required"], true);
        assert_eq!(profitability["gate_metrics"]["promotion_allowed"], false);
        assert_eq!(profitability["funded_ladder"]["promotion_allowed"], false);
        assert_eq!(profitability["funded_ladder"]["stage_authorized"], false);
        assert_eq!(profitability["funded_ladder"]["human_grant_required"], true);
        assert!(profitability["blocking_reason"]
            .as_str()
            .is_some_and(|blocker| blocker.contains("failed")));
    }

    #[test]
    fn complete_correction_preserves_current_eligibility_and_read_failure_blocks() {
        let complete = correction_gate_from_result(Ok(Some(test_correction_state("complete"))));
        let mut profitability = json!({
            "phase": "shadow_collecting",
            "promotion_allowed": false
        });
        apply_shadow_correction_to_profitability(&mut profitability, &complete);
        assert_eq!(complete.decision(), "ELIGIBILITY_UNCHANGED");
        assert_eq!(profitability["phase"], "shadow_collecting");
        assert!(profitability.get("pre_correction_phase").is_none());

        let unavailable = correction_gate_from_result(Err("unreadable".to_owned()));
        let payload = apply_shadow_correction_to_prospective(
            json!({"result": {"status": "passed", "rows": []}}),
            &unavailable,
        );
        assert_eq!(payload["correction"]["status"], "unavailable");
        assert_eq!(payload["correction"]["validation_error"], true);
        assert_eq!(payload["result"]["decision"], "NO-GO");
    }

    #[test]
    fn profitability_selection_prefers_newer_shadow_before_a_funded_ladder_exists() {
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap();
        let funded =
            test_promotion_manifest(now - Duration::hours(3), PromotionPhase::RiskRepair, false);
        let shadow = test_promotion_manifest(
            now - Duration::hours(1),
            PromotionPhase::ShadowCollecting,
            false,
        );

        let selected = select_profitability_artifact(
            ProfitabilityArtifact::new(
                serde_json::to_value(funded).unwrap(),
                ProfitabilitySource::Funded,
                now,
            ),
            ProfitabilityArtifact::new(
                serde_json::to_value(shadow).unwrap(),
                ProfitabilitySource::Shadow,
                now,
            ),
            now,
        );

        assert_eq!(selected.value["phase"], "shadow_collecting");
        assert_eq!(
            selected.provenance["selected_source"],
            "profitability_shadow"
        );
        assert_eq!(
            selected.provenance["selection_reason"],
            "active_campaign_shadow"
        );
        assert_eq!(selected.provenance["canonical_funded_state"], false);
        assert_eq!(selected.value["promotion_allowed"], false);
        assert!(selected.value.get("funded_ladder").is_none());
    }

    #[test]
    fn shadow_profitability_fails_closed_without_active_campaign_binding() {
        let now = Utc.with_ymd_and_hms(2026, 7, 22, 12, 0, 0).unwrap();
        let mut shadow = test_promotion_manifest(
            now - Duration::minutes(5),
            PromotionPhase::ShadowCollecting,
            false,
        );
        shadow.artifact_uris.remove("shadow_campaign_id");

        let selected = select_profitability_artifact(
            ProfitabilityArtifact::new(Value::Null, ProfitabilitySource::Funded, now),
            ProfitabilityArtifact::new(
                serde_json::to_value(shadow).unwrap(),
                ProfitabilitySource::Shadow,
                now,
            ),
            now,
        );

        assert_eq!(selected.value["promotion_allowed"], false);
        assert_eq!(selected.value["status"], "legacy_evidence_display_only");
        assert_eq!(
            selected.provenance["selected"]["valid_current_schema"],
            false
        );
        assert!(selected.provenance["selected"]["validation_error"]
            .as_str()
            .is_some_and(|error| error.contains("shadow_campaign_id")));
    }

    #[test]
    fn profitability_selection_keeps_canonical_funded_state_over_newer_shadow() {
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap();
        let mut funded =
            test_promotion_manifest(now - Duration::hours(4), PromotionPhase::ShadowPassed, true);
        let ladder = FundedLadderStateV1::new(funded.candidate.clone(), now - Duration::hours(3))
            .expect("valid funded ladder");
        funded.phase = ladder.phase;
        funded.funded_ladder = Some(ladder);
        let shadow = test_promotion_manifest(
            now - Duration::minutes(5),
            PromotionPhase::ShadowCollecting,
            false,
        );

        let selected = select_profitability_artifact(
            ProfitabilityArtifact::new(
                serde_json::to_value(funded).unwrap(),
                ProfitabilitySource::Funded,
                now,
            ),
            ProfitabilityArtifact::new(
                serde_json::to_value(shadow).unwrap(),
                ProfitabilitySource::Shadow,
                now,
            ),
            now,
        );

        assert_eq!(selected.value["phase"], "evidence_collecting");
        assert_eq!(selected.provenance["selected_source"], "funded_evidence");
        assert_eq!(
            selected.provenance["selection_reason"],
            "canonical_funded_state"
        );
        assert_eq!(selected.provenance["canonical_funded_state"], true);
        assert!(selected.value["funded_ladder"].is_object());
        assert_eq!(selected.value["promotion_allowed"], false);
    }

    #[test]
    fn shadow_storage_cannot_masquerade_as_canonical_funded_state() {
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap();
        let mut rogue_shadow = test_promotion_manifest(
            now - Duration::minutes(5),
            PromotionPhase::ShadowPassed,
            true,
        );
        let ladder = FundedLadderStateV1::new(rogue_shadow.candidate.clone(), now)
            .expect("valid ladder shape");
        rogue_shadow.phase = ladder.phase;
        rogue_shadow.funded_ladder = Some(ladder);

        let selected = select_profitability_artifact(
            ProfitabilityArtifact::new(Value::Null, ProfitabilitySource::Funded, now),
            ProfitabilityArtifact::new(
                serde_json::to_value(rogue_shadow).unwrap(),
                ProfitabilitySource::Shadow,
                now,
            ),
            now,
        );

        assert_eq!(selected.value["phase"], "risk_repair");
        assert_eq!(selected.value["status"], "legacy_evidence_display_only");
        assert_eq!(selected.value["promotion_allowed"], false);
        assert!(selected.value.get("funded_ladder").is_none());
        assert_eq!(selected.provenance["canonical_funded_state"], false);
        assert_eq!(
            selected.provenance["selected"]["valid_current_schema"],
            false
        );
    }

    #[test]
    fn venue_summary_uses_latest_probe_and_preserves_standard_lifecycle_schema() {
        let summary = json!({
            "schema_version": 3,
            "probes": [
                {
                    "evidence_protocol_version": 2,
                    "status": "completed",
                    "finished_ts": "2026-07-13T10:00:00Z",
                    "order_submitted": true,
                    "lifecycle": { "client_to_http_ack_ms": 9999 }
                },
                {
                    "evidence_protocol_version": 3,
                    "status": "completed",
                    "started_ts": "2026-07-13T11:00:00Z",
                    "finished_ts": "2026-07-13T11:01:00Z",
                    "order_submitted": true,
                    "order": { "size": 5, "inferredSizeAhead": 12 },
                    "lifecycle": {
                        "order_id": "order-new",
                        "client_to_http_ack_ms": 41,
                        "client_cancel_round_trip_ms": 52,
                        "client_to_user_cancel_ack_ms": 63,
                        "actual_matched_size": 2,
                        "partial_fill": true,
                        "fully_filled": false,
                        "fill_raced_cancellation": true,
                        "public_touch_trade_count": 4,
                        "public_strict_trade_through_count": 3,
                        "public_trade_through_without_fill_count": 1,
                        "reconciliation_complete": true,
                        "zero_open_orders_confirmed": true,
                        "data_gap_detected": false,
                        "cancellation_failure": false,
                        "markout_capture_complete": true,
                        "matched_size_source_agreement": true,
                        "trade_id_source_agreement": true,
                        "authenticated_user_channel_reconnects": 0,
                        "public_market_channel_reconnects": 1
                    },
                    "markouts": [{ "fill_id": "trade-1", "horizon_seconds": 1 }]
                }
            ]
        });

        let normalized = normalize_venue_execution_summary(summary);

        assert_eq!(normalized["evidence_protocol_version"], 3);
        assert_eq!(normalized["finished_ts"], "2026-07-13T11:01:00Z");
        assert_eq!(normalized["lifecycle"]["order_id"], "order-new");
        assert_eq!(normalized["lifecycle"]["client_to_http_ack_ms"], 41);
        assert_eq!(normalized["lifecycle"]["client_cancel_round_trip_ms"], 52);
        assert_eq!(normalized["lifecycle"]["client_to_user_cancel_ack_ms"], 63);
        assert_eq!(normalized["lifecycle"]["fill_raced_cancellation"], true);
        assert_eq!(
            normalized["lifecycle"]["public_strict_trade_through_count"],
            3
        );
        assert_eq!(
            normalized["lifecycle"]["matched_size_source_agreement"],
            true
        );
        assert_eq!(
            normalized["lifecycle"]["authenticated_user_channel_reconnects"],
            0
        );
        assert_eq!(normalized["markouts"][0]["fill_id"], "trade-1");
        assert_eq!(
            normalized["evidence_eligibility"]["exact_protocol_version"],
            true
        );
        assert_eq!(
            normalized["evidence_eligibility"]["counts_toward_protocol_evidence"],
            false
        );
        assert_eq!(
            normalized["evidence_eligibility"]["validation_status"],
            "terminal_binding_and_full_protocol_v3_validation_required"
        );
    }

    #[test]
    fn profitability_and_shadow_artifacts_route_to_isolated_research_container() {
        assert_eq!(
            artifact_container_env("reports/research/profitability/latest.json"),
            "AZURE_RESEARCH_STORAGE_CONTAINER_NAME"
        );
        assert_eq!(
            artifact_container_env("reports/research/shadow/daily/2026-07-12/latest.json"),
            "AZURE_RESEARCH_STORAGE_CONTAINER_NAME"
        );
        assert_eq!(
            artifact_container_env("reports/research/venue-probe/latest.json"),
            "AZURE_FUNDED_STORAGE_CONTAINER_NAME"
        );
        assert_eq!(
            artifact_container_env("reports/research/venue-probe/effective_queue_model.json"),
            "AZURE_MODEL_STORAGE_CONTAINER_NAME"
        );
        assert_eq!(
            artifact_container_env("reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json"),
            "AZURE_RESEARCH_STORAGE_CONTAINER_NAME"
        );
    }

    #[test]
    fn daily_report_payload_reads_only_verified_atomic_contract() {
        let date = "2099-12-31";
        let dir = PathBuf::from(ACTIVE_SHADOW_DAILY_ROOT).join(date);
        let _guard = CleanupPath(dir.clone());
        build_atomic_test_bundle(date, "api-run-001");

        let payload = daily_report_payload(date);

        assert_eq!(payload["date"], date);
        assert_eq!(payload["status"], "complete");
        assert_eq!(payload["run_id"], "api-run-001");
        assert_eq!(payload["sample_size"]["result"]["statistics"]["n"], 67);
        assert_eq!(
            payload["report"]["result"]["executive_summary"]["recommendation"],
            "collect"
        );
    }

    #[test]
    fn daily_report_payload_rejects_tampered_atomic_artifact_without_flat_fallback() {
        let date = "2099-12-30";
        let dir = PathBuf::from(ACTIVE_SHADOW_DAILY_ROOT).join(date);
        let _guard = CleanupPath(dir.clone());
        build_atomic_test_bundle(date, "api-run-002");
        fs::write(
            dir.join("runs/api-run-002/final_report.json"),
            r#"{"tampered":true}"#,
        )
        .expect("tamper test artifact");
        fs::write(
            dir.join("final_report.json"),
            r#"{"obsolete_flat_path":true}"#,
        )
        .expect("obsolete flat artifact");

        let payload = daily_report_payload(date);

        assert_eq!(payload["status"], "atomic_bundle_invalid");
        assert!(payload["report"].is_null());
        assert!(payload["artifacts"].as_array().is_some_and(Vec::is_empty));
    }

    #[test]
    fn daily_report_payload_prefers_verified_shadow_bundle() {
        let date = "2099-12-29";
        let primary_dir = PathBuf::from(PRIMARY_DAILY_ROOT).join(date);
        let shadow_dir = PathBuf::from(ACTIVE_SHADOW_DAILY_ROOT).join(date);
        let _primary_guard = CleanupPath(primary_dir);
        let _shadow_guard = CleanupPath(shadow_dir);
        build_atomic_test_bundle_at(
            PRIMARY_DAILY_ROOT,
            date,
            "api-primary-001",
            polyedge_config::RuntimeRole::Primary,
        );
        build_atomic_test_bundle_at(
            ACTIVE_SHADOW_DAILY_ROOT,
            date,
            "api-shadow-001",
            polyedge_config::RuntimeRole::ProfitabilityShadow,
        );

        let payload = daily_report_payload(date);

        assert_eq!(payload["status"], "complete");
        assert_eq!(payload["run_id"], "api-shadow-001");
        assert!(payload["source"]
            .as_str()
            .is_some_and(|source| source.contains(ACTIVE_SHADOW_DAILY_ROOT)));
        let artifact_id = payload["artifacts"]
            .as_array()
            .and_then(|artifacts| {
                artifacts
                    .iter()
                    .find(|artifact| artifact["path"] == "final_report.json")
            })
            .and_then(|artifact| artifact["artifact_id"].as_str())
            .expect("shadow artifact id");
        assert!(artifact_id.starts_with(
            "shadow~campaigns~campaign-2026-07-22~daily~2099-12-29~runs~api-shadow-001~"
        ));
        let artifact_path = PathBuf::from(REPORT_ROOT).join(artifact_id.replace('~', "/"));
        let artifact = artifact_payload(&artifact_path)
            .expect("read shadow artifact")
            .expect("shadow artifact exists");
        assert_eq!(
            artifact["content"]["result"]["executive_summary"]["recommendation"],
            "collect"
        );
    }

    #[test]
    fn invalid_shadow_bundle_does_not_fall_back_to_primary() {
        let date = "2099-12-28";
        let primary_dir = PathBuf::from(PRIMARY_DAILY_ROOT).join(date);
        let shadow_dir = PathBuf::from(ACTIVE_SHADOW_DAILY_ROOT).join(date);
        let _primary_guard = CleanupPath(primary_dir);
        let _shadow_guard = CleanupPath(shadow_dir.clone());
        build_atomic_test_bundle_at(
            PRIMARY_DAILY_ROOT,
            date,
            "api-primary-002",
            polyedge_config::RuntimeRole::Primary,
        );
        build_atomic_test_bundle_at(
            ACTIVE_SHADOW_DAILY_ROOT,
            date,
            "api-shadow-002",
            polyedge_config::RuntimeRole::ProfitabilityShadow,
        );
        fs::write(
            shadow_dir.join("runs/api-shadow-002/final_report.json"),
            r#"{"tampered":true}"#,
        )
        .expect("tamper shadow artifact");

        let payload = daily_report_payload(date);

        assert_eq!(payload["status"], "atomic_bundle_invalid");
        assert!(payload["report"].is_null());
        assert!(payload["artifacts"].as_array().is_some_and(Vec::is_empty));
    }

    #[test]
    fn daily_report_payload_rejects_runtime_role_root_mismatch() {
        let date = "2099-12-27";
        let shadow_dir = PathBuf::from(ACTIVE_SHADOW_DAILY_ROOT).join(date);
        let _shadow_guard = CleanupPath(shadow_dir);
        build_atomic_test_bundle_at(
            ACTIVE_SHADOW_DAILY_ROOT,
            date,
            "api-shadow-role-mismatch",
            polyedge_config::RuntimeRole::Primary,
        );

        let payload = daily_report_payload(date);

        assert_eq!(payload["status"], "atomic_bundle_invalid");
        assert_eq!(
            payload["detail"],
            "Atomic run manifest runtime role does not match its daily root"
        );
    }

    #[test]
    fn legacy_shadow_is_display_only_and_reset_boundary_is_ineligible() {
        let legacy_date = "2026-07-20";
        let legacy_dir = PathBuf::from(SHADOW_DAILY_ROOT).join(legacy_date);
        let _legacy_guard = CleanupPath(legacy_dir);
        build_atomic_test_bundle_at(
            SHADOW_DAILY_ROOT,
            legacy_date,
            "api-legacy-display-only",
            polyedge_config::RuntimeRole::ProfitabilityShadow,
        );

        let legacy = daily_report_payload(legacy_date);
        assert_eq!(legacy["status"], "historical_ineligible");
        assert_eq!(legacy["campaign_id"], LEGACY_SHADOW_CAMPAIGN_ID);
        assert_eq!(legacy["active_campaign_id"], ACTIVE_SHADOW_CAMPAIGN_ID);
        assert_eq!(legacy["bundle_status"], "complete");
        assert_eq!(legacy["evidence_eligibility"], "historical_ineligible");

        let reset = daily_report_payload("2026-07-21");
        assert_eq!(reset["status"], "historical_ineligible");
        assert!(reset["report"].is_null());
    }

    #[test]
    fn latest_report_awaits_then_uses_only_active_campaign_pointer() {
        let date = "2099-12-26";
        let legacy_date = "2026-07-20";
        let primary_date_dir = PathBuf::from(PRIMARY_DAILY_ROOT).join(legacy_date);
        let legacy_date_dir = PathBuf::from(SHADOW_DAILY_ROOT).join(legacy_date);
        let active_date_dir = PathBuf::from(ACTIVE_SHADOW_DAILY_ROOT).join(date);
        let _primary_date_guard = CleanupPath(primary_date_dir);
        let _legacy_date_guard = CleanupPath(legacy_date_dir);
        let _active_date_guard = CleanupPath(active_date_dir.clone());
        let _primary_pointer_guard =
            RestoreFile::new(PathBuf::from(PRIMARY_DAILY_ROOT).join("latest.json"));
        let _legacy_pointer_guard =
            RestoreFile::new(PathBuf::from(SHADOW_DAILY_ROOT).join("latest.json"));
        let active_pointer_path = PathBuf::from(ACTIVE_SHADOW_DAILY_ROOT).join("latest.json");
        let _active_pointer_guard = RestoreFile::new(active_pointer_path.clone());
        let _ = fs::remove_file(&active_pointer_path);
        build_atomic_test_bundle_at(
            PRIMARY_DAILY_ROOT,
            legacy_date,
            "api-primary-global",
            polyedge_config::RuntimeRole::Primary,
        );
        write_global_pointer(PRIMARY_DAILY_ROOT, legacy_date);
        build_atomic_test_bundle_at(
            SHADOW_DAILY_ROOT,
            legacy_date,
            "api-legacy-global",
            polyedge_config::RuntimeRole::ProfitabilityShadow,
        );
        write_global_pointer(SHADOW_DAILY_ROOT, legacy_date);

        let awaiting = read_latest_report_payload();
        assert_eq!(awaiting["status"], "awaiting_first_sealed_day");
        assert!(awaiting["report"].is_null());

        build_atomic_test_bundle_at(
            ACTIVE_SHADOW_DAILY_ROOT,
            date,
            "api-active-global",
            polyedge_config::RuntimeRole::ProfitabilityShadow,
        );
        write_global_pointer(ACTIVE_SHADOW_DAILY_ROOT, date);

        let active = read_latest_report_payload();
        assert_eq!(active["status"], "complete");
        assert_eq!(active["run_id"], "api-active-global");
        assert!(active["source"]
            .as_str()
            .is_some_and(|source| source.contains(ACTIVE_SHADOW_DAILY_ROOT)));

        fs::write(
            active_date_dir.join("runs/api-active-global/final_report.json"),
            r#"{"tampered":true}"#,
        )
        .expect("tamper global shadow artifact");
        let invalid = read_latest_report_payload();
        assert_eq!(invalid["status"], "atomic_bundle_invalid");
        assert!(invalid["report"].is_null());
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

    fn test_promotion_manifest(
        created_at: DateTime<Utc>,
        phase: PromotionPhase,
        shadow_gates_passed: bool,
    ) -> PromotionManifestV1 {
        let quality = DataQualitySummary::new(2, Decimal::ONE, Vec::new(), Vec::new());
        let metrics = ProfitabilityMetrics {
            observed_calendar_days: 0,
            clean_days: 0,
            settled_markets: 0,
            wallet_constrained: true,
            queue_conservative: true,
            wallet_constrained_net_pnl: Decimal::ZERO,
            wallet_constrained_ending_equity: Decimal::ZERO,
            queue_conservative_net_pnl: Decimal::ZERO,
            pnl_ci_95_low: Decimal::ZERO,
            consecutive_positive_weekly_blocks: 0,
            max_drawdown: Decimal::ZERO,
            drawdown_limit: Decimal::ONE,
            markout_30s_ci_low: Decimal::ZERO,
            replay_runtime_parity: true,
            decision_parity_rate: Decimal::ONE,
            execution_model_protocol_version: 3,
            execution_model_eligible_orders: 0,
            execution_model_filled_orders: 0,
            execution_model_non_filled_orders: 0,
            execution_model_brier_improvement: Decimal::ZERO,
            execution_model_expected_calibration_error: Decimal::ONE,
            execution_model_promotion_ready: false,
            execution_model_markout_30s_lower_95: Decimal::ZERO,
            data_quality: quality,
            missing_metrics: Vec::new(),
        };
        let candidate = CandidateIdentity {
            name: "dynamic_quote_style".to_owned(),
            candidate_version: "dynamic_quote_style@2026-06-14".to_owned(),
            config_hash: format!("sha256:{}", "a".repeat(64)),
        };
        let mut artifact_uris = BTreeMap::new();
        artifact_uris.insert(
            "campaign_contract".to_owned(),
            ACTIVE_SHADOW_CAMPAIGN_CONTRACT_PATH.to_owned(),
        );
        artifact_uris.insert(
            "shadow_campaign_id".to_owned(),
            ACTIVE_SHADOW_CAMPAIGN_ID.to_owned(),
        );
        artifact_uris.insert(
            "campaign_contract_sha256".to_owned(),
            active_shadow_campaign_contract()
                .expect("active test campaign contract")
                .sha256,
        );
        artifact_uris.insert(
            "shadow_daily_root".to_owned(),
            ACTIVE_SHADOW_DAILY_ROOT.to_owned(),
        );
        artifact_uris.insert(
            "shadow_prospective_result".to_owned(),
            ACTIVE_SHADOW_PROSPECTIVE_PATH.to_owned(),
        );
        artifact_uris.insert(
            "shadow_profitability_result".to_owned(),
            ACTIVE_SHADOW_PROFITABILITY_LATEST.to_owned(),
        );
        PromotionManifestV1::new(
            candidate,
            PromotionEvaluation {
                schema_version: 1,
                phase,
                promotion_allowed: shadow_gates_passed,
                gates: Vec::new(),
                metrics,
            },
            artifact_uris,
            ExecutionModelBinding {
                blob_uri: "azure://account/container/model.json".to_owned(),
                sha256: format!("sha256:{}", "b".repeat(64)),
                model_version: "conservative-execution-prior-v1".to_owned(),
            },
            created_at,
            created_at + Duration::hours(24),
        )
        .expect("valid test promotion manifest")
    }

    fn test_correction_state(status: &str) -> ShadowCorrectionState {
        ShadowCorrectionState {
            schema_version: 1,
            campaign_id: "shadow-profitability-2026-07-12".to_owned(),
            correction_id: "corr-2026-07-13".to_owned(),
            from: chrono::NaiveDate::from_ymd_opt(2026, 7, 13).unwrap(),
            through: chrono::NaiveDate::from_ymd_opt(2026, 7, 13).unwrap(),
            reason: "repair projected cache lineage".to_owned(),
            status: status.to_owned(),
            builder_git_sha: Some("a".repeat(40)),
            started_at: "2026-07-14T00:00:00Z".to_owned(),
            completed_at: (status == "complete").then(|| "2026-07-14T01:00:00Z".to_owned()),
        }
    }

    fn build_atomic_test_bundle(date: &str, run_id: &str) {
        build_atomic_test_bundle_at(
            ACTIVE_SHADOW_DAILY_ROOT,
            date,
            run_id,
            polyedge_config::RuntimeRole::ProfitabilityShadow,
        );
    }

    fn build_atomic_test_bundle_at(
        daily_root: &str,
        date: &str,
        run_id: &str,
        runtime_role: polyedge_config::RuntimeRole,
    ) {
        use chrono::NaiveDate;
        use polyedge_reporting::research::{
            DailyRunManifest, DataQualitySummary, LatestRunPointer, RunArtifact, RunStatus,
        };
        use rust_decimal::Decimal;

        let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        let quality = DataQualitySummary::new(2, Decimal::ONE, Vec::new(), Vec::new());
        let date_dir = PathBuf::from(daily_root).join(date.to_string());
        let run_dir = date_dir.join("runs").join(run_id);
        fs::create_dir_all(&run_dir).expect("atomic test run directory");
        let test_artifacts = [
            (
                "sample_size",
                "sample_size.json",
                br#"{"result":{"statistics":{"n":67}}}"#.as_slice(),
            ),
            (
                "final_report",
                "final_report.json",
                br#"{"result":{"executive_summary":{"recommendation":"collect"}}}"#.as_slice(),
            ),
        ];
        let mut artifacts = BTreeMap::new();
        for (name, relative_path, bytes) in test_artifacts {
            fs::write(run_dir.join(relative_path), bytes).expect("atomic test artifact");
            artifacts.insert(
                name.to_owned(),
                RunArtifact {
                    name: name.to_owned(),
                    relative_path: relative_path.to_owned(),
                    sha256: sha256_hex(bytes),
                    bytes: bytes.len() as u64,
                },
            );
        }
        let now = Utc::now();
        let manifest = DailyRunManifest {
            schema_version: 2,
            git_sha: Some("a".repeat(40)),
            runtime_role: Some(runtime_role),
            date,
            run_id: run_id.to_owned(),
            created_at: now,
            completed_at: Some(now),
            input_sha256: "a".repeat(64),
            status: RunStatus::Complete,
            artifacts,
            data_quality: quality,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
        fs::write(run_dir.join("run_manifest.json"), &manifest_bytes).unwrap();
        let pointer = LatestRunPointer {
            schema_version: 1,
            date,
            run_id: run_id.to_owned(),
            manifest_path: format!("runs/{run_id}/run_manifest.json"),
            manifest_sha256: sha256_hex(&manifest_bytes),
            promoted_at: now,
        };
        fs::write(
            date_dir.join("latest.json"),
            serde_json::to_vec_pretty(&pointer).unwrap(),
        )
        .unwrap();
    }

    fn write_global_pointer(daily_root: &str, date: &str) {
        let date_pointer_path = PathBuf::from(daily_root).join(date).join("latest.json");
        let mut pointer: LatestRunPointer = serde_json::from_slice(
            &fs::read(date_pointer_path).expect("read date pointer for global pointer"),
        )
        .expect("parse date pointer for global pointer");
        pointer.manifest_path = format!("{date}/{}", pointer.manifest_path);
        let root = PathBuf::from(daily_root);
        fs::create_dir_all(&root).expect("create global daily root");
        fs::write(
            root.join("latest.json"),
            serde_json::to_vec_pretty(&pointer).unwrap(),
        )
        .expect("write global pointer");
    }

    struct CleanupPath(PathBuf);

    impl Drop for CleanupPath {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct RestoreFile {
        path: PathBuf,
        original: Option<Vec<u8>>,
    }

    impl RestoreFile {
        fn new(path: PathBuf) -> Self {
            let original = fs::read(&path).ok();
            Self { path, original }
        }
    }

    impl Drop for RestoreFile {
        fn drop(&mut self) {
            if let Some(original) = &self.original {
                let _ = fs::write(&self.path, original);
            } else {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}
