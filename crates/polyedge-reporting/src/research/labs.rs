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
/// First snapshot backed by the immutable projected-day campaign chain.
/// Legacy schema-v1 wallets remain readable only before this date.
const PROJECTED_WALLET_PROTOCOL_CUTOFF: &str = "2026-07-13";
const CAMPAIGN_BOUND_WALLET_SCHEMA_VERSION: u64 = 3;
const SHADOW_CAMPAIGN_CONTRACT_SCHEMA_VERSION: u32 = 1;
const SHADOW_EVIDENCE_PROTOCOL_VERSION: u32 = 3;

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
    pub campaign_manifest: PathBuf,
    /// Optional immutable campaign contract. Historical callers may omit this
    /// and retain the schema-v2 July-2026 wallet contract. New protocol-v3
    /// campaigns must provide it and receive a schema-v3 snapshot.
    pub campaign_contract: Option<PathBuf>,
    pub snapshot_date: NaiveDate,
    pub out: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShadowCampaignContract {
    pub schema_version: u32,
    pub evidence_protocol_version: u32,
    pub campaign_id: String,
    pub start_date: NaiveDate,
    pub first_eligible_date: NaiveDate,
    pub terminal_date: NaiveDate,
    pub wallet_scope: String,
    pub wallet_baseline: Decimal,
    pub equity_floor: Decimal,
    pub maximum_drawdown: Decimal,
    pub event_blob_prefix: String,
    pub projected_cache_root: String,
    pub daily_root: String,
    pub prospective_path: String,
    pub correction_root: String,
    pub profitability_path: String,
    pub lease_blob: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShadowCampaignContractBinding {
    pub contract: ShadowCampaignContract,
    pub sha256: String,
}

impl ShadowCampaignContractBinding {
    pub fn campaign_id(&self) -> &str {
        &self.contract.campaign_id
    }
}

fn bind_campaign_contract(
    value: &mut Value,
    binding: Option<&ShadowCampaignContractBinding>,
) -> Result<(), ResearchError> {
    let Some(binding) = binding else {
        return Ok(());
    };
    let object = value.as_object_mut().ok_or_else(|| {
        ResearchError::InvalidInput("wallet campaign binding target must be an object".to_owned())
    })?;
    let contract = &binding.contract;
    object.insert(
        "evidence_protocol_version".to_owned(),
        json!(contract.evidence_protocol_version),
    );
    object.insert("campaign_id".to_owned(), json!(contract.campaign_id));
    object.insert("campaign_contract_sha256".to_owned(), json!(binding.sha256));
    object.insert(
        "campaign_first_eligible_date".to_owned(),
        json!(contract.first_eligible_date),
    );
    object.insert(
        "campaign_terminal_date".to_owned(),
        json!(contract.terminal_date),
    );
    object.insert(
        "campaign_baseline".to_owned(),
        json!(contract.wallet_baseline),
    );
    Ok(())
}

/// Binds the wallet ledger produced by the cumulative replay to the exact
/// normalized input manifest. The resulting file is included in the day's
/// immutable bundle; profitability refuses daily reset wallet metrics.
pub fn run_build_cumulative_wallet_snapshot(
    options: CumulativeWalletSnapshotOptions,
) -> Result<Value, ResearchError> {
    let campaign_contract = options
        .campaign_contract
        .as_deref()
        .map(load_shadow_campaign_contract)
        .transpose()?;
    let regimes_bytes = fs::read(&options.regimes)?;
    let regimes: Value = serde_json::from_slice(&regimes_bytes)?;
    let campaign_manifest_bytes = fs::read(&options.campaign_manifest)?;
    let campaign = read_verified_campaign_index(&options.campaign_manifest)?;
    let campaign_manifest_sha256 = format!("sha256:{}", sha256_hex(&campaign_manifest_bytes));
    if regimes
        .pointer("/result/projected_campaign_manifest_sha256")
        .and_then(Value::as_str)
        != Some(campaign_manifest_sha256.as_str())
    {
        return Err(ResearchError::InvalidInput(
            "cumulative regimes are not bound to the exact projected campaign manifest".to_owned(),
        ));
    }
    if campaign.through != options.snapshot_date {
        return Err(ResearchError::InvalidInput(format!(
            "projected campaign cutoff {} does not match wallet snapshot {}",
            campaign.through, options.snapshot_date
        )));
    }
    if let Some(binding) = &campaign_contract {
        let contract = &binding.contract;
        if campaign.campaign_id != contract.campaign_id
            || campaign.since != contract.first_eligible_date
            || options.snapshot_date < contract.first_eligible_date
            || options.snapshot_date > contract.terminal_date
        {
            return Err(ResearchError::InvalidInput(
                "projected campaign identity or date range does not match the immutable shadow campaign contract"
                    .to_owned(),
            ));
        }
    }
    let normalized_events = campaign.total_events;
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
    if let Some(binding) = &campaign_contract {
        let constraints = &profile["wallet_constraints"];
        let contract = &binding.contract;
        if decimal_from_value(&constraints["campaign_baseline"]) != Some(contract.wallet_baseline)
            || decimal_from_value(&constraints["equity_floor"]) != Some(contract.equity_floor)
            || decimal_from_value(&constraints["maximum_drawdown"])
                != Some(contract.maximum_drawdown)
        {
            return Err(ResearchError::InvalidInput(
                "cumulative replay wallet constraints do not match the immutable shadow campaign contract"
                    .to_owned(),
            ));
        }
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
    let schema_version = campaign_contract
        .as_ref()
        .map_or(2, |_| CAMPAIGN_BOUND_WALLET_SCHEMA_VERSION);
    let wallet_scope = campaign_contract
        .as_ref()
        .map(|binding| binding.contract.wallet_scope.as_str())
        .unwrap_or(CUMULATIVE_WALLET_SCOPE);
    let campaign_start = campaign_contract
        .as_ref()
        .map(|binding| binding.contract.start_date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| WALLET_CAMPAIGN_START.to_owned());
    let mut canonical_state = json!({
        "schema_version": schema_version,
        "wallet_scope": wallet_scope,
        "campaign_start": campaign_start,
        "snapshot_date": options.snapshot_date.format("%Y-%m-%d").to_string(),
        "cumulative_input_sha256": campaign.canonical_sha256.clone(),
        "candidate": "dynamic_quote_style",
        "fill_model": regimes.pointer("/result/fill_model").cloned().unwrap_or(Value::Null),
        "cumulative_events": cumulative_events,
        "wallet_constrained": true,
        "wallet_constrained_net_pnl": profile["wallet_constrained_net_pnl"].clone(),
        "wallet_constrained_ending_equity": profile["wallet_constrained_ending_equity"].clone(),
        "wallet_constrained_max_drawdown": profile["wallet_constrained_max_drawdown"].clone(),
        "wallet_constrained_accepted_orders": profile["wallet_constrained_accepted_orders"].clone(),
        "wallet_constrained_skipped_orders": profile["wallet_constrained_skipped_orders"].clone(),
        "wallet_constrained_accepted_filled_orders": profile["wallet_constrained_accepted_filled_orders"].clone(),
        "wallet_constrained_unresolved_orders": profile["wallet_constrained_unresolved_orders"].clone(),
        "wallet_constrained_skip_reasons": profile["wallet_constrained_skip_reasons"].clone(),
        "wallet_constrained_equity_curve": profile["wallet_constrained_equity_curve"].clone(),
        "wallet_constraints": profile["wallet_constraints"].clone()
    });
    bind_campaign_contract(&mut canonical_state, campaign_contract.as_ref())?;
    let canonical_state_bytes = serde_json::to_vec(&canonical_state)?;
    let campaign_parent_input_sha256 = campaign
        .segments
        .last()
        .and_then(|segment| segment.parent_chain_sha256.clone());
    let mut snapshot = json!({
        "schema_version": schema_version,
        "wallet_scope": wallet_scope,
        "campaign_start": campaign_start,
        "snapshot_date": options.snapshot_date.format("%Y-%m-%d").to_string(),
        "cumulative_input_sha256": campaign.canonical_sha256.clone(),
        "cumulative_parent_input_sha256": campaign_parent_input_sha256,
        "cumulative_input_manifest_sha256": campaign_manifest_sha256,
        "cumulative_state_sha256": format!("sha256:{}", sha256_hex(&canonical_state_bytes)),
        "cumulative_regimes_artifact_sha256": format!("sha256:{}", sha256_hex(&regimes_bytes)),
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
    bind_campaign_contract(&mut snapshot, campaign_contract.as_ref())?;
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
    // Statistical evidence is drawn only from the current contiguous clean
    // suffix. Dirty bootstrap/restart days stay visible in `rows`, but cannot
    // contribute markets, PnL, parity, markouts, or confidence bounds toward
    // promotion.
    let clean_rows = current_clean_suffix(&rows);
    let paired_improvement = paired_improvement_summary(clean_rows);
    let clean_rows_sha256 = format!("sha256:{}", sha256_hex(&serde_json::to_vec(clean_rows)?));
    let status = if rows.is_empty() {
        "collecting"
    } else {
        "tracking"
    };
    let result = json!({
        "status": status,
        "since": ts(options.since),
        "rows": rows,
        "eligible_clean_dates": clean_rows.iter().filter_map(|row| row["date"].as_str()).collect::<Vec<_>>(),
        "eligible_clean_rows": clean_rows.len(),
        "eligible_clean_rows_sha256": clean_rows_sha256,
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
    let config = load_profitability_gate(&options.gate_config)?;
    // `stopped_no_go` is an absorbing terminal state. Once the canonical
    // manifest reaches it, later data or model recomputation cannot silently
    // resurrect the candidate. A new candidate/version must use a new state.
    let existing_manifest = read_local_or_azure_json(&options.out)?
        .map(serde_json::from_value::<PromotionManifestV1>)
        .transpose()?;
    if let (Some(existing), Some(binding)) = (&existing_manifest, &config.campaign_contract) {
        let artifacts = &existing.artifact_uris;
        if artifacts.get("shadow_campaign_id") != Some(&binding.contract.campaign_id)
            || artifacts.get("campaign_contract_sha256") != Some(&binding.sha256)
            || artifacts.get("shadow_daily_root") != Some(&binding.contract.daily_root)
            || artifacts.get("shadow_prospective_result")
                != Some(&binding.contract.prospective_path)
            || artifacts.get("shadow_profitability_result")
                != Some(&binding.contract.profitability_path)
        {
            return Err(ResearchError::InvalidInput(
                "existing profitability state does not match the immutable shadow campaign contract"
                    .to_owned(),
            ));
        }
    }
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
    if let Some(binding) = &config.campaign_contract {
        let contract = &binding.contract;
        let normalized = |path: &Path| path.to_string_lossy().replace('\\', "/");
        if normalized(&options.daily_root) != contract.daily_root
            || normalized(&options.prospective) != contract.prospective_path
            || normalized(&options.out) != contract.profitability_path
        {
            return Err(ResearchError::InvalidInput(
                "profitability inputs and output do not match the immutable shadow campaign roots"
                    .to_owned(),
            ));
        }
    }
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
    let metrics = aggregate_profitability_metrics(
        &rows,
        &prospective,
        &execution_model,
        &config.thresholds,
        config.campaign_contract.as_ref(),
    );
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
    if let Some(binding) = &config.campaign_contract {
        manifest.artifact_uris.extend(BTreeMap::from([
            (
                "campaign_contract".to_owned(),
                options.gate_config.to_string_lossy().into_owned(),
            ),
            (
                "campaign_contract_sha256".to_owned(),
                binding.sha256.clone(),
            ),
            (
                "shadow_campaign_id".to_owned(),
                binding.contract.campaign_id.clone(),
            ),
            (
                "shadow_daily_root".to_owned(),
                binding.contract.daily_root.clone(),
            ),
            (
                "shadow_prospective_result".to_owned(),
                binding.contract.prospective_path.clone(),
            ),
            (
                "shadow_profitability_result".to_owned(),
                binding.contract.profitability_path.clone(),
            ),
        ]));
    }
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
    let local = load_local_daily_prospective_rows(reports_dir, since)?;
    let azure = load_azure_daily_prospective_rows(reports_dir, since)?;
    merge_daily_prospective_rows(local, azure)
}

fn merge_daily_prospective_rows(
    local: Vec<Value>,
    azure: Vec<Value>,
) -> Result<Vec<Value>, ResearchError> {
    let mut by_date = BTreeMap::new();
    for (source, rows) in [("azure", azure), ("local", local)] {
        for row in rows {
            let date = row
                .get("date")
                .and_then(Value::as_str)
                .filter(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok())
                .ok_or_else(|| {
                    ResearchError::InvalidInput(format!(
                        "{source} prospective row has no valid UTC date"
                    ))
                })?
                .to_owned();
            by_date.insert(date, row);
        }
    }
    Ok(by_date.into_values().collect())
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
        let (source_dir, manifest_quality, runtime_role) = if atomic_marker_present {
            match inspect_daily_dependency(reports_dir, report_date)? {
                DailyDependency::Ready {
                    bundle_dir,
                    manifest,
                    ..
                } => (
                    bundle_dir,
                    Some(manifest.data_quality.clone()),
                    manifest.runtime_role.clone(),
                ),
                DailyDependency::WaitingForDependency { reason, .. } => {
                    return Err(ResearchError::InvalidInput(format!(
                        "atomic daily bundle {date} is not verified: {reason}"
                    )))
                }
            }
        } else if legacy_daily_fallback_allowed(report_date, false) {
            (date_dir, None, None)
        } else {
            return Err(ResearchError::InvalidInput(format!(
                "atomic daily bundle is required on or after {ATOMIC_DAILY_PROTOCOL_CUTOFF}: {date}"
            )));
        };
        rows.push(daily_prospective_row(
            &date,
            &source_dir,
            manifest_quality,
            runtime_role,
        )?);
    }
    Ok(rows)
}

fn daily_prospective_row(
    date: &str,
    dir: &Path,
    manifest_quality: Option<DataQualitySummary>,
    runtime_role: Option<polyedge_config::RuntimeRole>,
) -> Result<Value, ResearchError> {
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
            manifest_quality,
            runtime_role,
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
            } => {
                let manifest_quality = Some(manifest.data_quality.clone());
                let runtime_role = manifest.runtime_role.clone();
                rows.push(daily_prospective_row_from_reports(
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
                        manifest_quality,
                        runtime_role,
                    },
                )?)
            }
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
                        manifest_quality: None,
                        runtime_role: None,
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
    if super::daily_provenance_required(pointer.date) && manifest.schema_version != 2 {
        return Ok(AzureDailyBundleState::Invalid {
            reason: "manifest_schema_downgrade".to_owned(),
        });
    }
    if manifest.schema_version == 2
        && !manifest
            .git_sha
            .as_deref()
            .is_some_and(polyedge_config::is_full_git_sha)
    {
        return Ok(AzureDailyBundleState::Invalid {
            reason: "manifest_git_sha_invalid".to_owned(),
        });
    }
    if manifest.schema_version == 2 && manifest.runtime_role.is_none() {
        return Ok(AzureDailyBundleState::Invalid {
            reason: "manifest_runtime_role_missing".to_owned(),
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
    manifest_quality: Option<DataQualitySummary>,
    runtime_role: Option<polyedge_config::RuntimeRole>,
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
        DailyRowEvidence {
            sample: documents.sample_size.as_ref(),
            audit: documents.audit.as_ref(),
            execution_quality: documents.execution_quality.as_ref(),
            cumulative_wallet: documents.cumulative_wallet.as_ref(),
            manifest_quality: documents.manifest_quality.as_ref(),
            runtime_role: documents.runtime_role.as_ref(),
        },
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

struct DailyRowEvidence<'a> {
    sample: Option<&'a Value>,
    audit: Option<&'a Value>,
    execution_quality: Option<&'a Value>,
    cumulative_wallet: Option<&'a Value>,
    manifest_quality: Option<&'a DataQualitySummary>,
    runtime_role: Option<&'a polyedge_config::RuntimeRole>,
}

fn json_row(
    date: &str,
    reports: DailyReportSources<'_>,
    evidence: DailyRowEvidence<'_>,
) -> Result<Value, ResearchError> {
    let DailyRowEvidence {
        sample,
        audit,
        execution_quality,
        cumulative_wallet,
        manifest_quality,
        runtime_role,
    } = evidence;
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
    let quality_summary = manifest_quality
        .cloned()
        .or_else(|| audit.map(quality_from_audit));
    let quality = quality_summary
        .as_ref()
        .map(manifest_quality_status)
        .unwrap_or_else(|| data_quality_status(audit));
    let quality_reasons = quality_summary
        .as_ref()
        .map(manifest_quality_reasons)
        .unwrap_or_else(|| data_quality_reasons(audit));
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
        "wallet_campaign_id": cumulative_wallet.and_then(|wallet| wallet["campaign_id"].as_str()),
        "wallet_campaign_contract_sha256": cumulative_wallet.and_then(|wallet| wallet["campaign_contract_sha256"].as_str()),
        "wallet_campaign_start": cumulative_wallet.and_then(|wallet| wallet["campaign_start"].as_str()),
        "wallet_campaign_first_eligible_date": cumulative_wallet.and_then(|wallet| wallet["campaign_first_eligible_date"].as_str()),
        "wallet_campaign_terminal_date": cumulative_wallet.and_then(|wallet| wallet["campaign_terminal_date"].as_str()),
        "wallet_campaign_baseline": cumulative_wallet.and_then(|wallet| value_to_string(&wallet["campaign_baseline"])),
        "wallet_evidence_protocol_version": cumulative_wallet.and_then(|wallet| wallet["evidence_protocol_version"].as_u64()),
        "wallet_snapshot_date": cumulative_wallet.and_then(|wallet| wallet["snapshot_date"].as_str()),
        "wallet_schema_version": cumulative_wallet.and_then(|wallet| wallet["schema_version"].as_u64()),
        "cumulative_input_sha256": cumulative_wallet.and_then(|wallet| wallet["cumulative_input_sha256"].as_str()),
        "cumulative_parent_input_sha256": cumulative_wallet.and_then(|wallet| wallet["cumulative_parent_input_sha256"].as_str()),
        "cumulative_input_manifest_sha256": cumulative_wallet.and_then(|wallet| wallet["cumulative_input_manifest_sha256"].as_str()),
        "cumulative_state_sha256": cumulative_wallet.and_then(|wallet| wallet["cumulative_state_sha256"].as_str()),
        "cumulative_regimes_artifact_sha256": cumulative_wallet.and_then(|wallet| wallet["cumulative_regimes_artifact_sha256"].as_str()),
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
        "runtime_role": runtime_role.map(polyedge_config::RuntimeRole::as_str),
        "wallet_constrained": dynamic_wallet_constrained,
        "decision_parity_rate": number_at(&source, &["/result/decision_parity_rate", "/decision_parity_rate"])
            .or_else(|| audit.and_then(|report| number_at(report, &["/result/decision_parity_rate", "/decision_parity_rate"]))),
        "decision_config_sha256": audit.and_then(|report| {
            report.pointer("/result/decision_config_sha256")
                .or_else(|| report.get("decision_config_sha256"))
                .and_then(Value::as_str)
        }),
        "decision_metadata_coverage": audit.and_then(|report| number_at(report, &["/result/decision_metadata_coverage", "/decision_metadata_coverage"])),
        "decision_grade_coverage": audit.and_then(|report| number_at(report, &["/result/decision_grade_coverage", "/decision_grade_coverage"])),
        "final_decision_grade_coverage": audit.and_then(|report| number_at(report, &["/result/final_decision_grade_coverage", "/final_decision_grade_coverage"])),
        "decision_grade_decision_coverage": audit.and_then(|report| number_at(report, &["/result/final_decision_grade_coverage", "/final_decision_grade_coverage"])),
        "execution_field_coverage": audit.and_then(|report| number_at(report, &["/result/execution_field_coverage", "/execution_field_coverage"])),
        "markout_30s_ci_low": execution_quality.and_then(|report| {
            report.pointer("/result/markouts/30/executable/ci_95_low")
                .or_else(|| report.pointer("/result/markouts/30/executable_markout_ci_95_low"))
        }).cloned(),
        "markout_30s_mean": execution_quality.and_then(|report| report.pointer("/result/markouts/30/executable/mean")).cloned(),
        "markout_30s_sample_std": execution_quality.and_then(|report| report.pointer("/result/markouts/30/executable/sample_std")).cloned(),
        "markout_30s_sample_size": execution_quality.and_then(|report| report.pointer("/result/markouts/30/executable/count")).cloned(),
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
    campaign_contract: Option<ShadowCampaignContractBinding>,
}

pub fn load_shadow_campaign_contract(
    path: &Path,
) -> Result<ShadowCampaignContractBinding, ResearchError> {
    let text = fs::read_to_string(path)?;
    let values = flatten_simple_yaml(&text);
    parse_shadow_campaign_contract(&values)?.ok_or_else(|| {
        ResearchError::InvalidInput(
            "profitability gate has no immutable shadow campaign contract".to_owned(),
        )
    })
}

fn parse_shadow_campaign_contract(
    values: &BTreeMap<String, String>,
) -> Result<Option<ShadowCampaignContractBinding>, ResearchError> {
    if !values.contains_key("campaign.id") {
        return Ok(None);
    }
    let required = |key: &str| {
        values.get(key).cloned().ok_or_else(|| {
            ResearchError::InvalidInput(format!("shadow campaign contract is missing {key}"))
        })
    };
    let parse_u32 = |key: &str| -> Result<u32, ResearchError> {
        required(key)?.parse().map_err(|_| {
            ResearchError::InvalidInput(format!("shadow campaign contract has invalid {key}"))
        })
    };
    let parse_date = |key: &str| -> Result<NaiveDate, ResearchError> {
        NaiveDate::parse_from_str(&required(key)?, "%Y-%m-%d").map_err(|_| {
            ResearchError::InvalidInput(format!("shadow campaign contract has invalid {key}"))
        })
    };
    let parse_decimal = |key: &str| -> Result<Decimal, ResearchError> {
        required(key)?.parse().map_err(|_| {
            ResearchError::InvalidInput(format!("shadow campaign contract has invalid {key}"))
        })
    };
    let contract = ShadowCampaignContract {
        schema_version: parse_u32("campaign.schema_version")?,
        evidence_protocol_version: parse_u32("campaign.evidence_protocol_version")?,
        campaign_id: required("campaign.id")?,
        start_date: parse_date("campaign.start_date")?,
        first_eligible_date: parse_date("campaign.first_eligible_date")?,
        terminal_date: parse_date("campaign.terminal_date")?,
        wallet_scope: required("campaign.wallet_scope")?,
        wallet_baseline: parse_decimal("campaign.wallet_baseline")?,
        equity_floor: parse_decimal("campaign.equity_floor")?,
        maximum_drawdown: parse_decimal("campaign.maximum_drawdown")?,
        event_blob_prefix: required("campaign.event_blob_prefix")?,
        projected_cache_root: required("campaign.projected_cache_root")?,
        daily_root: required("campaign.daily_root")?,
        prospective_path: required("campaign.prospective_path")?,
        correction_root: required("campaign.correction_root")?,
        profitability_path: required("campaign.profitability_path")?,
        lease_blob: required("campaign.lease_blob")?,
    };
    validate_shadow_campaign_contract(&contract)?;
    let canonical = serde_json::to_vec(&contract)?;
    Ok(Some(ShadowCampaignContractBinding {
        contract,
        sha256: format!("sha256:{}", sha256_hex(&canonical)),
    }))
}

fn validate_shadow_campaign_contract(
    contract: &ShadowCampaignContract,
) -> Result<(), ResearchError> {
    let valid_id = !contract.campaign_id.is_empty()
        && contract
            .campaign_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    let expected_days = (contract.terminal_date - contract.start_date).num_days() + 1;
    let scoped_paths = [
        &contract.event_blob_prefix,
        &contract.projected_cache_root,
        &contract.daily_root,
        &contract.prospective_path,
        &contract.correction_root,
        &contract.profitability_path,
        &contract.lease_blob,
    ];
    if contract.schema_version != SHADOW_CAMPAIGN_CONTRACT_SCHEMA_VERSION
        || contract.evidence_protocol_version != SHADOW_EVIDENCE_PROTOCOL_VERSION
        || !valid_id
        || contract.start_date != contract.first_eligible_date
        || expected_days != 60
        || contract.wallet_scope != format!("cumulative_since_{}", contract.start_date)
        || contract.wallet_baseline <= Decimal::ZERO
        || contract.equity_floor < Decimal::ZERO
        || contract.equity_floor >= contract.wallet_baseline
        || contract.maximum_drawdown <= Decimal::ZERO
        || scoped_paths.iter().any(|path| {
            path.is_empty()
                || path.starts_with('/')
                || path.split(['/', '\\']).any(|component| component == "..")
                || !path.contains(&contract.campaign_id)
        })
    {
        return Err(ResearchError::InvalidInput(
            "immutable shadow campaign contract is invalid or not fully campaign-scoped".to_owned(),
        ));
    }
    Ok(())
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
    let campaign_contract = parse_shadow_campaign_contract(&values)?;
    let thresholds = PromotionThresholds {
        required_clean_days: parse_u32("shadow.required_clean_days")?,
        maximum_extension_days: parse_u32("shadow.maximum_extension_days")?,
        required_settled_markets: parse_u64("shadow.required_settled_markets")?,
        maximum_extension_markets: parse_u64("shadow.maximum_extension_markets")?,
        required_positive_weekly_blocks: parse_u32("shadow.required_positive_weekly_blocks")?,
        minimum_decision_parity_rate: parse_decimal("shadow.minimum_decision_parity_rate")?,
        minimum_decision_grade_coverage: parse_decimal("shadow.minimum_decision_grade_coverage")?,
        maximum_modeled_drawdown: parse_decimal("shadow.maximum_modeled_drawdown")?,
        maximum_out_of_order_event_rate: parse_decimal("shadow.maximum_out_of_order_event_rate")?,
        execution_model_protocol_version: parse_u32("execution_model.evidence_protocol_version")?,
        minimum_execution_model_eligible_orders: parse_u64(
            "execution_model.minimum_eligible_orders",
        )?,
        minimum_execution_model_filled_orders: parse_u64("execution_model.minimum_filled_orders")?,
        minimum_execution_model_non_filled_orders: parse_u64(
            "execution_model.minimum_non_filled_orders",
        )?,
        minimum_brier_improvement_over_base_rate: parse_decimal(
            "execution_model.minimum_brier_improvement_over_base_rate",
        )?,
        maximum_expected_calibration_error: parse_decimal(
            "execution_model.maximum_expected_calibration_error",
        )?,
    };
    if let Some(binding) = &campaign_contract {
        let contract_days =
            (binding.contract.terminal_date - binding.contract.start_date).num_days() + 1;
        if u32::try_from(contract_days).ok() != Some(thresholds.maximum_extension_days)
            || binding.contract.maximum_drawdown != thresholds.maximum_modeled_drawdown
        {
            return Err(ResearchError::InvalidInput(
                "shadow campaign contract deadline or drawdown does not match profitability thresholds"
                    .to_owned(),
            ));
        }
    }
    Ok(LoadedProfitabilityGate {
        candidate: CandidateIdentity {
            name: required("candidate.name")?,
            candidate_version: required("candidate.version")?,
            config_hash: required("candidate.config_hash")?,
        },
        thresholds,
        shadow_prior_model_version: required("execution_model.shadow_prior_model_version")?,
        shadow_prior_sha256: required("execution_model.shadow_prior_sha256")?,
        campaign_contract,
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
    _prospective: &Value,
    execution_model: &Value,
    thresholds: &PromotionThresholds,
    campaign_contract: Option<&ShadowCampaignContractBinding>,
) -> ProfitabilityMetrics {
    let evidence_rows = current_clean_suffix(rows);
    let mut missing = Vec::new();
    if rows.is_empty() {
        missing.push("complete_daily_rows".to_owned());
    }
    if evidence_rows.is_empty() {
        missing.push("eligible_clean_daily_rows".to_owned());
    }
    let decision_configs = evidence_rows
        .iter()
        .map(|row| {
            row["decision_config_sha256"].as_str().filter(|hash| {
                hash.len() == 71
                    && hash.starts_with("sha256:")
                    && hash[7..]
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        })
        .collect::<Option<Vec<_>>>();
    if decision_configs
        .as_ref()
        .is_none_or(|hashes| hashes.is_empty() || hashes.iter().collect::<BTreeSet<_>>().len() != 1)
    {
        missing.push("frozen_decision_config_sha256".to_owned());
    }
    // If the latest day is dirty, surface its exact blockers. Otherwise the
    // quality summary describes the same clean suffix used by the statistical
    // gates instead of making one old bootstrap day poison the campaign
    // forever.
    let quality_rows = if evidence_rows.is_empty() {
        rows.get(rows.len().saturating_sub(1)..).unwrap_or_default()
    } else {
        evidence_rows
    };
    let qualities = quality_rows
        .iter()
        .filter_map(|row| {
            serde_json::from_value::<DataQualitySummary>(row["data_quality"].clone()).ok()
        })
        .collect::<Vec<_>>();
    if qualities.len() != quality_rows.len() || qualities.is_empty() {
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
    let minimum_component = |selector: fn(&DataQualityCoverageBreakdown) -> Option<Decimal>| {
        qualities
            .iter()
            .map(|quality| selector(&quality.coverage_breakdown))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .min()
    };
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
        coverage_breakdown: DataQualityCoverageBreakdown {
            start_price_capture_rate: minimum_component(|row| row.start_price_capture_rate),
            settlement_rate: minimum_component(|row| row.settlement_rate),
            exact_reference_hour_coverage: minimum_component(|row| {
                row.exact_reference_hour_coverage
            }),
            decision_metadata_coverage: minimum_component(|row| row.decision_metadata_coverage),
            decision_grade_coverage: minimum_component(|row| row.decision_grade_coverage),
            final_decision_grade_coverage: minimum_component(|row| {
                row.final_decision_grade_coverage
            }),
            execution_field_coverage: minimum_component(|row| row.execution_field_coverage),
            decision_parity_rate: minimum_component(|row| row.decision_parity_rate),
            queue_snapshot_coverage: minimum_component(|row| row.queue_snapshot_coverage),
            markout_1s_completion: minimum_component(|row| row.markout_1s_completion),
            markout_5s_completion: minimum_component(|row| row.markout_5s_completion),
            markout_30s_completion: minimum_component(|row| row.markout_30s_completion),
        },
    };
    let clean_days = consecutive_clean_day_streak(rows);

    let settled = evidence_rows
        .iter()
        .map(|row| row["settled_markets"].as_u64())
        .collect::<Option<Vec<_>>>();
    if settled.is_none() {
        missing.push("settled_markets".to_owned());
    }
    let settled_markets = settled.unwrap_or_default().into_iter().sum();

    let daily_pnl = evidence_rows
        .iter()
        .map(|row| decimal_from_value(&row["dynamic_quote_style_net_pnl"]))
        .collect::<Option<Vec<_>>>();
    if daily_pnl.is_none() {
        missing.push("queue_conservative_net_pnl".to_owned());
    }
    let pnl_values = daily_pnl.unwrap_or_default();
    let queue_conservative = !evidence_rows.is_empty()
        && evidence_rows
            .iter()
            .all(|row| row["fill_model"] == "queue_proxy_conservative");
    let queue_pnl: Decimal = pnl_values.iter().copied().sum();
    let cumulative_wallet = validated_cumulative_wallet_snapshots(rows, campaign_contract);
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
    // Reconcile the cumulative replay's intraday drawdown with a second,
    // independent lower bound derived from trusted end-of-day equity. The
    // full wallet chain is used here (including dirty days), so a loss on a
    // day excluded from statistical evidence still counts against the risk
    // gate. Taking the maximum preserves any larger intraday drawdown while
    // preventing an understated stored metric from passing.
    let wallet_max_drawdown = reconciled_wallet_max_drawdown(&wallet_snapshots);
    let wallet_daily_pnl = clean_wallet_daily_increments(evidence_rows, &wallet_snapshots);
    if wallet_daily_pnl.is_none() {
        missing.push("clean_wallet_daily_pnl".to_owned());
    }

    // Recompute the predeclared seven-day block-bootstrap bound from the exact
    // clean suffix. It is a bound on wallet-constrained queue-conservative
    // daily PnL, not on improvement over static or on unconstrained replay PnL.
    // A stale prospective artifact is never an authorization input.
    let pnl_ci = wallet_daily_pnl
        .as_deref()
        .and_then(block_bootstrap_daily_pnl_lower_95);
    if pnl_ci.is_none() {
        missing.push("pnl_ci_95_low".to_owned());
    }
    let markout_ci = block_bootstrap_daily_markout_lower_95(evidence_rows);
    if markout_ci.is_none() {
        missing.push("markout_30s_ci_low".to_owned());
    }
    let parity_values = evidence_rows
        .iter()
        .map(|row| decimal_from_value(&row["decision_parity_rate"]))
        .collect::<Option<Vec<_>>>();
    let parity_rate = parity_values
        .as_ref()
        .and_then(|values| values.iter().copied().min());
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
    // Weekly profitability uses the same capital-realistic ledger increments
    // as the confidence bound. Unfundable shadow intents cannot manufacture a
    // positive week.
    let (consecutive, complete_weekly_blocks) =
        trailing_positive_complete_weekly_blocks(wallet_daily_pnl.as_deref().unwrap_or_default());
    if complete_weekly_blocks == 0 {
        missing.push("weekly_blocks".to_owned());
    }
    missing.sort();
    missing.dedup();
    ProfitabilityMetrics {
        observed_calendar_days: observed_campaign_days(&wallet_snapshots),
        clean_days,
        settled_markets,
        wallet_constrained,
        queue_conservative,
        wallet_constrained_net_pnl: wallet_pnl,
        wallet_constrained_ending_equity: wallet_ending_equity,
        queue_conservative_net_pnl: queue_pnl,
        pnl_ci_95_low: pnl_ci.unwrap_or(Decimal::ZERO),
        // Promotion requires the current trailing run. An old winning streak
        // cannot survive a subsequently losing complete block.
        consecutive_positive_weekly_blocks: consecutive,
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

fn block_bootstrap_daily_pnl_lower_95(values: &[Decimal]) -> Option<Decimal> {
    const BLOCK_DAYS: usize = 7;
    const MIN_BLOCKS: usize = 4;
    const RESAMPLES: usize = 10_000;
    if values.len() < BLOCK_DAYS * MIN_BLOCKS {
        return None;
    }
    let encoded =
        serde_json::to_vec(&values.iter().map(Decimal::to_string).collect::<Vec<_>>()).ok()?;
    let digest = Sha256::digest(encoded);
    let mut seed = u64::from_le_bytes(digest[..8].try_into().ok()?);
    if seed == 0 {
        seed = 0x9e37_79b9_7f4a_7c15;
    }
    let mut estimates = Vec::with_capacity(RESAMPLES);
    for _ in 0..RESAMPLES {
        let mut total = Decimal::ZERO;
        let mut sampled = 0_usize;
        while sampled < values.len() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let start = (seed as usize) % values.len();
            for offset in 0..BLOCK_DAYS.min(values.len() - sampled) {
                total += values[(start + offset) % values.len()];
                sampled += 1;
            }
        }
        estimates.push(total / Decimal::from(values.len() as u64));
    }
    estimates.sort_unstable();
    estimates.get((RESAMPLES * 25) / 1_000).copied()
}

fn clean_wallet_daily_increments(
    evidence_rows: &[Value],
    snapshots: &[CumulativeWalletSnapshot],
) -> Option<Vec<Decimal>> {
    let by_date = snapshots
        .iter()
        .map(|snapshot| (snapshot.date, snapshot))
        .collect::<BTreeMap<_, _>>();
    let first = snapshots.first()?.date;
    evidence_rows
        .iter()
        .map(|row| {
            let date = row["date"]
                .as_str()
                .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())?;
            let current = *by_date.get(&date)?;
            if current.unresolved_orders > 0 {
                return None;
            }
            let previous = if date == first {
                Decimal::ZERO
            } else {
                by_date.get(&date.pred_opt()?)?.net_pnl
            };
            Some(current.net_pnl - previous)
        })
        .collect()
}

fn recomputed_wallet_equity_drawdown(snapshots: &[CumulativeWalletSnapshot]) -> Decimal {
    let mut high_watermark = WALLET_CAMPAIGN_BASELINE;
    let mut max_drawdown = Decimal::ZERO;
    for snapshot in snapshots {
        high_watermark = high_watermark.max(snapshot.ending_equity);
        max_drawdown = max_drawdown.max(high_watermark - snapshot.ending_equity);
    }
    max_drawdown
}

fn reconciled_wallet_max_drawdown(snapshots: &[CumulativeWalletSnapshot]) -> Decimal {
    snapshots
        .last()
        .map(|snapshot| snapshot.max_drawdown)
        .unwrap_or_default()
        .max(recomputed_wallet_equity_drawdown(snapshots))
}

fn trailing_positive_complete_weekly_blocks(daily_pnl: &[Decimal]) -> (u32, usize) {
    let mut blocks = daily_pnl.rchunks_exact(7);
    let complete_blocks = blocks.len();
    let consecutive = blocks
        .by_ref()
        .take_while(|block| block.iter().copied().sum::<Decimal>() > Decimal::ZERO)
        .count();
    (
        u32::try_from(consecutive).unwrap_or(u32::MAX),
        complete_blocks,
    )
}

fn block_bootstrap_daily_markout_lower_95(rows: &[Value]) -> Option<Decimal> {
    let daily_means = rows
        .iter()
        .filter(|row| row["markout_30s_sample_size"].as_u64().unwrap_or_default() > 0)
        .map(|row| decimal_from_value(&row["markout_30s_mean"]))
        .collect::<Option<Vec<_>>>()?;
    block_bootstrap_daily_pnl_lower_95(&daily_means)
}

#[derive(Clone, Debug)]
struct CumulativeWalletSnapshot {
    date: NaiveDate,
    schema_version: u64,
    campaign_start: NaiveDate,
    campaign_baseline: Decimal,
    input_sha256: String,
    parent_input_sha256: Option<String>,
    events: u64,
    net_pnl: Decimal,
    ending_equity: Decimal,
    max_drawdown: Decimal,
    unresolved_orders: u64,
}

fn validated_cumulative_wallet_snapshots(
    rows: &[Value],
    expected_contract: Option<&ShadowCampaignContractBinding>,
) -> Option<Vec<CumulativeWalletSnapshot>> {
    if rows.is_empty() {
        return None;
    }
    let legacy_campaign_start =
        NaiveDate::parse_from_str(WALLET_CAMPAIGN_START, "%Y-%m-%d").ok()?;
    let legacy_first_snapshot =
        NaiveDate::parse_from_str(PROJECTED_WALLET_PROTOCOL_CUTOFF, "%Y-%m-%d").ok()?;
    let expected_first_snapshot = expected_contract
        .map(|binding| binding.contract.first_eligible_date)
        .unwrap_or(legacy_first_snapshot);
    let expected_campaign_start = expected_contract
        .map(|binding| binding.contract.start_date)
        .unwrap_or(legacy_campaign_start);
    let expected_baseline = expected_contract
        .map(|binding| binding.contract.wallet_baseline)
        .unwrap_or(WALLET_CAMPAIGN_BASELINE);
    let expected_schema = expected_contract
        .map(|_| CAMPAIGN_BOUND_WALLET_SCHEMA_VERSION)
        .unwrap_or(2);
    let mut snapshots: Vec<CumulativeWalletSnapshot> = Vec::with_capacity(rows.len());
    for row in rows {
        let date_text = row["date"].as_str()?;
        let date = NaiveDate::parse_from_str(date_text, "%Y-%m-%d").ok()?;
        let input_hash = row["cumulative_input_sha256"].as_str()?;
        let state_hash = row["cumulative_state_sha256"].as_str()?;
        let schema_version = row["wallet_schema_version"].as_u64().unwrap_or(1);
        let parent_input_sha256 = row["cumulative_parent_input_sha256"]
            .as_str()
            .map(ToOwned::to_owned);
        let snapshot = CumulativeWalletSnapshot {
            date,
            schema_version,
            campaign_start: NaiveDate::parse_from_str(
                row["wallet_campaign_start"].as_str()?,
                "%Y-%m-%d",
            )
            .ok()?,
            campaign_baseline: if schema_version == CAMPAIGN_BOUND_WALLET_SCHEMA_VERSION {
                decimal_from_value(&row["wallet_campaign_baseline"])?
            } else {
                WALLET_CAMPAIGN_BASELINE
            },
            input_sha256: input_hash.to_owned(),
            parent_input_sha256,
            events: row["cumulative_events"].as_u64()?,
            net_pnl: decimal_from_value(&row["wallet_constrained_net_pnl"])?,
            ending_equity: decimal_from_value(&row["wallet_constrained_ending_equity"])?,
            max_drawdown: decimal_from_value(&row["wallet_constrained_max_drawdown"])?,
            unresolved_orders: row["wallet_constrained_unresolved_orders"].as_u64()?,
        };
        let contract_valid = if let Some(binding) = expected_contract {
            row["wallet_campaign_id"].as_str() == Some(binding.contract.campaign_id.as_str())
                && row["wallet_campaign_contract_sha256"].as_str() == Some(binding.sha256.as_str())
                && row["wallet_campaign_first_eligible_date"].as_str()
                    == Some(
                        binding
                            .contract
                            .first_eligible_date
                            .format("%Y-%m-%d")
                            .to_string()
                            .as_str(),
                    )
                && row["wallet_campaign_terminal_date"].as_str()
                    == Some(
                        binding
                            .contract
                            .terminal_date
                            .format("%Y-%m-%d")
                            .to_string()
                            .as_str(),
                    )
                && row["wallet_evidence_protocol_version"].as_u64()
                    == Some(u64::from(binding.contract.evidence_protocol_version))
        } else {
            true
        };
        if (snapshots.is_empty() && date != expected_first_snapshot)
            || row["wallet_scope"].as_str()
                != Some(
                    expected_contract
                        .map(|binding| binding.contract.wallet_scope.as_str())
                        .unwrap_or(CUMULATIVE_WALLET_SCOPE),
                )
            || snapshot.campaign_start != expected_campaign_start
            || row["wallet_snapshot_date"].as_str() != Some(date_text)
            || row["wallet_constrained"].as_bool() != Some(true)
            || date < expected_campaign_start
            || schema_version == 0
            || schema_version != expected_schema
            || !contract_valid
            || !valid_sha256(input_hash)
            || !valid_sha256(state_hash)
            || (schema_version >= 2
                && (!row["cumulative_input_manifest_sha256"]
                    .as_str()
                    .is_some_and(valid_sha256)
                    || !row["cumulative_regimes_artifact_sha256"]
                        .as_str()
                        .is_some_and(valid_sha256)
                    || snapshot
                        .parent_input_sha256
                        .as_deref()
                        .is_some_and(|hash| !valid_sha256(hash))
                    || (date == expected_first_snapshot && snapshot.parent_input_sha256.is_some())
                    || (date > expected_first_snapshot && snapshot.parent_input_sha256.is_none())))
            || snapshot.events == 0
            || snapshot.campaign_baseline != expected_baseline
            || snapshot.ending_equity != expected_baseline + snapshot.net_pnl
            || snapshot.max_drawdown < Decimal::ZERO
        {
            return None;
        }
        if let Some(previous) = snapshots.last() {
            if snapshot.date != previous.date.succ_opt()?
                || snapshot.events < previous.events
                || snapshot.max_drawdown < previous.max_drawdown
                || (snapshot.schema_version >= 2
                    && previous.schema_version == snapshot.schema_version
                    && snapshot.parent_input_sha256.as_deref()
                        != Some(previous.input_sha256.as_str()))
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
    u32::try_from(current_clean_suffix(rows).len()).unwrap_or(u32::MAX)
}

fn current_clean_suffix(rows: &[Value]) -> &[Value] {
    let mut start = rows.len();
    let mut next_date: Option<NaiveDate> = None;
    for index in (0..rows.len()).rev() {
        let row = &rows[index];
        let Some(date) = row["date"]
            .as_str()
            .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
        else {
            break;
        };
        let clean = serde_json::from_value::<DataQualitySummary>(row["data_quality"].clone())
            .ok()
            .is_some_and(|quality| quality.promotion_allowed())
            && row["runtime_role"].as_str() == Some("profitability_shadow");
        if !clean || next_date.is_some_and(|next| date.succ_opt() != Some(next)) {
            break;
        }
        start = index;
        next_date = Some(date);
    }
    &rows[start..]
}

fn observed_campaign_days(snapshots: &[CumulativeWalletSnapshot]) -> u32 {
    let Some(latest) = snapshots.last() else {
        return 0;
    };
    u32::try_from((latest.date - latest.campaign_start).num_days().max(0) + 1).unwrap_or(u32::MAX)
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

#[cfg(test)]
fn complete_paired_candidate_summary(
    rows: &[Value],
    candidate: &str,
    delta_field: &str,
    pnl_field: &str,
) -> Option<Value> {
    if rows.is_empty() {
        return None;
    }
    let summary = paired_candidate_summary(rows, candidate, delta_field, pnl_field);
    (summary["sample_size"].as_u64() == u64::try_from(rows.len()).ok()).then_some(summary)
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

fn manifest_quality_status(quality: &DataQualitySummary) -> &'static str {
    if quality.total_events == 0 || !quality.fatal_issues.is_empty() {
        "critical"
    } else if quality.promotion_allowed() {
        "healthy"
    } else {
        "warning"
    }
}

fn manifest_quality_reasons(quality: &DataQualitySummary) -> Vec<Value> {
    let mut reasons = quality
        .fatal_issues
        .iter()
        .map(|reason| json!(format!("fatal:{reason}")))
        .chain(
            quality
                .warnings
                .iter()
                .filter(|warning| warning.severity == super::run_bundle::WarningSeverity::Blocking)
                .map(|warning| json!(warning.rule_id)),
        )
        .collect::<Vec<_>>();
    if quality.decision_grade_coverage < Decimal::new(95, 2) {
        reasons.push(json!("decision_grade_coverage_below_95pct"));
    }
    if !quality.event_time_ordering_restored {
        reasons.push(json!("event_time_ordering_not_restored"));
    }
    reasons.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    reasons.dedup();
    reasons
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
        "/result/profiles",
        "/result/comparisons",
        "/result/regime_conditioned_profiles/result/profiles",
        "/result/regime_conditioned_profiles/result/comparisons",
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
    use chrono::TimeZone;

    fn measured_quality(
        total_events: u64,
        coverage: Decimal,
        fatal_issues: Vec<String>,
        warnings: Vec<String>,
    ) -> DataQualitySummary {
        let mut quality = DataQualitySummary::new(total_events, coverage, fatal_issues, warnings);
        quality.coverage_breakdown = DataQualityCoverageBreakdown {
            start_price_capture_rate: Some(coverage),
            settlement_rate: Some(coverage),
            exact_reference_hour_coverage: Some(coverage),
            decision_metadata_coverage: Some(coverage),
            decision_grade_coverage: Some(coverage),
            final_decision_grade_coverage: Some(coverage),
            execution_field_coverage: Some(coverage),
            decision_parity_rate: Some(Decimal::ONE),
            queue_snapshot_coverage: Some(coverage),
            markout_1s_completion: Some(coverage),
            markout_5s_completion: Some(coverage),
            markout_30s_completion: Some(coverage),
        };
        quality
    }

    #[test]
    fn block_bootstrap_bound_is_deterministic_for_daily_wallet_pnl() {
        let positive = vec![Decimal::ONE; 28];
        assert_eq!(
            block_bootstrap_daily_pnl_lower_95(&positive),
            Some(Decimal::ONE)
        );
        let mut mixed = positive;
        for value in mixed.iter_mut().take(14) {
            *value = -Decimal::ONE;
        }
        let first = block_bootstrap_daily_pnl_lower_95(&mixed);
        let second = block_bootstrap_daily_pnl_lower_95(&mixed);
        assert_eq!(first, second);
        assert!(first.is_some_and(|value| value < Decimal::ZERO));
        assert!(block_bootstrap_daily_pnl_lower_95(&vec![Decimal::ONE; 27]).is_none());
    }

    #[test]
    fn markout_bound_requires_four_daily_clusters_and_is_block_bootstrapped() {
        let too_few = (0..27)
            .map(|_| json!({"markout_30s_sample_size": 2, "markout_30s_mean": "0.02"}))
            .collect::<Vec<_>>();
        assert!(block_bootstrap_daily_markout_lower_95(&too_few).is_none());

        let enough = (0..28)
            .map(|_| json!({"markout_30s_sample_size": 2, "markout_30s_mean": "0.02"}))
            .collect::<Vec<_>>();
        assert_eq!(
            block_bootstrap_daily_markout_lower_95(&enough),
            Some(Decimal::new(2, 2))
        );
    }

    #[test]
    fn weekly_gate_counts_only_the_trailing_positive_complete_blocks() {
        let mut daily = vec![Decimal::ONE; 28];
        assert_eq!(trailing_positive_complete_weekly_blocks(&daily), (4, 4));

        // A later losing complete week resets the gate even though four
        // positive historical weeks still exist.
        daily.extend(vec![-Decimal::ONE; 7]);
        assert_eq!(trailing_positive_complete_weekly_blocks(&daily), (0, 5));

        // An incomplete in-progress week is not evidence for or against a
        // complete weekly block.
        daily.push(-Decimal::ONE);
        assert_eq!(trailing_positive_complete_weekly_blocks(&daily), (0, 5));

        // Oldest-aligned chunking would ignore this adverse one-day tail and
        // incorrectly retain four winning blocks. Latest-aligned blocks must
        // include it in the newest complete week.
        let mut adverse_tail = vec![Decimal::ONE; 28];
        adverse_tail.push(-Decimal::from(10));
        assert_eq!(
            trailing_positive_complete_weekly_blocks(&adverse_tail),
            (0, 4)
        );
    }

    #[test]
    fn campaign_gate_requires_one_frozen_decision_config_across_clean_days() {
        let quality = measured_quality(100, Decimal::ONE, Vec::new(), Vec::new());
        let row = |date: &str, digest: char| {
            json!({
                "date": date,
                "runtime_role": "profitability_shadow",
                "decision_config_sha256": format!("sha256:{}", digest.to_string().repeat(64)),
                "data_quality": quality
            })
        };
        let same = vec![row("2026-07-20", 'a'), row("2026-07-21", 'a')];
        let same_metrics = aggregate_profitability_metrics(
            &same,
            &json!({}),
            &json!({}),
            &PromotionThresholds::default(),
            None,
        );
        assert!(!same_metrics
            .missing_metrics
            .contains(&"frozen_decision_config_sha256".to_owned()));

        let changed = vec![row("2026-07-20", 'a'), row("2026-07-21", 'b')];
        let changed_metrics = aggregate_profitability_metrics(
            &changed,
            &json!({}),
            &json!({}),
            &PromotionThresholds::default(),
            None,
        );
        assert!(changed_metrics
            .missing_metrics
            .contains(&"frozen_decision_config_sha256".to_owned()));

        let mut noncanonical = same;
        noncanonical[1]["decision_config_sha256"] = json!(format!("sha256:{}", "A".repeat(64)));
        let noncanonical_metrics = aggregate_profitability_metrics(
            &noncanonical,
            &json!({}),
            &json!({}),
            &PromotionThresholds::default(),
            None,
        );
        assert!(noncanonical_metrics
            .missing_metrics
            .contains(&"frozen_decision_config_sha256".to_owned()));
    }

    #[test]
    fn drawdown_reconciliation_catches_underreported_dirty_day_loss() {
        let snapshot =
            |day: u32, equity: Decimal, stored_drawdown: Decimal| CumulativeWalletSnapshot {
                date: NaiveDate::from_ymd_opt(2026, 7, day).unwrap(),
                schema_version: 2,
                campaign_start: NaiveDate::from_ymd_opt(2026, 7, 12).unwrap(),
                campaign_baseline: WALLET_CAMPAIGN_BASELINE,
                input_sha256: format!("sha256:{}", "a".repeat(64)),
                parent_input_sha256: None,
                events: u64::from(day),
                net_pnl: equity - WALLET_CAMPAIGN_BASELINE,
                ending_equity: equity,
                max_drawdown: stored_drawdown,
                unresolved_orders: 0,
            };
        let snapshots = vec![
            snapshot(13, d("6"), Decimal::ZERO),
            // This snapshot can correspond to a dirty day. It must remain in
            // the cumulative risk ledger even when excluded from clean stats.
            snapshot(14, d("4.8"), Decimal::ZERO),
            snapshot(15, d("5.5"), Decimal::ZERO),
        ];
        assert_eq!(recomputed_wallet_equity_drawdown(&snapshots), d("1.2"));
        assert_eq!(reconciled_wallet_max_drawdown(&snapshots), d("1.2"));

        let mut larger_intraday = snapshots;
        larger_intraday.last_mut().unwrap().max_drawdown = d("1.4");
        assert_eq!(reconciled_wallet_max_drawdown(&larger_intraday), d("1.4"));
    }

    #[test]
    fn cumulative_wallet_selects_full_profile_before_lossy_comparison() {
        let report = json!({
            "result": {
                "comparisons": [{
                    "profile": "dynamic_quote_style",
                    "net_pnl": "1"
                }],
                "profiles": [{
                    "profile": "dynamic_quote_style",
                    "events": 42,
                    "wallet_constrained": true
                }]
            }
        });
        let profile = find_regime_profile(&report, "dynamic_quote_style").unwrap();
        assert_eq!(profile["events"], 42);
        assert_eq!(profile["wallet_constrained"], true);
    }

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
            "snapshot_date": "2026-07-13",
            "schema_version": 2,
            "cumulative_input_sha256": format!("sha256:{}", "a".repeat(64)),
            "cumulative_parent_input_sha256": Value::Null,
            "cumulative_input_manifest_sha256": format!("sha256:{}", "b".repeat(64)),
            "cumulative_state_sha256": format!("sha256:{}", "c".repeat(64)),
            "cumulative_regimes_artifact_sha256": format!("sha256:{}", "d".repeat(64)),
            "cumulative_events": 10,
            "wallet_constrained": true,
            "wallet_constrained_net_pnl": "0.25",
            "wallet_constrained_ending_equity": "5.280521",
            "wallet_constrained_max_drawdown": "0",
            "wallet_constrained_unresolved_orders": 0
        });
        let quality = measured_quality(100, Decimal::ONE, Vec::new(), Vec::new());
        let runtime_role = polyedge_config::RuntimeRole::ProfitabilityShadow;
        let row = json_row(
            "2026-07-13",
            DailyReportSources {
                final_report: None,
                regimes: Some(&regimes),
                baseline: None,
            },
            DailyRowEvidence {
                sample: None,
                audit: None,
                execution_quality: None,
                cumulative_wallet: Some(&cumulative_wallet),
                manifest_quality: Some(&quality),
                runtime_role: Some(&runtime_role),
            },
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
            None,
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
            let first_day = date == PROJECTED_WALLET_PROTOCOL_CUTOFF;
            json!({
                "date": date,
                "runtime_role": "profitability_shadow",
                "settled_markets": 10,
                "fill_model": "queue_proxy_conservative",
                // A reset-per-day implementation would incorrectly sum these to +2.
                "dynamic_quote_style_net_pnl": "1",
                "wallet_scope": CUMULATIVE_WALLET_SCOPE,
                "wallet_campaign_start": WALLET_CAMPAIGN_START,
                "wallet_snapshot_date": date,
                "wallet_schema_version": 2,
                "cumulative_input_sha256": format!("sha256:{}", if first_day { "a".repeat(64) } else { "b".repeat(64) }),
                "cumulative_parent_input_sha256": if first_day { Value::Null } else { json!(format!("sha256:{}", "a".repeat(64))) },
                "cumulative_input_manifest_sha256": format!("sha256:{}", "e".repeat(64)),
                "cumulative_state_sha256": format!("sha256:{}", if first_day { "c".repeat(64) } else { "d".repeat(64) }),
                "cumulative_regimes_artifact_sha256": format!("sha256:{}", "f".repeat(64)),
                "cumulative_events": events,
                "wallet_constrained": true,
                "wallet_constrained_net_pnl": pnl,
                "wallet_constrained_ending_equity": equity,
                "wallet_constrained_max_drawdown": drawdown,
                "wallet_constrained_unresolved_orders": unresolved,
                "data_quality": measured_quality(100, Decimal::ONE, Vec::new(), Vec::new())
            })
        }
        let rows = vec![
            row("2026-07-13", "1", "6.030521", "0", 100, 0),
            row("2026-07-14", "-0.5", "4.530521", "1.5", 200, 1),
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
            None,
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
            None,
        );
        assert!(!invalid.wallet_constrained);
        assert!(invalid
            .missing_metrics
            .contains(&"valid_cumulative_wallet_ledger".to_owned()));
    }

    #[test]
    fn cumulative_wallet_rejects_schema_downgrade_after_projected_chain_cutover() {
        fn row(date: &str, input: char, parent: Option<char>, schema: u64) -> Value {
            json!({
                "date": date,
                "wallet_scope": CUMULATIVE_WALLET_SCOPE,
                "wallet_campaign_start": WALLET_CAMPAIGN_START,
                "wallet_snapshot_date": date,
                "wallet_schema_version": schema,
                "cumulative_input_sha256": format!("sha256:{}", input.to_string().repeat(64)),
                "cumulative_parent_input_sha256": parent.map(|value| format!("sha256:{}", value.to_string().repeat(64))),
                "cumulative_input_manifest_sha256": format!("sha256:{}", "c".repeat(64)),
                "cumulative_state_sha256": format!("sha256:{}", "d".repeat(64)),
                "cumulative_regimes_artifact_sha256": format!("sha256:{}", "e".repeat(64)),
                "cumulative_events": if date.ends_with("13") { 100 } else { 200 },
                "wallet_constrained": true,
                "wallet_constrained_net_pnl": "0",
                "wallet_constrained_ending_equity": WALLET_CAMPAIGN_BASELINE.to_string(),
                "wallet_constrained_max_drawdown": "0",
                "wallet_constrained_unresolved_orders": 0
            })
        }

        let valid = vec![
            row("2026-07-13", 'a', None, 2),
            row("2026-07-14", 'b', Some('a'), 2),
        ];
        assert!(validated_cumulative_wallet_snapshots(&valid, None).is_some());

        let mut downgraded = valid;
        downgraded[1]["wallet_schema_version"] = json!(1);
        assert!(validated_cumulative_wallet_snapshots(&downgraded, None).is_none());

        let late_start = vec![row("2026-07-14", 'b', Some('a'), 2)];
        assert!(validated_cumulative_wallet_snapshots(&late_start, None).is_none());

        let gapped = vec![
            row("2026-07-13", 'a', None, 2),
            row("2026-07-15", 'b', Some('a'), 2),
        ];
        assert!(validated_cumulative_wallet_snapshots(&gapped, None).is_none());
    }

    #[test]
    fn clean_day_streak_resets_on_date_gap_and_dirty_day() {
        let clean = measured_quality(100, Decimal::ONE, Vec::new(), Vec::new());
        let dirty = measured_quality(100, Decimal::new(90, 2), Vec::new(), Vec::new());
        let row = |date: &str, quality: &DataQualitySummary| {
            json!({
                "date": date,
                "data_quality": quality,
                "runtime_role": "profitability_shadow"
            })
        };
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
    fn statistical_evidence_uses_only_the_current_clean_suffix() {
        let clean = measured_quality(100, Decimal::ONE, Vec::new(), Vec::new());
        let dirty = measured_quality(
            100,
            Decimal::new(50, 2),
            Vec::new(),
            vec!["daily capture gap exceeds 300000ms for 2026-07-12: max_gap_ms=600000".to_owned()],
        );
        let row = |date: &str, quality: &DataQualitySummary, delta: &str| {
            json!({
                "date": date,
                "runtime_role": "profitability_shadow",
                "data_quality": quality,
                "dynamic_quote_style_paired_delta": delta,
                "dynamic_quote_style_net_pnl": delta,
                "static_net_pnl": "0",
                "decision_gate": "PAPER_ONLY"
            })
        };
        let rows = vec![
            row("2026-07-12", &dirty, "100"),
            row("2026-07-13", &clean, "1"),
            row("2026-07-14", &clean, "2"),
            row("2026-07-15", &clean, "3"),
        ];

        let evidence = current_clean_suffix(&rows);
        assert_eq!(evidence.len(), 3);
        assert_eq!(evidence[0]["date"], "2026-07-13");
        assert_eq!(consecutive_clean_day_streak(&rows), 3);
        assert_eq!(
            paired_improvement_summary(evidence)["dynamic_quote_style"]["sample_size"],
            3
        );
        let mut incomplete = evidence.to_vec();
        incomplete[1]
            .as_object_mut()
            .unwrap()
            .remove("dynamic_quote_style_paired_delta");
        assert!(complete_paired_candidate_summary(
            &incomplete,
            "dynamic_quote_style",
            "dynamic_quote_style_paired_delta",
            "dynamic_quote_style_net_pnl"
        )
        .is_none());
    }

    #[test]
    fn dirty_profit_and_markets_cannot_help_gates_but_wallet_loss_remains() {
        let clean = measured_quality(100, Decimal::ONE, Vec::new(), Vec::new());
        let dirty = measured_quality(
            100,
            Decimal::new(50, 2),
            Vec::new(),
            vec!["daily capture gap exceeds 300000ms for 2026-07-13: max_gap_ms=600000".to_owned()],
        );
        let row = |date: &str,
                   quality: &DataQualitySummary,
                   input: char,
                   parent: Option<char>,
                   events: u64,
                   daily_pnl: &str,
                   wallet_pnl: &str,
                   drawdown: &str| {
            json!({
                "date": date,
                "runtime_role": "profitability_shadow",
                "settled_markets": 10,
                "fill_model": "queue_proxy_conservative",
                "dynamic_quote_style_net_pnl": daily_pnl,
                "wallet_scope": CUMULATIVE_WALLET_SCOPE,
                "wallet_campaign_start": WALLET_CAMPAIGN_START,
                "wallet_snapshot_date": date,
                "wallet_schema_version": 2,
                "cumulative_input_sha256": format!("sha256:{}", input.to_string().repeat(64)),
                "cumulative_parent_input_sha256": parent.map(|value| format!("sha256:{}", value.to_string().repeat(64))),
                "cumulative_input_manifest_sha256": format!("sha256:{}", "c".repeat(64)),
                "cumulative_state_sha256": format!("sha256:{}", input.to_ascii_uppercase().to_string().repeat(64)),
                "cumulative_regimes_artifact_sha256": format!("sha256:{}", "e".repeat(64)),
                "cumulative_events": events,
                "wallet_constrained": true,
                "wallet_constrained_net_pnl": wallet_pnl,
                "wallet_constrained_ending_equity": (WALLET_CAMPAIGN_BASELINE + d(wallet_pnl)).to_string(),
                "wallet_constrained_max_drawdown": drawdown,
                "wallet_constrained_unresolved_orders": 0,
                "data_quality": quality
            })
        };
        let mut rows = vec![
            row("2026-07-13", &dirty, 'a', None, 100, "100", "1", "0"),
            row("2026-07-14", &clean, 'b', Some('a'), 200, "1", "0.5", "0.5"),
            row(
                "2026-07-15",
                &clean,
                'd',
                Some('b'),
                300,
                "2",
                "-0.25",
                "1.25",
            ),
        ];
        // A stale prospective artifact and one favorable daily value must not
        // fill gaps in the exact clean suffix.
        rows[1]["markout_30s_ci_low"] = json!("0.10");
        rows[1]["decision_parity_rate"] = json!("1");
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
            None,
        );

        assert_eq!(metrics.clean_days, 2);
        assert_eq!(metrics.settled_markets, 20);
        assert_eq!(metrics.queue_conservative_net_pnl, d("3"));
        assert_eq!(metrics.wallet_constrained_net_pnl, d("-0.25"));
        assert_eq!(metrics.wallet_constrained_ending_equity, d("4.780521"));
        assert_eq!(metrics.max_drawdown, d("1.25"));
        assert!(metrics.data_quality.promotion_allowed());
        assert_eq!(metrics.pnl_ci_95_low, Decimal::ZERO);
        assert_eq!(metrics.markout_30s_ci_low, Decimal::ZERO);
        assert_eq!(metrics.decision_parity_rate, Decimal::ZERO);
        for required in [
            "pnl_ci_95_low",
            "markout_30s_ci_low",
            "decision_parity_rate",
        ] {
            assert!(metrics.missing_metrics.contains(&required.to_owned()));
        }
    }

    #[test]
    fn published_blocking_manifest_cannot_increment_clean_days() {
        let root = std::env::temp_dir().join(format!(
            "polyedge-manifest-quality-consumer-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let source = root.join("source");
        std::fs::create_dir_all(&source).unwrap();
        for name in [
            "baseline.json",
            "regimes.json",
            "final_report.json",
            "execution_quality.json",
        ] {
            std::fs::write(source.join(name), "{}").unwrap();
        }
        let observed_hours = (0..24)
            .map(|hour| (format!("2026-07-14T{hour:02}"), 100_u64))
            .collect::<BTreeMap<_, _>>();
        let audit = json!({
            "result": {
                "total_events": 2400,
                "start_price_capture_rate": 1.0,
                "settlement_rate": 1.0,
                "exact_resolution_reference_hour_coverage": 1.0,
                "decision_metadata_coverage": 1.0,
                "decision_grade_coverage": 1.0,
                "final_decision_grade_coverage": 1.0,
                "execution_field_coverage": 1.0,
                "decision_parity_rate": 1.0,
                "fatal_data_quality_issues": [],
                "warnings": [],
                "event_time_ordering_restored": true,
                "out_of_order_timestamps": 0,
                "first_event_timestamp": "2026-07-14T00:00:01Z",
                "last_event_timestamp": "2026-07-14T23:59:59Z",
                "event_count_by_hour": observed_hours,
                "largest_time_gaps": [{"gap_ms": 60000}]
            }
        });
        // Audit-only evidence is intentionally incomplete: queue and markout
        // coverage are merged from execution_quality.json during publication.
        assert!(!quality_from_audit(&audit).promotion_allowed());
        let audit_path = source.join("data_audit.json");
        std::fs::write(&audit_path, serde_json::to_vec_pretty(&audit).unwrap()).unwrap();
        let daily_root = root.join("daily");
        let published = super::super::run_bundle::publish_daily_directory(
            NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
            "shadow-blocked-20260714",
            "4".repeat(64),
            polyedge_config::RuntimeRole::ProfitabilityShadow,
            &source,
            &daily_root,
            &audit_path,
        )
        .unwrap();
        assert!(!published.manifest.data_quality.promotion_allowed());

        let rows = load_local_daily_prospective_rows(
            &daily_root,
            Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["runtime_role"], "profitability_shadow");
        let row_quality: DataQualitySummary =
            serde_json::from_value(rows[0]["data_quality"].clone()).unwrap();
        assert_eq!(row_quality, published.manifest.data_quality);
        assert_eq!(consecutive_clean_day_streak(&rows), 0);
    }

    #[test]
    fn fresh_local_day_merges_with_prior_azure_history() {
        let clean = measured_quality(100, Decimal::ONE, Vec::new(), Vec::new());
        let dirty = measured_quality(
            100,
            Decimal::ONE,
            vec!["duplicate stale current-day row".to_owned()],
            Vec::<String>::new(),
        );
        let row = |date: &str, quality: &DataQualitySummary, source: &str| {
            json!({
                "date": date,
                "data_quality": quality,
                "runtime_role": "profitability_shadow",
                "source": source
            })
        };
        let azure = vec![
            row("2026-07-12", &clean, "azure"),
            row("2026-07-13", &clean, "azure"),
            row("2026-07-14", &dirty, "azure-stale"),
        ];
        let local = vec![row("2026-07-14", &clean, "local")];
        let merged = merge_daily_prospective_rows(local, azure).unwrap();

        assert_eq!(merged.len(), 3);
        assert_eq!(merged[2]["date"], "2026-07-14");
        assert_eq!(merged[2]["source"], "local");
        assert_eq!(consecutive_clean_day_streak(&merged), 3);
    }

    #[test]
    fn clean_day_streak_resets_on_gap_or_dirty_day() {
        fn row(date: &str, clean: bool) -> Value {
            let quality = if clean {
                measured_quality(100, Decimal::ONE, Vec::new(), Vec::new())
            } else {
                measured_quality(
                    100,
                    Decimal::ONE,
                    vec!["fatal_test_gap".to_owned()],
                    Vec::<String>::new(),
                )
            };
            json!({
                "date": date,
                "data_quality": quality,
                "runtime_role": "profitability_shadow"
            })
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

    #[test]
    fn july_23_protocol_v3_campaign_contract_is_hash_bound_and_scoped() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../research/configs/profitability_gate_v3_2026-07-23.yaml");
        let first = load_shadow_campaign_contract(&path).unwrap();
        let second = load_shadow_campaign_contract(&path).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.contract.campaign_id, "campaign-2026-07-23");
        assert_eq!(
            first.contract.start_date,
            NaiveDate::from_ymd_opt(2026, 7, 23).unwrap()
        );
        assert_eq!(
            first.contract.terminal_date,
            NaiveDate::from_ymd_opt(2026, 9, 20).unwrap()
        );
        assert_eq!(first.contract.wallet_baseline, d("5.030521"));
        assert_eq!(first.contract.evidence_protocol_version, 3);
        assert_eq!(
            first.contract.daily_root,
            "reports/research/shadow/campaigns/campaign-2026-07-23/daily"
        );
        assert!(valid_sha256(&first.sha256));
    }

    #[test]
    fn schema_v3_wallet_chain_rejects_mixed_campaign_and_broken_parent() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../research/configs/profitability_gate_v3_2026-07-23.yaml");
        let binding = load_shadow_campaign_contract(&path).unwrap();
        let hash = |byte: char| format!("sha256:{}", byte.to_string().repeat(64));
        let row = |day: u32, input: String, parent: Option<String>, pnl: &str| {
            json!({
                "date": format!("2026-07-{day:02}"),
                "wallet_schema_version": 3,
                "wallet_scope": binding.contract.wallet_scope,
                "wallet_campaign_id": binding.contract.campaign_id,
                "wallet_campaign_contract_sha256": binding.sha256,
                "wallet_campaign_start": "2026-07-23",
                "wallet_campaign_first_eligible_date": "2026-07-23",
                "wallet_campaign_terminal_date": "2026-09-20",
                "wallet_campaign_baseline": "5.030521",
                "wallet_evidence_protocol_version": 3,
                "wallet_snapshot_date": format!("2026-07-{day:02}"),
                "wallet_constrained": true,
                "cumulative_input_sha256": input,
                "cumulative_parent_input_sha256": parent,
                "cumulative_input_manifest_sha256": hash('c'),
                "cumulative_state_sha256": hash('d'),
                "cumulative_regimes_artifact_sha256": hash('e'),
                "cumulative_events": u64::from(day - 22) * 100,
                "wallet_constrained_net_pnl": pnl,
                "wallet_constrained_ending_equity": (d("5.030521") + d(pnl)).to_string(),
                "wallet_constrained_max_drawdown": "0",
                "wallet_constrained_unresolved_orders": 0
            })
        };
        let first_input = hash('a');
        let valid = vec![
            row(23, first_input.clone(), None, "0.1"),
            row(24, hash('b'), Some(first_input), "0.2"),
        ];
        assert!(validated_cumulative_wallet_snapshots(&valid, Some(&binding)).is_some());

        let mut mixed = valid.clone();
        mixed[1]["wallet_campaign_id"] = json!("campaign-2026-07-12");
        assert!(validated_cumulative_wallet_snapshots(&mixed, Some(&binding)).is_none());

        let mut broken_parent = valid;
        broken_parent[1]["cumulative_parent_input_sha256"] = json!(hash('f'));
        assert!(validated_cumulative_wallet_snapshots(&broken_parent, Some(&binding)).is_none());
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
