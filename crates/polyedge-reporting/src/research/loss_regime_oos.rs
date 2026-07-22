use super::*;
use std::path::Component;

const ORDER_FACT_SCHEMA_V2: &str = "polyedge.loss_diagnostics.order_lifecycle_fact.v2";
const FILL_FACT_SCHEMA_V1: &str = "polyedge.loss_diagnostics.fill_markout_fact.v1";
const SUMMARY_SCHEMA_V1: &str = "polyedge.loss_diagnostics.summary.v1";
const ARTIFACT_MANIFEST_SCHEMA_V1: &str = "polyedge.loss_diagnostics.artifact_manifest.v1";
const OOS_SCHEMA_V1: &str = "polyedge.loss_regime_oos.v1";
const ORDER_FACT_FILE: &str = "order_lifecycle_fact.jsonl";
const FILL_FACT_FILE: &str = "fill_markout_fact.jsonl";
const SUMMARY_FILE: &str = "loss_diagnostics.json";
const MANIFEST_FILE: &str = "loss_diagnostics_artifact_manifest.json";
const BLOCK_DAYS: usize = 7;
const MIN_BLOCKS: usize = 4;
const BOOTSTRAP_RESAMPLES: usize = 10_000;

#[derive(Clone, Debug)]
pub struct LossRegimeOosOptions {
    pub facts: PathBuf,
    pub queue_evidence: PathBuf,
    pub config: PathBuf,
    pub source_campaign_id: String,
    pub out: PathBuf,
    pub markdown: PathBuf,
}

#[derive(Clone, Debug)]
struct LossRegimeConfig {
    schema_version: u32,
    experiment_id: String,
    evidence_version: String,
    frozen_at: DateTime<Utc>,
    source_campaign_id: String,
    research_only: bool,
    candidates: Vec<LossRegimeCandidate>,
    sha256: String,
}

#[derive(Clone, Debug, Default)]
struct LossRegimeCandidate {
    name: String,
    minimum_expected_edge: Option<Decimal>,
    maximum_pre_send_public_size_ahead: Option<Decimal>,
    maximum_spread_ticks: Option<Decimal>,
    maximum_sigma: Option<Decimal>,
    maximum_model_error: Option<Decimal>,
    minimum_seconds_to_expiry: Option<Decimal>,
    maximum_seconds_to_expiry: Option<Decimal>,
}

impl LossRegimeCandidate {
    fn has_no_filters(&self) -> bool {
        self.minimum_expected_edge.is_none()
            && self.maximum_pre_send_public_size_ahead.is_none()
            && self.maximum_spread_ticks.is_none()
            && self.maximum_sigma.is_none()
            && self.maximum_model_error.is_none()
            && self.minimum_seconds_to_expiry.is_none()
            && self.maximum_seconds_to_expiry.is_none()
    }

    fn as_json(&self) -> Value {
        json!({
            "name": self.name,
            "minimum_expected_edge": decimal_option_json(self.minimum_expected_edge),
            "maximum_pre_send_public_size_ahead": decimal_option_json(self.maximum_pre_send_public_size_ahead),
            "maximum_spread_ticks": decimal_option_json(self.maximum_spread_ticks),
            "maximum_sigma": decimal_option_json(self.maximum_sigma),
            "maximum_model_error": decimal_option_json(self.maximum_model_error),
            "minimum_seconds_to_expiry": decimal_option_json(self.minimum_seconds_to_expiry),
            "maximum_seconds_to_expiry": decimal_option_json(self.maximum_seconds_to_expiry),
        })
    }

    fn accepts(&self, features: &PreSendFeatures) -> bool {
        threshold_minimum(features.expected_edge, self.minimum_expected_edge)
            && threshold_maximum(
                features.public_size_ahead,
                self.maximum_pre_send_public_size_ahead,
            )
            && threshold_maximum(features.spread_ticks, self.maximum_spread_ticks)
            && threshold_maximum(features.sigma, self.maximum_sigma)
            && threshold_maximum(features.model_error, self.maximum_model_error)
            && threshold_minimum(features.seconds_to_expiry, self.minimum_seconds_to_expiry)
            && threshold_maximum(features.seconds_to_expiry, self.maximum_seconds_to_expiry)
    }
}

#[derive(Clone, Debug, Default)]
struct CandidateBuilder {
    candidate: LossRegimeCandidate,
    seen: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct VerifiedFacts {
    orders: Vec<Value>,
    fills: Vec<Value>,
    input_binding_sha256: String,
    artifact_manifest_sha256: String,
    artifact_manifest: Value,
    summary_sha256: String,
}

#[derive(Clone, Debug)]
struct VerifiedQueueEligibility {
    eligible_markets: BTreeSet<String>,
    artifact_sha256: String,
    market_eligibility_sha256: String,
}

#[derive(Clone, Debug, Default)]
struct PreSendFeatures {
    expected_edge: Option<Decimal>,
    public_size_ahead: Option<Decimal>,
    spread_ticks: Option<Decimal>,
    sigma: Option<Decimal>,
    model_error: Option<Decimal>,
    seconds_to_expiry: Option<Decimal>,
}

#[derive(Clone, Debug)]
struct OrderObservation {
    order_id: String,
    market_id: String,
    submitted_ts: DateTime<Utc>,
    market_end_ts: DateTime<Utc>,
    day: String,
    fill_count: usize,
    settled_net_pnl: Decimal,
    markout_30s_net_pnl: Decimal,
    markout_30s_count: usize,
    features: PreSendFeatures,
}

#[derive(Clone, Debug)]
struct CandidateEvidence {
    candidate: LossRegimeCandidate,
    validation: Value,
    validation_net_pnl: Decimal,
    validation_markout_30s_net_pnl: Decimal,
}

pub fn run_loss_regime_oos(options: LossRegimeOosOptions) -> Result<Value, ResearchError> {
    let started = Instant::now();
    if options.source_campaign_id.trim().is_empty() {
        return Err(invalid("source_campaign_id cannot be empty"));
    }
    if options.out == options.markdown {
        return Err(invalid("JSON and Markdown outputs must be different paths"));
    }
    if options.out.exists() || options.markdown.exists() {
        return Err(invalid(
            "loss-regime OOS output already exists; experiment artifacts are immutable",
        ));
    }

    let config = load_config(&options.config)?;
    if config.source_campaign_id != options.source_campaign_id {
        return Err(invalid(format!(
            "config source_campaign_id {} does not match requested {}",
            config.source_campaign_id, options.source_campaign_id
        )));
    }
    validate_output_path(&options.out, &config.experiment_id)?;
    validate_output_path(&options.markdown, &config.experiment_id)?;

    let facts = verify_facts(&options.facts)?;
    let queue_eligibility =
        verify_queue_eligibility(&options.queue_evidence, &facts.input_binding_sha256)?;
    let observations = derive_observations(&facts, &queue_eligibility, config.frozen_at)?;
    let days = observations
        .iter()
        .map(|row| row.day.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if days.len() < 3 {
        return Err(invalid(
            "loss-regime OOS requires at least three sealed market days",
        ));
    }

    let validation_days = days[1..days.len() - 1].to_vec();
    let final_test_day = days
        .last()
        .cloned()
        .ok_or_else(|| invalid("final test day is missing"))?;
    let folds = chronological_folds(&days);
    let mut evidence = config
        .candidates
        .iter()
        .cloned()
        .map(|candidate| {
            let validation = candidate_metrics(&candidate, &observations, &validation_days);
            let validation_net_pnl =
                value_decimal(&validation["queue_qualified_settled_net_pnl"]).unwrap_or_default();
            let validation_markout_30s_net_pnl =
                value_decimal(&validation["net_executable_markout_30s_pnl"]).unwrap_or_default();
            CandidateEvidence {
                candidate,
                validation,
                validation_net_pnl,
                validation_markout_30s_net_pnl,
            }
        })
        .collect::<Vec<_>>();
    evidence.sort_by(|left, right| {
        right
            .validation_net_pnl
            .cmp(&left.validation_net_pnl)
            .then(
                right
                    .validation_markout_30s_net_pnl
                    .cmp(&left.validation_markout_30s_net_pnl),
            )
            .then(left.candidate.name.cmp(&right.candidate.name))
    });
    let selected_name = evidence
        .first()
        .map(|row| row.candidate.name.clone())
        .ok_or_else(|| invalid("loss-regime OOS has no candidates"))?;
    let candidate_rows = evidence
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let selected = row.candidate.name == selected_name;
            let test = if selected {
                let metrics = candidate_metrics(
                    &row.candidate,
                    &observations,
                    std::slice::from_ref(&final_test_day),
                );
                json!({
                    "status": "opened_after_winner_fixed",
                    "day": final_test_day,
                    "metrics": metrics
                })
            } else {
                json!({"status": "sealed_not_selected", "day": Value::Null, "metrics": Value::Null})
            };
            json!({
                "candidate": row.candidate.as_json(),
                "validation_rank": index + 1,
                "selected": selected,
                "validation": row.validation,
                "sealed_test": test
            })
        })
        .collect::<Vec<_>>();

    let result = json!({
        "schema": OOS_SCHEMA_V1,
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "evidence_version": config.evidence_version,
        "config_schema_version": config.schema_version,
        "config_research_only": config.research_only,
        "source_campaign_id": config.source_campaign_id,
        "frozen_at": ts(config.frozen_at),
        "evidence_classification": "diagnostic_only_isolated_experiment",
        "diagnostic_only": true,
        "research_only": true,
        "promotion_eligible": false,
        "counts_toward_protocol_v3_evidence": false,
        "live_deployment_allowed": false,
        "queue_position_source": "paper_shadow_lifecycle_plus_public_l2",
        "queue_position": "inferred_size_ahead",
        "literal_fifo_rank_available": false,
        "pnl_scope": "observed_queue_shadow_fill_subset_after_pre_send_abstention_only",
        "lower_95_scope": "seven_day_circular_block_bootstrap_of_daily_observed_queue_qualified_settled_pnl",
        "source": {
            "facts_directory": options.facts.to_string_lossy(),
            "queue_evidence_path": options.queue_evidence.to_string_lossy(),
            "config_path": options.config.to_string_lossy(),
            "config_sha256": config.sha256,
            "loss_diagnostics_artifact_manifest_sha256": facts.artifact_manifest_sha256,
            "loss_diagnostics_summary_sha256": facts.summary_sha256,
            "queue_evidence_artifact_sha256": queue_eligibility.artifact_sha256,
            "queue_market_eligibility_sha256": queue_eligibility.market_eligibility_sha256,
            "exact_input_binding_sha256": facts.input_binding_sha256,
            "loss_diagnostics_artifact_manifest": facts.artifact_manifest
        },
        "namespace": {
            "json": options.out.to_string_lossy(),
            "markdown": options.markdown.to_string_lossy(),
            "active_campaign_paths_writable": false
        },
        "counts": {
            "order_rows": observations.len(),
            "fill_rows": facts.fills.len(),
            "sealed_market_days": days.len(),
            "candidate_count": config.candidates.len()
        },
        "split": {
            "method": "strict_chronological_market_day_walk_forward",
            "grouping": "whole UTC market-end day; every market and order remains in exactly one day",
            "market_days": days,
            "folds": folds,
            "aggregate_validation_days": validation_days,
            "final_test_day": final_test_day,
            "selection_uses_final_test": false
        },
        "selection": {
            "status": "winner_fixed_before_test_open",
            "candidate": selected_name,
            "rule": "validation queue-qualified settled PnL descending, then 30-second net executable markout PnL descending, then candidate name",
            "promotion_eligible": false
        },
        "candidates": candidate_rows,
        "warnings": [
            "Observed retained-order outcomes are diagnostic counterfactual subsets; skipped-order queue interactions are not replayed.",
            "A selected policy requires a separately frozen future campaign before it can become promotion evidence."
        ]
    });
    let report = envelope(
        "polyedge-rs research loss-regime-oos",
        &options.facts,
        "observed_queue_shadow_fill",
        "strict_chronological_market_day_walk_forward",
        started.elapsed(),
        vec![json!(
            "diagnostic-only isolated experiment; never promotion evidence"
        )],
        result,
    );
    let markdown = render_markdown(&report);
    write_outputs_new(&options.out, &options.markdown, &report, &markdown)?;
    Ok(report)
}

fn invalid(message: impl Into<String>) -> ResearchError {
    ResearchError::InvalidInput(message.into())
}

fn decimal_option_json(value: Option<Decimal>) -> Value {
    value
        .map(|number| Value::String(number.to_string()))
        .unwrap_or(Value::Null)
}

fn threshold_minimum(value: Option<Decimal>, threshold: Option<Decimal>) -> bool {
    threshold.is_none_or(|threshold| value.is_some_and(|value| value >= threshold))
}

fn threshold_maximum(value: Option<Decimal>, threshold: Option<Decimal>) -> bool {
    threshold.is_none_or(|threshold| value.is_some_and(|value| value <= threshold))
}

fn load_config(path: &Path) -> Result<LossRegimeConfig, ResearchError> {
    let bytes = fs::read(path)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| invalid("loss-regime OOS config must be UTF-8 YAML"))?;
    parse_config(text, sha256_prefixed(&bytes))
}

fn parse_config(text: &str, sha256: String) -> Result<LossRegimeConfig, ResearchError> {
    let mut schema_version = None;
    let mut experiment_id = None;
    let mut evidence_version = None;
    let mut frozen_at = None;
    let mut source_campaign_id = None;
    let mut research_only = None;
    let mut candidates = Vec::new();
    let mut current: Option<CandidateBuilder> = None;
    let mut in_candidates = false;
    let mut top_seen = BTreeSet::new();

    for raw in text.lines() {
        let raw = strip_yaml_comment(raw);
        let line = raw.trim();
        if line.is_empty() || line == "---" {
            continue;
        }
        if line == "candidates:" {
            if !top_seen.insert("candidates".to_owned()) {
                return Err(invalid("duplicate YAML field candidates"));
            }
            in_candidates = true;
            continue;
        }
        if let Some(value) = line.strip_prefix("- name:") {
            if !in_candidates {
                return Err(invalid("candidate appears before candidates"));
            }
            if let Some(builder) = current.take() {
                candidates.push(finish_candidate(builder)?);
            }
            let name = yaml_scalar(value);
            if name.is_empty() {
                return Err(invalid("candidate name cannot be empty"));
            }
            let mut builder = CandidateBuilder::default();
            builder.candidate.name = name;
            builder.seen.insert("name".to_owned());
            current = Some(builder);
            continue;
        }
        let (key, raw_value) = line
            .split_once(':')
            .ok_or_else(|| invalid(format!("invalid YAML line {line}")))?;
        let key = key.trim();
        let value = yaml_scalar(raw_value);
        if in_candidates {
            let builder = current
                .as_mut()
                .ok_or_else(|| invalid(format!("candidate field {key} appears before a name")))?;
            if !builder.seen.insert(key.to_owned()) {
                return Err(invalid(format!("duplicate candidate field {key}")));
            }
            set_candidate_field(&mut builder.candidate, key, &value)?;
            continue;
        }
        if !top_seen.insert(key.to_owned()) {
            return Err(invalid(format!("duplicate YAML field {key}")));
        }
        match key {
            "schema_version" => schema_version = value.parse::<u32>().ok(),
            "experiment_id" => experiment_id = Some(value),
            "evidence_version" => evidence_version = Some(value),
            "frozen_at" => frozen_at = parse_utc(&value),
            "source_campaign_id" => source_campaign_id = Some(value),
            "research_only" => research_only = parse_bool(&value),
            _ => {
                return Err(invalid(format!(
                    "unsupported loss-regime config field {key}"
                )))
            }
        }
    }
    if let Some(builder) = current.take() {
        candidates.push(finish_candidate(builder)?);
    }
    if schema_version != Some(1) {
        return Err(invalid("loss-regime config schema_version must equal 1"));
    }
    if research_only != Some(true) {
        return Err(invalid("loss-regime config research_only must equal true"));
    }
    if candidates.is_empty() {
        return Err(invalid("loss-regime config requires candidates"));
    }
    let unique = candidates
        .iter()
        .map(|candidate| candidate.name.as_str())
        .collect::<BTreeSet<_>>();
    if unique.len() != candidates.len() {
        return Err(invalid("loss-regime candidate names must be unique"));
    }
    if !candidates.iter().any(LossRegimeCandidate::has_no_filters) {
        return Err(invalid(
            "loss-regime config requires a baseline candidate containing only name",
        ));
    }
    let experiment_id = experiment_id
        .filter(|value| valid_identifier(value))
        .ok_or_else(|| {
            invalid("experiment_id must use only letters, digits, dot, underscore, or hyphen")
        })?;
    if experiment_id.starts_with("campaign-") {
        return Err(invalid("experiment_id cannot be a campaign identifier"));
    }
    Ok(LossRegimeConfig {
        schema_version: schema_version.unwrap_or_default(),
        experiment_id,
        evidence_version: evidence_version
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| invalid("evidence_version is required"))?,
        frozen_at: frozen_at.ok_or_else(|| invalid("frozen_at must be RFC3339"))?,
        source_campaign_id: source_campaign_id
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| invalid("source_campaign_id is required"))?,
        research_only: research_only.unwrap_or(false),
        candidates,
        sha256,
    })
}

fn finish_candidate(builder: CandidateBuilder) -> Result<LossRegimeCandidate, ResearchError> {
    let candidate = builder.candidate;
    if candidate
        .minimum_seconds_to_expiry
        .zip(candidate.maximum_seconds_to_expiry)
        .is_some_and(|(minimum, maximum)| minimum > maximum)
    {
        return Err(invalid(
            "minimum_seconds_to_expiry cannot exceed maximum_seconds_to_expiry",
        ));
    }
    Ok(candidate)
}

fn set_candidate_field(
    candidate: &mut LossRegimeCandidate,
    key: &str,
    value: &str,
) -> Result<(), ResearchError> {
    let parsed = || parse_nonnegative_decimal(value, key);
    match key {
        "minimum_expected_edge" => candidate.minimum_expected_edge = Some(parsed()?),
        "maximum_pre_send_public_size_ahead" => {
            candidate.maximum_pre_send_public_size_ahead = Some(parsed()?)
        }
        "maximum_spread_ticks" => candidate.maximum_spread_ticks = Some(parsed()?),
        "maximum_sigma" => candidate.maximum_sigma = Some(parsed()?),
        "maximum_model_error" => candidate.maximum_model_error = Some(parsed()?),
        "minimum_seconds_to_expiry" => candidate.minimum_seconds_to_expiry = Some(parsed()?),
        "maximum_seconds_to_expiry" => candidate.maximum_seconds_to_expiry = Some(parsed()?),
        _ => {
            return Err(invalid(format!(
                "unsupported or post-send candidate field {key}; only edge, public size ahead, spread, sigma/model_error, and time-to-expiry are allowed"
            )))
        }
    }
    Ok(())
}

fn parse_nonnegative_decimal(value: &str, field: &str) -> Result<Decimal, ResearchError> {
    Decimal::from_str(value)
        .ok()
        .filter(|value| *value >= Decimal::ZERO)
        .ok_or_else(|| invalid(format!("{field} must be a non-negative decimal")))
}

fn strip_yaml_comment(line: &str) -> &str {
    line.split_once('#').map_or(line, |(prefix, _)| prefix)
}

fn yaml_scalar(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn validate_output_path(path: &Path, experiment_id: &str) -> Result<(), ResearchError> {
    if path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(invalid(
            "loss-regime output path cannot contain parent traversal",
        ));
    }
    let normalized = path
        .to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase();
    let components = normalized
        .split('/')
        .filter(|component| !component.is_empty() && *component != ".")
        .collect::<Vec<_>>();
    if contains_components(&components, &["reports", "research", "shadow"])
        || contains_components(&components, &["data", "research", "shadow"])
    {
        return Err(invalid(
            "loss-regime experiment cannot write active shadow report or data roots",
        ));
    }
    let required = [
        "reports",
        "research",
        "experiments",
        &experiment_id.to_ascii_lowercase(),
    ];
    if !contains_components(&components, &required) {
        return Err(invalid(format!(
            "loss-regime output must be under reports/research/experiments/{experiment_id}"
        )));
    }
    Ok(())
}

fn contains_components(haystack: &[&str], needle: &[&str]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn verify_facts(root: &Path) -> Result<VerifiedFacts, ResearchError> {
    if !root.is_dir() {
        return Err(invalid("loss-regime facts path must be a directory"));
    }
    let manifest_path = root.join(MANIFEST_FILE);
    let manifest_bytes = fs::read(&manifest_path)?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes)?;
    if manifest["schema"].as_str() != Some(ARTIFACT_MANIFEST_SCHEMA_V1)
        || manifest["schema_version"].as_u64() != Some(1)
    {
        return Err(invalid(
            "loss-diagnostics artifact manifest schema is invalid",
        ));
    }
    let artifacts = manifest["artifacts"]
        .as_array()
        .ok_or_else(|| invalid("loss-diagnostics artifact manifest has no artifacts"))?;
    let mut seen = BTreeSet::new();
    for artifact in artifacts {
        let filename = artifact["filename"]
            .as_str()
            .ok_or_else(|| invalid("manifest artifact filename is missing"))?;
        if !safe_basename(filename) || !seen.insert(filename.to_owned()) {
            return Err(invalid(format!(
                "unsafe or duplicate manifest artifact {filename}"
            )));
        }
        let bytes = fs::read(root.join(filename))?;
        let expected_length = artifact["content_length"]
            .as_u64()
            .ok_or_else(|| invalid(format!("manifest artifact {filename} has no length")))?;
        if expected_length != bytes.len() as u64 {
            return Err(invalid(format!(
                "manifest artifact {filename} length mismatch"
            )));
        }
        if artifact["sha256"].as_str() != Some(sha256_prefixed(&bytes).as_str()) {
            return Err(invalid(format!(
                "manifest artifact {filename} SHA-256 mismatch"
            )));
        }
    }
    for required in [ORDER_FACT_FILE, FILL_FACT_FILE, SUMMARY_FILE] {
        if !seen.contains(required) {
            return Err(invalid(format!(
                "manifest is missing required artifact {required}"
            )));
        }
    }

    let orders = read_fact_rows(
        &root.join(ORDER_FACT_FILE),
        ORDER_FACT_SCHEMA_V2,
        manifest_row_count(artifacts, ORDER_FACT_FILE)?,
    )?;
    let fills = read_fact_rows(
        &root.join(FILL_FACT_FILE),
        FILL_FACT_SCHEMA_V1,
        manifest_row_count(artifacts, FILL_FACT_FILE)?,
    )?;
    if fills
        .iter()
        .any(|row| row["fill_source"].as_str() != Some("queue_shadow_fill"))
    {
        return Err(invalid(
            "loss-regime OOS accepts only queue_shadow_fill fact rows",
        ));
    }
    let summary_bytes = fs::read(root.join(SUMMARY_FILE))?;
    let summary: Value = serde_json::from_slice(&summary_bytes)?;
    let result = &summary["result"];
    if result["schema"].as_str() != Some(SUMMARY_SCHEMA_V1)
        || result["status"].as_str() != Some("complete_diagnostic")
        || result["eligible_protocol_v3_identity"].as_bool() != Some(true)
        || result["promotion_eligible"].as_bool() != Some(false)
        || result["counts_toward_protocol_v3_evidence"].as_bool() != Some(false)
        || !coverage_complete(&result["coverage"]["queue_fields"])
        || !coverage_complete(&result["coverage"]["markout_30s"])
    {
        return Err(invalid(
            "loss-regime OOS requires complete diagnostic-only Protocol-v3 queue and 30-second markout coverage",
        ));
    }
    let snapshot = &result["snapshot_identity"];
    let input_binding_sha256 = snapshot["manifest"]["canonical_sha256"]
        .as_str()
        .or_else(|| snapshot["source_inventory_canonical_sha256"].as_str())
        .filter(|value| valid_prefixed_sha256(value))
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            invalid(
                "loss-diagnostics summary has no canonical normalized input binding for queue eligibility",
            )
        })?;
    Ok(VerifiedFacts {
        orders,
        fills,
        input_binding_sha256,
        artifact_manifest_sha256: sha256_prefixed(&manifest_bytes),
        artifact_manifest: manifest,
        summary_sha256: sha256_prefixed(&summary_bytes),
    })
}

fn safe_basename(value: &str) -> bool {
    let path = Path::new(value);
    let mut components = path.components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn valid_prefixed_sha256(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn manifest_row_count(artifacts: &[Value], filename: &str) -> Result<usize, ResearchError> {
    artifacts
        .iter()
        .find(|artifact| artifact["filename"].as_str() == Some(filename))
        .and_then(|artifact| artifact["row_count"].as_u64())
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| invalid(format!("manifest row_count is missing for {filename}")))
}

fn read_fact_rows(
    path: &Path,
    expected_schema: &str,
    expected_rows: usize,
) -> Result<Vec<Value>, ResearchError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut identities = BTreeSet::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            return Err(invalid(format!("blank fact row at {}", index + 1)));
        }
        let mut row: Value = serde_json::from_str(&line)?;
        if row["schema"].as_str() != Some(expected_schema) {
            return Err(invalid(format!("fact row {} schema mismatch", index + 1)));
        }
        let fact_sha256 = row["fact_sha256"]
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| invalid(format!("fact row {} has no SHA-256", index + 1)))?;
        row.as_object_mut()
            .ok_or_else(|| invalid("fact row must be an object"))?
            .remove("fact_sha256");
        if canonical_value_sha256(&row).as_deref() != Some(fact_sha256.as_str()) {
            return Err(invalid(format!("fact row {} SHA-256 mismatch", index + 1)));
        }
        row.as_object_mut()
            .ok_or_else(|| invalid("fact row must be an object"))?
            .insert("fact_sha256".to_owned(), Value::String(fact_sha256));
        let identity_field = if expected_schema == ORDER_FACT_SCHEMA_V2 {
            "order_id"
        } else {
            "fill_lifecycle_id"
        };
        let identity = row[identity_field]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid(format!("fact row {} identity is missing", index + 1)))?;
        if !identities.insert(identity.to_owned()) {
            return Err(invalid(format!("duplicate fact identity {identity}")));
        }
        rows.push(row);
    }
    if rows.len() != expected_rows {
        return Err(invalid(format!(
            "fact row count mismatch: expected {expected_rows}, observed {}",
            rows.len()
        )));
    }
    Ok(rows)
}

fn coverage_complete(value: &Value) -> bool {
    value["denominator"]
        .as_u64()
        .zip(value["observed"].as_u64())
        .is_some_and(|(denominator, observed)| denominator > 0 && denominator == observed)
}

fn verify_queue_eligibility(
    path: &Path,
    expected_input_sha256: &str,
) -> Result<VerifiedQueueEligibility, ResearchError> {
    let bytes = fs::read(path)?;
    let report: Value = serde_json::from_slice(&bytes)?;
    let queue = report
        .pointer("/result/fill_models")
        .and_then(Value::as_array)
        .and_then(|rows| {
            rows.iter()
                .find(|row| row["fill_model"].as_str() == Some("queue_proxy_conservative"))
        })
        .and_then(|row| row.pointer("/replay_metrics/queue_proxy"))
        .or_else(|| {
            (report["fill_model"].as_str() == Some("queue_proxy_conservative"))
                .then(|| report.pointer("/result/replay_metrics/queue_proxy"))
                .flatten()
        })
        .ok_or_else(|| {
            invalid("queue evidence has no queue_proxy_conservative replay eligibility artifact")
        })?;
    if queue["queue_proxy_pnl_eligible"].as_bool() != Some(true)
        || queue["ineligible_queue_fills"].as_u64() != Some(0)
        || queue["market_eligibility_schema"].as_str()
            != Some("polyedge.queue_proxy.market_eligibility.v1")
        || queue["market_eligibility_diagnostic_only"].as_bool() != Some(true)
        || queue["market_eligibility_counts_toward_protocol_v3_evidence"].as_bool() != Some(false)
    {
        return Err(invalid(
            "queue evidence is not PnL-eligible, fail-closed market eligibility",
        ));
    }
    let input_binding = &queue["input_binding"];
    if input_binding["schema"].as_str() != Some("polyedge.queue_proxy.input_binding.v1")
        || input_binding["sha256"].as_str() != Some(expected_input_sha256)
    {
        return Err(invalid(
            "queue evidence is not bound to the exact loss-diagnostics normalized input",
        ));
    }
    let market_eligibility = queue["market_eligibility"]
        .as_object()
        .ok_or_else(|| invalid("queue evidence has no per-market eligibility map"))?;
    let market_eligibility_value = Value::Object(market_eligibility.clone());
    let computed_map_sha256 = canonical_value_sha256(&market_eligibility_value)
        .ok_or_else(|| invalid("queue market eligibility map could not be hashed"))?;
    if queue["market_eligibility_sha256"].as_str() != Some(computed_map_sha256.as_str()) {
        return Err(invalid("queue market eligibility map SHA-256 mismatch"));
    }
    let mut eligible_markets = BTreeSet::new();
    for (market_id, row) in market_eligibility {
        if row["market_id"].as_str() != Some(market_id.as_str()) {
            return Err(invalid(format!(
                "queue market eligibility identity mismatch for {market_id}"
            )));
        }
        let evidence = row
            .get("queue_evidence")
            .filter(|value| value.is_object())
            .ok_or_else(|| invalid(format!("queue evidence is missing for {market_id}")))?;
        let evidence_sha256 = canonical_value_sha256(evidence).ok_or_else(|| {
            invalid(format!(
                "queue evidence could not be hashed for {market_id}"
            ))
        })?;
        if row["queue_evidence_sha256"].as_str() != Some(evidence_sha256.as_str()) {
            return Err(invalid(format!(
                "queue evidence SHA-256 mismatch for {market_id}"
            )));
        }
        match row["eligible"].as_bool() {
            Some(true) => {
                eligible_markets.insert(market_id.clone());
            }
            Some(false) if row["queue_fill_event_count"].as_u64() == Some(0) => {}
            Some(false) => {
                return Err(invalid(format!(
                    "ineligible market {market_id} contains a queue fill"
                )))
            }
            None => {
                return Err(invalid(format!(
                    "queue eligibility is missing for {market_id}"
                )))
            }
        }
    }
    if eligible_markets.is_empty() {
        return Err(invalid("queue evidence has no eligible markets"));
    }
    Ok(VerifiedQueueEligibility {
        eligible_markets,
        artifact_sha256: sha256_prefixed(&bytes),
        market_eligibility_sha256: computed_map_sha256,
    })
}

fn derive_observations(
    facts: &VerifiedFacts,
    queue_eligibility: &VerifiedQueueEligibility,
    frozen_at: DateTime<Utc>,
) -> Result<Vec<OrderObservation>, ResearchError> {
    let mut fills_by_order = BTreeMap::<String, Vec<&Value>>::new();
    for fill in &facts.fills {
        let order_id = required_text(fill, "order_id")?;
        fills_by_order.entry(order_id).or_default().push(fill);
    }
    let order_ids = facts
        .orders
        .iter()
        .map(|order| required_text(order, "order_id"))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if fills_by_order
        .keys()
        .any(|order_id| !order_ids.contains(order_id))
    {
        return Err(invalid("fill fact references an unknown order"));
    }

    let mut observations = Vec::with_capacity(facts.orders.len());
    let mut market_days = BTreeMap::<String, String>::new();
    for order in &facts.orders {
        if order["schema_version"].as_u64() != Some(2)
            || order["evidence_classification"].as_str() != Some("protocol_v3_bound_diagnostic")
            || order["diagnostic_only"].as_bool() != Some(true)
            || order["counts_toward_protocol_v3_evidence"].as_bool() != Some(false)
            || order["execution_fields_complete"].as_bool() != Some(true)
            || order["queue_position_source"].as_str()
                != Some("paper_shadow_lifecycle_plus_public_l2")
            || order["queue_position"].as_str() != Some("inferred_size_ahead")
            || !nonempty_text(&order["queue_registration_event_sha256"])
            || !nonempty_text(&order["queue_snapshot_event_sha256"])
            || !nonempty_text(&order["terminal_settlement_event_sha256"])
            || !nonempty_text(&order["terminal_settlement_journal_sha256"])
        {
            return Err(invalid(
                "order fact is not a complete bound Protocol-v3 queue diagnostic",
            ));
        }
        let order_id = required_text(order, "order_id")?;
        let market_id = required_text(order, "market_id")?;
        if !queue_eligibility.eligible_markets.contains(&market_id) {
            return Err(invalid(format!(
                "order {order_id} is not in an eligible market from the exact bound queue replay"
            )));
        }
        let submitted_ts = parse_required_ts(&order["submitted_ts"], "submitted_ts")?;
        if submitted_ts <= frozen_at {
            return Err(invalid(format!(
                "order {order_id} is not out of sample because it is at or before frozen_at"
            )));
        }
        let market_end_ts =
            parse_required_ts(&order["terminal_market_end_ts"], "terminal_market_end_ts")?;
        let settlement_recorded_ts = parse_required_ts(
            &order["terminal_settlement_recorded_ts"],
            "terminal_settlement_recorded_ts",
        )?;
        if submitted_ts >= market_end_ts || market_end_ts > settlement_recorded_ts {
            return Err(invalid(format!(
                "order {order_id} settlement chronology is invalid"
            )));
        }
        let day = ts(market_end_ts)
            .get(0..10)
            .ok_or_else(|| invalid("market-end timestamp has no UTC day"))?
            .to_owned();
        if market_days
            .insert(market_id.clone(), day.clone())
            .is_some_and(|existing| existing != day)
        {
            return Err(invalid(format!(
                "market {market_id} crosses UTC market days"
            )));
        }

        let order_fills = fills_by_order.remove(&order_id).unwrap_or_default();
        let expected_fill_count = order["fill_count"]
            .as_u64()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| invalid(format!("order {order_id} fill_count is missing")))?;
        if expected_fill_count != order_fills.len() {
            return Err(invalid(format!("order {order_id} fill_count mismatch")));
        }
        let outcome = required_text(order, "outcome")?;
        let winner = required_text(order, "terminal_winning_outcome")?;
        let order_size = required_decimal(&order["order_size"], "order_size")?;
        let mut filled_size = Decimal::ZERO;
        let mut settled_net_pnl = Decimal::ZERO;
        let mut markout_30s_net_pnl = Decimal::ZERO;
        for fill in &order_fills {
            if required_text(fill, "market_id")? != market_id
                || required_text(fill, "token_id")? != required_text(order, "token_id")?
                || required_text(fill, "side")? != required_text(order, "side")?
            {
                return Err(invalid(format!("order {order_id} fill identity mismatch")));
            }
            let size = required_decimal(&fill["fill_size"], "fill_size")?;
            let price = required_decimal(&fill["fill_price"], "fill_price")?;
            let fee_per_share = required_decimal(&fill["fee_per_share"], "fee_per_share")?;
            if size <= Decimal::ZERO || price < Decimal::ZERO || fee_per_share < Decimal::ZERO {
                return Err(invalid(format!(
                    "order {order_id} has an invalid fill amount"
                )));
            }
            filled_size += size;
            settled_net_pnl += settled_fill_pnl(
                required_text(fill, "side")?.as_str(),
                &outcome,
                &winner,
                price,
                size,
                fee_per_share,
            );
            if fill["markout_30s_status"].as_str() != Some("observed") {
                return Err(invalid(format!(
                    "order {order_id} lacks an observed 30s markout"
                )));
            }
            markout_30s_net_pnl += required_decimal(
                &fill["net_executable_markout_30s_pnl"],
                "net_executable_markout_30s_pnl",
            )?;
        }
        if filled_size > order_size {
            return Err(invalid(format!("order {order_id} fills exceed order size")));
        }
        let recorded_settled = required_decimal(
            &order["terminal_settled_net_pnl"],
            "terminal_settled_net_pnl",
        )?;
        if recorded_settled != settled_net_pnl {
            return Err(invalid(format!("order {order_id} settled PnL mismatch")));
        }
        let features = pre_send_features(order)?;
        observations.push(OrderObservation {
            order_id,
            market_id,
            submitted_ts,
            market_end_ts,
            day,
            fill_count: order_fills.len(),
            settled_net_pnl,
            markout_30s_net_pnl,
            markout_30s_count: order_fills.len(),
            features,
        });
    }
    if !fills_by_order.is_empty() {
        return Err(invalid("one or more fill facts were not consumed"));
    }
    observations.sort_by(|left, right| {
        left.market_end_ts
            .cmp(&right.market_end_ts)
            .then(left.submitted_ts.cmp(&right.submitted_ts))
            .then(left.order_id.cmp(&right.order_id))
    });
    Ok(observations)
}

fn settled_fill_pnl(
    side: &str,
    outcome: &str,
    winning_outcome: &str,
    fill_price: Decimal,
    fill_size: Decimal,
    fee_per_share: Decimal,
) -> Decimal {
    let payout = if outcome.eq_ignore_ascii_case(winning_outcome) {
        Decimal::ONE
    } else {
        Decimal::ZERO
    };
    let gross_per_share = if side.eq_ignore_ascii_case("sell") {
        fill_price - payout
    } else {
        payout - fill_price
    };
    (gross_per_share - fee_per_share) * fill_size
}

fn pre_send_features(order: &Value) -> Result<PreSendFeatures, ResearchError> {
    let pre_send = order
        .get("pre_send")
        .filter(|value| value.is_object())
        .ok_or_else(|| invalid("order fact has no pre_send object"))?;
    let pipeline = pre_send
        .get("protocol_v3_pipeline")
        .filter(|value| value.is_object())
        .ok_or_else(|| invalid("order fact has no Protocol-v3 pre-send pipeline"))?;
    let best_bid = value_decimal(&pipeline["best_bid"]);
    let best_ask = value_decimal(&pipeline["best_ask"]);
    let tick_size = value_decimal(&pre_send["tick_size"]);
    let spread_ticks = best_bid
        .zip(best_ask)
        .zip(tick_size)
        .and_then(|((bid, ask), tick)| {
            (tick > Decimal::ZERO && ask >= bid).then_some((ask - bid) / tick)
        });
    Ok(PreSendFeatures {
        expected_edge: value_decimal(&pre_send["expected_edge"]),
        public_size_ahead: value_decimal(&pre_send["pre_send_public_size_ahead"])
            .or_else(|| value_decimal(&pipeline["pre_send_public_size_ahead"])),
        spread_ticks,
        sigma: value_decimal(&pipeline["sigma"]),
        model_error: value_decimal(&pipeline["model_error"]),
        seconds_to_expiry: value_decimal(&pipeline["regime_features"]["seconds_to_expiry"]),
    })
}

fn required_text(value: &Value, field: &str) -> Result<String, ResearchError> {
    value[field]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid(format!("required field {field} is missing")))
}

fn nonempty_text(value: &Value) -> bool {
    value.as_str().is_some_and(|value| !value.is_empty())
}

fn parse_required_ts(value: &Value, field: &str) -> Result<DateTime<Utc>, ResearchError> {
    value
        .as_str()
        .and_then(parse_utc)
        .ok_or_else(|| invalid(format!("required timestamp {field} is missing or invalid")))
}

fn required_decimal(value: &Value, field: &str) -> Result<Decimal, ResearchError> {
    value_decimal(value).ok_or_else(|| invalid(format!("required decimal {field} is missing")))
}

fn value_decimal(value: &Value) -> Option<Decimal> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| value.as_f64().map(|number| number.to_string()))
        .or_else(|| value.as_i64().map(|number| number.to_string()))
        .and_then(|value| Decimal::from_str(&value).ok())
}

fn chronological_folds(days: &[String]) -> Vec<Value> {
    if days.len() < 3 {
        return Vec::new();
    }
    (1..days.len() - 1)
        .map(|validation_index| {
            json!({
                "fold_index": validation_index - 1,
                "train_days": days[..validation_index].to_vec(),
                "validation_day": days[validation_index],
                "test_day": days[validation_index + 1],
                "strictly_chronological": true
            })
        })
        .collect()
}

fn candidate_metrics(
    candidate: &LossRegimeCandidate,
    observations: &[OrderObservation],
    days: &[String],
) -> Value {
    let mut daily = days
        .iter()
        .cloned()
        .map(|day| (day, DailyMetrics::default()))
        .collect::<BTreeMap<_, _>>();
    let mut total_orders = 0usize;
    let mut accepted_orders = 0usize;
    let mut fill_rows = 0usize;
    let mut markets = BTreeSet::new();
    let mut settled_pnl = Decimal::ZERO;
    let mut markout_pnl = Decimal::ZERO;
    let mut markout_count = 0usize;
    for row in observations {
        let Some(day) = daily.get_mut(&row.day) else {
            continue;
        };
        total_orders += 1;
        day.total_orders += 1;
        if !candidate.accepts(&row.features) {
            continue;
        }
        accepted_orders += 1;
        fill_rows += row.fill_count;
        markets.insert(row.market_id.clone());
        settled_pnl += row.settled_net_pnl;
        markout_pnl += row.markout_30s_net_pnl;
        markout_count += row.markout_30s_count;
        day.accepted_orders += 1;
        day.fill_rows += row.fill_count;
        day.markets.insert(row.market_id.clone());
        day.settled_pnl += row.settled_net_pnl;
        day.markout_pnl += row.markout_30s_net_pnl;
        day.markout_count += row.markout_30s_count;
    }
    let daily_rows = daily
        .iter()
        .map(|(date, metrics)| metrics.as_json(date))
        .collect::<Vec<_>>();
    let daily_pnl = daily
        .values()
        .map(|metrics| metrics.settled_pnl)
        .collect::<Vec<_>>();
    let lower = block_bootstrap_daily_lower_95(&daily_pnl);
    json!({
        "days": days,
        "total_orders": total_orders,
        "accepted_orders": accepted_orders,
        "filtered_orders": total_orders.saturating_sub(accepted_orders),
        "acceptance_rate": ratio_json(accepted_orders, total_orders),
        "markets": markets.len(),
        "queue_shadow_fill_rows": fill_rows,
        "queue_qualified": true,
        "queue_qualified_settled_net_pnl": settled_pnl.to_string(),
        "net_executable_markout_30s_pnl": markout_pnl.to_string(),
        "net_executable_markout_30s_count": markout_count,
        "net_executable_markout_30s_mean": (markout_count > 0).then(|| (markout_pnl / Decimal::from(markout_count as u64)).to_string()),
        "daily_increments": daily_rows,
        "pnl_lower_95": lower.map(|value| value.to_string()),
        "pnl_lower_95_available": lower.is_some(),
        "pnl_lower_95_minimum_daily_clusters": BLOCK_DAYS * MIN_BLOCKS,
        "pnl_lower_95_method": "seven_day_circular_block_bootstrap_10000_resamples"
    })
}

#[derive(Clone, Debug, Default)]
struct DailyMetrics {
    total_orders: usize,
    accepted_orders: usize,
    fill_rows: usize,
    markets: BTreeSet<String>,
    settled_pnl: Decimal,
    markout_pnl: Decimal,
    markout_count: usize,
}

impl DailyMetrics {
    fn as_json(&self, date: &str) -> Value {
        json!({
            "date": date,
            "total_orders": self.total_orders,
            "accepted_orders": self.accepted_orders,
            "filtered_orders": self.total_orders.saturating_sub(self.accepted_orders),
            "markets": self.markets.len(),
            "queue_shadow_fill_rows": self.fill_rows,
            "queue_qualified_settled_net_pnl": self.settled_pnl.to_string(),
            "net_executable_markout_30s_pnl": self.markout_pnl.to_string(),
            "net_executable_markout_30s_count": self.markout_count
        })
    }
}

fn ratio_json(numerator: usize, denominator: usize) -> Value {
    if denominator == 0 {
        Value::Null
    } else {
        Decimal::from(numerator as u64)
            .checked_div(Decimal::from(denominator as u64))
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null)
    }
}

fn block_bootstrap_daily_lower_95(values: &[Decimal]) -> Option<Decimal> {
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
    let mut estimates = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let mut total = Decimal::ZERO;
        let mut sampled = 0usize;
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
    estimates.get((BOOTSTRAP_RESAMPLES * 25) / 1_000).copied()
}

fn render_markdown(report: &Value) -> String {
    let result = &report["result"];
    format!(
        "# Loss-Regime OOS Experiment\n\n- Experiment: **{}**\n- Source campaign: **{}**\n- Evidence: **diagnostic only / never promotion eligible**\n- Frozen at: **{}**\n- Sealed market days: **{}**\n- Selected validation candidate: **{}**\n- Queue source: `paper_shadow_lifecycle_plus_public_l2`\n- Queue position: `inferred_size_ahead` (literal FIFO unavailable)\n\nThe final market day is exposed only for the validation-selected candidate. A selected policy requires a new, separately frozen future campaign.\n",
        result["experiment_id"].as_str().unwrap_or("unknown"),
        result["source_campaign_id"].as_str().unwrap_or("unknown"),
        result["frozen_at"].as_str().unwrap_or("unknown"),
        result["counts"]["sealed_market_days"].as_u64().unwrap_or_default(),
        result["selection"]["candidate"].as_str().unwrap_or("none")
    )
}

fn write_outputs_new(
    out: &Path,
    markdown: &Path,
    value: &Value,
    rendered: &str,
) -> Result<(), ResearchError> {
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = markdown.parent() {
        fs::create_dir_all(parent)?;
    }
    let json_bytes = serde_json::to_vec_pretty(value)?;
    let mut json_file = OpenOptions::new().create_new(true).write(true).open(out)?;
    json_file.write_all(&json_bytes)?;
    if let Err(error) = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(markdown)
        .and_then(|mut file| file.write_all(rendered.as_bytes()))
    {
        let _ = fs::remove_file(out);
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);
    const TEST_INPUT_SHA256: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[derive(Clone)]
    struct OrderSpec {
        day_offset: i64,
        suffix: &'static str,
        edge: &'static str,
        outcome: &'static str,
        winner: &'static str,
        price: &'static str,
        size: &'static str,
        markout: &'static str,
    }

    fn test_root(name: &str) -> PathBuf {
        let id = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "polyedge-loss-oos-{name}-{}-{id}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn config_text(experiment_id: &str) -> String {
        format!(
            "schema_version: 1\nexperiment_id: {experiment_id}\nevidence_version: loss-regime-oos-v1\nfrozen_at: \"2026-07-22T00:00:00Z\"\nsource_campaign_id: campaign-2026-07-23\nresearch_only: true\ncandidates:\n  - name: baseline_no_abstention\n  - name: high_edge\n    minimum_expected_edge: \"0.02\"\n"
        )
    }

    fn fact_hash(mut row: Value) -> Value {
        let hash = canonical_value_sha256(&row).unwrap();
        row.as_object_mut()
            .unwrap()
            .insert("fact_sha256".to_owned(), Value::String(hash));
        row
    }

    fn order_and_fill(spec: &OrderSpec) -> (Value, Value) {
        let base = parse_utc("2026-07-23T00:00:00Z").unwrap() + Duration::days(spec.day_offset);
        let submitted = base + Duration::minutes(1);
        let end = base + Duration::minutes(10);
        let recorded = base + Duration::minutes(11);
        let order_id = format!("order-{}-{}", spec.day_offset, spec.suffix);
        let market_id = format!("market-{}-{}", spec.day_offset, spec.suffix);
        let fill_size = Decimal::from_str(spec.size).unwrap();
        let price = Decimal::from_str(spec.price).unwrap();
        let payout = if spec.outcome == spec.winner {
            Decimal::ONE
        } else {
            Decimal::ZERO
        };
        let settled = (payout - price) * fill_size;
        let fill = fact_hash(json!({
            "schema": FILL_FACT_SCHEMA_V1,
            "schema_version": 1,
            "fill_lifecycle_id": format!("fill-{order_id}"),
            "fill_source": "queue_shadow_fill",
            "order_id": order_id,
            "market_id": market_id,
            "token_id": format!("token-{}", spec.suffix),
            "side": "buy",
            "fill_price": spec.price,
            "fill_size": spec.size,
            "fee_per_share": "0",
            "markout_30s_status": "observed",
            "net_executable_markout_30s_pnl": spec.markout
        }));
        let order = fact_hash(json!({
            "schema": ORDER_FACT_SCHEMA_V2,
            "schema_version": 2,
            "evidence_classification": "protocol_v3_bound_diagnostic",
            "diagnostic_only": true,
            "counts_toward_protocol_v3_evidence": false,
            "execution_fields_complete": true,
            "queue_position_source": "paper_shadow_lifecycle_plus_public_l2",
            "queue_position": "inferred_size_ahead",
            "queue_proxy_pnl_eligible": true,
            "queue_registration_event_sha256": format!("sha256:registration-{order_id}"),
            "queue_snapshot_event_sha256": format!("sha256:snapshot-{order_id}"),
            "terminal_settlement_event_sha256": format!("sha256:settlement-{order_id}"),
            "terminal_settlement_journal_sha256": format!("sha256:journal-{order_id}"),
            "order_id": order_id,
            "market_id": market_id,
            "token_id": format!("token-{}", spec.suffix),
            "side": "buy",
            "order_size": spec.size,
            "fill_count": 1,
            "submitted_ts": ts(submitted),
            "terminal_market_end_ts": ts(end),
            "terminal_settlement_recorded_ts": ts(recorded),
            "terminal_winning_outcome": spec.winner,
            "terminal_settled_net_pnl": settled.to_string(),
            "outcome": spec.outcome,
            "pre_send": {
                "expected_edge": spec.edge,
                "pre_send_public_size_ahead": "10",
                "tick_size": "0.01",
                "protocol_v3_pipeline": {
                    "best_bid": "0.40",
                    "best_ask": "0.42",
                    "pre_send_public_size_ahead": "10",
                    "sigma": 0.5,
                    "model_error": "0.01",
                    "regime_features": {"seconds_to_expiry": 600.0}
                }
            }
        }));
        (order, fill)
    }

    fn jsonl_bytes(rows: &[Value]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut bytes, row).unwrap();
            bytes.push(b'\n');
        }
        bytes
    }

    fn write_facts(root: &Path, specs: &[OrderSpec]) -> PathBuf {
        let facts = root.join("facts");
        fs::create_dir_all(&facts).unwrap();
        let (orders, fills): (Vec<_>, Vec<_>) = specs.iter().map(order_and_fill).unzip();
        let order_bytes = jsonl_bytes(&orders);
        let fill_bytes = jsonl_bytes(&fills);
        let summary = json!({
            "result": {
                "schema": SUMMARY_SCHEMA_V1,
                "status": "complete_diagnostic",
                "eligible_protocol_v3_identity": true,
                "promotion_eligible": false,
                "counts_toward_protocol_v3_evidence": false,
                "snapshot_identity": {
                    "source_inventory_canonical_sha256": TEST_INPUT_SHA256
                },
                "coverage": {
                    "queue_fields": {"denominator": orders.len(), "observed": orders.len()},
                    "markout_30s": {"denominator": fills.len(), "observed": fills.len()}
                }
            }
        });
        let summary_bytes = serde_json::to_vec_pretty(&summary).unwrap();
        fs::write(facts.join(ORDER_FACT_FILE), &order_bytes).unwrap();
        fs::write(facts.join(FILL_FACT_FILE), &fill_bytes).unwrap();
        fs::write(facts.join(SUMMARY_FILE), &summary_bytes).unwrap();
        let artifact = |filename: &str, schema: &str, bytes: &[u8], rows: Option<usize>| {
            json!({
                "filename": filename,
                "schema": schema,
                "row_count": rows,
                "content_length": bytes.len(),
                "sha256": sha256_prefixed(bytes)
            })
        };
        let manifest = json!({
            "schema": ARTIFACT_MANIFEST_SCHEMA_V1,
            "schema_version": 1,
            "artifacts": [
                artifact(ORDER_FACT_FILE, ORDER_FACT_SCHEMA_V2, &order_bytes, Some(orders.len())),
                artifact(FILL_FACT_FILE, FILL_FACT_SCHEMA_V1, &fill_bytes, Some(fills.len())),
                artifact(SUMMARY_FILE, SUMMARY_SCHEMA_V1, &summary_bytes, None)
            ]
        });
        fs::write(
            facts.join(MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        facts
    }

    fn write_queue_evidence(root: &Path, facts: &Path) -> PathBuf {
        let mut market_eligibility = Map::new();
        for line in fs::read_to_string(facts.join(ORDER_FACT_FILE))
            .unwrap()
            .lines()
        {
            let order: Value = serde_json::from_str(line).unwrap();
            let market_id = order["market_id"].as_str().unwrap();
            let evidence = json!({
                "book_snapshot_count": 1,
                "price_change_count": 1,
                "level_change_count": 1,
                "trade_event_count": 1,
                "trade_size_count": 1,
                "depletion_event_count": 1,
                "order_lifecycle_count": 1,
                "size_ahead_samples": ["10"],
                "ignored_opposite_trade_count": 0,
                "missing_or_unknown_trade_side_count": 0,
                "queue_fill_event_count": 1,
                "queue_partial_fill_event_count": 0
            });
            market_eligibility.insert(
                market_id.to_owned(),
                json!({
                    "market_id": market_id,
                    "eligible": true,
                    "reasons": [],
                    "queue_fill_event_count": 1,
                    "queue_partial_fill_event_count": 0,
                    "queue_evidence_sha256": canonical_value_sha256(&evidence),
                    "queue_evidence": evidence
                }),
            );
        }
        let market_eligibility = Value::Object(market_eligibility);
        let market_eligibility_sha256 = canonical_value_sha256(&market_eligibility);
        let queue = json!({
            "queue_proxy_pnl_eligible": true,
            "ineligible_queue_fills": 0,
            "market_eligibility_schema": "polyedge.queue_proxy.market_eligibility.v1",
            "market_eligibility": market_eligibility,
            "market_eligibility_sha256": market_eligibility_sha256,
            "market_eligibility_diagnostic_only": true,
            "market_eligibility_counts_toward_protocol_v3_evidence": false,
            "input_binding": {
                "schema": "polyedge.queue_proxy.input_binding.v1",
                "sha256": TEST_INPUT_SHA256
            }
        });
        let report = json!({
            "result": {
                "fill_models": [{
                    "fill_model": "queue_proxy_conservative",
                    "replay_metrics": {"queue_proxy": queue}
                }]
            }
        });
        let path = root.join("baseline.json");
        fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        path
    }

    fn refresh_order_artifact(facts: &Path) {
        let bytes = fs::read(facts.join(ORDER_FACT_FILE)).unwrap();
        let manifest_path = facts.join(MANIFEST_FILE);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let artifact = manifest["artifacts"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|row| row["filename"] == ORDER_FACT_FILE)
            .unwrap();
        artifact["content_length"] = json!(bytes.len());
        artifact["sha256"] = json!(sha256_prefixed(&bytes));
        fs::write(manifest_path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    }

    fn options(root: &Path, facts: PathBuf, experiment_id: &str) -> LossRegimeOosOptions {
        let config = root.join("config.yaml");
        fs::write(&config, config_text(experiment_id)).unwrap();
        let queue_evidence = write_queue_evidence(root, &facts);
        let output_root = root
            .join("reports/research/experiments")
            .join(experiment_id);
        LossRegimeOosOptions {
            facts,
            queue_evidence,
            config,
            source_campaign_id: "campaign-2026-07-23".to_owned(),
            out: output_root.join("result.json"),
            markdown: output_root.join("result.md"),
        }
    }

    #[test]
    fn path_guard_rejects_shadow_roots_and_parent_traversal() {
        assert!(validate_output_path(
            Path::new("reports/research/shadow/campaigns/campaign-2026-07-23/result.json"),
            "experiment-safe"
        )
        .is_err());
        assert!(validate_output_path(
            Path::new("reports/research/experiments/experiment-safe/../result.json"),
            "experiment-safe"
        )
        .is_err());
        assert!(validate_output_path(
            Path::new("reports/research/experiments/experiment-safe/result.json"),
            "experiment-safe"
        )
        .is_ok());
    }

    #[test]
    fn queue_evidence_requires_exact_input_binding_and_untampered_market_map() {
        let root = test_root("queue-binding");
        let facts = write_facts(
            &root,
            &[OrderSpec {
                day_offset: 0,
                suffix: "one",
                edge: "0.03",
                outcome: "up",
                winner: "up",
                price: "0.40",
                size: "1",
                markout: "0.01",
            }],
        );
        let verified = verify_facts(&facts).unwrap();
        let queue_path = write_queue_evidence(&root, &facts);
        assert!(verify_queue_eligibility(&queue_path, &verified.input_binding_sha256).is_ok());

        let mut report: Value = serde_json::from_slice(&fs::read(&queue_path).unwrap()).unwrap();
        report["result"]["fill_models"][0]["replay_metrics"]["queue_proxy"]["input_binding"]
            ["sha256"] =
            json!("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        fs::write(&queue_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        let error =
            verify_queue_eligibility(&queue_path, &verified.input_binding_sha256).unwrap_err();
        assert!(error
            .to_string()
            .contains("exact loss-diagnostics normalized input"));
    }

    #[test]
    fn updated_artifact_manifest_cannot_hide_a_mutated_fact_hash() {
        let root = test_root("mutated-fact");
        let facts = write_facts(
            &root,
            &[OrderSpec {
                day_offset: 0,
                suffix: "one",
                edge: "0.03",
                outcome: "up",
                winner: "up",
                price: "0.40",
                size: "1",
                markout: "0.01",
            }],
        );
        let path = facts.join(ORDER_FACT_FILE);
        let mut row: Value =
            serde_json::from_str(fs::read_to_string(&path).unwrap().lines().next().unwrap())
                .unwrap();
        row["pre_send"]["expected_edge"] = json!("0.99");
        fs::write(&path, jsonl_bytes(&[row])).unwrap();
        refresh_order_artifact(&facts);
        let error = verify_facts(&facts).unwrap_err();
        assert!(error.to_string().contains("fact row 1 SHA-256 mismatch"));
    }

    #[test]
    fn config_rejects_post_send_features() {
        let text = "schema_version: 1\nexperiment_id: experiment-safe\nevidence_version: v1\nfrozen_at: 2026-07-22T00:00:00Z\nsource_campaign_id: campaign-2026-07-23\nresearch_only: true\ncandidates:\n  - name: baseline\n  - name: leaked\n    maximum_markout_30s: 0\n";
        let error = parse_config(text, sha256_prefixed(text.as_bytes())).unwrap_err();
        assert!(error.to_string().contains("post-send candidate field"));
    }

    #[test]
    fn partial_fill_settlement_arithmetic_is_outcome_side_and_fee_aware() {
        let first = settled_fill_pnl(
            "buy",
            "up",
            "up",
            Decimal::new(40, 2),
            Decimal::new(50, 2),
            Decimal::new(1, 2),
        );
        let second = settled_fill_pnl(
            "buy",
            "up",
            "up",
            Decimal::new(40, 2),
            Decimal::new(25, 2),
            Decimal::new(1, 2),
        );
        assert_eq!(first + second, Decimal::new(4425, 4));
    }

    #[test]
    fn final_test_outcome_cannot_change_validation_selection() {
        let common = [
            OrderSpec {
                day_offset: 0,
                suffix: "train",
                edge: "0.03",
                outcome: "up",
                winner: "up",
                price: "0.40",
                size: "1",
                markout: "0.01",
            },
            OrderSpec {
                day_offset: 1,
                suffix: "low",
                edge: "0.01",
                outcome: "up",
                winner: "down",
                price: "0.40",
                size: "1",
                markout: "-0.02",
            },
            OrderSpec {
                day_offset: 1,
                suffix: "high",
                edge: "0.03",
                outcome: "up",
                winner: "up",
                price: "0.40",
                size: "1",
                markout: "0.03",
            },
        ];
        let run = |name: &str, final_winner: &'static str| {
            let root = test_root(name);
            let mut specs = common.to_vec();
            specs.push(OrderSpec {
                day_offset: 2,
                suffix: "test",
                edge: "0.03",
                outcome: "up",
                winner: final_winner,
                price: "0.40",
                size: "1",
                markout: if final_winner == "up" {
                    "0.04"
                } else {
                    "-0.04"
                },
            });
            let facts = write_facts(&root, &specs);
            run_loss_regime_oos(options(&root, facts, name)).unwrap()
        };
        let positive = run("experiment-seal-positive", "up");
        let negative = run("experiment-seal-negative", "down");
        assert_eq!(
            positive["result"]["selection"]["candidate"],
            negative["result"]["selection"]["candidate"]
        );
        assert_eq!(positive["result"]["selection"]["candidate"], "high_edge");
        assert_ne!(
            positive["result"]["candidates"][0]["sealed_test"],
            negative["result"]["candidates"][0]["sealed_test"]
        );
        assert!(positive["result"]["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["selected"] == false)
            .all(|row| row["sealed_test"]["status"] == "sealed_not_selected"));
    }

    #[test]
    fn lower_bound_is_null_until_28_daily_clusters_and_deterministic_afterward() {
        assert_eq!(block_bootstrap_daily_lower_95(&[Decimal::ONE; 27]), None);
        let values = [Decimal::ONE; 28];
        let first = block_bootstrap_daily_lower_95(&values);
        let second = block_bootstrap_daily_lower_95(&values);
        assert_eq!(first, second);
        assert!(first.is_some_and(|value| value > Decimal::ZERO));
    }
}
