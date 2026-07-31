use chrono::{DateTime, Duration, Utc};
use polyedge_config::{ExecutionMode, RuntimeSettings};
use polyedge_domain::{
    BookLevel, BookState, DecisionAction, ExecutionIntentV1, FairValue, MarketSpec, OrderKind,
    Side, TradeDecision, EXECUTION_INTENT_V1_SCHEMA,
};
use polyedge_engine::{crypto_taker_fee_per_share, FrozenStrategyMode, StrategyDecisionMetadata};
use polyedge_reporting::research::{parse_azure_artifact_uri, PromotionManifestV1, PromotionPhase};
use polyedge_storage::{AzureBlobClient, AzureBlobError, ImmutableBlobWrite};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::env;

const MAX_INTENT_TTL_MS: i64 = 30_000;
const VENUE_GTD_SECURITY_BUFFER_SECONDS: i64 = 60;
const FUNDED_CANONICAL_MANIFEST_BLOB: &str = "reports/research/profitability/latest.json";
const CONSERVATIVE_PRIOR_VERSION: &str = "conservative-execution-prior-v1";
const CONSERVATIVE_PRIOR_SHA256: &str =
    "sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4";

#[derive(Clone, Debug)]
pub(super) struct IntentPublisherConfig {
    account: String,
    container: String,
    client_id: Option<String>,
    prefix: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PublishedIntent {
    pub blob_name: String,
    pub artifact_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IntentExecutionModel {
    version: String,
    blob_uri: String,
    sha256: String,
}

impl IntentExecutionModel {
    fn from_static_settings(settings: &RuntimeSettings) -> Self {
        Self {
            version: settings.azure.strategy_canary_fill_model_version.clone(),
            blob_uri: settings
                .azure
                .strategy_canary_execution_model_blob_uri
                .clone(),
            sha256: settings
                .azure
                .strategy_canary_execution_model_sha256
                .clone(),
        }
    }
}

impl IntentPublisherConfig {
    pub(super) fn from_settings(settings: &RuntimeSettings) -> Result<Self, String> {
        if !settings.azure.publish_strategy_canary_intents {
            return Err("strategy canary intent publication is disabled".to_owned());
        }
        let account = settings
            .azure
            .storage_account_name
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "Azure Storage account is unavailable".to_owned())?;
        let prefix = settings
            .azure
            .strategy_canary_intent_prefix
            .trim()
            .trim_matches('/')
            .to_owned();
        if prefix.is_empty() {
            return Err("strategy canary intent prefix is empty".to_owned());
        }
        Ok(Self {
            account,
            container: settings.azure.storage_container_name.clone(),
            client_id: env::var("AZURE_CLIENT_ID").ok(),
            prefix,
        })
    }

    pub(super) fn publish(&self, intent: &ExecutionIntentV1) -> Result<PublishedIntent, String> {
        intent.validate()?;
        let bytes = serde_json::to_vec_pretty(intent).map_err(|error| error.to_string())?;
        let artifact_sha256 = sha256_bytes(&bytes);
        let blob_name = intent_blob_name(&self.prefix, &intent.decision_id)?;
        let mut client = AzureBlobClient::with_managed_identity(
            self.account.clone(),
            self.container.clone(),
            self.client_id.clone(),
        );
        match client
            .upload_block_blob_bytes_if_absent(&blob_name, &bytes, "application/json")
            .map_err(|error| error.to_string())?
        {
            ImmutableBlobWrite::Created => Ok(PublishedIntent {
                blob_name,
                artifact_sha256,
            }),
            ImmutableBlobWrite::AlreadyExists => Err(format!(
                "immutable strategy canary intent already exists: {blob_name}"
            )),
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) fn build_execution_intent(
    settings: &RuntimeSettings,
    market: &MarketSpec,
    fair_value: &FairValue,
    reference: &polyedge_domain::ReferencePrice,
    book: &BookState,
    decision: &TradeDecision,
    metadata: &StrategyDecisionMetadata,
    decision_ts: DateTime<Utc>,
) -> Result<ExecutionIntentV1, String> {
    let execution_model = IntentExecutionModel::from_static_settings(settings);
    build_execution_intent_with_model(
        settings,
        market,
        fair_value,
        reference,
        book,
        decision,
        metadata,
        decision_ts,
        &execution_model,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_execution_intent_with_model(
    settings: &RuntimeSettings,
    market: &MarketSpec,
    fair_value: &FairValue,
    reference: &polyedge_domain::ReferencePrice,
    book: &BookState,
    decision: &TradeDecision,
    metadata: &StrategyDecisionMetadata,
    decision_ts: DateTime<Utc>,
    execution_model: &IntentExecutionModel,
) -> Result<ExecutionIntentV1, String> {
    if settings.live.execution_mode != ExecutionMode::Paper
        || settings.live.allow_live
        || settings.live.polymarket_private_key.is_some()
    {
        return Err("intent publisher requires a credential-free paper runtime".to_owned());
    }
    let frozen = FrozenStrategyMode::DynamicQuoteStyle.candidate();
    if metadata.candidate != frozen {
        return Err("decision is not from the frozen dynamic_quote_style candidate".to_owned());
    }
    if !metadata.data_quality.decision_grade {
        return Err(
            "shared strategy evaluator did not mark the decision decision-grade".to_owned(),
        );
    }
    if decision.action != DecisionAction::Place {
        return Err("strategy decision is not a place action".to_owned());
    }
    if decision.market_id != market.market_id || fair_value.market_id != market.market_id {
        return Err("decision or fair value market_id does not match the market".to_owned());
    }
    if !market.is_tradeable() || !market.accepting_orders {
        return Err("market is not tradeable and accepting orders".to_owned());
    }
    if decision.side != Some(Side::Buy) || !decision.post_only {
        return Err("strategy decision is not a post-only BUY".to_owned());
    }
    if !matches!(
        decision.order_kind,
        Some(OrderKind::PostOnlyGtc | OrderKind::PostOnlyGtd)
    ) {
        return Err("strategy decision is not a post-only order kind".to_owned());
    }
    let condition_id = decision
        .condition_id
        .clone()
        .ok_or_else(|| "decision condition_id is missing".to_owned())?;
    if condition_id != market.condition_id {
        return Err("decision condition_id does not match the market".to_owned());
    }
    let token_id = decision
        .token_id
        .clone()
        .ok_or_else(|| "decision token_id is missing".to_owned())?;
    if token_id != book.token_id
        || (token_id != market.up_token_id && token_id != market.down_token_id)
    {
        return Err("decision token_id does not match the captured market book".to_owned());
    }
    let outcome = decision
        .outcome
        .clone()
        .ok_or_else(|| "decision outcome is missing".to_owned())?;
    let expected_token = match &outcome {
        polyedge_domain::Outcome::Up => &market.up_token_id,
        polyedge_domain::Outcome::Down => &market.down_token_id,
    };
    if &token_id != expected_token {
        return Err("decision outcome does not match the selected token".to_owned());
    }
    let price = decision
        .price
        .ok_or_else(|| "decision price is missing".to_owned())?;
    let requested_shares = decision
        .size
        .ok_or_else(|| "decision share size is missing".to_owned())?;
    if market.minimum_order_size <= Decimal::ZERO {
        return Err("venue minimum_order_size must be positive".to_owned());
    }
    let shares = requested_shares.max(market.minimum_order_size);
    let notional = price * shares;
    if price <= Decimal::ZERO
        || price >= Decimal::ONE
        || shares <= Decimal::ZERO
        || notional > Decimal::ONE
    {
        return Err(
            "decision price, size, or notional violates the one-dollar canary cap".to_owned(),
        );
    }
    let best_ask = book
        .best_ask()
        .ok_or_else(|| "captured book has no ask".to_owned())?
        .price;
    if price >= best_ask {
        return Err("post-only BUY would cross the captured ask".to_owned());
    }
    let reference_age_ms = source_age_ms(decision_ts, reference.local_ts, "reference")?;
    let book_age_ms = source_age_ms(decision_ts, book.local_ts, "book")?;
    if reference.stale || reference_age_ms > settings.risk.max_reference_age_ms {
        return Err("exact resolution reference is stale".to_owned());
    }
    if book_age_ms > settings.risk.max_book_age_ms {
        return Err("captured order book is stale".to_owned());
    }
    if !reference.exact_resolution_source
        || reference.source != market.resolution_source
        || reference.source != settings.target.resolution_source
    {
        return Err("exact market resolution source is not confirmed".to_owned());
    }
    let q = metadata
        .q
        .ok_or_else(|| "strategy probability q is missing".to_owned())?;
    let gross_edge = q - price;
    let fee_allowance = if market.fees_enabled {
        crypto_taker_fee_per_share(price).map_err(|error| error.to_string())?
    } else {
        Decimal::ZERO
    };
    let slippage_allowance = settings.strategy.slippage_buffer;
    let toxicity_allowance = settings.strategy.adverse_selection_buffer + fair_value.model_error;
    let net_edge_lower_bound = gross_edge - fee_allowance - slippage_allowance - toxicity_allowance;
    if net_edge_lower_bound <= Decimal::ZERO {
        return Err("conservative net-edge lower bound is not positive".to_owned());
    }
    let ttl_ms = decision
        .ttl_ms
        .unwrap_or(settings.strategy.order_ttl_seconds * 1_000)
        .clamp(1, MAX_INTENT_TTL_MS);
    let valid_until = decision_ts + Duration::milliseconds(ttl_ms);
    let gtd_expiry_ts = valid_until + Duration::seconds(VENUE_GTD_SECURITY_BUFFER_SECONDS);
    let book_hash = canonical_book_hash(market, book);
    let features_digest = sha256_bytes(
        &serde_json::to_vec(&metadata.features_summary).map_err(|error| error.to_string())?,
    );
    let identity = json!({
        "candidate_name": frozen.name,
        "candidate_version": frozen.version,
        "candidate_config_hash": frozen.config_hash,
        "market_id": market.market_id,
        "condition_id": market.condition_id,
        "token_id": token_id,
        "outcome": outcome,
        "side": "buy",
        "price": price.to_string(),
        "shares": shares.to_string(),
        "decision_ts": decision_ts,
        "valid_until": valid_until,
        "gtd_expiry_ts": gtd_expiry_ts,
        "minimum_order_size": market.minimum_order_size.to_string(),
        "book_hash": book_hash,
        "features_digest": features_digest,
        "reference_source_ts": reference.source_ts,
        "execution_model_blob_uri": execution_model.blob_uri,
        "execution_model_sha256": execution_model.sha256,
    });
    let decision_id =
        sha256_hex(&serde_json::to_vec(&identity).map_err(|error| error.to_string())?);
    let intent = ExecutionIntentV1 {
        schema: EXECUTION_INTENT_V1_SCHEMA.to_owned(),
        decision_id,
        candidate_name: frozen.name,
        candidate_version: frozen.version,
        candidate_config_hash: frozen.config_hash,
        market_id: market.market_id.clone(),
        condition_id,
        token_id,
        outcome,
        side: Side::Buy,
        price,
        shares,
        notional,
        minimum_order_size: market.minimum_order_size,
        post_only: true,
        order_kind: OrderKind::PostOnlyGtd,
        ttl_ms,
        decision_ts,
        valid_until,
        gtd_expiry_ts: Some(gtd_expiry_ts),
        book_hash,
        q,
        gross_edge,
        fee_allowance,
        slippage_allowance,
        toxicity_allowance,
        net_edge_lower_bound,
        regime: metadata.regime.as_str().to_owned(),
        features_digest,
        reference_age_ms,
        book_age_ms,
        exact_resolution_source: true,
        resolution_source: reference.source.clone(),
        required_fill_model_version: execution_model.version.clone(),
        execution_model_blob_uri: execution_model.blob_uri.clone(),
        execution_model_sha256: execution_model.sha256.clone(),
    };
    Ok(intent)
}

pub(super) fn resolve_execution_model(
    settings: &RuntimeSettings,
    decision_ts: DateTime<Utc>,
) -> Result<IntentExecutionModel, String> {
    let account = settings
        .azure
        .storage_account_name
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "Azure Storage account is unavailable for model control".to_owned())?;
    let funded_container = required_env("AZURE_FUNDED_STORAGE_CONTAINER_NAME")?;
    let model_container = required_env("AZURE_MODEL_STORAGE_CONTAINER_NAME")?;
    let client_id = env::var("AZURE_CLIENT_ID").ok();
    let mut canonical_client = AzureBlobClient::with_managed_identity(
        account.clone(),
        funded_container,
        client_id.clone(),
    );
    let canonical = match canonical_client.download_blob_bytes(FUNDED_CANONICAL_MANIFEST_BLOB) {
        Ok(bytes) => Some(bytes),
        Err(AzureBlobError::HttpStatus(404)) => None,
        Err(error) => {
            return Err(format!(
                "canonical funded model control is unreadable: {error}"
            ))
        }
    };
    select_execution_model_from_control(settings, decision_ts, canonical.as_deref(), |uri| {
        let (uri_account, uri_container, blob_name) =
            parse_azure_artifact_uri(uri).map_err(|error| error.to_string())?;
        if uri_account != account || uri_container != model_container {
            return Err(
                "canonical queue model escaped the configured account/model container".to_owned(),
            );
        }
        let mut model_client = AzureBlobClient::with_managed_identity(
            account.clone(),
            model_container.clone(),
            client_id.clone(),
        );
        model_client
            .download_blob_bytes(&blob_name)
            .map_err(|error| format!("exact canonical queue model is unreadable: {error}"))
    })
}

fn select_execution_model_from_control<F>(
    settings: &RuntimeSettings,
    decision_ts: DateTime<Utc>,
    canonical_bytes: Option<&[u8]>,
    load_model: F,
) -> Result<IntentExecutionModel, String>
where
    F: FnOnce(&str) -> Result<Vec<u8>, String>,
{
    let prior = validated_conservative_prior(settings)?;
    let Some(canonical_bytes) = canonical_bytes else {
        return Ok(prior);
    };
    let manifest: PromotionManifestV1 = serde_json::from_slice(canonical_bytes)
        .map_err(|error| format!("canonical funded model control is invalid JSON: {error}"))?;
    let ladder = manifest
        .funded_ladder
        .as_ref()
        .ok_or_else(|| "canonical funded model control has no ladder state".to_owned())?;
    ladder
        .validate()
        .map_err(|error| format!("canonical funded ladder is invalid: {error}"))?;
    let frozen = FrozenStrategyMode::DynamicQuoteStyle.candidate();
    let candidate_is_frozen = |candidate: &polyedge_reporting::research::CandidateIdentity| {
        candidate.name == frozen.name
            && candidate.candidate_version == frozen.version
            && candidate.config_hash == frozen.config_hash
    };
    if manifest.schema_version != "promotion_manifest_v1"
        || !candidate_is_frozen(&manifest.candidate)
        || !candidate_is_frozen(&ladder.candidate)
        || manifest.phase != ladder.phase
        || manifest.gate_metrics.phase != PromotionPhase::ShadowPassed
        || !manifest.gate_metrics.promotion_allowed
        || manifest.promotion_allowed
        || !manifest.human_authorization_required
        || manifest.created_at > decision_ts
        || manifest.expires_at <= decision_ts
    {
        return Err("canonical funded model control failed manifest invariants".to_owned());
    }
    if ladder.terminal {
        return Err(
            "canonical funded campaign is terminal; intent publication is stopped".to_owned(),
        );
    }
    if ladder.active_target_orders != 200 {
        if manifest.execution_model.model_version != prior.version
            || manifest.execution_model.blob_uri != prior.blob_uri
            || manifest.execution_model.sha256 != prior.sha256
        {
            return Err("pre-checkpoint-100 canonical state changed the frozen prior".to_owned());
        }
        return Ok(prior);
    }
    if manifest.phase != PromotionPhase::LimitedLive
        || ladder.phase != PromotionPhase::LimitedLive
        || ladder.completed_checkpoints != [1, 5, 25, 100]
        || ladder.metrics.cumulative_funded_orders != 100
        || ladder.metrics.cumulative_eligible_orders != 100
        || !ladder.metrics.data_quality_passed
        || !ladder.metrics.unresolved_exposure.is_zero()
    {
        return Err("post-checkpoint-100 canonical state is incomplete".to_owned());
    }
    let transition = ladder
        .queue_model_transition
        .as_ref()
        .filter(|transition| transition.model_quality_passed)
        .ok_or_else(|| {
            "post-checkpoint-100 queue-model transition is absent or failed".to_owned()
        })?;
    if manifest.execution_model != transition.binding
        || transition.binding.model_version != "queue-calibration-v1"
        || transition.training_cutoff >= transition.generated_at
        || transition.generated_at >= decision_ts
    {
        return Err("post-checkpoint-100 model binding or temporal lineage is invalid".to_owned());
    }
    let model_bytes = load_model(&transition.binding.blob_uri)?;
    if sha256_bytes(&model_bytes) != transition.binding.sha256 {
        return Err("post-checkpoint-100 model bytes do not match canonical SHA-256".to_owned());
    }
    validate_queue_model_bytes(
        &model_bytes,
        manifest.candidate.clone(),
        transition,
        decision_ts,
    )?;
    Ok(IntentExecutionModel {
        version: transition.binding.model_version.clone(),
        blob_uri: transition.binding.blob_uri.clone(),
        sha256: transition.binding.sha256.clone(),
    })
}

fn validated_conservative_prior(
    settings: &RuntimeSettings,
) -> Result<IntentExecutionModel, String> {
    let prior = IntentExecutionModel::from_static_settings(settings);
    if prior.version != CONSERVATIVE_PRIOR_VERSION
        || prior.sha256 != CONSERVATIVE_PRIOR_SHA256
        || parse_azure_artifact_uri(&prior.blob_uri).is_err()
    {
        return Err("static execution model is not the exact frozen conservative prior".to_owned());
    }
    Ok(prior)
}

fn validate_queue_model_bytes(
    bytes: &[u8],
    candidate: polyedge_reporting::research::CandidateIdentity,
    transition: &polyedge_reporting::research::QueueModelTransitionV1,
    decision_ts: DateTime<Utc>,
) -> Result<(), String> {
    let model: Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("queue model is invalid JSON: {error}"))?;
    let expected_candidate = serde_json::to_value(candidate).map_err(|error| error.to_string())?;
    let generated_at = model
        .get("generated_at")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    let training_cutoff = model
        .get("training_cutoff")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    let checkpoint_sha = model
        .pointer("/training_checkpoint/sha256")
        .and_then(Value::as_str);
    let dataset_sha = model
        .pointer("/training_dataset/sha256")
        .and_then(Value::as_str);
    let finite_array = |pointer: &str, positive: bool| {
        model
            .pointer(pointer)
            .and_then(Value::as_array)
            .is_some_and(|values| {
                values.len() == 10
                    && values.iter().all(|value| {
                        value
                            .as_f64()
                            .is_some_and(|number| number.is_finite() && (!positive || number > 0.0))
                    })
            })
    };
    let exact_features = [
        "bias",
        "log_inferred_size_ahead",
        "spread",
        "order_price",
        "order_size",
        "log_time_to_expiry",
        "log_pre_send_trade_size",
        "pre_send_depth_changes",
        "pre_send_volatility",
        "horizon_seconds",
    ];
    let training_orders = model
        .pointer("/training_dataset/orders")
        .and_then(Value::as_array);
    let base_rates_valid = ["1", "5", "30"].into_iter().all(|horizon| {
        model
            .pointer(&format!("/training_horizon_base_rates/{horizon}"))
            .and_then(Value::as_f64)
            .is_some_and(|rate| rate.is_finite() && (0.0..=1.0).contains(&rate))
    });
    if model.get("schema").and_then(Value::as_str) != Some("polyedge.execution_queue_model.v1")
        || model.get("model_version").and_then(Value::as_str) != Some("queue-calibration-v1")
        || model.get("status").and_then(Value::as_str) != Some("trained_research_only")
        || model
            .get("evidence_protocol_version")
            .and_then(Value::as_u64)
            != Some(3)
        || model.get("sample_size").and_then(Value::as_u64) != Some(100)
        || model
            .get("positive_fills")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            < 10
        || model
            .get("negative_non_fills")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            < 10
        || model.get("promotion_ready").and_then(Value::as_bool) != Some(true)
        || model.get("promotion_allowed").and_then(Value::as_bool) != Some(false)
        || model.get("queue_position_source").and_then(Value::as_str)
            != Some("authenticated_lifecycle_plus_public_l2")
        || model.get("queue_position_metric").and_then(Value::as_str) != Some("inferred_size_ahead")
        || model
            .get("literal_fifo_rank_available")
            .and_then(Value::as_bool)
            != Some(false)
        || model.get("candidate") != Some(&expected_candidate)
        || model.get("training_data_end_ts").and_then(Value::as_str)
            != model.get("training_cutoff").and_then(Value::as_str)
        || generated_at != Some(transition.generated_at)
        || training_cutoff != Some(transition.training_cutoff)
        || generated_at.is_none_or(|value| value >= decision_ts)
        || checkpoint_sha != Some(transition.training_checkpoint_sha256.as_str())
        || dataset_sha != Some(transition.training_dataset_sha256.as_str())
        || model
            .pointer("/training_dataset/exact_order_count")
            .and_then(Value::as_u64)
            != Some(100)
        || training_orders.is_none_or(|orders| orders.len() != 100)
        || model
            .get("feature_names")
            .and_then(Value::as_array)
            .is_none_or(|features| features.iter().filter_map(Value::as_str).ne(exact_features))
        || !finite_array("/weights", false)
        || !finite_array("/normalization/means", false)
        || !finite_array("/normalization/scales", true)
        || !base_rates_valid
        || model
            .pointer("/quality_gates/passed")
            .and_then(Value::as_bool)
            != Some(true)
        || model
            .get("brier_improvement_fraction")
            .and_then(Value::as_f64)
            .is_none_or(|value| value < 0.05)
        || model
            .get("expected_calibration_error")
            .and_then(Value::as_f64)
            .is_none_or(|value| value > 0.10)
        || model
            .get("net_executable_markout_30s_lower_confidence_bound_95")
            .and_then(Value::as_f64)
            .is_none_or(|value| value <= 0.0)
    {
        return Err("queue model schema, quality, or exact training lineage is invalid".to_owned());
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String, String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{name} is required for dynamic execution-model control"))
}

fn source_age_ms(now: DateTime<Utc>, source_ts: DateTime<Utc>, name: &str) -> Result<i64, String> {
    let age = now.signed_duration_since(source_ts).num_milliseconds();
    if age < 0 {
        return Err(format!("{name} timestamp is in the future"));
    }
    Ok(age)
}

fn canonical_book_hash(market: &MarketSpec, book: &BookState) -> String {
    let levels = |values: &[BookLevel]| {
        let mut values = values.to_vec();
        values.sort_by(|left, right| {
            left.price
                .cmp(&right.price)
                .then(left.size.cmp(&right.size))
        });
        values
            .into_iter()
            .map(|level| {
                json!({
                    "price": canonical_decimal(level.price),
                    "size": canonical_decimal(level.size)
                })
            })
            .collect::<Vec<Value>>()
    };
    let value = json!({
        "asks": levels(&book.asks),
        "bids": levels(&book.bids),
        "min_order_size": canonical_decimal(market.minimum_order_size),
        "tick_size": canonical_decimal(market.tick_size),
        "token_id": book.token_id.to_string(),
    });
    sha256_bytes(&serde_json::to_vec(&value).expect("book hash JSON is serializable"))
}

fn canonical_decimal(value: Decimal) -> String {
    value.round_dp(12).normalize().to_string()
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

fn intent_blob_name(prefix: &str, decision_id: &str) -> Result<String, String> {
    if decision_id.len() != 64
        || !decision_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("decision_id is not a lowercase SHA-256 digest".to_owned());
    }
    Ok(format!(
        "{}/{}.json",
        prefix.trim().trim_matches('/'),
        decision_id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyedge_domain::{
        BookLevel, ConditionId, MarketId, MarketStatus, Outcome, ReferencePrice, TokenId,
    };
    use polyedge_engine::{RegimeLabel, StrategyDataQuality};
    use polyedge_reporting::research::{
        CandidateIdentity, DataQualitySummary, ExecutionModelBinding, FundedLadderMetrics,
        FundedLadderStateV1, ImmutableArtifactBindingV1, ProfitabilityMetrics, PromotionEvaluation,
        QueueModelTransitionV1,
    };
    use std::collections::BTreeMap;

    fn fixture() -> (
        RuntimeSettings,
        MarketSpec,
        FairValue,
        ReferencePrice,
        BookState,
        TradeDecision,
        StrategyDecisionMetadata,
        DateTime<Utc>,
    ) {
        let now = Utc::now();
        let mut settings = RuntimeSettings::default();
        settings.risk.max_reference_age_ms = 1_500;
        settings.risk.max_book_age_ms = 1_500;
        settings.strategy.slippage_buffer = Decimal::new(1, 3);
        settings.azure.strategy_canary_execution_model_blob_uri =
            "azure://test-account/bot-events/reports/research/venue-probe/effective_queue_model.json"
                .to_owned();
        settings.azure.strategy_canary_execution_model_sha256 =
            format!("sha256:{}", "a".repeat(64));
        let market = MarketSpec {
            asset: "BTC".to_owned(),
            horizon: "15m".to_owned(),
            event_id: None,
            event_slug: None,
            market_id: MarketId::new("market-1"),
            market_slug: None,
            condition_id: ConditionId::new("condition-1"),
            question: "Up?".to_owned(),
            description: None,
            up_token_id: TokenId::new("token-up"),
            down_token_id: TokenId::new("token-down"),
            start_ts: now - Duration::minutes(1),
            end_ts: now + Duration::minutes(10),
            start_price: Some(Decimal::from(100)),
            resolution_source: "chainlink_reference".to_owned(),
            tick_size: Decimal::new(1, 2),
            minimum_order_size: Decimal::ONE,
            neg_risk: false,
            fees_enabled: false,
            accepting_orders: true,
            status: MarketStatus::Tradeable,
            raw: BTreeMap::new(),
        };
        let fair_value = FairValue {
            market_id: market.market_id.clone(),
            q_up: Decimal::new(55, 2),
            q_down: Decimal::new(45, 2),
            sigma: 0.2,
            drift_mu: 0.0,
            model_error: Decimal::new(1, 2),
            computed_ts: now,
        };
        let reference = ReferencePrice {
            source: "chainlink_reference".to_owned(),
            price: Decimal::from(100),
            source_ts: now - Duration::milliseconds(50),
            local_ts: now - Duration::milliseconds(40),
            latency_ms: 10.0,
            stale: false,
            exact_resolution_source: true,
            quality_flags: Vec::new(),
        };
        let book = BookState {
            token_id: market.up_token_id.clone(),
            bids: vec![BookLevel {
                price: Decimal::new(40, 2),
                size: Decimal::from(10),
            }],
            asks: vec![BookLevel {
                price: Decimal::new(50, 2),
                size: Decimal::from(10),
            }],
            last_trade_price: None,
            exchange_ts: Some(now - Duration::milliseconds(25)),
            local_ts: now - Duration::milliseconds(20),
            book_hash: None,
        };
        let decision = TradeDecision {
            action: DecisionAction::Place,
            market_id: market.market_id.clone(),
            condition_id: Some(market.condition_id.clone()),
            token_id: Some(market.up_token_id.clone()),
            outcome: Some(Outcome::Up),
            side: Some(Side::Buy),
            price: Some(Decimal::new(45, 2)),
            size: Some(Decimal::from(2)),
            quote_amount: None,
            order_kind: Some(OrderKind::PostOnlyGtc),
            reason: "maker edge exceeds threshold".to_owned(),
            ttl_ms: Some(10_000),
            expected_edge: Some(Decimal::new(85, 3)),
            post_only: true,
            tick_size: Some(market.tick_size),
            neg_risk: false,
        };
        let metadata = StrategyDecisionMetadata {
            candidate: FrozenStrategyMode::DynamicQuoteStyle.candidate(),
            regime: RegimeLabel::Normal,
            q: Some(fair_value.q_up),
            expected_edge: decision.expected_edge,
            data_quality: StrategyDataQuality {
                decision_grade: true,
                reference_stale: false,
                book_stale: false,
                market_active: true,
                has_start_price: true,
                has_books: true,
                flags: Vec::new(),
            },
            features_summary: json!({"book": "liquid", "shock_z": 0.1}),
        };
        (
            settings, market, fair_value, reference, book, decision, metadata, now,
        )
    }

    fn post_100_control(
        settings: &mut RuntimeSettings,
        decision_ts: DateTime<Utc>,
    ) -> (Vec<u8>, Vec<u8>) {
        settings.azure.strategy_canary_execution_model_sha256 =
            CONSERVATIVE_PRIOR_SHA256.to_owned();
        let frozen = FrozenStrategyMode::DynamicQuoteStyle.candidate();
        let candidate = CandidateIdentity {
            name: frozen.name,
            candidate_version: frozen.version,
            config_hash: frozen.config_hash,
        };
        let generated_at = decision_ts - Duration::hours(1);
        let training_cutoff = generated_at - Duration::minutes(1);
        let training_dataset_sha256 = format!("sha256:{}", "c".repeat(64));
        let training_checkpoint_sha256 = format!("sha256:{}", "d".repeat(64));
        let training_orders = (1..=100)
            .map(|index| {
                json!({
                    "run_id": format!("run-{index}"),
                    "probe_id": format!("probe-{index}"),
                    "order_id": format!("order-{index}"),
                    "observed_at": training_cutoff,
                    "summary_blob_name": format!("summary/{index}.json"),
                    "summary_sha256": format!("sha256:{:064x}", index)
                })
            })
            .collect::<Vec<_>>();
        let model = json!({
            "schema": "polyedge.execution_queue_model.v1",
            "generated_at": generated_at,
            "candidate": candidate,
            "training_checkpoint": {"blob_name": "checkpoint/100.json", "sha256": training_checkpoint_sha256},
            "training_cutoff": training_cutoff,
            "training_data_end_ts": training_cutoff,
            "training_dataset": {
                "schema": "polyedge.queue_calibration_training_dataset.v1",
                "exact_order_count": 100,
                "sha256": training_dataset_sha256,
                "orders": training_orders
            },
            "training_horizon_base_rates": {"1": 0.2, "5": 0.3, "30": 0.4},
            "model_version": "queue-calibration-v1",
            "status": "trained_research_only",
            "evidence_protocol_version": 3,
            "queue_position_source": "authenticated_lifecycle_plus_public_l2",
            "queue_position_metric": "inferred_size_ahead",
            "literal_fifo_rank_available": false,
            "sample_size": 100,
            "positive_fills": 50,
            "negative_non_fills": 50,
            "feature_names": [
                "bias", "log_inferred_size_ahead", "spread", "order_price", "order_size",
                "log_time_to_expiry", "log_pre_send_trade_size", "pre_send_depth_changes",
                "pre_send_volatility", "horizon_seconds"
            ],
            "weights": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "normalization": {
                "means": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "scales": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
            },
            "quality_gates": {"passed": true},
            "brier_improvement_fraction": 0.05,
            "expected_calibration_error": 0.10,
            "net_executable_markout_30s_lower_confidence_bound_95": 0.01,
            "promotion_ready": true,
            "promotion_allowed": false
        });
        let model_bytes = serde_json::to_vec(&model).unwrap();
        let binding = ExecutionModelBinding {
            blob_uri: "azure://test-account/polyedge-models/queue/model.json".to_owned(),
            sha256: sha256_bytes(&model_bytes),
            model_version: "queue-calibration-v1".to_owned(),
        };
        let transition = QueueModelTransitionV1 {
            schema_version: "queue_model_transition_v1".to_owned(),
            binding: binding.clone(),
            generated_at,
            training_cutoff,
            training_dataset_sha256,
            training_checkpoint_sha256,
            model_quality_passed: true,
        };
        let artifact = ImmutableArtifactBindingV1 {
            blob_name: "terminal/100.json".to_owned(),
            sha256: format!("sha256:{}", "e".repeat(64)),
        };
        let ladder = FundedLadderStateV1 {
            schema_version: "funded_ladder_state_v1".to_owned(),
            campaign_id: "campaign-post-100".to_owned(),
            candidate: candidate.clone(),
            phase: PromotionPhase::LimitedLive,
            stage_targets: vec![1, 5, 25, 100, 200],
            active_stage_index: 4,
            active_target_orders: 200,
            completed_checkpoints: vec![1, 5, 25, 100],
            metrics: FundedLadderMetrics {
                observed_calendar_days: 10,
                cumulative_eligible_orders: 100,
                cumulative_funded_orders: 100,
                cumulative_net_pnl: Decimal::ONE,
                cumulative_max_drawdown: Decimal::new(5, 1),
                mean_net_markout_30s: Decimal::new(1, 2),
                net_markout_30s_lower_95: Decimal::new(1, 3),
                markout_sample_size: 50,
                data_quality_passed: true,
                unresolved_exposure: Decimal::ZERO,
            },
            maximum_calendar_days: 60,
            maximum_funded_orders: 200,
            maximum_drawdown: Decimal::ONE,
            human_grant_required: true,
            stage_authorized: false,
            consumed_grant_ids: vec![
                "canary".to_owned(),
                "stage-5".to_owned(),
                "stage-25".to_owned(),
                "stage-100".to_owned(),
            ],
            checkpoint_1_protocol_v3_artifact: Some(ImmutableArtifactBindingV1 {
                blob_name: "summary/1.json".to_owned(),
                sha256: format!("sha256:{}", "f".repeat(64)),
            }),
            checkpoint_1_terminal_artifact: Some(artifact.clone()),
            last_verified_terminal_artifact: Some(artifact),
            queue_model_transition: Some(transition),
            holdout_evaluation: None,
            terminal: false,
            promotion_allowed: false,
            created_at: decision_ts - Duration::days(10),
            updated_at: decision_ts - Duration::minutes(30),
        };
        ladder.validate().unwrap();
        let metrics = ProfitabilityMetrics {
            observed_calendar_days: 30,
            clean_days: 30,
            settled_markets: 1_000,
            wallet_constrained: true,
            queue_conservative: true,
            wallet_constrained_net_pnl: Decimal::ONE,
            wallet_constrained_ending_equity: Decimal::from(10),
            queue_conservative_net_pnl: Decimal::ONE,
            pnl_ci_95_low: Decimal::new(1, 2),
            consecutive_positive_weekly_blocks: 4,
            max_drawdown: Decimal::new(5, 1),
            drawdown_limit: Decimal::ONE,
            markout_30s_ci_low: Decimal::new(1, 3),
            replay_runtime_parity: true,
            decision_parity_rate: Decimal::ONE,
            execution_model_protocol_version: 3,
            execution_model_eligible_orders: 100,
            execution_model_filled_orders: 50,
            execution_model_non_filled_orders: 50,
            execution_model_brier_improvement: Decimal::new(5, 2),
            execution_model_expected_calibration_error: Decimal::new(10, 2),
            execution_model_promotion_ready: true,
            execution_model_markout_30s_lower_95: Decimal::new(1, 3),
            data_quality: DataQualitySummary::new(1_000, Decimal::ONE, Vec::new(), Vec::new()),
            missing_metrics: Vec::new(),
        };
        let manifest = PromotionManifestV1 {
            schema_version: "promotion_manifest_v1".to_owned(),
            candidate,
            phase: PromotionPhase::LimitedLive,
            gate_metrics: PromotionEvaluation {
                schema_version: 1,
                phase: PromotionPhase::ShadowPassed,
                promotion_allowed: true,
                gates: Vec::new(),
                metrics,
            },
            artifact_uris: BTreeMap::new(),
            execution_model: binding,
            funded_ladder: Some(ladder),
            human_authorization_required: true,
            promotion_allowed: false,
            created_at: decision_ts - Duration::days(10),
            expires_at: decision_ts + Duration::days(50),
        };
        (serde_json::to_vec(&manifest).unwrap(), model_bytes)
    }

    #[test]
    fn publication_switch_defaults_off() {
        assert!(
            !RuntimeSettings::default()
                .azure
                .publish_strategy_canary_intents
        );
    }

    #[test]
    fn builds_canary_schema_with_hash_address_and_conservative_edge() {
        let (settings, market, fair, reference, book, decision, metadata, now) = fixture();
        let intent = build_execution_intent(
            &settings, &market, &fair, &reference, &book, &decision, &metadata, now,
        )
        .unwrap();
        intent.validate().unwrap();
        assert_eq!(intent.order_kind, OrderKind::PostOnlyGtd);
        assert_eq!(
            intent.gtd_expiry_ts,
            Some(intent.valid_until + Duration::seconds(60))
        );
        assert_eq!(intent.notional, Decimal::new(90, 2));
        assert_eq!(intent.resolution_source, "chainlink_reference");
        assert_eq!(
            intent.book_hash,
            "sha256:111ddff1675479d2785fafe1d826eb8b28e8d10931132ce39c8985284e859c54"
        );
        assert!(intent.net_edge_lower_bound > Decimal::ZERO);
        assert_eq!(intent.decision_id.len(), 64);
        assert_eq!(
            intent_blob_name("reports/intents", &intent.decision_id).unwrap(),
            format!("reports/intents/{}.json", intent.decision_id)
        );
    }

    #[test]
    fn checkpoint_100_control_switches_future_intents_to_exact_queue_model() {
        let (mut settings, market, fair, reference, book, decision, metadata, now) = fixture();
        let (canonical, model_bytes) = post_100_control(&mut settings, now);
        let model = select_execution_model_from_control(&settings, now, Some(&canonical), |uri| {
            assert_eq!(uri, "azure://test-account/polyedge-models/queue/model.json");
            Ok(model_bytes)
        })
        .unwrap();
        assert_eq!(model.version, "queue-calibration-v1");
        let intent = build_execution_intent_with_model(
            &settings, &market, &fair, &reference, &book, &decision, &metadata, now, &model,
        )
        .unwrap();
        assert_eq!(intent.required_fill_model_version, "queue-calibration-v1");
        assert_eq!(intent.execution_model_blob_uri, model.blob_uri);
        assert_eq!(intent.execution_model_sha256, model.sha256);
    }

    #[test]
    fn fails_closed_for_stale_or_inexact_sources_and_non_positive_edge() {
        let (mut settings, market, fair, mut reference, book, decision, metadata, now) = fixture();
        reference.exact_resolution_source = false;
        assert!(build_execution_intent(
            &settings, &market, &fair, &reference, &book, &decision, &metadata, now
        )
        .unwrap_err()
        .contains("exact market resolution"));

        reference.exact_resolution_source = true;
        settings.strategy.slippage_buffer = Decimal::new(20, 2);
        assert!(build_execution_intent(
            &settings, &market, &fair, &reference, &book, &decision, &metadata, now
        )
        .unwrap_err()
        .contains("net-edge"));
    }

    #[test]
    fn fails_closed_when_notional_exceeds_one_dollar_or_candidate_changes() {
        let (settings, market, fair, reference, book, mut decision, mut metadata, now) = fixture();
        decision.size = Some(Decimal::from(3));
        assert!(build_execution_intent(
            &settings, &market, &fair, &reference, &book, &decision, &metadata, now
        )
        .unwrap_err()
        .contains("one-dollar"));

        decision.size = Some(Decimal::ONE);
        metadata.candidate = FrozenStrategyMode::DynamicSafetyOnly.candidate();
        assert!(build_execution_intent(
            &settings, &market, &fair, &reference, &book, &decision, &metadata, now
        )
        .unwrap_err()
        .contains("frozen dynamic_quote_style"));
    }

    #[test]
    fn derives_venue_feasible_minimum_shares_without_exceeding_one_dollar() {
        let (settings, mut market, fair, reference, book, mut decision, metadata, now) = fixture();
        market.minimum_order_size = Decimal::from(5);
        decision.price = Some(Decimal::new(20, 2));
        decision.size = Some(Decimal::ONE);
        let intent = build_execution_intent(
            &settings, &market, &fair, &reference, &book, &decision, &metadata, now,
        )
        .unwrap();
        assert_eq!(intent.shares, Decimal::from(5));
        assert_eq!(intent.minimum_order_size, Decimal::from(5));
        assert_eq!(intent.notional, Decimal::ONE);

        decision.price = Some(Decimal::new(21, 2));
        assert!(build_execution_intent(
            &settings, &market, &fair, &reference, &book, &decision, &metadata, now
        )
        .unwrap_err()
        .contains("one-dollar"));
    }
}
