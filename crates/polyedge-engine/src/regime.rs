use chrono::{DateTime, Duration, Utc};
use polyedge_config::StrategyConfig;
use polyedge_domain::{DecisionAction, TradeDecision};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RegimeFeatures {
    pub seconds_since_start: Option<f64>,
    pub seconds_to_expiry: Option<f64>,
    pub distance_bps: Option<f64>,
    pub chainlink_return_5s_bps: Option<f64>,
    pub chainlink_return_10s_bps: Option<f64>,
    pub chainlink_return_30s_bps: Option<f64>,
    pub chainlink_return_120s_bps: Option<f64>,
    pub realized_vol_30s_bps: Option<f64>,
    pub realized_vol_120s_bps: Option<f64>,
    pub shock_z: Option<f64>,
    pub q_up: Option<f64>,
    pub q_down: Option<f64>,
    pub sigma: Option<f64>,
    pub up_bid: Option<f64>,
    pub up_ask: Option<f64>,
    pub up_spread_ticks: Option<f64>,
    pub up_top_size: Option<f64>,
    pub down_bid: Option<f64>,
    pub down_ask: Option<f64>,
    pub down_spread_ticks: Option<f64>,
    pub down_top_size: Option<f64>,
    pub book_update_rate_10s: Option<f64>,
    pub reference_age_ms: Option<f64>,
    pub book_age_ms: Option<f64>,
    pub feed_divergence_bps: Option<f64>,
    pub recent_feed_errors: u32,
    pub open_positions: Option<f64>,
    pub open_orders: usize,
    pub recent_fill_count: u32,
    pub recent_cancel_count: u32,
    pub adverse_move_after_fill_bps: Option<f64>,
    pub market_active: bool,
    pub has_start_price: bool,
    pub has_books: bool,
    pub reference_stale: bool,
    pub book_stale: bool,
    pub final_no_trade_seconds: i64,
    pub quality_flags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegimeReferencePoint {
    pub ts: DateTime<Utc>,
    pub price: Decimal,
    pub stale: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RegimeBookSnapshot {
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub bid_size: Option<Decimal>,
    pub ask_size: Option<Decimal>,
    pub local_ts: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegimeFeatureInput {
    pub now: DateTime<Utc>,
    pub market_start_ts: Option<DateTime<Utc>>,
    pub market_end_ts: Option<DateTime<Utc>>,
    pub start_price: Option<Decimal>,
    pub tick_size: Decimal,
    pub reference: Option<RegimeReferencePoint>,
    pub reference_history: Vec<RegimeReferencePoint>,
    pub q_up: Option<Decimal>,
    pub q_down: Option<Decimal>,
    pub sigma: Option<f64>,
    pub up_book: Option<RegimeBookSnapshot>,
    pub down_book: Option<RegimeBookSnapshot>,
    pub book_update_rate_10s: Option<f64>,
    pub feed_divergence_bps: Option<f64>,
    pub recent_feed_errors: u32,
    pub open_positions: Option<f64>,
    pub open_orders: usize,
    pub recent_fill_count: u32,
    pub recent_cancel_count: u32,
    pub adverse_move_after_fill_bps: Option<f64>,
    pub max_reference_age_ms: i64,
    pub max_book_age_ms: i64,
    pub final_no_trade_seconds: i64,
    pub quality_flags: Vec<String>,
}

impl RegimeFeatureInput {
    pub fn build(self) -> RegimeFeatures {
        let reference_age_ms = self
            .reference
            .as_ref()
            .map(|reference| age_ms(self.now, reference.ts));
        let up_age_ms = self
            .up_book
            .as_ref()
            .and_then(|book| book.local_ts.map(|ts| age_ms(self.now, ts)));
        let down_age_ms = self
            .down_book
            .as_ref()
            .and_then(|book| book.local_ts.map(|ts| age_ms(self.now, ts)));
        let book_age_ms = [up_age_ms, down_age_ms]
            .into_iter()
            .flatten()
            .max_by(f64::total_cmp);
        let chainlink_return_5s_bps = reference_return_bps(
            self.reference.as_ref(),
            &self.reference_history,
            self.now,
            5,
        );
        let chainlink_return_10s_bps = reference_return_bps(
            self.reference.as_ref(),
            &self.reference_history,
            self.now,
            10,
        );
        let realized_vol_120s_bps = realized_vol_bps(&self.reference_history, self.now, 120);
        RegimeFeatures {
            seconds_since_start: self.market_start_ts.map(|start| {
                self.now.signed_duration_since(start).num_milliseconds() as f64 / 1_000.0
            }),
            seconds_to_expiry: self
                .market_end_ts
                .map(|end| end.signed_duration_since(self.now).num_milliseconds() as f64 / 1_000.0),
            distance_bps: self.reference.as_ref().zip(self.start_price).and_then(
                |(reference, start)| {
                    (start > Decimal::ZERO)
                        .then(|| {
                            ((reference.price - start) / start * Decimal::from(10_000)).to_f64()
                        })
                        .flatten()
                },
            ),
            chainlink_return_5s_bps,
            chainlink_return_10s_bps,
            chainlink_return_30s_bps: reference_return_bps(
                self.reference.as_ref(),
                &self.reference_history,
                self.now,
                30,
            ),
            chainlink_return_120s_bps: reference_return_bps(
                self.reference.as_ref(),
                &self.reference_history,
                self.now,
                120,
            ),
            realized_vol_30s_bps: realized_vol_bps(&self.reference_history, self.now, 30),
            realized_vol_120s_bps,
            shock_z: chainlink_return_10s_bps
                .zip(realized_vol_120s_bps)
                .and_then(|(ret, vol)| (vol > 0.0).then_some(ret / vol)),
            q_up: self.q_up.and_then(|value| value.to_f64()),
            q_down: self.q_down.and_then(|value| value.to_f64()),
            sigma: self.sigma,
            up_bid: book_price(self.up_book.as_ref(), true),
            up_ask: book_price(self.up_book.as_ref(), false),
            up_spread_ticks: book_spread_ticks(self.up_book.as_ref(), self.tick_size),
            up_top_size: book_top_size(self.up_book.as_ref()),
            down_bid: book_price(self.down_book.as_ref(), true),
            down_ask: book_price(self.down_book.as_ref(), false),
            down_spread_ticks: book_spread_ticks(self.down_book.as_ref(), self.tick_size),
            down_top_size: book_top_size(self.down_book.as_ref()),
            book_update_rate_10s: self.book_update_rate_10s,
            reference_age_ms,
            book_age_ms,
            feed_divergence_bps: self.feed_divergence_bps,
            recent_feed_errors: self.recent_feed_errors,
            open_positions: self.open_positions,
            open_orders: self.open_orders,
            recent_fill_count: self.recent_fill_count,
            recent_cancel_count: self.recent_cancel_count,
            adverse_move_after_fill_bps: self.adverse_move_after_fill_bps,
            market_active: self
                .market_start_ts
                .zip(self.market_end_ts)
                .is_some_and(|(start, end)| start <= self.now && self.now < end),
            has_start_price: self.start_price.is_some(),
            has_books: valid_book(self.up_book.as_ref()) && valid_book(self.down_book.as_ref()),
            reference_stale: self
                .reference
                .as_ref()
                .is_none_or(|reference| reference.stale)
                || reference_age_ms.is_none_or(|age| age > self.max_reference_age_ms as f64),
            book_stale: book_age_ms.is_none_or(|age| age > self.max_book_age_ms as f64),
            final_no_trade_seconds: self.final_no_trade_seconds,
            quality_flags: self.quality_flags,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrozenStrategyMode {
    DynamicSafetyOnly,
    DynamicQuoteStyle,
    FullDeterministicProfile,
}

/// Canonical, explicit semantic identity for the frozen DynamicQuoteStyle
/// candidate. Any classifier threshold, policy transform, default parameter,
/// or evaluator semantic change requires a new canonical document and hash.
pub const DYNAMIC_QUOTE_STYLE_POLICY_CANONICAL_JSON: &str = "{\"base\":{\"adverse_selection_buffer\":\"0.005\",\"final_no_trade_seconds\":30,\"maker_margin\":\"0.015\",\"maker_min_edge\":\"0.01\",\"model_error_buffer\":\"0.01\",\"order_ttl_seconds\":10,\"size_multiplier\":\"1\"},\"candidate\":\"dynamic_quote_style\",\"classifier\":{\"calm_liquid\":{\"both_spread_ticks_lte\":1,\"realized_vol_30s_bps_lte\":5},\"feed_risk\":{\"book_stale\":true,\"feed_divergence_abs_bps_gt\":15,\"recent_feed_errors_gt\":0,\"reference_stale\":true},\"final_window_seconds_lte\":\"feature.final_no_trade_seconds\",\"min_dwell_seconds\":5,\"near_strike\":{\"distance_abs_bps_lte\":2,\"or_distance_abs_bps_lte\":5,\"or_seconds_to_expiry_lte\":180},\"shock\":{\"chainlink_return_10s_abs_bps_gt\":5,\"realized_vol_30s_bps_gt\":20,\"shock_z_abs_gte\":3},\"switch_confirm_seconds\":3,\"wide_or_thin_book\":{\"spread_missing_is_risky\":true,\"spread_ticks_gte\":3,\"top_size_lt\":5,\"top_size_missing_is_risky\":true}},\"evaluator\":{\"expected_edge_adjustment\":\"edge+original_price-transformed_price\",\"implementation\":\"polyedge_engine::regime::evaluate_frozen_strategy\",\"mode\":\"dynamic_quote_style\",\"no_trade_drops_decision\":true,\"semantic_version\":\"frozen-strategy-evaluator-v1\",\"transforms\":[\"quote_style\"]},\"profiles\":{\"calm_liquid\":{\"maker_margin\":\"clamp(max(0.010,base*0.75),0.005,0.080)\",\"quote_style\":\"improve_one_tick\"},\"feed_risk|market_inactive|final_window\":{\"cancel_existing\":true,\"no_trade\":true,\"quote_style\":\"no_quote\",\"size_multiplier\":\"0\"},\"near_strike\":{\"adverse_selection_buffer\":\"clamp(max(base*2,0.015),0,0.080)\",\"maker_margin\":\"clamp(max(base*1.5,0.025),0.005,0.080)\",\"maker_min_edge\":\"clamp(max(base*2,0.020),0.005,0.080)\",\"model_error_buffer\":\"clamp(max(base*2,0.020),0.005,0.080)\",\"order_ttl_seconds\":\"clamp(min(base,3),1,30)\",\"quote_style\":\"fair_minus_margin_only\",\"size_multiplier\":\"0.25\"},\"normal\":{\"quote_style\":\"improve_one_tick\"},\"volatility_shock\":{\"adverse_selection_buffer\":\"clamp(max(base*3,0.020),0,0.080)\",\"maker_margin\":\"clamp(max(base*2,0.030),0.005,0.080)\",\"maker_min_edge\":\"clamp(max(base*2,0.020),0.005,0.080)\",\"model_error_buffer\":\"clamp(max(base*2,0.020),0.005,0.080)\",\"order_ttl_seconds\":\"clamp(min(base,3),1,30)\",\"quote_style\":\"join_best_bid\",\"size_multiplier\":\"0.25\"},\"wide_or_thin_book\":{\"adverse_selection_buffer\":\"clamp(max(base*2,0.010),0,0.080)\",\"maker_margin\":\"clamp(max(base*1.5,0.025),0.005,0.080)\",\"maker_min_edge\":\"clamp(max(base*1.5,0.015),0.005,0.080)\",\"model_error_buffer\":\"clamp(max(base,0.015),0.005,0.080)\",\"order_ttl_seconds\":\"clamp(min(base,5),1,30)\",\"quote_style\":\"fair_minus_margin_only\",\"size_multiplier\":\"0.50\"}},\"quote_semantics\":{\"fair_minus_margin_only\":\"max(price-tick_size,tick_size)\",\"improve_one_tick\":\"unchanged\",\"join_best_bid\":\"min(price,best_bid)\",\"no_quote\":\"size=0\"},\"schema\":\"polyedge.frozen_strategy_policy.v1\",\"version\":\"dynamic_quote_style@2026-06-14\"}";

pub const DYNAMIC_QUOTE_STYLE_POLICY_SHA256: &str =
    "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c";

impl FrozenStrategyMode {
    pub fn from_runtime_mode(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "paper_only" | "dynamic_quote_style" => Some(Self::DynamicQuoteStyle),
            "dynamic_safety_only" => Some(Self::DynamicSafetyOnly),
            "full_deterministic_profile" | "full_deterministic" => {
                Some(Self::FullDeterministicProfile)
            }
            _ => None,
        }
    }

    pub fn candidate(self) -> FrozenCandidateIdentity {
        let (name, version, config_hash) = match self {
            Self::DynamicSafetyOnly => (
                "dynamic_safety_only",
                "dynamic_safety_only@2026-06-14",
                "sha256:dynamic-safety-only-profile-v1",
            ),
            Self::DynamicQuoteStyle => (
                "dynamic_quote_style",
                "dynamic_quote_style@2026-06-14",
                DYNAMIC_QUOTE_STYLE_POLICY_SHA256,
            ),
            Self::FullDeterministicProfile => (
                "full_deterministic_profile",
                "full_deterministic_profile@2026-06-14",
                "sha256:full-deterministic-profile-v1",
            ),
        };
        FrozenCandidateIdentity {
            name: name.to_owned(),
            version: version.to_owned(),
            config_hash: config_hash.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrozenCandidateIdentity {
    pub name: String,
    pub version: String,
    pub config_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyDataQuality {
    pub decision_grade: bool,
    pub reference_stale: bool,
    pub book_stale: bool,
    pub market_active: bool,
    pub has_start_price: bool,
    pub has_books: bool,
    pub flags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyDecisionMetadata {
    pub candidate: FrozenCandidateIdentity,
    pub regime: RegimeLabel,
    #[serde(default, with = "polyedge_domain::decimal_string_opt")]
    pub q: Option<Decimal>,
    #[serde(default, with = "polyedge_domain::decimal_string_opt")]
    pub expected_edge: Option<Decimal>,
    pub data_quality: StrategyDataQuality,
    pub features_summary: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyDecisionEnvelope {
    #[serde(flatten)]
    pub decision: TradeDecision,
    pub strategy_metadata: StrategyDecisionMetadata,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QuoteTransformContext {
    pub best_bid: Option<Decimal>,
    pub q: Option<Decimal>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrozenStrategyEvaluation {
    pub decision: Option<TradeDecision>,
    pub cancel_existing: bool,
    pub metadata: StrategyDecisionMetadata,
    pub adaptive: AdaptiveStrategyResult,
}

pub fn evaluate_frozen_strategy(
    mode: FrozenStrategyMode,
    classifier: &mut RegimeClassifier,
    policy: &RegimePolicy,
    features: &RegimeFeatures,
    now: DateTime<Utc>,
    decision: &TradeDecision,
    context: &QuoteTransformContext,
) -> FrozenStrategyEvaluation {
    let regime = classifier.classify(features, now);
    let adaptive = policy.apply(regime, features);
    let mut transformed = decision.clone();
    let mut keep = !adaptive.effective_params.no_trade;
    if keep && transformed.action == DecisionAction::Place {
        if matches!(
            mode,
            FrozenStrategyMode::DynamicQuoteStyle | FrozenStrategyMode::FullDeterministicProfile
        ) {
            let original_price = transformed.price;
            apply_quote_style(
                &mut transformed,
                adaptive.effective_params.quote_style,
                context.best_bid,
            );
            if let (Some(before), Some(after), Some(edge)) =
                (original_price, transformed.price, transformed.expected_edge)
            {
                transformed.expected_edge = Some(edge + before - after);
            }
        }
        if mode == FrozenStrategyMode::FullDeterministicProfile {
            transformed.size = transformed
                .size
                .map(|size| size * adaptive.effective_params.size_multiplier);
            keep = transformed.size.is_some_and(|size| size > Decimal::ZERO);
            transformed.ttl_ms = Some(
                transformed
                    .ttl_ms
                    .unwrap_or(adaptive.effective_params.order_ttl_seconds * 1_000)
                    .min(adaptive.effective_params.order_ttl_seconds * 1_000),
            );
            keep &= transformed.expected_edge.unwrap_or(Decimal::ZERO)
                >= adaptive.effective_params.maker_min_edge;
        }
    }
    let q = context.q.or_else(|| match transformed.outcome.as_ref() {
        Some(polyedge_domain::Outcome::Up) => features.q_up.and_then(Decimal::from_f64),
        Some(polyedge_domain::Outcome::Down) => features.q_down.and_then(Decimal::from_f64),
        None => None,
    });
    let data_quality = StrategyDataQuality {
        decision_grade: !features.reference_stale
            && !features.book_stale
            && features.market_active
            && features.has_start_price
            && features.has_books
            && features.quality_flags.is_empty(),
        reference_stale: features.reference_stale,
        book_stale: features.book_stale,
        market_active: features.market_active,
        has_start_price: features.has_start_price,
        has_books: features.has_books,
        flags: features.quality_flags.clone(),
    };
    let metadata = StrategyDecisionMetadata {
        candidate: mode.candidate(),
        regime,
        q,
        expected_edge: transformed.expected_edge,
        data_quality,
        features_summary: adaptive.features_summary.clone(),
    };
    FrozenStrategyEvaluation {
        decision: keep.then_some(transformed),
        cancel_existing: adaptive.effective_params.cancel_existing,
        metadata,
        adaptive,
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegimeLabel {
    FeedRisk,
    MarketInactive,
    FinalWindow,
    VolatilityShock,
    NearStrike,
    WideOrThinBook,
    CalmLiquid,
    Normal,
}

impl RegimeLabel {
    pub fn is_safety(self) -> bool {
        matches!(
            self,
            RegimeLabel::FeedRisk | RegimeLabel::MarketInactive | RegimeLabel::FinalWindow
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RegimeLabel::FeedRisk => "feed_risk",
            RegimeLabel::MarketInactive => "market_inactive",
            RegimeLabel::FinalWindow => "final_window",
            RegimeLabel::VolatilityShock => "volatility_shock",
            RegimeLabel::NearStrike => "near_strike",
            RegimeLabel::WideOrThinBook => "wide_or_thin_book",
            RegimeLabel::CalmLiquid => "calm_liquid",
            RegimeLabel::Normal => "normal",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteStyle {
    ImproveOneTick,
    JoinBestBid,
    FairMinusMarginOnly,
    NoQuote,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfiledStrategyConfig {
    #[serde(with = "polyedge_domain::decimal_string")]
    pub maker_margin: Decimal,
    #[serde(with = "polyedge_domain::decimal_string")]
    pub maker_min_edge: Decimal,
    #[serde(with = "polyedge_domain::decimal_string")]
    pub model_error_buffer: Decimal,
    #[serde(with = "polyedge_domain::decimal_string")]
    pub adverse_selection_buffer: Decimal,
    pub order_ttl_seconds: i64,
    #[serde(with = "polyedge_domain::decimal_string")]
    pub size_multiplier: Decimal,
    pub final_no_trade_seconds: i64,
    pub no_trade: bool,
    pub cancel_existing: bool,
    pub quote_style: QuoteStyle,
}

impl ProfiledStrategyConfig {
    pub fn from_base(base: &StrategyConfig) -> Self {
        Self {
            maker_margin: clamp_decimal(base.maker_margin, d005(), d080()),
            maker_min_edge: clamp_decimal(base.maker_min_edge, d005(), d080()),
            model_error_buffer: clamp_decimal(base.model_error_buffer, d005(), d080()),
            adverse_selection_buffer: clamp_decimal(
                base.adverse_selection_buffer,
                Decimal::ZERO,
                d080(),
            ),
            order_ttl_seconds: base.order_ttl_seconds.clamp(1, 30),
            size_multiplier: Decimal::ONE,
            final_no_trade_seconds: base.final_no_trade_seconds.clamp(30, 300),
            no_trade: false,
            cancel_existing: false,
            quote_style: QuoteStyle::ImproveOneTick,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegimeProfile {
    pub label: RegimeLabel,
    pub name: String,
    pub config: ProfiledStrategyConfig,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdaptiveStrategyResult {
    pub regime: RegimeLabel,
    pub profile: RegimeProfile,
    pub features_summary: Value,
    pub original_params: ProfiledStrategyConfig,
    pub effective_params: ProfiledStrategyConfig,
    pub reason: String,
}

#[derive(Clone, Debug)]
pub struct RegimePolicy {
    base: StrategyConfig,
}

impl RegimePolicy {
    pub fn new(base: StrategyConfig) -> Self {
        Self { base }
    }

    pub fn base_profile(&self) -> ProfiledStrategyConfig {
        ProfiledStrategyConfig::from_base(&self.base)
    }

    pub fn profile_for(&self, label: RegimeLabel) -> RegimeProfile {
        let mut config = ProfiledStrategyConfig::from_base(&self.base);
        let reason = match label {
            RegimeLabel::FeedRisk => {
                apply_no_trade(&mut config);
                "feed risk safety override".to_owned()
            }
            RegimeLabel::MarketInactive => {
                apply_no_trade(&mut config);
                "market inactive safety override".to_owned()
            }
            RegimeLabel::FinalWindow => {
                apply_no_trade(&mut config);
                "inside final no-trade window".to_owned()
            }
            RegimeLabel::VolatilityShock => {
                config.maker_margin = clamp_decimal(
                    max_decimal(config.maker_margin * Decimal::from(2), d030()),
                    d005(),
                    d080(),
                );
                config.maker_min_edge = clamp_decimal(
                    max_decimal(config.maker_min_edge * Decimal::from(2), d020()),
                    d005(),
                    d080(),
                );
                config.model_error_buffer = clamp_decimal(
                    max_decimal(config.model_error_buffer * Decimal::from(2), d020()),
                    d005(),
                    d080(),
                );
                config.adverse_selection_buffer = clamp_decimal(
                    max_decimal(config.adverse_selection_buffer * Decimal::from(3), d020()),
                    Decimal::ZERO,
                    d080(),
                );
                config.order_ttl_seconds = config.order_ttl_seconds.min(3).clamp(1, 30);
                config.size_multiplier = Decimal::new(25, 2);
                config.quote_style = QuoteStyle::JoinBestBid;
                "volatility shock conservative profile".to_owned()
            }
            RegimeLabel::NearStrike => {
                config.maker_margin = clamp_decimal(
                    max_decimal(config.maker_margin * Decimal::new(15, 1), d025()),
                    d005(),
                    d080(),
                );
                config.maker_min_edge = clamp_decimal(
                    max_decimal(config.maker_min_edge * Decimal::from(2), d020()),
                    d005(),
                    d080(),
                );
                config.model_error_buffer = clamp_decimal(
                    max_decimal(config.model_error_buffer * Decimal::from(2), d020()),
                    d005(),
                    d080(),
                );
                config.adverse_selection_buffer = clamp_decimal(
                    max_decimal(config.adverse_selection_buffer * Decimal::from(2), d015()),
                    Decimal::ZERO,
                    d080(),
                );
                config.order_ttl_seconds = config.order_ttl_seconds.min(3).clamp(1, 30);
                config.size_multiplier = Decimal::new(25, 2);
                config.quote_style = QuoteStyle::FairMinusMarginOnly;
                "near-strike conservative profile".to_owned()
            }
            RegimeLabel::WideOrThinBook => {
                config.maker_margin = clamp_decimal(
                    max_decimal(config.maker_margin * Decimal::new(15, 1), d025()),
                    d005(),
                    d080(),
                );
                config.maker_min_edge = clamp_decimal(
                    max_decimal(config.maker_min_edge * Decimal::new(15, 1), d015()),
                    d005(),
                    d080(),
                );
                config.model_error_buffer = clamp_decimal(
                    max_decimal(config.model_error_buffer, d015()),
                    d005(),
                    d080(),
                );
                config.adverse_selection_buffer = clamp_decimal(
                    max_decimal(config.adverse_selection_buffer * Decimal::from(2), d010()),
                    Decimal::ZERO,
                    d080(),
                );
                config.order_ttl_seconds = config.order_ttl_seconds.min(5).clamp(1, 30);
                config.size_multiplier = Decimal::new(50, 2);
                config.quote_style = QuoteStyle::FairMinusMarginOnly;
                "wide or thin book conservative profile".to_owned()
            }
            RegimeLabel::CalmLiquid => {
                config.maker_margin = clamp_decimal(
                    max_decimal(d010(), config.maker_margin * Decimal::new(75, 2)),
                    d005(),
                    d080(),
                );
                config.quote_style = QuoteStyle::ImproveOneTick;
                "calm liquid profile".to_owned()
            }
            RegimeLabel::Normal => {
                config.quote_style = QuoteStyle::ImproveOneTick;
                "static baseline profile".to_owned()
            }
        };
        config.size_multiplier = clamp_decimal(config.size_multiplier, Decimal::ZERO, Decimal::ONE);
        RegimeProfile {
            label,
            name: label.as_str().to_owned(),
            config,
            reason,
        }
    }

    pub fn apply(&self, label: RegimeLabel, features: &RegimeFeatures) -> AdaptiveStrategyResult {
        let original = self.base_profile();
        let profile = self.profile_for(label);
        AdaptiveStrategyResult {
            regime: label,
            reason: profile.reason.clone(),
            features_summary: features.summary(),
            original_params: original,
            effective_params: profile.config.clone(),
            profile,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegimeClassifierSnapshot {
    pub switch_confirm_seconds: i64,
    pub min_dwell_seconds: i64,
    pub current: Option<RegimeLabel>,
    pub candidate: Option<(RegimeLabel, DateTime<Utc>)>,
    pub last_switch: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct RegimeClassifier {
    switch_confirm: Duration,
    min_dwell: Duration,
    current: Option<RegimeLabel>,
    candidate: Option<(RegimeLabel, DateTime<Utc>)>,
    last_switch: Option<DateTime<Utc>>,
}

impl Default for RegimeClassifier {
    fn default() -> Self {
        Self::new(3, 5)
    }
}

impl RegimeClassifier {
    pub fn new(switch_confirm_seconds: i64, min_dwell_seconds: i64) -> Self {
        Self {
            switch_confirm: Duration::seconds(switch_confirm_seconds.max(0)),
            min_dwell: Duration::seconds(min_dwell_seconds.max(0)),
            current: None,
            candidate: None,
            last_switch: None,
        }
    }

    pub fn snapshot(&self) -> RegimeClassifierSnapshot {
        RegimeClassifierSnapshot {
            switch_confirm_seconds: self.switch_confirm.num_seconds(),
            min_dwell_seconds: self.min_dwell.num_seconds(),
            current: self.current,
            candidate: self.candidate,
            last_switch: self.last_switch,
        }
    }

    pub fn from_snapshot(snapshot: RegimeClassifierSnapshot) -> Self {
        Self {
            switch_confirm: Duration::seconds(snapshot.switch_confirm_seconds.max(0)),
            min_dwell: Duration::seconds(snapshot.min_dwell_seconds.max(0)),
            current: snapshot.current,
            candidate: snapshot.candidate,
            last_switch: snapshot.last_switch,
        }
    }

    pub fn classify_instant(features: &RegimeFeatures) -> RegimeLabel {
        if features.reference_stale
            || features.book_stale
            || features
                .feed_divergence_bps
                .is_some_and(|value| value.abs() > 15.0)
            || features.recent_feed_errors > 0
        {
            return RegimeLabel::FeedRisk;
        }
        if !features.market_active || !features.has_start_price || !features.has_books {
            return RegimeLabel::MarketInactive;
        }
        if features
            .seconds_to_expiry
            .is_some_and(|seconds| seconds <= features.final_no_trade_seconds as f64)
        {
            return RegimeLabel::FinalWindow;
        }
        if features
            .chainlink_return_10s_bps
            .is_some_and(|value| value.abs() > 5.0)
            || features.shock_z.is_some_and(|value| value.abs() >= 3.0)
            || features
                .realized_vol_30s_bps
                .is_some_and(|value| value > 20.0)
        {
            return RegimeLabel::VolatilityShock;
        }
        if features.distance_bps.is_some_and(|value| {
            value.abs() <= 2.0
                || (value.abs() <= 5.0
                    && features
                        .seconds_to_expiry
                        .is_some_and(|seconds| seconds <= 180.0))
        }) {
            return RegimeLabel::NearStrike;
        }
        if spread_or_size_risky(features.up_spread_ticks, features.up_top_size)
            || spread_or_size_risky(features.down_spread_ticks, features.down_top_size)
        {
            return RegimeLabel::WideOrThinBook;
        }
        if features.up_spread_ticks.is_some_and(|value| value <= 1.0)
            && features.down_spread_ticks.is_some_and(|value| value <= 1.0)
            && features.realized_vol_30s_bps.unwrap_or(0.0) <= 5.0
        {
            return RegimeLabel::CalmLiquid;
        }
        RegimeLabel::Normal
    }

    pub fn classify(&mut self, features: &RegimeFeatures, now: DateTime<Utc>) -> RegimeLabel {
        let instant = Self::classify_instant(features);
        if instant.is_safety() || self.current.is_none() {
            self.current = Some(instant);
            self.candidate = None;
            self.last_switch = Some(now);
            return instant;
        }
        if self.current == Some(instant) {
            self.candidate = None;
            return instant;
        }
        let dwell_ok = self
            .last_switch
            .is_none_or(|last| now.signed_duration_since(last) >= self.min_dwell);
        let confirm_ok = match self.candidate {
            Some((candidate, since)) if candidate == instant => {
                now.signed_duration_since(since) >= self.switch_confirm
            }
            _ => {
                self.candidate = Some((instant, now));
                false
            }
        };
        if dwell_ok && confirm_ok {
            self.current = Some(instant);
            self.candidate = None;
            self.last_switch = Some(now);
            instant
        } else {
            self.current.unwrap_or(instant)
        }
    }
}

impl RegimeFeatures {
    pub fn summary(&self) -> Value {
        json!({
            "seconds_to_expiry": self.seconds_to_expiry,
            "distance_bps": self.distance_bps,
            "chainlink_return_10s_bps": self.chainlink_return_10s_bps,
            "realized_vol_30s_bps": self.realized_vol_30s_bps,
            "shock_z": self.shock_z,
            "q_up": self.q_up,
            "q_down": self.q_down,
            "sigma": self.sigma,
            "up_spread_ticks": self.up_spread_ticks,
            "down_spread_ticks": self.down_spread_ticks,
            "reference_age_ms": self.reference_age_ms,
            "book_age_ms": self.book_age_ms,
            "feed_divergence_bps": self.feed_divergence_bps,
            "recent_feed_errors": self.recent_feed_errors,
            "open_orders": self.open_orders,
            "market_active": self.market_active,
            "has_start_price": self.has_start_price,
            "has_books": self.has_books,
            "reference_stale": self.reference_stale,
            "book_stale": self.book_stale,
            "quality_flags": self.quality_flags
        })
    }
}

fn age_ms(now: DateTime<Utc>, then: DateTime<Utc>) -> f64 {
    now.signed_duration_since(then)
        .num_microseconds()
        .map_or(0.0, |micros| micros.max(0) as f64 / 1_000.0)
}

fn reference_return_bps(
    current: Option<&RegimeReferencePoint>,
    history: &[RegimeReferencePoint],
    now: DateTime<Utc>,
    seconds: i64,
) -> Option<f64> {
    let current = current?;
    let target = now - Duration::seconds(seconds);
    let prior = history.iter().rev().find(|point| point.ts <= target)?;
    if prior.price <= Decimal::ZERO {
        return None;
    }
    ((current.price - prior.price) / prior.price * Decimal::from(10_000)).to_f64()
}

fn realized_vol_bps(
    history: &[RegimeReferencePoint],
    now: DateTime<Utc>,
    seconds: i64,
) -> Option<f64> {
    let lower = now - Duration::seconds(seconds);
    let points = history
        .iter()
        .filter(|point| point.ts >= lower && point.ts <= now && point.price > Decimal::ZERO)
        .collect::<Vec<_>>();
    if points.len() < 3 {
        return None;
    }
    let returns = points
        .windows(2)
        .filter_map(|pair| {
            let previous = pair[0].price.to_f64()?;
            let next = pair[1].price.to_f64()?;
            (previous > 0.0).then_some((next / previous).ln() * 10_000.0)
        })
        .collect::<Vec<_>>();
    sample_std(&returns)
}

fn sample_std(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    Some(variance.max(0.0).sqrt())
}

fn book_price(book: Option<&RegimeBookSnapshot>, bid: bool) -> Option<f64> {
    if bid {
        book?.bid?.to_f64()
    } else {
        book?.ask?.to_f64()
    }
}

fn book_spread_ticks(book: Option<&RegimeBookSnapshot>, tick_size: Decimal) -> Option<f64> {
    let book = book?;
    let (bid, ask) = (book.bid?, book.ask?);
    if tick_size <= Decimal::ZERO || bid >= ask {
        return None;
    }
    ((ask - bid) / tick_size).to_f64()
}

fn book_top_size(book: Option<&RegimeBookSnapshot>) -> Option<f64> {
    let book = book?;
    book.bid_size?.min(book.ask_size?).to_f64()
}

fn valid_book(book: Option<&RegimeBookSnapshot>) -> bool {
    book.and_then(|book| book.bid.zip(book.ask))
        .is_some_and(|(bid, ask)| bid < ask)
}

fn apply_quote_style(decision: &mut TradeDecision, style: QuoteStyle, best_bid: Option<Decimal>) {
    let Some(price) = decision.price else {
        return;
    };
    match style {
        QuoteStyle::ImproveOneTick => {}
        QuoteStyle::JoinBestBid => {
            if let Some(best_bid) = best_bid {
                decision.price = Some(price.min(best_bid));
            }
        }
        QuoteStyle::FairMinusMarginOnly => {
            if let Some(tick_size) = decision.tick_size {
                decision.price = Some((price - tick_size).max(tick_size));
            }
        }
        QuoteStyle::NoQuote => decision.size = Some(Decimal::ZERO),
    }
}

fn apply_no_trade(config: &mut ProfiledStrategyConfig) {
    config.no_trade = true;
    config.cancel_existing = true;
    config.size_multiplier = Decimal::ZERO;
    config.quote_style = QuoteStyle::NoQuote;
}

fn spread_or_size_risky(spread_ticks: Option<f64>, top_size: Option<f64>) -> bool {
    spread_ticks.is_none()
        || spread_ticks.is_some_and(|value| value >= 3.0)
        || top_size.is_none()
        || top_size.is_some_and(|value| value < 5.0)
}

fn clamp_decimal(value: Decimal, lower: Decimal, upper: Decimal) -> Decimal {
    value.max(lower).min(upper)
}

fn max_decimal(left: Decimal, right: Decimal) -> Decimal {
    if left > right {
        left
    } else {
        right
    }
}

fn d005() -> Decimal {
    Decimal::new(5, 3)
}

fn d010() -> Decimal {
    Decimal::new(10, 3)
}

fn d015() -> Decimal {
    Decimal::new(15, 3)
}

fn d020() -> Decimal {
    Decimal::new(20, 3)
}

fn d025() -> Decimal {
    Decimal::new(25, 3)
}

fn d030() -> Decimal {
    Decimal::new(30, 3)
}

fn d080() -> Decimal {
    Decimal::new(80, 3)
}
