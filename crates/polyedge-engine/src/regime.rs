use chrono::{DateTime, Duration, Utc};
use polyedge_config::StrategyConfig;
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
