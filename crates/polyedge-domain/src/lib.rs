use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;

pub mod decimal_string {
    use rust_decimal::Decimal;
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DecimalVisitor)
    }

    struct DecimalVisitor;

    impl<'de> Visitor<'de> for DecimalVisitor {
        type Value = Decimal;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a decimal string or number")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Decimal::from_str_exact(value).map_err(E::custom)
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_str(&value)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Decimal::from(value))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Decimal::from(value))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Decimal::from_f64_retain(value).ok_or_else(|| E::custom("invalid decimal float"))
        }
    }
}

pub mod decimal_string_opt {
    use rust_decimal::Decimal;
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<Decimal>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(decimal) => serializer.serialize_some(&decimal.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<serde_json::Value>::deserialize(deserializer)?;
        match value {
            Some(serde_json::Value::String(text)) => Decimal::from_str_exact(&text)
                .map(Some)
                .map_err(D::Error::custom),
            Some(serde_json::Value::Number(number)) => Decimal::from_str_exact(&number.to_string())
                .map(Some)
                .map_err(D::Error::custom),
            Some(serde_json::Value::Null) | None => Ok(None),
            Some(other) => Err(D::Error::custom(format!("invalid decimal value: {other}"))),
        }
    }
}

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(MarketId);
string_id!(ConditionId);
string_id!(TokenId);
string_id!(OrderId);

pub type Probability = Decimal;
pub type PriceTicks = Decimal;
pub type ShareSize = Decimal;
pub type UsdPrice = Decimal;

fn default_asset() -> String {
    "BTC".to_owned()
}

fn default_horizon() -> String {
    "15m".to_owned()
}

fn default_resolution_source() -> String {
    "chainlink_reference".to_owned()
}

fn default_tick_size() -> Decimal {
    Decimal::new(1, 2)
}

fn default_minimum_order_size() -> Decimal {
    Decimal::from(5)
}

fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Up,
    Down,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    PostOnlyGtc,
    PostOnlyGtd,
    Fak,
    Fok,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAction {
    Place,
    CancelAll,
    Hold,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketStatus {
    Tradeable,
    #[default]
    ObserveOnly,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BookLevel {
    #[serde(with = "decimal_string")]
    pub price: Decimal,
    #[serde(with = "decimal_string")]
    pub size: Decimal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketSpec {
    #[serde(default = "default_asset")]
    pub asset: String,
    #[serde(default = "default_horizon")]
    pub horizon: String,
    #[serde(default)]
    pub event_id: Option<String>,
    #[serde(default)]
    pub event_slug: Option<String>,
    pub market_id: MarketId,
    #[serde(default)]
    pub market_slug: Option<String>,
    pub condition_id: ConditionId,
    pub question: String,
    #[serde(default)]
    pub description: Option<String>,
    pub up_token_id: TokenId,
    pub down_token_id: TokenId,
    pub start_ts: DateTime<Utc>,
    pub end_ts: DateTime<Utc>,
    #[serde(default, with = "decimal_string_opt")]
    pub start_price: Option<Decimal>,
    #[serde(default = "default_resolution_source")]
    pub resolution_source: String,
    #[serde(default = "default_tick_size", with = "decimal_string")]
    pub tick_size: Decimal,
    #[serde(default = "default_minimum_order_size", with = "decimal_string")]
    pub minimum_order_size: Decimal,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default = "default_true")]
    pub fees_enabled: bool,
    #[serde(default = "default_true")]
    pub accepting_orders: bool,
    #[serde(default)]
    pub status: MarketStatus,
    #[serde(default)]
    pub raw: BTreeMap<String, Value>,
}

impl MarketSpec {
    pub fn is_tradeable(&self) -> bool {
        self.status == MarketStatus::Tradeable && self.start_price.is_some()
    }

    pub fn with_start_price(mut self, price: Decimal) -> Self {
        self.start_price = Some(price);
        self.status = if self.accepting_orders {
            MarketStatus::Tradeable
        } else {
            MarketStatus::ObserveOnly
        };
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BookState {
    pub token_id: TokenId,
    #[serde(default)]
    pub bids: Vec<BookLevel>,
    #[serde(default)]
    pub asks: Vec<BookLevel>,
    #[serde(default, with = "decimal_string_opt")]
    pub last_trade_price: Option<Decimal>,
    #[serde(default)]
    pub exchange_ts: Option<DateTime<Utc>>,
    #[serde(default = "utc_now")]
    pub local_ts: DateTime<Utc>,
    #[serde(default)]
    pub book_hash: Option<String>,
}

impl BookState {
    pub fn best_bid(&self) -> Option<&BookLevel> {
        self.bids
            .iter()
            .max_by(|left, right| left.price.cmp(&right.price))
    }

    pub fn best_ask(&self) -> Option<&BookLevel> {
        self.asks
            .iter()
            .min_by(|left, right| left.price.cmp(&right.price))
    }

    pub fn age_ms(&self, now: DateTime<Utc>) -> f64 {
        let age = now.signed_duration_since(self.local_ts);
        age.num_microseconds()
            .map_or(0.0, |micros| (micros.max(0) as f64) / 1000.0)
    }

    pub fn is_stale(&self, max_age_ms: i64, now: DateTime<Utc>) -> bool {
        self.age_ms(now) > max_age_ms as f64
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReferencePrice {
    pub source: String,
    #[serde(with = "decimal_string")]
    pub price: Decimal,
    pub source_ts: DateTime<Utc>,
    #[serde(default = "utc_now")]
    pub local_ts: DateTime<Utc>,
    #[serde(default)]
    pub latency_ms: f64,
    #[serde(default)]
    pub stale: bool,
    #[serde(default)]
    pub exact_resolution_source: bool,
    #[serde(default)]
    pub quality_flags: Vec<String>,
}

impl ReferencePrice {
    pub fn age_ms(&self, now: DateTime<Utc>) -> f64 {
        let age = now.signed_duration_since(self.local_ts);
        age.num_microseconds()
            .map_or(0.0, |micros| (micros.max(0) as f64) / 1000.0)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FairValue {
    pub market_id: MarketId,
    #[serde(with = "decimal_string")]
    pub q_up: Decimal,
    #[serde(with = "decimal_string")]
    pub q_down: Decimal,
    pub sigma: f64,
    pub drift_mu: f64,
    #[serde(with = "decimal_string")]
    pub model_error: Decimal,
    #[serde(default = "utc_now")]
    pub computed_ts: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeDecision {
    pub action: DecisionAction,
    pub market_id: MarketId,
    #[serde(default)]
    pub condition_id: Option<ConditionId>,
    #[serde(default)]
    pub token_id: Option<TokenId>,
    #[serde(default)]
    pub outcome: Option<Outcome>,
    #[serde(default)]
    pub side: Option<Side>,
    #[serde(default, with = "decimal_string_opt")]
    pub price: Option<Decimal>,
    #[serde(default, with = "decimal_string_opt")]
    pub size: Option<Decimal>,
    #[serde(default, with = "decimal_string_opt")]
    pub quote_amount: Option<Decimal>,
    #[serde(default)]
    pub order_kind: Option<OrderKind>,
    pub reason: String,
    #[serde(default)]
    pub ttl_ms: Option<i64>,
    #[serde(default, with = "decimal_string_opt")]
    pub expected_edge: Option<Decimal>,
    #[serde(default)]
    pub post_only: bool,
    #[serde(default, with = "decimal_string_opt")]
    pub tick_size: Option<Decimal>,
    #[serde(default)]
    pub neg_risk: bool,
}

pub const EXECUTION_INTENT_V1_SCHEMA: &str = "polyedge.execution_intent.v1";
pub const VENUE_EXECUTION_REPORT_V1_SCHEMA: &str = "polyedge.venue_execution_report.v1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionIntentV1 {
    pub schema: String,
    pub decision_id: String,
    pub candidate_name: String,
    pub candidate_version: String,
    pub candidate_config_hash: String,
    pub market_id: MarketId,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub outcome: Outcome,
    pub side: Side,
    #[serde(with = "decimal_string")]
    pub price: Decimal,
    #[serde(with = "decimal_string")]
    pub shares: Decimal,
    #[serde(with = "decimal_string")]
    pub notional: Decimal,
    #[serde(with = "decimal_string")]
    pub minimum_order_size: Decimal,
    pub post_only: bool,
    pub order_kind: OrderKind,
    pub ttl_ms: i64,
    pub decision_ts: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    #[serde(default)]
    pub gtd_expiry_ts: Option<DateTime<Utc>>,
    pub book_hash: String,
    #[serde(with = "decimal_string")]
    pub q: Decimal,
    #[serde(with = "decimal_string")]
    pub gross_edge: Decimal,
    #[serde(with = "decimal_string")]
    pub fee_allowance: Decimal,
    #[serde(with = "decimal_string")]
    pub slippage_allowance: Decimal,
    #[serde(with = "decimal_string")]
    pub toxicity_allowance: Decimal,
    #[serde(with = "decimal_string")]
    pub net_edge_lower_bound: Decimal,
    pub regime: String,
    pub features_digest: String,
    pub reference_age_ms: i64,
    pub book_age_ms: i64,
    pub exact_resolution_source: bool,
    pub resolution_source: String,
    pub required_fill_model_version: String,
    pub execution_model_blob_uri: String,
    pub execution_model_sha256: String,
}

impl ExecutionIntentV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != EXECUTION_INTENT_V1_SCHEMA {
            return Err("unsupported execution intent schema".to_owned());
        }
        if [
            self.decision_id.as_str(),
            self.candidate_name.as_str(),
            self.candidate_version.as_str(),
            self.candidate_config_hash.as_str(),
            self.market_id.as_ref(),
            self.condition_id.as_ref(),
            self.token_id.as_ref(),
            self.book_hash.as_str(),
            self.regime.as_str(),
            self.features_digest.as_str(),
            self.resolution_source.as_str(),
            self.required_fill_model_version.as_str(),
            self.execution_model_blob_uri.as_str(),
            self.execution_model_sha256.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err(
                "execution intent identity and evidence fields must be populated".to_owned(),
            );
        }
        let model_digest = self
            .execution_model_sha256
            .strip_prefix("sha256:")
            .unwrap_or(&self.execution_model_sha256);
        if model_digest.len() != 64
            || !model_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("execution intent model SHA-256 must be lowercase 64-hex".to_owned());
        }
        let config_digest = self
            .candidate_config_hash
            .strip_prefix("sha256:")
            .unwrap_or(&self.candidate_config_hash);
        if config_digest.len() != 64
            || !config_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(
                "execution intent candidate config hash must be lowercase 64-hex".to_owned(),
            );
        }
        if self.side != Side::Buy {
            return Err("execution intent v1 permits BUY only".to_owned());
        }
        if !self.post_only
            || !matches!(
                self.order_kind,
                OrderKind::PostOnlyGtc | OrderKind::PostOnlyGtd
            )
        {
            return Err("execution intent v1 requires a post-only order".to_owned());
        }
        if self.price <= Decimal::ZERO
            || self.price >= Decimal::ONE
            || self.shares <= Decimal::ZERO
            || self.notional != self.price * self.shares
            || self.notional > Decimal::ONE
            || self.minimum_order_size <= Decimal::ZERO
            || self.shares < self.minimum_order_size
        {
            return Err("invalid price, shares, or notional".to_owned());
        }
        if self.q < Decimal::ZERO || self.q > Decimal::ONE {
            return Err("q must be between zero and one".to_owned());
        }
        if self.ttl_ms <= 0 || self.valid_until <= self.decision_ts {
            return Err("intent TTL and validity window must be positive".to_owned());
        }
        if self.valid_until != self.decision_ts + chrono::Duration::milliseconds(self.ttl_ms) {
            return Err("valid_until must equal decision_ts plus ttl_ms".to_owned());
        }
        if self.order_kind == OrderKind::PostOnlyGtd
            && self.gtd_expiry_ts != Some(self.valid_until + chrono::Duration::seconds(60))
        {
            return Err(
                "post-only GTD venue expiry must equal active valid_until plus 60 seconds"
                    .to_owned(),
            );
        }
        if [
            self.fee_allowance,
            self.slippage_allowance,
            self.toxicity_allowance,
        ]
        .iter()
        .any(|value| *value < Decimal::ZERO)
        {
            return Err("cost and toxicity allowances cannot be negative".to_owned());
        }
        if self.net_edge_lower_bound
            != self.gross_edge
                - self.fee_allowance
                - self.slippage_allowance
                - self.toxicity_allowance
        {
            return Err("net edge lower bound does not reconcile".to_owned());
        }
        if self.net_edge_lower_bound <= Decimal::ZERO {
            return Err("net edge lower bound must be positive".to_owned());
        }
        if self.reference_age_ms < 0 || self.book_age_ms < 0 {
            return Err("source ages cannot be negative".to_owned());
        }
        if !self.exact_resolution_source {
            return Err("executable intent requires an exact resolution source".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VenueFillV1 {
    pub trade_id: String,
    #[serde(with = "decimal_string")]
    pub shares: Decimal,
    #[serde(with = "decimal_string")]
    pub price: Decimal,
    pub venue_ts: DateTime<Utc>,
    #[serde(default)]
    pub user_channel_ts: Option<DateTime<Utc>>,
    #[serde(default, with = "decimal_string_opt")]
    pub markout_1s: Option<Decimal>,
    #[serde(default, with = "decimal_string_opt")]
    pub markout_5s: Option<Decimal>,
    #[serde(default, with = "decimal_string_opt")]
    pub markout_30s: Option<Decimal>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VenueSourceAgreementV1 {
    pub rest_status: String,
    pub user_channel_status: String,
    pub order_id_agrees: bool,
    pub filled_shares_agree: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRiskSnapshotV1 {
    pub ts: DateTime<Utc>,
    #[serde(with = "decimal_string")]
    pub equity: Decimal,
    #[serde(with = "decimal_string")]
    pub available_balance: Decimal,
    #[serde(with = "decimal_string")]
    pub locked_notional: Decimal,
    #[serde(with = "decimal_string")]
    pub realized_pnl: Decimal,
    #[serde(with = "decimal_string")]
    pub unrealized_pnl: Decimal,
    #[serde(with = "decimal_string")]
    pub campaign_drawdown: Decimal,
    pub open_orders: usize,
    #[serde(with = "decimal_string")]
    pub unresolved_notional: Decimal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VenueExecutionReportV1 {
    pub schema: String,
    pub decision_id: String,
    #[serde(default)]
    pub order_id: Option<OrderId>,
    pub submitted_ts: DateTime<Utc>,
    #[serde(default)]
    pub acknowledged_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    pub cancel_requested_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    pub cancel_acknowledged_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    pub user_channel_acknowledged_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    pub fills: Vec<VenueFillV1>,
    pub source_agreement: VenueSourceAgreementV1,
    pub zero_open_orders_confirmed: bool,
    pub reconciliation_status: String,
    pub risk_before: ExecutionRiskSnapshotV1,
    pub risk_after: ExecutionRiskSnapshotV1,
}

impl VenueExecutionReportV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != VENUE_EXECUTION_REPORT_V1_SCHEMA {
            return Err("unsupported venue execution report schema".to_owned());
        }
        if self.decision_id.trim().is_empty() || self.reconciliation_status.trim().is_empty() {
            return Err("report identity and reconciliation status are required".to_owned());
        }
        if self
            .acknowledged_ts
            .is_some_and(|timestamp| timestamp < self.submitted_ts)
            || self.cancel_requested_ts.is_some_and(|timestamp| {
                self.acknowledged_ts
                    .is_some_and(|acknowledged| timestamp < acknowledged)
            })
            || self.cancel_acknowledged_ts.is_some_and(|timestamp| {
                self.cancel_requested_ts
                    .is_none_or(|requested| timestamp < requested)
            })
        {
            return Err("venue lifecycle timestamps are out of order".to_owned());
        }
        let mut trade_ids = std::collections::BTreeSet::new();
        for fill in &self.fills {
            if fill.trade_id.trim().is_empty()
                || !trade_ids.insert(fill.trade_id.as_str())
                || fill.shares <= Decimal::ZERO
                || fill.price <= Decimal::ZERO
                || fill.price >= Decimal::ONE
                || fill.venue_ts < self.submitted_ts
            {
                return Err("invalid or duplicate venue fill".to_owned());
            }
        }
        if self.source_agreement.order_id_agrees && self.order_id.is_none() {
            return Err("order-id agreement requires an order id".to_owned());
        }
        if self.risk_before.ts > self.risk_after.ts
            || self.risk_before.locked_notional < Decimal::ZERO
            || self.risk_after.locked_notional < Decimal::ZERO
            || self.risk_before.unresolved_notional < Decimal::ZERO
            || self.risk_after.unresolved_notional < Decimal::ZERO
        {
            return Err("invalid risk snapshots".to_owned());
        }
        if self.zero_open_orders_confirmed && self.risk_after.open_orders != 0 {
            return Err("zero-open-orders confirmation contradicts risk snapshot".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReport {
    #[serde(default)]
    pub order_id: Option<OrderId>,
    pub market_id: MarketId,
    #[serde(default)]
    pub token_id: Option<TokenId>,
    pub status: String,
    #[serde(default, with = "decimal_string")]
    pub filled_size: Decimal,
    #[serde(default, with = "decimal_string_opt")]
    pub avg_price: Option<Decimal>,
    #[serde(default, with = "decimal_string")]
    pub fee: Decimal,
    #[serde(default = "utc_now")]
    pub local_ts: DateTime<Utc>,
    #[serde(default)]
    pub raw: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RiskAssessment {
    pub allowed: bool,
    #[serde(default)]
    pub reasons: Vec<String>,
}

impl RiskAssessment {
    pub fn allow() -> Self {
        Self {
            allowed: true,
            reasons: Vec::new(),
        }
    }

    pub fn deny(reasons: Vec<String>) -> Self {
        Self {
            allowed: false,
            reasons: reasons
                .into_iter()
                .filter(|reason| !reason.is_empty())
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub ts: DateTime<Utc>,
    #[serde(default)]
    pub data: Value,
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn risk_snapshot(ts: DateTime<Utc>, open_orders: usize) -> ExecutionRiskSnapshotV1 {
        ExecutionRiskSnapshotV1 {
            ts,
            equity: Decimal::from(5),
            available_balance: Decimal::from(4),
            locked_notional: Decimal::ONE,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            campaign_drawdown: Decimal::ZERO,
            open_orders,
            unresolved_notional: Decimal::ONE,
        }
    }

    #[test]
    fn execution_intent_v1_validates_reconciled_buy_contract() {
        let decision_ts = Utc::now();
        let intent = ExecutionIntentV1 {
            schema: EXECUTION_INTENT_V1_SCHEMA.to_owned(),
            decision_id: "decision-1".to_owned(),
            candidate_name: "dynamic_quote_style".to_owned(),
            candidate_version: "dynamic_quote_style@2026-06-14".to_owned(),
            candidate_config_hash:
                "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c".to_owned(),
            market_id: MarketId::new("market-1"),
            condition_id: ConditionId::new("condition-1"),
            token_id: TokenId::new("token-1"),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(40, 2),
            shares: Decimal::from(2),
            notional: Decimal::new(80, 2),
            minimum_order_size: Decimal::ONE,
            post_only: true,
            order_kind: OrderKind::PostOnlyGtd,
            ttl_ms: 5_000,
            decision_ts,
            valid_until: decision_ts + Duration::seconds(5),
            gtd_expiry_ts: Some(decision_ts + Duration::seconds(65)),
            book_hash: "sha256:book".to_owned(),
            q: Decimal::new(46, 2),
            gross_edge: Decimal::new(6, 2),
            fee_allowance: Decimal::ZERO,
            slippage_allowance: Decimal::new(1, 2),
            toxicity_allowance: Decimal::new(2, 2),
            net_edge_lower_bound: Decimal::new(3, 2),
            regime: "normal".to_owned(),
            features_digest: "sha256:features".to_owned(),
            reference_age_ms: 100,
            book_age_ms: 80,
            exact_resolution_source: true,
            resolution_source: "chainlink_reference".to_owned(),
            required_fill_model_version: "queue-calibration-v1".to_owned(),
            execution_model_blob_uri:
                "azure://storage/bot-events/reports/research/venue-probe/effective_queue_model.json"
                    .to_owned(),
            execution_model_sha256: format!("sha256:{}", "7".repeat(64)),
        };
        intent.validate().unwrap();
        let serialized = serde_json::to_value(&intent).unwrap();
        assert_eq!(serialized["price"], "0.40");
        assert_eq!(serialized["schema"], EXECUTION_INTENT_V1_SCHEMA);

        let mut invalid = intent.clone();
        invalid.exact_resolution_source = false;
        assert!(invalid.validate().is_err());
        invalid.exact_resolution_source = true;
        invalid.notional = Decimal::new(101, 2);
        invalid.shares = invalid.notional / invalid.price;
        assert!(invalid.validate().is_err());
        invalid.notional = Decimal::new(80, 2);
        invalid.shares = Decimal::from(2);
        invalid.net_edge_lower_bound = Decimal::ZERO;
        invalid.gross_edge = Decimal::new(3, 2);
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn venue_execution_report_v1_rejects_false_zero_open_confirmation() {
        let submitted_ts = Utc::now();
        let mut report = VenueExecutionReportV1 {
            schema: VENUE_EXECUTION_REPORT_V1_SCHEMA.to_owned(),
            decision_id: "decision-1".to_owned(),
            order_id: Some(OrderId::new("order-1")),
            submitted_ts,
            acknowledged_ts: Some(submitted_ts + Duration::milliseconds(20)),
            cancel_requested_ts: None,
            cancel_acknowledged_ts: None,
            user_channel_acknowledged_ts: Some(submitted_ts + Duration::milliseconds(30)),
            fills: vec![VenueFillV1 {
                trade_id: "trade-1".to_owned(),
                shares: Decimal::ONE,
                price: Decimal::new(40, 2),
                venue_ts: submitted_ts + Duration::seconds(1),
                user_channel_ts: Some(submitted_ts + Duration::seconds(1)),
                markout_1s: Some(Decimal::ZERO),
                markout_5s: Some(Decimal::ZERO),
                markout_30s: Some(Decimal::ZERO),
            }],
            source_agreement: VenueSourceAgreementV1 {
                rest_status: "matched".to_owned(),
                user_channel_status: "matched".to_owned(),
                order_id_agrees: true,
                filled_shares_agree: true,
            },
            zero_open_orders_confirmed: true,
            reconciliation_status: "reconciled".to_owned(),
            risk_before: risk_snapshot(submitted_ts, 1),
            risk_after: risk_snapshot(submitted_ts + Duration::seconds(2), 0),
        };
        report.validate().unwrap();
        report.risk_after.open_orders = 1;
        assert!(report.validate().is_err());
    }
}
