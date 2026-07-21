use chrono::{DateTime, Utc};
use polyedge_config::{ExecutionMode, RuntimeSettings};
use polyedge_domain::{
    BookState, ExecutionReport, FairValue, MarketId, MarketSpec, OrderId, ReferencePrice, TokenId,
    TradeDecision,
};
use polyedge_engine::{
    evaluate_decision_pipeline_v3, DecisionPipelineInputV3, FrozenStrategyMode,
    LogReturnFairValueModel, MakerFirstStrategy, MarketStartEvidenceV1, OrderManager,
    PaperFillEngine, RegimeBookSnapshot, RegimeClassifier, RegimeFeatureInput,
    RegimeReferencePoint, RestingMakerOrder, RiskManager,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn fair_value_matches_fixture_golden_master() {
    let cases = fixture();
    for case in cases["fair_value_cases"].as_array().unwrap() {
        let settings = settings(&case["settings"]);
        let model = LogReturnFairValueModel::new(settings);
        let market: MarketSpec = serde_json::from_value(case["market"].clone()).unwrap();
        let reference: ReferencePrice = serde_json::from_value(case["reference"].clone()).unwrap();
        let now = parse_ts(case["now"].as_str().unwrap());
        let actual = model.compute(
            &market,
            &reference,
            now,
            Some(case["sigma"].as_f64().unwrap()),
            None,
        );
        assert_eq!(
            serde_json::to_value(actual.unwrap()).unwrap(),
            case["expected"]
        );
    }
}

#[test]
fn strategy_matches_fixture_golden_master() {
    let cases = fixture();
    for case in cases["strategy_cases"].as_array().unwrap() {
        let strategy = MakerFirstStrategy::new(settings(&case["settings"]));
        let market: MarketSpec = serde_json::from_value(case["market"].clone()).unwrap();
        let fair_value: FairValue = serde_json::from_value(case["fair_value"].clone()).unwrap();
        let actual = strategy.evaluate(&market, &fair_value, &books(&case["books"]));
        assert_eq!(serde_json::to_value(actual).unwrap(), case["expected"]);
    }
}

#[test]
fn risk_matches_fixture_golden_master() {
    let cases = fixture();
    for case in cases["risk_cases"].as_array().unwrap() {
        let risk = RiskManager::new(settings(&case["settings"]));
        let market: MarketSpec = serde_json::from_value(case["market"].clone()).unwrap();
        let reference: ReferencePrice = serde_json::from_value(case["reference"].clone()).unwrap();
        let actual = risk.assess_market(
            &market,
            &reference,
            &books(&case["books"]),
            parse_ts(case["now"].as_str().unwrap()),
            false,
        );
        assert_eq!(serde_json::to_value(actual).unwrap(), case["expected"]);
    }
}

#[test]
fn order_manager_matches_fixture_golden_master() {
    let cases = fixture();
    for case in cases["order_manager_cases"].as_array().unwrap() {
        let manager = OrderManager::new();
        let decisions = case["decisions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| serde_json::from_value::<TradeDecision>(value.clone()).unwrap())
            .collect::<Vec<_>>();
        let market_id = MarketId::new(case["market_id"].as_str().unwrap());
        let actual = manager.reconcile(
            &market_id,
            &decisions,
            None,
            parse_ts(case["now"].as_str().unwrap()),
        );
        assert_eq!(serde_json::to_value(actual).unwrap(), case["expected"]);
    }
}

#[test]
fn complete_decision_pipeline_snapshot_round_trips_and_replays_exactly() {
    let cases = fixture();
    let case = &cases["strategy_cases"][0];
    let risk_case = &cases["risk_cases"][0];
    let mut runtime_settings = settings(&case["settings"]);
    runtime_settings.strategy.adaptive_regime_enabled = true;
    runtime_settings.strategy.adaptive_regime_mode = "dynamic_quote_style".to_owned();
    let market: MarketSpec = serde_json::from_value(case["market"].clone()).unwrap();
    let fair_value: FairValue = serde_json::from_value(case["fair_value"].clone()).unwrap();
    let reference: ReferencePrice = serde_json::from_value(risk_case["reference"].clone()).unwrap();
    let books = books(&case["books"]);
    let decision_ts = parse_ts(case["fair_value"]["computed_ts"].as_str().unwrap());
    let regime_feature_input = RegimeFeatureInput {
        now: decision_ts,
        market_start_ts: Some(market.start_ts),
        market_end_ts: Some(market.end_ts),
        start_price: market.start_price,
        tick_size: market.tick_size,
        reference: Some(RegimeReferencePoint {
            ts: reference.local_ts,
            price: reference.price,
            stale: reference.stale,
        }),
        reference_history: Vec::new(),
        q_up: Some(fair_value.q_up),
        q_down: Some(fair_value.q_down),
        sigma: Some(fair_value.sigma),
        up_book: Some(RegimeBookSnapshot::default()),
        down_book: Some(RegimeBookSnapshot::default()),
        book_update_rate_10s: None,
        feed_divergence_bps: None,
        recent_feed_errors: 0,
        open_positions: None,
        open_orders: 0,
        recent_fill_count: 0,
        recent_cancel_count: 0,
        adverse_move_after_fill_bps: None,
        max_reference_age_ms: runtime_settings.risk.max_reference_age_ms,
        max_book_age_ms: runtime_settings.risk.max_book_age_ms,
        final_no_trade_seconds: 30,
        quality_flags: Vec::new(),
    };
    let market_start_evidence = MarketStartEvidenceV1 {
        schema_version: 1,
        market_id: market.market_id.clone(),
        market_start_ts: market.start_ts,
        market_end_ts: market.end_ts,
        start_price: market.start_price.unwrap(),
        reference_source: reference.source.clone(),
        reference_source_ts: market.start_ts,
        reference_exact_resolution_source: true,
        reference_stale: false,
    };
    let input = DecisionPipelineInputV3 {
        schema_version: 3,
        settings: runtime_settings.clone(),
        market,
        market_start_evidence,
        fair_value,
        reference,
        books,
        decision_ts,
        kill_switch_enabled: false,
        adaptive_mode: Some(FrozenStrategyMode::DynamicQuoteStyle),
        regime_feature_input,
        classifier_before: Some(RegimeClassifier::default().snapshot()),
        risk_before: RiskManager::new(runtime_settings).snapshot(),
        order_manager_before: OrderManager::new().snapshot(),
    };
    let expected = evaluate_decision_pipeline_v3(&input);
    let serialized = serde_json::to_value(&input).unwrap();
    let restored: DecisionPipelineInputV3 = serde_json::from_value(serialized).unwrap();
    assert_eq!(evaluate_decision_pipeline_v3(&restored), expected);
}

#[test]
fn paper_fill_matches_fixture_golden_master() {
    let cases = fixture();
    for case in cases["paper_fill_cases"].as_array().unwrap() {
        let mut engine = PaperFillEngine::new(settings(&case["settings"]));
        let book: BookState = serde_json::from_value(case["book"].clone()).unwrap();
        let markets_by_token = case["market_by_token"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                (
                    TokenId::new(key.clone()),
                    serde_json::from_value::<MarketSpec>(value.clone()).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let resting = &case["resting_order"];
        let resting_order = RestingMakerOrder {
            order_id: OrderId::new(resting["order_id"].as_str().unwrap()),
            decision: serde_json::from_value::<TradeDecision>(resting["decision"].clone()).unwrap(),
            report: serde_json::from_value::<ExecutionReport>(resting["report"].clone()).unwrap(),
        };
        let tracked = case["tracked_order_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| OrderId::new(value.as_str().unwrap()))
            .collect::<BTreeSet<_>>();
        let actual = engine.on_book(
            &book,
            &markets_by_token,
            &[resting_order],
            &tracked,
            parse_ts(case["current_time"].as_str().unwrap()),
        );
        assert_eq!(serde_json::to_value(actual).unwrap(), case["expected"]);
    }
}

fn settings(value: &Value) -> RuntimeSettings {
    let mut settings = RuntimeSettings::default();
    settings.live.execution_mode = if value["execution_mode"].as_str() == Some("live") {
        ExecutionMode::Live
    } else {
        ExecutionMode::Paper
    };
    settings.live.allow_live = value["allow_live"].as_bool().unwrap();
    settings.live.confirm_non_restricted_location =
        value["confirm_non_restricted_location"].as_bool().unwrap();
    settings.live.require_exact_resolution_source_for_live = value
        ["require_exact_resolution_source_for_live"]
        .as_bool()
        .unwrap();
    settings.strategy.maker_margin = dec(&value["maker_margin"]);
    settings.strategy.maker_min_edge = dec(&value["maker_min_edge"]);
    settings.strategy.adverse_selection_buffer = dec(&value["adverse_selection_buffer"]);
    settings.strategy.model_error_buffer = dec(&value["model_error_buffer"]);
    settings.strategy.slippage_buffer = dec(&value["slippage_buffer"]);
    settings.strategy.taker_min_edge = dec(&value["taker_min_edge"]);
    settings.strategy.enable_taker_orders = value["enable_taker_orders"].as_bool().unwrap();
    settings.strategy.ewma_lambda = value["ewma_lambda"].as_f64().unwrap();
    settings.strategy.sigma_floor = value["sigma_floor"].as_f64().unwrap();
    settings.strategy.sigma_cap = value["sigma_cap"].as_f64().unwrap();
    settings.strategy.drift_mu = value["drift_mu"].as_f64().unwrap();
    settings.strategy.final_no_trade_seconds = value["final_no_trade_seconds"].as_i64().unwrap();
    settings.strategy.order_ttl_seconds = value["order_ttl_seconds"].as_i64().unwrap();
    settings.risk.base_order_size = dec(&value["base_order_size"]);
    settings.risk.max_order_size = dec(&value["max_order_size"]);
    settings.risk.max_position_per_market = dec(&value["max_position_per_market"]);
    settings.risk.max_total_position = dec(&value["max_total_position"]);
    settings.risk.max_daily_loss = dec(&value["max_daily_loss"]);
    settings.risk.max_open_orders = value["max_open_orders"].as_u64().unwrap() as usize;
    settings.risk.max_reference_age_ms = value["max_reference_age_ms"].as_i64().unwrap();
    settings.risk.max_book_age_ms = value["max_book_age_ms"].as_i64().unwrap();
    settings.paper.maker_fill_policy = value["paper_maker_fill_policy"]
        .as_str()
        .unwrap()
        .to_owned();
    settings.paper.order_live_after_ms = value["paper_order_live_after_ms"].as_i64().unwrap();
    settings
}

fn books(value: &Value) -> BTreeMap<TokenId, BookState> {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| {
            (
                TokenId::new(key.clone()),
                serde_json::from_value(value.clone()).unwrap(),
            )
        })
        .collect()
}

fn dec(value: &Value) -> rust_decimal::Decimal {
    rust_decimal::Decimal::from_str_exact(value.as_str().unwrap()).unwrap()
}

fn parse_ts(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/rust_parity_cases.json"
    ))
    .unwrap()
}
