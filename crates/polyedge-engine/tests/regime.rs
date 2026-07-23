use chrono::{DateTime, Duration, Utc};
use polyedge_config::{ExecutionMode, RuntimeSettings};
use polyedge_domain::{DecisionAction, MarketId, OrderKind, Outcome, Side, TokenId, TradeDecision};
use polyedge_engine::{
    evaluate_frozen_strategy, FrozenStrategyMode, QuoteTransformContext, RegimeBookSnapshot,
    RegimeClassifier, RegimeFeatureInput, RegimeFeatures, RegimeLabel, RegimePolicy,
    RegimeReferencePoint, StrategyDecisionEnvelope, DYNAMIC_QUOTE_STYLE_POLICY_CANONICAL_JSON,
    DYNAMIC_QUOTE_STYLE_POLICY_SHA256,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

#[test]
fn frozen_dynamic_quote_policy_hash_matches_explicit_canonical_policy() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../research/configs/frozen_dynamic_quote_style_policy_v1.json");
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(
        std::str::from_utf8(&bytes).unwrap().trim_end(),
        DYNAMIC_QUOTE_STYLE_POLICY_CANONICAL_JSON
    );
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        serde_json::to_string(&parsed).unwrap(),
        DYNAMIC_QUOTE_STYLE_POLICY_CANONICAL_JSON
    );
    let digest = format!(
        "sha256:{:x}",
        Sha256::digest(DYNAMIC_QUOTE_STYLE_POLICY_CANONICAL_JSON)
    );
    assert_eq!(digest, DYNAMIC_QUOTE_STYLE_POLICY_SHA256);
    assert_eq!(
        FrozenStrategyMode::DynamicQuoteStyle
            .candidate()
            .config_hash,
        digest
    );
}

#[test]
fn regime_priority_prefers_feed_risk_over_other_safety_states() {
    let features = RegimeFeatures {
        reference_stale: true,
        market_active: false,
        has_start_price: false,
        has_books: false,
        seconds_to_expiry: Some(10.0),
        final_no_trade_seconds: 30,
        ..RegimeFeatures::default()
    };

    assert_eq!(
        RegimeClassifier::classify_instant(&features),
        RegimeLabel::FeedRisk
    );
}

#[test]
fn regime_hysteresis_requires_confirm_and_dwell_for_non_safety_switch() {
    let mut classifier = RegimeClassifier::new(3, 5);
    let now = Utc::now();
    let normal = RegimeFeatures {
        market_active: true,
        has_start_price: true,
        has_books: true,
        up_spread_ticks: Some(2.0),
        down_spread_ticks: Some(2.0),
        up_top_size: Some(20.0),
        down_top_size: Some(20.0),
        final_no_trade_seconds: 30,
        seconds_to_expiry: Some(300.0),
        ..RegimeFeatures::default()
    };
    let shock = RegimeFeatures {
        chainlink_return_10s_bps: Some(8.0),
        ..normal.clone()
    };

    assert_eq!(classifier.classify(&normal, now), RegimeLabel::Normal);
    assert_eq!(
        classifier.classify(&shock, now + Duration::seconds(2)),
        RegimeLabel::Normal
    );
    assert_eq!(
        classifier.classify(&shock, now + Duration::seconds(6)),
        RegimeLabel::VolatilityShock
    );
}

#[test]
fn classifier_snapshot_round_trip_preserves_the_next_decision() {
    let mut original = RegimeClassifier::new(3, 5);
    let now = Utc::now();
    let normal = RegimeFeatures {
        market_active: true,
        has_start_price: true,
        has_books: true,
        up_spread_ticks: Some(2.0),
        down_spread_ticks: Some(2.0),
        up_top_size: Some(20.0),
        down_top_size: Some(20.0),
        final_no_trade_seconds: 30,
        seconds_to_expiry: Some(300.0),
        ..RegimeFeatures::default()
    };
    let shock = RegimeFeatures {
        chainlink_return_10s_bps: Some(8.0),
        ..normal.clone()
    };
    original.classify(&normal, now);
    original.classify(&shock, now + Duration::seconds(2));

    let encoded = serde_json::to_value(original.snapshot()).unwrap();
    let snapshot = serde_json::from_value(encoded).unwrap();
    let mut restored = RegimeClassifier::from_snapshot(snapshot);

    assert_eq!(
        original.classify(&shock, now + Duration::seconds(6)),
        restored.classify(&shock, now + Duration::seconds(6))
    );
    assert_eq!(original.snapshot(), restored.snapshot());
}

#[test]
fn regime_profiles_are_bounded_and_never_increase_size() {
    let policy = RegimePolicy::new(RuntimeSettings::default().strategy);
    for label in [
        RegimeLabel::FeedRisk,
        RegimeLabel::MarketInactive,
        RegimeLabel::FinalWindow,
        RegimeLabel::VolatilityShock,
        RegimeLabel::NearStrike,
        RegimeLabel::WideOrThinBook,
        RegimeLabel::CalmLiquid,
        RegimeLabel::Normal,
    ] {
        let profile = policy.profile_for(label);
        assert!(profile.config.size_multiplier <= rust_decimal::Decimal::ONE);
        assert!(profile.config.order_ttl_seconds <= 30);
        assert!(profile.config.final_no_trade_seconds <= 300);
        if label.is_safety() {
            assert!(profile.config.no_trade);
            assert!(profile.config.cancel_existing);
        }
    }
}

#[test]
fn live_mode_rejects_adaptive_regime_profiles() {
    let mut settings = RuntimeSettings::default();
    settings.live.execution_mode = ExecutionMode::Live;
    settings.live.allow_live = true;
    settings.live.confirm_non_restricted_location = true;
    settings.live.polymarket_private_key = Some("redacted-test-key".to_owned());
    settings.strategy.adaptive_regime_enabled = true;

    let error = settings.validate_live_gates(true).unwrap_err().to_string();
    assert!(error.contains("adaptive regime profiles are not allowed in live mode"));
    assert!(settings.validate_adaptive_strategy().is_err());
}

#[test]
fn frozen_runtime_mode_rejects_unknown_candidate_and_accepts_dynamic_quote_style() {
    let mut settings = RuntimeSettings::default();
    settings.strategy.adaptive_regime_enabled = true;
    settings.strategy.adaptive_regime_mode = "dynamic_quote_style".to_owned();
    settings.validate_adaptive_strategy().unwrap();

    settings.strategy.adaptive_regime_mode = "typo_candidate".to_owned();
    assert!(settings.validate_adaptive_strategy().is_err());
}

#[test]
fn runtime_and_replay_inputs_produce_the_same_frozen_decision() {
    let now = DateTime::parse_from_rfc3339("2026-07-12T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let features = golden_feature_input(now).build();
    let decision = golden_decision();
    let context = QuoteTransformContext {
        best_bid: Some(Decimal::new(44, 2)),
        q: Some(Decimal::new(50, 2)),
    };
    let policy = RegimePolicy::new(RuntimeSettings::default().strategy);
    let runtime = evaluate_frozen_strategy(
        FrozenStrategyMode::DynamicQuoteStyle,
        &mut RegimeClassifier::default(),
        &policy,
        &features,
        now,
        &decision,
        &context,
    );
    let replay = evaluate_frozen_strategy(
        FrozenStrategyMode::DynamicQuoteStyle,
        &mut RegimeClassifier::default(),
        &policy,
        &golden_feature_input(now).build(),
        now,
        &decision,
        &context,
    );

    assert_eq!(runtime, replay);
    let envelope = StrategyDecisionEnvelope {
        decision: runtime.decision.unwrap(),
        strategy_metadata: runtime.metadata,
    };
    let golden = serde_json::to_value(envelope).unwrap();
    assert_eq!(golden["price"], "0.44");
    assert_eq!(golden["expected_edge"], "0.06");
    assert_eq!(
        golden["strategy_metadata"]["candidate"]["version"],
        "dynamic_quote_style@2026-06-14"
    );
    assert_eq!(
        golden["strategy_metadata"]["candidate"]["config_hash"],
        "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c"
    );
    assert_eq!(golden["strategy_metadata"]["regime"], "volatility_shock");
    assert_eq!(golden["strategy_metadata"]["q"], "0.50");
    assert_eq!(
        golden["strategy_metadata"]["data_quality"]["decision_grade"],
        true
    );
}

fn golden_feature_input(now: DateTime<Utc>) -> RegimeFeatureInput {
    let book = RegimeBookSnapshot {
        bid: Some(Decimal::new(44, 2)),
        ask: Some(Decimal::new(46, 2)),
        bid_size: Some(Decimal::from(20)),
        ask_size: Some(Decimal::from(20)),
        local_ts: Some(now - Duration::milliseconds(50)),
    };
    RegimeFeatureInput {
        now,
        market_start_ts: Some(now - Duration::minutes(5)),
        market_end_ts: Some(now + Duration::minutes(10)),
        start_price: Some(Decimal::from(100_000)),
        tick_size: Decimal::new(1, 2),
        reference: Some(RegimeReferencePoint {
            ts: now - Duration::milliseconds(20),
            price: Decimal::from(100_100),
            stale: false,
        }),
        reference_history: vec![
            RegimeReferencePoint {
                ts: now - Duration::seconds(120),
                price: Decimal::from(99_900),
                stale: false,
            },
            RegimeReferencePoint {
                ts: now - Duration::seconds(10),
                price: Decimal::from(100_000),
                stale: false,
            },
            RegimeReferencePoint {
                ts: now,
                price: Decimal::from(100_100),
                stale: false,
            },
        ],
        q_up: Some(Decimal::new(50, 2)),
        q_down: Some(Decimal::new(50, 2)),
        sigma: Some(0.5),
        up_book: Some(book.clone()),
        down_book: Some(book),
        book_update_rate_10s: None,
        feed_divergence_bps: None,
        recent_feed_errors: 0,
        open_positions: None,
        open_orders: 0,
        recent_fill_count: 0,
        recent_cancel_count: 0,
        adverse_move_after_fill_bps: None,
        max_reference_age_ms: 1_500,
        max_book_age_ms: 1_500,
        final_no_trade_seconds: 30,
        quality_flags: Vec::new(),
    }
}

fn golden_decision() -> TradeDecision {
    TradeDecision {
        action: DecisionAction::Place,
        market_id: MarketId::new("market-1"),
        condition_id: None,
        token_id: Some(TokenId::new("token-1")),
        outcome: Some(Outcome::Up),
        side: Some(Side::Buy),
        price: Some(Decimal::new(45, 2)),
        size: Some(Decimal::ONE),
        quote_amount: None,
        order_kind: Some(OrderKind::PostOnlyGtc),
        reason: "golden maker decision".to_owned(),
        ttl_ms: Some(10_000),
        expected_edge: Some(Decimal::new(5, 2)),
        post_only: true,
        tick_size: Some(Decimal::new(1, 2)),
        neg_risk: false,
    }
}
