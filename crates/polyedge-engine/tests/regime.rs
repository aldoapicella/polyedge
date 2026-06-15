use chrono::{Duration, Utc};
use polyedge_config::{ExecutionMode, RuntimeSettings};
use polyedge_engine::{RegimeClassifier, RegimeFeatures, RegimeLabel, RegimePolicy};

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
}
