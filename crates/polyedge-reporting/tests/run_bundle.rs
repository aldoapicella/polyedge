#![recursion_limit = "512"]

use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use polyedge_reporting::research::{
    classify_warning, expire_funded_manifest, initialize_funded_manifest_after_canary,
    inspect_daily_dependency, legacy_daily_fallback_allowed, parse_azure_artifact_uri,
    publish_daily_directory, run_evaluate_profitability, run_validate_prospective,
    stop_funded_manifest_from_stage_block, validate_protocol_v3_order_evidence,
    write_funded_ladder_state, write_promotion_manifest, AtomicDailyRun, CandidateIdentity,
    DailyDependency, DataQualityCoverageBreakdown, DataQualitySummary, ExecutionModelBinding,
    ExpireFundedManifestOptions, FundedCheckpointEvidenceV1, FundedHoldoutEvaluationV1,
    FundedLadderMetrics, FundedLadderStateV1, FundedStageBlockV1, FundedStageGrantV1, GateStatus,
    ImmutableArtifactBindingV1, InitializeFundedManifestOptions, LatestRunPointer,
    ProfitabilityEvaluationOptions, ProfitabilityMetrics, PromotionEvaluation, PromotionManifestV1,
    PromotionPhase, ProspectiveValidationOptions, QueueModelTransitionV1,
    StopFundedManifestFromStageBlockOptions, WarningSeverity, DEFAULT_PROFITABILITY_LATEST,
    WARNING_REGISTRY_VERSION,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn stable_json_for_test(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(stable_json_for_test)
                .collect::<Vec<_>>()
                .join(",")
        ),
        serde_json::Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|key| format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        stable_json_for_test(&values[key])
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        _ => serde_json::to_string(value).unwrap(),
    }
}

fn protocol_v3_raw_book(token_id: &str, best_bid: &str, best_ask: &str) -> serde_json::Value {
    serde_json::json!({
        "token_id": token_id,
        "tick_size": "0.01",
        "min_order_size": "1",
        "bids": [{"price": best_bid, "size": "10"}],
        "asks": [{"price": best_ask, "size": "10"}],
        "venue_hash": "1111111111111111111111111111111111111111"
    })
}

fn protocol_v3_book_hash(book: &serde_json::Value) -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(stable_json_for_test(book).as_bytes())
    )
}

fn protocol_v3_markout_rows(now: DateTime<Utc>) -> Vec<serde_json::Value> {
    [
        (1_i64, "0.24", "0.26"),
        (5, "0.25", "0.27"),
        (30, "0.26", "0.28"),
    ]
    .into_iter()
    .map(|(horizon, best_bid, best_ask)| {
        let book = protocol_v3_raw_book("token-1", best_bid, best_ask);
        let midpoint = (best_bid.parse::<Decimal>().unwrap()
            + best_ask.parse::<Decimal>().unwrap())
            / Decimal::from(2_u32);
        let executable = best_bid.parse::<Decimal>().unwrap();
        let fill_price = Decimal::new(20, 2);
        let target = now + Duration::milliseconds(600) + Duration::seconds(horizon);
        let response = target + Duration::milliseconds(100);
        serde_json::json!({
            "fill_id": "trade-1",
            "horizon_seconds": horizon,
            "fill_timestamp": (now + Duration::milliseconds(600)).to_rfc3339(),
            "venue_fill_timestamp": (now + Duration::milliseconds(600)).to_rfc3339(),
            "target_observation_ts": target.to_rfc3339(),
            "request_started_at": target.to_rfc3339(),
            "response_completed_at": response.to_rfc3339(),
            "observed_at": response.to_rfc3339(),
            "response_duration_ms": 100,
            "observation_delay_ms": 100,
            "book_hash": protocol_v3_book_hash(&book),
            "venue_book_hash": "1111111111111111111111111111111111111111",
            "venue_book_timestamp": response.to_rfc3339(),
            "raw_orderbook": book,
            "fill_size": "5",
            "fill_price": "0.20",
            "midpoint": midpoint.to_string(),
            "executable_price": best_bid,
            "midpoint_markout_per_share": (midpoint - fill_price).to_string(),
            "executable_markout_per_share": (executable - fill_price).to_string(),
            "trader_side": "MAKER",
            "authenticated_order_role": "MAKER",
            "authenticated_fee_rate_bps": null,
            "authenticated_fee_amount": null,
            "authenticated_fee_raw": null,
            "entry_fee_per_share": "0",
            "hypothetical_exit_fee_per_share": "0",
            "round_trip_fee_per_share": "0"
        })
    })
    .collect()
}

#[test]
fn warning_registry_is_versioned_and_unknown_warnings_block() {
    let known = classify_warning("42 out-of-order timestamps");
    assert_eq!(known.severity, WarningSeverity::Informational);
    assert!(known.known);

    let unknown = classify_warning("new recorder anomaly");
    assert_eq!(unknown.severity, WarningSeverity::Blocking);
    assert!(!unknown.known);

    let quality = DataQualitySummary::new(
        10,
        Decimal::new(100, 2),
        Vec::new(),
        vec!["new recorder anomaly".to_owned()],
    );
    assert_eq!(quality.registry_version, WARNING_REGISTRY_VERSION);
    assert!(!quality.promotion_allowed());

    for (message, expected_code) in [
        (
            "runtime/replay full decision pipeline parity below 100%: 0/1 replayed",
            "full_decision_pipeline_parity_below_100pct",
        ),
        (
            "settlement journal conflicts: 1",
            "settlement_journal_conflict",
        ),
        (
            "incomplete or hash-invalid settlement journals: 1",
            "settlement_journal_incomplete_or_hash_invalid",
        ),
        (
            "v3 paper settlements missing durable journal binding: 1",
            "settlement_journal_binding_missing",
        ),
        (
            "invalid exact market start price evidence: 1",
            "market_start_exact_evidence_invalid",
        ),
        (
            "decision config is missing or changed within the eligible day: 2 distinct hashes",
            "decision_config_missing_or_changed",
        ),
    ] {
        let classified = classify_warning(message);
        assert!(classified.known, "{message}");
        assert_eq!(classified.severity, WarningSeverity::Blocking);
        assert_eq!(classified.rule_id, expected_code);
    }
}

#[test]
fn execution_quality_join_warnings_are_known_and_remain_promotion_blocking() {
    let cases = [
        (
            "place-output application binding below 100%: 1/2 applied, 1 unbound, 0 orphan applications, 0 identity mismatches, 0 invalid, 0 conflicts, 0 reused order IDs",
            "decision_application_binding_below_100pct",
        ),
        (
            "durable actionable decision application binding below 100%: 1/2 applied, 1 unbound, 0 orphan applications",
            "decision_application_binding_below_100pct",
        ),
        (
            "invalid paper decision application proof blocks v3 replay",
            "decision_application_binding_invalid",
        ),
        (
            "1 queue registrations could not be joined because order_id is missing",
            "queue_registration_order_id_missing",
        ),
        (
            "conflicting queue registrations reused 1 order IDs",
            "queue_registration_order_id_conflict",
        ),
        (
            "1 queue registration order IDs lack complete lifecycle identity fields",
            "queue_registration_identity_invalid",
        ),
        (
            "1 queue registrations do not join one-to-one to applied place outputs",
            "queue_registration_application_join_invalid",
        ),
        (
            "orphan queue snapshots cannot satisfy registered orders: 2 events across 1 order IDs",
            "queue_snapshot_orphan",
        ),
        (
            "duplicate queue snapshots are promotion-blocking: 1 excess events across 1 order IDs",
            "queue_snapshot_duplicate",
        ),
        (
            "1 queue snapshots lack numeric inferred_size_ahead",
            "queue_snapshot_size_ahead_invalid",
        ),
        (
            "queue snapshot coverage below 95%: 94/100",
            "queue_snapshot_coverage_below_95pct",
        ),
        (
            "1 eligible fill lifecycle events lack the fields required for markout joins",
            "markout_fill_lifecycle_join_fields_missing",
        ),
        (
            "1 fill lifecycle joins conflict with registered order identity",
            "fill_lifecycle_registration_conflict",
        ),
        (
            "1 markout rows lack a supported horizon or lifecycle join fields",
            "markout_row_join_fields_invalid",
        ),
        (
            "1 orphan markout rows do not join to an eligible fill lifecycle",
            "markout_row_orphan",
        ),
        (
            "1 excess markout fill IDs cannot be matched to actual fill lifecycles",
            "markout_fill_id_excess",
        ),
        (
            "duplicate markouts are promotion-blocking: 1 excess rows across 1 lifecycle/horizon slots",
            "markout_row_duplicate",
        ),
        (
            "1 markout rows are missing, null, gross-only, fee-inconsistent, non-executable, or more than 2000ms late",
            "markout_row_invalid_or_untimely",
        ),
        (
            "30s markout completion below 95%: 94/100",
            "markout_completion_below_95pct",
        ),
        (
            "settlement journal events with incomplete or invalid binding: 1",
            "settlement_journal_event_binding_invalid",
        ),
    ];

    for (message, rule_id) in cases {
        let classified = classify_warning(message);
        assert!(classified.known, "warning should be registered: {message}");
        assert_eq!(classified.rule_id, rule_id, "wrong rule for {message}");
        assert_eq!(classified.severity, WarningSeverity::Blocking);

        let quality =
            DataQualitySummary::new(100, Decimal::ONE, Vec::new(), vec![message.to_owned()]);
        assert!(
            !quality.promotion_allowed(),
            "registered warning must continue to block promotion: {message}"
        );
    }

    let unknown = classify_warning(
        "a new execution-quality anomaly that has no reviewed warning-registry rule",
    );
    assert!(!unknown.known);
    assert_eq!(unknown.severity, WarningSeverity::Blocking);
}

#[test]
fn exact_azure_artifact_uri_requires_account_container_and_safe_blob_path() {
    assert_eq!(
        parse_azure_artifact_uri("azure://stpolyedge/polyedge-funded-evidence/runs/1.json")
            .unwrap(),
        (
            "stpolyedge".to_owned(),
            "polyedge-funded-evidence".to_owned(),
            "runs/1.json".to_owned()
        )
    );
    for invalid in [
        "polyedge-funded-evidence/runs/1.json",
        "azure://stpolyedge/polyedge-funded-evidence",
        "azure:///polyedge-funded-evidence/runs/1.json",
        "azure://stpolyedge//runs/1.json",
        "azure://stpolyedge/polyedge-funded-evidence/../grant.json",
    ] {
        assert!(parse_azure_artifact_uri(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn flat_daily_fallback_is_historical_only_and_never_overrides_atomic_markers() {
    let historical = NaiveDate::from_ymd_opt(2026, 7, 11).unwrap();
    let cutoff = NaiveDate::from_ymd_opt(2026, 7, 12).unwrap();

    assert!(legacy_daily_fallback_allowed(historical, false));
    assert!(!legacy_daily_fallback_allowed(historical, true));
    assert!(!legacy_daily_fallback_allowed(cutoff, false));
}

#[test]
fn latest_pointer_is_created_only_after_complete_verified_manifest() {
    let root = test_dir("atomic_complete");
    let date = NaiveDate::from_ymd_opt(2026, 7, 12).unwrap();
    let quality = clean_quality();
    let input_hash = format!("sha256:{}", "a".repeat(64));
    let mut run = AtomicDailyRun::begin(&root, date, "run-001", input_hash, quality).unwrap();

    assert!(!root.join("latest.json").exists());
    assert!(!root.join("2026-07-12/latest.json").exists());
    run.write_artifact("baseline", "baseline.json", br#"{"ok":true}"#)
        .unwrap();
    assert!(!root.join("latest.json").exists());

    let pointer = run.complete().unwrap();
    assert_eq!(pointer.run_id, "run-001");
    assert!(root.join("latest.json").is_file());
    assert!(root.join("2026-07-12/latest.json").is_file());
    match inspect_daily_dependency(&root, date).unwrap() {
        DailyDependency::Ready {
            run_id,
            bundle_dir,
            manifest,
            ..
        } => {
            assert_eq!(run_id, "run-001");
            assert_eq!(manifest.artifacts.len(), 1);
            assert!(bundle_dir.join("baseline.json").is_file());
        }
        dependency => panic!("expected ready dependency, got {dependency:?}"),
    }

    assert!(AtomicDailyRun::begin(
        &root,
        date,
        "run-001",
        format!("sha256:{}", "b".repeat(64)),
        clean_quality(),
    )
    .is_err());
}

#[test]
fn historical_correction_never_regresses_global_latest_pointer() {
    let root = test_dir("atomic_global_latest_monotonic");
    let newer_date = NaiveDate::from_ymd_opt(2026, 7, 13).unwrap();
    let older_date = NaiveDate::from_ymd_opt(2026, 7, 12).unwrap();

    let mut newer = AtomicDailyRun::begin(
        &root,
        newer_date,
        "run-newer",
        format!("sha256:{}", "a".repeat(64)),
        clean_quality(),
    )
    .unwrap();
    newer
        .write_artifact("baseline", "baseline.json", br#"{"day":13}"#)
        .unwrap();
    newer.complete().unwrap();

    let mut older = AtomicDailyRun::begin(
        &root,
        older_date,
        "run-corrected-older",
        format!("sha256:{}", "b".repeat(64)),
        clean_quality(),
    )
    .unwrap();
    older
        .write_artifact("baseline", "baseline.json", br#"{"day":12}"#)
        .unwrap();
    older.complete().unwrap();

    let global: LatestRunPointer =
        serde_json::from_slice(&fs::read(root.join("latest.json")).unwrap()).unwrap();
    assert_eq!(global.date, newer_date);
    assert_eq!(global.run_id, "run-newer");
    let corrected: LatestRunPointer =
        serde_json::from_slice(&fs::read(root.join("2026-07-12/latest.json")).unwrap()).unwrap();
    assert_eq!(corrected.run_id, "run-corrected-older");
}

#[test]
fn post_cutoff_manifest_schema_downgrade_is_rejected() {
    let root = test_dir("manifest_schema_downgrade");
    let date = NaiveDate::from_ymd_opt(2026, 7, 12).unwrap();
    let mut run = AtomicDailyRun::begin(
        &root,
        date,
        "run-schema-v2",
        format!("sha256:{}", "a".repeat(64)),
        clean_quality(),
    )
    .unwrap();
    run.write_artifact("baseline", "baseline.json", br#"{"ok":true}"#)
        .unwrap();
    run.complete().unwrap();

    let manifest_path = root.join("2026-07-12/runs/run-schema-v2/run_manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["schema_version"] = serde_json::json!(1);
    manifest["git_sha"] = serde_json::Value::Null;
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    fs::write(&manifest_path, &manifest_bytes).unwrap();
    let pointer_path = root.join("2026-07-12/latest.json");
    let mut pointer: LatestRunPointer =
        serde_json::from_slice(&fs::read(&pointer_path).unwrap()).unwrap();
    pointer.manifest_sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));
    fs::write(&pointer_path, serde_json::to_vec_pretty(&pointer).unwrap()).unwrap();

    let dependency = inspect_daily_dependency(&root, date).unwrap();
    assert!(
        matches!(
            &dependency,
            DailyDependency::WaitingForDependency { reason, .. }
                if reason == "manifest_schema_downgrade"
        ),
        "unexpected dependency: {dependency:?}"
    );
}

#[test]
fn incomplete_or_tampered_bundle_never_becomes_ready() {
    let root = test_dir("atomic_incomplete");
    let date = NaiveDate::from_ymd_opt(2026, 7, 12).unwrap();
    let mut run =
        AtomicDailyRun::begin(&root, date, "run-002", "c".repeat(64), clean_quality()).unwrap();
    run.write_artifact("baseline", "baseline.json", b"before")
        .unwrap();
    match inspect_daily_dependency(&root, date).unwrap() {
        DailyDependency::WaitingForDependency { reason, .. } => {
            assert_eq!(reason, "latest_pointer_absent")
        }
        dependency => panic!("expected waiting dependency, got {dependency:?}"),
    }

    run.complete().unwrap();
    fs::write(
        root.join("2026-07-12/runs/run-002/baseline.json"),
        "tampered",
    )
    .unwrap();
    match inspect_daily_dependency(&root, date).unwrap() {
        DailyDependency::WaitingForDependency { reason, .. } => {
            assert_eq!(reason, "artifact_verification_failed")
        }
        dependency => panic!("expected waiting dependency, got {dependency:?}"),
    }
}

#[test]
fn prospective_waiting_preserves_previous_latest_output() {
    let root = test_dir("prospective_waiting");
    let out = root.join("prospective/latest.json");
    let markdown = root.join("prospective/latest.md");
    fs::create_dir_all(out.parent().unwrap()).unwrap();
    fs::write(&out, "previous-json").unwrap();
    fs::write(&markdown, "previous-markdown").unwrap();

    let report = run_validate_prospective(ProspectiveValidationOptions {
        since: Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
        reports_dir: root.join("daily"),
        candidates: candidate_registry(),
        out: out.clone(),
        markdown: markdown.clone(),
        expected_daily_date: Some(NaiveDate::from_ymd_opt(2026, 7, 12).unwrap()),
    })
    .unwrap();

    assert_eq!(report["result"]["status"], "waiting_for_dependency");
    assert_eq!(report["result"]["previous_latest_preserved"], true);
    assert_eq!(report["result"]["output_written"], false);
    assert_eq!(fs::read_to_string(out).unwrap(), "previous-json");
    assert_eq!(fs::read_to_string(markdown).unwrap(), "previous-markdown");
}

#[test]
fn shadow_promotion_uses_the_pinned_prior_without_authenticated_model_evidence() {
    let passing = ProfitabilityMetrics {
        observed_calendar_days: 30,
        clean_days: 30,
        settled_markets: 1_000,
        wallet_constrained: true,
        queue_conservative: true,
        wallet_constrained_net_pnl: Decimal::ONE,
        wallet_constrained_ending_equity: Decimal::new(6_030_521, 6),
        queue_conservative_net_pnl: Decimal::ONE,
        pnl_ci_95_low: Decimal::new(1, 2),
        consecutive_positive_weekly_blocks: 4,
        max_drawdown: Decimal::new(50, 2),
        drawdown_limit: Decimal::ONE,
        markout_30s_ci_low: Decimal::new(1, 2),
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
        data_quality: clean_quality(),
        missing_metrics: Vec::new(),
    };
    let evaluation = PromotionEvaluation::evaluate_shadow(passing.clone());
    assert_eq!(evaluation.phase, PromotionPhase::ShadowPassed);
    assert!(evaluation.promotion_allowed);
    assert!(evaluation
        .gates
        .iter()
        .all(|gate| gate.status == GateStatus::Passed));

    let mut blocked = passing;
    blocked.data_quality = DataQualitySummary::new(
        10,
        Decimal::ONE,
        Vec::new(),
        vec!["unreviewed warning".to_owned()],
    );
    let evaluation = PromotionEvaluation::evaluate_shadow(blocked);
    assert_eq!(evaluation.phase, PromotionPhase::ShadowCollecting);
    assert!(!evaluation.promotion_allowed);
    assert_eq!(
        evaluation
            .gates
            .iter()
            .find(|gate| gate.gate == "data_quality")
            .unwrap()
            .status,
        GateStatus::Failed
    );

    let mutations: [fn(&mut ProfitabilityMetrics); 7] = [
        |metrics: &mut ProfitabilityMetrics| metrics.execution_model_eligible_orders = 99,
        |metrics: &mut ProfitabilityMetrics| metrics.execution_model_filled_orders = 9,
        |metrics: &mut ProfitabilityMetrics| metrics.execution_model_non_filled_orders = 9,
        |metrics: &mut ProfitabilityMetrics| {
            metrics.execution_model_brier_improvement = Decimal::new(49, 3)
        },
        |metrics: &mut ProfitabilityMetrics| {
            metrics.execution_model_expected_calibration_error = Decimal::new(101, 3)
        },
        |metrics: &mut ProfitabilityMetrics| metrics.execution_model_promotion_ready = false,
        |metrics: &mut ProfitabilityMetrics| {
            metrics.execution_model_markout_30s_lower_95 = Decimal::ZERO
        },
    ];
    for mutate in mutations {
        let mut metrics = passing_metrics();
        mutate(&mut metrics);
        assert!(PromotionEvaluation::evaluate_shadow(metrics).promotion_allowed);
    }
}

#[test]
fn failed_candidate_reaches_terminal_no_go_at_extension_limit() {
    let mut metrics = passing_metrics();
    metrics.observed_calendar_days = 60;
    metrics.wallet_constrained_net_pnl = Decimal::new(-1, 0);
    let evaluation = PromotionEvaluation::evaluate_shadow(metrics);
    assert_eq!(evaluation.phase, PromotionPhase::StoppedNoGo);
    assert!(!evaluation.promotion_allowed);
}

#[test]
fn promotion_manifest_is_fail_closed_serializable_and_atomically_publishable() {
    let root = test_dir("promotion_manifest");
    let evaluation = PromotionEvaluation::evaluate_shadow(passing_metrics());
    let created_at = Utc::now();
    let manifest = PromotionManifestV1::new(
        CandidateIdentity {
            name: "dynamic_quote_style".to_owned(),
            candidate_version: "dynamic_quote_style@2026-06-14".to_owned(),
            config_hash: "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c"
                .to_owned(),
        },
        evaluation,
        BTreeMap::from([
            (
                "daily_manifest".to_owned(),
                "azure://reports/2026-07-12/run_manifest.json".to_owned(),
            ),
            (
                "prospective".to_owned(),
                "azure://reports/prospective/latest.json".to_owned(),
            ),
        ]),
        ExecutionModelBinding {
            blob_uri:
                "azure://account/bot-events/reports/research/venue-probe/effective_queue_model.json"
                    .to_owned(),
            sha256: format!("sha256:{}", "b".repeat(64)),
            model_version: "queue-calibration-v1".to_owned(),
        },
        created_at,
        created_at + chrono::Duration::hours(24),
    )
    .unwrap();
    assert_eq!(manifest.schema_version, "promotion_manifest_v1");
    assert!(manifest.human_authorization_required);
    assert_eq!(
        manifest.execution_model.model_version,
        "queue-calibration-v1"
    );
    assert!(manifest.execution_model.sha256.starts_with("sha256:"));
    assert!(!manifest.promotion_allowed);
    assert_eq!(manifest.phase, PromotionPhase::ShadowPassed);

    let out = root.join(DEFAULT_PROFITABILITY_LATEST);
    write_promotion_manifest(&out, &manifest).unwrap();
    let stored: PromotionManifestV1 = serde_json::from_slice(&fs::read(&out).unwrap()).unwrap();
    assert_eq!(stored, manifest);

    let mut invalid = manifest;
    invalid.human_authorization_required = false;
    assert!(write_promotion_manifest(&out, &invalid).is_err());
    let still_stored: PromotionManifestV1 =
        serde_json::from_slice(&fs::read(&out).unwrap()).unwrap();
    assert!(still_stored.human_authorization_required);
}

#[test]
fn generated_daily_directory_is_packaged_with_required_artifacts_and_quality() {
    let root = test_dir("publish_directory");
    let source = root.join("generated");
    fs::create_dir_all(&source).unwrap();
    for name in [
        "baseline.json",
        "regimes.json",
        "final_report.json",
        "execution_quality.json",
    ] {
        fs::write(source.join(name), format!(r#"{{"artifact":"{name}"}}"#)).unwrap();
    }
    fs::write(
        source.join("execution_quality.json"),
        complete_execution_quality(),
    )
    .unwrap();
    let audit = source.join("data_audit.json");
    fs::write(&audit, complete_daily_audit("2026-07-12", 0.97)).unwrap();

    let published = publish_daily_directory(
        NaiveDate::from_ymd_opt(2026, 7, 12).unwrap(),
        "daily-20260712",
        "d".repeat(64),
        polyedge_config::RuntimeRole::ProfitabilityShadow,
        &source,
        &root.join("reports/research/daily"),
        &audit,
    )
    .unwrap();

    assert_eq!(published.manifest.artifacts.len(), 5);
    assert_eq!(published.manifest.schema_version, 2);
    assert_eq!(
        published.manifest.runtime_role,
        Some(polyedge_config::RuntimeRole::ProfitabilityShadow)
    );
    assert!(published
        .manifest
        .git_sha
        .as_deref()
        .is_some_and(polyedge_config::is_full_git_sha));
    assert_eq!(
        published.manifest.data_quality.decision_grade_coverage,
        Decimal::new(97, 2)
    );
    assert!(published.manifest.data_quality.promotion_allowed());
    assert!(published.bundle_dir.join("final_report.json").is_file());
}

#[test]
fn partial_or_materially_gapped_utc_day_is_published_but_never_clean() {
    let root = test_dir("partial_daily_capture");
    let source = root.join("generated");
    fs::create_dir_all(&source).unwrap();
    for name in [
        "baseline.json",
        "regimes.json",
        "final_report.json",
        "execution_quality.json",
    ] {
        fs::write(source.join(name), format!(r#"{{"artifact":"{name}"}}"#)).unwrap();
    }
    let audit = source.join("data_audit.json");
    let observed_hours = (9..24)
        .map(|hour| (format!("2026-07-12T{hour:02}"), 100_u64))
        .collect::<BTreeMap<_, _>>();
    fs::write(
        &audit,
        serde_json::to_vec_pretty(&serde_json::json!({
            "result": {
                "total_events": 1000,
                "decision_grade_coverage": 1.0,
                "fatal_data_quality_issues": [],
                "warnings": [],
                "event_time_ordering_restored": true,
                "out_of_order_timestamps": 0,
                "first_event_timestamp": "2026-07-12T09:44:20Z",
                "last_event_timestamp": "2026-07-12T23:59:59Z",
                "event_count_by_hour": observed_hours,
                "largest_time_gaps": [{"gap_ms": 600001}]
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let published = publish_daily_directory(
        NaiveDate::from_ymd_opt(2026, 7, 12).unwrap(),
        "partial-20260712",
        "f".repeat(64),
        polyedge_config::RuntimeRole::ProfitabilityShadow,
        &source,
        &root.join("reports/research/daily"),
        &audit,
    )
    .unwrap();
    let rules = published
        .manifest
        .data_quality
        .warnings
        .iter()
        .map(|warning| warning.rule_id.as_str())
        .collect::<Vec<_>>();
    assert!(rules.contains(&"daily_capture_window_incomplete"));
    assert!(rules.contains(&"daily_capture_gap_exceeds_5m"));
    assert!(!published.manifest.data_quality.promotion_allowed());
}

#[test]
fn missing_gap_evidence_is_known_blocking() {
    let warning = classify_warning("daily capture gap evidence missing for 2026-07-12");
    assert!(warning.known);
    assert_eq!(warning.severity, WarningSeverity::Blocking);
    assert_eq!(warning.rule_id, "daily_capture_gap_evidence_missing");
}

#[test]
fn empty_or_malformed_gap_evidence_is_blocking() {
    let root = test_dir("invalid_gap_evidence");
    for (run_id, gaps) in [
        ("empty-gaps", serde_json::json!([])),
        ("malformed-gaps", serde_json::json!([{"not_gap_ms": 1}])),
    ] {
        let source = root.join(run_id);
        fs::create_dir_all(&source).unwrap();
        for name in [
            "baseline.json",
            "regimes.json",
            "final_report.json",
            "execution_quality.json",
        ] {
            fs::write(source.join(name), format!(r#"{{"artifact":"{name}"}}"#)).unwrap();
        }
        let audit = source.join("data_audit.json");
        let mut audit_value: serde_json::Value =
            serde_json::from_slice(&complete_daily_audit("2026-07-14", 1.0)).unwrap();
        audit_value["result"]["largest_time_gaps"] = gaps;
        fs::write(&audit, serde_json::to_vec_pretty(&audit_value).unwrap()).unwrap();
        let published = publish_daily_directory(
            NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
            run_id,
            "1".repeat(64),
            polyedge_config::RuntimeRole::ProfitabilityShadow,
            &source,
            &root.join("reports/research/daily"),
            &audit,
        )
        .expect("publish bundle");
        assert!(!published.manifest.data_quality.promotion_allowed());
        assert!(published
            .manifest
            .data_quality
            .warnings
            .iter()
            .any(|warning| {
                warning.rule_id == "daily_capture_gap_evidence_missing"
                    && warning.severity == WarningSeverity::Blocking
            }));
    }
}

#[test]
fn runtime_provenance_missing_unknown_or_changed_is_blocking() {
    let root = test_dir("runtime_provenance_gate");
    for (run_id, expected_rule) in [
        ("missing", "daily_runtime_provenance_missing"),
        ("unknown", "daily_runtime_provenance_invalid"),
        ("v2-contract", "daily_runtime_provenance_invalid"),
        ("missing-config", "daily_runtime_provenance_invalid"),
        (
            "mid-day-change",
            "daily_runtime_provenance_identity_changed",
        ),
    ] {
        let source = root.join(run_id);
        fs::create_dir_all(&source).unwrap();
        for name in [
            "baseline.json",
            "regimes.json",
            "final_report.json",
            "execution_quality.json",
        ] {
            fs::write(source.join(name), format!(r#"{{"artifact":"{name}"}}"#)).unwrap();
        }
        let mut audit: serde_json::Value =
            serde_json::from_slice(&complete_daily_audit("2026-07-14", 1.0)).unwrap();
        match run_id {
            "missing" => {
                audit["result"]
                    .as_object_mut()
                    .unwrap()
                    .remove("runtime_provenance");
            }
            "unknown" => {
                audit["result"]["runtime_provenance"]["identities"][0]["git_sha"] =
                    serde_json::json!("unknown");
            }
            "v2-contract" => {
                audit["result"]["runtime_provenance"]["identities"][0]
                    ["decision_pipeline_schema"] =
                    serde_json::json!("polyedge.strategy_decision_batch.v2");
            }
            "missing-config" => {
                audit["result"]["runtime_provenance"]["identities"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("decision_config_sha256");
            }
            "mid-day-change" => {
                let mut changed = audit["result"]["runtime_provenance"]["identities"][0].clone();
                changed["runtime_config_hash"] =
                    serde_json::json!(format!("sha256:{}", "c".repeat(64)));
                audit["result"]["runtime_provenance"]["identities"]
                    .as_array_mut()
                    .unwrap()
                    .push(changed);
                audit["result"]["runtime_provenance"]["distinct_identity_count"] =
                    serde_json::json!(2);
            }
            _ => unreachable!(),
        }
        let audit_path = source.join("data_audit.json");
        fs::write(&audit_path, serde_json::to_vec_pretty(&audit).unwrap()).unwrap();
        let published = publish_daily_directory(
            NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
            run_id,
            "2".repeat(64),
            polyedge_config::RuntimeRole::ProfitabilityShadow,
            &source,
            &root.join("reports/research/daily"),
            &audit_path,
        )
        .unwrap();
        assert!(!published.manifest.data_quality.promotion_allowed());
        assert!(published
            .manifest
            .data_quality
            .warnings
            .iter()
            .any(|warning| warning.rule_id == expected_rule));
    }
}

#[test]
fn historical_reporter_sha_difference_is_informational_lineage() {
    let root = test_dir("runtime_provenance_historical_reporter");
    let source = root.join("mismatched");
    fs::create_dir_all(&source).unwrap();
    for name in [
        "baseline.json",
        "regimes.json",
        "final_report.json",
        "execution_quality.json",
    ] {
        fs::write(source.join(name), format!(r#"{{"artifact":"{name}"}}"#)).unwrap();
    }
    fs::write(
        source.join("execution_quality.json"),
        complete_execution_quality(),
    )
    .unwrap();
    let reporter_sha = current_git_sha();
    let runtime_sha = if reporter_sha == "f".repeat(40) {
        "e".repeat(40)
    } else {
        "f".repeat(40)
    };
    let mut audit: serde_json::Value =
        serde_json::from_slice(&complete_daily_audit("2026-07-14", 1.0)).unwrap();
    audit["result"]["runtime_provenance"]["identities"][0]["git_sha"] =
        serde_json::json!(runtime_sha.clone());
    let audit_path = source.join("data_audit.json");
    fs::write(&audit_path, serde_json::to_vec_pretty(&audit).unwrap()).unwrap();

    let published = publish_daily_directory(
        NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
        "historical-reporter",
        "2".repeat(64),
        polyedge_config::RuntimeRole::ProfitabilityShadow,
        &source,
        &root.join("reports/research/daily"),
        &audit_path,
    )
    .unwrap();

    let mismatch = published
        .manifest
        .data_quality
        .warnings
        .iter()
        .find(|warning| warning.rule_id == "daily_runtime_provenance_reporter_mismatch")
        .expect("the distinct recorder and reporter SHAs remain visible");
    assert_eq!(mismatch.severity, WarningSeverity::Informational);
    assert_eq!(
        published.manifest.git_sha.as_deref(),
        Some(reporter_sha.as_str())
    );
    let published_audit: serde_json::Value =
        serde_json::from_slice(&fs::read(published.bundle_dir.join("data_audit.json")).unwrap())
            .unwrap();
    assert_eq!(
        published_audit["result"]["runtime_provenance"]["identities"][0]["git_sha"],
        runtime_sha
    );
    assert!(published.manifest.data_quality.promotion_allowed());
}

#[test]
fn primary_daily_role_uses_its_own_provenance_contract() {
    let root = test_dir("primary_runtime_provenance_gate");
    let source = root.join("generated");
    fs::create_dir_all(&source).unwrap();
    for name in [
        "baseline.json",
        "regimes.json",
        "final_report.json",
        "execution_quality.json",
    ] {
        fs::write(source.join(name), format!(r#"{{"artifact":"{name}"}}"#)).unwrap();
    }
    fs::write(
        source.join("execution_quality.json"),
        complete_execution_quality(),
    )
    .unwrap();
    let mut audit: serde_json::Value =
        serde_json::from_slice(&complete_daily_audit("2026-07-14", 1.0)).unwrap();
    audit["result"]["runtime_provenance"]["identities"][0] =
        valid_primary_runtime_provenance_identity(&current_git_sha());
    let audit_path = source.join("data_audit.json");
    fs::write(&audit_path, serde_json::to_vec_pretty(&audit).unwrap()).unwrap();
    let published = publish_daily_directory(
        NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
        "primary-20260714",
        "3".repeat(64),
        polyedge_config::RuntimeRole::Primary,
        &source,
        &root.join("reports/research/daily"),
        &audit_path,
    )
    .unwrap();
    assert!(published.manifest.data_quality.promotion_allowed());
    assert_eq!(
        published.manifest.runtime_role,
        Some(polyedge_config::RuntimeRole::Primary)
    );
}

#[test]
fn out_of_order_events_require_a_low_measured_rate_and_restored_ordering() {
    let mut high_rate = measured_quality(
        1_000,
        Decimal::ONE,
        Vec::new(),
        vec!["42 out-of-order timestamps".to_owned()],
    );
    high_rate.event_time_ordering_restored = true;
    assert!(!high_rate.promotion_allowed());

    let mut low_rate = measured_quality(
        100_000,
        Decimal::ONE,
        Vec::new(),
        vec!["1 out-of-order timestamps".to_owned()],
    );
    assert!(!low_rate.promotion_allowed());
    low_rate.event_time_ordering_restored = true;
    assert!(low_rate.promotion_allowed());
}

#[test]
fn scalar_quality_without_measured_components_cannot_pass_open() {
    let quality = DataQualitySummary::new(100_000, Decimal::ONE, Vec::new(), Vec::<String>::new());
    assert_eq!(
        quality.coverage_breakdown,
        DataQualityCoverageBreakdown::default()
    );
    assert!(!quality.promotion_allowed());
}

#[test]
fn profitability_cli_core_passes_complete_metrics_but_never_arms_execution() {
    let root = test_dir("evaluate_profitability");
    let source = root.join("generated");
    fs::create_dir_all(&source).unwrap();
    fs::write(
        source.join("baseline.json"),
        r#"{"result":{"fill_model":"queue_proxy_conservative","wallet_constrained":true,"decision_parity_rate":1.0,"summary":{"complete_for_simulation":2}}}"#,
    )
    .unwrap();
    fs::write(
        source.join("regimes.json"),
        r#"{"result":{"fill_model":"queue_proxy_conservative","profiles":[{"profile":"dynamic_quote_style","net_pnl":"1.25","wallet_constrained":true,"wallet_constrained_net_pnl":"1.25"},{"profile":"static","net_pnl":"0.25","wallet_constrained":true,"wallet_constrained_net_pnl":"0.25"}]}}"#,
    )
    .unwrap();
    fs::write(source.join("final_report.json"), r#"{"result":{}}"#).unwrap();
    fs::write(
        source.join("execution_quality.json"),
        complete_execution_quality(),
    )
    .unwrap();
    let audit = source.join("data_audit.json");
    let daily_root = root.join("reports/research/shadow/daily");
    let first_date = NaiveDate::from_ymd_opt(2026, 7, 13).unwrap();
    let mut prior_input: Option<String> = None;
    for index in 0..28_u32 {
        let date = first_date + Duration::days(i64::from(index));
        let input = format!("{:064x}", index + 1);
        let cumulative_net = Decimal::new(125, 2) * Decimal::from(u64::from(index + 1));
        let ending_equity = Decimal::new(5_030_521, 6) + cumulative_net;
        let parent = prior_input
            .as_ref()
            .map(|value| serde_json::json!(format!("sha256:{value}")))
            .unwrap_or(serde_json::Value::Null);
        let wallet = serde_json::json!({
            "schema_version": 2,
            "wallet_scope": "cumulative_since_2026-07-12",
            "campaign_start": "2026-07-12",
            "snapshot_date": date.format("%Y-%m-%d").to_string(),
            "cumulative_input_sha256": format!("sha256:{input}"),
            "cumulative_parent_input_sha256": parent,
            "cumulative_input_manifest_sha256": format!("sha256:{:064x}", 1_000 + index),
            "cumulative_state_sha256": format!("sha256:{:064x}", 2_000 + index),
            "cumulative_regimes_artifact_sha256": format!("sha256:{:064x}", 3_000 + index),
            "cumulative_events": u64::from(index + 1) * 1_000,
            "wallet_constrained": true,
            "wallet_constrained_net_pnl": cumulative_net.to_string(),
            "wallet_constrained_ending_equity": ending_equity.to_string(),
            "wallet_constrained_max_drawdown": "0",
            "wallet_constrained_unresolved_orders": 0
        });
        fs::write(
            source.join("cumulative_wallet.json"),
            serde_json::to_vec_pretty(&wallet).unwrap(),
        )
        .unwrap();
        let date_text = date.format("%Y-%m-%d").to_string();
        fs::write(&audit, complete_daily_audit(&date_text, 1.0)).unwrap();
        publish_daily_directory(
            date,
            format!("shadow-{}", date.format("%Y%m%d")),
            format!("{:064x}", 4_000 + index),
            polyedge_config::RuntimeRole::ProfitabilityShadow,
            &source,
            &daily_root,
            &audit,
        )
        .unwrap();
        prior_input = Some(input);
    }
    let prospective = root.join("reports/research/prospective/prospective_validation.json");
    fs::create_dir_all(prospective.parent().unwrap()).unwrap();
    fs::write(
        &prospective,
        r#"{"result":{"paired_improvement":{"dynamic_quote_style":{"ci_95_low":"0.10"}},"decision_parity_rate":"1.0","markout_30s_ci_low":"0.02"}}"#,
    )
    .unwrap();
    let execution_model = root.join("conservative_execution_prior_v1.json");
    let prior_bytes = br#"{"model_version":"conservative-execution-prior-v1","status":"frozen_conservative_prior","generated_at":"2026-07-12T00:00:00Z","evidence_protocol_version":3,"prediction_policy":"zero_fill_probability_until_authenticated_calibration","sample_size":0,"positive_fills":0,"negative_non_fills":0,"brier_improvement_fraction":0,"expected_calibration_error":1,"promotion_ready":false,"promotion_allowed":false,"funded_execution_allowed":false}"#;
    fs::write(&execution_model, prior_bytes).unwrap();
    let prior_hash = format!("sha256:{:x}", Sha256::digest(prior_bytes));
    let gate = root.join("profitability_gate.yaml");
    fs::write(
        &gate,
        format!(
            r#"candidate:
  name: dynamic_quote_style
  version: dynamic_quote_style@test
  config_hash: sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c
shadow:
  required_clean_days: 2
  maximum_extension_days: 60
  required_settled_markets: 1
  maximum_extension_markets: 2000
  required_positive_weekly_blocks: 1
  minimum_decision_parity_rate: 1.0
  minimum_decision_grade_coverage: 0.95
  maximum_modeled_drawdown: 1.0
  maximum_out_of_order_event_rate: 0.0001
execution_model:
  shadow_prior_model_version: conservative-execution-prior-v1
  shadow_prior_sha256: {prior_hash}
  evidence_protocol_version: 3
  minimum_eligible_orders: 100
  minimum_filled_orders: 10
  minimum_non_filled_orders: 10
  minimum_brier_improvement_over_base_rate: 0.05
  maximum_expected_calibration_error: 0.10
"#
        ),
    )
    .unwrap();
    let out = root.join(DEFAULT_PROFITABILITY_LATEST);
    let manifest = run_evaluate_profitability(ProfitabilityEvaluationOptions {
        daily_root,
        prospective,
        gate_config: gate,
        execution_model,
        out: out.clone(),
        generated_at: Some(Utc::now()),
    })
    .unwrap();
    assert_eq!(
        manifest.phase,
        PromotionPhase::ShadowPassed,
        "{:#?}",
        manifest.gate_metrics
    );
    assert!(manifest.gate_metrics.promotion_allowed);
    assert!(manifest.gate_metrics.metrics.missing_metrics.is_empty());
    assert!(!manifest.promotion_allowed);
    assert!(manifest.human_authorization_required);
    assert!(out.is_file());
}

fn clean_quality() -> DataQualitySummary {
    measured_quality(10, Decimal::ONE, Vec::new(), Vec::new())
}

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

fn ladder_candidate() -> CandidateIdentity {
    CandidateIdentity {
        name: "dynamic_quote_style".to_owned(),
        candidate_version: "dynamic_quote_style@test".to_owned(),
        config_hash: format!("sha256:{}", "a".repeat(64)),
    }
}

fn passing_ladder_metrics(funded_orders: u32) -> FundedLadderMetrics {
    FundedLadderMetrics {
        observed_calendar_days: 1,
        cumulative_eligible_orders: funded_orders,
        cumulative_funded_orders: funded_orders,
        cumulative_net_pnl: Decimal::new(1, 1),
        cumulative_max_drawdown: Decimal::new(1, 1),
        mean_net_markout_30s: Decimal::new(1, 2),
        net_markout_30s_lower_95: Decimal::new(1, 2),
        markout_sample_size: funded_orders,
        data_quality_passed: true,
        unresolved_exposure: Decimal::ZERO,
    }
}

fn stage_grant(
    state: &FundedLadderStateV1,
    id: &str,
    now: chrono::DateTime<Utc>,
) -> FundedStageGrantV1 {
    FundedStageGrantV1 {
        schema_version: "funded_stage_grant_v1".to_owned(),
        grant_id: id.to_owned(),
        source_state_sha256: state.state_sha256().unwrap(),
        candidate: state.candidate.clone(),
        stage_target_orders: state.active_target_orders,
        single_use: true,
        authorized_at: now,
        expires_at: now + Duration::minutes(5),
    }
}

fn ladder_state_with_terminal(now: chrono::DateTime<Utc>) -> FundedLadderStateV1 {
    let mut state = FundedLadderStateV1::new(ladder_candidate(), now).unwrap();
    state.last_verified_terminal_artifact = Some(ImmutableArtifactBindingV1 {
        blob_name: "terminal/test.json".to_owned(),
        sha256: format!("sha256:{}", "a".repeat(64)),
    });
    state.checkpoint_1_protocol_v3_artifact = Some(ImmutableArtifactBindingV1 {
        blob_name: "runs/checkpoint-1/summary.json".to_owned(),
        sha256: format!("sha256:{}", "b".repeat(64)),
    });
    state.checkpoint_1_terminal_artifact = state.last_verified_terminal_artifact.clone();
    state
}

#[test]
fn funded_ladder_is_sequential_and_grants_cannot_be_replayed() {
    let now = Utc::now();
    let state = ladder_state_with_terminal(now);
    assert!(state
        .transition(
            passing_ladder_metrics(2),
            Some(&stage_grant(&state, "skip", now)),
            now
        )
        .is_err());
    let grant = stage_grant(&state, "stage-1", now);
    let stage_five = state
        .transition(passing_ladder_metrics(1), Some(&grant), now)
        .unwrap();
    assert_eq!(stage_five.active_target_orders, 5);
    assert_eq!(stage_five.completed_checkpoints, vec![1]);
    assert_eq!(stage_five.phase, PromotionPhase::LimitedLive);
    assert!(stage_five.human_grant_required);
    assert!(!stage_five.stage_authorized);
    assert!(!stage_five.promotion_allowed);
    assert!(stage_five
        .transition(passing_ladder_metrics(2), Some(&grant), now)
        .is_err());
}

#[test]
fn funded_ladder_stopped_no_go_is_absorbing_and_durable() {
    let root = test_dir("funded_ladder_terminal");
    let path = root.join("state.json");
    let now = Utc::now();
    let state = ladder_state_with_terminal(now);
    write_funded_ladder_state(&path, &state).unwrap();
    let grant = stage_grant(&state, "stage-1-loss", now);
    let mut losing = passing_ladder_metrics(1);
    losing.cumulative_net_pnl = Decimal::new(-1, 1);
    let stopped = state.transition(losing, Some(&grant), now).unwrap();
    assert_eq!(stopped.phase, PromotionPhase::StoppedNoGo);
    assert!(stopped.terminal);
    write_funded_ladder_state(&path, &stopped).unwrap();
    let attempted_resurrection = stopped
        .transition(passing_ladder_metrics(1), None, now + Duration::days(1))
        .unwrap();
    assert_eq!(attempted_resurrection, stopped);
    assert_eq!(write_funded_ladder_state(&path, &state).unwrap(), stopped);
}

#[test]
fn funded_ladder_rejects_forged_early_profitable_go() {
    let now = Utc::now();
    let mut forged = FundedLadderStateV1::new(ladder_candidate(), now).unwrap();
    forged.phase = PromotionPhase::ProfitableGo;
    forged.terminal = true;
    forged.human_grant_required = false;
    assert!(forged.validate().is_err());

    forged.active_stage_index = 4;
    forged.active_target_orders = 200;
    forged.completed_checkpoints = vec![1, 5, 25, 100];
    forged.consumed_grant_ids = vec![
        "canary".to_owned(),
        "stage-5".to_owned(),
        "stage-25".to_owned(),
        "stage-100".to_owned(),
        "stage-200".to_owned(),
    ];
    forged.metrics = passing_ladder_metrics(199);
    assert!(forged.validate().is_err());
    forged.metrics = passing_ladder_metrics(200);
    forged.last_verified_terminal_artifact = Some(ImmutableArtifactBindingV1 {
        blob_name: "terminal/final.json".to_owned(),
        sha256: format!("sha256:{}", "a".repeat(64)),
    });
    forged.checkpoint_1_protocol_v3_artifact = Some(ImmutableArtifactBindingV1 {
        blob_name: "runs/checkpoint-1/summary.json".to_owned(),
        sha256: format!("sha256:{}", "e".repeat(64)),
    });
    forged.checkpoint_1_terminal_artifact = forged.last_verified_terminal_artifact.clone();
    forged.queue_model_transition = Some(QueueModelTransitionV1 {
        schema_version: "queue_model_transition_v1".to_owned(),
        binding: ExecutionModelBinding {
            blob_uri: "azure://storage/models/model.json".to_owned(),
            sha256: format!("sha256:{}", "b".repeat(64)),
            model_version: "queue-calibration-v1".to_owned(),
        },
        generated_at: now - Duration::days(1),
        training_cutoff: now - Duration::days(2),
        training_dataset_sha256: format!("sha256:{}", "c".repeat(64)),
        training_checkpoint_sha256: format!("sha256:{}", "d".repeat(64)),
        model_quality_passed: true,
    });
    forged.holdout_evaluation = Some(FundedHoldoutEvaluationV1 {
        schema_version: "funded_holdout_evaluation_v1".to_owned(),
        exact_order_count: 100,
        label_sample_size: 400,
        filled_order_count: 50,
        non_filled_order_count: 50,
        brier_score: Decimal::new(20, 2),
        naive_base_rate_brier_score: Decimal::new(25, 2),
        brier_improvement_fraction: Decimal::new(20, 2),
        expected_calibration_error: Decimal::new(5, 2),
        markout_sample_size: 50,
        mean_net_markout_30s: Decimal::new(1, 2),
        net_markout_30s_lower_95: Decimal::new(1, 3),
        holdout_net_pnl: Decimal::ONE,
        holdout_max_drawdown: Decimal::new(5, 1),
        mean_holdout_net_pnl_per_order: Decimal::new(1, 2),
        holdout_net_pnl_per_order_lower_95: Decimal::new(1, 3),
        passed: true,
    });
    forged.metrics.markout_sample_size = 10;
    assert!(forged.validate().is_ok());
    let mut losing_holdout = forged;
    losing_holdout
        .holdout_evaluation
        .as_mut()
        .unwrap()
        .holdout_net_pnl = Decimal::NEGATIVE_ONE;
    assert!(losing_holdout.validate().is_err());
}

#[test]
fn exact_stage_block_forces_absorbing_stopped_no_go_without_order_authorization() {
    let root = test_dir("stage_block_terminal_transition");
    let now = Utc::now();
    let checkpoint_one = ladder_state_with_terminal(now);
    let stage_five = checkpoint_one
        .transition(
            passing_ladder_metrics(1),
            Some(&stage_grant(&checkpoint_one, "canary", now)),
            now,
        )
        .unwrap();
    let authorized = stage_five
        .transition(
            passing_ladder_metrics(1),
            Some(&stage_grant(
                &stage_five,
                "stage-5",
                now + Duration::seconds(1),
            )),
            now + Duration::seconds(1),
        )
        .unwrap();
    assert!(authorized.stage_authorized);
    let mut manifest = PromotionManifestV1::new(
        authorized.candidate.clone(),
        PromotionEvaluation::evaluate_shadow(passing_metrics()),
        BTreeMap::new(),
        ExecutionModelBinding {
            blob_uri: "azure://account/models/prior.json".to_owned(),
            sha256: format!("sha256:{}", "a".repeat(64)),
            model_version: "conservative-execution-prior-v1".to_owned(),
        },
        now,
        now + Duration::hours(1),
    )
    .unwrap();
    manifest.phase = PromotionPhase::LimitedLive;
    manifest.funded_ladder = Some(authorized.clone());
    let prior = root.join("prior.json");
    write_promotion_manifest(&prior, &manifest).unwrap();
    let prior_hash = hash_file(&prior);
    let campaign_control_id = format!("{:x}", Sha256::digest(authorized.campaign_id.as_bytes()));
    let block = FundedStageBlockV1 {
        schema: "polyedge.funded_stage_block.v1".to_owned(),
        grant_id: "stage-5".to_owned(),
        campaign_id: authorized.campaign_id.clone(),
        campaign_control_id,
        candidate: authorized.candidate.clone(),
        stage_target_orders: 5,
        source_manifest_sha256: prior_hash.clone(),
        source_state_sha256: authorized.state_sha256().unwrap(),
        decision_id: "decision-1".to_owned(),
        child_run_id: Some("child-1".to_owned()),
        reason: "terminal reconciliation failed".to_owned(),
        blocked_at: now + Duration::seconds(2),
    };
    let block_path = root.join("block.json");
    fs::write(&block_path, serde_json::to_vec_pretty(&block).unwrap()).unwrap();
    let block_hash = hash_file(&block_path);
    let out = root.join("stopped.json");
    let result = stop_funded_manifest_from_stage_block(StopFundedManifestFromStageBlockOptions {
        prior_manifest: prior.clone(),
        prior_manifest_sha256: prior_hash.clone(),
        stage_block: block_path.clone(),
        stage_block_sha256: block_hash,
        out,
        now: now + Duration::seconds(3),
    })
    .unwrap();
    let stopped = result.manifest.funded_ladder.unwrap();
    assert_eq!(stopped.phase, PromotionPhase::StoppedNoGo);
    assert!(stopped.terminal);
    assert!(!stopped.stage_authorized);
    assert!(!stopped.human_grant_required);
    assert!(!stopped.promotion_allowed);

    let mut forged = block;
    forged.source_state_sha256 = format!("sha256:{}", "f".repeat(64));
    let forged_path = root.join("forged-block.json");
    fs::write(&forged_path, serde_json::to_vec_pretty(&forged).unwrap()).unwrap();
    assert!(
        stop_funded_manifest_from_stage_block(StopFundedManifestFromStageBlockOptions {
            prior_manifest: prior,
            prior_manifest_sha256: prior_hash,
            stage_block: forged_path.clone(),
            stage_block_sha256: hash_file(&forged_path),
            out: root.join("forged-output.json"),
            now: now + Duration::seconds(3),
        },)
        .is_err()
    );
}

#[test]
fn exact_expired_campaign_forces_absorbing_stopped_no_go_without_inputs() {
    let root = test_dir("expired_terminal_transition");
    let now = Utc::now();
    let checkpoint_one = ladder_state_with_terminal(now);
    let stage_five = checkpoint_one
        .transition(
            passing_ladder_metrics(1),
            Some(&stage_grant(&checkpoint_one, "canary", now)),
            now,
        )
        .unwrap();
    let mut manifest = PromotionManifestV1::new(
        stage_five.candidate.clone(),
        PromotionEvaluation::evaluate_shadow(passing_metrics()),
        BTreeMap::new(),
        ExecutionModelBinding {
            blob_uri: "azure://account/models/prior.json".to_owned(),
            sha256: format!("sha256:{}", "a".repeat(64)),
            model_version: "conservative-execution-prior-v1".to_owned(),
        },
        now,
        now + Duration::hours(1),
    )
    .unwrap();
    manifest.phase = PromotionPhase::LimitedLive;
    manifest.funded_ladder = Some(stage_five);
    let prior = root.join("prior.json");
    write_promotion_manifest(&prior, &manifest).unwrap();
    let prior_hash = hash_file(&prior);
    assert!(expire_funded_manifest(ExpireFundedManifestOptions {
        prior_manifest: prior.clone(),
        prior_manifest_sha256: prior_hash.clone(),
        out: root.join("too-early.json"),
        now: now + Duration::minutes(59),
    })
    .is_err());
    let result = expire_funded_manifest(ExpireFundedManifestOptions {
        prior_manifest: prior,
        prior_manifest_sha256: prior_hash,
        out: root.join("expired.json"),
        now: now + Duration::hours(1),
    })
    .unwrap();
    let stopped = result.manifest.funded_ladder.unwrap();
    assert_eq!(stopped.phase, PromotionPhase::StoppedNoGo);
    assert!(stopped.terminal);
    assert!(!stopped.stage_authorized);
    assert!(!stopped.human_grant_required);
}

#[test]
fn canonical_manifest_initializes_only_from_hash_bound_protocol_v3_canary() {
    let root = test_dir("canonical_funded_manifest");
    let manifest_path = root.join("latest.json");
    let evidence_path = root.join("canary.json");
    let consumption_path = root.join("consumption.json");
    let terminal_path = root.join("terminal.json");
    let now = Utc::now();
    let manifest = PromotionManifestV1::new(
        ladder_candidate(),
        PromotionEvaluation::evaluate_shadow(passing_metrics()),
        BTreeMap::new(),
        ExecutionModelBinding {
            blob_uri: "azure://account/container/conservative-prior.json".to_owned(),
            sha256: format!("sha256:{}", "c".repeat(64)),
            model_version: "conservative-execution-prior-v1".to_owned(),
        },
        now,
        now + Duration::hours(1),
    )
    .unwrap();
    write_promotion_manifest(&manifest_path, &manifest).unwrap();
    assert_eq!(manifest.phase, PromotionPhase::ShadowPassed);
    assert!(manifest.funded_ladder.is_none());
    let hash_file = |path: &Path| {
        let bytes = fs::read(path).unwrap();
        format!("sha256:{:x}", Sha256::digest(bytes))
    };
    let consumption_blob = "reports/research/venue-probe/control/strategy-canary/human-grants/consumed/canary-grant-1.json";
    let consumption = serde_json::json!({
        "schema": "polyedge.strategy_canary_human_grant_consumption.v1",
        "grant_id": "canary-grant-1",
        "consumption_blob_name": consumption_blob,
        "selected_intent_container_name": "polyedge-shadow-events",
        "selected_intent_blob_name": "reports/research/venue-probe/control/strategy-canary/intents/decision-1.json",
        "selected_intent_sha256": format!("sha256:{}", "1".repeat(64)),
        "promotion_manifest_blob_name": "reports/research/profitability/latest.json",
        "promotion_manifest_container_name": "polyedge-research",
        "promotion_manifest_sha256": format!("sha256:{}", "2".repeat(64)),
        "decision_id": "decision-1"
    });
    fs::write(
        &consumption_path,
        serde_json::to_vec_pretty(&consumption).unwrap(),
    )
    .unwrap();
    let consumption_hash = hash_file(&consumption_path);
    let terminal = serde_json::json!({
        "schema": "polyedge.canary_terminal_risk_portfolio.v1",
        "producer": "polyedge_node_authenticated_risk_terminal",
        "source": "polymarket_data_api_plus_onchain_redemption",
        "run_id": "canary-run-1",
        "probe_id": "probe-1",
        "order_id": "order-1",
        "condition_id": "condition-1",
        "reservation_state": "position_settled",
        "settlement_verified": true,
        "settlement_transaction_hash": format!("0x{}", "a".repeat(64)),
        "polygon_chain_id": 137,
        "transaction_receipt_status": "success",
        "transaction_block_number": 1,
        "transaction_receipt_confirmations": 2,
        "settlement_wallet": "0x1111111111111111111111111111111111111111",
        "redemption_condition_ids": ["condition-1"],
        "trust_boundary_ready": true,
        "portfolio_reconciled": true,
        "reconciliation_discrepancy": "0",
        "zero_open_orders_confirmed": true,
        "unresolved_exposure": "0",
        "unresolved_risk_reservations": 0,
        "campaign_starting_equity": "5.030521",
        "net_external_cash_flows": "0",
        "liquid_collateral": "5.13",
        "summed_position_value": "0",
        "cash_flow_adjusted_ending_equity": "5.13",
        "minimum_observed_equity": "5.13",
        "maximum_observed_equity": "5.13",
        "campaign_cash_flow_ids": [],
        "observed_at": (now + Duration::seconds(32)).to_rfc3339()
    });
    fs::write(
        &terminal_path,
        serde_json::to_vec_pretty(&terminal).unwrap(),
    )
    .unwrap();
    let terminal_hash = hash_file(&terminal_path);
    let model_observations = [1, 5, 30, 60]
        .into_iter()
        .map(|horizon| {
            serde_json::json!({
                "horizon_seconds": horizon,
                "order_submitted": true,
                "eligible": true,
                "label_observed": true,
                "filled": true,
                "quality_eligible": true,
                "reconciliation_complete": true,
                "zero_open_orders_confirmed": true,
                "data_gap_detected": false,
                "cancellation_failure": false,
                "markout_complete": true,
                "markout_timing_valid": true,
                "executable_markout_30s_per_share": "0.06",
                "venue_fee_model": "polymarket_clob_v2_curve",
                "venue_fee_rate": "0",
                "venue_fee_rate_bps": "0",
                "venue_fee_exponent": 1,
                "venue_fee_taker_only": true,
                "entry_fee_per_share": "0",
                "hypothetical_exit_fee_per_share": "0",
                "estimated_round_trip_cost_per_share": "0",
                "inferred_size_ahead": "4",
                "spread": "0.02",
                "order_price": "0.2",
                "order_size": "5",
                "time_to_expiry_seconds": null,
                "pre_send_trade_size": "3",
                "pre_send_depth_changes": 4,
                "pre_send_volatility": "0.01"
            })
        })
        .collect::<Vec<_>>();
    let evidence = serde_json::json!({
        "schema_version": 3,
        "evidence_protocol_version": 3,
        "run_id": "canary-run-1",
        "status": "completed",
        "started_ts": now.to_rfc3339(),
        "finished_ts": (now + Duration::seconds(31)).to_rfc3339(),
        "funder_address": "0x1111111111111111111111111111111111111111",
        "order_submission_attempted": true,
        "order_submitted": true,
        "submitted_order_count": 1,
        "completed_probe_count": 1,
        "candidate": serde_json::to_value(ladder_candidate()).unwrap(),
        "prediction_model": {
            "blob_uri": "azure://account/container/conservative-prior.json",
            "container_name": "container",
            "blob_name": "conservative-prior.json",
            "sha256": format!("sha256:{}", "c".repeat(64)),
            "model_version": "conservative-execution-prior-v1",
            "generated_at": "2026-07-12T00:00:00Z"
        },
        "provenance": {
            "decision_id": "decision-1",
            "human_grant_id": "canary-grant-1",
            "human_grant_consumption_blob_name": consumption_blob,
            "human_grant_consumption_sha256": consumption_hash,
            "authorization_sha256": format!("sha256:{}", "4".repeat(64)),
            "authorization_container_name": "bot-events",
            "intent_container_name": "polyedge-shadow-events",
            "intent_blob_name": "reports/research/venue-probe/control/strategy-canary/intents/decision-1.json",
            "intent_sha256": format!("sha256:{}", "1".repeat(64)),
            "promotion_manifest_blob_name": "reports/research/profitability/latest.json",
            "promotion_manifest_container_name": "polyedge-research",
            "promotion_manifest_sha256": format!("sha256:{}", "2".repeat(64)),
            "terminal_evidence_blob_name": null,
            "terminal_evidence_sha256": null
        },
        "cumulative_net_pnl": "999",
        "data_quality_passed": true,
        "probes": [{
            "schema_version": 3,
            "evidence_protocol_version": 3,
            "probe_id": "probe-1",
            "status": "completed",
            "order_submitted": true,
            "market": {"conditionId": "condition-1", "tokenId": "token-1", "endTs": null},
            "order": {"side": "BUY", "size": "5", "price": "0.2", "spread": "0.02", "inferredSizeAhead": "4"},
            "pre_send_context": {
                "source": "public_market_channel_before_submission",
                "captured_wall_ms": now.timestamp_millis() - 50,
                "observed_trade_count": 2,
                "observed_trade_size": "3",
                "observed_depth_changes": 4,
                "price_volatility": "0.01"
            },
            "lifecycle": {
                "order_id": "order-1",
                "send_wall_ms": now.timestamp_millis(),
                "ack_wall_ms": now.timestamp_millis() + 100,
                "client_to_http_ack_ms": "100",
                "clock_server_minus_local_ms": "0",
                "clock_round_trip_ms": "10",
                "clock_uncertainty_ms": "5",
                "cancel_send_wall_ms": null,
                "client_cancel_round_trip_ms": null,
                "client_to_user_cancel_ack_ms": null,
                "live_duration_ms": "1000",
                "first_fill_after_ack_ms": "500",
                "reconciliation_complete": true,
                "zero_open_orders_confirmed": true,
                "data_gap_detected": false,
                "cancellation_failure": false,
                "actual_matched_size": "5",
                "venue_fee_model": "polymarket_clob_v2_curve",
                "venue_fee_rate": "0",
                "venue_fee_rate_bps": "0",
                "venue_fee_exponent": 1,
                "venue_fee_taker_only": true,
                "estimated_round_trip_cost_per_share": "0",
                "partial_fill": false,
                "fully_filled": true,
                "fill_raced_cancellation": false,
                "post_cancel_fill_count": 0,
                "first_fill_after_cancel_ms": null,
                "public_touch_trade_count": 1,
                "public_strict_trade_through_count": 1,
                "public_trade_through_without_fill_count": 0,
                "related_trade_ids": ["trade-1"],
                "live_user_trade_ids": ["trade-1"],
                "rest_order_matched_size": "5",
                "user_order_matched_size": "5",
                "rest_trade_matched_size": "5",
                "user_trade_matched_size": "5",
                "matched_size_source_agreement": true,
                "trade_id_source_agreement": true,
                "rest_order_returned": true,
                "authenticated_user_channel_reconnects": 0,
                "public_market_channel_reconnects": 0,
                "authenticated_user_channel_unparsed": 0,
                "public_market_channel_unparsed": 0,
                "authenticated_user_channel_duplicates": 0,
                "public_market_channel_duplicates": 0,
                "post_cancel_finality_stable": true,
                "post_cancel_observation_ms": 10000,
                "markout_capture_complete": true
            },
            "markouts": protocol_v3_markout_rows(now),
            "model_observations": model_observations
        }]
    });
    fs::write(
        &evidence_path,
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    let result = initialize_funded_manifest_after_canary(InitializeFundedManifestOptions {
        shadow_manifest: manifest_path.clone(),
        shadow_manifest_sha256: hash_file(&manifest_path),
        canary_evidence: evidence_path.clone(),
        canary_evidence_blob_name:
            "reports/research/venue-probe/runs/2026-07-13/canary/summary.json".to_owned(),
        canary_evidence_sha256: hash_file(&evidence_path),
        human_grant_consumption: consumption_path.clone(),
        human_grant_consumption_sha256: hash_file(&consumption_path),
        terminal_evidence: terminal_path.clone(),
        terminal_evidence_blob_name:
            "reports/research/venue-probe/terminal-risk-portfolio/2026-07-13/probe-1.json"
                .to_owned(),
        terminal_evidence_sha256: terminal_hash,
        out: manifest_path.clone(),
        now: now + Duration::minutes(1),
    })
    .unwrap();
    assert_eq!(result.manifest.phase, PromotionPhase::LimitedLive);
    assert_eq!(
        result.manifest.gate_metrics.phase,
        PromotionPhase::ShadowPassed
    );
    assert_eq!(
        result
            .manifest
            .funded_ladder
            .as_ref()
            .unwrap()
            .active_target_orders,
        5
    );
    let canonical: PromotionManifestV1 =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(canonical.phase, PromotionPhase::LimitedLive);
    assert!(!canonical.promotion_allowed);
    assert_eq!(
        canonical
            .funded_ladder
            .as_ref()
            .unwrap()
            .metrics
            .cumulative_net_pnl,
        Decimal::new(99479, 6)
    );

    let terminal_binding = ImmutableArtifactBindingV1 {
        blob_name: "reports/research/venue-probe/terminal-risk-portfolio/2026-07-13/probe-1.json"
            .to_owned(),
        sha256: hash_file(&terminal_path),
    };
    let assert_rejected = |candidate_evidence: serde_json::Value| {
        assert!(validate_protocol_v3_order_evidence(
            &manifest.candidate,
            &candidate_evidence,
            &terminal,
            &terminal_binding,
        )
        .is_err());
    };

    let mut fee_bearing_no_fill = evidence.clone();
    {
        let provenance = &mut fee_bearing_no_fill["provenance"];
        provenance["terminal_evidence_blob_name"] = serde_json::json!(terminal_binding.blob_name);
        provenance["terminal_evidence_sha256"] = serde_json::json!(terminal_binding.sha256);
        let lifecycle = &mut fee_bearing_no_fill["probes"][0]["lifecycle"];
        lifecycle["actual_matched_size"] = serde_json::json!("0");
        lifecycle["venue_fee_rate"] = serde_json::json!("0.07");
        lifecycle["venue_fee_rate_bps"] = serde_json::json!("700");
        lifecycle["venue_fee_exponent"] = serde_json::json!(1);
        lifecycle["venue_fee_taker_only"] = serde_json::json!(true);
        lifecycle["estimated_round_trip_cost_per_share"] = serde_json::json!("0");
        lifecycle["partial_fill"] = serde_json::json!(false);
        lifecycle["fully_filled"] = serde_json::json!(false);
        lifecycle["first_fill_after_ack_ms"] = serde_json::Value::Null;
        lifecycle["related_trade_ids"] = serde_json::json!([]);
        lifecycle["live_user_trade_ids"] = serde_json::json!([]);
        lifecycle["rest_order_matched_size"] = serde_json::json!("0");
        lifecycle["user_order_matched_size"] = serde_json::json!("0");
        lifecycle["rest_trade_matched_size"] = serde_json::json!("0");
        lifecycle["user_trade_matched_size"] = serde_json::json!("0");
        lifecycle["public_touch_trade_count"] = serde_json::json!(0);
        lifecycle["public_strict_trade_through_count"] = serde_json::json!(0);
        lifecycle["cancel_send_wall_ms"] = serde_json::json!(now.timestamp_millis() + 1_000);
        lifecycle["cancel_http_response_wall_ms"] =
            serde_json::json!(now.timestamp_millis() + 1_100);
        lifecycle["user_channel_cancel_received_wall_ms"] =
            serde_json::json!(now.timestamp_millis() + 1_100);
        lifecycle["client_cancel_round_trip_ms"] = serde_json::json!(100);
        lifecycle["client_to_user_cancel_ack_ms"] = serde_json::json!(100);
    }
    fee_bearing_no_fill["probes"][0]["markouts"] = serde_json::json!([]);
    for row in fee_bearing_no_fill["probes"][0]["model_observations"]
        .as_array_mut()
        .unwrap()
    {
        row["filled"] = serde_json::json!(false);
        row["executable_markout_30s_per_share"] = serde_json::Value::Null;
        row["venue_fee_rate"] = serde_json::json!("0.07");
        row["venue_fee_rate_bps"] = serde_json::json!("700");
        row["venue_fee_exponent"] = serde_json::json!(1);
        row["venue_fee_taker_only"] = serde_json::json!(true);
        row["entry_fee_per_share"] = serde_json::json!("0");
        row["hypothetical_exit_fee_per_share"] = serde_json::json!("0");
        row["estimated_round_trip_cost_per_share"] = serde_json::json!("0");
    }
    let mut no_fill_terminal = terminal.clone();
    no_fill_terminal["source"] = serde_json::json!("authenticated_no_fill");
    no_fill_terminal["reservation_state"] = serde_json::json!("order_cancelled_no_fill");
    no_fill_terminal["settlement_transaction_hash"] = serde_json::Value::Null;
    no_fill_terminal["polygon_chain_id"] = serde_json::Value::Null;
    no_fill_terminal["transaction_receipt_status"] = serde_json::Value::Null;
    no_fill_terminal["transaction_block_number"] = serde_json::Value::Null;
    no_fill_terminal["transaction_receipt_confirmations"] = serde_json::Value::Null;
    no_fill_terminal["settlement_wallet"] = serde_json::Value::Null;
    no_fill_terminal["redemption_condition_ids"] = serde_json::json!([]);
    assert!(validate_protocol_v3_order_evidence(
        &manifest.candidate,
        &fee_bearing_no_fill,
        &no_fill_terminal,
        &terminal_binding,
    )
    .is_ok());

    let mut canceled_fill = evidence.clone();
    canceled_fill["probes"][0]["lifecycle"]["cancel_send_wall_ms"] =
        serde_json::json!(now.timestamp_millis() + 400);
    canceled_fill["probes"][0]["lifecycle"]["cancel_http_response_wall_ms"] =
        serde_json::json!(now.timestamp_millis() + 450);
    canceled_fill["probes"][0]["lifecycle"]["user_channel_cancel_received_wall_ms"] =
        serde_json::json!(now.timestamp_millis() + 470);
    canceled_fill["probes"][0]["lifecycle"]["client_cancel_round_trip_ms"] = serde_json::json!(50);
    canceled_fill["probes"][0]["lifecycle"]["client_to_user_cancel_ack_ms"] = serde_json::json!(70);
    canceled_fill["probes"][0]["lifecycle"]["post_cancel_fill_count"] = serde_json::json!(1);
    canceled_fill["probes"][0]["lifecycle"]["first_fill_after_cancel_ms"] = serde_json::json!(200);
    canceled_fill["probes"][0]["lifecycle"]["fill_raced_cancellation"] = serde_json::json!(true);
    assert!(validate_protocol_v3_order_evidence(
        &manifest.candidate,
        &canceled_fill,
        &terminal,
        &terminal_binding,
    )
    .is_ok());

    let mut forged_source_agreement = evidence.clone();
    forged_source_agreement["probes"][0]["lifecycle"]["rest_trade_matched_size"] =
        serde_json::json!(0);
    assert_rejected(forged_source_agreement);

    let mut missing_horizon = evidence.clone();
    missing_horizon["probes"][0]["model_observations"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert_rejected(missing_horizon);

    let mut producer_only_context = evidence.clone();
    producer_only_context["probes"][0]["pre_send_context"]["captured_wall_ms"] =
        serde_json::json!(now.timestamp_millis() + 1);
    assert_rejected(producer_only_context);

    let mut late_markout = evidence.clone();
    late_markout["probes"][0]["markouts"][0]["observation_delay_ms"] = serde_json::json!(2_001);
    assert_rejected(late_markout);

    let mut unstable_finality = evidence.clone();
    unstable_finality["probes"][0]["lifecycle"]["post_cancel_observation_ms"] =
        serde_json::json!(9_999);
    assert_rejected(unstable_finality);

    let mut hidden_channel_gap = evidence.clone();
    hidden_channel_gap["probes"][0]["lifecycle"]["authenticated_user_channel_unparsed"] =
        serde_json::json!(1);
    assert_rejected(hidden_channel_gap);

    let mut missing_ack_latency = evidence.clone();
    missing_ack_latency["probes"][0]["lifecycle"]["client_to_http_ack_ms"] =
        serde_json::Value::Null;
    assert_rejected(missing_ack_latency);

    let mut missing_cancel_latency = evidence.clone();
    missing_cancel_latency["probes"][0]["lifecycle"]["cancel_send_wall_ms"] =
        serde_json::json!(now.timestamp_millis() + 500);
    assert_rejected(missing_cancel_latency);

    let mut forged_fill_race = evidence.clone();
    forged_fill_race["probes"][0]["lifecycle"]["fill_raced_cancellation"] = serde_json::json!(true);
    assert_rejected(forged_fill_race);

    let mut forged_first_fill = evidence.clone();
    forged_first_fill["probes"][0]["lifecycle"]["first_fill_after_ack_ms"] = serde_json::json!(1);
    assert_rejected(forged_first_fill);

    let mut forged_model_feature = evidence.clone();
    forged_model_feature["probes"][0]["model_observations"][0]["inferred_size_ahead"] =
        serde_json::json!(999);
    assert_rejected(forged_model_feature);

    let mut forged_positive_markout = evidence.clone();
    forged_positive_markout["probes"][0]["markouts"][2]["executable_markout_per_share"] =
        serde_json::json!("0.90");
    assert_rejected(forged_positive_markout);

    let mut inconsistent_fill = evidence.clone();
    inconsistent_fill["probes"][0]["markouts"][1]["fill_price"] = serde_json::json!("0.19");
    inconsistent_fill["probes"][0]["markouts"][1]["midpoint_markout_per_share"] =
        serde_json::json!("0.07");
    inconsistent_fill["probes"][0]["markouts"][1]["executable_markout_per_share"] =
        serde_json::json!("0.06");
    assert_rejected(inconsistent_fill);

    let mut forged_model_markout = evidence.clone();
    forged_model_markout["probes"][0]["model_observations"][0]
        ["executable_markout_30s_per_share"] = serde_json::json!("0.90");
    assert_rejected(forged_model_markout);

    let mut forged_fee_cost = evidence.clone();
    forged_fee_cost["probes"][0]["lifecycle"]["venue_fee_rate_bps"] = serde_json::json!("100");
    forged_fee_cost["probes"][0]["lifecycle"]["estimated_round_trip_cost_per_share"] =
        serde_json::json!("0.001");
    for row in forged_fee_cost["probes"][0]["model_observations"]
        .as_array_mut()
        .unwrap()
    {
        row["venue_fee_rate_bps"] = serde_json::json!("100");
        row["estimated_round_trip_cost_per_share"] = serde_json::json!("0.001");
    }
    assert_rejected(forged_fee_cost);

    let mut v2_fee_curve = evidence.clone();
    {
        let lifecycle = &mut v2_fee_curve["probes"][0]["lifecycle"];
        lifecycle["venue_fee_rate"] = serde_json::json!("0.07");
        lifecycle["venue_fee_rate_bps"] = serde_json::json!("700");
        lifecycle["venue_fee_exponent"] = serde_json::json!(1);
        lifecycle["venue_fee_taker_only"] = serde_json::json!(true);
        lifecycle["estimated_round_trip_cost_per_share"] = serde_json::json!("0.013468");
    }
    for markout in v2_fee_curve["probes"][0]["markouts"]
        .as_array_mut()
        .unwrap()
    {
        let price = markout["executable_price"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap();
        let fee = Decimal::new(7, 2) * price * (Decimal::ONE - price);
        markout["authenticated_fee_rate_bps"] = serde_json::json!("700");
        markout["entry_fee_per_share"] = serde_json::json!("0");
        markout["hypothetical_exit_fee_per_share"] = serde_json::json!(fee.to_string());
        markout["round_trip_fee_per_share"] = serde_json::json!(fee.to_string());
    }
    for row in v2_fee_curve["probes"][0]["model_observations"]
        .as_array_mut()
        .unwrap()
    {
        row["venue_fee_rate"] = serde_json::json!("0.07");
        row["venue_fee_rate_bps"] = serde_json::json!("700");
        row["venue_fee_exponent"] = serde_json::json!(1);
        row["venue_fee_taker_only"] = serde_json::json!(true);
        row["entry_fee_per_share"] = serde_json::json!("0");
        row["hypothetical_exit_fee_per_share"] = serde_json::json!("0.013468");
        row["estimated_round_trip_cost_per_share"] = serde_json::json!("0.013468");
    }
    assert!(validate_protocol_v3_order_evidence(
        &manifest.candidate,
        &v2_fee_curve,
        &terminal,
        &terminal_binding,
    )
    .is_ok());

    let mut forged_maker_fee = v2_fee_curve.clone();
    forged_maker_fee["probes"][0]["markouts"][2]["entry_fee_per_share"] = serde_json::json!("0.01");
    forged_maker_fee["probes"][0]["markouts"][2]["round_trip_fee_per_share"] =
        serde_json::json!("0.023468");
    assert_rejected(forged_maker_fee);

    let mut tiny_nonzero_maker_fee = v2_fee_curve.clone();
    tiny_nonzero_maker_fee["probes"][0]["markouts"][2]["authenticated_fee_amount"] =
        serde_json::json!("0.000000001");
    assert_rejected(tiny_nonzero_maker_fee);

    let mut missing_raw_fee_evidence = evidence.clone();
    missing_raw_fee_evidence["probes"][0]["markouts"][0]
        .as_object_mut()
        .unwrap()
        .remove("authenticated_fee_raw");
    assert_rejected(missing_raw_fee_evidence);

    let mut inconsistent_raw_fee_evidence = evidence.clone();
    inconsistent_raw_fee_evidence["probes"][0]["markouts"][1]["authenticated_fee_raw"] =
        serde_json::json!({"fee_rate_bps":"0"});
    assert_rejected(inconsistent_raw_fee_evidence);

    let mut forged_raw_book = evidence.clone();
    forged_raw_book["probes"][0]["markouts"][2]["raw_orderbook"]["bids"][0]["price"] =
        serde_json::json!("0.99");
    assert_rejected(forged_raw_book);

    let mut self_hashed_false_book = evidence.clone();
    self_hashed_false_book["probes"][0]["markouts"][2]["raw_orderbook"]["bids"][0]["price"] =
        serde_json::json!("0.25");
    let false_book = self_hashed_false_book["probes"][0]["markouts"][2]["raw_orderbook"].clone();
    self_hashed_false_book["probes"][0]["markouts"][2]["book_hash"] =
        serde_json::json!(protocol_v3_book_hash(&false_book));
    assert_rejected(self_hashed_false_book);

    let mut forged_venue_hash = evidence.clone();
    forged_venue_hash["probes"][0]["markouts"][0]["venue_book_hash"] =
        serde_json::json!("different-venue-hash");
    assert_rejected(forged_venue_hash);

    let mut missing_venue_hash = evidence.clone();
    missing_venue_hash["probes"][0]["markouts"][0]["raw_orderbook"]
        .as_object_mut()
        .unwrap()
        .remove("venue_hash");
    missing_venue_hash["probes"][0]["markouts"][0]
        .as_object_mut()
        .unwrap()
        .remove("venue_book_hash");
    assert_rejected(missing_venue_hash);

    let mut null_venue_hash = evidence.clone();
    null_venue_hash["probes"][0]["markouts"][0]["raw_orderbook"]["venue_hash"] =
        serde_json::Value::Null;
    null_venue_hash["probes"][0]["markouts"][0]["venue_book_hash"] = serde_json::Value::Null;
    assert_rejected(null_venue_hash);

    let mut malformed_venue_hash = evidence.clone();
    malformed_venue_hash["probes"][0]["markouts"][0]["raw_orderbook"]["venue_hash"] =
        serde_json::json!("not-a-40-hex-venue-hash");
    malformed_venue_hash["probes"][0]["markouts"][0]["venue_book_hash"] =
        serde_json::json!("not-a-40-hex-venue-hash");
    let malformed_book = malformed_venue_hash["probes"][0]["markouts"][0]["raw_orderbook"].clone();
    malformed_venue_hash["probes"][0]["markouts"][0]["book_hash"] =
        serde_json::json!(protocol_v3_book_hash(&malformed_book));
    assert_rejected(malformed_venue_hash);

    let mut oversized_fill = evidence.clone();
    oversized_fill["probes"][0]["lifecycle"]["actual_matched_size"] = serde_json::json!("6");
    for field in [
        "rest_order_matched_size",
        "user_order_matched_size",
        "rest_trade_matched_size",
        "user_trade_matched_size",
    ] {
        oversized_fill["probes"][0]["lifecycle"][field] = serde_json::json!("6");
    }
    assert_rejected(oversized_fill);

    let mut forged_request_chronology = evidence.clone();
    forged_request_chronology["probes"][0]["markouts"][0]["request_started_at"] =
        serde_json::json!((now + Duration::milliseconds(1599)).to_rfc3339());
    assert_rejected(forged_request_chronology);

    let mut pre_order_fill = evidence.clone();
    let forged_fill_at = now - Duration::milliseconds(10);
    for row in pre_order_fill["probes"][0]["markouts"]
        .as_array_mut()
        .unwrap()
    {
        let horizon = row["horizon_seconds"].as_i64().unwrap();
        let target = forged_fill_at + Duration::seconds(horizon);
        let response = target + Duration::milliseconds(100);
        row["fill_timestamp"] = serde_json::json!(forged_fill_at.to_rfc3339());
        row["venue_fill_timestamp"] = serde_json::json!(forged_fill_at.to_rfc3339());
        row["target_observation_ts"] = serde_json::json!(target.to_rfc3339());
        row["request_started_at"] = serde_json::json!(target.to_rfc3339());
        row["response_completed_at"] = serde_json::json!(response.to_rfc3339());
        row["observed_at"] = serde_json::json!(response.to_rfc3339());
        row["venue_book_timestamp"] = serde_json::json!(response.to_rfc3339());
    }
    assert_rejected(pre_order_fill);

    let mut terminal_before_summary = terminal.clone();
    terminal_before_summary["observed_at"] =
        serde_json::json!((now + Duration::seconds(30)).to_rfc3339());
    assert!(validate_protocol_v3_order_evidence(
        &manifest.candidate,
        &evidence,
        &terminal_before_summary,
        &terminal_binding,
    )
    .is_err());

    let mut single_confirmation = terminal.clone();
    single_confirmation["transaction_receipt_confirmations"] = serde_json::json!(1);
    assert!(validate_protocol_v3_order_evidence(
        &manifest.candidate,
        &evidence,
        &single_confirmation,
        &terminal_binding,
    )
    .is_err());

    let mut wrong_redeemed_condition = terminal.clone();
    wrong_redeemed_condition["redemption_condition_ids"] = serde_json::json!(["condition-2"]);
    assert!(validate_protocol_v3_order_evidence(
        &manifest.candidate,
        &evidence,
        &wrong_redeemed_condition,
        &terminal_binding,
    )
    .is_err());

    let mut duplicate_redeemed_condition = terminal.clone();
    duplicate_redeemed_condition["redemption_condition_ids"] =
        serde_json::json!(["condition-1", "condition-1"]);
    assert!(validate_protocol_v3_order_evidence(
        &manifest.candidate,
        &evidence,
        &duplicate_redeemed_condition,
        &terminal_binding,
    )
    .is_err());

    let mut empty_redeemed_condition = terminal.clone();
    empty_redeemed_condition["redemption_condition_ids"] = serde_json::json!(["condition-1", ""]);
    assert!(validate_protocol_v3_order_evidence(
        &manifest.candidate,
        &evidence,
        &empty_redeemed_condition,
        &terminal_binding,
    )
    .is_err());

    let forged_manifest_path = root.join("forged-manifest.json");
    let forged_evidence_path = root.join("forged-canary.json");
    write_promotion_manifest(&forged_manifest_path, &manifest).unwrap();
    let mut forged_evidence = evidence.clone();
    forged_evidence["candidate"]["config_hash"] =
        serde_json::json!(format!("sha256:{}", "f".repeat(64)));
    fs::write(
        &forged_evidence_path,
        serde_json::to_vec_pretty(&forged_evidence).unwrap(),
    )
    .unwrap();
    assert!(
        initialize_funded_manifest_after_canary(InitializeFundedManifestOptions {
            shadow_manifest: forged_manifest_path.clone(),
            shadow_manifest_sha256: hash_file(&forged_manifest_path),
            canary_evidence: forged_evidence_path.clone(),
            canary_evidence_blob_name:
                "reports/research/venue-probe/runs/2026-07-13/canary/summary.json".to_owned(),
            canary_evidence_sha256: hash_file(&forged_evidence_path),
            human_grant_consumption: consumption_path.clone(),
            human_grant_consumption_sha256: hash_file(&consumption_path),
            terminal_evidence: terminal_path.clone(),
            terminal_evidence_blob_name:
                "reports/research/venue-probe/terminal-risk-portfolio/2026-07-13/probe-1.json"
                    .to_owned(),
            terminal_evidence_sha256: hash_file(&terminal_path),
            out: forged_manifest_path.clone(),
            now: now + Duration::minutes(1),
        })
        .is_err()
    );

    let mismatched_terminal_path = root.join("mismatched-terminal.json");
    let mut mismatched_terminal = terminal.clone();
    mismatched_terminal["order_id"] = serde_json::json!("different-order");
    fs::write(
        &mismatched_terminal_path,
        serde_json::to_vec_pretty(&mismatched_terminal).unwrap(),
    )
    .unwrap();
    write_promotion_manifest(&forged_manifest_path, &manifest).unwrap();
    assert!(
        initialize_funded_manifest_after_canary(InitializeFundedManifestOptions {
            shadow_manifest: forged_manifest_path.clone(),
            shadow_manifest_sha256: hash_file(&forged_manifest_path),
            canary_evidence: evidence_path.clone(),
            canary_evidence_blob_name:
                "reports/research/venue-probe/runs/2026-07-13/canary/summary.json".to_owned(),
            canary_evidence_sha256: hash_file(&evidence_path),
            human_grant_consumption: consumption_path.clone(),
            human_grant_consumption_sha256: hash_file(&consumption_path),
            terminal_evidence: mismatched_terminal_path.clone(),
            terminal_evidence_blob_name:
                "reports/research/venue-probe/terminal-risk-portfolio/2026-07-13/probe-1.json"
                    .to_owned(),
            terminal_evidence_sha256: hash_file(&mismatched_terminal_path),
            out: forged_manifest_path,
            now: now + Duration::minutes(1),
        })
        .is_err()
    );
}

#[test]
fn funded_checkpoint_rejects_forged_parent_bindings_and_progress_chain() {
    #[derive(serde::Serialize)]
    struct ModelBinding<'a> {
        blob_uri: &'a str,
        sha256: &'a str,
        model_version: &'a str,
    }
    #[derive(serde::Serialize)]
    struct ParentBinding<'a> {
        child_run_id: &'a str,
        consumption_blob_name: &'a str,
        consumption_sha256: &'a str,
        authorization_blob_name: &'a str,
        authorization_sha256: &'a str,
        intent_blob_name: &'a str,
        intent_sha256: &'a str,
        manifest_blob_name: &'a str,
        manifest_sha256: &'a str,
        prediction_model: ModelBinding<'a>,
    }
    #[derive(serde::Serialize)]
    struct ChainRoot<'a> {
        schema: &'static str,
        campaign_id: &'a str,
        candidate: &'a CandidateIdentity,
        sequence: u32,
        protocol_v3_summary: &'a ImmutableArtifactBindingV1,
        terminal_risk_portfolio: &'a ImmutableArtifactBindingV1,
    }
    #[derive(serde::Serialize)]
    struct ProgressPayload<'a> {
        schema: &'static str,
        sequence: u32,
        decision_id: &'a str,
        expected_control_binding: &'a ParentBinding<'a>,
        protocol_v3_summary: &'a ImmutableArtifactBindingV1,
        terminal_risk_portfolio: &'a ImmutableArtifactBindingV1,
    }
    #[derive(serde::Serialize)]
    struct Cumulative<'a> {
        prior: &'a str,
        progress: &'a str,
    }

    let root = test_dir("funded_checkpoint_progress_chain");
    let candidate = ladder_candidate();
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 12, 0, 0).unwrap();
    let strict_hash = |value: char| format!("sha256:{}", value.to_string().repeat(64));
    let hash_file = |path: &Path| format!("sha256:{:x}", Sha256::digest(fs::read(path).unwrap()));
    let mut summaries = Vec::new();
    let mut terminals = Vec::new();
    let mut controls = Vec::new();
    for sequence in 1_u32..=5 {
        let run_id = format!("funded-run-{sequence}");
        let probe_id = format!("probe-{sequence}");
        let order_id = format!("order-{sequence}");
        let decision_id = format!("decision-{sequence}");
        let started = now + Duration::minutes(i64::from(sequence));
        let finished = started + Duration::seconds(1);
        let observed = started + Duration::seconds(2);
        let terminal_path = root.join(format!("terminal-{sequence}.json"));
        let ending = Decimal::from(100_u32) + Decimal::new(i64::from(sequence), 2);
        let terminal = serde_json::json!({
            "schema": "polyedge.canary_terminal_risk_portfolio.v1",
            "producer": "polyedge_node_authenticated_risk_terminal",
            "source": "authenticated_no_fill",
            "run_id": run_id,
            "probe_id": probe_id,
            "order_id": order_id,
            "condition_id": "condition-1",
            "reservation_state": "finalized_no_fill",
            "settlement_verified": true,
            "trust_boundary_ready": true,
            "settlement_transaction_hash": null,
            "polygon_chain_id": null,
            "transaction_receipt_status": null,
            "transaction_block_number": null,
            "transaction_receipt_confirmations": null,
            "settlement_wallet": null,
            "redemption_condition_ids": [],
            "portfolio_reconciled": true,
            "reconciliation_discrepancy": "0",
            "zero_open_orders_confirmed": true,
            "unresolved_exposure": "0",
            "unresolved_risk_reservations": 0,
            "campaign_starting_equity": "100",
            "net_external_cash_flows": "0",
            "liquid_collateral": ending.to_string(),
            "summed_position_value": "0",
            "cash_flow_adjusted_ending_equity": ending.to_string(),
            "minimum_observed_equity": ending.to_string(),
            "maximum_observed_equity": ending.to_string(),
            "campaign_cash_flow_ids": [],
            "observed_at": observed.to_rfc3339()
        });
        fs::write(
            &terminal_path,
            serde_json::to_vec_pretty(&terminal).unwrap(),
        )
        .unwrap();
        let terminal_binding = ImmutableArtifactBindingV1 {
            blob_name: terminal_path.to_string_lossy().to_string(),
            sha256: hash_file(&terminal_path),
        };
        let control = serde_json::json!({
            "child_run_id": run_id,
            "consumption_blob_name": format!("control/consumption-{sequence}.json"),
            "consumption_sha256": strict_hash('a'),
            "authorization_blob_name": format!("control/authorization-{sequence}.json"),
            "authorization_sha256": strict_hash('b'),
            "intent_blob_name": format!("control/intent-{sequence}.json"),
            "intent_sha256": strict_hash('c'),
            "manifest_blob_name": format!("control/manifest-{sequence}.json"),
            "manifest_sha256": strict_hash('d'),
            "prediction_model": {
                "blob_uri": "azure://account/container/prior.json",
                "sha256": strict_hash('e'),
                "model_version": "conservative-execution-prior-v1"
            }
        });
        let observations = [1, 5, 30, 60]
            .into_iter()
            .map(|horizon| {
                serde_json::json!({
                    "horizon_seconds": horizon,
                    "order_submitted": true,
                    "eligible": true,
                    "label_observed": true,
                    "filled": false,
                    "quality_eligible": true,
                    "reconciliation_complete": true,
                    "zero_open_orders_confirmed": true,
                    "data_gap_detected": false,
                    "cancellation_failure": false,
                    "markout_complete": true,
                    "markout_timing_valid": true,
                    "executable_markout_30s_per_share": null,
                    "venue_fee_model": "polymarket_clob_v2_curve",
                    "venue_fee_rate": "0",
                    "venue_fee_rate_bps": "0",
                    "venue_fee_exponent": 1,
                    "venue_fee_taker_only": true,
                    "entry_fee_per_share": "0",
                    "hypothetical_exit_fee_per_share": "0",
                    "estimated_round_trip_cost_per_share": "0",
                    "inferred_size_ahead": "4",
                    "spread": "0.02",
                    "order_price": "0.2",
                    "order_size": "5",
                    "time_to_expiry_seconds": null,
                    "pre_send_trade_size": "0",
                    "pre_send_depth_changes": 0,
                    "pre_send_volatility": "0"
                })
            })
            .collect::<Vec<_>>();
        let summary_path = root.join(format!("summary-{sequence}.json"));
        let summary = serde_json::json!({
            "schema_version": 3,
            "evidence_protocol_version": 3,
            "run_id": run_id,
            "status": "completed",
            "started_ts": started.to_rfc3339(),
            "finished_ts": finished.to_rfc3339(),
            "funder_address": "0x1111111111111111111111111111111111111111",
            "order_submission_attempted": true,
            "order_submitted": true,
            "submitted_order_count": 1,
            "completed_probe_count": 1,
            "candidate": candidate,
            "prediction_model": {
                "blob_uri": "azure://account/container/prior.json",
                "container_name": "container",
                "blob_name": "prior.json",
                "sha256": strict_hash('e'),
                "model_version": "conservative-execution-prior-v1",
                "generated_at": "2026-07-12T00:00:00Z",
                "training_data_end_ts": "2026-07-11T00:00:00Z"
            },
            "provenance": {
                "funded_stage_consumption_blob_name": control["consumption_blob_name"],
                "funded_stage_consumption_sha256": control["consumption_sha256"],
                "authorization_blob_name": control["authorization_blob_name"],
                "authorization_sha256": control["authorization_sha256"],
                "intent_blob_name": control["intent_blob_name"],
                "intent_sha256": control["intent_sha256"],
                "promotion_manifest_blob_name": control["manifest_blob_name"],
                "promotion_manifest_sha256": control["manifest_sha256"],
                "terminal_evidence_blob_name": terminal_binding.blob_name,
                "terminal_evidence_sha256": terminal_binding.sha256
            },
            "probes": [{
                "schema_version": 3,
                "evidence_protocol_version": 3,
                "probe_id": probe_id,
                "status": "completed",
                "order_submitted": true,
                "market": {"conditionId":"condition-1","tokenId":"token-1","endTs":null},
                "order": {"side":"BUY","size":"5","price":"0.2","spread":"0.02","inferredSizeAhead":"4"},
                "pre_send_context": {"source":"public_market_channel_before_submission","captured_wall_ms":started.timestamp_millis()-10,"observed_trade_count":0,"observed_trade_size":"0","observed_depth_changes":0,"price_volatility":"0"},
                "lifecycle": {
                    "order_id": order_id,
                    "send_wall_ms": started.timestamp_millis(),
                    "ack_wall_ms": started.timestamp_millis()+100,
                    "client_to_http_ack_ms": "100",
                    "clock_server_minus_local_ms":"0","clock_round_trip_ms":"10","clock_uncertainty_ms":"5",
                    "cancel_send_wall_ms": started.timestamp_millis()+200,
                    "cancel_http_response_wall_ms": started.timestamp_millis()+250,
                    "user_channel_cancel_received_wall_ms": started.timestamp_millis()+270,
                    "client_cancel_round_trip_ms":"50","client_to_user_cancel_ack_ms":"70",
                    "live_duration_ms":"1000","first_fill_after_ack_ms":null,
                    "actual_matched_size":"0","partial_fill":false,"fully_filled":false,
                    "post_cancel_fill_count":0,"first_fill_after_cancel_ms":null,"fill_raced_cancellation":false,
                    "public_touch_trade_count":0,"public_strict_trade_through_count":0,"public_trade_through_without_fill_count":0,
                    "venue_fee_model":"polymarket_clob_v2_curve","venue_fee_rate":"0","venue_fee_rate_bps":"0","venue_fee_exponent":1,"venue_fee_taker_only":true,
                    "estimated_round_trip_cost_per_share":"0",
                    "related_trade_ids":[],"live_user_trade_ids":[],
                    "rest_order_matched_size":"0","user_order_matched_size":"0","rest_trade_matched_size":"0","user_trade_matched_size":"0",
                    "matched_size_source_agreement":true,"trade_id_source_agreement":true,"rest_order_returned":true,
                    "authenticated_user_channel_reconnects":0,"public_market_channel_reconnects":0,
                    "authenticated_user_channel_unparsed":0,"public_market_channel_unparsed":0,
                    "authenticated_user_channel_duplicates":0,"public_market_channel_duplicates":0,
                    "post_cancel_finality_stable":true,"post_cancel_observation_ms":10000,
                    "reconciliation_complete":true,"zero_open_orders_confirmed":true,
                    "data_gap_detected":false,"cancellation_failure":false,"markout_capture_complete":true
                },
                "markouts": [],
                "model_observations": observations
            }]
        });
        fs::write(&summary_path, serde_json::to_vec_pretty(&summary).unwrap()).unwrap();
        summaries.push(ImmutableArtifactBindingV1 {
            blob_name: summary_path.to_string_lossy().to_string(),
            sha256: hash_file(&summary_path),
        });
        terminals.push(terminal_binding);
        controls.push(control);
        let _ = decision_id;
    }

    let mut state = FundedLadderStateV1::new(candidate.clone(), now).unwrap();
    state.phase = PromotionPhase::LimitedLive;
    state.active_stage_index = 1;
    state.active_target_orders = 5;
    state.completed_checkpoints = vec![1];
    state.human_grant_required = true;
    state.stage_authorized = false;
    state.checkpoint_1_protocol_v3_artifact = Some(summaries[0].clone());
    state.checkpoint_1_terminal_artifact = Some(terminals[0].clone());
    state.last_verified_terminal_artifact = Some(terminals[0].clone());

    let chain_root = {
        let value = ChainRoot {
            schema: "polyedge.funded_checkpoint_1_chain_root.v1",
            campaign_id: &state.campaign_id,
            candidate: &state.candidate,
            sequence: 1,
            protocol_v3_summary: &summaries[0],
            terminal_risk_portfolio: &terminals[0],
        };
        format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&value).unwrap())
        )
    };
    let mut cumulative = chain_root.clone();
    let mut progress_artifacts = Vec::new();
    for sequence in 2_u32..=5 {
        let index = sequence as usize - 1;
        let decision_id = format!("decision-{sequence}");
        let parent_value = &controls[index];
        let parent = ParentBinding {
            child_run_id: parent_value["child_run_id"].as_str().unwrap(),
            consumption_blob_name: parent_value["consumption_blob_name"].as_str().unwrap(),
            consumption_sha256: parent_value["consumption_sha256"].as_str().unwrap(),
            authorization_blob_name: parent_value["authorization_blob_name"].as_str().unwrap(),
            authorization_sha256: parent_value["authorization_sha256"].as_str().unwrap(),
            intent_blob_name: parent_value["intent_blob_name"].as_str().unwrap(),
            intent_sha256: parent_value["intent_sha256"].as_str().unwrap(),
            manifest_blob_name: parent_value["manifest_blob_name"].as_str().unwrap(),
            manifest_sha256: parent_value["manifest_sha256"].as_str().unwrap(),
            prediction_model: ModelBinding {
                blob_uri: parent_value["prediction_model"]["blob_uri"]
                    .as_str()
                    .unwrap(),
                sha256: parent_value["prediction_model"]["sha256"].as_str().unwrap(),
                model_version: parent_value["prediction_model"]["model_version"]
                    .as_str()
                    .unwrap(),
            },
        };
        let payload = ProgressPayload {
            schema: "polyedge.funded_stage_progress_payload.v1",
            sequence,
            decision_id: &decision_id,
            expected_control_binding: &parent,
            protocol_v3_summary: &summaries[index],
            terminal_risk_portfolio: &terminals[index],
        };
        let payload_hash = format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&payload).unwrap())
        );
        let next_cumulative = format!(
            "sha256:{:x}",
            Sha256::digest(
                serde_json::to_vec(&Cumulative {
                    prior: &cumulative,
                    progress: &payload_hash,
                })
                .unwrap()
            )
        );
        let progress_path = root.join(format!("progress-{sequence}.json"));
        let progress = serde_json::json!({
            "schema":"polyedge.funded_stage_order_progress.v1",
            "grant_id":"grant-1",
            "campaign_id":state.campaign_id,
            "campaign_control_id":"campaign-control-1",
            "candidate":candidate,
            "stage_target_orders":5,
            "sequence":sequence,
            "decision_id":decision_id,
            "intent_blob_name":controls[index]["intent_blob_name"],
            "intent_sha256":controls[index]["intent_sha256"],
            "authorization_blob_name":controls[index]["authorization_blob_name"],
            "authorization_sha256":controls[index]["authorization_sha256"],
            "child_run_id":controls[index]["child_run_id"],
            "protocol_v3_summary_blob_name":summaries[index].blob_name,
            "protocol_v3_summary_sha256":summaries[index].sha256,
            "terminal_evidence_blob_name":terminals[index].blob_name,
            "terminal_evidence_sha256":terminals[index].sha256,
            "expected_control_binding":controls[index],
            "progress_payload_sha256":payload_hash,
            "prior_cumulative_evidence_sha256":cumulative,
            "cumulative_evidence_sha256":next_cumulative,
            "attempted_order_count":1,"submitted_order_count":1,"eligible_order_count":1,
            "completed_at":(now+Duration::minutes(i64::from(sequence))+Duration::seconds(3)).to_rfc3339()
        });
        fs::write(
            &progress_path,
            serde_json::to_vec_pretty(&progress).unwrap(),
        )
        .unwrap();
        progress_artifacts.push(ImmutableArtifactBindingV1 {
            blob_name: progress_path.to_string_lossy().to_string(),
            sha256: hash_file(&progress_path),
        });
        cumulative = next_cumulative;
    }
    let checkpoint_value = serde_json::json!({
        "schema_version":"funded_checkpoint_evidence_v1",
        "evidence_protocol_version":3,
        "candidate":candidate,
        "source_state_sha256":state.state_sha256().unwrap(),
        "stage_target_orders":5,
        "exact_eligible_order_count":5,
        "exact_funded_order_count":5,
        "observed_calendar_days":1,
        "cumulative_net_pnl":"0.05",
        "cumulative_max_drawdown":"0",
        "mean_net_markout_30s":"0",
        "net_markout_30s_lower_95":"0",
        "markout_sample_size":0,
        "data_quality_passed":true,
        "unresolved_exposure":"0",
        "lifecycle_reconciled":true,
        "checkpoint_1_chain_root_sha256":chain_root,
        "final_cumulative_evidence_sha256":cumulative,
        "protocol_v3_order_artifacts":summaries,
        "terminal_risk_portfolio_artifacts":terminals,
        "progress_artifacts":progress_artifacts,
        "control_bindings":controls
    });
    let checkpoint: FundedCheckpointEvidenceV1 = serde_json::from_value(checkpoint_value).unwrap();
    assert!(checkpoint.validated_metrics(&state).is_ok());

    let mut wrong_parent = checkpoint.clone();
    wrong_parent.control_bindings[2].manifest_sha256 = strict_hash('f');
    assert!(wrong_parent.validated_metrics(&state).is_err());

    let mut wrong_model = checkpoint.clone();
    wrong_model.control_bindings[3].prediction_model.sha256 = strict_hash('f');
    assert!(wrong_model.validated_metrics(&state).is_err());

    let mut wrong_chain = checkpoint.clone();
    wrong_chain.final_cumulative_evidence_sha256 = strict_hash('f');
    assert!(wrong_chain.validated_metrics(&state).is_err());

    let mut reordered_progress = checkpoint.clone();
    reordered_progress.progress_artifacts.swap(0, 1);
    assert!(reordered_progress.validated_metrics(&state).is_err());
}

fn passing_metrics() -> ProfitabilityMetrics {
    ProfitabilityMetrics {
        observed_calendar_days: 30,
        clean_days: 30,
        settled_markets: 1_000,
        wallet_constrained: true,
        queue_conservative: true,
        wallet_constrained_net_pnl: Decimal::ONE,
        wallet_constrained_ending_equity: Decimal::new(6_030_521, 6),
        queue_conservative_net_pnl: Decimal::ONE,
        pnl_ci_95_low: Decimal::new(1, 2),
        consecutive_positive_weekly_blocks: 4,
        max_drawdown: Decimal::new(50, 2),
        drawdown_limit: Decimal::ONE,
        markout_30s_ci_low: Decimal::new(1, 2),
        replay_runtime_parity: true,
        decision_parity_rate: Decimal::ONE,
        execution_model_protocol_version: 3,
        execution_model_eligible_orders: 100,
        execution_model_filled_orders: 10,
        execution_model_non_filled_orders: 90,
        execution_model_brier_improvement: Decimal::new(5, 2),
        execution_model_expected_calibration_error: Decimal::new(10, 2),
        execution_model_promotion_ready: true,
        execution_model_markout_30s_lower_95: Decimal::new(1, 2),
        data_quality: clean_quality(),
        missing_metrics: Vec::new(),
    }
}

fn candidate_registry() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../research/configs/frozen_candidates.yaml")
}

fn hash_file(path: &Path) -> String {
    format!("sha256:{:x}", Sha256::digest(fs::read(path).unwrap()))
}

fn complete_daily_audit(date: &str, coverage: f64) -> Vec<u8> {
    let observed_hours = (0..24)
        .map(|hour| (format!("{date}T{hour:02}"), 100_u64))
        .collect::<BTreeMap<_, _>>();
    let git_sha = current_git_sha();
    let runtime_provenance = serde_json::json!({
        "schema_version": 1,
        "observations": 1440,
        "valid_observations": 1440,
        "invalid_observations": 0,
        "first_timestamp": format!("{date}T00:00:01Z"),
        "last_timestamp": format!("{date}T23:59:59Z"),
        "max_gap_ms": 60000,
        "distinct_identity_count": 1,
        "identities": [valid_runtime_provenance_identity(&git_sha)],
        "invalid_reasons": []
    });
    serde_json::to_vec_pretty(&serde_json::json!({
        "result": {
            "total_events": 1000,
            "start_price_capture_rate": coverage,
            "settlement_rate": coverage,
            "exact_resolution_reference_hour_coverage": coverage,
            "decision_metadata_coverage": coverage,
            "decision_grade_coverage": coverage,
            "final_decision_grade_coverage": coverage,
            "execution_field_coverage": coverage,
            "decision_parity_rate": 1.0,
            "decision_config_sha256": format!("sha256:{}", "d".repeat(64)),
            "fatal_data_quality_issues": [],
            "warnings": [],
            "event_time_ordering_restored": true,
            "out_of_order_timestamps": 0,
            "first_event_timestamp": format!("{date}T00:00:01Z"),
            "last_event_timestamp": format!("{date}T23:59:59Z"),
            "event_count_by_hour": observed_hours,
            "largest_time_gaps": [{"gap_ms": 1000}],
            "runtime_provenance": runtime_provenance
        }
    }))
    .unwrap()
}

fn valid_runtime_provenance_identity(git_sha: &str) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "backend_impl": "rust",
        "git_sha": git_sha,
        "runtime_config_hash": format!("sha256:{}", "a".repeat(64)),
        "app_name": "polyedge-shadow-neu",
        "runtime_role": "profitability_shadow",
        "shadow_only": true,
        "execution_mode": "paper",
        "allow_live": false,
        "enable_taker_orders": false,
        "allow_emergency_account_cancel": false,
        "paper_maker_fill_policy": "none",
        "adaptive_regime_enabled": true,
        "adaptive_regime_mode": "dynamic_quote_style",
        "decision_pipeline_schema": "polyedge.strategy_decision_batch.v3",
        "decision_pipeline_parity_scope": "full_decision_pipeline_recomputation",
        "decision_config_schema": "polyedge.decision_config.v1",
        "decision_config_sha256": format!("sha256:{}", "d".repeat(64)),
        "candidate": {
            "name": "dynamic_quote_style",
            "version": "dynamic_quote_style@2026-06-14",
            "config_hash": "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c"
        },
        "storage_account": "stpolyedgedev",
        "storage_container": "polyedge-shadow-events",
        "event_blob_prefix": "shadow-events/campaign-2026-07-12",
        "publish_strategy_canary_intents": true,
        "execution_model": {
            "version": "conservative-execution-prior-v1",
            "blob_uri": "azure://stpolyedgedev/polyedge-models/conservative-execution-prior-v1.json",
            "sha256": format!("sha256:{}", "b".repeat(64))
        },
        "research_only": true
    })
}

fn complete_execution_quality() -> Vec<u8> {
    serde_json::to_vec_pretty(&serde_json::json!({
        "result": {
            "queue_snapshot_coverage": 1.0,
            "markouts": {
                "1": {"completion_rate": 1.0},
                "5": {"completion_rate": 1.0},
                "30": {
                    "completion_rate": 1.0,
                    "executable": {
                        "count": 10,
                        "mean": "0.02",
                        "sample_std": "0.001",
                        "ci_95_low": "0.019"
                    }
                }
            },
            "warnings": []
        }
    }))
    .unwrap()
}

fn valid_primary_runtime_provenance_identity(git_sha: &str) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "backend_impl": "rust",
        "git_sha": git_sha,
        "runtime_config_hash": format!("sha256:{}", "d".repeat(64)),
        "app_name": "polyedge",
        "runtime_role": "primary",
        "shadow_only": false,
        "execution_mode": "paper",
        "allow_live": false,
        "enable_taker_orders": false,
        "allow_emergency_account_cancel": false,
        "paper_maker_fill_policy": "touch_after_quote_was_live",
        "adaptive_regime_enabled": false,
        "adaptive_regime_mode": "paper_only",
        "candidate": null,
        "storage_account": "stpolyedgedev",
        "storage_container": "bot-events",
        "event_blob_prefix": "events",
        "publish_strategy_canary_intents": false,
        "execution_model": {
            "version": "conservative-execution-prior-v1",
            "blob_uri": "",
            "sha256": ""
        },
        "research_only": true
    })
}

fn current_git_sha() -> String {
    if let Some(value) =
        option_env!("GIT_SHA").filter(|value| polyedge_config::is_full_git_sha(value))
    {
        return value.to_owned();
    }
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("git rev-parse HEAD");
    let value = String::from_utf8(output.stdout)
        .expect("git output")
        .trim()
        .to_ascii_lowercase();
    assert!(polyedge_config::is_full_git_sha(&value));
    value
}

fn test_dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "polyedge-reporting-{name}-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}
