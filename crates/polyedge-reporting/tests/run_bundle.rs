use chrono::{Duration, NaiveDate, TimeZone, Utc};
use polyedge_reporting::research::{
    classify_warning, expire_funded_manifest, initialize_funded_manifest_after_canary,
    inspect_daily_dependency, legacy_daily_fallback_allowed, parse_azure_artifact_uri,
    publish_daily_directory, run_evaluate_profitability, run_validate_prospective,
    stop_funded_manifest_from_stage_block, write_funded_ladder_state, write_promotion_manifest,
    AtomicDailyRun, CandidateIdentity, DailyDependency, DataQualitySummary, ExecutionModelBinding,
    ExpireFundedManifestOptions, FundedHoldoutEvaluationV1, FundedLadderMetrics,
    FundedLadderStateV1, FundedStageBlockV1, FundedStageGrantV1, GateStatus,
    ImmutableArtifactBindingV1, InitializeFundedManifestOptions, ProfitabilityEvaluationOptions,
    ProfitabilityMetrics, PromotionEvaluation, PromotionManifestV1, PromotionPhase,
    ProspectiveValidationOptions, QueueModelTransitionV1, StopFundedManifestFromStageBlockOptions,
    WarningSeverity, DEFAULT_PROFITABILITY_LATEST, WARNING_REGISTRY_VERSION,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

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
    let audit = source.join("data_audit.json");
    fs::write(
        &audit,
        r#"{
          "result": {
            "total_events": 1000,
            "decision_grade_coverage": 0.97,
            "fatal_data_quality_issues": [],
            "warnings": [],
            "event_time_ordering_restored": true,
            "out_of_order_timestamps": 0
          }
        }"#,
    )
    .unwrap();

    let published = publish_daily_directory(
        NaiveDate::from_ymd_opt(2026, 7, 12).unwrap(),
        "daily-20260712",
        "d".repeat(64),
        &source,
        &root.join("reports/research/daily"),
        &audit,
    )
    .unwrap();

    assert_eq!(published.manifest.artifacts.len(), 5);
    assert_eq!(
        published.manifest.data_quality.decision_grade_coverage,
        Decimal::new(97, 2)
    );
    assert!(published.manifest.data_quality.promotion_allowed());
    assert!(published.bundle_dir.join("final_report.json").is_file());
}

#[test]
fn out_of_order_events_require_a_low_measured_rate_and_restored_ordering() {
    let mut high_rate = DataQualitySummary::new(
        1_000,
        Decimal::ONE,
        Vec::new(),
        vec!["42 out-of-order timestamps".to_owned()],
    );
    high_rate.event_time_ordering_restored = true;
    assert!(!high_rate.promotion_allowed());

    let mut low_rate = DataQualitySummary::new(
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
        source.join("cumulative_wallet.json"),
        format!(
            r#"{{"schema_version":1,"wallet_scope":"cumulative_since_2026-07-12","campaign_start":"2026-07-12","snapshot_date":"2026-07-12","cumulative_input_sha256":"sha256:{}","cumulative_state_sha256":"sha256:{}","cumulative_events":1000,"wallet_constrained":true,"wallet_constrained_net_pnl":"1.25","wallet_constrained_ending_equity":"6.280521","wallet_constrained_max_drawdown":"0","wallet_constrained_unresolved_orders":0}}"#,
            "a".repeat(64),
            "b".repeat(64)
        ),
    )
    .unwrap();
    fs::write(
        source.join("execution_quality.json"),
        r#"{"result":{"markouts":{"30":{"executable":{"ci_95_low":"0.02"}}}}}"#,
    )
    .unwrap();
    let audit = source.join("data_audit.json");
    fs::write(
        &audit,
        r#"{"result":{"total_events":1000,"decision_grade_coverage":1.0,"fatal_data_quality_issues":[],"warnings":[]}}"#,
    )
    .unwrap();
    let daily_root = root.join("reports/research/shadow/daily");
    publish_daily_directory(
        NaiveDate::from_ymd_opt(2026, 7, 12).unwrap(),
        "shadow-20260712",
        "e".repeat(64),
        &source,
        &daily_root,
        &audit,
    )
    .unwrap();
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
  required_clean_days: 1
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
    assert_eq!(manifest.phase, PromotionPhase::ShadowPassed);
    assert!(manifest.gate_metrics.promotion_allowed);
    assert!(manifest.gate_metrics.metrics.missing_metrics.is_empty());
    assert!(!manifest.promotion_allowed);
    assert!(manifest.human_authorization_required);
    assert!(out.is_file());
}

fn clean_quality() -> DataQualitySummary {
    DataQualitySummary::new(10, Decimal::ONE, Vec::new(), Vec::<String>::new())
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
        "settlement_transaction_hash": "0xabc",
        "portfolio_reconciled": true,
        "reconciliation_discrepancy": "0",
        "zero_open_orders_confirmed": true,
        "unresolved_exposure": "0",
        "campaign_starting_equity": "5.030521",
        "net_external_cash_flows": "0",
        "liquid_collateral": "5.13",
        "summed_position_value": "0",
        "cash_flow_adjusted_ending_equity": "5.13",
        "minimum_observed_equity": "5.13",
        "maximum_observed_equity": "5.13",
        "campaign_cash_flow_ids": [],
        "observed_at": (now + Duration::seconds(30)).to_rfc3339()
    });
    fs::write(
        &terminal_path,
        serde_json::to_vec_pretty(&terminal).unwrap(),
    )
    .unwrap();
    let terminal_hash = hash_file(&terminal_path);
    let evidence = serde_json::json!({
        "schema_version": 3,
        "evidence_protocol_version": 3,
        "run_id": "canary-run-1",
        "status": "completed",
        "started_ts": now.to_rfc3339(),
        "finished_ts": (now + Duration::seconds(2)).to_rfc3339(),
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
            "probe_id": "probe-1",
            "order_submitted": true,
            "lifecycle": {
                "order_id": "order-1",
                "reconciliation_complete": true,
                "zero_open_orders_confirmed": true,
                "data_gap_detected": false,
                "cancellation_failure": false,
                "actual_matched_size": "5",
                "related_trade_ids": ["trade-1"]
            },
            "markouts": [
                {"fill_id":"trade-1","horizon_seconds":1,"observation_delay_ms":100,"fill_size":"5","midpoint":"0.25","executable_price":"0.24","executable_markout_per_share":"0.04"},
                {"fill_id":"trade-1","horizon_seconds":5,"observation_delay_ms":100,"fill_size":"5","midpoint":"0.26","executable_price":"0.25","executable_markout_per_share":"0.05"},
                {"fill_id":"trade-1","horizon_seconds":30,"observation_delay_ms":100,"fill_size":"5","midpoint":"0.27","executable_price":"0.26","executable_markout_per_share":"0.06"}
            ],
            "model_observations": [{
                "eligible": true,
                "quality_eligible": true,
                "reconciliation_complete": true,
                "zero_open_orders_confirmed": true
            }]
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

fn test_dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "polyedge-reporting-{name}-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}
