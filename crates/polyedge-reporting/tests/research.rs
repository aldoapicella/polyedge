use polyedge_reporting::research::{
    load_default_exclusions, load_frozen_candidate_registry, run_audit, run_backfill, run_baseline,
    run_build_markets, run_calibration, run_chart_backfill, run_final_report, run_normalize,
    run_queue_audit, run_regimes, run_replay, run_sample_size, run_sweep, run_validate_prospective,
    AuditOptions, BackfillOptions, BaselineOptions, BuildMarketsOptions, CalibrationOptions,
    ChartBackfillOptions, ExcludedTimeWindow, FillModel, FinalReportOptions, NormalizeOptions,
    ProspectiveValidationOptions, QueueAuditOptions, RegimesOptions, ReplayOptions,
    SampleSizeOptions, SweepOptions,
};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn audit_counts_fixture_and_malformed_lines() {
    let dir = test_dir("audit");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!("{}\nnot-json\n", market_line("m1", "up", "down")),
    );

    let report = run_audit(AuditOptions {
        input: events,
        out: dir.join("data_audit.json"),
        markdown: dir.join("data_audit.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();

    assert_eq!(report["result"]["markets_seen"], 1);
    assert_eq!(report["result"]["malformed_lines"], 1);
}

#[test]
fn exclusion_registry_loads_put_bug_window_by_default() {
    let dir = test_dir("exclusion_registry");
    let registry = dir.join("exclusion_windows.yaml");
    fs::write(
        &registry,
        r#"version: 1
updated_at: "2026-06-14T00:00:00Z"
windows:
  - id: "azure-put-bug-2026-06-11"
    start: "2026-06-11T10:00:00Z"
    end: "2026-06-12T22:00:00Z"
    reason: "Azure PUT bug: tiny/incomplete blobs"
    evidence:
      - "events/2026/06/11/11 had mostly tiny blobs"
    default_exclude: true
"#,
    )
    .unwrap();

    let windows = load_default_exclusions(&registry).unwrap();

    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].start.to_rfc3339(), "2026-06-11T10:00:00+00:00");
    assert_eq!(windows[0].end.to_rfc3339(), "2026-06-12T22:00:00+00:00");
}

#[test]
fn frozen_candidates_must_stay_disabled_and_research_only() {
    let dir = test_dir("frozen_candidates");
    let candidates = dir.join("frozen_candidates.yaml");
    fs::write(&candidates, frozen_candidates_yaml()).unwrap();

    let registry = load_frozen_candidate_registry(&candidates).unwrap();

    assert_eq!(registry.candidates.len(), 4);
    assert!(registry
        .candidates
        .iter()
        .all(|candidate| { !candidate.enabled_by_default && !candidate.deployment_allowed }));
    assert!(registry
        .candidates
        .iter()
        .all(|candidate| !candidate.candidate_version.is_empty()
            && !candidate.config_hash.is_empty()
            && !candidate.reason.is_empty()));
}

#[test]
fn prospective_and_backfill_reports_keep_research_safety_flags() {
    let dir = test_dir("prospective_backfill");
    let candidates = dir.join("frozen_candidates.yaml");
    fs::write(&candidates, frozen_candidates_yaml()).unwrap();

    let prospective = run_validate_prospective(ProspectiveValidationOptions {
        since: chrono::DateTime::parse_from_rfc3339("2026-06-14T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        reports_dir: dir.join("daily"),
        candidates,
        out: dir.join("prospective.json"),
        markdown: dir.join("prospective.md"),
    })
    .unwrap();

    assert_eq!(prospective["result"]["research_only"], true);
    assert_eq!(prospective["result"]["live_deployment_allowed"], false);
    assert_eq!(prospective["result"]["status"], "collecting");
    assert_eq!(
        prospective["result"]["frozen_candidates"]["candidates"][0]["candidate_version"],
        "static_baseline@2026-06-14"
    );
    assert_eq!(
        prospective["result"]["paired_improvement"]["dynamic_quote_style"]["sample_size"],
        0
    );
    assert_eq!(
        prospective["result"]["paired_improvement"]["dynamic_quote_style"]
            ["live_deployment_allowed"],
        false
    );

    let backfill = run_backfill(BackfillOptions {
        start: "2026-06-14".to_owned(),
        end: "2026-06-14".to_owned(),
        task: "reports".to_owned(),
        exclude_windows: Vec::new(),
        out: dir.join("backfill.json"),
        markdown: dir.join("backfill.md"),
    })
    .unwrap();

    assert_eq!(backfill["result"]["raw_data_mutated"], false);
    assert_eq!(backfill["result"]["live_trading_enabled"], false);
}

#[test]
fn exclude_window_skips_events_and_prevents_contaminated_fills() {
    let dir = test_dir("exclude_window");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            decision_line("m1", "up", "up", "2026-06-11T10:01:00+00:00"),
            book_line("up", "0.50", "2026-06-11T10:01:01+00:00"),
            reference_line("101", "2026-06-11T10:15:01+00:00")
        ),
    );
    let excluded =
        vec![ExcludedTimeWindow::parse("2026-06-11T10:00:00Z..2026-06-11T11:00:00Z").unwrap()];

    let audit = run_audit(AuditOptions {
        input: events.clone(),
        out: dir.join("audit.json"),
        markdown: dir.join("audit.md"),
        exclude_windows: excluded.clone(),
    })
    .unwrap();
    assert_eq!(audit["result"]["total_events"], 1);
    assert_eq!(audit["result"]["excluded_event_count"], 3);

    let replay = run_replay(ReplayOptions {
        input: events,
        markets: None,
        strategy_config: None,
        fill_model: FillModel::Touch,
        out: dir.join("replay.json"),
        markdown: dir.join("replay.md"),
        exclude_windows: excluded,
    })
    .unwrap();
    assert_eq!(replay["result"]["fills"], 0);
    assert_eq!(replay["result"]["excluded_event_count"], 3);
    assert!(replay["result"]["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|warning| warning
            .as_str()
            .is_some_and(|text| text.contains("events skipped by 1 excluded"))));
}

#[test]
fn normalize_and_build_markets_preserve_incomplete_markets() {
    let dir = test_dir("normalize_markets");
    let raw = dir.join("raw.jsonl");
    write_events(&raw, &market_line("m1", "up", "down"));
    let normalized = dir.join("normalized");

    run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed".to_owned(),
        overwrite: false,
    })
    .unwrap();
    let report = run_build_markets(BuildMarketsOptions {
        input: normalized,
        out: dir.join("markets.json"),
        markdown: dir.join("markets.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();

    assert_eq!(report["result"]["summary"]["markets"], 1);
    assert_eq!(report["result"]["summary"]["complete_for_simulation"], 0);
    assert!(report["result"]["markets"][0]["data_quality_flags"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "missing_final_price"));
}

#[test]
fn normalize_writes_queue_evidence_and_queue_audit_marks_eligibility() {
    let dir = test_dir("queue_audit");
    let raw = dir.join("raw.jsonl");
    write_events(
        &raw,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            bid_book_line("up", "0.50", "5", "2026-06-01T00:00:30+00:00"),
            raw_price_change_line("up", "0.50", "5", "2026-06-01T00:00:45+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line("up", "0.50", "10", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    let normalized = dir.join("normalized");

    let manifest = run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed".to_owned(),
        overwrite: false,
    })
    .unwrap();

    assert!(normalized.join("book_snapshots.jsonl").is_file());
    assert!(normalized.join("price_changes.jsonl").is_file());
    assert!(normalized.join("last_trades.jsonl").is_file());
    assert_eq!(manifest["result"]["files"]["book_snapshot"]["rows"], 1);
    assert_eq!(manifest["result"]["files"]["price_change"]["rows"], 1);
    assert_eq!(manifest["result"]["files"]["last_trade"]["rows"], 1);

    let markets_path = dir.join("markets.json");
    run_build_markets(BuildMarketsOptions {
        input: normalized.clone(),
        out: markets_path.clone(),
        markdown: dir.join("markets.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let audit = run_queue_audit(QueueAuditOptions {
        input: normalized,
        markets: markets_path,
        out: dir.join("queue_audit.json"),
        markdown: dir.join("queue_audit.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();

    assert_eq!(audit["result"]["queue_proxy_eligible_markets"], 1);
    assert_eq!(audit["result"]["queue_proxy_ineligible_markets"], 0);
    assert_eq!(audit["result"]["events_by_market"]["m1"]["eligible"], true);
    assert_eq!(audit["result"]["live_trading_enabled"], false);
}

#[test]
fn chart_backfill_writes_read_only_chart_artifact() {
    let dir = test_dir("chart_backfill");
    let raw = dir.join("raw.jsonl");
    write_events(
        &raw,
        &format!(
            "{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            fair_value_line("m1", "0.60", "2026-06-01T00:00:30+00:00"),
            book_line("up", "0.50", "2026-06-01T00:00:45+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            execution_report_line("m1", "up", "paper_filled", "2026-06-01T00:01:03+00:00")
        ),
    );
    let normalized = dir.join("normalized");
    run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed".to_owned(),
        overwrite: false,
    })
    .unwrap();

    let report = run_chart_backfill(ChartBackfillOptions {
        input: normalized,
        out: dir.join("chart-backfill.json"),
        markdown: dir.join("chart-backfill.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();

    assert_eq!(report["result"]["status"], "completed");
    assert_eq!(report["result"]["job_id"], "chart-backfill");
    assert_eq!(report["result"]["raw_data_mutated"], false);
    assert_eq!(report["result"]["live_trading_enabled"], false);
    assert_eq!(report["result"]["chart_store"]["market_count"], 1);
    assert_eq!(report["result"]["chart_store"]["decision_marker_count"], 1);
    assert_eq!(report["result"]["chart_store"]["fill_marker_count"], 1);
    assert!(report["result"]["markets"][0]["points"]
        .as_array()
        .is_some_and(|points| points.len() >= 2));
    assert!(dir.join("chart-backfill.md").is_file());
}

#[test]
fn gzip_normalized_outputs_feed_build_markets_and_replay() {
    let dir = test_dir("gzip_normalized");
    let raw = dir.join("raw.jsonl");
    write_events(&raw, &filled_touch_fixture("2026-06-01T00:01:01+00:00"));
    let normalized = dir.join("normalized");

    let manifest = run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed-gzip".to_owned(),
        overwrite: false,
    })
    .unwrap();

    assert_eq!(manifest["result"]["compression"], "gzip");
    assert!(normalized.join("events.jsonl.gz").is_file());
    assert!(!normalized.join("events.jsonl").exists());

    let markets_path = dir.join("markets.json");
    let markets = run_build_markets(BuildMarketsOptions {
        input: normalized.clone(),
        out: markets_path.clone(),
        markdown: dir.join("markets.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    assert_eq!(markets["result"]["summary"]["complete_for_simulation"], 1);

    let replay = run_replay(ReplayOptions {
        input: normalized,
        markets: Some(markets_path),
        strategy_config: None,
        fill_model: FillModel::Touch,
        out: dir.join("replay.json"),
        markdown: dir.join("replay.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    assert_eq!(replay["result"]["fills"], 1);
}

#[test]
fn sharded_gzip_normalized_outputs_merge_by_event_time_for_replay() {
    let dir = test_dir("sharded_gzip_normalized");
    let raw = dir.join("raw.jsonl");
    write_events(&raw, &filled_touch_fixture("2026-06-01T00:01:01+00:00"));
    let normalized = dir.join("normalized");

    let manifest = run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed-gzip-sharded".to_owned(),
        overwrite: false,
    })
    .unwrap();

    assert_eq!(manifest["result"]["format"], "jsonl-indexed-gzip-sharded");
    assert_eq!(manifest["result"]["event_log_written"], false);
    assert!(!normalized.join("events.jsonl.gz").exists());
    assert!(normalized.join("markets.jsonl.gz").is_file());
    assert!(normalized.join("books.jsonl.gz").is_file());
    let progress: Value = serde_json::from_str(
        &fs::read_to_string(normalized.join("normalize_progress.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(progress["status"], "completed");
    assert_eq!(progress["events"], 4);

    let markets_path = dir.join("markets.json");
    let markets = run_build_markets(BuildMarketsOptions {
        input: normalized.clone(),
        out: markets_path.clone(),
        markdown: dir.join("markets.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    assert_eq!(markets["result"]["summary"]["complete_for_simulation"], 1);

    let replay = run_replay(ReplayOptions {
        input: normalized,
        markets: Some(markets_path),
        strategy_config: None,
        fill_model: FillModel::Touch,
        out: dir.join("replay.json"),
        markdown: dir.join("replay.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    assert_eq!(replay["result"]["fills"], 1);
}

#[test]
fn sharded_gzip_reader_reorders_local_shard_timestamp_inversions() {
    let dir = test_dir("sharded_gzip_local_reorder");
    let raw = dir.join("raw.jsonl");
    write_events(
        &raw,
        &format!(
            "{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            book_line("up", "0.55", "2026-06-01T00:05:00+00:00"),
            book_line("up", "0.50", "2026-06-01T00:01:00+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    let normalized = dir.join("normalized");

    run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed-gzip-sharded".to_owned(),
        overwrite: false,
    })
    .unwrap();

    let audit = run_audit(AuditOptions {
        input: normalized,
        out: dir.join("audit.json"),
        markdown: dir.join("audit.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();

    assert_eq!(audit["result"]["out_of_order_timestamps"], 0);
}

#[test]
fn normalize_rejects_unknown_format_without_removing_output() {
    let dir = test_dir("normalize_bad_format");
    let raw = dir.join("raw.jsonl");
    write_events(&raw, &market_line("m1", "up", "down"));
    let normalized = dir.join("normalized");
    fs::create_dir_all(&normalized).unwrap();
    fs::write(normalized.join("keep.txt"), "do not remove").unwrap();

    let error = run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "parquet-ish".to_owned(),
        overwrite: true,
    })
    .unwrap_err()
    .to_string();

    assert!(error.contains("unsupported normalize format"));
    assert_eq!(
        fs::read_to_string(normalized.join("keep.txt")).unwrap(),
        "do not remove"
    );
}

#[test]
fn touch_fills_but_trade_through_requires_one_tick_better() {
    let dir = test_dir("touch_vs_trade_through");
    let events = dir.join("events.jsonl");
    write_events(&events, &filled_touch_fixture("2026-06-01T00:01:01+00:00"));

    let touch = replay(&dir, &events, FillModel::Touch);
    let trade_through = replay(&dir, &events, FillModel::TradeThrough);

    assert_eq!(touch["result"]["fills"], 1);
    assert_eq!(touch["result"]["fees"], "0");
    assert_eq!(trade_through["result"]["fills"], 0);
}

#[test]
fn replay_prevents_fill_after_cancel_close_and_final_window() {
    let dir = test_dir("fill_guards");
    let cancelled = dir.join("cancelled.jsonl");
    write_events(
        &cancelled,
        &format!(
            "{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            cancel_line("m1", "2026-06-01T00:01:01+00:00"),
            book_line("up", "0.50", "2026-06-01T00:01:02+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    assert_eq!(
        replay(&dir, &cancelled, FillModel::Touch)["result"]["fills"],
        0
    );

    let final_window = dir.join("final_window.jsonl");
    write_events(
        &final_window,
        &format!(
            "{}\n{}\n{}\n{}",
            market_line("m2", "up2", "down2"),
            decision_line("m2", "up2", "up", "2026-06-01T00:14:20+00:00"),
            book_line("up2", "0.50", "2026-06-01T00:14:40+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    assert_eq!(
        replay(&dir, &final_window, FillModel::Touch)["result"]["fills"],
        0
    );

    let closed = dir.join("closed.jsonl");
    write_events(
        &closed,
        &format!(
            "{}\n{}\n{}\n{}",
            market_line("m3", "up3", "down3"),
            decision_line("m3", "up3", "up", "2026-06-01T00:01:00+00:00"),
            book_line("up3", "0.50", "2026-06-01T00:15:01+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    assert_eq!(
        replay(&dir, &closed, FillModel::Touch)["result"]["fills"],
        0
    );
}

#[test]
fn baseline_calibration_sample_size_sweep_and_final_report_generate_outputs() {
    let dir = test_dir("full_flow");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            fair_value_line("m1", "0.60", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            book_line("up", "0.50", "2026-06-01T00:01:01+00:00"),
            book_line("down", "0.50", "2026-06-01T00:01:01+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    let reports = dir.join("reports");

    run_audit(AuditOptions {
        input: events.clone(),
        out: reports.join("data_audit.json"),
        markdown: reports.join("data_audit.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let baseline = run_baseline(BaselineOptions {
        input: events.clone(),
        markets: None,
        out: reports.join("baseline.json"),
        markdown: reports.join("baseline.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let calibration = run_calibration(CalibrationOptions {
        input: events.clone(),
        markets: None,
        out: reports.join("calibration.json"),
        markdown: reports.join("calibration.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let sweep = run_sweep(SweepOptions {
        input: events.clone(),
        markets: None,
        search: None,
        split: "walk_forward".to_owned(),
        max_experiments: 2,
        out: reports.join("parameter_sweep.json"),
        markdown: reports.join("parameter_sweep.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let sample = run_sample_size(SampleSizeOptions {
        results: reports.join("baseline.json"),
        out: reports.join("sample_size.json"),
        markdown: reports.join("sample_size.md"),
    })
    .unwrap();
    let final_report = run_final_report(FinalReportOptions {
        reports_dir: reports.clone(),
        out: reports.join("final_strategy_research_report.json"),
        markdown: reports.join("final_strategy_research_report.md"),
    })
    .unwrap();

    assert!(baseline["result"]["fill_models"].as_array().unwrap().len() >= 6);
    assert_eq!(
        calibration["result"]["q_up_buckets"]["0.60-0.70"]["decision_count"],
        1
    );
    assert_eq!(
        sweep["result"]["split_plan"]["no_future_leakage_rule"],
        "training days must be strictly earlier than validation/test days"
    );
    assert_eq!(sample["result"]["statistics"]["n"], 1);
    assert_eq!(
        final_report["result"]["executive_summary"]["live_trading_enabled"],
        false
    );
    let daily_root = dir.join("daily");
    let daily_dir = daily_root.join("2026-06-01");
    fs::create_dir_all(&daily_dir).unwrap();
    fs::copy(
        reports.join("data_audit.json"),
        daily_dir.join("data_audit.json"),
    )
    .unwrap();
    fs::copy(
        reports.join("baseline.json"),
        daily_dir.join("baseline.json"),
    )
    .unwrap();
    fs::copy(
        reports.join("sample_size.json"),
        daily_dir.join("sample_size.json"),
    )
    .unwrap();
    fs::copy(
        reports.join("final_strategy_research_report.json"),
        daily_dir.join("final_report.json"),
    )
    .unwrap();
    let candidates = dir.join("frozen_candidates.yaml");
    fs::write(&candidates, frozen_candidates_yaml()).unwrap();
    let prospective = run_validate_prospective(ProspectiveValidationOptions {
        since: chrono::DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        reports_dir: daily_root,
        candidates,
        out: reports.join("prospective_validation.json"),
        markdown: reports.join("prospective_validation.md"),
    })
    .unwrap();
    assert_eq!(prospective["result"]["status"], "tracking");
    assert_eq!(prospective["result"]["rows"][0]["settled_markets"], 1);
    let expected_static = baseline["result"]["fill_models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["fill_model"].as_str() == Some("touch_after_250ms"))
        .unwrap()["net_pnl"]
        .clone();
    assert_eq!(
        prospective["result"]["rows"][0]["static_net_pnl"],
        expected_static
    );
    assert_ne!(
        prospective["result"]["rows"][0]["static_net_pnl"],
        Value::String("0".to_owned())
    );
    assert_eq!(
        prospective["result"]["rows"][0]["ci_95_low"],
        sample["result"]["statistics"]["ci_low"]
    );
    assert!(prospective["result"]["rows"][0]
        .as_object()
        .unwrap()
        .contains_key("dynamic_quote_style_paired_delta"));
    assert!(prospective["result"]["rows"][0]
        .as_object()
        .unwrap()
        .contains_key("dynamic_quote_style_decision_gate"));
    assert_eq!(
        prospective["result"]["paired_improvement"]["dynamic_quote_style"]["research_only"],
        true
    );
    assert!(!serde_json::to_string(&final_report)
        .unwrap()
        .contains("secret-token"));
}

#[test]
fn queue_proxy_remains_skipped_without_validated_depletion_semantics() {
    let dir = test_dir("queue_proxy");
    let missing_evidence = dir.join("missing_evidence.jsonl");
    write_events(
        &missing_evidence,
        &filled_touch_fixture("2026-06-01T00:01:01+00:00"),
    );
    let missing = replay(&dir, &missing_evidence, FillModel::QueueProxy);

    assert_eq!(missing["result"]["fills"], 0);
    assert_eq!(
        missing["result"]["replay_metrics"]["queue_proxy"]["status"],
        "skipped_missing_queue_depletion_trade_evidence"
    );
    assert_eq!(
        missing["result"]["replay_metrics"]["queue_proxy"]["evidence_complete"],
        false
    );

    let with_evidence = dir.join("with_evidence.jsonl");
    write_events(
        &with_evidence,
        &format!(
            "{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            queue_evidence_book_line("up", "0.50", "2026-06-01T00:01:01+00:00"),
            book_line("up", "0.50", "2026-06-01T00:01:02+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    let present = replay(&dir, &with_evidence, FillModel::QueueProxy);

    assert_eq!(present["result"]["fills"], 0);
    assert_eq!(
        present["result"]["replay_metrics"]["queue_proxy"]["status"],
        "skipped_missing_queue_depletion_trade_evidence"
    );
    assert_eq!(
        present["result"]["replay_metrics"]["queue_proxy"]["evidence_complete"],
        false
    );
}

#[test]
fn queue_proxy_conservative_requires_trade_prints_to_cross_size_ahead() {
    let dir = test_dir("queue_proxy_conservative");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            bid_book_line("up", "0.50", "5", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line("up", "0.50", "10", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["fills"], 1);
    assert_eq!(report["result"]["maker_fills"], 1);
    assert_eq!(report["result"]["queue_proxy_enabled"], true);
    assert_eq!(report["result"]["queue_proxy_mode"], "conservative");
    assert_eq!(report["result"]["avg_size_ahead"], "5");
}

#[test]
fn queue_proxy_uses_raw_price_change_size_for_size_ahead() {
    let dir = test_dir("queue_proxy_price_change_size");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            bid_book_no_level_line("up", "0.49", "10", "2026-06-01T00:00:30+00:00"),
            raw_price_change_line("up", "0.50", "5", "2026-06-01T00:00:45+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line("up", "0.50", "10", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["fills"], 1);
    assert_eq!(report["result"]["queue_proxy_enabled"], true);
    assert_eq!(report["result"]["avg_size_ahead"], "5");
}

#[test]
fn queue_proxy_refuses_market_without_level_evidence() {
    let dir = test_dir("queue_proxy_missing_level");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            bid_book_no_level_line("up", "0.50", "5", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line("up", "0.50", "10", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["fills"], 0);
    assert_eq!(report["result"]["queue_proxy_enabled"], false);
    assert_eq!(
        report["result"]["replay_metrics"]["queue_proxy"]["ineligible_reasons"]
            ["missing_price_change_or_level_update"],
        1
    );
}

#[test]
fn queue_proxy_allows_multiple_trade_prints_to_complete_partial_fill() {
    let dir = test_dir("queue_proxy_multi_print");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            bid_book_line("up", "0.50", "5", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line("up", "0.50", "7", "2026-06-01T00:01:03+00:00"),
            trade_line("up", "0.50", "3", "2026-06-01T00:01:04+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["fills"], 2);
    assert_eq!(report["result"]["maker_fills"], 2);
    assert_eq!(report["result"]["queue_proxy_partial_fills"], 1);
    assert_eq!(report["result"]["market_results"][0]["filled_orders"], 1);
}

#[test]
fn queue_proxy_balanced_allows_level_decrease_to_reduce_size_ahead_but_not_fill() {
    let dir = test_dir("queue_proxy_balanced");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            bid_book_line("up", "0.50", "5", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            bid_book_line("up", "0.50", "1", "2026-06-01T00:01:02+00:00"),
            trade_line("up", "0.50", "5", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let conservative = replay(&dir, &events, FillModel::QueueProxyConservative);
    let balanced = replay(&dir, &events, FillModel::QueueProxyBalanced);

    assert_eq!(conservative["result"]["fills"], 0);
    assert_eq!(balanced["result"]["fills"], 1);
    assert_eq!(balanced["result"]["queue_proxy_mode"], "balanced");
    assert_eq!(balanced["result"]["queue_proxy_partial_fills"], 1);
}

#[test]
fn queue_proxy_refuses_market_without_size_ahead_book() {
    let dir = test_dir("queue_proxy_ineligible");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line("up", "0.50", "10", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["fills"], 0);
    assert_eq!(
        report["result"]["queue_proxy_ineligible_markets"],
        serde_json::json!(1)
    );
    assert_eq!(
        report["result"]["replay_metrics"]["queue_proxy"]["ineligible_reasons"]
            ["missing_book_snapshot_at_order_live_ts"],
        serde_json::json!(1)
    );
}

#[test]
fn future_settlement_reference_is_not_a_decision_time_feature() {
    let dir = test_dir("no_future_leakage");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            reference_line("200", "2026-06-01T00:15:01+00:00"),
            book_line("up", "0.50", "2026-06-01T00:00:30+00:00"),
            book_line("down", "0.50", "2026-06-01T00:00:30+00:00"),
            fair_value_line("m1", "0.60", "2026-06-01T00:00:45+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00")
        ),
    );

    let report = run_regimes(RegimesOptions {
        input: events,
        markets: None,
        fill_model: FillModel::Touch,
        profile_config: None,
        out: dir.join("regimes.json"),
        markdown: dir.join("regimes.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let profiles = report["result"]["profiles"].as_array().unwrap();
    let full = profiles
        .iter()
        .find(|profile| profile["profile"] == "full_deterministic_profile")
        .unwrap();
    let log = &full["adaptive_decision_log_sample"][0]["features_summary"];

    assert!(log["distance_bps"].is_null());
    assert!(log["reference_age_ms"].is_null());
    let serialized_log = serde_json::to_string(log).unwrap();
    assert!(!serialized_log.contains("final_price"));
    assert!(!serialized_log.contains("winning_outcome"));
}

#[test]
fn normalize_redacts_secret_fields_without_redacting_public_token_ids() {
    let dir = test_dir("redaction");
    let raw = dir.join("raw.jsonl");
    write_events(
        &raw,
        r#"{"event_type":"market","payload":{"market_id":"m1","up_token_id":"public-up","down_token_id":"public-down","api_key":"top-secret","authorization":"Bearer hidden"},"recorded_ts":"2026-06-01T00:00:00+00:00"}"#,
    );
    let normalized = dir.join("normalized");

    run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed".to_owned(),
        overwrite: false,
    })
    .unwrap();
    let normalized_events = fs::read_to_string(normalized.join("events.jsonl")).unwrap();

    assert!(normalized_events.contains("[redacted]"));
    assert!(normalized_events.contains("public-up"));
    assert!(!normalized_events.contains("top-secret"));
    assert!(!normalized_events.contains("Bearer hidden"));
}

#[test]
fn sweep_reports_walk_forward_and_leave_one_day_splits() {
    let dir = test_dir("sweep_splits");
    let events = dir.join("events.jsonl");
    write_events(&events, &five_day_fixture());

    let report = run_sweep(SweepOptions {
        input: events,
        markets: None,
        search: None,
        split: "walk_forward".to_owned(),
        max_experiments: 1,
        out: dir.join("sweep.json"),
        markdown: dir.join("sweep.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let plan = &report["result"]["split_plan"];

    assert_eq!(plan["market_days"].as_array().unwrap().len(), 5);
    assert_eq!(
        plan["latest_walk_forward"]["train_days"],
        serde_json::json!(["2026-06-01", "2026-06-02", "2026-06-03"])
    );
    assert_eq!(plan["latest_walk_forward"]["validation_day"], "2026-06-04");
    assert_eq!(plan["latest_walk_forward"]["test_day"], "2026-06-05");
    assert_eq!(
        plan["leave_one_day_out"]["folds"].as_array().unwrap().len(),
        5
    );
    assert!(
        report["result"]["candidates"][0]["fill_model_results"][0]["split_performance"]["test"]
            ["markets"]
            .as_u64()
            .unwrap()
            > 0
    );
}

fn replay(dir: &Path, events: &Path, fill_model: FillModel) -> Value {
    run_replay(ReplayOptions {
        input: events.to_path_buf(),
        markets: None,
        strategy_config: None,
        fill_model,
        out: dir.join(format!("replay-{fill_model}.json")),
        markdown: dir.join(format!("replay-{fill_model}.md")),
        exclude_windows: Vec::new(),
    })
    .unwrap()
}

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("polyedge-research-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_events(path: &Path, text: &str) {
    fs::write(path, text).unwrap();
}

fn market_line(market_id: &str, up: &str, down: &str) -> String {
    format!(
        r#"{{"event_type":"market","payload":{{"market_id":"{market_id}","condition_id":"c-{market_id}","market_slug":"slug-{market_id}","question":"BTC Up or Down","asset":"BTC","horizon":"15m","up_token_id":"{up}","down_token_id":"{down}","start_ts":"2026-06-01T00:00:00Z","end_ts":"2026-06-01T00:15:00Z","start_price":"100","tick_size":"0.01"}},"recorded_ts":"2026-06-01T00:00:00+00:00"}}"#
    )
}

fn decision_line(market_id: &str, token: &str, outcome: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"decision","payload":{{"action":"place","market_id":"{market_id}","token_id":"{token}","outcome":"{outcome}","side":"buy","price":"0.50","size":"5","order_kind":"post_only_gtc","ttl_ms":10000,"expected_edge":"0.02","tick_size":"0.01"}},"recorded_ts":"{ts}"}}"#
    )
}

fn cancel_line(market_id: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"decision","payload":{{"action":"cancel_all","market_id":"{market_id}","reason":"cancel test"}},"recorded_ts":"{ts}"}}"#
    )
}

fn book_line(token: &str, ask: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"book","payload":{{"token_id":"{token}","bids":[{{"price":"0.49","size":"10"}}],"asks":[{{"price":"{ask}","size":"10"}}],"local_ts":"{ts}"}},"recorded_ts":"{ts}"}}"#
    )
}

fn bid_book_line(token: &str, bid: &str, size: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"book","payload":{{"token_id":"{token}","bids":[{{"price":"{bid}","size":"{size}"}}],"asks":[{{"price":"0.60","size":"10"}}],"previous_size":"10","local_ts":"{ts}"}},"recorded_ts":"{ts}"}}"#
    )
}

fn bid_book_no_level_line(token: &str, bid: &str, size: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"book","payload":{{"token_id":"{token}","bids":[{{"price":"{bid}","size":"{size}"}}],"asks":[{{"price":"0.60","size":"10"}}],"local_ts":"{ts}"}},"recorded_ts":"{ts}"}}"#
    )
}

fn trade_line(token: &str, price: &str, size: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"last_trade_price","payload":{{"token_id":"{token}","price":"{price}","size":"{size}","side":"buy","local_ts":"{ts}"}},"recorded_ts":"{ts}"}}"#
    )
}

fn execution_report_line(market_id: &str, token: &str, status: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"execution_report","payload":{{"market_id":"{market_id}","token_id":"{token}","status":"{status}","avg_price":"0.50","filled_size":"5","local_ts":"{ts}"}},"recorded_ts":"{ts}"}}"#
    )
}

fn raw_price_change_line(token: &str, bid: &str, size: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"raw_market_event","payload":{{"event_type":"price_change","token_id":"{token}","best_bid":"{bid}","price":"{bid}","size":"{size}","side":"BUY","local_ts":"{ts}","raw_payload":{{"event_type":"price_change","asset_id":"{token}","best_bid":"{bid}","price":"{bid}","size":"{size}","side":"BUY"}}}},"recorded_ts":"{ts}"}}"#
    )
}

fn queue_evidence_book_line(token: &str, ask: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"book","payload":{{"token_id":"{token}","bids":[{{"price":"0.49","size":"10"}}],"asks":[{{"price":"{ask}","size":"10"}}],"queue_depth":"3","trade_size":"2","previous_size":"12","local_ts":"{ts}"}},"recorded_ts":"{ts}"}}"#
    )
}

fn frozen_candidates_yaml() -> &'static str {
    r#"version: 1
updated_at: "2026-06-14T00:00:00Z"
research_only: true
paper_only: true
enabled_by_default: false
selection_rule: "Frozen candidates only."
candidates:
  - name: "static_baseline"
    profile: "static"
    candidate_version: "static_baseline@2026-06-14"
    config_hash: "sha256:static-baseline-profile-v1"
    created_at: "2026-06-14T00:00:00Z"
    frozen_since: "2026-06-14T00:00:00Z"
    reason: "Control profile for paired validation."
    enabled_by_default: false
    deployment_allowed: false
  - name: "dynamic_quote_style"
    profile: "dynamic_quote_style"
    candidate_version: "dynamic_quote_style@2026-06-14"
    config_hash: "sha256:dynamic-quote-style-profile-v1"
    created_at: "2026-06-14T00:00:00Z"
    frozen_since: "2026-06-14T00:00:00Z"
    reason: "Frozen quote-style candidate."
    enabled_by_default: false
    deployment_allowed: false
  - name: "full_deterministic_profile"
    profile: "full_deterministic_profile"
    candidate_version: "full_deterministic_profile@2026-06-14"
    config_hash: "sha256:full-deterministic-profile-v1"
    created_at: "2026-06-14T00:00:00Z"
    frozen_since: "2026-06-14T00:00:00Z"
    reason: "Frozen full deterministic candidate."
    enabled_by_default: false
    deployment_allowed: false
  - name: "dynamic_safety_only"
    profile: "dynamic_safety_only"
    candidate_version: "dynamic_safety_only@2026-06-14"
    config_hash: "sha256:dynamic-safety-only-profile-v1"
    created_at: "2026-06-14T00:00:00Z"
    frozen_since: "2026-06-14T00:00:00Z"
    reason: "Frozen safety-only candidate."
    enabled_by_default: false
    deployment_allowed: false
"#
}

fn reference_line(price: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"reference","payload":{{"source":"polymarket_rtds_chainlink_btc_usd","price":"{price}","source_ts":"{ts}","stale":false}},"recorded_ts":"{ts}"}}"#
    )
}

fn fair_value_line(market_id: &str, q_up: &str, ts: &str) -> String {
    let q_down = 1.0 - q_up.parse::<f64>().unwrap();
    format!(
        r#"{{"event_type":"fair_value","payload":{{"market_id":"{market_id}","q_up":"{q_up}","q_down":"{q_down:.2}","sigma":0.2}},"recorded_ts":"{ts}"}}"#
    )
}

fn filled_touch_fixture(book_ts: &str) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        market_line("m1", "up", "down"),
        decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
        book_line("up", "0.50", book_ts),
        reference_line("101", "2026-06-01T00:15:01+00:00")
    )
}

fn five_day_fixture() -> String {
    (1..=5)
        .map(|day| {
            let date = format!("2026-06-{day:02}");
            let market = format!("m{day}");
            let up = format!("up{day}");
            let down = format!("down{day}");
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}",
                market_line_at(&market, &up, &down, &date),
                fair_value_line(&market, "0.60", &format!("{date}T00:00:30+00:00")),
                book_line(&up, "0.50", &format!("{date}T00:00:45+00:00")),
                book_line(&down, "0.50", &format!("{date}T00:00:45+00:00")),
                decision_line(&market, &up, "up", &format!("{date}T00:01:00+00:00")),
                reference_line("101", &format!("{date}T00:15:01+00:00"))
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn market_line_at(market_id: &str, up: &str, down: &str, date: &str) -> String {
    format!(
        r#"{{"event_type":"market","payload":{{"market_id":"{market_id}","condition_id":"c-{market_id}","market_slug":"slug-{market_id}","question":"BTC Up or Down","asset":"BTC","horizon":"15m","up_token_id":"{up}","down_token_id":"{down}","start_ts":"{date}T00:00:00Z","end_ts":"{date}T00:15:00Z","start_price":"100","tick_size":"0.01"}},"recorded_ts":"{date}T00:00:00+00:00"}}"#
    )
}
