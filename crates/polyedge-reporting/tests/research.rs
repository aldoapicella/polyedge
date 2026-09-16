use polyedge_reporting::research::{
    load_default_exclusions, load_frozen_candidate_registry, run_audit, run_backfill, run_baseline,
    run_build_markets, run_build_replay_index, run_calibration, run_chart_backfill,
    run_final_report, run_normalize, run_queue_audit, run_regimes, run_replay, run_sample_size,
    run_sweep, run_validate_prospective, AuditOptions, BackfillOptions, BaselineOptions,
    BuildMarketsOptions, CalibrationOptions, ChartBackfillOptions, ExcludedTimeWindow, FillModel,
    FinalReportOptions, NormalizeOptions, ProspectiveValidationOptions, QueueAuditOptions,
    RegimesOptions, ReplayIndexOptions, ReplayOptions, SampleSizeOptions, SweepOptions,
};
use serde_json::{json, Value};
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
        settlement_carry: None,
    })
    .unwrap();

    assert_eq!(report["result"]["markets_seen"], 1);
    assert_eq!(report["result"]["malformed_lines"], 1);
}

#[test]
fn invalid_event_timestamps_are_rejected_in_raw_and_merged_inputs() {
    let dir = test_dir("invalid_timestamps");
    let lines = [
        serde_json::json!({"event_type":"reference","recorded_ts":"2026-06-01T00:00:00Z"}),
        serde_json::json!({"event_type":"reference"}),
        serde_json::json!({"event_type":"reference","recorded_ts":"invalid"}),
        serde_json::json!({"event_type":"reference","recorded_ts":"invalid","ts":"2026-06-01T00:00:01Z"}),
        serde_json::json!({"event_type":"reference","recorded_ts":null}),
        serde_json::json!({"event_type":"reference","ts":"2026-06-01T00:00:02Z"}),
    ].map(|row| row.to_string()).join("\n");
    for merged in [false, true] {
        let input = dir.join(if merged { "merged" } else { "raw" });
        fs::create_dir_all(&input).unwrap();
        write_events(&input.join("other.jsonl"), &lines);
        if merged {
            fs::write(input.join("events_manifest.json"), "{}").unwrap();
        }
        let report = run_audit(AuditOptions {
            input: input.clone(),
            out: input.join("audit.json"),
            markdown: input.join("audit.md"),
            exclude_windows: Vec::new(),
            settlement_carry: None,
        })
        .unwrap();
        assert_eq!(report["result"]["total_events"], 2);
        assert_eq!(report["result"]["malformed_lines"], 4);
        assert_eq!(report["result"]["invalid_timestamps"], 4);
        assert!(report["warnings"].as_array().unwrap().iter().any(|v| v
            .as_str()
            .is_some_and(|s| s.contains("4 records with missing or invalid event timestamps"))));
    }
    let report = run_normalize(NormalizeOptions {
        input: dir.join("raw/other.jsonl"),
        out: dir.join("normalized"),
        format: "jsonl-indexed-gzip-sharded".to_owned(),
        overwrite: false,
        decision_grade_projection: false,
    })
    .unwrap();
    assert_eq!(report["result"]["events"], 2);
    assert_eq!(report["result"]["invalid_timestamps"], 4);
}

#[test]
fn explicit_wallet_changes_capital_and_binds_every_baseline_fill_model() {
    let dir = test_dir("explicit_wallet");
    let raw = dir.join("raw.jsonl");
    write_events(&raw, &filled_touch_fixture("2026-06-01T00:01:01+00:00"));
    let wallet = dir.join("wallet.json");
    let mut config = json!({
        "campaign_baseline": "20", "equity_floor": "19",
        "maximum_drawdown": "0.5", "maximum_order_notional": "0.1",
        "maximum_unresolved_orders_or_positions": 1
    });
    fs::write(&wallet, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut options = ReplayOptions {
        wallet_config: None,
        input: raw.clone(),
        markets: None,
        strategy_config: None,
        fill_model: FillModel::Touch,
        out: dir.join("replay.json"),
        markdown: dir.join("replay.md"),
        exclude_windows: Vec::new(),
    };
    let historical = run_replay(options.clone()).unwrap();
    options.wallet_config = Some(wallet.clone());
    let configured = run_replay(options.clone()).unwrap();
    assert_ne!(
        historical["result"]["wallet_constrained_net_pnl"],
        configured["result"]["wallet_constrained_net_pnl"]
    );
    assert_eq!(
        configured["result"]["wallet_constrained_equity_curve"][0]["equity"],
        "20"
    );
    let hash = configured["result"]["wallet_config_sha256"].clone();
    assert!(hash.as_str().unwrap().starts_with("sha256:"));
    let baseline = run_baseline(BaselineOptions {
        wallet_config: Some(wallet.clone()),
        input: raw,
        markets: None,
        out: dir.join("baseline.json"),
        markdown: dir.join("baseline.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    for row in baseline["result"]["fill_models"].as_array().unwrap() {
        assert_eq!(row["wallet_config_sha256"], hash);
        assert_eq!(row["wallet_constraints"]["campaign_baseline"], "20");
        assert_eq!(row["wallet_constraints"]["maximum_order_notional"], "0.1");
    }
    config["maximum_unresolved_orders_or_positions"] = json!(2);
    fs::write(&wallet, serde_json::to_vec(&config).unwrap()).unwrap();
    assert!(run_replay(options.clone()).is_err());
    config["maximum_unresolved_orders_or_positions"] = json!(1);
    config["equity_floor"] = json!("20");
    fs::write(&wallet, serde_json::to_vec(&config).unwrap()).unwrap();
    assert!(run_replay(options.clone()).is_err());
    config["equity_floor"] = json!("19");
    config["ignored_limit"] = json!("999");
    fs::write(&wallet, serde_json::to_vec(&config).unwrap()).unwrap();
    assert!(run_replay(options).is_err());
}

#[test]
fn current_equity_wallet_requires_causal_minimum_and_separates_replay_profit() {
    let dir = test_dir("current_equity_wallet");
    let raw = dir.join("raw.jsonl");
    let events = filled_touch_fixture("2026-06-01T00:01:01+00:00");
    write_events(&raw, &events);
    let wallet = dir.join("wallet.json");
    let mut config = json!({
        "campaign_baseline":"29.505501", "simulated_initial_equity":"50.690567",
        "equity_floor":"0", "maximum_drawdown":"29.505501", "maximum_order_notional":"10.5",
        "maximum_unresolved_orders_or_positions":1,
        "current_equity_policy":{"reserve_ratio":"0.1", "minimum_reserve":"2",
            "target_order_ratio":"0.05", "operating_buffer_ratio":"0.01", "minimum_order_notional":"1",
            "fee_rate":"0.07", "fee_exponent":1}
    });
    fs::write(&wallet, serde_json::to_vec(&config).unwrap()).unwrap();
    let options = ReplayOptions {
        wallet_config: Some(wallet.clone()),
        input: raw.clone(),
        markets: None,
        strategy_config: None,
        fill_model: FillModel::Touch,
        out: dir.join("replay.json"),
        markdown: dir.join("replay.md"),
        exclude_windows: Vec::new(),
    };
    assert!(run_replay(options.clone())
        .unwrap_err()
        .to_string()
        .contains("decision-time venue minimum"));
    let mut rows: Vec<Value> = events
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for row in &mut rows {
        if row["event_type"] == "market" {
            row["payload"]["minimum_order_size"] = json!("5");
        }
    }
    write_events(
        &raw,
        &rows
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let report = run_replay(options.clone()).unwrap();
    let start = &report["result"]["wallet_constrained_equity_curve"][0];
    assert_eq!(start["equity"], "50.690567");
    assert_eq!(start["net_pnl"], "0");
    assert_eq!(start["campaign_net_pnl"], "21.185066");
    assert!(report["result"]["wallet_config_sha256"].is_string());
    config["simulated_initial_equity"] = Value::Null;
    fs::write(&wallet, serde_json::to_vec(&config).unwrap()).unwrap();
    assert!(run_replay(options).is_err());
}

#[test]
fn configured_replay_and_profiles_are_used_and_index_binds_real_shards() {
    let dir = test_dir("configured_replay_index");
    let raw = dir.join("raw.jsonl");
    write_events(
        &raw,
        &filled_touch_fixture("2026-06-01T00:01:01+00:00").replace("post_only_gtc", "fak"),
    );
    let config = dir.join("strategy.json");
    fs::write(&config, b"{ invalid json }").unwrap();
    let options = ReplayOptions {
        wallet_config: None,
        input: raw.clone(),
        markets: None,
        strategy_config: Some(config.clone()),
        fill_model: FillModel::Touch,
        out: dir.join("replay.json"),
        markdown: dir.join("replay.md"),
        exclude_windows: Vec::new(),
    };
    assert!(run_replay(options.clone()).is_err());
    fs::write(
        &config,
        serde_json::to_vec(&polyedge_config::StrategyConfig::default()).unwrap(),
    )
    .unwrap();
    let default_replay = run_replay(options.clone()).unwrap();
    assert_eq!(default_replay["result"]["taker_fills"], 0);
    fs::write(
        &config,
        serde_json::to_vec(&polyedge_config::StrategyConfig {
            enable_taker_orders: true,
            ..Default::default()
        })
        .unwrap(),
    )
    .unwrap();
    let replay = run_replay(options).unwrap();
    assert_eq!(replay["result"]["taker_fills"], 1);
    assert!(replay["result"]["strategy_config_sha256"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    let profiles = dir.join("profiles.yaml");
    fs::write(
        &profiles,
        frozen_candidates_yaml().replace("profile: \"static\"", "profile: \"unknown\""),
    )
    .unwrap();
    let error = run_regimes(RegimesOptions {
        wallet_config: None,
        input: raw.clone(),
        markets: None,
        fill_model: FillModel::Touch,
        profile_config: Some(profiles),
        out: dir.join("regimes.json"),
        markdown: dir.join("regimes.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap_err();
    assert!(error.to_string().contains("unsupported replay profile"));
    let normalized = dir.join("normalized");
    run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed-gzip-sharded".to_owned(),
        overwrite: false,
        decision_grade_projection: false,
    })
    .unwrap();
    let options = ReplayIndexOptions {
        input: normalized.clone(),
        out: dir.join("index"),
        exclude_windows: Vec::new(),
    };
    let index = run_build_replay_index(options.clone()).unwrap();
    assert_eq!(index["result"]["status"], "normalized_input_bound");
    assert!(index["result"]["index_contents"].is_null());
    let shards = index["result"]["input_files"]["normalized_shards"]
        .as_array()
        .unwrap();
    assert!(!shards.is_empty());
    for shard in shards {
        use sha2::{Digest, Sha256};
        let bytes = fs::read(normalized.join(shard["file"].as_str().unwrap())).unwrap();
        assert_eq!(
            shard["sha256"],
            format!("sha256:{:x}", Sha256::digest(bytes))
        );
    }
    fs::remove_file(normalized.join(shards[0]["file"].as_str().unwrap())).unwrap();
    assert!(run_build_replay_index(options).is_err());
    assert!(run_build_replay_index(ReplayIndexOptions {
        input: PathBuf::from("azure://account/container/prefix"),
        out: dir.join("remote-index"),
        exclude_windows: Vec::new()
    })
    .is_err());
}

#[test]
fn sample_size_requires_bound_conservative_model_and_temporal_independence() {
    let dir = test_dir("temporal_sample");
    let source = dir.join("baseline.json");
    let make_source = |days: u32, correlated: bool| {
        let rows=(1..=days).flat_map(|day|(0..100).map(move |market|serde_json::json!({
            "market_id":format!("{day}-{market}"),"start_ts":format!("2026-06-{day:02}T00:00:00Z"),
            "winning_outcome":"up","complete_for_simulation":true,"net_pnl":if correlated && day>21 {"-2"} else {"1"}
        }))).collect::<Vec<_>>();
        serde_json::json!({"result":{"fill_models":[{"fill_model":"queue_proxy_conservative","profile":"static",
            "queue_proxy_pnl_eligible":true,"warnings":[],"market_results":rows}]}})
    };
    let options = SampleSizeOptions {
        results: source.clone(),
        fill_model: Some(FillModel::QueueProxyConservative),
        out: dir.join("sample.json"),
        markdown: dir.join("sample.md"),
    };
    fs::write(
        &source,
        serde_json::to_vec(&make_source(27, false)).unwrap(),
    )
    .unwrap();
    let mut missing = options.clone();
    missing.fill_model = None;
    assert!(run_sample_size(missing)
        .unwrap_err()
        .to_string()
        .contains("requires --fill-model"));
    let short = run_sample_size(options.clone()).unwrap();
    assert_eq!(short["result"]["profitability_claim_allowed"], false);
    assert!(short["result"]["statistics"]["ci_low"].is_null());
    fs::write(&source, serde_json::to_vec(&make_source(28, true)).unwrap()).unwrap();
    let first = run_sample_size(options.clone()).unwrap();
    let second = run_sample_size(options.clone()).unwrap();
    let stats = &first["result"]["statistics"];
    assert!(
        stats["iid_descriptive_ci_low"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
            > 0.0
    );
    assert!(stats["ci_low"].as_str().unwrap().parse::<f64>().unwrap() < 0.0);
    assert_eq!(stats["ci_low"], second["result"]["statistics"]["ci_low"]);
    assert_eq!(stats["profitability_claim_allowed"], false);
    assert_eq!(stats["temporal_confidence"]["bootstrap_resamples"], 10000);
    let mut ineligible = make_source(28, false);
    ineligible["result"]["fill_models"][0]["queue_proxy_pnl_eligible"] = serde_json::json!(false);
    fs::write(&source, serde_json::to_vec(&ineligible).unwrap()).unwrap();
    let result = run_sample_size(options.clone()).unwrap();
    assert_eq!(result["result"]["profitability_claim_allowed"], false);
    let duplicate = ineligible["result"]["fill_models"][0]["market_results"][0].clone();
    ineligible["result"]["fill_models"][0]["market_results"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    fs::write(&source, serde_json::to_vec(&ineligible).unwrap()).unwrap();
    assert!(run_sample_size(options)
        .unwrap_err()
        .to_string()
        .contains("duplicate settled market"));
}

#[test]
fn adverse_penalty_uses_fresh_fill_reference_and_requires_later_evidence() {
    let dir = test_dir("adverse_fill_reference");
    for (name, fresh_reference, post_ts, eligible) in [
        ("fresh", true, "2026-06-01T00:01:02Z", true),
        ("stale", false, "2026-06-01T00:01:02Z", false),
        ("same_time", true, "2026-06-01T00:01:01Z", false),
    ] {
        let mut lines = vec![
            market_line("m1", "up", "down"),
            market_start_line("m1"),
            reference_line("100", "2026-06-01T00:00:30Z"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00Z"),
        ];
        if fresh_reference {
            lines.push(reference_line("110", "2026-06-01T00:01:00.500Z"));
        }
        lines.extend([
            book_line("up", "0.50", "2026-06-01T00:01:01Z"),
            reference_line("105", post_ts),
            reference_line("101", "2026-06-01T00:15:01Z"),
        ]);
        let input = dir.join(format!("{name}.jsonl"));
        write_events(&input, &lines.join("\n"));
        let report = run_replay(ReplayOptions {
            wallet_config: None,
            input,
            markets: None,
            strategy_config: None,
            fill_model: FillModel::AdverseSelectionPenalized,
            out: dir.join(format!("{name}.json")),
            markdown: dir.join(format!("{name}.md")),
            exclude_windows: Vec::new(),
        })
        .unwrap();
        assert_eq!(report["result"]["fills"], 1);
        assert_eq!(report["result"]["adverse_selection_pnl_eligible"], eligible);
        if eligible {
            assert_eq!(report["result"]["adverse_penalty"], "0.025");
        }
    }
}

#[test]
fn normalized_audit_preserves_and_summarizes_runtime_provenance() {
    let dir = test_dir("runtime_provenance_audit");
    let events = dir.join("events.jsonl");
    let identity = valid_runtime_provenance_identity();
    let lines = [
        "2026-07-14T00:00:01Z",
        "2026-07-14T00:01:01Z",
        "2026-07-14T00:02:01Z",
    ]
    .into_iter()
    .map(|recorded_ts| {
        serde_json::json!({
            "event_type": "runtime_provenance",
            "recorded_ts": recorded_ts,
            "payload": identity
        })
        .to_string()
    })
    .collect::<Vec<_>>()
    .join("\n");
    write_events(&events, &format!("{lines}\n"));

    let normalized = dir.join("normalized");
    let normalize = run_normalize(NormalizeOptions {
        input: events,
        out: normalized.clone(),
        format: "jsonl-indexed-gzip-sharded".to_owned(),
        overwrite: true,
        decision_grade_projection: false,
    })
    .unwrap();
    assert_eq!(
        normalize["result"]["files"]["runtime_provenance"]["rows"],
        3
    );

    let audit = run_audit(AuditOptions {
        input: normalized,
        out: dir.join("data_audit.json"),
        markdown: dir.join("data_audit.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
    })
    .unwrap();
    assert_eq!(audit["result"]["runtime_provenance"]["observations"], 3);
    assert_eq!(
        audit["result"]["runtime_provenance"]["valid_observations"],
        3
    );
    assert_eq!(
        audit["result"]["runtime_provenance"]["distinct_identity_count"],
        1
    );
    assert_eq!(audit["result"]["runtime_provenance"]["max_gap_ms"], 60_000);
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
        expected_daily_date: None,
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
        settlement_carry: None,
    })
    .unwrap();
    assert_eq!(audit["result"]["total_events"], 1);
    assert_eq!(audit["result"]["excluded_event_count"], 3);

    let replay = run_replay(ReplayOptions {
        wallet_config: None,
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
        decision_grade_projection: false,
    })
    .unwrap();
    let report = run_build_markets(BuildMarketsOptions {
        input: normalized,
        out: dir.join("markets.json"),
        markdown: dir.join("markets.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
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
fn explicit_build_markets_outputs_do_not_overwrite_each_other() {
    let dir = test_dir("explicit_market_outputs");
    let daily_input = dir.join("daily.jsonl");
    let cumulative_input = dir.join("cumulative.jsonl");
    write_events(
        &daily_input,
        &format!("{}\n", market_line("m1", "up-1", "down-1")),
    );
    write_events(
        &cumulative_input,
        &format!(
            "{}\n{}\n",
            market_line("m1", "up-1", "down-1"),
            market_line("m2", "up-2", "down-2")
        ),
    );
    let daily_out = dir.join("markets_summary.json");
    let cumulative_out = dir.join("cumulative_markets_summary.json");

    run_build_markets(BuildMarketsOptions {
        input: daily_input,
        out: daily_out.clone(),
        markdown: dir.join("markets_summary.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
    })
    .unwrap();
    run_build_markets(BuildMarketsOptions {
        input: cumulative_input,
        out: cumulative_out,
        markdown: dir.join("cumulative_markets_summary.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
    })
    .unwrap();

    let daily: Value = serde_json::from_slice(&fs::read(daily_out).unwrap()).unwrap();
    assert_eq!(daily["result"]["summary"]["markets"], 1);
}

#[test]
fn normalize_writes_queue_evidence_and_queue_audit_marks_eligibility() {
    let dir = test_dir("queue_audit");
    let raw = dir.join("raw.jsonl");
    write_events(
        &raw,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
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
        decision_grade_projection: false,
    })
    .unwrap();

    assert!(normalized.join("books.jsonl").is_file());
    assert!(normalized.join("raw_market_events.jsonl").is_file());
    assert!(normalized.join("last_trades.jsonl").is_file());
    assert_eq!(manifest["result"]["files"]["book"]["rows"], 1);
    assert_eq!(manifest["result"]["files"]["raw_market_event"]["rows"], 1);
    assert_eq!(manifest["result"]["files"]["price_change"]["rows"], 0);
    assert_eq!(manifest["result"]["files"]["last_trade"]["rows"], 1);

    let markets_path = dir.join("markets.json");
    run_build_markets(BuildMarketsOptions {
        input: normalized.clone(),
        out: markets_path.clone(),
        markdown: dir.join("markets.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
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
fn decision_grade_projection_bounds_books_and_preserves_pre_decision_state_and_trades() {
    let dir = test_dir("decision_grade_projection");
    let raw = dir.join("raw.jsonl");
    write_events(
        &raw,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
            bid_book_line("up", "0.48", "5", "2026-06-01T00:00:30.100+00:00"),
            bid_book_line("up", "0.49", "5", "2026-06-01T00:00:30.500+00:00"),
            raw_price_change_line("up", "0.49", "5", "2026-06-01T00:00:30.600+00:00"),
            trade_line("up", "0.49", "2", "2026-06-01T00:00:30.700+00:00"),
            bid_book_line("up", "0.50", "5", "2026-06-01T00:00:31.100+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:00:31.200+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    let normalized = dir.join("normalized");

    let manifest = run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed".to_owned(),
        overwrite: false,
        decision_grade_projection: true,
    })
    .unwrap();

    assert_eq!(manifest["result"]["input_events"], 9);
    assert_eq!(manifest["result"]["events"], 8);
    assert_eq!(manifest["result"]["files"]["book"]["rows"], 2);
    assert_eq!(manifest["result"]["files"]["raw_market_event"]["rows"], 1);
    assert_eq!(manifest["result"]["files"]["last_trade"]["rows"], 1);
    let books = fs::read_to_string(normalized.join("books.jsonl")).unwrap();
    assert!(!books.contains("\"0.48\""));
    assert!(books.contains("\"0.49\""));
    assert!(books.contains("\"0.50\""));

    let markets = dir.join("markets.json");
    run_build_markets(BuildMarketsOptions {
        input: normalized.clone(),
        out: markets.clone(),
        markdown: dir.join("markets.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
    })
    .unwrap();
    let queue_audit = run_queue_audit(QueueAuditOptions {
        input: normalized,
        markets,
        out: dir.join("queue_audit.json"),
        markdown: dir.join("queue_audit.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    assert_eq!(queue_audit["result"]["queue_proxy_eligible_markets"], 1);
}

#[test]
fn decision_grade_projection_flushes_sparse_tokens_in_global_time_order() {
    let dir = test_dir("decision_grade_projection_ordering");
    let raw = dir.join("raw.jsonl");
    write_events(
        &raw,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            bid_book_line("up", "0.48", "5", "2026-06-01T00:00:30.100+00:00"),
            bid_book_line("down", "0.51", "5", "2026-06-01T00:00:30.200+00:00"),
            bid_book_line("up", "0.49", "5", "2026-06-01T00:00:31.100+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:00:31.200+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );
    let normalized = dir.join("normalized");
    run_normalize(NormalizeOptions {
        input: raw,
        out: normalized.clone(),
        format: "jsonl-indexed".to_owned(),
        overwrite: false,
        decision_grade_projection: true,
    })
    .unwrap();

    let audit = run_audit(AuditOptions {
        input: normalized,
        out: dir.join("audit.json"),
        markdown: dir.join("audit.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
    })
    .unwrap();
    assert_eq!(audit["result"]["out_of_order_timestamps"], 0);
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
        decision_grade_projection: false,
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
        decision_grade_projection: false,
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
        settlement_carry: None,
    })
    .unwrap();
    assert_eq!(markets["result"]["summary"]["complete_for_simulation"], 1);

    let replay = run_replay(ReplayOptions {
        wallet_config: None,
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
        decision_grade_projection: false,
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
    assert_eq!(progress["events"], 5);

    let markets_path = dir.join("markets.json");
    let markets = run_build_markets(BuildMarketsOptions {
        input: normalized.clone(),
        out: markets_path.clone(),
        markdown: dir.join("markets.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
    })
    .unwrap();
    assert_eq!(markets["result"]["summary"]["complete_for_simulation"], 1);

    let replay = run_replay(ReplayOptions {
        wallet_config: None,
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
        decision_grade_projection: false,
    })
    .unwrap();

    let audit = run_audit(AuditOptions {
        input: normalized,
        out: dir.join("audit.json"),
        markdown: dir.join("audit.md"),
        exclude_windows: Vec::new(),
        settlement_carry: None,
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
        decision_grade_projection: false,
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
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
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
        settlement_carry: None,
    })
    .unwrap();
    let baseline = run_baseline(BaselineOptions {
        wallet_config: None,
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
        wallet_config: None,
        test_input: None,
        test_markets: None,
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
        fill_model: Some(FillModel::TradeThrough),
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
        "Holdout events and market truth must be separate inputs and are read only after the winner is fixed."
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
        expected_daily_date: None,
    })
    .unwrap();
    assert_eq!(prospective["result"]["status"], "tracking");
    assert_eq!(prospective["result"]["rows"][0]["settled_markets"], 1);
    let expected_static = baseline["result"]["fill_models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row["fill_model"].as_str() == prospective["result"]["rows"][0]["fill_model"].as_str()
        })
        .unwrap()["net_pnl"]
        .clone();
    assert_eq!(
        prospective["result"]["rows"][0]["static_net_pnl"],
        expected_static
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
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
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
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
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
fn queue_proxy_ignores_valid_opposite_trade_without_poisoning_or_consuming_bid_queue() {
    let dir = test_dir("queue_proxy_opposite_trade");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
            bid_book_line("up", "0.50", "5", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line_with_side("up", "0.50", "10", "buy", "2026-06-01T00:01:02+00:00"),
            trade_line("up", "0.50", "5", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["queue_proxy_enabled"], true);
    assert_eq!(report["result"]["queue_proxy_pnl_eligible"], true);
    assert_eq!(report["result"]["fills"], 0);
    assert_eq!(
        report["result"]["replay_metrics"]["queue_proxy"]["ignored_opposite_trade_count"],
        1
    );
}

#[test]
fn queue_proxy_missing_trade_side_blocks_authorization_pnl_even_after_a_fill() {
    let dir = test_dir("queue_proxy_missing_trade_side");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
            bid_book_line("up", "0.50", "5", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line_without_side("up", "0.50", "1", "2026-06-01T00:01:02+00:00"),
            trade_line("up", "0.50", "10", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["fills"], 1);
    assert_eq!(report["result"]["net_pnl"], "2.50");
    assert_eq!(report["result"]["queue_proxy_pnl_eligible"], false);
    assert_eq!(report["result"]["ineligible_queue_fills"], 1);
    assert!(report["result"]["queue_proxy_net_pnl"].is_null());
    assert!(report["result"]["queue_proxy_wallet_constrained_net_pnl"].is_null());
    assert_eq!(
        report["result"]["replay_metrics"]["queue_proxy"]["ineligible_reasons"]
            ["trade_print_missing_aggressor_side"],
        1
    );
    let eligibility = &report["result"]["replay_metrics"]["queue_proxy"];
    assert_eq!(eligibility["market_eligibility"]["m1"]["market_id"], "m1");
    assert_eq!(eligibility["market_eligibility"]["m1"]["eligible"], false);
    assert_eq!(
        eligibility["market_eligibility"]["m1"]["queue_fill_event_count"],
        1
    );
    assert!(eligibility["market_eligibility"]["m1"]["reasons"]
        .as_array()
        .is_some_and(|reasons| reasons
            .iter()
            .any(|reason| { reason.as_str() == Some("trade_print_missing_aggressor_side") })));
    assert!(
        eligibility["market_eligibility"]["m1"]["queue_evidence_sha256"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:") && hash.len() == 71)
    );
    assert!(eligibility["market_eligibility_sha256"]
        .as_str()
        .is_some_and(|hash| hash.starts_with("sha256:") && hash.len() == 71));
    assert_eq!(
        eligibility["input_binding"]["schema"],
        "polyedge.queue_proxy.input_binding.v1"
    );
    assert!(eligibility["input_binding"]["sha256"]
        .as_str()
        .is_some_and(|hash| hash.starts_with("sha256:") && hash.len() == 71));
}

#[test]
fn queue_proxy_below_ninety_five_percent_eligibility_cannot_authorize_positive_pnl() {
    let dir = test_dir("queue_proxy_low_eligibility");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up-1", "down-1"),
            market_start_line("m1"),
            bid_book_line("up-1", "0.50", "5", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up-1", "up", "2026-06-01T00:01:00+00:00"),
            trade_line("up-1", "0.50", "10", "2026-06-01T00:01:03+00:00"),
            market_line("m2", "up-2", "down-2"),
            market_start_line("m2"),
            decision_line("m2", "up-2", "up", "2026-06-01T00:01:04+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["net_pnl"], "2.50");
    assert_eq!(report["result"]["queue_proxy_eligibility_rate"], 0.5);
    assert_eq!(report["result"]["queue_proxy_pnl_eligible"], false);
    assert_eq!(report["result"]["ineligible_queue_fills"], 0);
    assert!(report["result"]["queue_proxy_net_pnl"].is_null());
}

#[test]
fn queue_proxy_uses_runtime_full_depth_snapshot_instead_of_compact_top_only_book() {
    let dir = test_dir("queue_proxy_runtime_size_ahead");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
            bid_book_no_level_line("up", "0.50", "2", "2026-06-01T00:00:30+00:00"),
            raw_price_change_line("up", "0.50", "2", "2026-06-01T00:00:45+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            r#"{"event_type":"paper_order_queue_registration","payload":{"order_id":"order-1","market_id":"m1","token_id":"up","side":"buy","quote_price":"0.50","order_size":"5"},"recorded_ts":"2026-06-01T00:01:00.010+00:00"}"#,
            trade_line("up", "0.50", "10", "2026-06-01T00:01:00.300+00:00"),
            bid_book_no_level_line("up", "0.50", "2", "2026-06-01T00:01:00.400+00:00"),
            r#"{"event_type":"paper_order_queue_snapshot","payload":{"order_id":"order-1","market_id":"m1","token_id":"up","side":"buy","quote_price":"0.50","order_size":"5","visible_size_ahead_estimate":"12"},"recorded_ts":"2026-06-01T00:01:00.401+00:00"}"#,
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["queue_proxy_enabled"], true);
    assert_eq!(report["result"]["avg_size_ahead"], "12");
    assert_eq!(report["result"]["fills"], 0);
}

#[test]
fn queue_proxy_uses_raw_price_change_size_for_size_ahead() {
    let dir = test_dir("queue_proxy_price_change_size");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
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
fn queue_proxy_counts_better_bid_depth_as_size_ahead() {
    let dir = test_dir("queue_proxy_better_bid_depth");
    let events = dir.join("events.jsonl");
    write_events(
        &events,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            market_line("m1", "up", "down"),
            market_start_line("m1"),
            bid_book_line("up", "0.55", "7", "2026-06-01T00:00:30+00:00"),
            decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
            trade_line("up", "0.50", "12", "2026-06-01T00:01:03+00:00"),
            reference_line("101", "2026-06-01T00:15:01+00:00")
        ),
    );

    let report = replay(&dir, &events, FillModel::QueueProxyConservative);

    assert_eq!(report["result"]["queue_proxy_enabled"], true);
    assert_eq!(report["result"]["avg_size_ahead"], "7");
    assert_eq!(report["result"]["fills"], 1);
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
        wallet_config: None,
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
        decision_grade_projection: false,
    })
    .unwrap();
    let normalized_events = fs::read_to_string(normalized.join("events.jsonl")).unwrap();

    assert!(normalized_events.contains("[redacted]"));
    assert!(normalized_events.contains("public-up"));
    assert!(!normalized_events.contains("top-secret"));
    assert!(!normalized_events.contains("Bearer hidden"));
}

#[test]
fn sweep_without_separate_holdout_is_validation_only() {
    let dir = test_dir("sweep_validation_only");
    let input = dir.join("events.jsonl");
    write_events(&input, &five_day_fixture());
    let report = run_sweep(SweepOptions {
        wallet_config: None,
        input,
        markets: None,
        test_input: None,
        test_markets: None,
        search: None,
        split: "walk_forward".to_owned(),
        max_experiments: 1,
        out: dir.join("sweep.json"),
        markdown: dir.join("sweep.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    assert_eq!(
        report["result"]["split_plan"]["scope"],
        "selection_input_only"
    );
    assert_eq!(
        report["result"]["selection"]["status"],
        "validation_winner_fixed_test_not_evaluated"
    );
    assert_eq!(report["result"]["selection"]["robust_candidate"], false);
    assert!(report["result"]["selection"]["sealed_test"].is_null());
    assert_eq!(
        report["result"]["candidates"][0]["fill_model_results"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
}

#[test]
fn sweep_parses_and_applies_supported_search_space() {
    let dir = test_dir("sweep_search_applied");
    let events = dir.join("events.jsonl");
    let search = dir.join("search_space.yaml");
    write_events(&events, &filled_five_day_fixture("101"));
    fs::write(
        &search,
        r#"version: 1
maker_min_edge: [0.030]
ttl_seconds: [2]
final_no_trade_seconds: [90]
quote_style: [fair_minus_margin_only]
"#,
    )
    .unwrap();

    let report = run_sweep(SweepOptions {
        wallet_config: None,
        test_input: None,
        test_markets: None,
        input: events,
        markets: None,
        search: Some(search.clone()),
        split: "walk_forward".to_owned(),
        max_experiments: 2,
        out: dir.join("sweep.json"),
        markdown: dir.join("sweep.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let candidates = report["result"]["candidates"].as_array().unwrap();
    let configured = candidates
        .iter()
        .find(|row| row["candidate"] != "baseline")
        .unwrap();
    let baseline = candidates
        .iter()
        .find(|row| row["candidate"] == "baseline")
        .unwrap();

    assert_eq!(
        report["result"]["search"],
        search.to_string_lossy().as_ref()
    );
    assert_eq!(report["result"]["search_space"]["configured"], true);
    assert_eq!(configured["parameters"]["maker_min_edge"], "0.030");
    assert_eq!(configured["parameters"]["ttl_seconds"], 2);
    assert_eq!(configured["parameters"]["final_no_trade_seconds"], 90);
    assert_eq!(
        configured["parameters"]["quote_style"],
        "fair_minus_margin_only"
    );
    let touch_validation_pnl = |candidate: &Value| {
        candidate["fill_model_results"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["fill_model"] == "touch_after_250ms")
            .unwrap()["validation"]["net_pnl"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
    };
    assert!(touch_validation_pnl(baseline) > 0.0);
    assert_eq!(touch_validation_pnl(configured), 0.0);
}

#[test]
fn sweep_rejects_search_parameters_that_are_not_applied() {
    let dir = test_dir("sweep_search_rejects_unused");
    let events = dir.join("events.jsonl");
    let search = dir.join("search_space.yaml");
    write_events(&events, &five_day_fixture());
    fs::write(&search, "version: 1\nmaker_margin: [0.01, 0.02]\n").unwrap();

    let error = run_sweep(SweepOptions {
        wallet_config: None,
        test_input: None,
        test_markets: None,
        input: events,
        markets: None,
        search: Some(search),
        split: "walk_forward".to_owned(),
        max_experiments: 2,
        out: dir.join("sweep.json"),
        markdown: dir.join("sweep.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("unsupported sweep search parameter maker_margin"));
}

#[test]
fn sweep_search_rejects_zero_configured_runs_duplicate_json_and_multiple_versions() {
    let dir = test_dir("sweep_search_fail_closed");
    let events = dir.join("events.jsonl");
    write_events(&events, &five_day_fixture());

    for (name, text, max_experiments, expected) in [
        (
            "zero-configured.yaml",
            "version: 1\nmaker_min_edge: [0.01]\n",
            1,
            "requires max_experiments >= 2",
        ),
        (
            "duplicate.json",
            r#"{"version":1,"maker_min_edge":[0.01],"maker_min_edge":[0.02]}"#,
            2,
            "duplicate sweep search JSON parameter maker_min_edge",
        ),
        (
            "versions.yaml",
            "version: [1, 2]\nmaker_min_edge: [0.01]\n",
            2,
            "version must contain exactly one scalar",
        ),
    ] {
        let search = dir.join(name);
        fs::write(&search, text).unwrap();
        let error = run_sweep(SweepOptions {
            wallet_config: None,
            test_input: None,
            test_markets: None,
            input: events.clone(),
            markets: None,
            search: Some(search),
            split: "walk_forward".to_owned(),
            max_experiments,
            out: dir.join(format!("{name}.out.json")),
            markdown: dir.join(format!("{name}.out.md")),
            exclude_windows: Vec::new(),
        })
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn sweep_selection_is_invariant_to_a_physically_separate_holdout() {
    let first = run_leakage_sweep("sweep_leakage_up", "101");
    let second = run_leakage_sweep("sweep_leakage_down", "99");
    let ranks = |report: &Value| {
        report["result"]["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r["candidate"].clone(),
                    r["validation_rank"].clone(),
                    r["validation_total_fill_model_net_pnl"].clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(ranks(&first), ranks(&second));
    assert_eq!(
        first["result"]["selection"]["candidate"],
        second["result"]["selection"]["candidate"]
    );
    assert!(first["result"]["selection"]["sealed_test"].is_null());
    for report in [&first, &second] {
        assert_eq!(
            report["result"]["selection"]["status"],
            "insufficient_validation_evidence_holdout_unopened"
        );
        assert_eq!(report["result"]["selection"]["robust_candidate"], false);
        for row in report["result"]["candidates"].as_array().unwrap() {
            if row["selected"] != true {
                assert!(row["sealed_test"].is_null());
            }
        }
    }
}

#[test]
fn sweep_report_rule_text_matches_fail_closed_computation() {
    let dir = test_dir("sweep_rule_contract");
    let events = dir.join("events.jsonl");
    let markdown = dir.join("sweep.md");
    write_events(&events, &filled_five_day_fixture("101"));

    let report = run_sweep(SweepOptions {
        wallet_config: None,
        test_input: None,
        test_markets: None,
        input: events,
        markets: None,
        search: None,
        split: "walk_forward".to_owned(),
        max_experiments: 1,
        out: dir.join("sweep.json"),
        markdown: markdown.clone(),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let robust_rule = report["result"]["robust_candidate_rule"].as_str().unwrap();
    let rendered = fs::read_to_string(markdown).unwrap();

    assert!(robust_rule.contains("7-day circular block-bootstrap lower 95% bound"));
    assert!(robust_rule.contains("10000 resamples, at least 28 daily clusters"));
    assert!(robust_rule.contains("positive wallet-constrained PnL"));
    assert!(rendered.contains(robust_rule));
    assert!(report["result"]["test_sealing_rule"]
        .as_str()
        .unwrap()
        .contains("separate test input and separate market truth"));
    assert_eq!(report["result"]["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(report["result"]["selection"]["robust_candidate"], false);
    assert!(report["result"]["candidates"][0]["fill_model_results"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| {
            row["validation"]["block_confidence_lower_95"].is_null()
                && row["net_pnl"] == row["validation"]["net_pnl"]
        }));
}

fn replay(dir: &Path, events: &Path, fill_model: FillModel) -> Value {
    run_replay(ReplayOptions {
        wallet_config: None,
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

fn market_start_line(market_id: &str) -> String {
    market_start_line_at(market_id, "2026-06-01")
}

fn market_start_line_at(market_id: &str, date: &str) -> String {
    serde_json::json!({
        "event_type": "market_start_price",
        "payload": {
            "schema_version": 1,
            "schema": "polyedge.market_start_price.v1",
            "market_id": market_id,
            "market_start_ts": format!("{date}T00:00:00Z"),
            "market_end_ts": format!("{date}T00:15:00Z"),
            "start_price": "100",
            "reference_source": "polymarket_rtds_chainlink_btc_usd",
            "reference_source_ts": format!("{date}T00:00:01Z"),
            "reference_exact_resolution_source": true,
            "reference_stale": false
        },
        "recorded_ts": format!("{date}T00:00:01+00:00")
    })
    .to_string()
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
    trade_line_with_side(token, price, size, "sell", ts)
}

fn trade_line_with_side(token: &str, price: &str, size: &str, side: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"last_trade_price","payload":{{"token_id":"{token}","price":"{price}","size":"{size}","side":"{side}","local_ts":"{ts}"}},"recorded_ts":"{ts}"}}"#
    )
}

fn trade_line_without_side(token: &str, price: &str, size: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"last_trade_price","payload":{{"token_id":"{token}","price":"{price}","size":"{size}","local_ts":"{ts}"}},"recorded_ts":"{ts}"}}"#
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
    config_hash: "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c"
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

fn valid_runtime_provenance_identity() -> Value {
    serde_json::json!({
        "schema_version": 1,
        "backend_impl": "rust",
        "git_sha": "c40d9093783808b010eabd9c43697e9dcceb667b",
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

fn reference_line(price: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"reference","payload":{{"source":"polymarket_rtds_chainlink_btc_usd","price":"{price}","source_ts":"{ts}","stale":false,"exact_resolution_source":true}},"recorded_ts":"{ts}"}}"#
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
        "{}\n{}\n{}\n{}\n{}",
        market_line("m1", "up", "down"),
        market_start_line("m1"),
        decision_line("m1", "up", "up", "2026-06-01T00:01:00+00:00"),
        book_line("up", "0.50", book_ts),
        reference_line("101", "2026-06-01T00:15:01+00:00")
    )
}

fn run_leakage_sweep(name: &str, final_day_price: &str) -> Value {
    let dir = test_dir(name);
    let raw = dir.join("selection-raw.jsonl");
    write_events(&raw, &filled_five_day_fixture("101"));
    let test_raw = dir.join("holdout-raw.jsonl");
    write_events(
        &test_raw,
        &filled_touch_fixture("2026-06-01T00:01:01+00:00")
            .replace("2026-06-01", "2026-06-06")
            .replace("m1", "holdout-m1")
            .replace(
                "\"price\":\"101\"",
                &format!("\"price\":\"{final_day_price}\""),
            ),
    );
    for (input, out) in [
        (raw, dir.join("selection")),
        (test_raw, dir.join("holdout")),
    ] {
        run_normalize(NormalizeOptions {
            input,
            out,
            format: "jsonl-indexed-gzip-sharded".to_owned(),
            overwrite: false,
            decision_grade_projection: false,
        })
        .unwrap();
    }
    let options = SweepOptions {
        wallet_config: None,
        input: dir.join("selection"),
        markets: None,
        test_input: Some(dir.join("holdout")),
        test_markets: None,
        search: None,
        split: "walk_forward".to_owned(),
        max_experiments: 4,
        out: dir.join("sweep.json"),
        markdown: dir.join("sweep.md"),
        exclude_windows: Vec::new(),
    };
    let report = run_sweep(options.clone()).unwrap();
    assert!(!dir.join("sweep.winner-before-test.json").exists());
    // An unreadable held-out shard must not be opened when validation fails.
    fs::remove_dir_all(dir.join("holdout")).unwrap();
    fs::create_dir(dir.join("holdout")).unwrap();
    let rerun = run_sweep(options.clone()).unwrap();
    assert_eq!(rerun["result"]["selection"], report["result"]["selection"]);
    let mut same = options;
    same.test_input = Some(same.input.clone());
    same.out = dir.join("same.json");
    assert!(run_sweep(same)
        .unwrap_err()
        .to_string()
        .contains("disjoint paths"));
    report
}

fn filled_five_day_fixture(final_day_price: &str) -> String {
    (1..=5)
        .map(|day| {
            let date = format!("2026-06-{day:02}");
            let market = format!("filled-m{day}");
            let up = format!("filled-up{day}");
            let down = format!("filled-down{day}");
            let final_price = if day == 5 { final_day_price } else { "101" };
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n{}",
                market_line_at(&market, &up, &down, &date),
                market_start_line_at(&market, &date),
                fair_value_line(&market, "0.60", &format!("{date}T00:00:30+00:00")),
                decision_line(&market, &up, "up", &format!("{date}T00:01:00+00:00")),
                book_line(&up, "0.50", &format!("{date}T00:01:01+00:00")),
                book_line(&down, "0.50", &format!("{date}T00:01:01+00:00")),
                reference_line(final_price, &format!("{date}T00:15:01+00:00"))
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn five_day_fixture() -> String {
    (1..=5)
        .map(|day| {
            let date = format!("2026-06-{day:02}");
            let market = format!("m{day}");
            let up = format!("up{day}");
            let down = format!("down{day}");
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n{}",
                market_line_at(&market, &up, &down, &date),
                market_start_line_at(&market, &date),
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
