use chrono::NaiveDate;
use polyedge_reporting::research::{
    run_audit, run_build_cumulative_wallet_snapshot, run_build_markets,
    run_materialize_projected_campaign, run_normalize, run_publish_projected_day, run_regimes,
    AuditOptions, BuildMarketsOptions, CumulativeWalletSnapshotOptions, FillModel,
    MaterializeProjectedCampaignOptions, NormalizeOptions, PublishProjectedDayOptions,
    RegimesOptions,
};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

const CAMPAIGN: &str = "campaign-2026-07-12";

#[test]
fn projected_days_equal_full_cross_midnight_replay_and_bound_readers() {
    let dir = test_dir("equivalence");
    let day_one = dir.join("day-one.jsonl");
    let day_two = dir.join("day-two.jsonl");
    fs::write(&day_one, format!("{}\n", cross_midnight_day_one())).unwrap();
    fs::write(&day_two, format!("{}\n", cross_midnight_day_two())).unwrap();

    let normalized_one = normalize(&day_one, &dir.join("normalized-one"));
    let normalized_two = normalize(&day_two, &dir.join("normalized-two"));
    let cache = dir.join("cache");
    publish_day(
        &normalized_one,
        date(2026, 7, 12),
        &cache,
        &dir.join("day-one-manifest.json"),
    );
    publish_day(
        &normalized_two,
        date(2026, 7, 13),
        &cache,
        &dir.join("day-two-manifest.json"),
    );
    let campaign = dir.join("campaign");
    materialize(
        &cache,
        date(2026, 7, 12),
        date(2026, 7, 13),
        &campaign,
        &dir.join("campaign-input.json"),
    )
    .unwrap();

    let combined = dir.join("combined.jsonl");
    fs::write(
        &combined,
        format!(
            "{}\n{}\n",
            cross_midnight_day_one(),
            cross_midnight_day_two()
        ),
    )
    .unwrap();
    let full = normalize(&combined, &dir.join("normalized-full"));

    let campaign_markets_path = dir.join("campaign-markets.json");
    let full_markets_path = dir.join("full-markets.json");
    let campaign_markets = build_markets(&campaign, &campaign_markets_path);
    let full_markets = build_markets(&full, &full_markets_path);
    let campaign_regimes = regimes(
        &campaign,
        &campaign_markets_path,
        &dir.join("campaign-regimes.json"),
    );
    let full_regimes = regimes(&full, &full_markets_path, &dir.join("full-regimes.json"));

    assert_eq!(
        campaign_markets["result"]["markets"],
        full_markets["result"]["markets"]
    );
    assert_eq!(
        campaign_regimes["result"]["profiles"],
        full_regimes["result"]["profiles"]
    );
    let static_profile = campaign_regimes["result"]["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|profile| profile["profile"] == "static")
        .unwrap();
    assert_eq!(static_profile["orders"], 1);

    let wallet = run_build_cumulative_wallet_snapshot(CumulativeWalletSnapshotOptions {
        regimes: dir.join("campaign-regimes.json"),
        campaign_manifest: dir.join("campaign-input.json"),
        snapshot_date: date(2026, 7, 13),
        out: dir.join("wallet.json"),
    })
    .unwrap();
    assert_eq!(wallet["schema_version"], 2);
    assert_eq!(wallet["snapshot_date"], "2026-07-13");
    assert!(wallet["cumulative_input_sha256"]
        .as_str()
        .is_some_and(|hash| hash.starts_with("sha256:")));
    assert!(wallet["cumulative_state_sha256"]
        .as_str()
        .is_some_and(|hash| hash.starts_with("sha256:")));

    let mut mismatched_regimes = campaign_regimes.clone();
    mismatched_regimes["result"]["projected_campaign_manifest_sha256"] =
        Value::String(format!("sha256:{}", "f".repeat(64)));
    let mismatched_regimes_path = dir.join("mismatched-regimes.json");
    fs::write(
        &mismatched_regimes_path,
        serde_json::to_vec_pretty(&mismatched_regimes).unwrap(),
    )
    .unwrap();
    let mismatch = run_build_cumulative_wallet_snapshot(CumulativeWalletSnapshotOptions {
        regimes: mismatched_regimes_path,
        campaign_manifest: dir.join("campaign-input.json"),
        snapshot_date: date(2026, 7, 13),
        out: dir.join("mismatched-wallet.json"),
    })
    .unwrap_err();
    assert!(mismatch
        .to_string()
        .contains("not bound to the exact projected campaign manifest"));

    let audit = run_audit(AuditOptions {
        input: campaign,
        out: dir.join("audit.json"),
        markdown: dir.join("audit.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap();
    let notice = audit["result"]["stream_notices"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .find(|notice| notice.starts_with("projected campaign streamed"))
        .unwrap();
    assert!(notice.contains("2 sealed day segment(s)"));
    assert!(!notice.contains("30 open shard reader(s)"));
}

#[test]
fn projected_day_rerun_is_canonical_and_materialization_rejects_gap_or_corruption() {
    let dir = test_dir("integrity");
    let raw = dir.join("day.jsonl");
    fs::write(&raw, format!("{}\n", simple_day())).unwrap();
    let first = normalize(&raw, &dir.join("first"));
    let second = normalize(&raw, &dir.join("second"));
    let cache = dir.join("cache");
    let first_manifest = publish_day(
        &first,
        date(2026, 7, 12),
        &cache,
        &dir.join("first-manifest.json"),
    );
    let second_manifest = publish_day(
        &second,
        date(2026, 7, 12),
        &cache,
        &dir.join("second-manifest.json"),
    );
    assert_eq!(
        first_manifest["canonical_sha256"],
        second_manifest["canonical_sha256"]
    );

    let gap = materialize(
        &cache,
        date(2026, 7, 12),
        date(2026, 7, 13),
        &dir.join("gap"),
        &dir.join("gap-index.json"),
    )
    .unwrap_err();
    assert!(gap.to_string().contains("missing a complete day"));
    assert!(!dir.join("gap").exists());

    let materialized = dir.join("materialized");
    materialize(
        &cache,
        date(2026, 7, 12),
        date(2026, 7, 12),
        &materialized,
        &dir.join("materialized-index.json"),
    )
    .unwrap();
    fs::write(find_first_gzip(&materialized), b"corrupt-after-copy").unwrap();
    let replay_error = run_audit(AuditOptions {
        input: materialized,
        out: dir.join("corrupt-replay-audit.json"),
        markdown: dir.join("corrupt-replay-audit.md"),
        exclude_windows: Vec::new(),
    })
    .unwrap_err();
    assert!(replay_error.to_string().contains("failed size or SHA-256"));

    let gzip = find_first_gzip(&cache);
    fs::write(&gzip, b"corrupt").unwrap();
    let corrupt = materialize(
        &cache,
        date(2026, 7, 12),
        date(2026, 7, 12),
        &dir.join("corrupt"),
        &dir.join("corrupt-index.json"),
    )
    .unwrap_err();
    assert!(corrupt.to_string().contains("failed size or SHA-256"));
    assert!(!dir.join("corrupt").exists());
}

fn normalize(input: &Path, out: &Path) -> PathBuf {
    run_normalize(NormalizeOptions {
        input: input.to_path_buf(),
        out: out.to_path_buf(),
        format: "jsonl-indexed-gzip-sharded".to_owned(),
        overwrite: true,
        decision_grade_projection: true,
    })
    .unwrap();
    out.to_path_buf()
}

fn publish_day(normalized: &Path, day: NaiveDate, cache: &Path, out: &Path) -> Value {
    run_publish_projected_day(PublishProjectedDayOptions {
        normalized: normalized.to_path_buf(),
        date: day,
        campaign_id: CAMPAIGN.to_owned(),
        cache_root: cache.to_string_lossy().into_owned(),
        out: out.to_path_buf(),
    })
    .unwrap()
}

fn materialize(
    cache: &Path,
    since: NaiveDate,
    through: NaiveDate,
    out: &Path,
    manifest: &Path,
) -> Result<Value, polyedge_reporting::research::ResearchError> {
    run_materialize_projected_campaign(MaterializeProjectedCampaignOptions {
        since,
        through,
        campaign_id: CAMPAIGN.to_owned(),
        cache_root: cache.to_string_lossy().into_owned(),
        out: out.to_path_buf(),
        manifest: manifest.to_path_buf(),
    })
}

fn build_markets(input: &Path, out: &Path) -> Value {
    run_build_markets(BuildMarketsOptions {
        input: input.to_path_buf(),
        out: out.to_path_buf(),
        markdown: out.with_extension("md"),
        exclude_windows: Vec::new(),
    })
    .unwrap()
}

fn regimes(input: &Path, markets: &Path, out: &Path) -> Value {
    run_regimes(RegimesOptions {
        input: input.to_path_buf(),
        markets: Some(markets.to_path_buf()),
        fill_model: FillModel::QueueProxyConservative,
        profile_config: None,
        out: out.to_path_buf(),
        markdown: out.with_extension("md"),
        exclude_windows: Vec::new(),
    })
    .unwrap()
}

fn cross_midnight_day_one() -> String {
    [
        r#"{"event_type":"market","payload":{"market_id":"m1","condition_id":"c1","market_slug":"m1","up_token_id":"up","down_token_id":"down","start_ts":"2026-07-12T23:45:00Z","end_ts":"2026-07-13T00:00:10Z","start_price":"100"},"recorded_ts":"2026-07-12T23:45:00Z"}"#,
        r#"{"event_type":"fair_value","payload":{"market_id":"m1","q_up":"0.60","q_down":"0.40","sigma":"0.2"},"recorded_ts":"2026-07-12T23:59:57Z"}"#,
        r#"{"event_type":"book","payload":{"token_id":"up","bids":[{"price":"0.50","size":"2"}],"asks":[{"price":"0.60","size":"10"}],"local_ts":"2026-07-12T23:59:58Z"},"recorded_ts":"2026-07-12T23:59:58Z"}"#,
        r#"{"event_type":"decision","payload":{"action":"place","market_id":"m1","token_id":"up","outcome":"up","side":"buy","price":"0.50","size":"1","order_kind":"post_only_gtc","ttl_ms":20000,"expected_edge":"0.02","tick_size":"0.01"},"recorded_ts":"2026-07-12T23:59:59Z"}"#,
        r#"{"event_type":"paper_order_queue_registration","payload":{"order_id":"o1","market_id":"m1","token_id":"up","side":"buy","quote_price":"0.50","order_size":"1"},"recorded_ts":"2026-07-12T23:59:59.010Z"}"#,
        r#"{"event_type":"paper_order_queue_snapshot","payload":{"order_id":"o1","market_id":"m1","token_id":"up","side":"buy","quote_price":"0.50","order_size":"1","visible_size_ahead_estimate":"2"},"recorded_ts":"2026-07-12T23:59:59.401Z"}"#,
        // This sampled state remains pending until the projection flushes at
        // the UTC partition boundary. Combined normalization flushes it on
        // the next day's first event; both replay paths must remain equal.
        r#"{"event_type":"book","payload":{"token_id":"down","bids":[{"price":"0.40","size":"3"}],"asks":[{"price":"0.50","size":"4"}],"local_ts":"2026-07-12T23:59:59.900Z"},"recorded_ts":"2026-07-12T23:59:59.900Z"}"#,
    ]
    .join("\n")
}

fn cross_midnight_day_two() -> String {
    [
        r#"{"event_type":"last_trade_price","payload":{"token_id":"up","price":"0.50","size":"4","side":"sell","local_ts":"2026-07-13T00:00:01Z"},"recorded_ts":"2026-07-13T00:00:01Z"}"#,
        r#"{"event_type":"reference","payload":{"price":"101","source_ts":"2026-07-13T00:00:10Z","stale":false},"recorded_ts":"2026-07-13T00:00:10Z"}"#,
    ]
    .join("\n")
}

fn simple_day() -> String {
    [
        r#"{"event_type":"market","payload":{"market_id":"m1","up_token_id":"up","down_token_id":"down","start_ts":"2026-07-12T00:00:00Z","end_ts":"2026-07-12T00:15:00Z","start_price":"100"},"recorded_ts":"2026-07-12T00:00:00Z"}"#,
        r#"{"event_type":"reference","payload":{"price":"101","source_ts":"2026-07-12T00:15:00Z","stale":false},"recorded_ts":"2026-07-12T00:15:00Z"}"#,
    ]
    .join("\n")
}

fn find_first_gzip(root: &Path) -> PathBuf {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".jsonl.gz"))
            {
                return path;
            }
        }
    }
    panic!("projected cache did not contain a gzip shard")
}

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "polyedge-projected-cache-{name}-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}
