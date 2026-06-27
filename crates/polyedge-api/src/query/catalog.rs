use serde_json::{json, Value};

use super::{DEFAULT_LIMIT, MAX_LIMIT};

pub(super) fn datasets() -> Vec<Value> {
    vec![
        dataset(
            "markets",
            "Markets",
            &["asset", "horizon", "status", "outcome"],
            &["count", "mean_q_up"],
        ),
        dataset(
            "decisions",
            "Decisions",
            &["market_id", "action", "outcome", "candidate"],
            &["count", "mean_q_up"],
        ),
        dataset(
            "fills",
            "Fills",
            &["market_id", "status", "outcome", "fill_model", "regime"],
            &["count", "fill_count", "avg_pnl"],
        ),
        dataset(
            "reports",
            "Reports",
            &["date", "quality", "candidate"],
            &["count", "net_pnl", "max_drawdown"],
        ),
        dataset(
            "data_quality",
            "Data Quality",
            &["status", "quality_flag", "date"],
            &["count"],
        ),
        dataset(
            "jobs",
            "Jobs",
            &["job_id", "job_type", "status", "trigger"],
            &["count"],
        ),
        dataset(
            "artifacts",
            "Artifacts",
            &["kind", "date", "quality", "source_job"],
            &["count"],
        ),
        dataset(
            "regimes",
            "Regimes",
            &["regime", "profile", "candidate"],
            &["count", "net_pnl", "fill_count", "cancel_per_fill"],
        ),
        dataset(
            "calibration",
            "Calibration",
            &["q_bucket", "time_to_expiry_bucket", "distance_bucket"],
            &["count", "observed_up_rate", "brier_score"],
        ),
        dataset(
            "fill_models",
            "Fill Models",
            &["fill_model", "candidate"],
            &["count", "net_pnl", "fill_count", "cancel_per_fill"],
        ),
        dataset(
            "sample_size",
            "Sample Size",
            &["candidate", "date"],
            &["count", "avg_pnl"],
        ),
        dataset(
            "market_truth",
            "Market Truth",
            &[
                "market_id",
                "asset",
                "horizon",
                "winning_outcome",
                "quality",
            ],
            &["count"],
        ),
        dataset(
            "decision_features",
            "Decision Features",
            &[
                "market_id",
                "action",
                "outcome",
                "regime",
                "time_to_expiry_bucket",
            ],
            &["count", "mean_q_up"],
        ),
        dataset(
            "fill_candidates",
            "Fill Candidates",
            &["market_id", "status", "outcome", "fill_model", "source"],
            &["count", "fill_count", "fill_rate"],
        ),
        dataset(
            "queue_evidence",
            "Queue Evidence",
            &["market_id", "event_type", "token_id", "side", "source"],
            &["count"],
        ),
        dataset(
            "queue_proxy_results",
            "Queue Proxy Results",
            &["fill_model", "queue_proxy_mode", "candidate", "quality"],
            &["count", "net_pnl", "fill_count", "fill_rate"],
        ),
        dataset(
            "prospective_daily",
            "Prospective Daily",
            &["candidate", "date", "recommendation", "quality"],
            &["count", "net_pnl", "avg_pnl"],
        ),
        dataset(
            "candidate_market_pnl",
            "Candidate Market PnL",
            &["candidate", "market_id", "date", "quality"],
            &["count", "net_pnl", "avg_pnl"],
        ),
        dataset(
            "regime_market_pnl",
            "Regime Market PnL",
            &["regime", "profile", "market_id", "quality"],
            &["count", "net_pnl", "fill_count"],
        ),
        dataset(
            "calibration_buckets",
            "Calibration Buckets",
            &[
                "q_bucket",
                "time_to_expiry_bucket",
                "distance_bucket",
                "quality",
            ],
            &["count", "observed_up_rate", "brier_score"],
        ),
    ]
}

pub(super) fn templates() -> Vec<Value> {
    vec![
        template(
            "toxic_fills",
            "Toxic fills",
            "fills",
            vec![filter("status", "contains", json!("fill"))],
            vec!["outcome"],
            vec!["count", "fill_count", "avg_pnl"],
        ),
        template(
            "losing_regimes",
            "Losing regimes",
            "regimes",
            vec![filter("net_pnl", "lt", json!(0))],
            vec!["regime"],
            vec!["count", "net_pnl", "fill_count"],
        ),
        template(
            "high_q_up_down_won",
            "High q_up but Down won",
            "decisions",
            vec![
                filter("q_up", "gte", json!(0.65)),
                filter("outcome", "eq", json!("down")),
            ],
            vec!["market_id"],
            vec!["count", "mean_q_up"],
        ),
        template(
            "final_window_activity",
            "Final-window activity",
            "decisions",
            vec![filter("time_to_expiry_bucket", "contains", json!("final"))],
            vec!["action"],
            vec!["count"],
        ),
        template(
            "data_quality_exclusions",
            "Data-quality exclusions",
            "data_quality",
            vec![filter("default_exclude", "eq", json!(true))],
            vec!["quality_flag"],
            vec!["count"],
        ),
        template(
            "dynamic_beats_static",
            "Dynamic beats static",
            "reports",
            vec![filter(
                "dynamic_quote_style_net_pnl",
                "gt",
                json_path("static_net_pnl"),
            )],
            vec!["date"],
            vec!["count", "net_pnl"],
        ),
        template(
            "calibration_failures",
            "Calibration failures",
            "calibration",
            vec![filter("calibration_error", "gte", json!(0.05))],
            vec!["q_bucket"],
            vec!["count", "brier_score"],
        ),
        template(
            "missing_start_price",
            "Markets with missing start price",
            "markets",
            vec![filter("start_price", "eq", Value::Null)],
            vec!["status"],
            vec!["count"],
        ),
        template(
            "large_drawdown",
            "Markets with large drawdown",
            "reports",
            vec![filter("max_drawdown", "lt", json!(-5))],
            vec!["date"],
            vec!["count", "max_drawdown"],
        ),
        template(
            "regime_switch_storms",
            "Regime switch storms",
            "regimes",
            vec![filter("regime_switches", "gte", json!(10))],
            vec!["regime"],
            vec!["count"],
        ),
        template(
            "queue_evidence_coverage",
            "Queue evidence coverage",
            "queue_evidence",
            vec![],
            vec!["event_type"],
            vec!["count"],
        ),
        template(
            "queue_proxy_fill_realism",
            "QueueProxy fill realism",
            "queue_proxy_results",
            vec![],
            vec!["fill_model"],
            vec!["count", "fill_count", "fill_rate", "net_pnl"],
        ),
        template(
            "prospective_candidate_progress",
            "Prospective candidate progress",
            "prospective_daily",
            vec![],
            vec!["candidate", "recommendation"],
            vec!["count", "net_pnl", "avg_pnl"],
        ),
    ]
}

fn dataset(id: &str, label: &str, filters: &[&str], metrics: &[&str]) -> Value {
    json!({
        "id": id,
        "label": label,
        "filters": filters,
        "group_by": filters,
        "metrics": metrics,
        "default_limit": DEFAULT_LIMIT,
        "max_limit": MAX_LIMIT
    })
}

fn template(
    id: &str,
    name: &str,
    dataset: &str,
    filters: Vec<Value>,
    group_by: Vec<&str>,
    metrics: Vec<&str>,
) -> Value {
    json!({
        "id": id,
        "name": name,
        "description": format!("{name} structured query template."),
        "request": {
            "dataset": dataset,
            "filters": filters,
            "group_by": group_by,
            "metrics": metrics,
            "limit": DEFAULT_LIMIT
        }
    })
}

fn filter(field: &str, op: &str, value: Value) -> Value {
    json!({ "field": field, "op": op, "value": value })
}

fn json_path(field: &str) -> Value {
    json!({ "field_ref": field })
}
