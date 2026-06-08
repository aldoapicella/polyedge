from __future__ import annotations

from decimal import Decimal, ROUND_CEILING
from typing import Any


def render_markdown(payload: dict[str, Any]) -> str:
    summary = payload["pnl_summary"]
    actual = summary["actual_runtime_paper"]
    replay = summary["replay_estimate"]
    health = payload["deployment_health"]
    coverage = payload["data_coverage"]
    actual_stats = payload["market_level_statistics"]["actual_runtime"]
    replay_stats = payload["market_level_statistics"]["replay_estimate"]
    fill_quality = payload["fill_quality"]
    lines = [
        "# BTC 15m Bot 24h Paper Analysis",
        "",
        "## 1. Executive Summary",
        "",
        (
            "Current status is operationally healthy but statistically inconclusive. "
            f"The rolling 24h window produced actual runtime paper PnL of {actual['net_pnl']} "
            f"and replay-estimated PnL of {replay['net_pnl']}. The sample is useful for "
            "measurement and diagnostics, but one rolling day is not enough to prove profitability."
        ),
        "",
        "## 2. Deployment Health",
        "",
        f"- Mode: {health['health'].get('execution_mode')}",
        f"- Kill switch: {health['health'].get('kill_switch')}",
        f"- Started at: {health['status'].get('started_at')}",
        f"- Recorder: {health['status'].get('azure_recorder')}",
        f"- Reference: {health['status'].get('reference')}",
        f"- Open orders: {health['status'].get('tracked_open_orders')}",
        f"- Paper fill: {health['status'].get('paper_fill')}",
        "",
        "## 3. Data Coverage",
        "",
        f"- Window type: {payload['window']['type']}",
        f"- Window: {payload['window']['start_ts']} to {payload['window']['end_ts']}",
        f"- Minute blobs: {payload['window']['minute_blob_count']}",
        f"- Missing minutes: {payload['window']['missing_minute_count']}",
        f"- Data size GiB: {payload['window']['total_gib']:.3f}",
        f"- Event count: {coverage['event_count']}",
        f"- Event type counts: {coverage['event_type_counts']}",
        f"- Markets seen: {coverage['markets_seen']}",
        f"- Markets with start price: {coverage['markets_with_start_price']}",
        f"- Markets settled: {coverage['markets_settled']}",
        f"- Completed settled markets in window: {coverage['completed_settled_markets_in_window']}",
        f"- Start-price capture rate: {coverage['start_price_capture_rate']}",
        f"- Settlement rate: {coverage['settlement_rate']}",
        "",
        "## 4. PnL Summary",
        "",
        f"- Actual runtime net PnL: {actual['net_pnl']}",
        f"- Actual runtime notional cost: {actual['notional_cost']}",
        f"- Actual runtime ROI on cost: {actual['roi_on_cost']}",
        f"- Replay net PnL: {replay['net_pnl']}",
        f"- Replay notional cost: {replay['notional_cost']}",
        f"- Replay ROI on cost: {replay['roi_on_cost']}",
        f"- Runtime vs replay fill delta: {summary['runtime_vs_replay']['runtime_vs_replay_fill_delta']}",
        f"- Runtime vs replay PnL delta: {summary['runtime_vs_replay']['runtime_vs_replay_pnl_delta']}",
        f"- Execution report status counts: {actual['execution_report_status_counts']}",
        f"- Replay metrics: {replay['replay_metrics']}",
        "",
        "## 5. Market-Level Statistics",
        "",
        "### Actual Runtime",
        "",
        stats_lines(actual_stats),
        "",
        "### Replay Estimate",
        "",
        stats_lines(replay_stats),
        "",
        "## 6. Time Bucket Analysis",
        "",
        markdown_table(
            payload["time_bucket_analysis"],
            [
                "bucket",
                "orders_placed",
                "cancels",
                "runtime_fills",
                "replay_fills",
                "actual_runtime_pnl",
                "replay_pnl",
                "avg_expected_edge",
                "runtime_fill_rate",
                "replay_fill_rate",
            ],
        ),
        "",
        "## 7. Calibration Analysis",
        "",
        markdown_table(
            payload["calibration_analysis"],
            [
                "bucket",
                "count_of_settled_market_decisions",
                "observed_up_frequency",
                "average_q_up",
                "calibration_error",
                "actual_runtime_pnl",
                "replay_pnl",
            ],
        ),
        "",
        "## 8. Fill Quality and Replay Realism",
        "",
        f"- Runtime paper fills: {fill_quality['runtime_paper_fills']}",
        f"- Offline replay fills: {fill_quality['offline_replay_fills']}",
        f"- Runtime minus replay fills: {fill_quality['runtime_minus_replay_fills']}",
        f"- Replay more optimistic than runtime: {fill_quality['replay_more_optimistic_than_runtime']}",
        f"- Paper resting reports: {fill_quality['paper_resting_reports']}",
        f"- Paper cancelled reports: {fill_quality['paper_cancelled_reports']}",
        f"- Paper filled maker reports: {fill_quality['paper_filled_maker_reports']}",
        f"- Fills after cancel prevented: {fill_quality['fills_after_cancel_prevented']}",
        f"- Fills prevented before live: {fill_quality['fills_prevented_not_live']}",
        f"- Fills prevented by TTL expiry: {fill_quality['fills_prevented_expired']}",
        f"- Fills prevented in final window: {fill_quality['fills_prevented_final_window']}",
        f"- Winning replay fills: {fill_quality['winning_replay_fills']}",
        f"- Losing replay fills: {fill_quality['losing_replay_fills']}",
        f"- Unfilled orders that would have won: {fill_quality['unfilled_orders_that_would_have_won']}",
        "",
        "## 9. Bugs or Risks Found",
        "",
        markdown_table(payload["bugs_or_risks"], ["severity", "status", "area", "finding", "evidence"]),
        "",
        "## 10. Statistical Evidence and Required Sample Size",
        "",
        (
            "This 24h sample is operationally useful but statistically inconclusive. "
            "Use the market-level mean, standard deviation, standard error, and confidence interval "
            "above. If the 95% confidence interval includes zero, positive expected value is not proven."
        ),
        "",
        f"- Actual required markets for +/-$0.05 precision: {actual_stats['required_markets_for_0_05_precision']}",
        f"- Actual required markets for +/-$0.10 precision: {actual_stats['required_markets_for_0_10_precision']}",
        f"- Actual required markets to detect current mean: {actual_stats['required_markets_to_detect_current_mean']}",
        f"- Replay required markets for +/-$0.05 precision: {replay_stats['required_markets_for_0_05_precision']}",
        f"- Replay required markets for +/-$0.10 precision: {replay_stats['required_markets_for_0_10_precision']}",
        f"- Replay required markets to detect current mean: {replay_stats['required_markets_to_detect_current_mean']}",
        "",
        "## 11. Recommendation",
        "",
        payload["recommendation"],
        "",
        "## 12. Next 5 Actions",
        "",
    ]
    lines.extend(f"{idx}. {item}" for idx, item in enumerate(payload["next_5_actions"], start=1))
    lines.extend(
        [
            "",
            "## Commands And Endpoints Used",
            "",
            "- `GET /health` with bearer auth",
            "- `GET /status` with bearer auth",
            "- `GET /reports/latest` with bearer auth",
            "- `GET /reports/daily/2026-06-03` with bearer auth",
            "- `az storage blob list --prefix events/`",
            "- `scripts/analyze_24h_paper.py` streamed selected Azure Blob event data",
        ]
    )
    return "\n".join(lines) + "\n"


def stats_lines(stats: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"- n settled markets: {stats['n_settled_markets']}",
            f"- mean net PnL: {stats['mean_net_pnl_per_settled_market']}",
            f"- median net PnL: {stats['median_net_pnl_per_settled_market']}",
            f"- std net PnL: {stats['std_net_pnl_per_settled_market']}",
            f"- standard error: {stats['standard_error']}",
            f"- approx 95% CI: {stats['approx_95ci_low']} to {stats['approx_95ci_high']}",
            f"- best market: {stats['best_market']}",
            f"- worst market: {stats['worst_market']}",
            f"- max drawdown: {stats['max_drawdown']}",
            f"- profitable markets: {stats['profitable_markets']} ({stats['profitable_market_pct']})",
            f"- losing markets: {stats['losing_markets']} ({stats['losing_market_pct']})",
        ]
    )


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "_No rows._"
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = []
    for row in rows:
        body.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
    return "\n".join([header, divider, *body])


def sample_std(values: list[Decimal], mean: Decimal | None) -> Decimal | None:
    if mean is None or len(values) < 2:
        return None
    variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values) - 1)
    return variance.sqrt()


def max_drawdown(values: list[Decimal]) -> Decimal:
    peak = Decimal("0")
    equity = Decimal("0")
    drawdown = Decimal("0")
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return drawdown


def required_precision(std: Decimal | None, margin: Decimal) -> int | None:
    if std is None:
        return None
    if std == 0:
        return 1
    return int(((Decimal("1.96") * std / margin) ** 2).to_integral_value(rounding=ROUND_CEILING))


def required_detect_mean(std: Decimal | None, mean: Decimal | None) -> int | None:
    if std is None or mean is None or mean == 0:
        return None
    if std == 0:
        return 1
    return int((Decimal("7.84") * (std / abs(mean)) ** 2).to_integral_value(rounding=ROUND_CEILING))


def market_summary(row: dict[str, Any] | None, key: str) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "market_id": row["market_id"],
        "market_slug": row["market_slug"],
        "end_ts": row["end_ts"],
        "value": row[key],
    }


def included_hour_prefixes(blobs: list[dict[str, Any]]) -> list[str]:
    return sorted({"/".join(blob["name"].split("/")[:5]) + "/" for blob in blobs})
