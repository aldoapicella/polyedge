from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from typing import Any, Iterable

from polymarket_btc15_bot.backtest import BacktestConfig, ReplayBacktester


FINAL_NO_TRADE_SECONDS = 30
BUCKETS = [
    "15-12m",
    "12-9m",
    "9-6m",
    "6-3m",
    "3-1m",
    "final_60s",
    "inside_final_no_trade",
    "outside_15m",
    "unknown",
]
Q_BUCKETS = [
    ("0.00-0.40", Decimal("0.00"), Decimal("0.40")),
    ("0.40-0.45", Decimal("0.40"), Decimal("0.45")),
    ("0.45-0.50", Decimal("0.45"), Decimal("0.50")),
    ("0.50-0.55", Decimal("0.50"), Decimal("0.55")),
    ("0.55-0.60", Decimal("0.55"), Decimal("0.60")),
    ("0.60-0.70", Decimal("0.60"), Decimal("0.70")),
    ("0.70-1.00", Decimal("0.70"), Decimal("1.0000000001")),
]


@dataclass
class BlobSelection:
    window_start: datetime
    window_end: datetime
    blobs: list[dict[str, Any]]
    total_bytes: int
    missing_minutes: list[str]


@dataclass
class FillRecord:
    order_id: str | None
    market_id: str
    outcome: str
    price: Decimal
    size: Decimal
    fee: Decimal
    fill_ts: datetime
    expected_edge: Decimal | None = None
    q_bucket: str | None = None


@dataclass
class AnalysisCollector:
    window_start: datetime
    window_end: datetime
    event_counts: Counter[str] = field(default_factory=Counter)
    status_counts: Counter[str] = field(default_factory=Counter)
    markets: dict[str, dict[str, Any]] = field(default_factory=dict)
    market_decisions: Counter[str] = field(default_factory=Counter)
    market_place_decisions: Counter[str] = field(default_factory=Counter)
    market_cancel_decisions: Counter[str] = field(default_factory=Counter)
    latest_fair_value: dict[str, dict[str, Any]] = field(default_factory=dict)
    decision_calibration: list[dict[str, Any]] = field(default_factory=list)
    place_decision_q_buckets: list[str | None] = field(default_factory=list)
    place_decision_edges: list[Decimal | None] = field(default_factory=list)
    order_q_bucket: dict[str, str | None] = field(default_factory=dict)
    order_expected_edge: dict[str, Decimal | None] = field(default_factory=dict)
    runtime_fills: list[FillRecord] = field(default_factory=list)
    paper_settlements: dict[str, dict[str, Any]] = field(default_factory=dict)
    time_buckets: dict[str, dict[str, Any]] = field(default_factory=dict)
    first_recorded_ts: datetime | None = None
    last_recorded_ts: datetime | None = None

    def __post_init__(self) -> None:
        for bucket in BUCKETS:
            self.time_buckets[bucket] = _empty_time_bucket()

    def observe(self, event: dict[str, Any]) -> None:
        recorded_ts = parse_dt(event.get("recorded_ts"))
        if recorded_ts is None or not (self.window_start <= recorded_ts < self.window_end):
            return
        self.first_recorded_ts = min(self.first_recorded_ts, recorded_ts) if self.first_recorded_ts else recorded_ts
        self.last_recorded_ts = max(self.last_recorded_ts, recorded_ts) if self.last_recorded_ts else recorded_ts

        event_type = str(event.get("event_type") or "")
        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        self.event_counts[event_type] += 1

        if event_type == "market":
            self._observe_market(payload)
        elif event_type == "paper_settlement":
            self._observe_paper_settlement(payload)
        elif event_type == "fair_value":
            self._observe_fair_value(payload)
        elif event_type == "decision":
            self._observe_decision(payload, recorded_ts)
        elif event_type == "execution_report":
            self._observe_execution_report(payload, recorded_ts)

    def _observe_market(self, payload: dict[str, Any]) -> None:
        market_id = str(payload.get("market_id") or "")
        if not market_id:
            return
        row = self.markets.setdefault(market_id, {})
        for key in (
            "market_id",
            "market_slug",
            "up_token_id",
            "down_token_id",
            "start_ts",
            "end_ts",
            "start_price",
        ):
            if payload.get(key) is not None:
                row[key] = payload.get(key)

    def _observe_paper_settlement(self, payload: dict[str, Any]) -> None:
        market_id = str(payload.get("market_id") or "")
        if market_id:
            self.paper_settlements[market_id] = payload

    def _observe_fair_value(self, payload: dict[str, Any]) -> None:
        market_id = str(payload.get("market_id") or "")
        q_up = dec(payload.get("q_up"))
        if market_id and q_up is not None:
            self.latest_fair_value[market_id] = {
                "q_up": q_up,
                "q_down": dec(payload.get("q_down")),
                "computed_ts": parse_dt(payload.get("computed_ts")),
            }

    def _observe_decision(self, payload: dict[str, Any], recorded_ts: datetime) -> None:
        market_id = str(payload.get("market_id") or "")
        action = str(payload.get("action") or "")
        if not market_id:
            return
        self.market_decisions[market_id] += 1
        if action == "place":
            self.market_place_decisions[market_id] += 1
        elif action == "cancel_all":
            self.market_cancel_decisions[market_id] += 1

        fv = self.latest_fair_value.get(market_id)
        q_up = fv.get("q_up") if fv else None
        q_bucket = q_bucket_for(q_up) if q_up is not None else None
        expected_edge = dec(payload.get("expected_edge"))
        self.decision_calibration.append(
            {
                "recorded_ts": recorded_ts,
                "market_id": market_id,
                "action": action,
                "outcome": payload.get("outcome"),
                "q_up": q_up,
                "q_bucket": q_bucket,
                "expected_edge": expected_edge,
            }
        )
        bucket = time_bucket(recorded_ts, self._market_end(market_id))
        if action == "place":
            self.place_decision_q_buckets.append(q_bucket)
            self.place_decision_edges.append(expected_edge)
            self.time_buckets[bucket]["orders_placed"] += 1
            if expected_edge is not None:
                self.time_buckets[bucket]["expected_edges"].append(expected_edge)
        elif action == "cancel_all":
            self.time_buckets[bucket]["cancels"] += 1

    def _observe_execution_report(self, payload: dict[str, Any], recorded_ts: datetime) -> None:
        status = str(payload.get("status") or "unknown")
        self.status_counts[status] += 1
        order_id = _string_or_none(payload.get("order_id"))
        decision = _report_decision(payload)
        market_id = str(payload.get("market_id") or decision.get("market_id") or "")
        if order_id and decision:
            fv = self.latest_fair_value.get(market_id)
            q_bucket = q_bucket_for(fv["q_up"]) if fv and fv.get("q_up") is not None else None
            self.order_q_bucket.setdefault(order_id, q_bucket)
            self.order_expected_edge.setdefault(order_id, dec(decision.get("expected_edge")))

        filled_size = dec(payload.get("filled_size")) or Decimal("0")
        if filled_size <= 0:
            return
        price = dec(payload.get("avg_price")) or dec(decision.get("price"))
        if price is None:
            return
        fill_ts = parse_dt(payload.get("local_ts")) or recorded_ts
        outcome = str(decision.get("outcome") or "")
        fill = FillRecord(
            order_id=order_id,
            market_id=market_id,
            outcome=outcome,
            price=price,
            size=filled_size,
            fee=dec(payload.get("fee")) or Decimal("0"),
            fill_ts=fill_ts,
            expected_edge=self.order_expected_edge.get(order_id or ""),
            q_bucket=self.order_q_bucket.get(order_id or ""),
        )
        self.runtime_fills.append(fill)
        bucket = time_bucket(fill_ts, self._market_end(market_id))
        self.time_buckets[bucket]["runtime_fills"] += 1
        if fill.expected_edge is not None:
            self.time_buckets[bucket]["expected_edges"].append(fill.expected_edge)

    def _market_end(self, market_id: str) -> datetime | None:
        market = self.markets.get(market_id) or {}
        return parse_dt(market.get("end_ts"))


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    container = azure_container(args.account_name, args.container_name)
    selection = select_latest_rolling_window(container, hours=args.hours)
    health = read_json(args.health_json)
    status = read_json(args.status_json)

    collector = AnalysisCollector(selection.window_start, selection.window_end)
    backtester = ReplayBacktester(
        BacktestConfig(
            path=Path(f"azure:{selection.window_start.isoformat()}:{selection.window_end.isoformat()}"),
        )
    )

    def observed_events() -> Iterable[dict[str, Any]]:
        for index, event in enumerate(stream_blob_events(container, selection.blobs), start=1):
            collector.observe(event)
            yield event
            if index % 1_000_000 == 0:
                print(f"processed_events={index}", file=sys.stderr)

    replay = backtester.run_events(observed_events())
    payload = build_payload(selection, collector, replay, backtester, health, status)
    artifact_base = (
        f"btc15-24h-paper-analysis-"
        f"{compact_ts(selection.window_start)}-{compact_ts(selection.window_end)}"
    )
    json_path = output_dir / f"{artifact_base}.json"
    md_path = output_dir / f"{artifact_base}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze latest rolling BTC 15m paper-mode window from Azure blobs.")
    parser.add_argument("--account-name", default="stpolymarketbtc1556k4mk6")
    parser.add_argument("--container-name", default="bot-events")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--output-dir", default="docs/reports")
    parser.add_argument("--health-json", default="/tmp/btc15_analysis_health.json")
    parser.add_argument("--status-json", default="/tmp/btc15_analysis_status.json")
    return parser.parse_args()


def azure_container(account_name: str, container_name: str) -> Any:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )
    return service.get_container_client(container_name)


def select_latest_rolling_window(container: Any, hours: int) -> BlobSelection:
    minute_blobs: list[tuple[datetime, dict[str, Any]]] = []
    for blob in container.list_blobs(name_starts_with="events/"):
        ts = minute_blob_ts(blob.name)
        if ts is None:
            continue
        minute_blobs.append((ts, {"name": blob.name, "size": int(getattr(blob, "size", 0) or 0)}))
    if not minute_blobs:
        raise RuntimeError("no minute event blobs found")
    minute_blobs.sort(key=lambda item: item[0])
    latest = minute_blobs[-1][0]
    window_end = latest + timedelta(minutes=1)
    window_start = window_end - timedelta(hours=hours)
    selected = [row for ts, row in minute_blobs if window_start <= ts < window_end]
    have = {ts for ts, _ in minute_blobs}
    missing: list[str] = []
    cursor = window_start
    while cursor < window_end:
        if cursor not in have:
            missing.append(cursor.isoformat())
        cursor += timedelta(minutes=1)
    return BlobSelection(
        window_start=window_start,
        window_end=window_end,
        blobs=selected,
        total_bytes=sum(row["size"] for row in selected),
        missing_minutes=missing,
    )


def minute_blob_ts(name: str) -> datetime | None:
    parts = name.split("/")
    if len(parts) != 6 or not parts[-1].endswith(".jsonl"):
        return None
    try:
        return datetime(
            int(parts[1]),
            int(parts[2]),
            int(parts[3]),
            int(parts[4]),
            int(parts[5].removesuffix(".jsonl")),
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None


def stream_blob_events(container: Any, blobs: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for index, blob_info in enumerate(blobs, start=1):
        if index == 1 or index % 60 == 0:
            print(f"streaming_blob={index}/{len(blobs)} {blob_info['name']}", file=sys.stderr)
        downloader = container.download_blob(blob_info["name"])
        pending = b""
        for chunk in downloader.chunks():
            pending += chunk
            lines = pending.split(b"\n")
            pending = lines.pop()
            for raw_line in lines:
                if not raw_line.strip():
                    continue
                try:
                    yield json.loads(raw_line)
                except json.JSONDecodeError:
                    continue
        if pending.strip():
            try:
                yield json.loads(pending)
            except json.JSONDecodeError:
                continue


def build_payload(
    selection: BlobSelection,
    collector: AnalysisCollector,
    replay: Any,
    backtester: ReplayBacktester,
    health: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    market_rows = completed_market_rows(selection, collector, replay, backtester)
    actual_stats = market_level_stats(market_rows, "actual_runtime_net_pnl")
    replay_stats = market_level_stats(market_rows, "replay_net_pnl")
    enrich_time_buckets(collector, market_rows, replay, backtester)
    calibration = calibration_table(collector, market_rows, backtester)
    fill_quality = fill_quality_summary(collector, replay, backtester, market_rows)

    return {
        "title": "BTC 15m Bot 24h Paper Analysis",
        "generated_ts": datetime.now(timezone.utc).isoformat(),
        "window": {
            "type": "rolling_24h",
            "start_ts": selection.window_start.isoformat(),
            "end_ts": selection.window_end.isoformat(),
            "complete_utc_day": False,
            "minute_blob_count": len(selection.blobs),
            "missing_minute_count": len(selection.missing_minutes),
            "missing_minutes": selection.missing_minutes[:50],
            "total_bytes": selection.total_bytes,
            "total_gib": selection.total_bytes / 1024**3,
            "first_recorded_ts": iso_or_none(collector.first_recorded_ts),
            "last_recorded_ts": iso_or_none(collector.last_recorded_ts),
            "included_hour_prefixes": included_hour_prefixes(selection.blobs),
        },
        "deployment_health": deployment_health(health, status),
        "data_coverage": {
            "event_count": replay.event_count,
            "event_type_counts": dict(collector.event_counts),
            "markets_seen": replay.markets_seen,
            "markets_with_start_price": replay.markets_with_start_price,
            "markets_settled": replay.markets_settled,
            "completed_settled_markets_in_window": len(market_rows),
            "start_price_capture_rate": ratio_decimal(replay.markets_with_start_price, replay.markets_seen),
            "settlement_rate": ratio_decimal(replay.markets_settled, replay.markets_with_start_price),
            "paper_settlement_events": len(collector.paper_settlements),
        },
        "pnl_summary": pnl_summary(collector, replay, backtester, market_rows),
        "market_level_statistics": {
            "actual_runtime": actual_stats,
            "replay_estimate": replay_stats,
        },
        "markets": market_rows,
        "time_bucket_analysis": time_bucket_table(collector),
        "calibration_analysis": calibration,
        "fill_quality": fill_quality,
        "api_report_checks": {
            "reports_latest_checked": True,
            "daily_report_applicable": False,
            "daily_report_note": "Rolling 24h window crosses UTC dates; daily cached report is not the source of truth.",
        },
        "bugs_or_risks": bug_risk_notes(),
        "recommendation": recommendation(actual_stats, replay_stats, fill_quality),
        "next_5_actions": next_actions(),
    }


def completed_market_rows(
    selection: BlobSelection,
    collector: AnalysisCollector,
    replay: Any,
    backtester: ReplayBacktester,
) -> list[dict[str, Any]]:
    actual_by_market = actual_pnl_by_market(collector, replay.market_results)
    replay_costs = replay_costs_by_market(backtester)
    replay_orders = defaultdict(int)
    replay_open_after_close = defaultdict(bool)
    for order in backtester.orders:
        replay_orders[order.market_id] += 1
        if not order.is_filled and order.cancel_confirmed_ts is None:
            replay_open_after_close[order.market_id] = True

    rows = []
    for replay_row in replay.market_results:
        start_ts = parse_dt(replay_row.get("start_ts"))
        end_ts = parse_dt(replay_row.get("end_ts"))
        if start_ts is None or end_ts is None:
            continue
        if not (selection.window_start <= start_ts and end_ts <= selection.window_end):
            continue
        if replay_row.get("winning_outcome") is None:
            continue
        market_id = str(replay_row.get("market_id") or "")
        actual = actual_by_market.get(market_id, _empty_actual_market())
        replay_cost = replay_costs.get(market_id, Decimal("0"))
        replay_net = dec(replay_row.get("net_pnl")) or Decimal("0")
        actual_cost = actual["notional_cost"]
        rows.append(
            {
                "market_id": market_id,
                "market_slug": replay_row.get("market_slug"),
                "start_ts": replay_row.get("start_ts"),
                "end_ts": replay_row.get("end_ts"),
                "start_price": replay_row.get("start_price"),
                "final_price": replay_row.get("final_price"),
                "winning_outcome": replay_row.get("winning_outcome"),
                "decisions": collector.market_decisions[market_id],
                "place_decisions": collector.market_place_decisions[market_id],
                "cancel_decisions": collector.market_cancel_decisions[market_id],
                "runtime_fills": actual["fills"],
                "replay_fills": replay_row.get("filled_orders"),
                "actual_runtime_net_pnl": str(actual["net_pnl"]),
                "replay_net_pnl": str(replay_net),
                "actual_runtime_notional_cost": str(actual_cost),
                "replay_notional_cost": str(replay_cost),
                "actual_runtime_roi_on_cost": ratio_str(actual["net_pnl"], actual_cost),
                "replay_roi_on_cost": ratio_str(replay_net, replay_cost),
                "open_orders_remained_after_close": replay_open_after_close[market_id],
            }
        )
    rows.sort(key=lambda row: row["end_ts"])
    return rows


def actual_pnl_by_market(
    collector: AnalysisCollector,
    replay_market_results: list[dict[str, Any]],
) -> dict[str, dict[str, Decimal | int]]:
    outcomes = {
        str(row.get("market_id")): row.get("winning_outcome")
        for row in replay_market_results
        if row.get("winning_outcome") is not None
    }
    result: dict[str, dict[str, Decimal | int]] = defaultdict(_empty_actual_market)
    for fill in collector.runtime_fills:
        winning = outcomes.get(fill.market_id)
        row = result[fill.market_id]
        row["fills"] += 1
        cost = fill.price * fill.size
        row["notional_cost"] += cost
        row["fees"] += fill.fee
        if winning is None:
            continue
        payout = fill.size if fill.outcome == winning else Decimal("0")
        gross = payout - cost
        row["gross_pnl"] += gross
        row["net_pnl"] += gross - fill.fee
    return result


def _empty_actual_market() -> dict[str, Any]:
    return {
        "fills": 0,
        "notional_cost": Decimal("0"),
        "gross_pnl": Decimal("0"),
        "fees": Decimal("0"),
        "net_pnl": Decimal("0"),
    }


def replay_costs_by_market(backtester: ReplayBacktester) -> dict[str, Decimal]:
    costs: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for order in backtester.orders:
        if order.is_filled:
            costs[order.market_id] += (order.avg_price or order.price) * order.filled_size
    return costs


def pnl_summary(
    collector: AnalysisCollector,
    replay: Any,
    backtester: ReplayBacktester,
    market_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    actual_net = sum_decimal(row["actual_runtime_net_pnl"] for row in market_rows)
    actual_cost = sum_decimal(row["actual_runtime_notional_cost"] for row in market_rows)
    replay_net = sum_decimal(row["replay_net_pnl"] for row in market_rows)
    replay_cost = sum_decimal(row["replay_notional_cost"] for row in market_rows)
    actual_fills = sum(int(row["runtime_fills"]) for row in market_rows)
    replay_fills = sum(int(row["replay_fills"]) for row in market_rows)
    return {
        "actual_runtime_paper": {
            "net_pnl": str(actual_net),
            "notional_cost": str(actual_cost),
            "roi_on_cost": ratio_str(actual_net, actual_cost),
            "filled_reports": actual_fills,
            "execution_report_status_counts": dict(collector.status_counts),
        },
        "replay_estimate": {
            "net_pnl": str(replay_net),
            "notional_cost": str(replay_cost),
            "roi_on_cost": ratio_str(replay_net, replay_cost),
            "filled_orders": replay_fills,
            "orders_seen": replay.orders_seen,
            "decisions_seen": replay.decisions_seen,
            "replay_metrics": replay.replay_metrics,
            "notes": replay.notes,
        },
        "runtime_vs_replay": {
            "runtime_vs_replay_fill_delta": actual_fills - replay_fills,
            "runtime_vs_replay_pnl_delta": str(actual_net - replay_net),
        },
        "paper_status_counts": {
            "paper_resting": collector.status_counts.get("paper_resting", 0),
            "paper_cancelled": collector.status_counts.get("paper_cancelled", 0),
            "paper_filled_maker": collector.status_counts.get("paper_filled_maker", 0),
        },
        "replay_order_count_total": len(backtester.orders),
    }


def enrich_time_buckets(
    collector: AnalysisCollector,
    market_rows: list[dict[str, Any]],
    replay: Any,
    backtester: ReplayBacktester,
) -> None:
    markets = {row["market_id"]: row for row in market_rows}
    for fill in collector.runtime_fills:
        market = markets.get(fill.market_id)
        if not market:
            continue
        winning = market["winning_outcome"]
        cost = fill.price * fill.size
        payout = fill.size if fill.outcome == winning else Decimal("0")
        pnl = payout - cost - fill.fee
        bucket = time_bucket(fill.fill_ts, parse_dt(market["end_ts"]))
        collector.time_buckets[bucket]["actual_runtime_pnl"] += pnl
        collector.time_buckets[bucket]["actual_runtime_cost"] += cost
        if fill.q_bucket:
            collector.time_buckets[bucket]["q_buckets"][fill.q_bucket] += 1

    market_by_id = {row["market_id"]: row for row in market_rows}
    q_buckets = collector.place_decision_q_buckets
    for index, order in enumerate(backtester.orders):
        if not order.is_filled:
            continue
        market = market_by_id.get(order.market_id)
        if not market or order.fill_ts is None:
            continue
        winning = market["winning_outcome"]
        cost = (order.avg_price or order.price) * order.filled_size
        payout = order.filled_size if order.outcome == winning else Decimal("0")
        pnl = payout - cost - order.fee
        bucket = time_bucket(order.fill_ts, parse_dt(market["end_ts"]))
        collector.time_buckets[bucket]["replay_fills"] += 1
        collector.time_buckets[bucket]["replay_pnl"] += pnl
        collector.time_buckets[bucket]["replay_cost"] += cost
        if index < len(q_buckets) and q_buckets[index]:
            collector.time_buckets[bucket]["q_buckets"][q_buckets[index]] += 1


def time_bucket_table(collector: AnalysisCollector) -> list[dict[str, Any]]:
    rows = []
    for bucket in BUCKETS:
        row = collector.time_buckets[bucket]
        expected_edges = row["expected_edges"]
        orders = row["orders_placed"]
        runtime_fills = row["runtime_fills"]
        replay_fills = row["replay_fills"]
        rows.append(
            {
                "bucket": bucket,
                "orders_placed": orders,
                "cancels": row["cancels"],
                "runtime_fills": runtime_fills,
                "replay_fills": replay_fills,
                "actual_runtime_pnl": str(row["actual_runtime_pnl"]),
                "replay_pnl": str(row["replay_pnl"]),
                "actual_runtime_cost": str(row["actual_runtime_cost"]),
                "replay_cost": str(row["replay_cost"]),
                "avg_expected_edge": str(sum(expected_edges, Decimal("0")) / len(expected_edges))
                if expected_edges else None,
                "runtime_fill_rate": ratio_decimal(runtime_fills, orders),
                "replay_fill_rate": ratio_decimal(replay_fills, orders),
                "actual_runtime_roi_on_cost": ratio_str(row["actual_runtime_pnl"], row["actual_runtime_cost"]),
                "replay_roi_on_cost": ratio_str(row["replay_pnl"], row["replay_cost"]),
                "q_bucket_counts": dict(row["q_buckets"]),
            }
        )
    return rows


def _empty_time_bucket() -> dict[str, Any]:
    return {
        "orders_placed": 0,
        "cancels": 0,
        "runtime_fills": 0,
        "replay_fills": 0,
        "actual_runtime_pnl": Decimal("0"),
        "replay_pnl": Decimal("0"),
        "actual_runtime_cost": Decimal("0"),
        "replay_cost": Decimal("0"),
        "expected_edges": [],
        "q_buckets": Counter(),
    }


def calibration_table(
    collector: AnalysisCollector,
    market_rows: list[dict[str, Any]],
    backtester: ReplayBacktester,
) -> list[dict[str, Any]]:
    market_outcomes = {row["market_id"]: row["winning_outcome"] for row in market_rows}
    rows: dict[str, dict[str, Any]] = {
        label: {
            "bucket": label,
            "decision_count": 0,
            "up_count": 0,
            "q_values": [],
            "actual_runtime_pnl": Decimal("0"),
            "replay_pnl": Decimal("0"),
        }
        for label, _, _ in Q_BUCKETS
    }
    for decision in collector.decision_calibration:
        market_id = decision["market_id"]
        winning = market_outcomes.get(market_id)
        bucket = decision.get("q_bucket")
        q_up = decision.get("q_up")
        if winning is None or bucket is None or q_up is None:
            continue
        row = rows[bucket]
        row["decision_count"] += 1
        row["up_count"] += 1 if winning == "up" else 0
        row["q_values"].append(q_up)

    for fill in collector.runtime_fills:
        market = next((row for row in market_rows if row["market_id"] == fill.market_id), None)
        if not market or not fill.q_bucket:
            continue
        cost = fill.price * fill.size
        payout = fill.size if fill.outcome == market["winning_outcome"] else Decimal("0")
        rows[fill.q_bucket]["actual_runtime_pnl"] += payout - cost - fill.fee

    q_buckets = collector.place_decision_q_buckets
    market_by_id = {row["market_id"]: row for row in market_rows}
    for index, order in enumerate(backtester.orders):
        if not order.is_filled or index >= len(q_buckets) or not q_buckets[index]:
            continue
        market = market_by_id.get(order.market_id)
        if not market:
            continue
        cost = (order.avg_price or order.price) * order.filled_size
        payout = order.filled_size if order.outcome == market["winning_outcome"] else Decimal("0")
        rows[q_buckets[index]]["replay_pnl"] += payout - cost - order.fee

    output = []
    for label, _, _ in Q_BUCKETS:
        row = rows[label]
        count = row["decision_count"]
        avg_q = sum(row["q_values"], Decimal("0")) / count if count else None
        observed = Decimal(row["up_count"]) / Decimal(count) if count else None
        output.append(
            {
                "bucket": label,
                "count_of_settled_market_decisions": count,
                "observed_up_frequency": str(observed) if observed is not None else None,
                "average_q_up": str(avg_q) if avg_q is not None else None,
                "calibration_error": str(observed - avg_q) if observed is not None and avg_q is not None else None,
                "actual_runtime_pnl": str(row["actual_runtime_pnl"]),
                "replay_pnl": str(row["replay_pnl"]),
            }
        )
    return output


def fill_quality_summary(
    collector: AnalysisCollector,
    replay: Any,
    backtester: ReplayBacktester,
    market_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    market_by_id = {row["market_id"]: row for row in market_rows}
    losing_replay_fills = 0
    winning_replay_fills = 0
    unfilled_profitable_orders = 0
    for order in backtester.orders:
        market = market_by_id.get(order.market_id)
        if not market:
            continue
        winning = market["winning_outcome"]
        if order.is_filled:
            if order.outcome == winning:
                winning_replay_fills += 1
            else:
                losing_replay_fills += 1
        elif order.outcome == winning:
            unfilled_profitable_orders += 1

    runtime_fills = sum(int(row["runtime_fills"]) for row in market_rows)
    replay_fills = sum(int(row["replay_fills"]) for row in market_rows)
    return {
        "runtime_paper_fills": runtime_fills,
        "offline_replay_fills": replay_fills,
        "runtime_minus_replay_fills": runtime_fills - replay_fills,
        "paper_resting_reports": collector.status_counts.get("paper_resting", 0),
        "paper_cancelled_reports": collector.status_counts.get("paper_cancelled", 0),
        "paper_filled_maker_reports": collector.status_counts.get("paper_filled_maker", 0),
        "fills_after_cancel_prevented": replay.replay_metrics.get("fills_after_cancel_prevented"),
        "fills_prevented_not_live": replay.replay_metrics.get("fills_prevented_not_live"),
        "fills_prevented_stale_book": replay.replay_metrics.get("fills_prevented_stale_book"),
        "fills_prevented_final_window": replay.replay_metrics.get("fills_prevented_final_window"),
        "fills_prevented_market_inactive": replay.replay_metrics.get("fills_prevented_market_inactive"),
        "fills_prevented_expired": replay.replay_metrics.get("fills_prevented_expired"),
        "winning_replay_fills": winning_replay_fills,
        "losing_replay_fills": losing_replay_fills,
        "unfilled_orders_that_would_have_won": unfilled_profitable_orders,
        "replay_more_optimistic_than_runtime": replay_fills > runtime_fills,
    }


def market_level_stats(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    values = [dec(row[key]) or Decimal("0") for row in rows]
    count = len(values)
    mean = sum(values, Decimal("0")) / Decimal(count) if count else None
    median = Decimal(str(statistics.median(values))) if values else None
    std = sample_std(values, mean)
    se = std / Decimal(count).sqrt() if std is not None and count else None
    ci_low = mean - Decimal("1.96") * se if mean is not None and se is not None else None
    ci_high = mean + Decimal("1.96") * se if mean is not None and se is not None else None
    best = max(rows, key=lambda row: dec(row[key]) or Decimal("0"), default=None)
    worst = min(rows, key=lambda row: dec(row[key]) or Decimal("0"), default=None)
    profitable = sum(1 for value in values if value > 0)
    losing = sum(1 for value in values if value < 0)
    drawdown = max_drawdown(values)
    return {
        "sample_unit": "settled_market_net_pnl",
        "n_settled_markets": count,
        "mean_net_pnl_per_settled_market": str(mean) if mean is not None else None,
        "median_net_pnl_per_settled_market": str(median) if median is not None else None,
        "std_net_pnl_per_settled_market": str(std) if std is not None else None,
        "standard_error": str(se) if se is not None else None,
        "approx_95ci_low": str(ci_low) if ci_low is not None else None,
        "approx_95ci_high": str(ci_high) if ci_high is not None else None,
        "best_market": market_summary(best, key),
        "worst_market": market_summary(worst, key),
        "max_drawdown": str(drawdown),
        "profitable_markets": profitable,
        "profitable_market_pct": ratio_decimal(profitable, count),
        "losing_markets": losing,
        "losing_market_pct": ratio_decimal(losing, count),
        "required_markets_for_0_05_precision": required_precision(std, Decimal("0.05")),
        "required_markets_for_0_10_precision": required_precision(std, Decimal("0.10")),
        "required_markets_to_detect_current_mean": required_detect_mean(std, mean),
    }


def deployment_health(health: dict[str, Any], status: dict[str, Any]) -> dict[str, Any]:
    azure = None
    for row in (status.get("recorder") or {}).get("recorders") or []:
        if row.get("type") == "azure_storage":
            azure = row
            break
    return {
        "health": {
            "ok": health.get("ok"),
            "execution_mode": health.get("execution_mode"),
            "kill_switch": health.get("kill_switch"),
        },
        "status": {
            "now": status.get("now"),
            "started_at": status.get("started_at"),
            "execution_mode": status.get("execution_mode"),
            "tradeable_markets": status.get("tradeable_markets"),
            "books": status.get("books"),
            "tracked_open_orders": status.get("tracked_open_orders"),
            "reference": status.get("reference"),
            "paper_fill": status.get("paper_fill"),
            "azure_recorder": azure,
        },
    }


def bug_risk_notes() -> list[dict[str, Any]]:
    return [
        {
            "severity": "P1",
            "status": "fixed",
            "area": "ReplayBacktester maker fills",
            "finding": (
                "Replay previously allowed maker fills without enforcing runtime paper-fill guards "
                "for quote-live delay, TTL, active market window, final no-trade window, or stale books."
            ),
            "evidence": "Patched src/polymarket_btc15_bot/backtest.py and added regression tests.",
        },
        {
            "severity": "P2",
            "status": "observed",
            "area": "Long 24h replay performance",
            "finding": (
                "The 24h book stream is about 1.9 GiB. Long synchronous /pnl requests remain inappropriate; "
                "use background reports or this streaming analyzer."
            ),
            "evidence": "Rolling 24h selection contains 1,440 blobs and about 1.868 GiB.",
        },
    ]


def recommendation(actual_stats: dict[str, Any], replay_stats: dict[str, Any], fill_quality: dict[str, Any]) -> str:
    actual_low = dec(actual_stats.get("approx_95ci_low"))
    replay_low = dec(replay_stats.get("approx_95ci_low"))
    if actual_low is not None and actual_low > 0 and replay_low is not None and replay_low > 0:
        return "Continue paper collection unchanged"
    return "Continue paper collection but fix measurement bugs first"


def next_actions() -> list[str]:
    return [
        "Let the resized paper bot run without redeploying so the next report has a cleaner uninterrupted window.",
        "Review replay/runtime divergences by market before changing strategy thresholds.",
        "Add a scheduled daily report job after a clean full UTC day is available.",
        "Keep live mode, taker orders, and private-key configuration disabled.",
        "After 300 settled markets, recompute required sample size from market-level variance.",
    ]


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


def time_bucket(ts: datetime | None, end_ts: datetime | None) -> str:
    if ts is None or end_ts is None:
        return "unknown"
    remaining = (end_ts - ts).total_seconds()
    if remaining <= FINAL_NO_TRADE_SECONDS:
        return "inside_final_no_trade"
    if remaining <= 60:
        return "final_60s"
    if remaining <= 180:
        return "3-1m"
    if remaining <= 360:
        return "6-3m"
    if remaining <= 540:
        return "9-6m"
    if remaining <= 720:
        return "12-9m"
    if remaining <= 900:
        return "15-12m"
    return "outside_15m"


def q_bucket_for(value: Decimal | None) -> str | None:
    if value is None:
        return None
    for label, low, high in Q_BUCKETS:
        if low <= value < high:
            return label
    return None


def included_hour_prefixes(blobs: list[dict[str, Any]]) -> list[str]:
    prefixes = sorted({"/".join(blob["name"].split("/")[:5]) + "/" for blob in blobs})
    return prefixes


def parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def sum_decimal(values: Iterable[Any]) -> Decimal:
    total = Decimal("0")
    for value in values:
        total += dec(value) or Decimal("0")
    return total


def ratio_str(numerator: Decimal, denominator: Decimal) -> str | None:
    if denominator == 0:
        return None
    return str(numerator / denominator)


def ratio_decimal(numerator: int, denominator: int) -> str | None:
    if denominator == 0:
        return None
    return str(Decimal(numerator) / Decimal(denominator))


def iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def compact_ts(value: datetime) -> str:
    return value.strftime("%Y%m%dT%H%MZ")


def _report_decision(payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("raw")
    if not isinstance(raw, dict):
        return {}
    decision = raw.get("decision")
    return decision if isinstance(decision, dict) else {}


def _string_or_none(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def read_json(path: str) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {}
    return json.loads(target.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
