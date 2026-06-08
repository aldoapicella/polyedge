from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Literal

from ..models import MarketSpec

ChartRange = Literal["full", "5m", "1m"]

_RANGE_MS: dict[str, int] = {
    "5m": 5 * 60 * 1000,
    "1m": 60 * 1000,
}

_CHART_FIELDS = (
    "qUp",
    "qDown",
    "upBid",
    "upAsk",
    "downBid",
    "downAsk",
    "distanceBps",
    "referencePrice",
    "fillPrice",
    "fillOutcome",
    "fillSize",
)


@dataclass(frozen=True)
class ChartSample:
    market_id: str
    bucket: int
    q_up: float | None = None
    q_down: float | None = None
    up_bid: float | None = None
    up_ask: float | None = None
    down_bid: float | None = None
    down_ask: float | None = None
    distance_bps: float | None = None
    reference_price: float | None = None
    fill_price: float | None = None
    fill_outcome: str | None = None
    fill_size: float | None = None

    @property
    def bucket_ts(self) -> str:
        return datetime.fromtimestamp(self.bucket / 1000, tz=timezone.utc).isoformat()

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "market_id": self.market_id,
            "bucket": self.bucket,
            "bucket_ts": self.bucket_ts,
        }
        _set_if_not_none(record, "qUp", self.q_up)
        _set_if_not_none(record, "qDown", self.q_down)
        _set_if_not_none(record, "upBid", self.up_bid)
        _set_if_not_none(record, "upAsk", self.up_ask)
        _set_if_not_none(record, "downBid", self.down_bid)
        _set_if_not_none(record, "downAsk", self.down_ask)
        _set_if_not_none(record, "distanceBps", self.distance_bps)
        _set_if_not_none(record, "referencePrice", self.reference_price)
        _set_if_not_none(record, "fillPrice", self.fill_price)
        _set_if_not_none(record, "fillOutcome", self.fill_outcome)
        _set_if_not_none(record, "fillSize", self.fill_size)
        return record


@dataclass(frozen=True)
class MarketChartSummary:
    market_id: str
    sample_count: int
    first_sample_ts: str | None = None
    last_sample_ts: str | None = None
    start_price: str | None = None
    q_up: str | None = None
    q_down: str | None = None
    fair_value_ts: str | None = None

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "market_id": self.market_id,
            "sample_count": self.sample_count,
            "first_sample_ts": self.first_sample_ts,
            "last_sample_ts": self.last_sample_ts,
            "start_price": self.start_price,
            "q_up": self.q_up,
            "q_down": self.q_down,
            "fair_value_ts": self.fair_value_ts,
        }
        return {key: value for key, value in record.items() if value is not None}


@dataclass(frozen=True)
class ChartQueryResult:
    source: str
    records: list[dict[str, Any]]
    warning: str | None = None


def _set_if_not_none(record: dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        record[key] = value


def _bucket_ms(value: datetime) -> int:
    current = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return int(current.timestamp() // 1) * 1000


def _float_or_none(value: Decimal | int | float | str | None) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric == numeric else None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bucket_iso(value: Any) -> str | None:
    bucket = _int_or_none(value)
    if bucket is None:
        return None
    return datetime.fromtimestamp(bucket / 1000, tz=timezone.utc).isoformat()


def _string_number(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except Exception:
        return None


def _derive_start_price(points: Iterable[dict[str, Any]]) -> str | None:
    for point in points:
        reference_price = point.get("referencePrice")
        distance_bps = point.get("distanceBps")
        if reference_price is None or distance_bps is None:
            continue
        try:
            reference = Decimal(str(reference_price))
            distance = Decimal(str(distance_bps))
        except Exception:
            continue
        denominator = Decimal("1") + (distance / Decimal("10000"))
        if denominator <= 0:
            continue
        return str(reference / denominator)
    return None


def _token_outcome(token_id: str | None, market: MarketSpec | None) -> str | None:
    if token_id is None or market is None:
        return None
    if token_id == market.up_token_id:
        return "UP"
    if token_id == market.down_token_id:
        return "DOWN"
    return None


def _market_domain(market: MarketSpec) -> tuple[datetime, datetime]:
    end = market.end_ts if market.end_ts > market.start_ts else market.start_ts
    return market.start_ts, end


def _visible_domain(domain: tuple[datetime, datetime], chart_range: ChartRange, now: datetime) -> tuple[int, int]:
    start = _bucket_ms(domain[0])
    end = _bucket_ms(domain[1])
    if chart_range == "full":
        return start, end
    visible_end = min(max(_bucket_ms(now), start), end)
    return max(start, visible_end - _RANGE_MS[chart_range]), visible_end


def _merge_records(
    records: Iterable[dict[str, Any]],
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    start_bucket = _bucket_ms(start)
    end_bucket = _bucket_ms(end)
    buckets: dict[int, dict[str, Any]] = {}
    for record in records:
        bucket = _int_or_none(record.get("bucket"))
        if bucket is None or bucket < start_bucket or bucket > end_bucket:
            continue
        point = buckets.setdefault(
            bucket,
            {
                "bucket": bucket,
                "time": _format_chart_time(bucket),
            },
        )
        for key in _CHART_FIELDS:
            value = record.get(key)
            if value is not None:
                point[key] = value
    return [buckets[key] for key in sorted(buckets)]


def _format_chart_time(bucket: int) -> str:
    return datetime.fromtimestamp(bucket / 1000).strftime("%H:%M:%S")


def _partition_key(market_id: str) -> str:
    return hashlib.sha256(market_id.encode("utf-8")).hexdigest()


def _row_key(bucket: int) -> str:
    return datetime.fromtimestamp(bucket / 1000, tz=timezone.utc).strftime("%Y%m%dT%H%M%S000Z")


def _sample_to_entity(sample: ChartSample) -> dict[str, Any]:
    record = sample.to_record()
    entity: dict[str, Any] = {
        "PartitionKey": _partition_key(sample.market_id),
        "RowKey": _row_key(sample.bucket),
        "marketId": sample.market_id,
        "bucket": str(sample.bucket),
        "bucketTs": sample.bucket_ts,
    }
    for key in _CHART_FIELDS:
        value = record.get(key)
        if value is not None:
            entity[key] = value
    return entity


def _market_map(markets: Iterable[MarketSpec]) -> dict[str, MarketSpec]:
    current: dict[str, MarketSpec] = {}
    for market in markets:
        existing = current.get(market.market_id)
        if existing is None:
            current[market.market_id] = market
            continue
        if existing.start_price is None and market.start_price is not None:
            current[market.market_id] = market
            continue
        if existing.start_price is not None and market.start_price is None:
            continue
        if market.start_ts > existing.start_ts:
            current[market.market_id] = market
    return current


def _summary_map(summaries: Iterable[MarketChartSummary]) -> dict[str, MarketChartSummary]:
    current: dict[str, MarketChartSummary] = {}
    for summary in summaries:
        existing = current.get(summary.market_id)
        if existing is None or _summary_rank(summary) >= _summary_rank(existing):
            current[summary.market_id] = summary
    return current


def _best_summary(summaries: Iterable[MarketChartSummary]) -> MarketChartSummary | None:
    ranked = list(summaries)
    if not ranked:
        return None
    return max(ranked, key=_summary_rank)


def _summary_rank(summary: MarketChartSummary) -> tuple[int, str]:
    return summary.sample_count, summary.last_sample_ts or ""


def _market_row_key(market_id: str) -> str:
    return hashlib.sha256(market_id.encode("utf-8")).hexdigest()


def _market_to_entity(market: MarketSpec) -> dict[str, Any]:
    return {
        "PartitionKey": "market",
        "RowKey": _market_row_key(market.market_id),
        "marketId": market.market_id,
        "startTs": market.start_ts.isoformat(),
        "endTs": market.end_ts.isoformat(),
        "question": market.question,
        "payloadJson": json.dumps(market.model_dump(mode="json"), separators=(",", ":"), sort_keys=True),
    }


def _summary_to_entity(summary: MarketChartSummary) -> dict[str, Any]:
    entity: dict[str, Any] = {
        "PartitionKey": "market",
        "RowKey": _market_row_key(summary.market_id),
        "marketId": summary.market_id,
        "chartSampleCount": summary.sample_count,
    }
    _set_if_not_none(entity, "chartFirstSampleTs", summary.first_sample_ts)
    _set_if_not_none(entity, "chartLastSampleTs", summary.last_sample_ts)
    _set_if_not_none(entity, "chartStartPrice", summary.start_price)
    _set_if_not_none(entity, "latestQUp", summary.q_up)
    _set_if_not_none(entity, "latestQDown", summary.q_down)
    _set_if_not_none(entity, "latestFairValueTs", summary.fair_value_ts)
    return entity


def _entity_to_market(entity: Any) -> MarketSpec | None:
    payload = entity.get("payloadJson")
    if not payload:
        return None
    try:
        return MarketSpec.model_validate(json.loads(payload))
    except (json.JSONDecodeError, ValueError):
        return None


def _entity_to_summary(entity: Any) -> MarketChartSummary | None:
    market_id = entity.get("marketId")
    sample_count = _int_or_none(entity.get("chartSampleCount"))
    if not market_id or sample_count is None:
        return None
    return MarketChartSummary(
        market_id=str(market_id),
        sample_count=sample_count,
        first_sample_ts=str(entity.get("chartFirstSampleTs")) if entity.get("chartFirstSampleTs") is not None else None,
        last_sample_ts=str(entity.get("chartLastSampleTs")) if entity.get("chartLastSampleTs") is not None else None,
        start_price=str(entity.get("chartStartPrice")) if entity.get("chartStartPrice") is not None else None,
        q_up=str(entity.get("latestQUp")) if entity.get("latestQUp") is not None else None,
        q_down=str(entity.get("latestQDown")) if entity.get("latestQDown") is not None else None,
        fair_value_ts=str(entity.get("latestFairValueTs")) if entity.get("latestFairValueTs") is not None else None,
    )


def _summary_from_record(record: dict[str, Any]) -> MarketChartSummary:
    return MarketChartSummary(
        market_id=str(record["market_id"]),
        sample_count=int(record.get("sample_count") or 0),
        first_sample_ts=record.get("first_sample_ts"),
        last_sample_ts=record.get("last_sample_ts"),
        start_price=record.get("start_price"),
        q_up=record.get("q_up"),
        q_down=record.get("q_down"),
        fair_value_ts=record.get("fair_value_ts"),
    )


def _entity_to_record(entity: Any) -> dict[str, Any]:
    record = {
        "market_id": entity.get("marketId"),
        "bucket": entity.get("bucket"),
        "bucket_ts": entity.get("bucketTs"),
    }
    for key in _CHART_FIELDS:
        value = entity.get(key)
        if value is not None:
            record[key] = value
    return record
