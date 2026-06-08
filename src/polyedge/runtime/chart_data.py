from __future__ import annotations

import hashlib
import json
import queue
import threading
import time
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

from ..config import Settings
from ..models import BookState, ExecutionReport, FairValue, MarketSpec, ReferencePrice
from .chart_records import (
    ChartQueryResult,
    ChartRange,
    ChartSample,
    MarketChartSummary,
    _best_summary,
    _bucket_iso,
    _bucket_ms,
    _derive_start_price,
    _entity_to_market,
    _entity_to_record,
    _entity_to_summary,
    _float_or_none,
    _int_or_none,
    _market_domain,
    _market_map,
    _market_to_entity,
    _merge_records,
    _partition_key,
    _row_key,
    _sample_to_entity,
    _string_number,
    _summary_from_record,
    _summary_map,
    _summary_to_entity,
    _token_outcome,
    _visible_domain,
)


class ChartSink(Protocol):
    def write(self, sample: ChartSample) -> None:
        ...

    def write_market(self, market: MarketSpec) -> None:
        ...

    def write_market_summary(self, summary: MarketChartSummary) -> None:
        ...

    def query(self, market_id: str, start: datetime, end: datetime) -> ChartQueryResult:
        ...

    def get_market(self, market_id: str) -> MarketSpec | None:
        ...

    def get_market_summary(self, market_id: str) -> MarketChartSummary | None:
        ...

    def list_markets(self, limit: int = 100) -> list[MarketSpec]:
        ...

    def close(self) -> None:
        ...

    def flush(self, timeout: float = 30.0, target_pending: int = 0) -> None:
        ...

    def pending_count(self) -> int:
        ...

    def status(self) -> dict[str, Any]:
        ...


class LocalChartSink:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def write(self, sample: ChartSample) -> None:
        path = self._path(sample.market_id)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(sample.to_record(), separators=(",", ":"), sort_keys=True) + "\n")

    def write_market(self, market: MarketSpec) -> None:
        with self._market_path().open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(market.model_dump(mode="json"), separators=(",", ":"), sort_keys=True) + "\n")

    def write_market_summary(self, summary: MarketChartSummary) -> None:
        with self._market_summary_path().open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(summary.to_record(), separators=(",", ":"), sort_keys=True) + "\n")

    def query(self, market_id: str, start: datetime, end: datetime) -> ChartQueryResult:
        path = self._path(market_id)
        if not path.exists():
            return ChartQueryResult(source="local_chart_jsonl", records=[])
        start_bucket = _bucket_ms(start)
        end_bucket = _bucket_ms(end)
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("market_id") != market_id:
                    continue
                bucket = _int_or_none(record.get("bucket"))
                if bucket is None or bucket < start_bucket or bucket > end_bucket:
                    continue
                records.append(record)
        return ChartQueryResult(source="local_chart_jsonl", records=records)

    def get_market(self, market_id: str) -> MarketSpec | None:
        return _market_map(self._read_markets()).get(market_id)

    def get_market_summary(self, market_id: str) -> MarketChartSummary | None:
        return _summary_map(self._read_market_summaries()).get(market_id)

    def list_markets(self, limit: int = 100) -> list[MarketSpec]:
        markets = sorted(
            _market_map(self._read_markets()).values(),
            key=lambda market: market.start_ts,
            reverse=True,
        )
        return markets[: max(1, min(limit, 5000))]

    def close(self) -> None:
        return None

    def flush(self, timeout: float = 30.0, target_pending: int = 0) -> None:
        return None

    def pending_count(self) -> int:
        return 0

    def status(self) -> dict[str, Any]:
        return {
            "type": "local_chart_jsonl",
            "path": str(self.root),
        }

    def _path(self, market_id: str) -> Path:
        digest = hashlib.sha256(market_id.encode("utf-8")).hexdigest()[:32]
        return self.root / f"{digest}.jsonl"

    def _market_path(self) -> Path:
        return self.root / "markets.jsonl"

    def _market_summary_path(self) -> Path:
        return self.root / "market-summaries.jsonl"

    def _read_markets(self) -> list[MarketSpec]:
        path = self._market_path()
        if not path.exists():
            return []
        markets: list[MarketSpec] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    payload = json.loads(line)
                    markets.append(MarketSpec.model_validate(payload))
                except (json.JSONDecodeError, ValueError):
                    continue
        return markets

    def _read_market_summaries(self) -> list[MarketChartSummary]:
        path = self._market_summary_path()
        if not path.exists():
            return []
        summaries: list[MarketChartSummary] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    payload = json.loads(line)
                    summaries.append(_summary_from_record(payload))
                except (json.JSONDecodeError, ValueError):
                    continue
        return summaries


class AzureTableChartSink:
    def __init__(self, settings: Settings):
        if not settings.azure_storage_account_name:
            raise ValueError("azure_storage_account_name is required")

        from azure.data.tables import TableServiceClient, UpdateMode
        from azure.identity import DefaultAzureCredential

        self.settings = settings
        self.error_count = 0
        self.dropped_count = 0
        self.last_error: str | None = None
        self._update_mode = UpdateMode.MERGE
        self._pending_lock = threading.Lock()
        self._pending_count = 0
        table_url = f"https://{settings.azure_storage_account_name}.table.core.windows.net"
        self.table_service = TableServiceClient(
            endpoint=table_url,
            credential=DefaultAzureCredential(),
        )
        self.table = self.table_service.get_table_client(settings.azure_chart_table_name)
        self.market_table = self.table_service.get_table_client(settings.azure_market_table_name)
        with suppress(Exception):
            self.table_service.create_table(settings.azure_chart_table_name)
        with suppress(Exception):
            self.table_service.create_table(settings.azure_market_table_name)
        self._queue: queue.Queue[ChartSample | None] = queue.Queue(
            maxsize=settings.chart_data_queue_max_events
        )
        self._closed = threading.Event()
        self._worker = threading.Thread(
            target=self._run_worker,
            name="azure-chart-data",
            daemon=True,
        )
        self._worker.start()

    def write(self, sample: ChartSample) -> None:
        if self._closed.is_set():
            self.dropped_count += 1
            self.last_error = "azure chart sink is closed"
            return
        self._increment_pending(1)
        try:
            self._queue.put_nowait(sample)
        except queue.Full:
            self._safe_flush([sample])
            self._decrement_pending(1)

    def write_market(self, market: MarketSpec) -> None:
        entity = _market_to_entity(market)
        try:
            self.market_table.upsert_entity(entity, mode=self._update_mode)
        except Exception as exc:
            self.error_count += 1
            self.last_error = str(exc)

    def write_market_summary(self, summary: MarketChartSummary) -> None:
        entity = _summary_to_entity(summary)
        try:
            self.market_table.upsert_entity(entity, mode=self._update_mode)
        except Exception as exc:
            self.error_count += 1
            self.last_error = str(exc)

    def query(self, market_id: str, start: datetime, end: datetime) -> ChartQueryResult:
        partition = _partition_key(market_id)
        start_key = _row_key(_bucket_ms(start))
        end_key = _row_key(_bucket_ms(end))
        try:
            entities = self.table.query_entities(
                query_filter="PartitionKey eq @partition and RowKey ge @start and RowKey le @end",
                parameters={"partition": partition, "start": start_key, "end": end_key},
            )
            return ChartQueryResult(
                source="azure_chart_table",
                records=[_entity_to_record(entity) for entity in entities],
            )
        except Exception as exc:
            self.error_count += 1
            self.last_error = str(exc)
            return ChartQueryResult(
                source="azure_chart_table",
                records=[],
                warning=f"Azure chart query failed: {exc}",
            )

    def get_market(self, market_id: str) -> MarketSpec | None:
        try:
            entity = self.market_table.get_entity("market", _market_row_key(market_id))
        except Exception:
            return None
        return _entity_to_market(entity)

    def get_market_summary(self, market_id: str) -> MarketChartSummary | None:
        try:
            entity = self.market_table.get_entity("market", _market_row_key(market_id))
        except Exception:
            return None
        return _entity_to_summary(entity)

    def list_markets(self, limit: int = 100) -> list[MarketSpec]:
        try:
            entities = self.market_table.query_entities("PartitionKey eq 'market'")
            markets = [_entity_to_market(entity) for entity in entities]
        except Exception as exc:
            self.error_count += 1
            self.last_error = str(exc)
            return []
        compacted = _market_map(market for market in markets if market is not None)
        return sorted(compacted.values(), key=lambda market: market.start_ts, reverse=True)[: max(1, min(limit, 5000))]

    def close(self) -> None:
        if self._closed.is_set():
            return
        self.flush(timeout=max(5.0, self.settings.chart_data_flush_interval_seconds * 2.0), target_pending=0)
        self._closed.set()
        with suppress(queue.Full):
            self._queue.put(None, timeout=1.0)
        self._worker.join(timeout=max(5.0, self.settings.chart_data_flush_interval_seconds * 2.0))

    def flush(self, timeout: float = 30.0, target_pending: int = 0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._pending_lock:
                if self._pending_count <= target_pending:
                    return
            time.sleep(0.05)

    def pending_count(self) -> int:
        with self._pending_lock:
            return self._pending_count

    def status(self) -> dict[str, Any]:
        return {
            "type": "azure_chart_table",
            "table_name": self.settings.azure_chart_table_name,
            "market_table_name": self.settings.azure_market_table_name,
            "queue_size": self._queue.qsize(),
            "queue_max_events": self.settings.chart_data_queue_max_events,
            "flush_interval_seconds": self.settings.chart_data_flush_interval_seconds,
            "dropped_count": self.dropped_count,
            "pending_count": self._pending_count,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "worker_alive": self._worker.is_alive(),
        }

    def _run_worker(self) -> None:
        batch: list[ChartSample] = []
        deadline = time.monotonic() + self.settings.chart_data_flush_interval_seconds
        while True:
            timeout = max(0.0, deadline - time.monotonic()) if batch else self.settings.chart_data_flush_interval_seconds
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None

            should_stop = item is None and self._closed.is_set()
            if item is not None:
                batch.append(item)

            should_flush = bool(batch) and (
                should_stop
                or len(batch) >= self.settings.chart_data_batch_max_events
                or time.monotonic() >= deadline
            )
            if should_flush:
                self._safe_flush(batch)
                self._decrement_pending(len(batch))
                batch = []
                deadline = time.monotonic() + self.settings.chart_data_flush_interval_seconds

            if should_stop:
                while True:
                    with suppress(queue.Empty):
                        pending = self._queue.get_nowait()
                        if pending is not None:
                            batch.append(pending)
                            continue
                    break
                if batch:
                    self._safe_flush(batch)
                    self._decrement_pending(len(batch))
                return

    def _safe_flush(self, samples: list[ChartSample]) -> None:
        attempts = max(1, self.settings.chart_data_flush_retries + 1)
        for attempt in range(attempts):
            try:
                self._flush(samples)
                return
            except Exception as exc:
                self.error_count += 1
                self.last_error = str(exc)
                if attempt < attempts - 1:
                    time.sleep(min(1.0, self.settings.chart_data_flush_interval_seconds))

    def _flush(self, samples: list[ChartSample]) -> None:
        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for sample in samples:
            entity = _sample_to_entity(sample)
            key = (str(entity["PartitionKey"]), str(entity["RowKey"]))
            current = merged.setdefault(key, {})
            current.update({name: value for name, value in entity.items() if value is not None})
        for entity in merged.values():
            self.table.upsert_entity(entity, mode=self._update_mode)

    def _increment_pending(self, count: int) -> None:
        with self._pending_lock:
            self._pending_count += count

    def _decrement_pending(self, count: int) -> None:
        with self._pending_lock:
            self._pending_count = max(0, self._pending_count - count)


class ChartDataStore:
    def __init__(self, sinks: list[ChartSink]):
        self.sinks = sinks

    def record_fair_value(self, fair_value: FairValue) -> None:
        self._write(
            ChartSample(
                market_id=fair_value.market_id,
                bucket=_bucket_ms(fair_value.computed_ts),
                q_up=_float_or_none(fair_value.q_up),
                q_down=_float_or_none(fair_value.q_down),
            )
        )

    def record_market(self, market: MarketSpec) -> None:
        for sink in self.sinks:
            with suppress(Exception):
                sink.write_market(market)

    def record_market_summary(self, summary: MarketChartSummary) -> None:
        for sink in self.sinks:
            with suppress(Exception):
                sink.write_market_summary(summary)

    def record_book(self, market: MarketSpec | None, book: BookState) -> None:
        if market is None:
            return
        outcome = _token_outcome(book.token_id, market)
        if outcome is None:
            return
        best_bid = _float_or_none(book.best_bid.price) if book.best_bid else None
        best_ask = _float_or_none(book.best_ask.price) if book.best_ask else None
        self._write(
            ChartSample(
                market_id=market.market_id,
                bucket=_bucket_ms(book.local_ts),
                up_bid=best_bid if outcome == "UP" else None,
                up_ask=best_ask if outcome == "UP" else None,
                down_bid=best_bid if outcome == "DOWN" else None,
                down_ask=best_ask if outcome == "DOWN" else None,
            )
        )

    def record_reference(self, reference: ReferencePrice, markets: Iterable[MarketSpec]) -> None:
        for market in markets:
            if market.start_price is None or market.start_price <= 0:
                continue
            if reference.source_ts < market.start_ts or reference.source_ts > market.end_ts:
                continue
            reference_price = _float_or_none(reference.price)
            start_price = _float_or_none(market.start_price)
            if reference_price is None or start_price is None or start_price <= 0:
                continue
            self._write(
                ChartSample(
                    market_id=market.market_id,
                    bucket=_bucket_ms(reference.source_ts),
                    reference_price=reference_price,
                    distance_bps=round(((reference_price / start_price) - 1) * 10000, 10),
                )
            )

    def record_execution_report(self, report: ExecutionReport, market: MarketSpec | None) -> None:
        if report.filled_size <= 0 or report.avg_price is None:
            return
        self._write(
            ChartSample(
                market_id=report.market_id,
                bucket=_bucket_ms(report.local_ts),
                fill_price=_float_or_none(report.avg_price),
                fill_outcome=_token_outcome(report.token_id, market),
                fill_size=_float_or_none(report.filled_size),
            )
        )

    def series(
        self,
        market: MarketSpec,
        *,
        chart_range: ChartRange = "full",
        now: datetime | None = None,
    ) -> dict[str, Any]:
        domain = _market_domain(market)
        visible_domain = _visible_domain(domain, chart_range, now or datetime.now(timezone.utc))
        result = self.query(market.market_id, domain[0], domain[1])
        all_points = _merge_records(result.records, start=domain[0], end=domain[1])
        points = [
            point for point in all_points
            if visible_domain[0] <= int(point["bucket"]) <= visible_domain[1]
        ]
        fills = [point for point in points if point.get("fillPrice") is not None]
        return {
            "source": result.source,
            "warning": result.warning,
            "market_id": market.market_id,
            "range": chart_range,
            "domain": [visible_domain[0], visible_domain[1]],
            "marketChart": points,
            "fills": fills,
            "sampleCount": len(all_points),
        }

    def compute_market_summary(self, market: MarketSpec) -> MarketChartSummary | None:
        result = self.query(market.market_id, market.start_ts, market.end_ts)
        points = _merge_records(result.records, start=market.start_ts, end=market.end_ts)
        if not points:
            return None
        first = points[0]
        last = points[-1]
        latest_q = next(
            (
                point for point in reversed(points)
                if point.get("qUp") is not None and point.get("qDown") is not None
            ),
            None,
        )
        start_price = (
            str(market.start_price)
            if market.start_price is not None
            else _derive_start_price(points)
        )
        return MarketChartSummary(
            market_id=market.market_id,
            sample_count=len(points),
            first_sample_ts=_bucket_iso(first.get("bucket")),
            last_sample_ts=_bucket_iso(last.get("bucket")),
            start_price=start_price,
            q_up=_string_number(latest_q.get("qUp")) if latest_q else None,
            q_down=_string_number(latest_q.get("qDown")) if latest_q else None,
            fair_value_ts=_bucket_iso(latest_q.get("bucket")) if latest_q else None,
        )

    def get_market_summary(self, market_id: str) -> MarketChartSummary | None:
        summaries: list[MarketChartSummary] = []
        for sink in self.sinks:
            with suppress(Exception):
                summary = sink.get_market_summary(market_id)
                if summary is not None:
                    summaries.append(summary)
        return _best_summary(summaries)

    def query(self, market_id: str, start: datetime, end: datetime) -> ChartQueryResult:
        results = [sink.query(market_id, start, end) for sink in self.sinks]
        records = [record for result in results for record in result.records]
        source = "+".join(result.source for result in results)
        warning = "; ".join(result.warning for result in results if result.warning) or None
        return ChartQueryResult(source=source, records=records, warning=warning)

    def get_market(self, market_id: str) -> MarketSpec | None:
        for sink in self.sinks:
            with suppress(Exception):
                market = sink.get_market(market_id)
                if market is not None:
                    return market
        return None

    def list_markets(self, limit: int = 100) -> list[MarketSpec]:
        markets: dict[str, MarketSpec] = {}
        for sink in self.sinks:
            with suppress(Exception):
                markets.update(_market_map(sink.list_markets(limit * 2)))
        return sorted(markets.values(), key=lambda market: market.start_ts, reverse=True)[: max(1, min(limit, 5000))]

    def close(self) -> None:
        for sink in self.sinks:
            with suppress(Exception):
                sink.close()

    def flush(self, timeout: float = 30.0, target_pending: int = 0) -> None:
        deadline = time.monotonic() + timeout
        for sink in self.sinks:
            remaining = max(0.0, deadline - time.monotonic())
            with suppress(Exception):
                sink.flush(remaining, target_pending)

    def pending_count(self) -> int:
        total = 0
        for sink in self.sinks:
            with suppress(Exception):
                total += sink.pending_count()
        return total

    def status(self) -> dict[str, Any]:
        return {
            "type": "chart_data_store",
            "sinks": [sink.status() for sink in self.sinks],
        }

    def _write(self, sample: ChartSample) -> None:
        for sink in self.sinks:
            with suppress(Exception):
                sink.write(sample)


def build_chart_data_store(settings: Settings) -> ChartDataStore:
    if not settings.chart_data_enabled:
        return ChartDataStore([])
    sinks: list[ChartSink] = [LocalChartSink(_local_chart_path(settings))]
    if settings.azure_storage_account_name:
        sinks.append(AzureTableChartSink(settings))
    return ChartDataStore(sinks)


def _local_chart_path(settings: Settings) -> Path:
    default_chart_path = Path("data/chart-points")
    if settings.chart_data_path == default_chart_path and settings.recorder_path != Path("data/events.jsonl"):
        return settings.recorder_path.parent / "chart-points"
    return settings.chart_data_path
