from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Literal
from uuid import uuid4

from ..config import Settings
from ..models import BookLevel, BookState, ExecutionReport, FairValue, MarketSpec, MarketStatus, ReferencePrice
from ..runtime.chart_data import ChartDataStore, ChartRange, MarketChartSummary

ChartBackfillSource = Literal["auto", "local", "azure"]


class ChartBackfillJobAlreadyRunning(RuntimeError):
    def __init__(self, status: dict[str, Any]):
        super().__init__("A chart backfill job is already running")
        self.status = status


class ChartBackfillJobManager:
    def __init__(self, chart_service: ChartService):
        self.chart_service = chart_service
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._running_task: asyncio.Task[None] | None = None
        self._running_job_id: str | None = None

    async def start(
        self,
        *,
        source: ChartBackfillSource = "auto",
        prefix: str | None = None,
        report_date: date | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            if self._running_task is not None and not self._running_task.done():
                current = self._jobs.get(self._running_job_id or "")
                raise ChartBackfillJobAlreadyRunning(current or {"status": "running"})
            job = {
                "job_id": f"chart-backfill-{uuid4().hex}",
                "status": "queued",
                "source": source,
                "prefix": prefix,
                "date": report_date.isoformat() if report_date else None,
                "created_ts": _now_iso(),
                "started_ts": None,
                "finished_ts": None,
                "error": None,
                "summary": None,
            }
            self._jobs[job["job_id"]] = job
            self._running_job_id = job["job_id"]
            self._running_task = asyncio.create_task(
                self._run(job["job_id"], source, prefix, report_date),
                name=f"chart-backfill-{job['job_id']}",
            )
            return job

    async def start_hydrate(self, *, limit: int = 1000) -> dict[str, Any]:
        async with self._lock:
            if self._running_task is not None and not self._running_task.done():
                current = self._jobs.get(self._running_job_id or "")
                raise ChartBackfillJobAlreadyRunning(current or {"status": "running"})
            job = {
                "job_id": f"chart-hydrate-{uuid4().hex}",
                "status": "queued",
                "source": "chart_store",
                "prefix": None,
                "date": None,
                "created_ts": _now_iso(),
                "started_ts": None,
                "finished_ts": None,
                "error": None,
                "summary": None,
                "limit": limit,
                "job_type": "hydrate",
            }
            self._jobs[job["job_id"]] = job
            self._running_job_id = job["job_id"]
            self._running_task = asyncio.create_task(
                self._run_hydrate(job["job_id"], limit),
                name=f"chart-hydrate-{job['job_id']}",
            )
            return job

    async def get(self, job_id: str) -> dict[str, Any] | None:
        return self._jobs.get(job_id)

    def status(self) -> dict[str, Any]:
        running = self._jobs.get(self._running_job_id or "")
        return {
            "running_job": running if running and running.get("status") == "running" else None,
            "known_jobs": len(self._jobs),
        }

    async def _run(
        self,
        job_id: str,
        source: ChartBackfillSource,
        prefix: str | None,
        report_date: date | None,
    ) -> None:
        job = self._jobs[job_id]
        job["job_type"] = "backfill"
        job["status"] = "running"
        job["started_ts"] = _now_iso()
        try:
            summary = await asyncio.to_thread(
                self.chart_service.backfill,
                source=source,
                prefix=prefix,
                report_date=report_date,
            )
        except Exception as exc:
            job.update(
                {
                    "status": "failed",
                    "finished_ts": _now_iso(),
                    "error": str(exc),
                }
            )
            return
        job.update(
            {
                "status": "completed",
                "finished_ts": _now_iso(),
                "summary": summary,
                "error": None,
            }
        )

    async def _run_hydrate(self, job_id: str, limit: int) -> None:
        job = self._jobs[job_id]
        job["status"] = "running"
        job["started_ts"] = _now_iso()
        try:
            summary = await asyncio.to_thread(self.chart_service.hydrate_market_summaries, limit=limit)
        except Exception as exc:
            job.update(
                {
                    "status": "failed",
                    "finished_ts": _now_iso(),
                    "error": str(exc),
                }
            )
            return
        job.update(
            {
                "status": "completed",
                "finished_ts": _now_iso(),
                "summary": summary,
                "error": None,
            }
        )


class ChartService:
    def __init__(self, settings: Settings, chart_store: ChartDataStore):
        self.settings = settings
        self.chart_store = chart_store

    def get_market(self, market_id: str) -> MarketSpec | None:
        return self.chart_store.get_market(market_id)

    def list_markets(self, limit: int = 100) -> list[dict[str, Any]]:
        return [
            self.market_payload(market)
            for market in self.chart_store.list_markets(limit)
        ]

    def market_payload(self, market: MarketSpec) -> dict[str, Any]:
        summary = self.chart_store.get_market_summary(market.market_id)
        return _market_payload(market, summary)

    def series(self, market: MarketSpec, chart_range: ChartRange = "full") -> dict[str, Any]:
        return self.chart_store.series(market, chart_range=chart_range)

    def hydrate_market_summaries(self, *, limit: int = 1000) -> dict[str, Any]:
        markets = self.chart_store.list_markets(limit)
        hydrated = 0
        without_samples = 0
        first_sample_ts: str | None = None
        last_sample_ts: str | None = None
        for market in markets:
            summary = self.chart_store.compute_market_summary(market)
            if summary is None:
                fallback = _raw_market_summary(market)
                if fallback is None:
                    without_samples += 1
                    continue
                summary = fallback
            self.chart_store.record_market_summary(summary)
            hydrated += 1
            if summary.first_sample_ts:
                first_sample_ts = (
                    summary.first_sample_ts
                    if first_sample_ts is None
                    else min(first_sample_ts, summary.first_sample_ts)
                )
            if summary.last_sample_ts:
                last_sample_ts = (
                    summary.last_sample_ts
                    if last_sample_ts is None
                    else max(last_sample_ts, summary.last_sample_ts)
                )
        self.chart_store.flush(timeout=120.0)
        return {
            "markets_seen": len(markets),
            "markets_hydrated": hydrated,
            "markets_without_samples": without_samples,
            "first_sample_ts": first_sample_ts,
            "last_sample_ts": last_sample_ts,
        }

    def backfill(
        self,
        *,
        source: ChartBackfillSource = "auto",
        prefix: str | None = None,
        report_date: date | None = None,
    ) -> dict[str, Any]:
        resolved_source = _resolved_source(source, self.settings)
        resolved_prefix = _resolved_prefix(prefix, report_date, resolved_source)
        if resolved_source == "azure":
            events, blob_names = _azure_events(self.settings, resolved_prefix or "events/")
            summary = self._materialize(events)
            summary["source"] = "azure"
            summary["prefix"] = resolved_prefix or "events/"
            summary["blob_count"] = len(blob_names)
            return summary
        summary = self._materialize(_iter_jsonl(self.settings.recorder_path))
        summary["source"] = "local"
        summary["path"] = str(self.settings.recorder_path)
        return summary

    def _materialize(self, events: Iterable[dict[str, Any]]) -> dict[str, Any]:
        state = _MaterializationState(self.chart_store)
        pending_before_backfill = self.chart_store.pending_count()
        for event in events:
            state.handle(event)
        self.chart_store.flush(timeout=120.0, target_pending=pending_before_backfill)
        summary = state.summary()
        hydrate_summary = self._hydrate_touched_market_summaries(state.markets.values())
        if hydrate_summary:
            summary["market_summaries"] = hydrate_summary
        return summary

    def _hydrate_touched_market_summaries(self, markets: Iterable[MarketSpec]) -> dict[str, Any]:
        hydrated = 0
        without_samples = 0
        for market in markets:
            summary = self.chart_store.compute_market_summary(market)
            if summary is None:
                fallback = _raw_market_summary(market)
                if fallback is None:
                    without_samples += 1
                    continue
                summary = fallback
            self.chart_store.record_market_summary(summary)
            hydrated += 1
        self.chart_store.flush(timeout=120.0)
        return {
            "markets_hydrated": hydrated,
            "markets_without_samples": without_samples,
        }


class _MaterializationState:
    def __init__(self, chart_store: ChartDataStore):
        self.chart_store = chart_store
        self.markets: dict[str, MarketSpec] = {}
        self.token_to_market_id: dict[str, str] = {}
        self.events_seen = 0
        self.markets_seen = 0
        self.chart_samples_written = 0
        self.first_event_ts: datetime | None = None
        self.last_event_ts: datetime | None = None

    def handle(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("event_type") or "")
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return
        recorded_ts = _parse_datetime(event.get("recorded_ts")) or datetime.now(timezone.utc)
        self.events_seen += 1
        self.first_event_ts = recorded_ts if self.first_event_ts is None else min(self.first_event_ts, recorded_ts)
        self.last_event_ts = recorded_ts if self.last_event_ts is None else max(self.last_event_ts, recorded_ts)

        if event_type == "market":
            self._handle_market(payload)
        elif event_type == "market_start_price":
            self._handle_market_start_price(payload)
        elif event_type == "fair_value":
            self._handle_fair_value(payload, recorded_ts)
        elif event_type == "book":
            self._handle_book(payload, recorded_ts)
        elif event_type == "reference":
            self._handle_reference(payload, recorded_ts)
        elif event_type == "execution_report":
            self._handle_execution_report(payload, recorded_ts)

    def summary(self) -> dict[str, Any]:
        return {
            "events_seen": self.events_seen,
            "markets_seen": self.markets_seen,
            "markets_persisted": len(self.markets),
            "chart_samples_written": self.chart_samples_written,
            "first_event_ts": self.first_event_ts.isoformat() if self.first_event_ts else None,
            "last_event_ts": self.last_event_ts.isoformat() if self.last_event_ts else None,
        }

    def _handle_market(self, payload: dict[str, Any]) -> None:
        market = _market_from_payload(payload)
        if market is None:
            return
        existing = self.markets.get(market.market_id)
        if existing is not None and existing.start_price is not None and market.start_price is None:
            market = market.model_copy(update={"start_price": existing.start_price, "status": existing.status})
        self.markets[market.market_id] = market
        self.token_to_market_id[market.up_token_id] = market.market_id
        self.token_to_market_id[market.down_token_id] = market.market_id
        self.markets_seen += 1
        self.chart_store.record_market(market)

    def _handle_market_start_price(self, payload: dict[str, Any]) -> None:
        market_id = str(payload.get("market_id") or "")
        price = _decimal(payload.get("start_price"))
        market = self.markets.get(market_id)
        if market is None or price is None:
            return
        updated = market.model_copy(update={"start_price": price, "status": MarketStatus.TRADEABLE})
        self.markets[market_id] = updated
        self.chart_store.record_market(updated)

    def _handle_fair_value(self, payload: dict[str, Any], recorded_ts: datetime) -> None:
        market_id = str(payload.get("market_id") or "")
        if not market_id:
            return
        q_up = _decimal(payload.get("q_up"))
        q_down = _decimal(payload.get("q_down"))
        if q_up is None or q_down is None:
            return
        fair_value = FairValue(
            market_id=market_id,
            q_up=q_up,
            q_down=q_down,
            sigma=float(payload.get("sigma") or 0),
            drift_mu=float(payload.get("drift_mu") or 0),
            model_error=_decimal(payload.get("model_error")) or Decimal("0"),
            computed_ts=_parse_datetime(payload.get("computed_ts")) or recorded_ts,
        )
        self.chart_store.record_fair_value(fair_value)
        self.chart_samples_written += 1

    def _handle_book(self, payload: dict[str, Any], recorded_ts: datetime) -> None:
        token_id = str(payload.get("token_id") or "")
        market = self.markets.get(str(payload.get("market_id") or "")) or self.markets.get(
            self.token_to_market_id.get(token_id, "")
        )
        if market is None:
            return
        book = BookState(
            token_id=token_id,
            bids=_levels(payload.get("bids")),
            asks=_levels(payload.get("asks")),
            last_trade_price=_decimal(payload.get("last_trade_price")),
            exchange_ts=_parse_datetime(payload.get("exchange_ts")),
            local_ts=_parse_datetime(payload.get("local_ts")) or recorded_ts,
            book_hash=str(payload.get("book_hash")) if payload.get("book_hash") is not None else None,
        )
        self.chart_store.record_book(market, book)
        self.chart_samples_written += 1

    def _handle_reference(self, payload: dict[str, Any], recorded_ts: datetime) -> None:
        price = _decimal(payload.get("price"))
        if price is None:
            return
        source_ts = _parse_datetime(payload.get("source_ts")) or recorded_ts
        reference = ReferencePrice(
            source=str(payload.get("source") or ""),
            price=price,
            source_ts=source_ts,
            local_ts=_parse_datetime(payload.get("local_ts")) or recorded_ts,
            latency_ms=float(payload.get("latency_ms") or 0),
            stale=bool(payload.get("stale")),
            exact_resolution_source=bool(payload.get("exact_resolution_source")),
            quality_flags=list(payload.get("quality_flags") or []),
        )
        active_markets = [
            market for market in self.markets.values()
            if market.start_ts <= source_ts <= market.end_ts and market.start_price is not None
        ]
        if not active_markets:
            return
        self.chart_store.record_reference(reference, active_markets)
        self.chart_samples_written += len(active_markets)

    def _handle_execution_report(self, payload: dict[str, Any], recorded_ts: datetime) -> None:
        market_id = str(payload.get("market_id") or "")
        if not market_id:
            return
        report = ExecutionReport(
            order_id=str(payload.get("order_id")) if payload.get("order_id") is not None else None,
            market_id=market_id,
            token_id=str(payload.get("token_id")) if payload.get("token_id") is not None else None,
            status=str(payload.get("status") or ""),
            filled_size=_decimal(payload.get("filled_size")) or Decimal("0"),
            avg_price=_decimal(payload.get("avg_price")),
            fee=_decimal(payload.get("fee")) or Decimal("0"),
            local_ts=_parse_datetime(payload.get("local_ts")) or recorded_ts,
            raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else {},
        )
        self.chart_store.record_execution_report(report, self.markets.get(market_id))
        if report.filled_size > 0 and report.avg_price is not None:
            self.chart_samples_written += 1


def _market_from_payload(payload: dict[str, Any]) -> MarketSpec | None:
    market_id = str(payload.get("market_id") or "")
    start_ts = _parse_datetime(payload.get("start_ts"))
    end_ts = _parse_datetime(payload.get("end_ts"))
    up_token_id = str(payload.get("up_token_id") or "")
    down_token_id = str(payload.get("down_token_id") or "")
    if not market_id or start_ts is None or end_ts is None or not up_token_id or not down_token_id:
        return None
    start_price = _decimal(payload.get("start_price"))
    try:
        return MarketSpec.model_validate(
            {
                **payload,
                "market_id": market_id,
                "condition_id": str(payload.get("condition_id") or payload.get("conditionId") or market_id),
                "question": str(payload.get("question") or market_id),
                "up_token_id": up_token_id,
                "down_token_id": down_token_id,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "start_price": start_price,
                "status": payload.get("status") or (MarketStatus.TRADEABLE if start_price is not None else MarketStatus.OBSERVE_ONLY),
            }
        )
    except ValueError:
        return None


def _market_payload(market: MarketSpec, summary: MarketChartSummary | None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    data = market.model_dump(mode="json")
    if data.get("start_price") is None and summary and summary.start_price is not None:
        data["start_price"] = summary.start_price
    data["is_active"] = market.start_ts <= now < market.end_ts
    data["is_tradeable"] = market.status == MarketStatus.TRADEABLE and data.get("start_price") is not None and market.end_ts > now
    if market.end_ts <= now:
        data["status"] = MarketStatus.CLOSED.value
    data["fair_value"] = _fair_value_payload(market, summary)
    if summary is not None:
        data["chart_summary"] = summary.to_record()
    return data


def _fair_value_payload(market: MarketSpec, summary: MarketChartSummary | None) -> dict[str, Any] | None:
    if summary and summary.q_up is not None and summary.q_down is not None:
        return {
            "market_id": market.market_id,
            "q_up": summary.q_up,
            "q_down": summary.q_down,
            "sigma": 0,
            "drift_mu": 0,
            "model_error": "0",
            "computed_ts": summary.fair_value_ts or summary.last_sample_ts or market.end_ts.isoformat(),
        }
    raw_q = _raw_outcome_prices(market)
    if raw_q is None:
        return None
    q_up, q_down = raw_q
    return {
        "market_id": market.market_id,
        "q_up": q_up,
        "q_down": q_down,
        "sigma": 0,
        "drift_mu": 0,
        "model_error": "0",
        "computed_ts": market.end_ts.isoformat(),
    }


def _raw_market_summary(market: MarketSpec) -> MarketChartSummary | None:
    raw_q = _raw_outcome_prices(market)
    if raw_q is None:
        return None
    q_up, q_down = raw_q
    return MarketChartSummary(
        market_id=market.market_id,
        sample_count=0,
        first_sample_ts=None,
        last_sample_ts=None,
        start_price=str(market.start_price) if market.start_price is not None else None,
        q_up=q_up,
        q_down=q_down,
        fair_value_ts=market.end_ts.isoformat(),
    )


def _raw_outcome_prices(market: MarketSpec) -> tuple[str, str] | None:
    raw_market = market.raw.get("market") if isinstance(market.raw, dict) else None
    candidates = []
    if isinstance(raw_market, dict):
        candidates.append(raw_market.get("outcomePrices"))
    candidates.append(market.raw.get("outcomePrices") if isinstance(market.raw, dict) else None)
    for candidate in candidates:
        parsed = _parse_price_pair(candidate)
        if parsed is not None:
            return parsed
    return None


def _parse_price_pair(value: Any) -> tuple[str, str] | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if not isinstance(value, list) or len(value) < 2:
        return None
    up = _decimal(value[0])
    down = _decimal(value[1])
    if up is None or down is None:
        return None
    return str(up), str(down)


def _levels(value: Any) -> list[BookLevel]:
    if not isinstance(value, list):
        return []
    levels: list[BookLevel] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        price = _decimal(item.get("price"))
        size = _decimal(item.get("size"))
        if price is not None and size is not None:
            levels.append(BookLevel(price=price, size=size))
    return levels


def _parse_datetime(value: Any) -> datetime | None:
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


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _azure_events(settings: Settings, prefix: str) -> tuple[Iterable[dict[str, Any]], list[str]]:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    blob_url = f"https://{settings.azure_storage_account_name}.blob.core.windows.net"
    blob_service = BlobServiceClient(
        account_url=blob_url,
        credential=DefaultAzureCredential(),
    )
    container = blob_service.get_container_client(settings.azure_storage_container_name)
    blob_names = [
        blob.name
        for blob in container.list_blobs(name_starts_with=prefix)
        if blob.name.endswith(".jsonl")
    ]
    blob_names.sort()
    return _iter_azure_jsonl(container, blob_names), blob_names


def _iter_azure_jsonl(container: Any, blob_names: list[str]) -> Iterable[dict[str, Any]]:
    for blob_name in blob_names:
        downloader = container.download_blob(blob_name)
        pending = b""
        for chunk in downloader.chunks():
            pending += chunk
            lines = pending.split(b"\n")
            pending = lines.pop()
            for raw_line in lines:
                if not raw_line.strip():
                    continue
                try:
                    yield json.loads(raw_line.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
        if pending.strip():
            try:
                yield json.loads(pending.decode("utf-8"))
            except json.JSONDecodeError:
                continue


def _resolved_source(source: ChartBackfillSource, settings: Settings) -> Literal["local", "azure"]:
    if source == "auto":
        return "azure" if settings.azure_storage_account_name else "local"
    return source


def _resolved_prefix(prefix: str | None, report_date: date | None, source: Literal["local", "azure"]) -> str | None:
    if source != "azure":
        return None
    if prefix:
        return prefix
    if report_date:
        return f"events/{report_date:%Y/%m/%d}/"
    return "events/"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
