from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone

from .config import Settings
from .execution import ExecutionClient, build_execution_client
from .fair_value import LogReturnFairValueModel
from .market_discovery import MarketDiscovery
from .models import (
    BookState,
    DecisionAction,
    ExecutionReport,
    FairValue,
    MarketSpec,
    ReferencePrice,
    TradeDecision,
    utc_now,
)
from .polymarket_feed import PolymarketMarketFeed
from .polymarket_rtds import PolymarketRtdsFeed, binance_subscription, chainlink_subscription
from .recorder import JsonlRecorder
from .resolution_feed import (
    BinanceBookTickerFeed,
    ChainlinkHttpReference,
    CoinbaseTickerFeed,
    ReferenceAggregator,
)
from .risk import RiskManager
from .strategy import MakerFirstStrategy


class PolymarketBtc15Bot:
    def __init__(
        self,
        settings: Settings,
        execution_client: ExecutionClient | None = None,
        recorder: JsonlRecorder | None = None,
    ):
        self.settings = settings
        self.discovery = MarketDiscovery(settings)
        self.market_feed = PolymarketMarketFeed(settings)
        self.chainlink = ChainlinkHttpReference(settings)
        self.reference_aggregator = ReferenceAggregator(
            settings.max_reference_age_ms,
            settings.reference_divergence_pause_threshold,
        )
        self.fair_model = LogReturnFairValueModel(settings)
        self.strategy = MakerFirstStrategy(settings)
        self.risk = RiskManager(settings)
        self.execution = execution_client or build_execution_client(settings)
        self.recorder = recorder or JsonlRecorder(settings.recorder_path)

        self.markets: dict[str, MarketSpec] = {}
        self.books: dict[str, BookState] = {}
        self.reference: ReferencePrice | None = None
        self.fair_values: dict[str, FairValue] = {}
        self.decisions: list[TradeDecision] = []
        self.execution_reports: list[ExecutionReport] = []
        self.started_at: datetime = utc_now()
        self._tasks: list[asyncio.Task[None]] = []
        self._stop_event = asyncio.Event()

    async def discover_once(self) -> list[MarketSpec]:
        markets = await self.discovery.discover()
        merged: dict[str, MarketSpec] = {}
        for market in markets:
            existing = self.markets.get(market.market_id)
            if existing is not None and existing.start_price is not None and market.start_price is None:
                market = market.with_start_price(existing.start_price)
            merged[market.market_id] = market
        self.markets = merged
        for market in markets:
            self.recorder.record("market", market)
        return markets

    async def evaluate_once(self, execute: bool = True) -> list[TradeDecision]:
        if self.reference is None:
            return []

        emitted: list[TradeDecision] = []
        for market in self._active_markets():
            fair_value = self.fair_model.compute(market, self.reference)
            if fair_value is None:
                continue
            self.fair_values[market.market_id] = fair_value
            self.recorder.record("fair_value", fair_value)

            raw_decisions = self.strategy.evaluate(market, fair_value, self.books)
            assessment = self.risk.assess_market(market, self.reference, self.books)
            decisions = self.risk.filter_decisions(raw_decisions, market, assessment)
            for decision in decisions:
                self.decisions.append(decision)
                emitted.append(decision)
                self.recorder.record("decision", decision)
                if execute and decision.action in {DecisionAction.PLACE, DecisionAction.CANCEL_ALL}:
                    report = await self.execution.submit(decision)
                    self.execution_reports.append(report)
                    self.risk.on_execution_report(report)
                    self.recorder.record("execution_report", report)
        return emitted

    async def run_forever(self) -> None:
        self._stop_event.clear()
        self._tasks = [
            asyncio.create_task(self._discovery_loop(), name="discovery"),
            asyncio.create_task(self._chainlink_loop(), name="chainlink"),
            asyncio.create_task(self._market_feed_loop(), name="polymarket-feed"),
            asyncio.create_task(self._strategy_loop(), name="strategy"),
        ]
        if self.settings.enable_polymarket_rtds_chainlink:
            self._tasks.append(
                asyncio.create_task(
                    self._feed_loop(
                        "rtds-chainlink",
                        PolymarketRtdsFeed(self.settings, [chainlink_subscription()]),
                    ),
                    name="reference-rtds-chainlink",
                )
            )
        if self.settings.enable_polymarket_rtds_binance:
            self._tasks.append(
                asyncio.create_task(
                    self._feed_loop(
                        "rtds-binance",
                        PolymarketRtdsFeed(self.settings, [binance_subscription()]),
                    ),
                    name="reference-rtds-binance",
                )
            )
        self._tasks.extend(
            [
                asyncio.create_task(self._feed_loop("binance", BinanceBookTickerFeed()), name="reference-binance"),
                asyncio.create_task(self._feed_loop("coinbase", CoinbaseTickerFeed()), name="reference-coinbase"),
            ]
        )
        try:
            await self._stop_event.wait()
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._tasks = []

    def status(self) -> dict[str, object]:
        now = utc_now()
        return {
            "app": self.settings.app_name,
            "execution_mode": self.settings.execution_mode,
            "started_at": self.started_at.isoformat(),
            "now": now.isoformat(),
            "markets": len(self.markets),
            "tradeable_markets": len(self._active_markets()),
            "books": len(self.books),
            "reference": self.reference.model_dump(mode="json") if self.reference else None,
            "latest_decisions": [item.model_dump(mode="json") for item in self.decisions[-20:]],
            "latest_execution_reports": [
                item.model_dump(mode="json") for item in self.execution_reports[-20:]
            ],
        }

    def _active_markets(self) -> list[MarketSpec]:
        now = datetime.now(timezone.utc)
        return [
            market for market in self.markets.values()
            if market.start_ts <= now < market.end_ts
        ]

    async def _discovery_loop(self) -> None:
        while not self._stop_event.is_set():
            with suppress(Exception):
                await self.discover_once()
            await asyncio.sleep(self.settings.discovery_interval_seconds)

    async def _feed_loop(self, name: str, feed: object) -> None:
        stream = getattr(feed, "stream")
        while not self._stop_event.is_set():
            try:
                async for reference in stream():
                    composite = self.reference_aggregator.update(reference)
                    self.reference = composite
                    self._capture_market_start_prices(reference)
                    self.fair_model.update_volatility(composite)
                    self.recorder.record("reference", composite)
                    if self._stop_event.is_set():
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.recorder.record(
                    "feed_error",
                    {"feed": name, "error": str(exc)},
                )
                await asyncio.sleep(2.0)

    async def _chainlink_loop(self) -> None:
        while not self._stop_event.is_set():
            with suppress(Exception):
                reference = await self.chainlink.fetch_once()
                if reference is not None:
                    self.reference = self.reference_aggregator.update(reference)
                    self._capture_market_start_prices(self.reference)
                    self.fair_model.update_volatility(self.reference)
                    self.recorder.record("reference", self.reference)
            await asyncio.sleep(1.0)

    async def _market_feed_loop(self) -> None:
        while not self._stop_event.is_set():
            token_ids = sorted(
                {
                    token
                    for market in self.markets.values()
                    for token in (market.up_token_id, market.down_token_id)
                }
            )
            if not token_ids:
                await asyncio.sleep(2.0)
                continue
            async for book in self.market_feed.stream(token_ids):
                self.books[book.token_id] = book
                self.recorder.record("book", book)
                if self._stop_event.is_set():
                    break

    async def _strategy_loop(self) -> None:
        while not self._stop_event.is_set():
            with suppress(Exception):
                await self.evaluate_once(execute=True)
            await asyncio.sleep(1.0)

    def _capture_market_start_prices(self, reference: ReferencePrice) -> None:
        if reference.stale or not reference.exact_resolution_source:
            return
        now = reference.source_ts
        grace = self.settings.start_price_capture_grace_seconds
        for market_id, market in list(self.markets.items()):
            if market.start_price is not None:
                continue
            seconds_after_start = (now - market.start_ts).total_seconds()
            if 0 <= seconds_after_start <= grace:
                updated = market.with_start_price(reference.price)
                self.markets[market_id] = updated
                self.recorder.record(
                    "market_start_price",
                    {
                        "market_id": market_id,
                        "market_slug": market.market_slug,
                        "start_price": str(reference.price),
                        "reference_source": reference.source,
                        "reference_source_ts": reference.source_ts.isoformat(),
                    },
                )
