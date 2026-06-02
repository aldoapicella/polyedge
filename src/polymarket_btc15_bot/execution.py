from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any, Protocol

from .config import Settings
from .math_utils import crypto_taker_fee_per_share
from .models import DecisionAction, ExecutionReport, OrderKind, Side, TradeDecision


class ExecutionClient(Protocol):
    async def submit(self, decision: TradeDecision) -> ExecutionReport:
        ...

    async def cancel_all(self, market_id: str | None = None) -> list[ExecutionReport]:
        ...


class LiveTradingBlocked(RuntimeError):
    pass


class PaperExecutionClient:
    def __init__(self) -> None:
        self.open_orders: dict[str, TradeDecision] = {}

    async def submit(self, decision: TradeDecision) -> ExecutionReport:
        if decision.action == DecisionAction.CANCEL_ALL:
            reports = await self.cancel_all(decision.market_id)
            return reports[-1] if reports else ExecutionReport(
                order_id=None,
                market_id=decision.market_id,
                status="paper_cancel_all_noop",
            )
        if decision.action != DecisionAction.PLACE:
            return ExecutionReport(
                order_id=None,
                market_id=decision.market_id,
                token_id=decision.token_id,
                status=f"paper_{decision.action.value}",
            )
        order_id = f"paper-{uuid.uuid4()}"
        filled = decision.size if decision.order_kind in {OrderKind.FAK, OrderKind.FOK} else None
        if filled is None:
            self.open_orders[order_id] = decision
        fee = Decimal("0")
        if filled and decision.price is not None:
            fee = crypto_taker_fee_per_share(decision.price) * filled
        return ExecutionReport(
            order_id=order_id,
            market_id=decision.market_id,
            token_id=decision.token_id,
            status="paper_filled" if filled else "paper_resting",
            filled_size=filled or 0,
            avg_price=decision.price if filled else None,
            fee=fee,
            raw={"decision": decision.model_dump(mode="json")},
        )

    async def cancel_all(self, market_id: str | None = None) -> list[ExecutionReport]:
        cancelled: list[ExecutionReport] = []
        for order_id, decision in list(self.open_orders.items()):
            if market_id is not None and decision.market_id != market_id:
                continue
            self.open_orders.pop(order_id, None)
            cancelled.append(
                ExecutionReport(
                    order_id=order_id,
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    status="paper_cancelled",
                )
            )
        return cancelled


class LiveClobExecutionClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._assert_live_gates()
        self.client = self._build_client()

    def _assert_live_gates(self) -> None:
        if not self.settings.live_requested:
            raise LiveTradingBlocked("execution_mode must be live")
        if not self.settings.allow_live:
            raise LiveTradingBlocked("ALLOW_LIVE must be true")
        if not self.settings.confirm_non_restricted_location:
            raise LiveTradingBlocked("CONFIRM_NON_RESTRICTED_LOCATION must be true")
        if not self.settings.polymarket_private_key:
            raise LiveTradingBlocked("POLYMARKET_PRIVATE_KEY is required")

    def _build_client(self) -> Any:
        try:
            from py_clob_client.client import ClobClient
        except ImportError as exc:
            raise LiveTradingBlocked(
                "py-clob-client-v2 is not installed; install with pip install -e '.[live]'"
            ) from exc

        client = ClobClient(
            self.settings.polymarket_clob_url,
            key=self.settings.polymarket_private_key,
            chain_id=self.settings.polymarket_chain_id,
            signature_type=self.settings.polymarket_signature_type,
            funder=self.settings.polymarket_funder,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        return client

    async def submit(self, decision: TradeDecision) -> ExecutionReport:
        if decision.action == DecisionAction.CANCEL_ALL:
            reports = await self.cancel_all(decision.market_id)
            return reports[-1] if reports else ExecutionReport(
                order_id=None,
                market_id=decision.market_id,
                status="live_cancel_all_noop",
            )
        if decision.action != DecisionAction.PLACE:
            return ExecutionReport(
                order_id=None,
                market_id=decision.market_id,
                token_id=decision.token_id,
                status=f"live_{decision.action.value}",
            )
        if decision.token_id is None or decision.price is None or decision.size is None:
            return ExecutionReport(
                order_id=None,
                market_id=decision.market_id,
                token_id=decision.token_id,
                status="live_rejected_invalid_decision",
            )

        try:
            response = self._submit_sync(decision)
        except Exception as exc:
            return ExecutionReport(
                order_id=None,
                market_id=decision.market_id,
                token_id=decision.token_id,
                status="live_error",
                raw={"error": str(exc)},
            )
        return ExecutionReport(
            order_id=str(response.get("orderID") or response.get("id") or ""),
            market_id=decision.market_id,
            token_id=decision.token_id,
            status=str(response.get("status") or "live_submitted"),
            raw=response if isinstance(response, dict) else {"response": str(response)},
        )

    async def cancel_all(self, market_id: str | None = None) -> list[ExecutionReport]:
        try:
            response = self.client.cancel_all()
        except Exception as exc:
            return [
                ExecutionReport(
                    order_id=None,
                    market_id=market_id or "",
                    status="live_cancel_all_error",
                    raw={"error": str(exc)},
                )
            ]
        return [
            ExecutionReport(
                order_id=None,
                market_id=market_id or "",
                status="live_cancel_all_submitted",
                raw=response if isinstance(response, dict) else {"response": str(response)},
            )
        ]

    def _submit_sync(self, decision: TradeDecision) -> dict[str, Any]:
        from py_clob_client.clob_types import OrderArgs, OrderType

        side_value = _sdk_side(decision.side)
        order_type = _sdk_order_type(decision.order_kind, OrderType)
        options = {
            "tickSize": str(decision.tick_size or "0.01"),
            "negRisk": decision.neg_risk,
        }

        if decision.order_kind in {OrderKind.FAK, OrderKind.FOK}:
            order = self.client.create_market_order(
                {
                    "tokenID": decision.token_id,
                    "side": side_value,
                    "amount": float(_market_order_amount(decision)),
                    "price": float(decision.price),
                },
                options,
            )
            return self.client.post_order(order, order_type)

        order_args = OrderArgs(
            token_id=decision.token_id,
            price=float(decision.price),
            size=float(decision.size),
            side=side_value,
        )
        signed = self.client.create_order(order_args, options)
        return self.client.post_order(signed, order_type, decision.post_only)


def build_execution_client(settings: Settings) -> ExecutionClient:
    if settings.live_requested:
        return LiveClobExecutionClient(settings)
    return PaperExecutionClient()


def _sdk_side(side: Side | None) -> Any:
    try:
        from py_clob_client.clob_types import Side as SdkSide

        return SdkSide.BUY if side == Side.BUY else SdkSide.SELL
    except ImportError:
        return "BUY" if side == Side.BUY else "SELL"


def _sdk_order_type(order_kind: OrderKind | None, order_type_cls: Any) -> Any:
    if order_kind == OrderKind.FAK:
        return order_type_cls.FAK
    if order_kind == OrderKind.FOK:
        return order_type_cls.FOK
    if order_kind == OrderKind.POST_ONLY_GTD:
        return order_type_cls.GTD
    return order_type_cls.GTC


def _market_order_amount(decision: TradeDecision) -> Decimal:
    if decision.size is None:
        return Decimal("0")
    if decision.side == Side.BUY:
        if decision.quote_amount is not None:
            return decision.quote_amount
        if decision.price is not None:
            return decision.price * decision.size
    return decision.size
