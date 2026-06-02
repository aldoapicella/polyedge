from decimal import Decimal

import pytest

from polymarket_btc15_bot.execution import PaperExecutionClient, _market_order_amount
from polymarket_btc15_bot.models import DecisionAction, OrderKind, Side, TradeDecision


def test_market_buy_amount_uses_quote_amount_when_present() -> None:
    decision = TradeDecision(
        action=DecisionAction.PLACE,
        market_id="m1",
        token_id="up",
        side=Side.BUY,
        price=Decimal("0.20"),
        size=Decimal("5"),
        quote_amount=Decimal("1.00"),
        order_kind=OrderKind.FAK,
        reason="test",
    )

    assert _market_order_amount(decision) == Decimal("1.00")


def test_market_buy_amount_falls_back_to_price_times_share_size() -> None:
    decision = TradeDecision(
        action=DecisionAction.PLACE,
        market_id="m1",
        token_id="up",
        side=Side.BUY,
        price=Decimal("0.20"),
        size=Decimal("5"),
        order_kind=OrderKind.FAK,
        reason="test",
    )

    assert _market_order_amount(decision) == Decimal("1.00")


def test_market_sell_amount_remains_share_size() -> None:
    decision = TradeDecision(
        action=DecisionAction.PLACE,
        market_id="m1",
        token_id="up",
        side=Side.SELL,
        price=Decimal("0.20"),
        size=Decimal("5"),
        quote_amount=Decimal("1.00"),
        order_kind=OrderKind.FAK,
        reason="test",
    )

    assert _market_order_amount(decision) == Decimal("5")


@pytest.mark.asyncio
async def test_paper_taker_fill_is_not_kept_as_open_order() -> None:
    client = PaperExecutionClient()
    decision = TradeDecision(
        action=DecisionAction.PLACE,
        market_id="m1",
        token_id="up",
        side=Side.BUY,
        price=Decimal("0.20"),
        size=Decimal("5"),
        quote_amount=Decimal("1.00"),
        order_kind=OrderKind.FAK,
        reason="test",
    )

    report = await client.submit(decision)

    assert report.status == "paper_filled"
    assert report.filled_size == Decimal("5")
    assert client.open_orders == {}
