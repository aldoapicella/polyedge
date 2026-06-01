import json
from datetime import datetime, timezone
from decimal import Decimal

from polymarket_btc15_bot.backtest import run_backtest


def test_backtest_replays_taker_fill_and_settlement(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    events = [
        {
            "recorded_ts": "2026-06-01T22:00:00+00:00",
            "event_type": "market",
            "payload": {
                "market_id": "m1",
                "market_slug": "btc-updown-15m-test",
                "up_token_id": "up",
                "down_token_id": "down",
                "start_ts": "2026-06-01T22:00:00Z",
                "end_ts": "2026-06-01T22:15:00Z",
                "start_price": None,
                "question": "Bitcoin Up or Down",
            },
        },
        {
            "recorded_ts": "2026-06-01T22:00:01+00:00",
            "event_type": "market_start_price",
            "payload": {
                "market_id": "m1",
                "start_price": "100000",
            },
        },
        {
            "recorded_ts": "2026-06-01T22:01:00+00:00",
            "event_type": "decision",
            "payload": {
                "action": "place",
                "market_id": "m1",
                "token_id": "up",
                "outcome": "up",
                "side": "buy",
                "price": "0.50",
                "size": "5",
                "order_kind": "fak",
                "expected_edge": "0.04",
            },
        },
        {
            "recorded_ts": "2026-06-01T22:15:01+00:00",
            "event_type": "reference",
            "payload": {
                "source": "polymarket_rtds_chainlink_btc_usd",
                "price": "100100",
                "source_ts": "2026-06-01T22:15:01Z",
                "local_ts": "2026-06-01T22:15:01Z",
                "stale": False,
            },
        },
    ]
    path.write_text("\n".join(json.dumps(event) for event in events), encoding="utf-8")

    result = run_backtest(path)

    assert result.markets_seen == 1
    assert result.markets_with_start_price == 1
    assert result.markets_settled == 1
    assert result.orders_seen == 1
    assert result.filled_orders == 1
    assert result.gross_pnl == Decimal("2.50")
    assert result.fees == Decimal("0.087500")
    assert result.net_pnl == Decimal("2.412500")
    assert result.market_results[0]["winning_outcome"] == "up"

