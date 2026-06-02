# Replay Backtesting

The first replay engine reads `data/events.jsonl` and reconstructs:

```text
markets
captured RTDS Chainlink start prices
RTDS Chainlink settlement prices
fair-value decisions
paper order fills
estimated PnL
```

Run:

```bash
source .venv/bin/activate
polymarket-btc15-bot backtest --path data/events.jsonl
```

## Current Fill Model

This is a conservative research replay, not a full exchange simulator.

```text
FAK/FOK decisions:
  fill immediately at decision price
  interpret size as shares
  pay crypto taker fee

Post-only maker decisions:
  rest as open replay orders
  interpret size as shares
  fill only if a later book ask is less than or equal to the bid price
  pay no taker fee
```

The replay does not yet model exact queue priority, trade prints, partial maker
fills, or cancellation latency. Add those after enough CLOB book/trade data is
recorded.

Live CLOB FAK/FOK BUY orders use quote-dollar `amount = price * size`, but the
recorded decision and replay engine keep `size` as share quantity. The optional
`quote_amount` field is recorded to audit that live conversion.

## Settlement Model

For each market:

```text
start_price = captured market_start_price from RTDS Chainlink btc/usd
final_price = first RTDS Chainlink btc/usd tick at or after market end
```

If no tick exists at or after market end, replay uses the closest tick inside
the configured settlement window.

Default:

```text
settlement_window_seconds = 15
```

Outcome:

```text
Up wins   if final_price >= start_price
Down wins otherwise
```

## Output Fields

```text
markets_seen
markets_with_start_price
markets_settled
decisions_seen
orders_seen
filled_orders
gross_pnl
fees
net_pnl
market_results
notes
```

Before interpreting profitability, require many settled markets. A single
market only proves the pipeline works.
