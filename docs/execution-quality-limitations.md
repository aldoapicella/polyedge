# Execution Quality: Measurement Limits and Venue-Real Path

PolyEdge currently runs paper-only and observes the public Polymarket level-2
market channel. The execution-quality events produced by this mode are useful
for rejecting unrealistic fills, but they are not exchange-confirmed order
lifecycle measurements.

## Important Limitation

The public market channel publishes aggregate price levels and trade prints. It
does not identify individual orders at a level or expose an order's matching
priority. Therefore PolyEdge cannot observe a true FIFO queue rank from public
data.

The runtime records the best defensible paper estimate as:

```text
queue_position_source = public_l2_shadow
visible_size_ahead_estimate = better-price public depth
                            + same-price public depth
                            + earlier PolyEdge shadow size
```

This estimate cannot determine:

- whether same-price orders were added ahead of or behind the shadow order;
- hidden, delayed, or non-displayed liquidity;
- the venue's internal priority changes;
- which cancellations removed liquidity ahead of the order;
- whether a packet gap hid a book transition;
- the exact instant the venue accepted the order.

Likewise, `paper_cancel_latency` measures the local paper cancellation pipeline.
It is not venue cancellation acknowledgement latency. Shadow partial fills and
1/5/30-second markouts are research observations, not account fills.

These labels are a correctness boundary. Reports and models must not rename the
estimate to `real_queue_position`, use local cancellation latency as a network
or venue SLA, or promote a strategy to live trading from these fields alone.

## What Is Already Captured

The paper-only runtime emits immutable research events for:

```text
paper_order_queue_registration
paper_order_queue_snapshot
paper_queue_shadow_fill
paper_queue_shadow_observation
paper_cancel_latency
paper_fill_markout
paper_fill_markout_missing
```

They include public same-price and better-price depth, visible size ahead,
trade price/size/aggressor side, strict trade-through, shadow partial size,
remaining size, and midpoint plus executable 1/5/30-second markouts.

An authenticated operator can run `POST /api/v1/execution-quality/probe`. The
probe uses an isolated in-memory tracker with synthetic books, trades, fills,
and cancellation timestamps. It contacts no venue, places no order, records
`probe=true`, and exercises registration, queue snapshot, partial/full shadow
fills, strict trade-through, local cancel latency, and all three markout
horizons. Daily evidence reports exclude probe events so a passing probe cannot
be mistaken for real coverage.

The daily `execution_quality.json` artifact reports queue-snapshot coverage,
size-ahead distributions, partial/full shadow fills, trade-through counts,
cancel-latency percentiles, markout completion and observation delay, and both
midpoint and executable markout distributions. Its gate is:

```text
COLLECTING  no real paper lifecycles yet
PASS        queue snapshots and every observed markout horizon are >= 95%
FAIL        an observed coverage requirement is below 95%
```

## How to Obtain Venue-Real Measurements

Authenticated order lifecycle data can solve order identity, actual partial
fills, and venue acknowledgement timing. It does not automatically solve true
FIFO rank unless the venue explicitly exposes rank or size ahead.

Implement a separate, disabled-by-default `venue_probe` execution mode:

1. Use server-side Polymarket L1/L2 authentication. Never expose the signing
   key, API secret, or passphrase to the frontend or event payloads.
2. Submit post-only GTC/GTD maker orders and record a monotonic client-send
   timestamp before the request.
3. Record the returned `orderID`, status, making/taking amounts, and client
   receive timestamp. A `live` response establishes order identity and the
   measured client-to-ack interval.
4. Subscribe to the authenticated user WebSocket channel and persist every
   order placement, update, cancellation, and trade lifecycle message. Reconcile
   these messages with `GET /order/{orderID}` after reconnects or gaps.
5. For cancellation, record cancel-send, HTTP response, user-channel terminal
   update, and any fill racing the cancellation. Report both client round-trip
   and user-channel acknowledgement latency.
6. Use authenticated `size_matched` and trade IDs for actual partial fills.
   Compute 1/5/30-second markouts from the first market snapshot at or after
   each horizon, while retaining observation delay and executable bid/ask.
7. Snapshot the public book immediately before send and immediately after the
   venue acknowledges the order. Continue to call any derived queue value
   `inferred_size_ahead` unless the venue supplies explicit priority/rank.
8. Add sequence/gap detection, reconnect reconciliation, synchronized clocks,
   immutable raw capture, and duplicate trade/order-event protection.

Relevant venue interfaces:

- [Create orders](https://docs.polymarket.com/trading/orders/create)
- [Authenticated user channel](https://docs.polymarket.com/market-data/websocket/user-channel)
- [Public market channel](https://docs.polymarket.com/market-data/websocket/market-channel)
- [Get an order by ID](https://docs.polymarket.com/api-reference/trade/get-single-order-by-id)

## Safe Canary Gate

Venue probes create real orders and therefore require an explicit, separately
approved deployment. They must not reuse `EXECUTION_MODE=paper` as permission.
Before enabling a probe, require all of the following:

```text
EXECUTION_MODE=venue_probe
ALLOW_LIVE=false
ALLOW_VENUE_PROBE=true
ENABLE_TAKER_ORDERS=false
post_only=true
maximum_open_orders=1
minimum venue-supported order size
short GTD/TTL and immediate cancel fallback
dedicated minimally funded wallet and API credentials
hard daily loss/notional cap
operator-approved time window
working kill switch and heartbeat
```

The probe must fail closed if post-only is rejected, the user channel is stale,
book continuity is uncertain, reconciliation disagrees, or cancellation cannot
be confirmed. Production strategy promotion remains a separate decision after
enough representative probe observations show stable fill calibration and
post-cost markouts.

## Acceptance Criteria

The limitation is considered materially reduced only when a report can join,
without ambiguity:

```text
decision -> send -> venue orderID/live ack -> user-channel updates
         -> actual partial fills/trade IDs -> cancel send/ack or terminal fill
         -> 1/5/30-second midpoint and executable markouts
```

Until then, queue and cancellation metrics remain explicitly research-only.
