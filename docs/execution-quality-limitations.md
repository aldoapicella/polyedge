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

The complete venue-real design consists of:

1. Use server-side Polymarket L1/L2 authentication. Never expose the signing
   key, API key identifier, API secret, or passphrase to the frontend or event
   payloads. Authenticated lifecycle fields named `owner` and `order_owner`
   contain the API key identifier on Polymarket and must also be redacted.
2. Submit post-only short-lived GTD maker orders and record a monotonic client-send
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
   each horizon for every distinct authenticated trade ID, while retaining
   observation delay and executable bid/ask.
7. Snapshot the public book immediately before send and immediately after the
   venue acknowledges the order. Continue to call any derived queue value
   `inferred_size_ahead` unless the venue supplies explicit priority/rank.
8. Add sequence/gap detection, reconnect reconciliation, synchronized clocks,
   immutable raw capture, and duplicate trade/order-event protection.

The implemented probe is deliberately isolated from the paper runtime and has
hard gates for one open order at a time, post-only maker execution, no takers,
a maximum $1.00 order notional, a $1.00 cross-day campaign drawdown, and a
$4.03 equity floor. A canary may submit one order. Any continuation uses a
human-authorized 1/5/25/100/200 ladder, with at most one order per controller
invocation and a fresh exact-state grant before each stage. It persists an immutable event ledger,
the venue-confirmed lifecycle, public depth before send and after
acknowledgement, fill/cancellation races, and real 1/5/30-second markouts when a
real fill occurs. Credentials are referenced from Azure Key Vault; they are not
placed in the frontend, image, or evidence payload.

Socket close/error events are persisted. Both authenticated and market channels
reconnect and resubscribe with bounded retries, exact duplicate messages are
suppressed, and each final order is reconciled through authenticated REST after
cancellation. Any reconnect marks the affected observation as having an
unprovable stream gap and therefore ineligible for model training. The
documented Polymarket channels do not expose a universal sequence number, so a
reconnect cannot prove that every intermediate event was received. Every order
and every campaign also confirms zero open orders, with a fail-closed account
cancel recovery if the isolated probe order remains open.
The public book is refreshed after WebSocket prewarm and immediately before
submission. A market that no longer supports a safe post-only price is stopped
before submission. Once a durable pre-send reservation exists, any ambiguous
HTTP submission failure stops the campaign, confirms zero open orders, and
leaves the full reserved notional unresolved; it is never silently released or
retried. If a later probe fails, already completed
probes are retained in the failed-run summary so their fills, markouts, and
conservative campaign-risk consumption cannot disappear from training or limits.

Evidence protocol v3 adds a 60-second renewable Azure Blob lease so only one
campaign can own the funded account. Before each send it writes a durable
probe risk reservation, and immediately after acknowledgement it durably adds
the venue order ID. Until terminal REST reconciliation and zero open orders are
both confirmed, the full reserved notional consumes the daily cap. An
unresolved reservation blocks every subsequent live run across UTC-day
boundaries and requires explicit order/trade reconciliation; dry-run
diagnostics remain available. Every live
order uses GTD expiry as a venue-side backstop, and SIGTERM/SIGINT initiates
cancel-all plus strict zero-open verification.

Lease acquisition, renewal, and release have ten-second request deadlines. The
worker tracks the last confirmed renewal with a monotonic clock and refuses to
send once lease freshness exceeds 45 seconds, even if an SDK renewal call hangs
without returning an error. Lease health and termination state are rechecked
after the durable reservation and immediately before the non-awaiting send
critical section.

The protocol version is an exact frozen contract: only
`evidence_protocol_version = 3` is eligible. A later, unknown version is not
silently treated as compatible. Existing funded orders from older protocols
remain in the lifetime account PnL and audit history, but cannot be relabeled as
v3 evidence because the missing lifecycle guarantees cannot be reconstructed
after the fact.

The reservation includes principal plus a conservative fee upper bound derived
from the venue-reported base fee in basis points. Order selection is capped
against remaining cross-day campaign risk, the equity floor, and liquid
collateral after applying that bound; finalized filled risk retains the same
fee bound. Therefore the campaign cap cannot be bypassed merely because fees
were omitted from principal or because the UTC date changed.

For filled probes, protocol v3 requires REST order quantity, REST trade
quantity, authenticated user-channel order quantity, and user-channel trade
quantity to agree within a small numeric tolerance. REST and user-channel trade
ID sets must also agree. Each distinct trade ID must have its own timely
1/5/30-second midpoint and executable markout triplet, with no null values and
no observation more than two seconds late. A single triplet cannot stand in for
multiple partial fills. Any disagreement, gap, missing trade, late markout, or
null price makes the entire submitted probe ineligible and fails the global
quality gate.

After cancellation or terminal fill, the probe polls REST order state, REST
trades, authenticated user events, and the complete account open-order list for
at least ten seconds. It requires an unchanged terminal snapshot for at least
five seconds before reducing the full reservation to matched risk. A delayed
cancel-race fill changes the snapshot and restarts the quiescence timer. If
stable finality is not established within thirty seconds, the full reservation
remains unresolved and later live runs are blocked. Reservation manifests that
lack a complete probe observation are synthesized as submitted, ineligible
audit rows, so a crash cannot disappear from the model quality gate.

The order is never kept live merely to manufacture a 60-second label. Its rest
time is bounded by both the deployed 30-second ceiling and the immutable intent
validity window. Once cancellation or terminal reconciliation has observed at
least ten seconds of stable finality, including at least five unchanged
seconds, zero open orders proves that the order cannot fill later. That proof
creates valid negative 1/5/30/60-second fill labels for horizons after terminal
finality. Filled orders still require their real per-trade 1/5/30-second
markouts; stable cancellation cannot manufacture a filled markout.

Admission does not trust producer booleans alone. The independent JavaScript
controller and Rust reporting validator recompute acknowledgement and
cancellation chronology, per-fill timing, cancellation races, markout timing,
source agreement, and exact horizon labels from the raw lifecycle rows. They
also bind every model feature back to the observed order, market, and pre-send
book context. Any mismatch makes the submitted order ineligible.

Protocol v3 also binds the child summary to the exact parent run ID, intent,
authorization, human-grant consumption, and canonical promotion manifest by
both blob name and SHA-256. Pending terminal work persists that complete
binding and rechecks it against the objects reloaded from Azure before it can
advance a funded checkpoint. A summary produced for a different controller
invocation cannot be substituted merely because its candidate or decision ID
looks similar.

The authenticated and public WebSocket clients send recurring heartbeats at
least every ten seconds and require a fresh PONG for each heartbeat. A stale
PONG, disconnect, reconnect, or unparsed message fails the observation closed.
Venue clock offset is measured with the HTTP round trip and carried with an
explicit uncertainty bound; protocol-v3 evidence rejects uncertainty above
750 ms and rejects fill/cancel or horizon labels whose ordering is ambiguous
inside that bound. Venue timestamps are normalized with the signed offset, not
mixed directly with the local wall clock.

Each markout records request start, response completion, response duration,
the venue book timestamp when supplied, and a canonical book hash. Observation
delay begins when the book response finishes, so HTTP time cannot be hidden as
a zero-delay observation. The validators independently recompute BUY midpoint
and executable markouts from authenticated fill price and observed book price.
They also recompute the round-trip fee cost from fill price, 30-second
executable price, and the venue-reported fee rate; a producer cannot make a
losing fill appear profitable by claiming a zero or smaller cost.

Terminal filled evidence is eligible only after a successful Polygon receipt
on chain 137, a positive block number, at least two confirmations, the exact
funded wallet, exact condition ID, zero open orders, zero unresolved durable
risk reservations, and portfolio arithmetic within one cent. No-fill terminal
evidence uses the independently reconciled authenticated no-fill path and may
not contain a settlement transaction claim.

## Remaining Funded-Evidence Trust Boundary

There is one important implementation limitation beyond Polymarket's missing
FIFO feed. The currently deployed canary and funded-ladder image can both write
funded control/evidence and access the venue signing credentials through the
same managed identity and process boundary. The protocol validators make
accidental or forged-file admission substantially harder, but that topology is
not independent attestation: a compromised credentialed process could attempt
to manufacture both an action and its control record.

For that reason every deployed funded, canary, probe, and redemption job has:

```text
FUNDED_EVIDENCE_TRUST_BOUNDARY_READY=false
```

Any non-dry-run canary, probe, or ladder execution fails closed while this
value is false, and terminal evidence is ineligible unless it records
`trust_boundary_ready=true`. This is a deliberate funded-activation blocker,
not a data-collection blocker; the credential-free paper and shadow recorders
continue unchanged.

Closing this boundary requires the following Azure split before the flag may
be changed:

1. Run the canonical controller under an identity with no Key Vault access. It
   may read shadow/research inputs, consume grants, write canonical control,
   and start one exact signer job.
2. Run the signer under a different identity that can read immutable control
   inputs and the four narrow venue secrets but cannot write canonical control
   or promotion state.
3. Store signer lifecycle output in a separate append-only evidence container;
   store grant consumption, authorizations, progress, and canonical state in a
   controller-only control container.
4. Run terminal portfolio/receipt reconciliation under a third no-signing
   identity, or an equivalently isolated attestor, which reads raw evidence and
   on-chain state and writes only immutable terminal attestations.
5. Give the controller permission to start only the fixed signer job, bind the
   exact job execution ID and input hashes into its authorization, and admit
   only evidence carrying that execution binding.
6. Prove with Azure RBAC inspection and negative integration tests that the
   signer cannot update control, the controller cannot read secrets or sign,
   and neither producer can overwrite the other's immutable artifacts.

Only after those checks pass may deployment set the trust flag to true for a
separately human-authorized, capped stage. Until then, collecting shadow data
and validating dry runs is safe, but no new funded evidence may count.

The worker checks CLOB server time at startup and immediately before every
submission. It also checks `https://polymarket.com/api/geoblock` at startup and
immediately before every submission. Live submission requires `blocked=false`,
country `IE`, and an exact match to the worker's configured static Azure NAT
Gateway egress IP. A changed IP, country, blocked response, active kill switch,
clock drift, existing open order, insufficient balance, or exhausted daily
risk budget prevents the order from being signed and submitted.

The funded canary account is an email-login deposit wallet. Authenticated
diagnostics reconcile its 9.23 pUSD on-chain balance only with `POLY_1271`
signature type `3`; legacy proxy/safe types `1` and `2` correctly report zero
for this signer/funder pair. The probe uses the reconciled type `3` mapping and
fails closed if the CLOB balance is below the capped maker order.

The practical queue limitation is addressed by training:

```text
P(fill within 1/5/30/60 seconds |
  inferred size ahead, order age, trade flow, depth changes,
  spread, volatility, price, size, time to expiry)
```

Training requires at least 100 distinct protocol-v3 eligible order probes with at least 10
distinct filled probes and 10 distinct non-filled probes. The four horizon
labels from one order never count as four independent probes. The first 80% of
whole probes are used for fitting and the last 20% as a temporal holdout, so no
order can appear on both sides of the split. Incomplete probes remain reported
but are excluded from fitting and make the quality gate fail. Protocol-v2 and
older observations remain visible as legacy evidence but do not count toward
the v3 threshold or promotion. The model reports out-of-sample Brier score plus
calibration bins and remains research-only; it cannot promote a strategy.

The live dashboard exposes the authenticated lifecycle and model under **Labs
> Venue Execution**. It always labels the value as `inferred_size_ahead` and
shows that literal FIFO rank is unavailable. It also exposes each selected
artifact's source, authoritative timestamp, freshness, schema validity, and
selection reason. Labs is a display surface only: it never independently makes
a protocol-admission or promotion claim. A valid canonical funded ladder is
authoritative once it exists; before then, a newer valid shadow manifest cannot
be hidden by a stale funded placeholder.

The same panel reconciles liquid collateral with all current position values
against the configured starting capital. A resolved winner's redeemable value
is a gross payout, not profit. `true_net_account_pnl` is calculated as liquid
collateral plus current position value minus starting capital, so resolved
losers are not omitted. Redemption converts a resolved winning conditional
token into collateral; it does not create additional PnL.

Automatic gasless redemption uses a separate manual Azure North Europe job. It
derives and verifies the configured UUPS deposit wallet, confirms zero open
orders, selects only resolved positive-payout conditions within a $25 and
five-condition ceiling, and submits one atomic deposit-wallet batch. The batch
temporarily approves only Polymarket's official current CTF collateral adapter,
redeems via that adapter, and restores the prior ERC-1155 approval state. It
then requires a successful Polygon receipt, zero remaining condition-token
balances, restored adapter approval, increased on-chain pUSD, matching CLOB
collateral, no open orders, and no remaining redeemable winner before reporting
success.

The job is persisted disabled and dry-run. Submission additionally requires a
dedicated Relayer API key from Polymarket Settings > API Keys, stored only as
`polymarket-relayer-api-key` in Key Vault. CLOB credentials are not relayer
credentials. The worker verifies the key/address pair against the venue's key
inventory before signing, persists intent and the relayer transaction ID, and
will not blindly retry an ambiguous submission. Redemption converts existing
value to liquid collateral; it neither creates profit nor resets the UTC probe
risk budget.

Recent public redemption activity is shown separately and is attributed to the
Azure worker only when its transaction hash matches that durable submission
record. Manual or venue-UI redemptions remain explicitly `external_or_manual`.

The existing East US execution job is retired because Polymarket correctly
reports that origin as US/VA and blocks it. The authenticated worker is deployed
in an isolated Azure Container Apps environment in North Europe (Ireland), with
a dedicated VNet, NAT Gateway, and static egress IP. It has no inbound endpoint,
uses managed identity for ACR, Blob Storage, and Key Vault, and remains manual
and dry-run by default. The East US environment remains the data, dashboard,
research, and model control plane and cannot submit venue orders.

This cloud-region design is a technical execution origin, not a determination
of the account holder's legal eligibility. It must continue to obey the live
Polymarket response and applicable account/venue terms. If Polymarket changes
Ireland's status, the worker fails closed automatically.

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

Filled risk is intentionally more conservative than the eventually consistent
positions API. Reconciliation closes the order lifecycle but does not release
capital: the durable reservation becomes `position_unresolved` and blocks the
next submission until either the Data API positively marks the condition
redeemable or the redemption worker verifies the on-chain transaction. A mere
temporary absence from the positions API never releases the reservation.

HTTP acknowledgement time is recorded immediately using a monotonic elapsed
clock. Authenticated partial fills start independent concurrent deadlines for
their own 1/5/30-second markouts; a late REST-only fill is retained as evidence
but fails the timing-quality gate instead of being treated as timely.

## Acceptance Criteria

The limitation is considered materially reduced only when a report can join,
without ambiguity:

```text
decision -> send -> venue orderID/live ack -> user-channel updates
         -> actual partial fills/trade IDs -> cancel send/ack or terminal fill
         -> 1/5/30-second midpoint and executable markouts
```

Literal FIFO rank can only be closed if Polymarket supplies explicit
`queue_rank`/`size_ahead`, per-order priority events, or an institutional
matching-priority feed. Until then, queue metrics remain explicitly inferred
and research-only even when order identity, fills, cancellation latency, and
markouts are venue-real.
