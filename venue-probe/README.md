# PolyEdge authenticated venue probe

This worker is research-only and fail-closed. `ALLOW_LIVE` remains `false`; funded probe orders require the separate `ALLOW_VENUE_PROBE` gate and are always post-only.

## Funded campaign risk

The primary funded-risk boundary is an immutable, cross-day campaign stored under
`reports/research/venue-probe/control/campaign-risk/<campaign-id>/`. UTC daily matched notional remains a turnover diagnostic and never resets campaign drawdown.

Defaults:

- campaign ID: `funded-campaign-2026-07-12`
- baseline equity: `$5.030521`
- equity floor: `$4.03`
- maximum campaign drawdown: `$1.00`
- maximum principal per order: `$1.00`
- maximum account reconciliation discrepancy: `$0.01`
- one open order and at most one unresolved position

The worker reconciles CLOB liquid collateral, the sum of Data API position values, the independent Data API total-position-value endpoint, all-date durable reservations, and open orders at startup, before reservation, immediately before send, and after reconciliation. A pre-existing unresolved position blocks another submission.

External deposits and withdrawals must be declared as append-only cash-flow records with `VENUE_PROBE_CAMPAIGN_CASH_FLOWS`, for example:

```json
[{"id":"deposit-1","amount":5,"transaction_hash":"0x...64 hex characters..."}]
```

Deposits are positive and withdrawals are negative. Once persisted, an ID cannot be changed or removed from campaign accounting. Starting a genuinely new campaign requires a new campaign ID and an explicitly reviewed baseline; changing environment values cannot rewrite an existing baseline.

## Strategy-qualified canary executor

`npm run canary-controller` is the manual Azure entrypoint; `npm run canary` is its exact one-intent executor. The controller accepts a short-lived human grant bound to the immutable passing `shadow_passed` research manifest, frozen candidate/version/config hash, fill-model version, `$1` cap, and the fixed intent prefix. It waits at most five minutes for the first fresh future `ExecutionIntentV1`; it never selects strategy parameters or invents a quote.

In funded mode, the controller atomically burns the human grant with `If-None-Match: *`, recording the exact selected intent name/hash, before it creates a derived `canary_ready` transition manifest and exact intent-bound internal authorization. It then invokes the existing executor exactly once. The derived manifest remains `promotion_allowed=false` and is useless without the separately consumed authorization. A crash after the burn can lose the attempt but can never reuse it. Dry runs are completely read-only: they do not burn a grant, write an authorization, or invoke the child executor.

Deployment defaults are `STRATEGY_CANARY_CONTROLLER_ENABLED=false`, `ALLOW_STRATEGY_CANARY=false`, and `STRATEGY_CANARY_DRY_RUN=true`, with empty grant/manifest references. A future manual start must override the two enable flags and exact artifact references deliberately. `ALLOW_LIVE` remains false and taker orders are always forbidden.

Required contract:

- `EXECUTION_MODE=strategy_canary`
- `STRATEGY_CANARY_CONTROLLER_ENABLED=true`
- `STRATEGY_CANARY_HUMAN_GRANT_BLOB_NAME` and `STRATEGY_CANARY_HUMAN_GRANT_SHA256`
- `STRATEGY_CANARY_INTENT_PREFIX`
- `STRATEGY_CANARY_INTENT_BLOB_NAME` and `STRATEGY_CANARY_INTENT_SHA256`
- `STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME` and `STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256`
- `STRATEGY_CANARY_AUTHORIZATION_BLOB_NAME` and `STRATEGY_CANARY_AUTHORIZATION_SHA256`
- `STRATEGY_CANARY_CANDIDATE_NAME`, `STRATEGY_CANARY_CANDIDATE_VERSION`, and `STRATEGY_CANARY_CANDIDATE_CONFIG_HASH`
- `STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION`
- `STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE=chainlink_reference`
- the existing North Europe country/static-egress, Azure storage, CLOB credential, campaign-equity, and reconciliation settings

The intent extends the shared domain contract with `condition_id`, `resolution_source`, and `exact_resolution_source=true`, because those venue identities and the live exact-source gate cannot be inferred safely at execution time. `book_hash` uses SHA-256 over canonical JSON containing the token ID, tick size, minimum order size, and price/size-sorted bid and ask levels; `canonicalBookHash` in `src/canary-lib.mjs` is the authoritative implementation.

The human grant schema is `polyedge.strategy_canary_human_grant.v1`, with `selection_policy=first_fresh_after_authorized_at`, a maximum five-minute validity window, and `single_use=true`. The generated authorization schema is `polyedge.strategy_canary_authorization.v1`. It binds the decision ID, exact intent/derived-manifest hashes, human-grant hash, and immutable grant-consumption receipt hash. Immediately before the only signing path, the executor atomically creates `reports/research/venue-probe/control/strategy-canary/consumed/<authorization_id>.json` with `If-None-Match: *`. There is no automatic promotion or automatic token creation.

The executor reuses the funded campaign baseline/reservation ledger and CLOB lifecycle controls, rejects shares below the venue `minimum_order_size`, opens the authenticated user and public market channels before signing, uses BUY/post-only/GTD only, cancels and reconciles REST against user-channel fills, confirms zero open orders, and captures executable and midpoint 1/5/30-second markouts for real fills. Azure persists the controller as the manual-only `polyedge-strategy-canary-neu-job`; deployment leaves it disabled, in dry-run mode, and without artifact references, so an accidental start fails closed before any signing path.

## Isolated cloud evidence path

The North Europe deployment separates credential-free intent publication, derived research, authenticated evidence, and trained models into distinct Azure containers. Shadow runtime and research identities have no Key Vault access. The model trainer has read-only access to funded evidence and write access only to the model container. The East US paper/API identity is read-only for authenticated evidence and models and has neither a storage account key nor the Key Vault secret-reader role.

The funded ladder controller consumes one exact grant per stage and submits at most one order per invocation. It resumes from immutable authorizations and progress, refuses replacement spending after a crash or ineligible submission, and blocks while a filled order awaits settlement/redemption and terminal portfolio proof. When a stage is complete it publishes one exact checkpoint binding all cumulative protocol-v3 summaries and terminal artifacts. The manual promotion-transition job re-derives PnL, drawdown, markouts, lifecycle quality, and order counts from those artifacts; caller-supplied rollup claims cannot advance the state.

At checkpoint 100, the manual no-Key-Vault trainer fits `queue-calibration-v1` from the exact first 100 eligible orders and writes a content-addressed model. Orders 101–200 use that frozen artifact. The terminal decision evaluates only orders 101–200 as the temporal holdout, so training evidence cannot also serve as final validation evidence.
