# Profitability Promotion and Funded-Risk Contract

PolyEdge must finish research in one of two terminal states:

- `profitable_go`: future shadow and bounded venue evidence support positive net value after realistic execution and costs.
- `stopped_no_go`: the edge is negative, inconclusive after the maximum evidence window, or unsafe to measure within the capital limit.

Profit is not guaranteed. A safe `stopped_no_go` is successful completion of the research process because it prevents further loss.

## Separate PnL Ledgers

The dashboard and reports must never combine:

1. historical simulated PnL;
2. execution-probe cost;
3. wallet-constrained shadow PnL;
4. funded strategy PnL.

The original account baseline is `$9.23`. The repaired campaign baseline is `$5.030521`. Campaign PnL is:

```text
current equity + campaign withdrawals - campaign deposits - 5.030521
```

A profitable repaired campaign does not erase the existing lifetime account loss. Gross redemption payout is not profit.

## Capital Boundary

Funded execution remains manual and disabled by default. Before any future order:

- campaign drawdown must be below `$1.00`;
- conservative equity must remain at or above `$4.03`;
- order notional must not exceed `$1.00`;
- there may be at most one open order and one unresolved position;
- unresolved principal and fee risk count at worst case until terminal reconciliation;
- the account and durable ledger must agree within `$0.01`;
- risk state must survive restart and UTC midnight without resetting;
- a reconciled fill remains `position_unresolved` and reserves its full risk until the venue reports the condition terminal or the redemption worker verifies settlement;
- the user channel, REST state, clock, book, exact resolution source, campaign lease, geoblock result, country, and static egress IP must all pass.

Any failure activates the durable kill state. The worker rejects new intents, cancels the tracked order, and confirms zero open orders.

## Promotion Evidence

The frozen candidate is `dynamic_quote_style@2026-06-14`. The first shadow decision is made only by the shared engine path also used by replay. Promotion requires:

- 30 consecutive clean future days and at least 1,000 settled markets;
- wallet-constrained positive PnL under queue-conservative execution;
- a positive 95% block-bootstrap PnL lower bound;
- four positive chronological weekly blocks;
- modeled drawdown no greater than `$1.00`;
- 100% replay/runtime decision parity;
- at least 95% decision-grade data coverage;
- zero fatal, blocking, or unclassified data warnings;
- a positive 30-second executable markout lower bound after costs.

A clean day must cover the complete UTC window: the first event must arrive
within five minutes of `00:00`, the last event within five minutes of `24:00`,
all 24 UTC hour buckets must contain events, and no observed event-time gap may
exceed five minutes. A bootstrap day, restart gap, or partial rerun is still
published for auditability but receives a known blocking warning and cannot
increase the clean-day streak. Every report embeds the exact 40-character Git
revision, and each runtime writes a `runtime_provenance` event binding that
revision, full runtime-configuration hash, role, safety settings, frozen
candidate hash, execution-model binding, and storage destination. Provenance is
durably flushed before feeds start and repeated every minute. A clean day also
requires provenance within both five-minute UTC boundaries, no provenance gap
over five minutes, and one unchanged valid identity for the whole day. The
recorder SHA and report-builder SHA are stored independently: a historical
rebuild is expected to use a newer reporter while preserving the exact runtime
SHA from the source events. Their difference remains an informational lineage
warning; a missing/unknown SHA, invalid identity, wrong runtime role, or
mid-day-changing identity remains blocking. The publisher requires an explicit
expected runtime role and stores it with the report-builder SHA in the
immutable manifest; the shadow job passes `profitability_shadow`, while the
primary paper job passes `primary`.

Promotion statistics use only the current contiguous suffix of clean shadow
days. Older bootstrap, restart, or gapped days remain immutable and visible in
the prospective rows and cumulative wallet ledger, but their markets, model
PnL, parity, markouts, and confidence bounds cannot help a candidate pass. The
overall wallet-constrained equity and drawdown still include the full campaign,
so excluding a dirty day from statistical evidence cannot hide a real capital
loss.

An inconclusive result may extend once to 60 calendar days or 2,000 markets. If every promotion gate has not passed when either limit is reached, the immutable manifest enters terminal `stopped_no_go`; the candidate is not tuned inside its holdout.

### Projected-day cumulative replay

The daily job normally normalizes exactly one sealed UTC day. During a
historical correction or schema migration it automatically walks the affected
range in chronological order. Every normalization records the exhaustive,
ordered Azure raw-source inventory: account, container, exact day prefix, blob
name, ETag, length, last-modified time, blob type, seal state, optional Azure
version/MD5 metadata, and a SHA-256 computed from the bytes actually read. Each
download is conditional on the listed ETag and the complete listing is repeated
after the read; a changed, added, removed, duplicated, truncated, wrong-prefix,
or unexpected non-JSONL source fails before publication.

The job publishes decision-grade gzip shards under a content-addressed
immutable path, writes the day manifest last, and updates that day's
`latest.json` with compare-and-swap against the exact prior pointer. Campaign
replay first snapshots every daily pointer in the requested contiguous range,
verifies all manifests, raw inventories, and shard hashes, then revalidates the
pointer snapshots immediately before atomic materialization. It streams one
day's bounded shard set at a time. The Rust library rejects the current or
future UTC day even when the CLI is called directly; the shell check is not the
only protection against look-ahead.

The cumulative wallet snapshot schema is version 2 from 2026-07-13 onward. It
binds the campaign terminal hash, its parent hash, the exact campaign-index
bytes, cumulative replay state, and cumulative regimes artifact. A missing
date, late first snapshot, modified local shard, schema downgrade, bad parent,
or correction that breaks the existing sequence invalidates the entire wallet
ledger and blocks promotion. The validated sequence must start on the first
recorded projected day, 2026-07-13, and advance by exactly one UTC day; the
campaign identity remains anchored to 2026-07-12 without fabricating an empty
wallet snapshot for that bootstrap date.

All scheduled and manual shadow writers run under one renewable Azure Blob
lease. The child process is killed if renewal or ownership is lost, and the
daily script refuses direct execution without the lease context. A durable,
content-addressed correction journal is marked `in_progress` before the first
day and `complete` only after the entire chronological range, terminal
prospective validation, and profitability evaluation succeed. Labs and every
promotion-facing view fail closed while a correction is active or invalid.
Per-date pointers may advance during recovery, but the root daily pointer is
monotonic and cannot regress to an older corrected date.

One historical provenance limitation remains: schema-v2 proves the exact Azure
bytes observed during the corrected rerun, not what an unversioned append blob
contained before that first inventory was captured. ETag plus computed SHA-256
detects every later difference, but cannot reconstruct an earlier state that
Azure never retained. Azure Blob/container soft delete is retained for 14 days
and Change Feed for 30 days as extra deletion and mutation evidence. Blob
versioning is deliberately not enabled solely for this stream because Azure
does not create versions for `AppendBlock` operations. These protections cannot
retroactively prove pre-schema-v2 bytes.

Future physical sealing requires a writer-owned day-close barrier first. The
recorder must prove its queue and Azure buffers drained, publish an immutable
closed-day watermark, and reject or quarantine every later event for that day.
Only then can a dedicated least-privilege sealer job seal each AppendBlob and
make `sealed=true` mandatory in projected-day admission. Sealing without that
barrier is unsafe: a delayed prior-day append would return `BlobIsSealed` and
could obstruct later recorder flushes. Until that closure is deployed, reports
must describe a day as a *sealed UTC date boundary*, not claim that every raw
Azure AppendBlob is physically sealed. This limitation blocks any claim that a
rerun is the original historical capture, but it does not permit mixed or
silently mutated evidence into the shadow gate.

## Execution Model

Shadow promotion uses an immutable, zero-fill conservative prior. It deliberately has zero authenticated samples and is not marked promotion-ready; requiring a trained model before the first authenticated order would create an impossible circular gate.

The first practical model remains regularized and low-capacity. It is trained exactly once from orders 1–100, only after checkpoint 100 contains at least 100 distinct protocol-v3 eligible orders, including 10 fills and 10 non-fills. The immutable model artifact binds the exact checkpoint, dataset hash, training cutoff, order identities, and generation time. Orders 101–200 then use that frozen model without refitting. The terminal evaluation recomputes Brier score, expected calibration error, markout performance, cash-flow-adjusted net PnL, maximum drawdown, and the per-order PnL lower 95% bound only on this genuinely later holdout.

Protocol-v3 admission is exact and independently revalidated from raw evidence.
The controller and reporter derive fill timing, cancellation races, source
agreement, markout completeness, and 1/5/30/60-second labels instead of trusting
summary flags. They bind the recorded model features to the actual order,
market, and pre-send book context. Older funded orders remain visible in
lifetime PnL, but are display-only evidence and never increase a v3 checkpoint.

The trained model must improve Brier score over the horizon base-rate predictor by at least 5%, keep expected calibration error at or below 0.10, and retain a positive lower confidence bound for net executable markout. At checkpoints 25/100/200, the predeclared execution threshold is at least 10 filled orders with complete 30-second markouts and a positive lower 95% bound; it does not incorrectly require every funded order to fill. The orders 101–200 holdout must independently have positive net PnL, drawdown within `$1.00`, and a positive per-order PnL lower 95% bound. In-sample gains cannot mask a losing holdout or produce `profitable_go`.

Exact FIFO position remains unavailable. The only honest label is:

```text
queue_position_source = authenticated_lifecycle_plus_public_l2
queue_position = inferred_size_ahead
```

No model can reconstruct hidden or venue-internal priority that Polymarket does not publish.

## Azure Rollout

The continuous shadow worker runs in North Europe without wallet credentials, public ingress, or funded execution. Azure enforces separate storage trust domains:

- `polyedge-shadow-events`: the credential-free runtime can write events and immutable candidate intents only;
- `polyedge-research`: a separate no-Key-Vault research identity can read shadow events and write derived daily bundles and the shadow manifest;
- `polyedge-funded-evidence`: only the authenticated North Europe worker can write venue lifecycle, risk, control, and terminal evidence;
- `polyedge-models`: the no-Key-Vault trainer can write immutable trained models but can only read funded evidence.

The East US paper/API identity has no storage account key, no Key Vault secret-reader role, and no write access to funded evidence or trained models. Historical/research jobs publish immutable, hashed `COMPLETE` bundles before updating `latest`; dependent jobs wait instead of reading partial or stale results.

The current North Europe funded topology still shares one managed identity and
process boundary between the credentialed child and funded control writer. It
therefore does **not** yet satisfy independent funded-evidence attestation.
Deployment keeps `FUNDED_EVIDENCE_TRUST_BOUNDARY_READY=false`, and every
non-dry-run probe, canary, or ladder path fails closed while that remains false.
Shadow collection and research publishing are unaffected.

Before any funded stage can be authorized, Azure must split canonical control,
the venue signer, and terminal attestation into distinct identities and
containers: the controller has no secrets; the signer cannot write control or
promotion state; the attestor cannot sign orders; and all communication is by
immutable, hash-bound artifacts plus an exact Container Apps Job execution ID.
RBAC-negative tests must prove each forbidden access before the trust flag may
be enabled. The detailed closure plan is in
[`execution-quality-limitations.md`](execution-quality-limitations.md#remaining-funded-evidence-trust-boundary).

A passing shadow creates `shadow_passed`, not `canary_ready` and not a live order. Only the isolated controller can create the one-use `canary_ready` transition, and only after it atomically consumes a short-lived human grant bound to the exact candidate, promotion manifest, execution-model artifact, and first qualifying future intent. Dry-run controllers are read-only and cannot burn a grant, create an authorization, or invoke an order child.

The funded ladder is sequential and re-evaluated after exactly 1, 5, 25, 100, and at most 200 eligible orders. Each later stage requires a new short-lived, exact-state human grant. A submitted but ineligible order, orphan authorization, unresolved fill, missing terminal artifact, crash after authorization, drawdown breach, or data-quality failure blocks replacement spending. Stages 1 and 5 require positive realized PnL and positive mean 30-second net markout; stages 25 and later require a positive 95% markout lower bound. Checkpoint 100 additionally requires the immutable trained-model transition. Checkpoint 200 additionally requires the frozen-model orders-101–200 holdout to pass. No deposit or automatic replenishment is permitted by the default configuration.

Azure persists the canary, ladder, model-training, redemption, and promotion-transition jobs as manual, disabled, fail-closed jobs with empty exact artifact references. Deployment never starts them. A passing terminal `profitable_go` remains evidence, not automatic permission to trade; future capital deployment is a separate human decision.

The Labs API reports source, freshness, schema validity, and selection reason
for the shadow, funded, model, probe, and redemption artifacts. A real valid
canonical funded ladder remains authoritative even if older than a shadow
artifact. Before canonical funded state exists, selection prefers the newest
fresh valid manifest so a stale funded placeholder cannot hide current shadow
progress. These fields are observability metadata only and cannot authorize an
order or satisfy the terminal evidence validator.

Canonical promotion transitions are race-safe. Each result is first written to an immutable content-addressed transition blob, then `latest.json` is replaced with an Azure ETag compare-and-swap against `PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256`. A stale or concurrent job fails closed and cannot regress the ladder. Stage authorization itself also moves canonical state with ETag compare-and-swap, so the exact authorized prior hash must equal the expected canonical hash before advancement. The first passed-shadow initialization may create funded `latest.json` only with `If-None-Match: *`.

An immutable funded stage block has a separate terminal-only transition. The block binds the exact campaign, candidate, active target, authorized canonical manifest hash, and funded-ladder state hash. The credential-free promotion-transition identity can consume it with the same content-addressed/ETag-CAS path to move the manifest and ladder to absorbing `stopped_no_go`. This path has no grant input, cannot authorize an order, and the deployed manual job remains disabled with empty artifact references.

The same disabled credential-free job has an `expire` transition for sparse campaigns. It accepts only the exact prior canonical hash, requires the active nonterminal campaign to have reached `expires_at`, takes no evidence, grant, credential, or order input, and can only publish absorbing `stopped_no_go` through the same ETag-CAS path.

The transition workload uses a separate managed identity with Research Reader, Funded Evidence Contributor, Model Reader, and ACR Pull only; it has no Key Vault access and cannot retrieve venue credentials. The funded identity receives secret-level access only to the four required Polymarket credentials, not vault-wide access. Storage shared-key authorization is disabled; deployment migration uses the GitHub OIDC identity with container-scoped data roles.
