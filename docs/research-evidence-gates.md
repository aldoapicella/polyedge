# Research evidence gates

Research runs remain separate from funded execution and qset promotion. The default replay wallet is the existing paper campaign wallet; it is not an authenticated current funded balance.

## Primary capture and prospective collection

The primary paper collector retains recent actual book observations for delayed
source-timestamp fills. It preserves the original fill and observation times;
the 1/5/30-second horizons and inclusive two-second deadline remain unchanged.
Absent executable observations remain explicit missing evidence. This cannot
repair a previously failed frozen experiment.

Only primary paper capture also requests public REST books while a markout is
pending. A request gets at most one second, capped by the remaining observation
deadline; the OCI host measured about 500 ms per fresh public-book request.
Actual response receipt time remains the observation time. The raw
response must be durably recorded before it can complete a markout; the snapshot
does not feed strategy decisions, paper fills or risk state. Reservations protect
an on-time response during its durable write, without extending the deadline.

`POLYEDGE_DATA_QUALITY_ONLY=true` selects the existing daily normalizer, audit and
execution-quality commands without candidate evaluation. The native
`check-primary-daily-quality` command shares the daily publisher's provenance,
coverage and warning checks. Its receipt binds the normalized manifest and the
two reports; a successful completion can be reused only with matching evidence.
Primary local daily/replay jobs use the existing host research lock. Azure-backed
qset leases remain independent.

`research/verify_primary_oci_day.py` binds a closed local day to an exhaustive
authenticated OCI listing, object ETags, SHA-256 checksums and recorder ranges.
It reads remote manifests and metadata, not market payloads. This source proof
must match the native quality receipt; neither receipt alone is promotion proof.

`research/primary_normalized_snapshot.py` archives admitted normalized snapshots
to content-addressed OCI objects. Cache payloads may be evicted only after full
authenticated readback matches every byte hash. Local manifests, completion
markers, reports and source receipts remain for daily continuity checks. Restore
the required partition from the receipt before native research stages; check
working-space capacity first. Raw source objects and failed evidence are retained.

The installed `polyedge-primary-research-day` driver reads a fixed pilot date,
future start date, immutable binary/revision and frozen wallet/candidate bindings
from `/etc/polyedge/primary-research.json`. It uses the ordinary daily timer.
Disable the standalone replay timer during this collection: replay/index creation
belongs to corpus admission after the fixed selection window closes.
Before the pilot closes it waits. After a successful pilot and source proof it
runs `research/preregister_primary.py --publish`, which freezes and reads back
create-only OCI contracts. Failed or incomplete preregistration blocks subsequent
days. No automatic replacement experiment or date shift is allowed.

The replacement uses one training day, 28 validation days, one settlement-only
carry day, 28 sealed test days and one test carry day. The driver processes only
the pilot and selection days. It refuses to skip an unverified previous selection
day and never opens test payloads. Original failed contracts and raw evidence are
retained as evidence; deprecated executable configuration is removed.

After selection closes, use the existing native market/index, baseline, regimes,
sample-size and sweep commands on the admitted content-addressed corpus. Verify
the baseline twice, freeze identical market populations and wallet assumptions,
then execute 20 profile replays plus at most 96 five-model sweep candidates
(500 total). A single eligible validation winner may open the sealed test once.
The carry audit, corpus admission, candidate selection, test opening and separate
authenticated funded reconciliation remain evidence-dependent research stages;
the daily collector does not claim or manufacture their completion.

Focused checks:

```sh
cargo test -p polyedge-api runtime::execution_quality::tests --lib
sh research/test_primary_daily_jobs.sh
python3 -m unittest discover -s research -p test_verify_primary_oci_day.py
python3 research/test_preregister_primary.py
python3 ops/conduit/test/test-primary-research-day.py
```

`replay`, `baseline`, `regimes`, and `sweep` accept `--wallet-config` with an exact-byte-hashed JSON object containing `campaign_baseline`, `equity_floor`, `maximum_drawdown`, `maximum_order_notional`, and `maximum_unresolved_orders_or_positions` (currently exactly one). Positive decimal limits and a nonnegative floor below the baseline are required; unknown fields fail. The same parsed wallet is used across every candidate, fill model and fixed-winner holdout, and its hash is included in the pre-test receipt. An optional `simulated_initial_equity` and `current_equity_policy` must be supplied together. The policy requires `reserve_ratio`, `minimum_reserve`, `target_order_ratio`, `operating_buffer_ratio`, `minimum_order_notional`, `fee_rate` and integer `fee_exponent`. Replay then follows the funded current-equity reserve and sizing rules, including venue minimums, two-decimal shares, six-decimal money and fee reservation for maker orders. Venue minimums must be observed before each decision; preloaded final truth cannot supply them. Fee inputs are frozen counterfactual contract assumptions, not observed historical market evidence. Replay PnL starts at zero from the supplied initial equity; campaign PnL remains separately measured from its immutable baseline. Current-equity sizing does not apply the legacy trailing drawdown budget; drawdown remains reported for research risk assessment. Omitting both options preserves the historical static wallet.

- Event ingestion skips missing or invalid top-level timestamps, counts them as malformed, and reports `invalid_timestamps`. It never substitutes wall-clock time.
- `replay --strategy-config` accepts the serialized `polyedge_config::StrategyConfig` JSON shape. The exact bytes are hashed. Without the option, existing defaults apply. `regimes --profile-config` reads the frozen candidate registry and hashes the same bytes it parses.
- `build-replay-index` binds every local normalized shard and the exhaustive raw-source inventory by SHA-256. It rejects missing, extra, or unbound files and remote paths. Its output is an input binding manifest, not a materialized feature database.
- `sample-size --fill-model queue_proxy_conservative` selects that exact baseline row. Multi-model inputs require the option. Claims use settled markets clustered into at least 28 consecutive UTC validation days (29 total selection days including the first training day), seven-day circular blocks and 10,000 deterministic resamples. IID intervals are descriptive. Missing queue or adverse-reference evidence prevents claims. Consumers match both model/profile and source hash; a static confidence interval cannot authorize an adaptive profile.
- `sweep` uses its ordinary input only for chronological training/validation. It compares five fill models, ranks wallet-constrained settlement PnL across the conservative models, and caps the search at 100 candidates / 500 selection replays. Touch is diagnostic; no-maker fills is a control.
- An optional `--test-input` must be a separate local normalized corpus. Supply separate `--test-markets`, or omit it to derive market truth from the held-out events. The holdout stays unopened unless validation passes. The winner and source bindings are persisted before holdout events or settlement labels are read. Input hashes are checked before and after replay; overlapping market lifecycles fail closed.
- Holdout consumption receipts live under `${XDG_STATE_HOME:-$HOME/.local/state}/polyedge/research/holdout-receipts`, keyed by immutable raw-source content hashes. Changing `--out` does not permit another evaluation. This is a local single-research-owner safeguard, not a distributed authorization service. Preserve that state when moving research to another host. Do not retune after a holdout result; use new future evidence.
- A fixed winner needs positive held-out wallet PnL and a positive block lower bound across the conservative models, with the same minimum temporal coverage. These research gates never authorize live deployment.

Example validation run (no holdout content opened):

```bash
polyedge-rs research sweep --input /data/selection-normalized \
  --markets /data/selection-markets.json --max-experiments 100 \
  --out /reports/selection.json --markdown /reports/selection.md
```

Example fixed-winner holdout evaluation after validation sufficiency is established:

```bash
polyedge-rs research sweep --input /data/selection-normalized \
  --markets /data/selection-markets.json \
  --test-input /data/sealed-test-normalized --test-markets /data/sealed-test-markets.json \
  --max-experiments 100 --out /reports/evaluation.json --markdown /reports/evaluation.md
```

Do not substitute qset holdouts, historical diagnostic campaign artifacts, incomplete intervals, or partially listed data for a missing eligible corpus. Metadata inventory alone is not a content-verified research dataset.
