# Research evidence gates

Research runs remain separate from funded execution and qset promotion. The default replay wallet is the existing paper campaign wallet; it is not an authenticated current funded balance.

- Event ingestion skips missing or invalid top-level timestamps, counts them as malformed, and reports `invalid_timestamps`. It never substitutes wall-clock time.
- `replay --strategy-config` accepts the serialized `polyedge_config::StrategyConfig` JSON shape. The exact bytes are hashed. Without the option, existing defaults apply. `regimes --profile-config` reads the frozen candidate registry and hashes the same bytes it parses.
- `build-replay-index` binds every local normalized shard and the exhaustive raw-source inventory by SHA-256. It rejects missing, extra, or unbound files and remote paths. Its output is an input binding manifest, not a materialized feature database.
- `sample-size --fill-model queue_proxy_conservative` selects that exact baseline row. Multi-model inputs require the option. Claims use settled markets clustered into at least 28 consecutive UTC days, seven-day circular blocks and 10,000 deterministic resamples. IID intervals are descriptive. Missing queue or adverse-reference evidence prevents claims. Consumers match both model/profile and source hash; a static confidence interval cannot authorize an adaptive profile.
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
