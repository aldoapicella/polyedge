#!/bin/sh
set -eu

DATE="$(date -u -d 'yesterday' +%Y-%m-%d)"
TODAY="$(date -u +%Y-%m-%d)"
DAY="$(date -u -d "$DATE" +%Y/%m/%d)"
RUN_ID="shadow-$DATE-$(date -u +%Y%m%dT%H%M%SZ)"
SOURCE_CONTAINER="${SHADOW_SOURCE_CONTAINER_NAME:?SHADOW_SOURCE_CONTAINER_NAME is required}"
EXECUTION_MODEL_BLOB_NAME="${SHADOW_EXECUTION_MODEL_BLOB_NAME:?SHADOW_EXECUTION_MODEL_BLOB_NAME is required}"
ROOT="azure://$AZURE_STORAGE_ACCOUNT_NAME/$SOURCE_CONTAINER"
CAMPAIGN_PREFIX="shadow-events/campaign-2026-07-12"
INPUT="$ROOT/$CAMPAIGN_PREFIX/$DAY/?prefetch_blobs=16"
CUMULATIVE_INPUT="$ROOT/$CAMPAIGN_PREFIX/?prefetch_blobs=16"
NORMALIZED="data/research/shadow/$DATE/normalized"
CUMULATIVE_NORMALIZED="data/research/shadow/cumulative/$DATE/normalized"
STAGING="reports/research/shadow/staging/$RUN_ID"
MARKETS="$STAGING/markets_summary.json"
CUMULATIVE_MARKETS="$STAGING/cumulative_markets_summary.json"
CUMULATIVE_REGIMES="$STAGING/cumulative_regimes.json"
CUMULATIVE_EXCLUSION="${TODAY}T00:00:00Z..2100-01-01T00:00:00Z"

mkdir -p "$STAGING" "$NORMALIZED" "$CUMULATIVE_NORMALIZED"

polyedge-rs research audit --input "$INPUT" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/raw_data_audit.json" --markdown "$STAGING/raw_data_audit.md"
polyedge-rs research normalize --input "$INPUT" --out "$NORMALIZED" --format jsonl-indexed-gzip-sharded --overwrite true --decision-grade-projection true
polyedge-rs research audit --input "$NORMALIZED" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/data_audit.json" --markdown "$STAGING/data_audit.md"
polyedge-rs research execution-quality --input "$NORMALIZED" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/execution_quality.json" --markdown "$STAGING/execution_quality.md"
polyedge-rs research build-markets --input "$NORMALIZED" --exclude-file data_quality/exclusion_windows.yaml --out "$MARKETS" --markdown "$STAGING/markets_summary.md"
polyedge-rs research baseline --input "$NORMALIZED" --markets "$MARKETS" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/baseline.json" --markdown "$STAGING/baseline.md"
polyedge-rs research regimes --input "$NORMALIZED" --markets "$MARKETS" --fill-model queue_proxy_conservative --profile-config research/configs/frozen_candidates.yaml --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/regimes.json" --markdown "$STAGING/regimes.md"
polyedge-rs research calibration --input "$NORMALIZED" --markets "$MARKETS" --exclude-file data_quality/exclusion_windows.yaml --out "$STAGING/calibration.json" --markdown "$STAGING/calibration.md"
polyedge-rs research report --reports-dir "$STAGING" --out "$STAGING/final_report.json" --markdown "$STAGING/final_report.md"

# Rebuild the campaign wallet from the fixed shadow stream every day. Current-
# day events are normalized for deterministic inventory but excluded from both
# truth construction and replay, preventing look-ahead beyond the snapshot.
polyedge-rs research normalize --input "$CUMULATIVE_INPUT" --out "$CUMULATIVE_NORMALIZED" --format jsonl-indexed-gzip-sharded --overwrite true --decision-grade-projection true
polyedge-rs research build-markets --input "$CUMULATIVE_NORMALIZED" --exclude-file data_quality/exclusion_windows.yaml --exclude-window "$CUMULATIVE_EXCLUSION" --out "$CUMULATIVE_MARKETS" --markdown "$STAGING/cumulative_markets_summary.md"
polyedge-rs research regimes --input "$CUMULATIVE_NORMALIZED" --markets "$CUMULATIVE_MARKETS" --fill-model queue_proxy_conservative --profile-config research/configs/frozen_candidates.yaml --exclude-file data_quality/exclusion_windows.yaml --exclude-window "$CUMULATIVE_EXCLUSION" --out "$CUMULATIVE_REGIMES" --markdown "$STAGING/cumulative_regimes.md"
polyedge-rs research build-cumulative-wallet --regimes "$CUMULATIVE_REGIMES" --normalized-manifest "$CUMULATIVE_NORMALIZED/events_manifest.json" --snapshot-date "$DATE" --out "$STAGING/cumulative_wallet.json"

INPUT_SHA="sha256:$(sha256sum "$NORMALIZED/events_manifest.json" | cut -d' ' -f1)"
polyedge-rs research publish-daily-bundle --date "$DATE" --run-id "$RUN_ID" --input-sha256 "$INPUT_SHA" --expected-runtime-role profitability_shadow --source-dir "$STAGING" --output-root reports/research/shadow/daily --data-audit "$STAGING/data_audit.json"
polyedge-rs research validate-prospective --since 2026-07-12T00:00:00Z --candidates research/configs/frozen_candidates.yaml --reports-dir reports/research/shadow/daily --expected-daily-date "$DATE" --out reports/research/shadow/prospective/prospective_validation.json --markdown reports/research/shadow/prospective/prospective_validation.md
polyedge-rs research evaluate-profitability --daily-root reports/research/shadow/daily --prospective reports/research/shadow/prospective/prospective_validation.json --gate-config research/configs/profitability_gate.yaml --execution-model "$EXECUTION_MODEL_BLOB_NAME" --out reports/research/profitability/latest.json
