#!/bin/sh
set -eu

if [ "${PROMOTION_TRANSITION_ENABLED:-false}" != "true" ]; then
  echo "promotion transition is disabled" >&2
  exit 64
fi

OUT="${PROMOTION_OUTPUT_BLOB_NAME:-reports/research/profitability/latest.json}"
MODE="${PROMOTION_TRANSITION_MODE:-}"

require() {
  name="$1"
  value="$(printenv "$name" 2>/dev/null || true)"
  if [ -z "$value" ]; then
    echo "$name is required" >&2
    exit 64
  fi
}

normalize_sha256() {
  value="$1"
  value="$(printf '%s' "$value" | tr 'A-F' 'a-f')"
  printf '%s' "${value#sha256:}"
}

require PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256

case "$MODE" in
  initialize)
    for name in \
      PROMOTION_SHADOW_MANIFEST_URI PROMOTION_SHADOW_MANIFEST_SHA256 \
      PROMOTION_CANARY_EVIDENCE_URI PROMOTION_CANARY_EVIDENCE_BLOB_NAME \
      PROMOTION_CANARY_EVIDENCE_SHA256 \
      PROMOTION_CANARY_CONSUMPTION_URI PROMOTION_CANARY_CONSUMPTION_SHA256 \
      PROMOTION_TERMINAL_EVIDENCE_URI PROMOTION_TERMINAL_EVIDENCE_BLOB_NAME \
      PROMOTION_TERMINAL_EVIDENCE_SHA256
    do
      require "$name"
    done
    if [ "$(normalize_sha256 "$PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256")" != "$(normalize_sha256 "$PROMOTION_SHADOW_MANIFEST_SHA256")" ]; then
      echo "initialization expected canonical SHA-256 must equal the exact shadow source SHA-256" >&2
      exit 64
    fi
    export PROMOTION_TRANSITION_INITIALIZE_IF_ABSENT=true
    polyedge-rs research initialize-funded-manifest \
      --shadow-manifest "$PROMOTION_SHADOW_MANIFEST_URI" \
      --shadow-manifest-sha256 "$PROMOTION_SHADOW_MANIFEST_SHA256" \
      --canary-evidence "$PROMOTION_CANARY_EVIDENCE_URI" \
      --canary-evidence-blob-name "$PROMOTION_CANARY_EVIDENCE_BLOB_NAME" \
      --canary-evidence-sha256 "$PROMOTION_CANARY_EVIDENCE_SHA256" \
      --human-grant-consumption "$PROMOTION_CANARY_CONSUMPTION_URI" \
      --human-grant-consumption-sha256 "$PROMOTION_CANARY_CONSUMPTION_SHA256" \
      --terminal-evidence "$PROMOTION_TERMINAL_EVIDENCE_URI" \
      --terminal-evidence-blob-name "$PROMOTION_TERMINAL_EVIDENCE_BLOB_NAME" \
      --terminal-evidence-sha256 "$PROMOTION_TERMINAL_EVIDENCE_SHA256" \
      --out "$OUT"
    ;;
  advance)
    for name in \
      PROMOTION_PRIOR_MANIFEST_URI PROMOTION_PRIOR_MANIFEST_SHA256 \
      PROMOTION_CHECKPOINT_URI PROMOTION_CHECKPOINT_SHA256
    do
      require "$name"
    done
    if [ "$(normalize_sha256 "$PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256")" != "$(normalize_sha256 "$PROMOTION_PRIOR_MANIFEST_SHA256")" ]; then
      echo "advance expected canonical SHA-256 must equal the exact prior manifest SHA-256" >&2
      exit 64
    fi
    export PROMOTION_TRANSITION_INITIALIZE_IF_ABSENT=false
    if [ -n "${PROMOTION_NEXT_MODEL_URI:-}" ] || [ -n "${PROMOTION_NEXT_MODEL_SHA256:-}" ]; then
      require PROMOTION_NEXT_MODEL_URI
      require PROMOTION_NEXT_MODEL_SHA256
      polyedge-rs research advance-funded-manifest \
        --prior-manifest "$PROMOTION_PRIOR_MANIFEST_URI" \
        --prior-manifest-sha256 "$PROMOTION_PRIOR_MANIFEST_SHA256" \
        --observation "$PROMOTION_CHECKPOINT_URI" \
        --observation-sha256 "$PROMOTION_CHECKPOINT_SHA256" \
        --next-execution-model "$PROMOTION_NEXT_MODEL_URI" \
        --next-execution-model-blob-uri "$PROMOTION_NEXT_MODEL_URI" \
        --next-execution-model-sha256 "$PROMOTION_NEXT_MODEL_SHA256" \
        --out "$OUT"
    else
      polyedge-rs research advance-funded-manifest \
        --prior-manifest "$PROMOTION_PRIOR_MANIFEST_URI" \
        --prior-manifest-sha256 "$PROMOTION_PRIOR_MANIFEST_SHA256" \
        --observation "$PROMOTION_CHECKPOINT_URI" \
        --observation-sha256 "$PROMOTION_CHECKPOINT_SHA256" \
        --out "$OUT"
    fi
    ;;
  stop-stage-block)
    for name in \
      PROMOTION_PRIOR_MANIFEST_URI PROMOTION_PRIOR_MANIFEST_SHA256 \
      PROMOTION_STAGE_BLOCK_URI PROMOTION_STAGE_BLOCK_SHA256
    do
      require "$name"
    done
    if [ "$(normalize_sha256 "$PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256")" != "$(normalize_sha256 "$PROMOTION_PRIOR_MANIFEST_SHA256")" ]; then
      echo "stage-block expected canonical SHA-256 must equal the exact prior manifest SHA-256" >&2
      exit 64
    fi
    export PROMOTION_TRANSITION_INITIALIZE_IF_ABSENT=false
    polyedge-rs research stop-funded-manifest-from-stage-block \
      --prior-manifest "$PROMOTION_PRIOR_MANIFEST_URI" \
      --prior-manifest-sha256 "$PROMOTION_PRIOR_MANIFEST_SHA256" \
      --stage-block "$PROMOTION_STAGE_BLOCK_URI" \
      --stage-block-sha256 "$PROMOTION_STAGE_BLOCK_SHA256" \
      --out "$OUT"
    ;;
  expire)
    for name in PROMOTION_PRIOR_MANIFEST_URI PROMOTION_PRIOR_MANIFEST_SHA256
    do
      require "$name"
    done
    if [ "$(normalize_sha256 "$PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256")" != "$(normalize_sha256 "$PROMOTION_PRIOR_MANIFEST_SHA256")" ]; then
      echo "expiration expected canonical SHA-256 must equal the exact prior manifest SHA-256" >&2
      exit 64
    fi
    export PROMOTION_TRANSITION_INITIALIZE_IF_ABSENT=false
    polyedge-rs research expire-funded-manifest \
      --prior-manifest "$PROMOTION_PRIOR_MANIFEST_URI" \
      --prior-manifest-sha256 "$PROMOTION_PRIOR_MANIFEST_SHA256" \
      --out "$OUT"
    ;;
  *)
    echo "PROMOTION_TRANSITION_MODE must be initialize, advance, stop-stage-block, or expire" >&2
    exit 64
    ;;
esac
