#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT HUP INT TERM

cat >"$TMP/polyedge-rs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$PROMOTION_TEST_ARGS"
EOF
chmod +x "$TMP/polyedge-rs"

HASH_A="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
HASH_B="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
export PATH="$TMP:$PATH"
export PROMOTION_TEST_ARGS="$TMP/args"
export PROMOTION_TRANSITION_ENABLED=true
export PROMOTION_TRANSITION_MODE=stop-stage-block
export PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256="$HASH_A"
export PROMOTION_PRIOR_MANIFEST_URI=azure://st/funded/reports/research/profitability/latest.json
export PROMOTION_PRIOR_MANIFEST_SHA256="$HASH_A"
export PROMOTION_STAGE_BLOCK_URI=azure://st/funded/control/stage-block.json
export PROMOTION_STAGE_BLOCK_SHA256="$HASH_B"
export PROMOTION_OUTPUT_BLOB_NAME=reports/research/profitability/latest.json

sh "$ROOT/research/run_promotion_transition.sh"
grep -Fx 'stop-funded-manifest-from-stage-block' "$TMP/args" >/dev/null
grep -Fx "$PROMOTION_STAGE_BLOCK_URI" "$TMP/args" >/dev/null
grep -Fx "$HASH_B" "$TMP/args" >/dev/null

if PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256="$HASH_B" \
  sh "$ROOT/research/run_promotion_transition.sh" >"$TMP/out" 2>"$TMP/err"; then
  echo "mismatched canonical hash unexpectedly passed" >&2
  exit 1
fi
grep -F 'must equal the exact prior manifest' "$TMP/err" >/dev/null

export PROMOTION_TRANSITION_MODE=expire
export PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256="$HASH_A"
sh "$ROOT/research/run_promotion_transition.sh"
grep -Fx 'expire-funded-manifest' "$TMP/args" >/dev/null
if grep -F -- '--stage-block' "$TMP/args" >/dev/null; then
  echo "expiration unexpectedly received a stage block" >&2
  exit 1
fi

echo "promotion transition shell tests passed"
