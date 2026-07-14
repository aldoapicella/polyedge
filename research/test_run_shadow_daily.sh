#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/work"

cat >"$TMP/bin/polyedge-rs" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$POLYEDGE_TEST_ARGS"
out=""
command=""
previous=""
for argument in "$@"; do
  if [ "$previous" = "--out" ]; then
    out="$argument"
  fi
  if [ "$argument" = "normalize" ]; then
    command="normalize"
  elif [ "$argument" = "materialize-projected-campaign" ]; then
    command="materialize"
  fi
  previous="$argument"
done
if [ -n "$out" ]; then
  if [ "$command" = "normalize" ]; then
    mkdir -p "$out"
    printf '%s\n' '{"events":1}' >"$out/events_manifest.json"
  elif [ "$command" = "materialize" ]; then
    mkdir -p "$out"
  else
    mkdir -p "$(dirname "$out")"
    printf '%s\n' '{}' >"$out"
  fi
fi
exit 0
EOF
chmod +x "$TMP/bin/polyedge-rs"

(
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args" \
  SHADOW_REPORT_DATE=2026-07-13 \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
  sh "$REPO/research/run_shadow_daily.sh" >"$TMP/stdout"
)

test "$(grep -c '^research normalize ' "$TMP/args")" -eq 1
grep -F 'research normalize --input azure://stpolyedge/polyedge-shadow-events/shadow-events/campaign-2026-07-12/2026/07/13/' "$TMP/args" >/dev/null
if grep -E 'research normalize --input .*campaign-2026-07-12/\?prefetch' "$TMP/args" >/dev/null; then
  echo "campaign-wide raw normalization was invoked" >&2
  exit 1
fi
grep -F 'research publish-projected-day ' "$TMP/args" >/dev/null
grep -F 'research materialize-projected-campaign --since 2026-07-13 --through 2026-07-13 ' "$TMP/args" >/dev/null
grep -F 'research build-cumulative-wallet ' "$TMP/args" | grep -F -- '--campaign-manifest ' >/dev/null
grep -F 'stage=normalize-day date=2026-07-13 status=starting' "$TMP/stdout" >/dev/null
if grep -F '{' "$TMP/stdout" >/dev/null; then
  echo "shadow daily emitted verbose JSON instead of stage markers" >&2
  exit 1
fi

if (
  cd "$TMP/work"
  PATH="$TMP/bin:$PATH" \
  POLYEDGE_TEST_ARGS="$TMP/args-current" \
  SHADOW_REPORT_DATE="$(date -u +%Y-%m-%d)" \
  SHADOW_SOURCE_CONTAINER_NAME=polyedge-shadow-events \
  SHADOW_EXECUTION_MODEL_BLOB_NAME=models/prior.json \
  AZURE_STORAGE_ACCOUNT_NAME=stpolyedge \
  AZURE_STORAGE_CONTAINER_NAME=polyedge-research \
  sh "$REPO/research/run_shadow_daily.sh" >/dev/null 2>&1
); then
  echo "shadow daily accepted an unsealed current UTC day" >&2
  exit 1
fi
