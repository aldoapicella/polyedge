#!/usr/bin/env bash
set -euo pipefail

: "${AZURE_RESOURCE_GROUP:?}"
: "${SERVICE_BUS_NAMESPACE:?}"
: "${SERVICE_BUS_QUEUE:?}"
readonly output_file="${1:?queue snapshot output file required}"

queue_fenced=false
for attempt in 1 2 3; do
  if az servicebus queue update \
      --resource-group "$AZURE_RESOURCE_GROUP" \
      --namespace-name "$SERVICE_BUS_NAMESPACE" \
      --name "$SERVICE_BUS_QUEUE" \
      --status SendDisabled \
      --only-show-errors -o none; then
    queue_fenced=true
    break
  fi
  sleep "$((attempt * 5))"
done
test "$queue_fenced" = true

queue_drained=false
for attempt in $(seq 1 36); do
  az servicebus queue show \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --namespace-name "$SERVICE_BUS_NAMESPACE" \
    --name "$SERVICE_BUS_QUEUE" \
    -o json > "$output_file"
  if jq -e '
    .status == "SendDisabled"
    and .countDetails.activeMessageCount == 0
    and .countDetails.scheduledMessageCount == 0
  ' "$output_file" >/dev/null; then
    queue_drained=true
    break
  fi
  sleep 5
done
test "$queue_drained" = true
