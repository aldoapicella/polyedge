#!/usr/bin/env bash
set -euo pipefail

resource_group="${POLYEDGE_RESOURCE_GROUP:-rg-polyedge-dev}"
job_name="${POLYEDGE_VENUE_JOB:-polyedge-venue-probe-neu-job}"
container_name="${POLYEDGE_VENUE_CONTAINER:-venue-probe}"

read_persisted_dry_run() {
  az containerapp job show \
    --resource-group "$resource_group" \
    --name "$job_name" \
    --query "properties.template.containers[?name=='$container_name'] | [0].env[?name=='VENUE_PROBE_DRY_RUN'].value | [0]" \
    --output tsv
}

restore_dry_run() {
  local attempt state
  for attempt in 1 2 3 4 5; do
    if az containerapp job update \
      --resource-group "$resource_group" \
      --name "$job_name" \
      --container-name "$container_name" \
      --set-env-vars VENUE_PROBE_DRY_RUN=true \
      --output none; then
      state="$(read_persisted_dry_run || true)"
      if [[ "$state" == "true" ]]; then
        return 0
      fi
    fi
    sleep "$((attempt * 2))"
  done
  echo "critical: unable to restore and verify persisted VENUE_PROBE_DRY_RUN=true after 5 attempts" >&2
  return 1
}

if [[ "$(read_persisted_dry_run)" != "true" ]]; then
  echo "refusing start: persisted VENUE_PROBE_DRY_RUN is not true" >&2
  exit 1
fi

running="$(az containerapp job execution list \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --query "[?properties.status=='Running'] | length(@)" \
  --output tsv)"
if [[ "$running" != "0" ]]; then
  echo "refusing start: $running execution(s) already running" >&2
  exit 1
fi

trap restore_dry_run EXIT
az containerapp job update \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --container-name "$container_name" \
  --set-env-vars VENUE_PROBE_DRY_RUN=false \
  --output none

if [[ "$(read_persisted_dry_run)" != "false" ]]; then
  echo "refusing start: failed to arm the manual execution" >&2
  exit 1
fi

execution="$(az containerapp job start \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --query name \
  --output tsv)"

restore_dry_run
trap - EXIT

if [[ "$(read_persisted_dry_run)" != "true" ]]; then
  echo "critical: execution started but persisted dry-run was not restored" >&2
  exit 1
fi

execution_dry_run="$(az containerapp job execution show \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --job-execution-name "$execution" \
  --query "properties.template.containers[?name=='$container_name'] | [0].env[?name=='VENUE_PROBE_DRY_RUN'].value | [0]" \
  --output tsv)"
if [[ "$execution_dry_run" != "false" ]]; then
  echo "critical: execution snapshot is not order-enabled; execution=$execution" >&2
  exit 1
fi

printf '%s\n' "$execution"
