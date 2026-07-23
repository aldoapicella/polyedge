#!/usr/bin/env bash
set -euo pipefail

resource_group="${POLYEDGE_RESOURCE_GROUP:-rg-polyedge-dev}"
job_name="${POLYEDGE_REDEMPTION_JOB:-polyedge-redeem-neu-job}"
container_name="${POLYEDGE_REDEMPTION_CONTAINER:-venue-redemption}"
vault_name="${POLYEDGE_KEY_VAULT:-kvpolyedge6urdjr5nmwx7w}"

read_env() {
  local name="$1"
  az containerapp job show \
    --resource-group "$resource_group" \
    --name "$job_name" \
    --query "properties.template.containers[?name=='$container_name'] | [0].env[?name=='$name'].value | [0]" \
    --output tsv
}

if [[ "$(read_env VENUE_REDEMPTION_DRY_RUN)" != "true" || "$(read_env VENUE_REDEMPTION_ENABLED)" != "false" ]]; then
  echo "refusing start: persisted redemption job is not disabled and dry-run" >&2
  exit 1
fi

if ! az keyvault secret show --vault-name "$vault_name" --name polymarket-relayer-api-key --query id --output tsv >/dev/null; then
  echo "refusing start: polymarket-relayer-api-key is absent from Key Vault" >&2
  exit 1
fi

if ! az containerapp job show \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --query "properties.template.containers[?name=='$container_name'] | [0].env[?name=='POLYMARKET_RELAYER_API_KEY'].secretRef | [0]" \
  --output tsv | grep -qx 'polymarket-relayer-api-key'; then
  echo "refusing start: job has not been redeployed with relayerApiKeySecretConfigured=true" >&2
  exit 1
fi

running="$(az containerapp job execution list \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --query "[?properties.status=='Running'] | length(@)" \
  --output tsv)"
if [[ "$running" != "0" ]]; then
  echo "refusing start: $running redemption execution(s) already running" >&2
  exit 1
fi

execution="$(az containerapp job start \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --container-name "$container_name" \
  --env-vars VENUE_REDEMPTION_DRY_RUN=false VENUE_REDEMPTION_ENABLED=true \
  --query name \
  --output tsv)"

if [[ "$(read_env VENUE_REDEMPTION_DRY_RUN)" != "true" || "$(read_env VENUE_REDEMPTION_ENABLED)" != "false" ]]; then
  echo "critical: execution started but persisted redemption defaults changed" >&2
  exit 1
fi

execution_dry_run="$(az containerapp job execution show \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --job-execution-name "$execution" \
  --query "properties.template.containers[?name=='$container_name'] | [0].env[?name=='VENUE_REDEMPTION_DRY_RUN'].value | [0]" \
  --output tsv)"
execution_enabled="$(az containerapp job execution show \
  --resource-group "$resource_group" \
  --name "$job_name" \
  --job-execution-name "$execution" \
  --query "properties.template.containers[?name=='$container_name'] | [0].env[?name=='VENUE_REDEMPTION_ENABLED'].value | [0]" \
  --output tsv)"
if [[ "$execution_dry_run" != "false" || "$execution_enabled" != "true" ]]; then
  echo "critical: execution snapshot is not explicitly redemption-enabled; execution=$execution" >&2
  exit 1
fi

printf '%s\n' "$execution"
