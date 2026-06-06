#!/usr/bin/env bash
set -euo pipefail

subscription="${AZURE_SUBSCRIPTION:-Visual Studio Professional Subscription}"
legacy_resource_group="${LEGACY_RESOURCE_GROUP:-rg-polymarket-btc15-dev}"
legacy_app="${LEGACY_CONTAINER_APP:-polymarket-btc15-dev}"
polyedge_resource_group="${POLYEDGE_RESOURCE_GROUP:-rg-polyedge-dev}"
polyedge_app="${POLYEDGE_CONTAINER_APP:-polyedge-dev}"
polyedge_deployment_name="${POLYEDGE_DEPLOYMENT_NAME:-polyedge-active-cutover}"
legacy_storage_account="${LEGACY_STORAGE_ACCOUNT:-stpolymarketbtc1556k4mk6}"
polyedge_storage_account="${POLYEDGE_STORAGE_ACCOUNT:-stpolyedge6urdjr5nmwx7w}"
storage_container="${AZURE_STORAGE_CONTAINER_NAME:-bot-events}"
token_file="${API_BEARER_TOKEN_FILE:-data/api-bearer-token.txt}"
state_dir="${CUTOVER_STATE_DIR:-output/cutover}"
cutover_id="$(date -u +%Y%m%dT%H%M%SZ)"
state_file="${state_dir}/${cutover_id}.json"
summary_file="${state_dir}/${cutover_id}-final-copy.json"
python_bin="${PYTHON_BIN:-python3}"

legacy_writer_disabled=false
polyedge_activation_started=false
polyedge_writer_verified=false

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

env_value() {
  local resource_group="$1"
  local app_name="$2"
  local container_name="$3"
  local env_name="$4"
  az containerapp show \
    --resource-group "$resource_group" \
    --name "$app_name" \
    -o json \
    | jq -r --arg container_name "$container_name" --arg env_name "$env_name" '
        .properties.template.containers[]
        | select(.name == $container_name)
        | (.env // [])[]
        | select(.name == $env_name)
        | (.value // (if .secretRef then "secretref:" + .secretRef else "" end))
      ' \
    | sed -n '1p'
}

container_image() {
  local resource_group="$1"
  local app_name="$2"
  local container_name="$3"
  az containerapp show \
    --resource-group "$resource_group" \
    --name "$app_name" \
    --query "properties.template.containers[?name=='${container_name}'].image | [0]" \
    -o tsv
}

containerapp_fqdn() {
  az containerapp show \
    --resource-group "$1" \
    --name "$2" \
    --query properties.configuration.ingress.fqdn \
    -o tsv
}

wait_containerapp_ready() {
  local resource_group="$1"
  local app_name="$2"
  local timeout_seconds="${3:-300}"
  local deadline=$((SECONDS + timeout_seconds))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local state running
    state="$(az containerapp show --resource-group "$resource_group" --name "$app_name" --query properties.provisioningState -o tsv)"
    running="$(az containerapp show --resource-group "$resource_group" --name "$app_name" --query properties.runningStatus -o tsv)"
    if [ "$state" = "Succeeded" ] && [ "$running" = "Running" ]; then
      return 0
    fi
    sleep 5
  done
  echo "Timed out waiting for ${app_name} to become ready." >&2
  return 1
}

disable_writer() {
  local resource_group="$1"
  local app_name="$2"
  local expected="$3"
  local current
  current="$(env_value "$resource_group" "$app_name" bot RUN_BOT_ON_STARTUP || true)"
  if [ "$current" = "$expected" ]; then
    log "${app_name} RUN_BOT_ON_STARTUP is already ${expected}"
    return 0
  fi
  az containerapp update \
    --resource-group "$resource_group" \
    --name "$app_name" \
    --container-name bot \
    --set-env-vars "RUN_BOT_ON_STARTUP=${expected}" \
    --termination-grace-period 60 \
    --only-show-errors \
    --output none
  wait_containerapp_ready "$resource_group" "$app_name" 420
  current="$(env_value "$resource_group" "$app_name" bot RUN_BOT_ON_STARTUP)"
  if [ "$current" != "$expected" ]; then
    echo "${app_name} RUN_BOT_ON_STARTUP is ${current}, expected ${expected}" >&2
    return 1
  fi
}

rollback_on_error() {
  local exit_code=$?
  local line_no=${1:-unknown}
  if [ "$exit_code" -eq 0 ]; then
    return
  fi
  log "Cutover failed at line ${line_no} with exit code ${exit_code}."
  if [ "$polyedge_writer_verified" != "true" ]; then
    if [ "$polyedge_activation_started" = "true" ]; then
      log "Disabling PolyEdge writer before rollback."
      disable_writer "$polyedge_resource_group" "$polyedge_app" false || true
    fi
    if [ "$legacy_writer_disabled" = "true" ]; then
      log "Re-enabling legacy writer as rollback."
      disable_writer "$legacy_resource_group" "$legacy_app" true || true
    fi
  fi
  exit "$exit_code"
}

trap 'rollback_on_error $LINENO' ERR

for command_name in az curl jq "$python_bin"; do
  require_cmd "$command_name"
done

"$python_bin" - <<'PY'
from azure.storage.blob import BlobServiceClient  # noqa: F401
PY

mkdir -p "$state_dir"

if [ -z "${API_BEARER_TOKEN:-}" ]; then
  if [ ! -s "$token_file" ]; then
    echo "Missing API_BEARER_TOKEN and $token_file is not readable." >&2
    exit 1
  fi
  API_BEARER_TOKEN="$(<"$token_file")"
fi

az account set --subscription "$subscription"

legacy_fqdn="$(containerapp_fqdn "$legacy_resource_group" "$legacy_app")"
polyedge_fqdn="$(containerapp_fqdn "$polyedge_resource_group" "$polyedge_app")"
polyedge_backend_image="$(container_image "$polyedge_resource_group" "$polyedge_app" bot)"
polyedge_frontend_image="$(container_image "$polyedge_resource_group" "$polyedge_app" frontend)"

log "Preflight legacy health: https://${legacy_fqdn}/api/backend/health"
curl -fsS -o /tmp/polyedge-cutover-legacy-health.json "https://${legacy_fqdn}/api/backend/health" >/dev/null
log "Preflight PolyEdge standby health: https://${polyedge_fqdn}/api/backend/health"
curl -fsS -o /tmp/polyedge-cutover-standby-health.json "https://${polyedge_fqdn}/api/backend/health" >/dev/null

log "Disabling legacy writer on ${legacy_app}."
disable_writer "$legacy_resource_group" "$legacy_app" false
legacy_writer_disabled=true
log "Legacy writer disabled; waiting briefly for recorder shutdown flush."
sleep 20

log "Running final old-to-PolyEdge blob copy and exact source-snapshot verification."
legacy_key="$(az storage account keys list \
  --resource-group "$legacy_resource_group" \
  --account-name "$legacy_storage_account" \
  --query '[0].value' \
  -o tsv)"
polyedge_key="$(az storage account keys list \
  --resource-group "$polyedge_resource_group" \
  --account-name "$polyedge_storage_account" \
  --query '[0].value' \
  -o tsv)"

export SRC_ACCOUNT="$legacy_storage_account"
export DST_ACCOUNT="$polyedge_storage_account"
export CONTAINER="$storage_container"
export SRC_KEY="$legacy_key"
export DST_KEY="$polyedge_key"
export FINAL_COPY_SUMMARY="$summary_file"

"$python_bin" - <<'PY'
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions, generate_container_sas

src_account = os.environ["SRC_ACCOUNT"]
dst_account = os.environ["DST_ACCOUNT"]
container_name = os.environ["CONTAINER"]
src_key = os.environ["SRC_KEY"]
dst_key = os.environ["DST_KEY"]
summary_path = os.environ["FINAL_COPY_SUMMARY"]
prefixes = ["events/", "reports/", "config/", "control/"]
max_workers = int(os.environ.get("MAX_WORKERS", "12"))

src_service = BlobServiceClient(account_url=f"https://{src_account}.blob.core.windows.net", credential=src_key)
dst_service = BlobServiceClient(account_url=f"https://{dst_account}.blob.core.windows.net", credential=dst_key)
src_container = src_service.get_container_client(container_name)
dst_container = dst_service.get_container_client(container_name)
sas = generate_container_sas(
    account_name=src_account,
    container_name=container_name,
    account_key=src_key,
    permission=ContainerSasPermissions(read=True, list=True),
    expiry=datetime.now(timezone.utc) + timedelta(hours=6),
)


def source_url(name: str) -> str:
    return f"https://{src_account}.blob.core.windows.net/{container_name}/{quote(name, safe='/~')}?{sas}"


def list_map(prefix: str, client):
    return {blob.name: blob for blob in client.list_blobs(name_starts_with=prefix)}


def copy_one(item):
    name, src_size, dst_size = item
    dst_blob = dst_container.get_blob_client(name)
    if dst_size is not None:
        try:
            dst_blob.delete_blob(delete_snapshots="include")
        except ResourceNotFoundError:
            pass
    dst_blob.start_copy_from_url(source_url(name))
    deadline = time.monotonic() + 900
    while True:
        props = dst_blob.get_blob_properties()
        status = getattr(props.copy, "status", None) if props.copy is not None else None
        if status == "success":
            if props.size != src_size:
                raise RuntimeError(f"size mismatch after copy: {name}: src={src_size} dst={props.size}")
            return name, src_size, dst_size
        if status in {"failed", "aborted"}:
            description = getattr(props.copy, "status_description", "") if props.copy is not None else ""
            raise RuntimeError(f"copy {status}: {name}: {description}")
        if time.monotonic() > deadline:
            raise TimeoutError(f"copy timed out: {name}")
        time.sleep(0.5)


started_at = datetime.now(timezone.utc)
source_snapshot = {}
dest_snapshot = {}
prefix_rows = []
for prefix in prefixes:
    src_blobs = list_map(prefix, src_container)
    dst_blobs = list_map(prefix, dst_container)
    source_snapshot.update(src_blobs)
    dest_snapshot.update(dst_blobs)
    prefix_rows.append(
        {
            "prefix": prefix.rstrip("/"),
            "source_count": len(src_blobs),
            "destination_count_before": len(dst_blobs),
            "source_bytes": sum((blob.size or 0) for blob in src_blobs.values()),
            "destination_bytes_before": sum((blob.size or 0) for blob in dst_blobs.values()),
        }
    )

tasks = []
for name, src_blob in source_snapshot.items():
    dst_blob = dest_snapshot.get(name)
    dst_size = None if dst_blob is None else (dst_blob.size or 0)
    src_size = src_blob.size or 0
    if dst_size != src_size:
        tasks.append((name, src_size, dst_size))

print(
    f"final-copy: source_snapshot={len(source_snapshot)} destination_before={len(dest_snapshot)} to_copy={len(tasks)}",
    flush=True,
)
for item in tasks[:20]:
    old = "missing" if item[2] is None else f"old_size={item[2]}"
    print(f"  to_copy {item[0]} size={item[1]} {old}", flush=True)

copied = 0
copied_bytes = 0
if tasks:
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(copy_one, item) for item in tasks]
        for fut in as_completed(futures):
            name, src_size, dst_size = fut.result()
            copied += 1
            copied_bytes += src_size
            if copied <= 20 or copied % 100 == 0:
                old = "missing" if dst_size is None else f"old_size={dst_size}"
                print(f"  copied {copied}/{len(tasks)} {name} size={src_size} {old}", flush=True)

dest_after = {}
for prefix in prefixes:
    dest_after.update(list_map(prefix, dst_container))

missing = []
mismatch = []
for name, src_blob in source_snapshot.items():
    dst_blob = dest_after.get(name)
    src_size = src_blob.size or 0
    if dst_blob is None:
        missing.append(name)
    elif (dst_blob.size or 0) != src_size:
        mismatch.append({"name": name, "source_size": src_size, "destination_size": dst_blob.size or 0})

summary = {
    "started_at": started_at.isoformat(),
    "finished_at": datetime.now(timezone.utc).isoformat(),
    "source_account": src_account,
    "destination_account": dst_account,
    "container": container_name,
    "prefixes": prefix_rows,
    "source_snapshot_count": len(source_snapshot),
    "destination_after_count": len(dest_after),
    "source_snapshot_bytes": sum((blob.size or 0) for blob in source_snapshot.values()),
    "destination_after_bytes": sum((blob.size or 0) for blob in dest_after.values()),
    "copied_count": copied,
    "copied_bytes": copied_bytes,
    "missing_count": len(missing),
    "size_mismatch_count": len(mismatch),
    "missing": missing[:100],
    "size_mismatches": mismatch[:100],
}
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, indent=2, sort_keys=True)
    handle.write("\n")

print(
    f"final-copy: copied={copied} missing={len(missing)} size_mismatch={len(mismatch)} summary={summary_path}",
    flush=True,
)
if missing or mismatch:
    sys.exit(2)
PY

log "Deploying PolyEdge active writer with local frontend/backend proxy."
polyedge_activation_started=true
az deployment group create \
  --name "$polyedge_deployment_name" \
  --resource-group "$polyedge_resource_group" \
  --template-file infra/main.bicep \
  --parameters infra/parameters/polyedge-active.bicepparam \
  --parameters image="$polyedge_backend_image" frontendImage="$polyedge_frontend_image" apiBearerToken="$API_BEARER_TOKEN" \
  --only-show-errors \
  --output none
wait_containerapp_ready "$polyedge_resource_group" "$polyedge_app" 420

polyedge_run_on_startup="$(env_value "$polyedge_resource_group" "$polyedge_app" bot RUN_BOT_ON_STARTUP)"
polyedge_backend_base="$(env_value "$polyedge_resource_group" "$polyedge_app" frontend BACKEND_API_BASE_URL)"
if [ "$polyedge_run_on_startup" != "true" ]; then
  echo "PolyEdge RUN_BOT_ON_STARTUP is ${polyedge_run_on_startup}, expected true." >&2
  exit 1
fi
if [ "$polyedge_backend_base" != "http://127.0.0.1:8000/api/v1" ]; then
  echo "PolyEdge frontend backend base is ${polyedge_backend_base}, expected local backend." >&2
  exit 1
fi

log "Verifying PolyEdge active health and status."
curl -fsS -o /tmp/polyedge-cutover-active-health.json "https://${polyedge_fqdn}/api/backend/health" >/dev/null
deadline=$((SECONDS + 180))
while [ "$SECONDS" -lt "$deadline" ]; do
  if curl -fsS -o /tmp/polyedge-cutover-active-status.json "https://${polyedge_fqdn}/api/backend/status" >/dev/null; then
    if jq -e '.reference != null and (.recorder.recorders[]? | select(.type == "azure_storage") | .worker_alive == true)' /tmp/polyedge-cutover-active-status.json >/dev/null; then
      break
    fi
  fi
  sleep 5
done
if ! jq -e '.reference != null and (.recorder.recorders[]? | select(.type == "azure_storage") | .worker_alive == true)' /tmp/polyedge-cutover-active-status.json >/dev/null; then
  echo "PolyEdge writer did not publish a reference with a live Azure recorder before timeout." >&2
  exit 1
fi

log "Waiting for a new PolyEdge storage write after activation."
export ACTIVATION_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
"$python_bin" - <<'PY'
import os
import sys
import time
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient

account = os.environ["DST_ACCOUNT"]
container_name = os.environ["CONTAINER"]
key = os.environ["DST_KEY"]
activation_started = datetime.fromisoformat(os.environ["ACTIVATION_STARTED_AT"].replace("Z", "+00:00"))
client = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net", credential=key).get_container_client(container_name)

deadline = time.monotonic() + 120
latest = None
while time.monotonic() < deadline:
    latest = None
    for blob in client.list_blobs(name_starts_with="events/"):
        if latest is None or blob.last_modified > latest.last_modified:
            latest = blob
    if latest is not None and latest.last_modified >= activation_started:
        print(f"polyedge-write: {latest.name} last_modified={latest.last_modified.isoformat()} size={latest.size}")
        sys.exit(0)
    time.sleep(5)
if latest is not None:
    print(f"latest destination blob did not advance after activation: {latest.name} {latest.last_modified.isoformat()}", file=sys.stderr)
else:
    print("no destination event blobs found", file=sys.stderr)
sys.exit(2)
PY

polyedge_writer_verified=true

legacy_run_on_startup="$(env_value "$legacy_resource_group" "$legacy_app" bot RUN_BOT_ON_STARTUP)"
polyedge_revision="$(az containerapp show --resource-group "$polyedge_resource_group" --name "$polyedge_app" --query properties.latestRevisionName -o tsv)"
legacy_revision="$(az containerapp show --resource-group "$legacy_resource_group" --name "$legacy_app" --query properties.latestRevisionName -o tsv)"

jq -n \
  --arg cutover_id "$cutover_id" \
  --arg legacy_app "$legacy_app" \
  --arg legacy_revision "$legacy_revision" \
  --arg legacy_run_on_startup "$legacy_run_on_startup" \
  --arg polyedge_app "$polyedge_app" \
  --arg polyedge_revision "$polyedge_revision" \
  --arg polyedge_run_on_startup "$polyedge_run_on_startup" \
  --arg polyedge_fqdn "$polyedge_fqdn" \
  --arg final_copy_summary "$summary_file" \
  '{
    cutover_id: $cutover_id,
    legacy: {
      app: $legacy_app,
      latest_revision: $legacy_revision,
      run_bot_on_startup: $legacy_run_on_startup
    },
    polyedge: {
      app: $polyedge_app,
      latest_revision: $polyedge_revision,
      run_bot_on_startup: $polyedge_run_on_startup,
      fqdn: $polyedge_fqdn
    },
    final_copy_summary: $final_copy_summary
  }' > "$state_file"

log "Cutover complete. State: ${state_file}"
log "PolyEdge active endpoint: https://${polyedge_fqdn}/dashboard"
