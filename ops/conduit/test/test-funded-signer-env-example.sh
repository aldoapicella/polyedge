#!/bin/bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$root"

node --input-type=module <<'NODE'
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

const envText = readFileSync("ops/conduit/env/funded-signer.env.example", "utf8");
const env = Object.fromEntries(envText
  .split("\n")
  .filter((line) => line && !line.startsWith("#"))
  .map((line) => [line.slice(0, line.indexOf("=")), line.slice(line.indexOf("=") + 1)]));
const session = JSON.parse(env.FUNDED_DIRECT_SESSION_MANIFEST_JSON);
const source = JSON.parse(readFileSync("research/configs/funded_direct_dynamic_quote_2026-08-13_v10.json", "utf8"));
assert.deepEqual(session, source);
assert.equal(`sha256:${createHash("sha256").update(JSON.stringify(session, null, 2)).digest("hex")}`, env.FUNDED_DIRECT_SESSION_MANIFEST_SHA256);
const expected = {
  FUNDED_DIRECT_SERVICE_ENABLED: "false",
  FUNDED_DIRECT_ENGINE: "persistent_v1",
  FUNDED_DIRECT_WORKER_ENABLED: "false",
  ALLOW_FUNDED_DIRECT: "false",
  FUNDED_DIRECT_DRY_RUN: "true",
  FUNDED_DIRECT_PREFLIGHT_ONLY: "false",
  FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "false",
  ALLOW_LIVE: "false",
  ALLOW_STRATEGY_CANARY: "false",
  ENABLE_TAKER_ORDERS: "false",
  FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "false",
  VENUE_PROBE_EXECUTION_ORIGIN: "oci_bogota_static_egress",
  VENUE_PROBE_EXPECTED_COUNTRY: "CO",
  VENUE_PROBE_EXPECTED_EGRESS_IP: "149.130.186.60",
  AZURE_TENANT_ID: "9767f0dc-e83f-4cc1-94e1-0d5f9d287d32",
  AZURE_CLIENT_ID: "d9ce9154-66a6-4bdb-839f-0da7b02b38da",
  AZURE_TOKEN_CREDENTIALS: "WorkloadIdentityCredential",
  AZURE_FEDERATED_TOKEN_FILE: "/run/credentials/azure-federated-token",
  AZURE_STORAGE_ACCOUNT_NAME: "stpolyedge6urdjr5nmwx7w",
  AZURE_STORAGE_CONTAINER_NAME: "polyedge-funded-evidence",
  FUNDED_DIRECT_SERVICE_BUS_NAMESPACE: "",
  FUNDED_DIRECT_SERVICE_BUS_QUEUE: "",
  FUNDED_DIRECT_OCI_QUEUE_BRIDGE_URL: "http://10.89.0.1:8182/v1/messages",
  FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS: "7000",
  FUNDED_DIRECT_SERVICE_RESTART_DELAY_MS: "1000",
  FUNDED_DIRECT_SERVICE_RISK_PAUSE_MS: "60000",
  FUNDED_DIRECT_SERVICE_HEARTBEAT_MS: "60000",
  FUNDED_DIRECT_SERVICE_MAX_CYCLES: "0",
  FUNDED_DIRECT_AUTO_REDEMPTION_INTERVAL_MS: "60000",
  FUNDED_DIRECT_AUTO_REDEMPTION_MIN_SECONDS_TO_EXPIRY: "30",
  FUNDED_DIRECT_AUTO_REDEMPTION_MAX_SECONDS_TO_EXPIRY: "300",
  FUNDED_DIRECT_SESSION_MANIFEST_BLOB_NAME: "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/session.json",
  FUNDED_DIRECT_SESSION_MANIFEST_SHA256: "sha256:c516c052fc5c01eed5403842ff24bf4b08512d6c38e8260e2d064580c82322f8",
  FUNDED_DIRECT_CONTROL_PREFIX: "reports/funded/dynamic-quote",
  FUNDED_DIRECT_MAX_ITERATIONS: "2000",
  FUNDED_DIRECT_POLL_INTERVAL_MS: "1000",
  FUNDED_DIRECT_MAX_IDLE_MS: "3600000",
  FUNDED_DIRECT_MIN_REMAINING_TTL_MS: "7000",
  FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS: "2000",
  STRATEGY_CANARY_INTENT_PREFIX: "reports/research/venue-probe/control/strategy-canary/intents",
  STRATEGY_CANARY_INTENT_CONTAINER_NAME: "polyedge-shadow-events",
  STRATEGY_CANARY_MANIFEST_CONTAINER_NAME: "polyedge-funded-evidence",
  STRATEGY_CANARY_CANDIDATE_NAME: "dynamic_quote_style",
  STRATEGY_CANARY_CANDIDATE_VERSION: "dynamic_quote_style@2026-06-14",
  STRATEGY_CANARY_CANDIDATE_CONFIG_HASH: "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c",
  STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION: "conservative-execution-prior-v1",
  STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE: "chainlink_reference",
  STRATEGY_INTENT_TARGET_ORDER_NOTIONAL: "10.5",
  STRATEGY_CANARY_MAX_ORDER_NOTIONAL: "10.5",
  STRATEGY_INTENT_MIN_SECONDS_TO_EXPIRY: "360",
  STRATEGY_INTENT_MAX_SECONDS_TO_EXPIRY: "900",
  STRATEGY_CANARY_MAX_REFERENCE_AGE_MS: "2000",
  STRATEGY_CANARY_MAX_BOOK_AGE_MS: "1000",
  STRATEGY_CANARY_REST_SECONDS: "30",
  MAX_OPEN_ORDERS: "1",
  VENUE_PROBE_FUNDED_CAMPAIGN_ID: "dynamic-quote-funded-2026-08-13-v10",
  VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY: "29.505501",
  VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR: "0",
  VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN: "29.505501",
  VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY: "0.01",
  VENUE_PROBE_CAMPAIGN_CASH_FLOWS: "[]",
  VENUE_PROBE_MAX_CLOCK_DRIFT_MS: "5000",
  VENUE_PROBE_MAX_CLOCK_UNCERTAINTY_MS: "750",
  POLYMARKET_FUNDER_ADDRESS: "0x3d701b05d7c36aFaB01a06Fd26eBe789c0B7baD8",
  POLYMARKET_SIGNATURE_TYPE: "3",
  POLYMARKET_RELAYER_API_KEY_ADDRESS: "0xc9f6f0D01e5eEf2446819Ce21C4f1F9b688A9921",
  VENUE_REDEMPTION_MAX_CONDITIONS: "1"
};
const declared = envText
  .split("\n")
  .filter((line) => line && !line.startsWith("#"))
  .map((line) => line.slice(0, line.indexOf("=")));
assert.deepEqual([...declared].sort(), [...new Set(declared)].sort());
assert.deepEqual([...declared].sort(), [...Object.keys(expected), "FUNDED_DIRECT_SESSION_MANIFEST_JSON"].sort());
const assertExactBindings = (env) => {
  for (const [name, value] of Object.entries(expected)) assert.equal(env[name], value, name);
};
assertExactBindings(env);
for (const name of Object.keys(expected)) {
  assert.throws(() => assertExactBindings({ ...env, [name]: "wrong" }), { name: "AssertionError" });
}
NODE

printf 'test-funded-signer-env-example: ok\n'
