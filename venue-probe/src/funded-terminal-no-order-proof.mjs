import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { Readable } from "node:stream";
import { pathToFileURL } from "node:url";
import {
  assertExistingAuthorizationBinding,
  authorizationWasConsumed,
  loadTerminalNoExposureReservation
} from "./funded-direct-worker.mjs";
import { storageContainer } from "./lib.mjs";

// One missing rollout record, not a general permission to waive prior evidence.
export const RECOVERY_IMAGE = "ghcr.io/aldoapicella/polyedge-venue-probe@sha256:cf701ac5ebdf1a66c10ed52feab9fbca3dfb6eb7937e2501c5a41f812a29f28f";
const hash = bytes => createHash("sha256").update(bytes).digest("hex");
const epoch = value => { const ms = Date.parse(value); assert(Number.isFinite(ms)); return ms; };
const control = "polyedge-funded-evidence";

export async function verifyTerminalNoOrderProof({ bundle, runtime, journal }, now = new Date()) {
  assert.equal(bundle.schema, "polyedge.terminal_no_order_source_bundle.v1");
  assert.equal(bundle.read_only, true);
  assert.equal(runtime.image, RECOVERY_IMAGE);
  assert.equal(runtime.revision, "4208d541f193c85bd121692bdff46b8898f2c2fc");
  assert.match(runtime.invocationId, /^[0-9a-f]{32}$/);
  assert.match(runtime.containerId, /^[0-9a-f]{64}$/);
  const docs = {}, byPath = new Map();
  assert.deepEqual(Object.keys(bundle.documents).sort(),
    ["authorization", "consumption", "intent", "legacy_completion", "redemption", "reservation", "settlement"]);
  for (const [key, row] of Object.entries(bundle.documents)) {
    assert.equal(row.container, key === "intent" ? "polyedge-shadow-events" : control);
    assert.match(row.etag, /^"?0x[0-9a-f]+"?$/i);
    assert.match(row.sha256, /^[0-9a-f]{64}$/);
    assert.match(row.path, /^reports\/[a-zA-Z0-9_./-]+\.json$/);
    assert(!row.path.split("/").includes(".."));
    const bytes = Buffer.from(row.bytes_base64, "base64");
    assert(bytes.length > 0 && bytes.length <= 1048576);
    assert.equal(hash(bytes), row.sha256, `${key} source hash`);
    const doc = { value: JSON.parse(bytes), blobName: row.path, hash: `sha256:${row.sha256}` };
    docs[key] = doc;
    assert(!byPath.has(row.path));
    byPath.set(row.path, bytes);
  }
  // Reuse the worker's exact authorization, consumption and terminal predicate.
  const container = { getBlobClient: name => ({
    exists: async () => byPath.has(name),
    download: async () => { assert(byPath.has(name)); return { readableStreamBody: Readable.from([byPath.get(name)]) }; }
  }) };
  const i = docs.intent.value, a = docs.authorization.value, c = docs.legacy_completion.value;
  assert.equal(i.schema, "polyedge.execution_intent.v1");
  assert.match(i.decision_id, /^[0-9a-f]{64}$/);
  assert.equal(a.session_id, "dynamic-quote-funded-2026-09-16-v11");
  const sessionRoot = `reports/funded/dynamic-quote/sessions/${a.session_id}`;
  assert.equal(docs.intent.blobName, `reports/research/venue-probe/control/strategy-canary/intents/${i.decision_id}.json`);
  assertExistingAuthorizationBinding(docs.authorization, {
    controlPrefix: "reports/funded/dynamic-quote", candidate: i.candidate_name,
    candidateVersion: i.candidate_version, candidateConfigHash: i.candidate_config_hash
  }, { session_id: a.session_id }, docs.intent);
  assert.equal(await authorizationWasConsumed(container, docs.authorization, docs.intent), true);
  assert.equal(docs.legacy_completion.blobName, `${sessionRoot}/completed/${i.decision_id}.json`);
  assert.equal(c.schema, "polyedge.operator_funded_intent_completion.v1");
  assert.equal(c.session_id, a.session_id);
  assert.equal(c.decision_id, i.decision_id);
  assert.equal(c.child_run_id, a.child_run_id);
  assert.equal(c.authorization_blob_name, docs.authorization.blobName);
  assert.equal(c.authorization_sha256, docs.authorization.hash);
  assert.equal(c.status, "child_failed_closed_post_submission_unresolved");
  assert.equal(c.order_id, null);
  assert.equal(c.matched_notional, 0);
  assert.equal(c.reconciliation_complete, false);
  assert.equal(c.zero_open_orders_confirmed, false);
  assert.match(c.post_submission_error, /^fail closed: venue rejected the order without acknowledgement; no-order risk was reconciled and released \(post_only_crosses_book:/);
  for (const key of ["order_submission_attempted", "authorization_consumed", "risk_reservation_created"]) assert.equal(c[key], true);
  const reservation = await loadTerminalNoExposureReservation(container, docs.intent, docs.authorization, now);
  assert(reservation, "no exact terminal reservation");
  assert.equal(reservation.state, "released_no_order");
  assert.equal(reservation.reconciliation_reason, "post_only_crosses_book");
  assert.equal(reservation.campaign_id, a.session_id);
  assert(epoch(i.decision_ts) <= epoch(a.authorized_at));
  assert(epoch(a.authorized_at) <= epoch(docs.consumption.value.consumed_at));
  assert(epoch(docs.consumption.value.consumed_at) <= epoch(reservation.created_ts));
  assert(epoch(reservation.created_ts) <= epoch(reservation.updated_ts));
  assert(epoch(reservation.updated_ts) <= epoch(c.completed_at));
  assert(epoch(c.completed_at) <= now.getTime());

  const r = docs.redemption.value, s = docs.settlement.value;
  assert.equal(r.schema_version, 1);
  assert.equal(r.status, "redeemed_and_verified");
  assert.equal(r.dry_run, false);
  assert.equal(r.redemption_submitted, true);
  assert.equal(r.zero_open_orders_confirmed, true);
  assert.equal(s.schema, "polyedge.verified_internal_settlement.v1");
  assert.equal(s.session_id, a.session_id);
  assert.equal(s.transaction_hash, r.transaction_hash);
  assert.match(r.transaction_hash, /^0x[0-9a-f]{64}$/);
  assert(Number.isInteger(s.receipt_confirmations) && s.receipt_confirmations >= 2);
  assert.equal(s.redemption_transfer_chain_verified, true);
  assert.equal(s.redemption_evidence_decoded, true);
  assert.equal(s.payout, r.realized_payout);
  assert(typeof s.payout === "number" && Number.isFinite(s.payout) && s.payout > 0);
  assert.deepEqual(r.internal_settlement_blobs, [docs.settlement.blobName]);
  assert(docs.redemption.blobName.startsWith(`${sessionRoot}/redemptions/`));
  assert(docs.settlement.blobName.startsWith(`${sessionRoot}/internal-settlements/`));
  assert.equal(r.selection.selected.length, 1);
  assert.equal(r.selection.selected[0].condition_id, s.condition_id);
  assert.equal(r.selection.selected_gross_payout, s.payout);
  assert(epoch(r.finished_ts) <= epoch(i.decision_ts));

  const events = [];
  let pending = "";
  for (const line of journal.trim().split("\n")) {
    const row = JSON.parse(line);
    assert.equal(row._SYSTEMD_INVOCATION_ID, runtime.invocationId);
    assert.equal(row.CONTAINER_ID_FULL, runtime.containerId);
    const message = row.MESSAGE;
    assert.equal(typeof message, "string", "complete journal MESSAGE required (journalctl --all)");
    const text = pending ? pending + message : message;
    let event;
    try { event = JSON.parse(text); pending = ""; }
    catch {
      if (pending || message.startsWith('{"schema":"polyedge.funded_redemption_service.v1","status":"redemption_worker_summary"')) pending = text;
      continue;
    }
    if (!event || typeof event !== "object" || Array.isArray(event)) continue;
    const ms = Number(row.__REALTIME_TIMESTAMP) / 1000;
    assert(Number.isFinite(ms) && ms <= now.getTime());
    events.push({ ms, event });
  }
  assert.equal(pending, "", "incomplete redemption journal record");
  assert(events.some(({ ms, event: e }) => ms >= epoch(r.finished_ts) && ms - epoch(r.finished_ts) <= 60000 && ms < epoch(i.decision_ts) &&
    e.schema === "polyedge.funded_redemption_service.v1" &&
    e.status === "redemption_worker_summary" && e.redemption?.status === r.status &&
    e.redemption?.run_id === r.run_id && e.redemption?.transaction_hash === r.transaction_hash &&
    e.redemption?.finished_ts === r.finished_ts && e.redemption?.selection?.selected_gross_payout === s.payout &&
    JSON.stringify(e.redemption?.internal_settlement_blobs) === JSON.stringify(r.internal_settlement_blobs)),
  "redemption is not bound to this runtime");
  const alerts = events.filter(({ event: e }) => e.schema === "polyedge.funded_direct_alert.v1" ||
    (e.schema === "polyedge.funded_direct_service.v1" && e.status === "failed_closed") ||
    (e.schema === "polyedge.funded_direct_service.v2" && e.status === "persistent_message_failed_closed"));
  alerts.sort((a, b) => a.ms - b.ms);
  const last = alerts.at(-1);
  assert.equal(last?.event.schema, "polyedge.funded_direct_alert.v1");
  assert.equal(last.event.status, "paused_by_account_risk_state");
  assert.equal(last.event.decision_id, i.decision_id);
  assert.equal(last.event.account_risk_pause, true);
  assert.equal(last.event.error, c.post_submission_error);
  assert(last.ms >= epoch(c.completed_at) && last.ms - epoch(c.completed_at) <= 60000);
  return { schema: "polyedge.terminal_no_order_proof.v1", status: "verified_terminal_no_order",
    decisionId: i.decision_id, childRunId: a.child_run_id, reason: reservation.reconciliation_reason,
    cleanSinceEpoch: last.ms / 1000, runtime, sourceHashes: Object.fromEntries(Object.entries(docs).map(([k, d]) => [k, d.hash])),
    redemption: { runId: r.run_id, transactionHash: r.transaction_hash, grossPayout: s.payout,
      settlementBlob: docs.settlement.blobName, summarySha256: docs.redemption.hash, settlementSha256: docs.settlement.hash },
    originalRolloutReceiptPresent: false, readOnly: true };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  assert.deepEqual(process.argv.slice(2), ["--stdin"]);
  assert(!process.env.AZURE_STORAGE_ACCOUNT_KEY, "storage keys forbidden");
  assert.equal(process.env.AZURE_STORAGE_ACCOUNT_NAME, "stpolyedge6urdjr5nmwx7w");
  const bytes = readFileSync(0), input = JSON.parse(bytes);
  const bundleBytes = Buffer.from(input.bundleBytesBase64, "base64");
  assert.deepEqual(JSON.parse(bundleBytes), input.bundle);
  await verifyTerminalNoOrderProof(input);
  for (const row of Object.values(input.bundle.documents)) {
    const container = storageContainer({ storageAccount: process.env.AZURE_STORAGE_ACCOUNT_NAME,
      storageContainer: row.container, azureClientId: process.env.AZURE_CLIENT_ID });
    const result = await container.getBlobClient(row.path).download(0, undefined, { conditions: { ifMatch: row.etag } });
    const parts = []; for await (const part of result.readableStreamBody) parts.push(part);
    assert.equal(hash(Buffer.concat(parts)), row.sha256, "authenticated source changed");
  }
  const proof = await verifyTerminalNoOrderProof(input);
  console.log(JSON.stringify({ ...proof, sourceBundleSha256: `sha256:${hash(bundleBytes)}`,
    journalSha256: `sha256:${hash(input.journal)}`, inputSha256: `sha256:${hash(bytes)}`,
    sources: Object.fromEntries(Object.entries(input.bundle.documents).map(([key, { container, path, etag, sha256 }]) => [key, { container, path, etag, sha256 }])),
    authenticatedSourcesVerified: true, verifiedAtUtc: new Date().toISOString() }));
}
