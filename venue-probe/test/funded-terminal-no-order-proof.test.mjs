import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import { RECOVERY_IMAGE, verifyTerminalNoOrderProof } from "../src/funded-terminal-no-order-proof.mjs";

const hash = bytes => createHash("sha256").update(bytes).digest("hex");
const session = "dynamic-quote-funded-2026-09-16-v11", decision = "a".repeat(64);
const root = `reports/funded/dynamic-quote/sessions/${session}`;
const at = seconds => `2026-09-16T23:05:${String(seconds).padStart(2, "0")}.000Z`;
const now = new Date("2026-09-17T01:00:00Z");
const error = "fail closed: venue rejected the order without acknowledgement; no-order risk was reconciled and released (post_only_crosses_book: invalid post-only order: order crosses book)";
function fixture() {
  const bundle = { schema: "polyedge.terminal_no_order_source_bundle.v1", read_only: true, documents: {} };
  const put = (key, path, value) => {
    const bytes = Buffer.from(JSON.stringify(value));
    return bundle.documents[key] = { container: key === "intent" ? "polyedge-shadow-events" : "polyedge-funded-evidence",
      path, sha256: hash(bytes), etag: '"0xABC"', bytes_base64: bytes.toString("base64") };
  };
  const intent = { schema: "polyedge.execution_intent.v1", decision_id: decision, decision_ts: at(45),
    candidate_name: "dynamic_quote_style", candidate_version: "v1", candidate_config_hash: `sha256:${"b".repeat(64)}`,
    market_id: "1", condition_id: `0x${"c".repeat(64)}`, token_id: "2" };
  const i = put("intent", `reports/research/venue-probe/control/strategy-canary/intents/${decision}.json`, intent);
  const auth = { schema: "polyedge.operator_funded_intent_authorization.v1", authorization_id: "auth-1", session_id: session,
    decision_id: decision, child_run_id: "child-1", intent_blob_name: i.path, intent_sha256: `sha256:${i.sha256}`,
    candidate_name: intent.candidate_name, candidate_version: intent.candidate_version, candidate_config_hash: intent.candidate_config_hash,
    single_use: true, authorized_at: at(46), expires_at: at(59) };
  const a = put("authorization", `${root}/authorizations/${decision}.json`, auth);
  put("consumption", "reports/research/venue-probe/control/strategy-canary/consumed/auth-1.json", {
    schema: "polyedge.strategy_canary_authorization_consumption.v1", authorization_id: auth.authorization_id,
    authorization_sha256: `sha256:${a.sha256}`, decision_id: decision, run_id: auth.child_run_id, consumed_at: at(47) });
  put("reservation", `reports/research/venue-probe/risk-reservations/2026-09-16/funded-direct-${decision}.json`, {
    schema_version: 1, evidence_protocol_version: 3, campaign_id: session, probe_id: `funded-direct-${decision}`, run_id: auth.child_run_id,
    market_id: intent.market_id, condition_id: intent.condition_id, token_id: intent.token_id,
    order_submission_intended: true, order_submitted: false, order_id: null, matched_notional: 0,
    state: "released_no_order", reconciliation_reason: "post_only_crosses_book", reconciliation_complete: true, zero_open_orders_confirmed: true,
    created_ts: at(48), updated_ts: at(51), reconciliation_evidence: { source: "authenticated_clob_and_user_channel",
      zero_open_orders: true, zero_unresolved_positions: true, post_send_authenticated_trade_count: 0 } });
  put("legacy_completion", `${root}/completed/${decision}.json`, {
    schema: "polyedge.operator_funded_intent_completion.v1", session_id: session, decision_id: decision, child_run_id: auth.child_run_id,
    authorization_blob_name: a.path, authorization_sha256: `sha256:${a.sha256}`, status: "child_failed_closed_post_submission_unresolved",
    order_submission_attempted: true, authorization_consumed: true, risk_reservation_created: true, completed_at: at(52),
    order_id: null, matched_notional: 0, reconciliation_complete: false, zero_open_orders_confirmed: false, post_submission_error: error });
  const settlementPath = `${root}/internal-settlements/example.json`;
  const redemption = { schema_version: 1, status: "redeemed_and_verified", dry_run: false, redemption_submitted: true,
    zero_open_orders_confirmed: true, run_id: "redemption-1", transaction_hash: `0x${"d".repeat(64)}`, realized_payout: 5,
    finished_ts: at(0), internal_settlement_blobs: [settlementPath], selection: { selected_gross_payout: 5, selected: [{ condition_id: intent.condition_id }] } };
  put("redemption", `${root}/redemptions/2026-09-16/redemption-1.json`, redemption);
  put("settlement", settlementPath, { schema: "polyedge.verified_internal_settlement.v1", session_id: session,
    transaction_hash: redemption.transaction_hash, condition_id: intent.condition_id, receipt_confirmations: 2,
    redemption_transfer_chain_verified: true, redemption_evidence_decoded: true, payout: 5 });
  const runtime = { image: RECOVERY_IMAGE, revision: "4208d541f193c85bd121692bdff46b8898f2c2fc", invocationId: "1".repeat(32), containerId: "2".repeat(64) };
  const row = (event, seconds) => JSON.stringify({ MESSAGE: JSON.stringify(event), __REALTIME_TIMESTAMP: String(Date.parse(at(seconds)) * 1000),
    _SYSTEMD_INVOCATION_ID: runtime.invocationId, CONTAINER_ID_FULL: runtime.containerId });
  const journal = [row({ schema: "polyedge.funded_redemption_service.v1", status: "redemption_worker_summary", redemption }, 1),
    row({ schema: "polyedge.funded_direct_alert.v1", status: "paused_by_account_risk_state", decision_id: decision, account_risk_pause: true, error }, 53)].join("\n");
  return { input: { bundle, runtime, journal }, row };
}
function change(input, key, mutate) {
  const row = input.bundle.documents[key], value = JSON.parse(Buffer.from(row.bytes_base64, "base64"));
  mutate(value); const bytes = Buffer.from(JSON.stringify(value)); row.sha256 = hash(bytes); row.bytes_base64 = bytes.toString("base64");
}
test("one exact authenticated-source candidate, terminal reservation and latest runtime alert are required", async () => {
  assert.equal((await verifyTerminalNoOrderProof(fixture().input, now)).status, "verified_terminal_no_order");
  for (const mutate of [
    x => { x.runtime.image = "different"; },
    x => { x.bundle.documents.intent.sha256 = "0".repeat(64); },
    x => { x.bundle.documents.intent.container = "polyedge-shadow-qset-v8"; },
    x => change(x, "consumption", d => { d.authorization_sha256 = `sha256:${"0".repeat(64)}`; }),
    x => change(x, "reservation", d => { d.matched_notional = 1; }),
    x => change(x, "reservation", d => { d.evidence_protocol_version = 2; }),
    x => change(x, "reservation", d => { d.reconciliation_evidence.zero_unresolved_positions = false; }),
    x => change(x, "legacy_completion", d => { d.order_id = `0x${"0".repeat(64)}`; }),
    x => change(x, "legacy_completion", d => { d.post_submission_error = "unknown failure"; }),
    x => change(x, "legacy_completion", d => { d.post_submission_error += "changed"; }),
    x => change(x, "redemption", d => { d.finished_ts = at(55); }),
    x => change(x, "settlement", d => { d.redemption_transfer_chain_verified = false; }),
    x => { x.journal = x.journal.split("\n").slice(1).join("\n"); },
    x => { x.journal = x.journal.replace('"_SYSTEMD_INVOCATION_ID":"111', '"_SYSTEMD_INVOCATION_ID":"000'); },
    x => { const lines = x.journal.split("\n"), row = JSON.parse(lines[0]); row.MESSAGE = null; lines[0] = JSON.stringify(row); x.journal = lines.join("\n"); }
  ]) { const { input } = fixture(); mutate(input); await assert.rejects(verifyTerminalNoOrderProof(input, now)); }
  const { input, row } = fixture();
  input.journal += `\n${row({ schema: "polyedge.funded_direct_alert.v1", status: "paused_by_account_risk_state", decision_id: "f".repeat(64), account_risk_pause: true, error }, 54)}`;
  await assert.rejects(verifyTerminalNoOrderProof(input, now));
});
