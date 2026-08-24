import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  encodeAbiParameters,
  encodeEventTopics,
  verifyTypedData
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import {
  RELAYER_DEADLINE_BUFFER_SECONDS,
  assertStableRedemptionSelection,
  augmentRedeemableCandidatesFromRiskReservations,
  confirmedRedemptionControlMatches,
  commitCanonicalRecoveryJournal,
  discoverOnchainRedeemableConditions,
  expectedRecoveredAdapterApprovals,
  persistCanonicalRecoveryJournal,
  putCanonicalRecoveryJournal,
  recoveryEvidenceOwnershipAfterResume,
  rejectedRelayerSubmissionMatches,
  safeRelayerErrorDetail,
  shouldUploadRedemptionEvidence,
  validateCanonicalRecoveryJournal
} from "../src/redeem.mjs";
import {
  CONDITIONAL_TOKENS,
  CTF_COLLATERAL_ADAPTER,
  NEG_RISK_CTF_COLLATERAL_ADAPTER,
  buildRedemptionCalls,
  depositWalletRequest,
  depositWalletTypedData,
  deriveLegacyUupsDepositWallet,
  loadRedemptionConfig,
  selectRedeemableConditions,
  summarizeRecentRedemptions
} from "../src/redemption.mjs";

const owner = "0xc9f6f0D01e5eEf2446819Ce21C4f1F9b688A9921";
const funder = "0x3d701b05d7c36aFaB01a06Fd26eBe789c0B7baD8";
const conditionA = `0x${"11".repeat(32)}`;
const conditionB = `0x${"22".repeat(32)}`;

function recoveryJournalFixture() {
  const prefix = "reports/funded/dynamic-quote/sessions/campaign";
  const predecessorRunId = "venue-redemption-20260815001356008-deadbeef";
  const recoveryRunId = "venue-redemption-20260815001656008-acde1234";
  const journalBlobName = `${prefix}/recovery-controls/${predecessorRunId}.json`;
  const summaryBlobName = `${prefix}/redemptions/2026-08-15/${recoveryRunId}.json`;
  const transactionHash = `0x${"ab".repeat(32)}`;
  const settlementBlob = `${prefix}/internal-settlements/${"cd".repeat(32)}.json`;
  const summary = {
    schema_version: 1,
    run_id: recoveryRunId,
    status: "recovered_confirmed_and_verified",
    finished_ts: "2026-08-15T00:16:56.008Z",
    execution_origin: "azure_chile_central_static_egress",
    execution_country: "CL",
    static_egress_verified: true,
    funder,
    redemption_submitted: true,
    new_submission_attempted: false,
    confirmed_transaction_reused: true,
    zero_open_orders_confirmed: true,
    research_only: false,
    live_strategy_enabled: true,
    predecessor_run_id: predecessorRunId,
    recovery_journal_blob_name: journalBlobName,
    transaction_id: "fixture-transaction",
    transaction_hash: transactionHash,
    selection: {
      selected: [{ condition_id: conditionA, gross_payout: 2 }],
      selected_gross_payout: 2
    },
    realized_payout: 2,
    internal_settlement_blobs: [settlementBlob]
  };
  const summaryPayload = Buffer.from(JSON.stringify(summary, null, 2));
  const finalizedControl = {
    schema_version: 1,
    state: "confirmed_and_verified",
    run_id: predecessorRunId,
    owner: "[REDACTED]",
    signer_address: "[REDACTED]",
    funder,
    submission_attempted: true,
    transaction_id: "fixture-transaction",
    transaction_hash: transactionHash,
    condition_ids: [conditionA],
    expected_gross_payout: 2,
    recovery_run_id: recoveryRunId,
    recovery_journal_blob_name: journalBlobName,
    recovery_summary_blob_name: summaryBlobName,
    recovery_summary_sha256:
      `sha256:${createHash("sha256").update(summaryPayload).digest("hex")}`,
    recovery_publication_complete: true,
    realized_payout: 2,
    internal_settlement_blobs: [settlementBlob]
  };
  return {
    prefix,
    journalBlobName,
    summaryBlobName,
    summary,
    finalizedControl,
    journal: {
      schema_version: 1,
      type: "redemption_recovery_journal",
      predecessor_run_id: predecessorRunId,
      recovery_run_id: recoveryRunId,
      recovery_journal_blob_name: journalBlobName,
      finalized_control: finalizedControl,
      recovered_summary: summary
    }
  };
}

function memoryBlobContainer({
  ambiguousSuccessOnceAt = null,
  downloadErrorsRemaining = 0,
  failOnceAt = null
} = {}) {
  const values = new Map();
  let failed = false;
  let ambiguousSuccessFailed = false;
  let remainingDownloadErrors = downloadErrorsRemaining;
  const maybeFail = (label) => {
    if (!failed && failOnceAt === label) {
      failed = true;
      throw new Error(`injected ${label} failure`);
    }
  };
  return {
    values,
    getBlockBlobClient(blobName) {
      return {
        async uploadData(payload, options = {}) {
          const label = blobName.includes("/recovery-controls/")
            ? "journal"
            : blobName.includes("/redemptions/")
              ? "archive"
              : "latest";
          maybeFail(label);
          if (options.conditions?.ifNoneMatch === "*" && values.has(blobName)) {
            const error = new Error("already exists");
            error.statusCode = 412;
            throw error;
          }
          values.set(blobName, Buffer.from(payload));
          if (!ambiguousSuccessFailed && ambiguousSuccessOnceAt === label) {
            ambiguousSuccessFailed = true;
            const error = new Error(`injected ${label} timeout after durable write`);
            error.code = "ETIMEDOUT";
            throw error;
          }
        },
        async download() {
          if (remainingDownloadErrors > 0) {
            remainingDownloadErrors -= 1;
            const error = new Error("injected read-back failure");
            error.code = "ETIMEDOUT";
            throw error;
          }
          if (!values.has(blobName)) {
            const error = new Error("not found");
            error.statusCode = 404;
            throw error;
          }
          return {
            readableStreamBody: (async function* () {
              yield values.get(blobName);
            })()
          };
        }
      };
    },
    maybeFail
  };
}

test("canonical recovery journal is predecessor keyed and collision checked", async () => {
  const fixture = recoveryJournalFixture();
  const container = memoryBlobContainer();
  const created = await putCanonicalRecoveryJournal(
    container,
    fixture.prefix,
    fixture.journal
  );
  assert.equal(created.blob_name, fixture.journalBlobName);
  assert.equal(created.created, true);
  const originalBytes = Buffer.from(container.values.get(fixture.journalBlobName));
  const replay = await putCanonicalRecoveryJournal(
    container,
    fixture.prefix,
    fixture.journal
  );
  assert.equal(replay.created, false);
  assert.deepEqual(container.values.get(fixture.journalBlobName), originalBytes);
  await assert.rejects(
    putCanonicalRecoveryJournal(container, fixture.prefix, {
      ...fixture.journal,
      recovery_run_id: "venue-redemption-20260815001656008-ffffffff"
    }),
    /immutable recovery journal collision/
  );
});

test("ambiguous journal upload success is confirmed by exact read-back", async () => {
  const fixture = recoveryJournalFixture();
  const container = memoryBlobContainer({
    ambiguousSuccessOnceAt: "journal"
  });
  const persisted = await putCanonicalRecoveryJournal(
    container,
    fixture.prefix,
    fixture.journal
  );
  assert.equal(persisted.created, false);
  assert.equal(persisted.immutable_write_outcome, "accepted_owned");
  const durableBytes = Buffer.from(container.values.get(fixture.journalBlobName));
  const replay = await putCanonicalRecoveryJournal(
    container,
    fixture.prefix,
    fixture.journal
  );
  assert.equal(replay.created, false);
  assert.deepEqual(container.values.get(fixture.journalBlobName), durableBytes);
  await commitCanonicalRecoveryJournal(container, fixture.prefix,
    fixture.journal, { writeControl: async () => {} });
  assert.deepEqual(
    JSON.parse(container.values.get(fixture.summaryBlobName).toString("utf8")),
    fixture.summary
  );
});

test("ambiguous journal upload with indeterminate read-back owns evidence and resumes", async () => {
  const fixture = recoveryJournalFixture();
  const container = memoryBlobContainer({
    ambiguousSuccessOnceAt: "journal",
    downloadErrorsRemaining: 3
  });
  let ownsEvidence = false;
  await assert.rejects(
    persistCanonicalRecoveryJournal(
      container,
      fixture.prefix,
      fixture.journal,
      { claimEvidence: () => { ownsEvidence = true; } }
    ),
    /upload outcome is indeterminate after 3 read-back attempts/
  );
  assert.equal(ownsEvidence, true);
  assert.equal(
    shouldUploadRedemptionEvidence({ status: "failed_closed" }, ownsEvidence),
    false
  );
  const journalBytes = Buffer.from(container.values.get(fixture.journalBlobName));
  const restartedJournal = JSON.parse(journalBytes.toString("utf8"));

  let restartOwnsEvidence = false;
  const replay = await persistCanonicalRecoveryJournal(
    container,
    fixture.prefix,
    restartedJournal,
    { claimEvidence: () => { restartOwnsEvidence = true; } }
  );
  assert.equal(replay.created, false);
  assert.equal(restartOwnsEvidence, true);
  await commitCanonicalRecoveryJournal(
    container,
    fixture.prefix,
    restartedJournal,
    { writeControl: async () => {} }
  );
  assert.deepEqual(container.values.get(fixture.journalBlobName), journalBytes);
  assert.deepEqual(
    JSON.parse(container.values.get(fixture.summaryBlobName).toString("utf8")),
    fixture.summary
  );
});

test("known pre-write journal failure with reliable absence leaves evidence unowned", async () => {
  const fixture = recoveryJournalFixture();
  const container = memoryBlobContainer({ failOnceAt: "journal" });
  let ownsEvidence = false;
  await assert.rejects(
    persistCanonicalRecoveryJournal(
      container,
      fixture.prefix,
      fixture.journal,
      { claimEvidence: () => { ownsEvidence = true; } }
    ),
    /injected journal failure/
  );
  assert.equal(ownsEvidence, false);
  assert.equal(container.values.has(fixture.journalBlobName), false);
  assert.equal(
    shouldUploadRedemptionEvidence({ status: "failed_closed" }, ownsEvidence),
    true
  );
});

test("different immutable journal bytes claim the evidence lane and fail closed", async () => {
  const fixture = recoveryJournalFixture();
  const container = memoryBlobContainer();
  await putCanonicalRecoveryJournal(container, fixture.prefix, fixture.journal);
  let ownsEvidence = false;
  await assert.rejects(
    persistCanonicalRecoveryJournal(
      container,
      fixture.prefix,
      {
        ...fixture.journal,
        recovery_run_id: "venue-redemption-20260815001656008-ffffffff"
      },
      { claimEvidence: () => { ownsEvidence = true; } }
    ),
    /immutable recovery journal collision/
  );
  assert.equal(ownsEvidence, true);
  assert.equal(
    shouldUploadRedemptionEvidence({ status: "failed_closed" }, ownsEvidence),
    false
  );
});

test("canonical recovery validates predecessor transaction settlement and origin bindings", () => {
  const fixture = recoveryJournalFixture();
  const control = {
    ...fixture.finalizedControl,
    state: "confirmed_pending_verification"
  };
  assert.equal(validateCanonicalRecoveryJournal(fixture.journal, {
    account: owner,
    config: {
      funderAddress: funder,
      executionOrigin: "azure_chile_central_static_egress",
      expectedCountry: "CL"
    },
    control,
    journalBlobName: fixture.journalBlobName
  }), true);
  assert.throws(() => validateCanonicalRecoveryJournal({
    ...fixture.journal,
    recovered_summary: {
      ...fixture.summary,
      execution_country: "US"
    }
  }, {
    account: owner,
    config: {
      funderAddress: funder,
      executionOrigin: "azure_chile_central_static_egress",
      expectedCountry: "CL"
    },
    control,
    journalBlobName: fixture.journalBlobName
  }), /canonical recovery journal binding is invalid/);
});

test("every recovery publication write boundary resumes with exact journal bytes", async () => {
  for (const boundary of ["journal", "archive", "pending", "latest", "final"]) {
    const fixture = recoveryJournalFixture();
    const container = memoryBlobContainer({
      failOnceAt: ["journal", "archive", "latest"].includes(boundary)
        ? boundary
        : null
    });
    const controls = [];
    let controlFailureInjected = false;
    const writeControl = async (value) => {
      const label = value.state === "recovery_commit_pending_publication"
        ? "pending"
        : "final";
      if (!controlFailureInjected && boundary === label) {
        controlFailureInjected = true;
        throw new Error(`injected ${label} failure`);
      }
      controls.push(structuredClone(value));
    };
    if (boundary === "journal") {
      await assert.rejects(
        putCanonicalRecoveryJournal(container, fixture.prefix, fixture.journal),
        /injected journal failure/
      );
    }
    await putCanonicalRecoveryJournal(container, fixture.prefix, fixture.journal);
    const journalBytes = Buffer.from(container.values.get(fixture.journalBlobName));
    if (boundary !== "journal") {
      await assert.rejects(
        commitCanonicalRecoveryJournal(container, fixture.prefix,
          fixture.journal, { writeControl }),
        new RegExp(`injected ${boundary} failure`)
      );
    }
    await putCanonicalRecoveryJournal(container, fixture.prefix, fixture.journal);
    await commitCanonicalRecoveryJournal(container, fixture.prefix,
      fixture.journal, { writeControl });
    assert.deepEqual(container.values.get(fixture.journalBlobName), journalBytes);
    assert.deepEqual(
      JSON.parse(container.values.get(fixture.summaryBlobName).toString("utf8")),
      fixture.summary
    );
    assert.deepEqual(
      JSON.parse(container.values.get(`${fixture.prefix}/latest-redemption.json`)
        .toString("utf8")),
      fixture.summary
    );
    assert.deepEqual(controls.at(-1), fixture.finalizedControl);
  }
});

test("finalized recovery repairs missing archive and latest without a new run", async () => {
  const fixture = recoveryJournalFixture();
  const container = memoryBlobContainer();
  await putCanonicalRecoveryJournal(container, fixture.prefix, fixture.journal);
  const controls = [];
  const publication = await commitCanonicalRecoveryJournal(
    container,
    fixture.prefix,
    fixture.journal,
    {
      finalizedResume: true,
      writeControl: async (value) => controls.push(structuredClone(value))
    }
  );
  assert.equal(publication.repaired, true);
  assert.deepEqual(controls, [fixture.finalizedControl]);
  assert.ok(container.values.has(fixture.summaryBlobName));
  assert.ok(container.values.has(`${fixture.prefix}/latest-redemption.json`));
});

test("canonical recovery owns its archive even when publication fails", () => {
  assert.equal(shouldUploadRedemptionEvidence({ status: "failed_closed" }, true), false);
  assert.equal(shouldUploadRedemptionEvidence({ status: "failed_closed" }, false), true);
  assert.equal(shouldUploadRedemptionEvidence({
    status: "recovered_confirmed_and_verified"
  }, false), false);
});

test("no-op finalized resume releases normal evidence publication", () => {
  const ownership = recoveryEvidenceOwnershipAfterResume(null);
  assert.equal(ownership, false);
  assert.equal(shouldUploadRedemptionEvidence({
    status: "nothing_to_redeem"
  }, ownership), true);
  assert.equal(shouldUploadRedemptionEvidence({
    status: "failed_closed"
  }, ownership), true);
});

test("redemption uses the current Polymarket collateral adapters", () => {
  assert.equal(CTF_COLLATERAL_ADAPTER, "0xAdA100Db00Ca00073811820692005400218FcE1f");
  assert.equal(NEG_RISK_CTF_COLLATERAL_ADAPTER, "0xadA2005600Dec949baf300f4C6120000bDB6eAab");
});

test("redemption uses the documented ten-minute relayer deadline buffer", () => {
  assert.equal(RELAYER_DEADLINE_BUFFER_SECONDS, 600);
});

const safeEnv = {
  EXECUTION_MODE: "venue_redemption",
  ALLOW_LIVE: "false",
  ENABLE_TAKER_ORDERS: "false",
  VENUE_REDEMPTION_DRY_RUN: "true",
  POLYMARKET_SIGNATURE_TYPE: "3",
  POLYMARKET_PRIVATE_KEY: "key",
  POLYMARKET_FUNDER_ADDRESS: funder,
  POLYMARKET_API_KEY: "api",
  POLYMARKET_API_SECRET: "secret",
  POLYMARKET_API_PASSPHRASE: "pass",
  AZURE_STORAGE_ACCOUNT_NAME: "storage"
};

test("known email-login signer derives the funded legacy UUPS deposit wallet", () => {
  assert.equal(deriveLegacyUupsDepositWallet(owner), funder);
});

test("redemption defaults to disabled dry-run and requires separate relayer auth for live submission", () => {
  const config = loadRedemptionConfig(safeEnv);
  assert.equal(config.enabled, false);
  assert.equal(config.dryRun, true);
  assert.throws(() => loadRedemptionConfig({ ...safeEnv, VENUE_REDEMPTION_DRY_RUN: "false" }), /TRUST_BOUNDARY_READY.*ENABLED.*EXPECTED_COUNTRY.*EXPECTED_EGRESS_IP.*RELAYER_API_KEY/s);
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    VENUE_REDEMPTION_ENABLED: "true",
    VENUE_REDEMPTION_DRY_RUN: "false",
    VENUE_PROBE_EXPECTED_COUNTRY: "IE",
    VENUE_PROBE_EXPECTED_EGRESS_IP: "203.0.113.8",
    POLYMARKET_RELAYER_API_KEY: "relayer-key",
    POLYMARKET_RELAYER_API_KEY_ADDRESS: owner
  }), /FUNDED_EVIDENCE_TRUST_BOUNDARY_READY/);
  const live = loadRedemptionConfig({
    ...safeEnv,
    VENUE_REDEMPTION_ENABLED: "true",
    VENUE_REDEMPTION_DRY_RUN: "false",
    FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "true",
    VENUE_PROBE_EXPECTED_COUNTRY: "IE",
    VENUE_PROBE_EXPECTED_EGRESS_IP: "203.0.113.8",
    POLYMARKET_RELAYER_API_KEY: "relayer-key",
    POLYMARKET_RELAYER_API_KEY_ADDRESS: owner
  });
  assert.equal(live.enabled, true);
  assert.equal(live.dryRun, false);
});

test("redemption defaults to Tenderly Polygon RPC", () => {
  const config = loadRedemptionConfig(safeEnv);
  assert.equal(config.rpcUrl, "https://tenderly.rpc.polygon.community/");
  assert.deepEqual(config.rpcUrls, [config.rpcUrl]);
});

test("redemption accepts an explicit HTTPS Polygon RPC fallback", () => {
  assert.deepEqual(loadRedemptionConfig({
    ...safeEnv,
    POLYGON_RPC_URL: "https://primary.example/rpc",
    POLYGON_RPC_FALLBACK_URLS: " https://secondary.example/rpc "
  }).rpcUrls, ["https://primary.example/rpc", "https://secondary.example/rpc"]);
});

test("redemption deduplicates explicit Polygon RPC endpoints", () => {
  assert.deepEqual(loadRedemptionConfig({
    ...safeEnv,
    POLYGON_RPC_URL: "https://primary.example/rpc",
    POLYGON_RPC_FALLBACK_URLS: "https://secondary.example/rpc,https://primary.example/rpc"
  }).rpcUrls, ["https://primary.example/rpc", "https://secondary.example/rpc"]);
});

test("redemption rejects non-HTTPS Polygon RPC endpoints", () => {
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    POLYGON_RPC_FALLBACK_URLS: "http://not-secure.example"
  }), /POLYGON_RPC_URL and POLYGON_RPC_FALLBACK_URLS must contain HTTPS URLs/);
});

test("a rejected relayer submission is retryable only from exact immutable and onchain evidence", () => {
  const control = {
    state: "submission_attempted",
    run_id: "venue-redemption-rejected",
    transaction_id: null,
    condition_ids: [conditionA],
    expected_gross_payout: 11.12
  };
  const evidence = {
    run_id: control.run_id,
    status: "failed_closed",
    error: "relayer /submit returned HTTP 400",
    redemption_submitted: true
  };
  const selection = {
    selected: [{ condition_id: conditionA }],
    selected_gross_payout: 11.12
  };
  assert.equal(rejectedRelayerSubmissionMatches(control, evidence, selection), true);
  assert.equal(rejectedRelayerSubmissionMatches(
    control,
    { ...evidence, error: "relayer confirmation timed out" },
    selection
  ), false);
  assert.equal(rejectedRelayerSubmissionMatches(
    control,
    evidence,
    { ...selection, selected: [{ condition_id: conditionB }] }
  ), false);
  assert.equal(rejectedRelayerSubmissionMatches(
    control,
    evidence,
    { ...selection, selected_gross_payout: 11.11 }
  ), false);
});

test("only an exact confirmed redemption control can enter no-resubmit recovery", () => {
  const control = {
    schema_version: 1,
    state: "confirmed_pending_verification",
    run_id: "venue-redemption-20260731072438297-c7ba96ce",
    owner,
    signer_address: owner,
    funder,
    condition_ids: [conditionA],
    expected_gross_payout: 11.12,
    submission_attempted: true,
    transaction_id: "relayer-transaction-1",
    transaction_hash: `0x${"ab".repeat(32)}`,
    created_ts: "2026-07-31T07:24:38.297Z",
    updated_ts: "2026-07-31T07:26:59.000Z"
  };
  const binding = { owner, funder, maxPayout: 25, maxConditions: 1 };
  assert.equal(confirmedRedemptionControlMatches(control, binding), true);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    transaction_hash: null
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    condition_ids: [conditionA, conditionA]
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    condition_ids: [conditionA, conditionB]
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    expected_gross_payout: 25.01
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    expected_gross_payout: 28.69
  }, { ...binding, maxPayout: null }), true);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    expected_gross_payout: "Infinity"
  }, { ...binding, maxPayout: null }), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    funder: owner
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    signer_address: funder
  }, binding), false);
  const legacySanitized = {
    ...control,
    owner: "[REDACTED]"
  };
  delete legacySanitized.signer_address;
  assert.equal(
    confirmedRedemptionControlMatches(legacySanitized, binding),
    true
  );
  assert.equal(confirmedRedemptionControlMatches({
    ...legacySanitized,
    schema_version: 2
  }, binding), false);
});

test("confirmed recovery preserves preapproval or proves temporary grant and revoke", () => {
  const approvalEvent = [{
    type: "event",
    name: "ApprovalForAll",
    anonymous: false,
    inputs: [
      { name: "account", type: "address", indexed: true },
      { name: "operator", type: "address", indexed: true },
      { name: "approved", type: "bool", indexed: false }
    ]
  }];
  const log = (operator, approved) => ({
    address: CONDITIONAL_TOKENS,
    topics: encodeEventTopics({
      abi: approvalEvent,
      eventName: "ApprovalForAll",
      args: { account: funder, operator }
    }),
    data: encodeAbiParameters([{ type: "bool" }], [approved])
  });
  assert.deepEqual(
    expectedRecoveredAdapterApprovals(
      { logs: [] },
      funder,
      [CTF_COLLATERAL_ADAPTER]
    ),
    { [CTF_COLLATERAL_ADAPTER.toLowerCase()]: true }
  );
  assert.deepEqual(
    expectedRecoveredAdapterApprovals({
      logs: [
        log(CTF_COLLATERAL_ADAPTER, true),
        log(CTF_COLLATERAL_ADAPTER, false)
      ]
    }, funder, [CTF_COLLATERAL_ADAPTER]),
    { [CTF_COLLATERAL_ADAPTER.toLowerCase()]: false }
  );
  assert.throws(() => expectedRecoveredAdapterApprovals({
    logs: [log(CTF_COLLATERAL_ADAPTER, true)]
  }, funder, [CTF_COLLATERAL_ADAPTER]), /approval sequence is invalid/);
  assert.throws(() => expectedRecoveredAdapterApprovals({
    logs: [log(NEG_RISK_CTF_COLLATERAL_ADAPTER, true)]
  }, funder, [CTF_COLLATERAL_ADAPTER]), /unexpected adapter approval/);
});

test("relayer error details retain bounded diagnostics without secrets or signed payloads", () => {
  const secret = "relayer-secret-value";
  const detail = safeRelayerErrorDetail({
    message: `invalid batch ${secret} 0x${"ab".repeat(65)}`
  }, [secret]);
  assert.equal(detail, "message=invalid batch [redacted] [hex-redacted]");
  assert.equal(safeRelayerErrorDetail({ request: "must not be retained" }), "");
  assert.equal(safeRelayerErrorDetail("x".repeat(700)), "[token-redacted]");
});

test("funded-service redemption is bound to an approved static-egress protected-compounding session", () => {
  const session = {
    session_id: "dynamic-quote-funded-2026-07-29-v5",
    allow_compounding: true,
    no_deposits: true,
    allow_automatic_replenishment: false,
    target_order_notional: 10.5,
    max_order_notional: 10.5
  };
  const funded = loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_chile_central_static_egress",
    VENUE_REDEMPTION_MAX_PAYOUT: "25",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  });
  assert.equal(funded.fundedServiceManaged, true);
  assert.equal(funded.dustRedemptionEnabled, false);
  assert.equal(loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_DUST_REDEMPTION_ENABLED: "false"
  }).dustRedemptionEnabled, false);
  assert.equal(funded.executionOrigin, "azure_chile_central_static_egress");
  assert.equal(funded.maxOrderNotional, 10.5);
  assert.equal(funded.maxPayout, null);
  assert.equal(loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_DUST_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_chile_central_static_egress",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  }).dustRedemptionEnabled, true);
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_DUST_REDEMPTION_ENABLED: "true"
  }), /requires funded-service redemption/);
  assert.equal(loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    VENUE_PROBE_EXECUTION_ORIGIN: "oci_bogota_static_egress",
    VENUE_PROBE_EXPECTED_COUNTRY: "CO",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  }).executionOrigin, "oci_bogota_static_egress");
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: "other-session",
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_chile_central_static_egress",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  }), /must match the funded operator session/);
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_north_europe_static_egress",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  }), /approved static egress origin/);
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify({
      ...session,
      target_order_notional: 1
    }),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_chile_central_static_egress",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  }), /target\/max order notional/);
});

test("only positive redeemable condition payouts are selected once and within the cap", () => {
  const selection = selectRedeemableConditions([
    { conditionId: conditionA, redeemable: true, currentValue: 5, initialValue: 3, negativeRisk: false, title: "A", asset: "101", oppositeAsset: "102", outcomeIndex: 0 },
    { conditionId: conditionA, redeemable: true, currentValue: 0, initialValue: 1, negativeRisk: false, title: "A", asset: "102", oppositeAsset: "101", outcomeIndex: 1 },
    { conditionId: conditionB, redeemable: true, currentValue: 30, negativeRisk: true, title: "B" },
    { conditionId: `0x${"33".repeat(32)}`, redeemable: false, currentValue: 4, negativeRisk: false }
  ], 25, 5);
  assert.equal(selection.selected.length, 1);
  assert.equal(selection.selected[0].condition_id, conditionA);
  assert.equal(selection.selected_gross_payout, 5);
  assert.equal(selection.selected[0].principal, 4);
  assert.equal(selection.skipped_winner_conditions, 1);
  assert.deepEqual(selection.selected[0].assets, [
    { asset: "101", outcome_index: 0 },
    { asset: "102", outcome_index: 1 }
  ]);
});

test("funded redemption selects the full winning condition without a payout cap", () => {
  const selection = selectRedeemableConditions([
    { conditionId: conditionA, redeemable: true, currentValue: 28.69, initialValue: 10.0414, negativeRisk: false, title: "funded winner", asset: "101", oppositeAsset: "102", outcomeIndex: 0 },
    { conditionId: conditionB, redeemable: true, currentValue: 5, initialValue: 4, negativeRisk: false, title: "other winner", asset: "201", oppositeAsset: "202", outcomeIndex: 0 }
  ], null, 1);
  assert.equal(selection.selected.length, 1);
  assert.equal(selection.selected[0].condition_id, conditionA);
  assert.equal(selection.selected_gross_payout, 28.69);
  assert.equal(selection.skipped_winner_conditions, 1);
});

test("onchain discovery skips stale candidates and uses authoritative payout", async () => {
  const conditionC = `0x${"33".repeat(32)}`;
  const collections = new Map([
    [`${conditionA}:1`, "a1"],
    [`${conditionA}:2`, "a2"],
    [`${conditionB}:1`, "b1"],
    [`${conditionB}:2`, "b2"],
    [`${conditionC}:1`, "c1"],
    [`${conditionC}:2`, "c2"]
  ]);
  const assets = new Map([
    ["a1", 101n], ["a2", 102n],
    ["b1", 201n], ["b2", 202n],
    ["c1", 301n], ["c2", 302n]
  ]);
  const publicClient = {
    async readContract({ functionName, args }) {
      if (functionName === "getCollectionId") return collections.get(`${args[1]}:${args[2]}`);
      if (functionName === "getPositionId") return assets.get(args[1]);
      if (functionName === "payoutDenominator") return args[0] === conditionA ? 0n : 1n;
      if (functionName === "payoutNumerators") return args[1] === 0n ? 1n : 0n;
      if (functionName === "balanceOf") return args[1] === 301n ? 7_000_000n : 0n;
      throw new Error(`unexpected contract call: ${functionName}`);
    }
  };
  const positions = [
    { conditionId: conditionA, redeemable: true, currentValue: 5, negativeRisk: false, asset: "101", oppositeAsset: "102", outcomeIndex: 0 },
    { conditionId: conditionB, redeemable: true, currentValue: 999, negativeRisk: false, asset: "201", oppositeAsset: "202", outcomeIndex: 0 },
    { conditionId: conditionC, redeemable: true, currentValue: 0, negativeRisk: false, asset: "301", oppositeAsset: "302", outcomeIndex: 0 }
  ];

  const selection = await discoverOnchainRedeemableConditions(publicClient, positions, {
    funderAddress: funder,
    maxPayout: null,
    maxConditions: 1
  });

  assert.equal(selection.selected.length, 1);
  assert.equal(selection.selected[0].condition_id, conditionC);
  assert.equal(selection.selected[0].gross_payout, 7);
  assert.equal(selection.selected_gross_payout, 7);
  assert.equal(selection.payout_source, "onchain_balances_and_payout_vector");
});

test("dust candidate augmentation is explicit and remains bound to durable, Gamma, chain, and repeat evidence", async () => {
  const decisionId = "a".repeat(64);
  const probeId = `funded-direct-${decisionId}`;
  const orderId = `0x${"ab".repeat(32)}`;
  const record = {
    blob_name: `reports/research/venue-probe/risk-reservations/2026-08-24/${probeId}.json`,
    reservation_sha256: `sha256:${"c".repeat(64)}`,
    reservation: {
      schema_version: 1,
      evidence_protocol_version: 3,
      state: "position_unresolved",
      market_id: "3801022",
      condition_id: conditionA,
      token_id: "101",
      run_id: "funded-direct-20260824095256162-6ad533f2",
      probe_id: probeId,
      order_id: orderId,
      matched_notional: 0.01564794,
      reconciliation_complete: true,
      zero_open_orders_confirmed: true
    }
  };
  const market = {
    id: "3801022",
    conditionId: conditionA,
    closed: true,
    acceptingOrders: false,
    negRisk: false,
    question: "BTC Up or Down",
    clobTokenIds: JSON.stringify(["101", "102"]),
    outcomes: JSON.stringify(["Up", "Down"]),
    outcomePrices: JSON.stringify(["1", "0"])
  };
  let marketCalls = 0;
  const disabled = await augmentRedeemableCandidatesFromRiskReservations([], [record], {
    fetchMarket: async () => { marketCalls += 1; return market; }
  });
  assert.deepEqual(disabled, []);
  assert.equal(marketCalls, 0);

  const candidates = await augmentRedeemableCandidatesFromRiskReservations([], [record], {
    enabled: true,
    fetchMarket: async () => { marketCalls += 1; return market; }
  });
  assert.equal(candidates.length, 1);
  assert.equal(marketCalls, 1);
  const publicClient = {
    async readContract({ functionName, args }) {
      if (functionName === "getCollectionId") return args[2] === 1n ? "up" : "down";
      if (functionName === "getPositionId") return args[1] === "up" ? 101n : 102n;
      if (functionName === "payoutDenominator") return 1n;
      if (functionName === "payoutNumerators") return args[1] === 0n ? 1n : 0n;
      if (functionName === "balanceOf") return args[1] === 101n ? 20_000n : 0n;
      throw new Error(`unexpected contract call: ${functionName}`);
    }
  };
  const selection = await discoverOnchainRedeemableConditions(publicClient, candidates, {
    funderAddress: funder,
    maxPayout: null,
    maxConditions: 1
  });
  assert.equal(selection.selected[0].gross_payout, 0.02);
  assert.equal(selection.selected[0].candidate_source,
    "durable_unresolved_reservation_plus_resolved_gamma");
  assert.equal(selection.selected[0].risk_reservation_bindings[0].sha256,
    record.reservation_sha256);
  assert.doesNotThrow(() => assertStableRedemptionSelection(selection, structuredClone(selection)));
  const changed = structuredClone(selection);
  changed.selected[0].onchain_balances_base_units[0] = "0";
  assert.throws(() => assertStableRedemptionSelection(selection, changed), /changed after preflight/);

  for (const mutate of [
    (value) => { value.record.reservation_sha256 = "invalid"; },
    (value) => { value.market.closed = false; },
    (value) => { value.market.outcomePrices = JSON.stringify(["0", "1"]); },
    (value) => { value.market.clobTokenIds = JSON.stringify(["201", "202"]); }
  ]) {
    const value = { record: structuredClone(record), market: structuredClone(market) };
    mutate(value);
    await assert.rejects(augmentRedeemableCandidatesFromRiskReservations([], [value.record], {
      enabled: true,
      fetchMarket: async () => value.market
    }), /fail closed/);
  }
});

test("recent redemption activity is attributed only to a matching durable worker control record", () => {
  const transactionHash = `0x${"ab".repeat(32)}`;
  const rows = summarizeRecentRedemptions([
    { type: "TRADE", timestamp: 200, usdcSize: 99 },
    { type: "REDEEM", timestamp: 100, usdcSize: 5, transactionHash, conditionId: `0x${"cd".repeat(32)}`, title: "winner" }
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].gross_payout, 5);
  assert.equal(rows[0].attribution, "external_or_manual");
  assert.equal(rows[0].redeemed_ts, "1970-01-01T00:01:40.000Z");

  const controlled = summarizeRecentRedemptions([
    { type: "REDEEM", timestamp: 100, usdcSize: 5, transactionHash }
  ], { transaction_hash: transactionHash.toUpperCase().replace("0X", "0x") });
  assert.equal(controlled[0].attribution, "azure_redemption_worker");
});

test("call plan grants only official adapter approval then redeems standard and neg-risk conditions", () => {
  const selection = {
    selected: [
      { condition_id: conditionA, negative_risk: false },
      { condition_id: conditionB, negative_risk: true }
    ]
  };
  const calls = buildRedemptionCalls(selection, {
    [CTF_COLLATERAL_ADAPTER.toLowerCase()]: false,
    [NEG_RISK_CTF_COLLATERAL_ADAPTER.toLowerCase()]: true
  });
  assert.equal(calls.length, 4);
  assert.equal(calls[0].target, CONDITIONAL_TOKENS);
  assert.equal(calls[0].adapter, CTF_COLLATERAL_ADAPTER);
  assert.equal(calls[1].target, CTF_COLLATERAL_ADAPTER);
  assert.equal(calls[2].target, NEG_RISK_CTF_COLLATERAL_ADAPTER);
  assert.equal(calls[3].target, CONDITIONAL_TOKENS);
  assert.equal(calls[3].purpose, "revoke_official_collateral_adapter");
});

test("deposit wallet batch uses the documented EIP-712 domain and WALLET wire type", () => {
  const calls = buildRedemptionCalls({ selected: [{ condition_id: conditionA, negative_risk: false }] }, {
    [CTF_COLLATERAL_ADAPTER.toLowerCase()]: true
  });
  const typed = depositWalletTypedData(funder, "7", "123456", calls);
  assert.equal(typed.domain.name, "DepositWallet");
  assert.equal(typed.domain.version, "1");
  assert.equal(typed.domain.verifyingContract, funder);
  assert.equal(typed.primaryType, "Batch");
  const request = depositWalletRequest(owner, funder, "7", "123456", calls, "0xsignature");
  assert.equal(request.type, "WALLET");
  assert.equal(request.from, owner);
  assert.equal(request.depositWalletParams.depositWallet, funder);
  assert.equal(request.depositWalletParams.calls.length, 1);
});

test("documented deposit wallet batch produces a recoverable 65-byte owner signature", async () => {
  const account = privateKeyToAccount(`0x${"01".repeat(32)}`);
  const calls = buildRedemptionCalls({ selected: [{ condition_id: conditionA, negative_risk: false }] }, {
    [CTF_COLLATERAL_ADAPTER.toLowerCase()]: true
  });
  const typed = depositWalletTypedData(funder, "3", "2000000000", calls);
  const signature = await account.signTypedData(typed);
  assert.match(signature, /^0x[0-9a-f]{130}$/);
  assert.equal(await verifyTypedData({ address: account.address, ...typed, signature }), true);
});
