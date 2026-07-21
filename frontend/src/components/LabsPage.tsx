"use client";

import { useQuery } from "@tanstack/react-query";
import { Beaker, RefreshCw } from "lucide-react";
import type { ReactElement } from "react";
import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import {
  getLabArtifacts,
  getLabArtifact,
  getLabCalibrationLatest,
  getLabCandidates,
  getLabFillModelsLatest,
  getLabProspective,
  getLabRegimesLatest,
  getLabSampleSizeLatest,
  getLabSummary,
  getLabVenueExecution
} from "@/lib/api";
import type { JsonRecord, LabArtifactPayload, LabCandidateEvidence, LabSummary, ProspectiveValidationRow, VenueExecutionEvidence } from "@/lib/types";
import { compact, dateTime, numberText } from "@/lib/format";
import {
  CALIBRATION_COLUMNS,
  FILL_MODEL_COLUMNS,
  QUEUE_PROXY_COLUMNS,
  REGIME_PROFILE_COLUMNS,
  type ReportColumn,
  selectCalibrationBucketRows,
  selectFillModelSummaryRows,
  selectQueueProxyRows,
  selectRegimeProfileRows
} from "@/lib/reportRows";
import { EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";
import { CorrectionGateNotice } from "@/components/CorrectionGateNotice";

const tabs = ["Overview", "Prospective Validation", "Regime Profiles", "Calibration", "Fill Models", "QueueProxy / Fill Realism", "Venue Execution", "Sample Size", "Artifacts"] as const;

export function LabsPage() {
  const [tab, setTab] = useState<(typeof tabs)[number]>("Overview");
  const labSummary = useQuery({ queryKey: ["labs", "summary"], queryFn: getLabSummary, retry: false });
  const candidateEvidence = useQuery({ queryKey: ["labs", "candidates"], queryFn: getLabCandidates, retry: false });
  const prospective = useQuery({ queryKey: ["labs", "prospective"], queryFn: getLabProspective, retry: false });
  const regimes = useQuery({ queryKey: ["labs", "regimes"], queryFn: getLabRegimesLatest, retry: false });
  const calibration = useQuery({ queryKey: ["labs", "calibration"], queryFn: getLabCalibrationLatest, retry: false });
  const fillModels = useQuery({ queryKey: ["labs", "fill-models"], queryFn: getLabFillModelsLatest, retry: false });
  const sampleSize = useQuery({ queryKey: ["labs", "sample-size"], queryFn: getLabSampleSizeLatest, retry: false });
  const venueExecution = useQuery({ queryKey: ["labs", "venue-execution"], queryFn: getLabVenueExecution, retry: false });
  const artifacts = useQuery({ queryKey: ["labs", "artifacts", "labs"], queryFn: () => getLabArtifacts(""), retry: false });

  const rows = prospective.data?.result?.rows ?? [];
  const frozenCandidates = candidateRows(prospective.data?.result?.frozen_candidates);
  const correction = venueExecution.data?.correction ?? prospective.data?.correction;

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Labs</h1>
        </div>
        <IconButton label="Refresh labs" onClick={() => void Promise.all([labSummary.refetch(), candidateEvidence.refetch(), prospective.refetch(), regimes.refetch(), calibration.refetch(), fillModels.refetch(), venueExecution.refetch(), sampleSize.refetch(), artifacts.refetch()])}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      <CorrectionGateNotice correction={correction} />

      <div className="flex flex-wrap gap-1 border border-line bg-white p-1 shadow-hairline">
        {tabs.map((item) => (
          <button
            key={item}
            onClick={() => setTab(item)}
            className={`h-9 rounded-sm px-3 text-sm font-medium ${tab === item ? "bg-ink text-white" : "text-ink/70 hover:bg-panel"}`}
          >
            {item}
          </button>
        ))}
      </div>

      {tab === "Overview" ? (
        <Overview
          rows={rows}
          candidates={frozenCandidates}
          apiCandidates={candidateEvidence.data?.candidates ?? []}
          summary={labSummary.data}
        />
      ) : null}
      {tab === "Prospective Validation" ? <ProspectiveTable rows={rows} loading={prospective.isLoading} /> : null}
      {tab === "Regime Profiles" ? (
        <ReportWithExplanation
          title="Regime Profiles"
          answer="Shows which regimes lose money, produce fills, trigger cancels, or are skipped before any strategy change is considered."
          rows={selectRegimeProfileRows(regimes.data?.report)}
          columns={REGIME_PROFILE_COLUMNS}
          emptyLabel="No top-level regime comparison/profile summary found. Nested market_results stay in artifact drilldowns instead of the summary table."
        />
      ) : null}
      {tab === "Calibration" ? (
        <ReportWithExplanation
          title="Calibration"
          answer="Compares predicted q_up with observed outcomes, highlighting overconfidence by probability bucket, expiry, and distance when reported."
          rows={selectCalibrationBucketRows(calibration.data?.report)}
          columns={CALIBRATION_COLUMNS}
          emptyLabel="No calibration bucket summary found. Grouped drilldowns may exist in the artifact, but they are not mixed into this top-level table."
        />
      ) : null}
      {tab === "Fill Models" ? (
        <ReportWithExplanation
          title="Fill Models"
          answer="Checks whether candidate PnL survives less optimistic paper fill assumptions before a recommendation is trusted."
          rows={selectFillModelSummaryRows(fillModels.data?.report)}
          columns={FILL_MODEL_COLUMNS}
          emptyLabel="No fill-model summary rows found. Per-market replay rows stay in artifact drilldowns instead of this summary table."
        />
      ) : null}
      {tab === "QueueProxy / Fill Realism" ? (
        <ReportWithExplanation
          title="QueueProxy / Fill Realism"
          answer="Shows whether QueueProxy shadow models have enough book, level-change, trade-print, and order-lifecycle evidence. Ineligible markets remain skipped with explicit reasons."
          rows={selectQueueProxyRows(fillModels.data?.report)}
          columns={QUEUE_PROXY_COLUMNS}
          emptyLabel="No QueueProxy fill-realism rows found. Run queue-audit or a fill-model report with queue_proxy_conservative/balanced evidence."
        />
      ) : null}
      {tab === "Venue Execution" ? <VenueExecutionPanel evidence={venueExecution.data} loading={venueExecution.isLoading} error={venueExecution.error} /> : null}
      {tab === "Sample Size" ? <SampleSizePanel report={sampleSize.data?.report} /> : null}
      {tab === "Artifacts" ? <ArtifactsPanel artifacts={artifacts.data?.artifacts ?? []} loading={artifacts.isLoading} /> : null}
    </div>
  );
}

function VenueExecutionPanel({ evidence, loading, error }: { evidence?: VenueExecutionEvidence; loading: boolean; error: Error | null }) {
  if (!evidence) {
    return <Panel><EmptyState label={loading ? "Loading authenticated venue evidence" : error?.message ?? "No authenticated venue evidence yet"} /></Panel>;
  }
  const latest = evidence.latest;
  const correction = evidence.correction;
  const correctionBlocked = correction?.blocks_promotion === true;
  const lifecycle = latest?.lifecycle;
  const order = latest?.order;
  const model = evidence.model;
  const profitabilityProvenance = evidence.artifact_provenance?.profitability;
  const selectedProfitability = profitabilityProvenance?.selected;
  const latestProvenance = evidence.artifact_provenance?.latest;
  const evidenceEligibility = latest?.evidence_eligibility;
  const latestAttempt = evidence.latest_attempt;
  const portfolio = evidence.redemption?.portfolio ?? evidence.preflight?.portfolio ?? latestAttempt?.portfolio ?? latest?.portfolio;
  const redemption = evidence.redemption;
  const profitability = evidence.profitability;
  const promotionMetrics = profitability?.gate_metrics?.metrics;
  const fundedLadder = profitability?.funded_ladder;
  const fundedHoldout = fundedLadder?.holdout_evaluation;
  const queueTransition = fundedLadder?.queue_model_transition;
  const promotionQuality = promotionMetrics?.data_quality;
  const coverageBreakdown = profitability?.data_quality?.coverage_breakdown ?? promotionQuality?.coverage_breakdown;
  const blockingWarnings = promotionQuality?.warnings?.filter((warning) => warning.severity === "blocking").length;
  const unclassifiedWarnings = promotionQuality?.warnings?.filter((warning) => warning.known === false).length;
  const durableRisk = evidence.preflight?.risk_at_end ?? latestAttempt?.risk_at_end ?? latest?.risk_at_end;
  const campaignRisk = durableRisk?.campaign;
  const dailyTurnover = durableRisk?.daily_turnover ?? durableRisk;
  const mostRecentRedemption = redemption?.recent_redemptions?.[0];
  const globalUnresolvedRisk = dailyTurnover?.global_unresolved_risk_reservations ??
    dailyTurnover?.unresolved_risk_reservations ?? campaignRisk?.unresolved_risk_reservation_count ?? 0;
  const markouts = latest?.markouts ?? [];
  const latestLegacyProtocolOrders = latest?.order_submitted && latest?.evidence_protocol_version !== 3
    ? latest.submitted_order_count ?? 1
    : 0;
  return (
    <div className="space-y-5">
      <Panel>
        <PanelHeader title="Profitability Path" meta={profitability?.generated_at ?? profitability?.created_at ?? "awaiting Azure promotion manifest"} />
        <div className="grid gap-3 p-4 md:grid-cols-4">
          <Metric label="Phase" value={correctionBlocked ? "risk_repair — correction NO-GO" : profitability?.phase ?? "risk repair / funded freeze"} />
          <Metric label="Selected Source" value={profitabilityProvenance?.selected_source ?? "API fallback"} />
          <Metric label="Selection Reason" value={profitabilityProvenance?.selection_reason ?? "no valid artifact"} />
          <Metric label="Artifact Freshness" value={selectedProfitability?.freshness ?? (selectedProfitability?.fresh ? "fresh" : "unknown")} />
          <Metric label="Artifact Time" value={dateTime(selectedProfitability?.authoritative_ts)} />
          <Metric label="Candidate" value={profitability?.candidate?.name ?? "dynamic_quote_style (frozen)"} />
          <Metric label="Candidate Version" value={profitability?.candidate?.version ?? profitability?.candidate?.candidate_version ?? "2026-06-14"} />
          <Metric label="Promotion" value={correctionBlocked ? "NO-GO — shadow correction blocks promotion" : fundedLadder?.phase === "profitable_go" ? "terminal validated GO — execution still unarmed" : fundedLadder?.phase === "stopped_no_go" || profitability?.phase === "stopped_no_go" ? "terminal NO-GO" : fundedLadder?.human_grant_required ? "awaiting exact human stage grant" : fundedLadder?.stage_authorized ? "stage authorized — collecting evidence" : profitability?.phase === "shadow_collecting" ? "shadow evidence collecting" : profitability?.promotion_allowed ? "armed" : "blocked — human authorization required"} />
          <Metric label="Funded Ladder" value={fundedLadder ? `${fundedLadder.phase ?? "evidence_collecting"} — ${fundedLadder.metrics?.cumulative_funded_orders ?? 0} / ${fundedLadder.active_target_orders ?? 1}` : "not started"} />
          <Metric label="Next Stage Grant" value={correctionBlocked ? "disabled — correction must complete" : fundedLadder?.phase === "profitable_go" ? "validated GO — still not automatically armed" : fundedLadder?.phase === "stopped_no_go" ? "terminal NO-GO — immutable stop" : fundedLadder?.human_grant_required ? "exact one-shot human grant required" : fundedLadder?.terminal ? "terminal" : "stage grant consumed"} />
          <Metric label="Eligible Evidence" value={`${fundedLadder?.metrics?.cumulative_eligible_orders ?? 0} eligible orders`} />
          <Metric label="Filled Markout Samples" value={`${fundedLadder?.metrics?.markout_sample_size ?? 0} (non-fills remain fill labels)`} />
          <Metric label="Funded Markout L95" value={signedUsd(fundedLadder?.metrics?.net_markout_30s_lower_95)} />
          <Metric label="Intent Model" value={queueTransition?.model_quality_passed ? `${queueTransition.binding?.model_version ?? "queue model"} — checkpoint 100 bound` : profitability?.execution_model?.model_version ?? "frozen conservative prior"} />
          <Metric label="Campaign Expiry" value={dateTime(profitability?.expires_at)} />
          <Metric label="Campaign Equity" value={usd(profitability?.capital?.current_equity ?? campaignRisk?.account_equity ?? portfolio?.account_equity)} />
          <Metric label="Campaign PnL" value={signedUsd(profitability?.capital?.campaign_net_pnl)} />
          <Metric label="Lifetime Account PnL" value={signedUsd(profitability?.capital?.lifetime_net_pnl ?? portfolio?.account_net_pnl)} />
          <Metric label="Equity Floor" value={usd(profitability?.capital?.equity_floor ?? campaignRisk?.equity_floor)} />
          <Metric label="Current Drawdown" value={usd(profitability?.capital?.current_drawdown ?? campaignRisk?.campaign_drawdown)} />
          <Metric label="Locked Principal" value={usd(profitability?.capital?.locked_principal)} />
          <Metric label="Unresolved Exposure" value={usd(profitability?.capital?.unresolved_exposure)} />
          <Metric label="Blocking Reason" value={correction?.blocker ?? profitability?.blocking_reason ?? campaignRisk?.blockers?.join(", ") ?? "funded execution remains disabled"} />
        </div>
        <div className="border-t border-line bg-amber-50 px-4 py-3 text-sm leading-relaxed text-amber-950">
          Historical simulation, execution-probe cost, shadow PnL, and live-strategy PnL are separate ledgers. A profitable post-reset campaign does not erase the lifetime account loss, and no phase automatically arms a real order.
        </div>
      </Panel>

      {queueTransition || fundedHoldout ? (
        <Panel>
          <PanelHeader title="Orders 101–200 Holdout" meta={fundedHoldout?.passed ? "terminal holdout passed" : queueTransition?.model_quality_passed ? "frozen model active — collecting later orders" : "awaiting checkpoint 100"} />
          <div className="grid gap-3 p-4 md:grid-cols-4">
            <Metric label="Frozen Model" value={queueTransition?.binding?.model_version ?? "pending"} />
            <Metric label="Training Cutoff" value={dateTime(queueTransition?.training_cutoff)} />
            <Metric label="Exact Holdout Orders" value={`${fundedHoldout?.exact_order_count ?? 0} / 100`} />
            <Metric label="Fill / Non-fill Labels" value={`${fundedHoldout?.filled_order_count ?? 0} / ${fundedHoldout?.non_filled_order_count ?? 0}`} />
            <Metric label="Holdout Net PnL" value={signedUsd(fundedHoldout?.holdout_net_pnl)} />
            <Metric label="PnL / Order L95" value={signedUsd(fundedHoldout?.holdout_net_pnl_per_order_lower_95)} />
            <Metric label="Holdout Max Drawdown" value={usd(fundedHoldout?.holdout_max_drawdown)} />
            <Metric label="Holdout Markout L95" value={signedUsd(fundedHoldout?.net_markout_30s_lower_95)} />
            <Metric label="Brier Improvement" value={fundedHoldout?.brier_improvement_fraction === undefined ? "pending" : percentage(fundedHoldout.brier_improvement_fraction)} />
            <Metric label="Calibration Error" value={fundedHoldout?.expected_calibration_error ?? "pending"} />
            <Metric label="Decision" value={fundedHoldout?.passed ? "passed" : "not yet passed"} />
          </div>
          <div className="border-t border-line px-4 py-3 text-sm text-ink/70">
            GO requires the later 101–200 orders themselves to be profitable with a positive per-order 95% lower bound; gains from orders 1–100 cannot mask a losing holdout.
          </div>
        </Panel>
      ) : null}

      <div className="grid gap-5 xl:grid-cols-2">
        <Panel>
          <PanelHeader title="Shadow Profitability Gate" meta="30 clean days and 1,000 settled markets" />
          <div className="grid gap-3 p-4 md:grid-cols-2">
            <Metric label="Clean Days" value={`${profitability?.shadow?.clean_days ?? promotionMetrics?.clean_days ?? 0} / ${profitability?.shadow?.required_clean_days ?? 30}`} />
            <Metric label="Campaign Window" value={`${promotionMetrics?.observed_calendar_days ?? 0} / 60 calendar days`} />
            <Metric label="Settled Markets" value={`${profitability?.shadow?.settled_markets ?? promotionMetrics?.settled_markets ?? 0} / ${profitability?.shadow?.required_settled_markets ?? 1000}`} />
            <Metric label="Wallet-constrained PnL" value={signedUsd(profitability?.shadow?.wallet_constrained_net_pnl ?? promotionMetrics?.wallet_constrained_net_pnl)} />
            <Metric label="Queue-conservative PnL (all intents)" value={signedUsd(profitability?.shadow?.queue_conservative_net_pnl ?? promotionMetrics?.queue_conservative_net_pnl)} />
            <Metric label="Daily Wallet PnL L95" value={signedUsd(profitability?.shadow?.pnl_ci_lower_95 ?? promotionMetrics?.pnl_ci_95_low)} />
            <Metric label="Positive Weekly Blocks" value={`${profitability?.shadow?.positive_weekly_blocks ?? promotionMetrics?.consecutive_positive_weekly_blocks ?? 0} / ${profitability?.shadow?.required_positive_weekly_blocks ?? 4}`} />
            <Metric label="Full Decision Replay Parity" value={percentage(profitability?.shadow?.decision_parity_rate ?? promotionMetrics?.decision_parity_rate)} />
          </div>
        </Panel>
        <Panel>
          <PanelHeader title="Promotion Data Quality" meta={profitability?.data_quality?.status ?? promotionQuality?.registry_version ?? "collecting"} />
          <div className="grid gap-3 p-4 md:grid-cols-2 xl:grid-cols-3">
            <Metric label="Decision-grade Evaluations" value={percentage(profitability?.data_quality?.decision_grade_coverage ?? promotionQuality?.decision_grade_coverage)} />
            <Metric label="Minimum Coverage" value={percentage(profitability?.data_quality?.minimum_coverage ?? 0.95)} />
            <Metric label="Start-price Coverage" value={percentage(coverageBreakdown?.start_price_capture_rate)} />
            <Metric label="Settlement Coverage" value={percentage(coverageBreakdown?.settlement_rate)} />
            <Metric label="Exact-source Hour Coverage" value={percentage(coverageBreakdown?.exact_reference_hour_coverage)} />
            <Metric label="Decision Metadata" value={percentage(coverageBreakdown?.decision_metadata_coverage)} />
            <Metric label="Execution Fields" value={percentage(coverageBreakdown?.execution_field_coverage)} />
            <Metric label="Full Decision Replay" value={percentage(coverageBreakdown?.decision_parity_rate)} />
            <Metric label="Queue Snapshots" value={percentage(coverageBreakdown?.queue_snapshot_coverage)} />
            <Metric label="1s Markout Completion" value={percentage(coverageBreakdown?.markout_1s_completion)} />
            <Metric label="5s Markout Completion" value={percentage(coverageBreakdown?.markout_5s_completion)} />
            <Metric label="30s Markout Completion" value={percentage(coverageBreakdown?.markout_30s_completion)} />
            <Metric label="Fatal Warnings" value={profitability?.data_quality?.fatal_warnings ?? promotionQuality?.fatal_issues?.length ?? 0} />
            <Metric label="Blocking Warnings" value={profitability?.data_quality?.blocking_warnings ?? blockingWarnings ?? 0} />
            <Metric label="Unclassified Warnings" value={profitability?.data_quality?.unclassified_warnings ?? unclassifiedWarnings ?? 0} />
            <Metric label="Unknown Warnings" value={(profitability?.data_quality?.unclassified_warnings ?? unclassifiedWarnings ?? 0) > 0 ? "promotion blocked" : "none"} />
          </div>
        </Panel>
      </div>

      <Panel>
        <PanelHeader title="Authenticated Venue Evidence" meta={latest?.finished_ts ?? evidence.generated_ts} />
        <div className="grid gap-3 p-4 md:grid-cols-4">
          <Metric label="Probe Status" value={latest?.status ?? "not run"} />
          <Metric label="Evidence Protocol" value={latest?.evidence_protocol_version ? `v${latest.evidence_protocol_version}` : "legacy"} />
          <Metric label="Protocol Admission" value={evidenceEligibility?.counts_toward_protocol_evidence ? "eligible" : "display-only — terminal validator required"} />
          <Metric label="Evidence Freshness" value={latestProvenance?.freshness ?? (latestProvenance?.fresh ? "fresh" : "unknown")} />
          <Metric label="Evidence Time" value={dateTime(latestProvenance?.authoritative_ts)} />
          <Metric label="Execution Origin" value={latest?.execution_country ? `${latest.execution_country} / Azure North Europe` : latest?.execution_origin ?? "not verified"} />
          <Metric label="Static Egress Verified" value={latest?.static_egress_verified ? "yes" : "no"} />
          <Metric label="Campaign Progress" value={`${latest?.completed_probe_count ?? 0} / ${latest?.submitted_order_count ?? 0} reconciled`} />
          <Metric label="Order Submitted" value={latest?.order_submitted ? "yes" : "no"} />
          <Metric label="Venue Ack" value={milliseconds(lifecycle?.client_to_http_ack_ms)} />
          <Metric label="Cancel Round Trip" value={milliseconds(lifecycle?.client_cancel_round_trip_ms)} />
          <Metric label="User Cancel Ack" value={milliseconds(lifecycle?.client_to_user_cancel_ack_ms)} />
          <Metric label="Matched Size" value={lifecycle?.actual_matched_size ?? 0} />
          <Metric label="Partial Fill" value={lifecycle?.partial_fill ? "yes" : "no"} />
          <Metric label="Cancel Race Fill" value={lifecycle?.fill_raced_cancellation ? "yes" : "no"} />
          <Metric label="Strict Trade-throughs" value={lifecycle?.public_strict_trade_through_count ?? 0} />
          <Metric label="Trade-throughs Without Fill" value={lifecycle?.public_trade_through_without_fill_count ?? 0} />
          <Metric label="Planned Rest" value={lifecycle?.planned_rest_seconds === undefined ? "not observed" : `${lifecycle.planned_rest_seconds}s`} />
          <Metric label="Zero Open Orders" value={lifecycle?.zero_open_orders_confirmed ? "confirmed" : "not confirmed"} />
          <Metric label="Data Gap" value={lifecycle?.data_gap_detected ? "ineligible" : "none detected"} />
          <Metric label="Markouts Complete" value={lifecycle?.markout_capture_complete ? "yes" : "not applicable / incomplete"} />
          <Metric label="Matched-size Agreement" value={lifecycle?.matched_size_source_agreement ? "REST = user channel" : "not confirmed"} />
          <Metric label="Trade-ID Agreement" value={lifecycle?.trade_id_source_agreement ? "REST = user channel" : "not confirmed"} />
          <Metric label="WS Reconnects" value={(lifecycle?.authenticated_user_channel_reconnects ?? 0) + (lifecycle?.public_market_channel_reconnects ?? 0)} />
          <Metric label="Campaign Risk" value={campaignRisk?.passed ? "passed" : campaignRisk?.blockers?.join(", ") ?? "legacy evidence"} />
          <Metric label="Campaign Drawdown" value={usd(campaignRisk?.campaign_drawdown)} />
          <Metric label="Legacy Daily Turnover" value={dailyTurnover?.conservative_loss_budget_consumed ?? 0} />
          <Metric label="Global Unresolved Risk Reservations" value={globalUnresolvedRisk} />
        </div>
        {latest?.stop_reason ? <div className="border-t border-line px-4 py-3 text-sm text-ink/70">Campaign stop reason: {latest.stop_reason}.</div> : null}
        {latest?.order_submitted && !evidenceEligibility?.counts_toward_protocol_evidence ? (
          <div className="border-t border-line bg-sky-50 px-4 py-3 text-sm leading-relaxed text-sky-950">
            Labs is showing this artifact for audit only. Protocol admission is decided by the terminal identity-bound controller and reporting validators, not by this dashboard response.
          </div>
        ) : null}
        {latestLegacyProtocolOrders > 0 ? (
          <div className="border-t border-line bg-amber-50 px-4 py-3 text-sm leading-relaxed text-amber-950">
            These {latestLegacyProtocolOrders} real funded order{latestLegacyProtocolOrders === 1 ? "" : "s"} remain included in lifetime account PnL, but cannot count toward protocol-v3 promotion evidence. Their artifacts predate the immutable v3 lifecycle contract, and missing guarantees cannot be asserted retroactively. No spend or PnL is erased or reclassified.
          </div>
        ) : null}
        {latestAttempt?.run_id && latestAttempt.run_id !== latest?.run_id ? (
          <div className="border-t border-line px-4 py-3 text-sm text-ink/70">
            Latest attempt: {latestAttempt.status ?? "unknown"} at {latestAttempt.finished_ts ?? "unknown time"}. {latestAttempt.error ?? "No additional error reported."}
          </div>
        ) : null}
      </Panel>

      <Panel>
        <PanelHeader title="Portfolio Settlement" meta={portfolio?.captured_ts ?? "awaiting Azure portfolio snapshot"} />
        {portfolio?.status === "available" ? (
          <>
            <div className="grid gap-3 p-4 md:grid-cols-4">
              <Metric label="Liquid Collateral" value={usd(portfolio.liquid_collateral)} />
              <Metric label="Gross Redeemable Payout" value={usd(portfolio.gross_redeemable_value)} />
              <Metric label="Resolved Position Cost" value={usd(portfolio.resolved_position_cost)} />
              <Metric label="Resolved Losing Cost" value={usd(portfolio.resolved_losing_cost)} />
              <Metric label="Current Position Value" value={usd(portfolio.current_position_value)} />
              <Metric label="Account Equity" value={usd(portfolio.account_equity)} />
              <Metric label="Starting Capital" value={usd(portfolio.starting_capital)} />
              <Metric label="True Net Account PnL" value={signedUsd(portfolio.account_net_pnl)} />
              <Metric label="Redeemable Winners" value={portfolio.redeemable_winner_count ?? 0} />
              <Metric label="Redemption Worker" value={redemption?.status ?? "not run"} />
              <Metric label="Selected Payout" value={usd(redemption?.selection?.selected_gross_payout)} />
              <Metric label="Realized Payout" value={usd(redemption?.realized_payout)} />
              <Metric label="Gasless Submission" value={redemption?.redemption_submitted ? "confirmed" : redemption?.dry_run ? "dry-run only" : "not submitted"} />
              <Metric label="Most Recent Redemption" value={usd(mostRecentRedemption?.gross_payout)} />
              <Metric label="Redemption Attribution" value={mostRecentRedemption?.attribution === "azure_redemption_worker" ? "Azure worker" : mostRecentRedemption ? "external / manual" : "none observed"} />
              <Metric label="Redeemed At" value={dateTime(mostRecentRedemption?.redeemed_ts)} />
              <Metric label="Redemption Open Orders" value={redemption?.zero_open_orders_confirmed ? "zero confirmed" : "not confirmed"} />
            </div>
            <div className="border-t border-line bg-amber-50 px-4 py-3 text-sm leading-relaxed text-amber-950">
              Gross payout is not profit. True account PnL equals liquid collateral plus current position value minus starting capital; losing resolved positions remain included in the calculation.
            </div>
            {redemption ? (
              <div className="border-t border-line px-4 py-3 text-sm text-ink/70">
                Azure redemption: {redemption.status ?? "unknown"}. Wallet derivation {redemption.derived_wallet_match ? "verified" : "not verified"}; {redemption.planned_calls?.length ?? 0} bounded call(s) planned. Redemption only converts resolved outcome tokens into liquid collateral and does not reset the UTC trading-risk budget.
                {mostRecentRedemption ? ` Most recent observed payout: ${usd(mostRecentRedemption.gross_payout)} via ${mostRecentRedemption.attribution === "azure_redemption_worker" ? "the Azure worker" : "an external/manual relayer transaction"}.` : ""}
              </div>
            ) : null}
          </>
        ) : <EmptyState label="No current Azure portfolio snapshot is available." />}
      </Panel>

      <div className="grid gap-5 xl:grid-cols-2">
        <Panel>
          <PanelHeader title="Effective Queue Evidence" meta={evidence.queue_position_source} />
          <div className="grid gap-3 p-4 md:grid-cols-2">
            <Metric label="Queue Metric" value={evidence.queue_position_metric} />
            <Metric label="Inferred Size Ahead" value={order?.inferredSizeAhead ?? "not observed"} />
            <Metric label="Same-price Public Size" value={order?.samePricePublicSize ?? "not observed"} />
            <Metric label="Better-price Public Size" value={order?.betterPricePublicSize ?? "not observed"} />
            <Metric label="Probe Notional" value={order?.notional ?? "not submitted"} />
            <Metric label="Literal FIFO Rank" value="unavailable from venue feeds" />
          </div>
          <div className="border-t border-line bg-amber-50 px-4 py-3 text-sm leading-relaxed text-amber-950">
            This is not exact FIFO queue position. It combines your authenticated order/fill lifecycle with public aggregated L2 depth. {evidence.remaining_limitation}
          </div>
        </Panel>

        <Panel>
          <PanelHeader title="Empirical Fill Model" meta={model?.status ?? "not trained"} />
          <div className="grid gap-3 p-4 md:grid-cols-2">
            <Metric label="Target" value="P(fill within 1/5/30/60s)" />
            <Metric label="Model Version" value={model?.model_version ?? evidence.profitability?.execution_model?.model_version ?? "pending"} />
            <Metric label="Promotion-bound Model SHA-256" value={evidence.profitability?.execution_model?.sha256 ?? "not bound"} />
            <Metric label="Eligible Order Probes" value={model?.sample_size ?? 0} />
            <Metric label="Eligible Horizon Labels" value={model?.label_sample_size ?? 0} />
            <Metric label="Filled Probes" value={model?.positive_fills ?? 0} />
            <Metric label="Non-filled Probes" value={model?.negative_non_fills ?? 0} />
            <Metric label="Excluded Probes" value={model?.excluded_observations ?? 0} />
            <Metric label="Legacy Protocol Probes" value={model?.legacy_protocol_observations ?? latestLegacyProtocolOrders} />
            <Metric label="Minimum Eligible Probes" value={model?.minimum_samples ?? 100} />
            <Metric label="Temporal Holdout" value={model?.temporal_split ?? "required before training"} />
            <Metric label="OOS Brier Score" value={model?.out_of_sample_brier_score ?? "pending"} />
            <Metric label="Naive Brier Score" value={model?.naive_horizon_base_rate_brier_score ?? "pending"} />
            <Metric label="Brier Improvement" value={model?.brier_improvement_fraction === undefined ? "pending" : percentage(model.brier_improvement_fraction)} />
            <Metric label="Expected Calibration Error" value={model?.expected_calibration_error ?? "pending"} />
            <Metric label="Quality Gates" value={model?.quality_gates?.passed ? "passed" : "collecting / failed"} />
            <Metric label="Excluded Data-gap Probes" value={model?.quality_gates?.excluded_data_gap_observations ?? 0} />
            <Metric label="Early Markout Exclusions" value={model?.quality_gates?.early_markout_observations ?? 0} />
            <Metric label="Net 30s Markout" value={model?.net_markout_30s_sample_size ? numberText(model.mean_net_executable_markout_30s_per_share) : "pending"} />
            <Metric label="Net 30s Markout 95% Lower" value={model?.net_executable_markout_30s_lower_confidence_bound_95 ?? "pending"} />
            <Metric label="Promotion Ready" value={model?.promotion_ready ? "yes — human approval required" : "no"} />
          </div>
          <div className="border-t border-line px-4 py-3 text-sm text-ink/70">
            {model?.reason ?? model?.promotion_block_reason ?? "The model stays research-only and cannot be promoted until temporal out-of-sample validation is available."}
          </div>
          {(model?.legacy_protocol_observations ?? 0) > 0 ? (
            <div className="border-t border-line bg-amber-50 px-4 py-3 text-sm leading-relaxed text-amber-950">
              Legacy protocol observations remain visible but do not qualify for promotion. Protocol v3 requires durable pre-send risk accounting, single-campaign leasing, REST/user-channel agreement, and one timely 1/5/30-second markout triplet per authenticated fill.
            </div>
          ) : null}
        </Panel>
      </div>

      <Panel>
        <PanelHeader title="Post-fill Markouts" meta="1 / 5 / 30 seconds" />
        <div className="border-b border-line px-4 py-2 text-xs leading-relaxed text-ink/60">
          Shadow net executable markout subtracts the recorded entry fee only. It is an adverse-selection diagnostic, not round-trip liquidation PnL; a round-trip fee is shown only when explicitly recorded.
        </div>
        {markouts.length ? (
          <div className="overflow-auto">
            <table className="w-full min-w-[1050px] text-left text-sm">
              <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
                <tr>{["Fill ID", "Fill Size", "Horizon", "Gross Midpoint / Share", "Gross Executable / Share", "Recorded / Entry Fee / Share", "Explicit Round-trip Fee / Share", "Recorded Net Executable / Share", "Observation Delay"].map((header) => <th key={header} className="px-3 py-2">{header}</th>)}</tr>
              </thead>
              <tbody>{markouts.map((markout) => {
                const delay = optionalNumeric(markout.observation_delay_ms);
                const recordedFee = optionalNumeric(markout.fee_per_share ?? markout.entry_fee_per_share);
                const explicitRoundTripFee = optionalNumeric(markout.round_trip_fee_per_share);
                const recordedNetExecutable = optionalNumeric(markout.net_executable_markout_per_share);
                const invalid = markout.observation_delay_ms === null || markout.observation_delay_ms === undefined ||
                  !Number.isFinite(delay) || delay < 0 || delay > 2000 ||
                  markout.midpoint_markout_per_share === null || markout.midpoint_markout_per_share === undefined ||
                  markout.executable_markout_per_share === null || markout.executable_markout_per_share === undefined ||
                  !Number.isFinite(recordedFee) || !Number.isFinite(recordedNetExecutable);
                return (
                <tr key={`${markout.fill_id ?? "legacy"}-${markout.horizon_seconds}`} className={`border-b border-line last:border-b-0 ${invalid ? "bg-amber-50" : ""}`}>
                  <td className="px-3 py-2 font-mono text-xs">{markout.fill_id ? `${markout.fill_id.slice(0, 10)}…` : "legacy"}</td>
                  <td className="px-3 py-2">{numberText(markout.fill_size)}</td>
                  <td className="px-3 py-2">{markout.horizon_seconds}s</td>
                  <td className="px-3 py-2">{numberText(markout.midpoint_markout_per_share)}</td>
                  <td className="px-3 py-2">{numberText(markout.executable_markout_per_share)}</td>
                  <td className="px-3 py-2">{Number.isFinite(recordedFee) ? numberText(recordedFee) : "missing"}</td>
                  <td className="px-3 py-2">{Number.isFinite(explicitRoundTripFee) ? numberText(explicitRoundTripFee) : "not recorded"}</td>
                  <td className="px-3 py-2">{Number.isFinite(recordedNetExecutable) ? numberText(recordedNetExecutable) : "missing"}</td>
                  <td className="px-3 py-2">{milliseconds(markout.observation_delay_ms)}{invalid ? " — excluded from v3 model" : ""}</td>
                </tr>
                );
              })}</tbody>
            </table>
          </div>
        ) : <EmptyState label="No real fill occurred, so no markout can honestly be reported yet." />}
      </Panel>
    </div>
  );
}

function milliseconds(value: number | null | undefined) {
  return value === null || value === undefined ? "not observed" : `${numberText(value)} ms`;
}

function optionalNumeric(value: string | number | null | undefined) {
  if (value === null || value === undefined || value === "") return Number.NaN;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function usd(value: number | null | undefined) {
  return value === null || value === undefined ? "n/a" : `$${numberText(value, 6)}`;
}

function signedUsd(value: string | number | null | undefined) {
  if (value === null || value === undefined) return "n/a";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "n/a";
  if (numeric > 0) return `+$${numberText(numeric, 6)}`;
  if (numeric < 0) return `-$${numberText(Math.abs(numeric), 6)}`;
  return "$0";
}

function percentage(value: string | number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "pending";
  return `${numberText(Number(value) * 100, 2)}%`;
}

function Overview({
  rows,
  candidates,
  apiCandidates,
  summary
}: {
  rows: ProspectiveValidationRow[];
  candidates: JsonRecord[];
  apiCandidates: LabCandidateEvidence[];
  summary?: LabSummary;
}) {
  const latest = rows.at(-1);
  const [selected, setSelected] = useState<CandidateEvidence | null>(null);
  const evidence = apiCandidates.length ? apiEvidenceRows(apiCandidates, latest) : evidenceRows(rows, candidates);
  const summaryStats = sampleSizeStats(summary?.sample_size);
  const candidateList = candidates.length
    ? candidates
    : apiCandidates.length
      ? apiCandidates.map((candidate) => ({
          name: candidate.candidate,
          profile: candidate.profile ?? candidate.candidate,
          candidate_version: candidate.candidate_version,
          frozen_since: candidate.frozen_since
        }))
      : fallbackCandidates();
  return (
    <div className="space-y-5">
      <Panel>
        <PanelHeader title="Research Status" meta={summary?.status ?? latest?.date ?? "collecting evidence"} />
        <div className="grid gap-3 p-4 md:grid-cols-4">
          <Metric label="Settled Sample" value={summaryStats?.n ?? latest?.settled_markets ?? "waiting for research job"} />
          <Metric label="Prospective Rows" value={summary?.prospective_rows ?? rows.length} />
          <Metric label="Candidates" value={summary?.candidate_count ?? (apiCandidates.length || candidates.length || 4)} />
          <Metric label="Data Quality" value={summary?.data_quality ?? latest?.data_quality_status ?? "unknown"} />
        </div>
        <div className="border-t border-line px-4 py-3 text-sm text-ink/70">
          {compact(summary?.recommendation ?? latest?.recommendation ?? recommendationText(latest), "collect more evidence")}
        </div>
      </Panel>
      <div className="grid gap-5 xl:grid-cols-[360px_1fr]">
        <Panel>
          <PanelHeader title="Frozen Candidates" meta={`${candidates.length || apiCandidates.length || 4} tracked`} />
          <div className="space-y-2 p-4">
            {candidateList.map((candidate) => (
              <div key={String(candidate.name)} className="border border-line bg-panel px-3 py-2">
                <div className="flex items-center justify-between gap-2">
                  <span className="truncate text-sm font-semibold text-ink">{String(candidate.name)}</span>
                  <Pill tone="neutral">disabled</Pill>
                </div>
                <div className="mt-1 truncate text-xs text-ink/55">{String(candidate.profile ?? candidate.name)}</div>
                <div className="mt-1 truncate text-xs text-ink/45">{String(candidate.candidate_version ?? candidate.frozen_since ?? "frozen metadata pending")}</div>
              </div>
            ))}
          </div>
        </Panel>
        <Panel>
          <PanelHeader title="Candidate Evidence Matrix" meta={latest?.date ?? summary?.status ?? "collecting evidence"} />
          <EvidenceMatrix rows={evidence} onSelect={setSelected} />
          {selected ? <CandidateDrawer candidate={selected} onClose={(): void => setSelected(null)} /> : null}
        </Panel>
      </div>
    </div>
  );
}

function ProspectiveTable({ rows, loading }: { rows: ProspectiveValidationRow[]; loading: boolean }) {
  if (!rows.length) {
    return <Panel><EmptyState label={loading ? "Loading prospective rows" : "No prospective rows yet"} /></Panel>;
  }
  return (
    <div className="space-y-5">
      <ProspectiveCharts rows={rows} />
      <Panel>
        <PanelHeader title="Prospective Validation" meta={`${rows.length} rows`} />
        <div className="overflow-auto">
          <table className="w-full min-w-[1040px] text-left text-sm">
            <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
              <tr>
                {["Date", "Markets", "Static", "Dynamic Quote", "Dynamic Δ", "Full Deterministic", "Best Δ", "Gate", "Quality", "Recommendation"].map((header) => (
                  <th key={header} className="px-3 py-2">{header}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.date} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2">{row.date}</td>
                  <td className="px-3 py-2">{numberText(row.settled_markets)}</td>
                  <td className="px-3 py-2">{numberText(row.static_net_pnl)}</td>
                  <td className="px-3 py-2">{numberText(row.dynamic_quote_style_net_pnl)}</td>
                  <td className="px-3 py-2">{numberText(row.dynamic_quote_style_paired_delta)}</td>
                  <td className="px-3 py-2">{numberText(row.full_deterministic_profile_net_pnl)}</td>
                  <td className="px-3 py-2">{numberText(row.best_candidate_paired_delta)}</td>
                  <td className="px-3 py-2"><Pill tone={gateTone(row.decision_gate)}>{row.decision_gate ?? "RESEARCH_ONLY"}</Pill></td>
                  <td className="px-3 py-2"><Pill tone={row.data_quality_status === "healthy" ? "good" : "warn"}>{row.data_quality_status ?? "unknown"}</Pill></td>
                  <td className="px-3 py-2">{row.recommendation ?? recommendationText(row)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}

function ReportWithExplanation({
  title,
  answer,
  rows,
  columns,
  emptyLabel
}: {
  title: string;
  answer: string;
  rows: JsonRecord[];
  columns: ReportColumn[];
  emptyLabel: string;
}) {
  return (
    <div className="space-y-5">
      <Panel>
        <PanelHeader title={`${title} Evidence`} meta="recommendation context" />
        <div className="p-4 text-sm leading-relaxed text-ink/70">{answer}</div>
      </Panel>
      <GenericReport title={title} rows={rows} columns={columns} emptyLabel={emptyLabel} />
    </div>
  );
}

type CandidateEvidence = {
  candidate: string;
  version?: string;
  status: string;
  latestPnl: unknown;
  pairedDelta: unknown;
  decisionGate: string;
  ci: string;
  maxDrawdown: unknown;
  fillModelAgreement: string;
  dataQuality: string;
  recommendation: string;
  lastUpdated: string;
  explanation: string;
  latest?: ProspectiveValidationRow;
};

function EvidenceMatrix({ rows, onSelect }: { rows: CandidateEvidence[]; onSelect: (candidate: CandidateEvidence) => void }) {
  if (!rows.length) {
    return <EmptyState label="No candidate evidence yet. Run prospective validation or select another report date." />;
  }
  return (
    <div className="overflow-auto">
      <table className="w-full min-w-[980px] text-left text-sm">
        <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
          <tr>
            {["Candidate", "Version", "Status", "Latest PnL", "Paired Δ", "Gate", "95% CI", "Quality", "Updated"].map((header) => (
              <th key={header} className="px-3 py-2">{header}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.candidate} className="cursor-pointer border-b border-line last:border-b-0 hover:bg-panel" onClick={() => onSelect(row)}>
              <td className="px-3 py-2 font-medium text-ink">{row.candidate}</td>
              <td className="px-3 py-2 text-xs text-ink/65">{row.version ?? "frozen"}</td>
              <td className="px-3 py-2"><Pill tone={row.status === "candidate_leader" ? "good" : row.status === "blocked" || row.status.includes("rejected") ? "danger" : "warn"}>{row.status}</Pill></td>
              <td className="px-3 py-2">{numberText(row.latestPnl)}</td>
              <td className="px-3 py-2">{numberText(row.pairedDelta)}</td>
              <td className="px-3 py-2"><Pill tone={gateTone(row.decisionGate)}>{row.decisionGate}</Pill></td>
              <td className="px-3 py-2">{row.ci}</td>
              <td className="px-3 py-2"><Pill tone={row.dataQuality === "healthy" ? "good" : "warn"}>{row.dataQuality}</Pill></td>
              <td className="px-3 py-2">{row.lastUpdated}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CandidateDrawer({ candidate, onClose }: { candidate: CandidateEvidence; onClose: () => void }) {
  return (
    <div className="border-t border-line bg-panel p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-ink">{candidate.candidate}</h2>
          <p className="mt-1 text-sm text-ink/65">{candidate.explanation}</p>
        </div>
        <button className="h-8 rounded-sm border border-line bg-white px-3 text-sm text-ink/70 hover:bg-panel" onClick={onClose}>
          Close
        </button>
      </div>
      <div className="mt-4 grid gap-3 md:grid-cols-4">
        <Metric label="PnL by Day" value={candidate.latestPnl} />
        <Metric label="Paired Delta" value={candidate.pairedDelta} />
        <Metric label="Decision Gate" value={candidate.decisionGate} />
        <Metric label="Market Count" value={candidate.latest?.settled_markets ?? "collecting"} />
      </div>
    </div>
  );
}

function ProspectiveCharts({ rows }: { rows: ProspectiveValidationRow[] }) {
  const chartRows = rows.map((row) => ({
    date: row.date,
    static: numeric(row.static_net_pnl),
    dynamic: numeric(row.dynamic_quote_style_net_pnl),
    full: numeric(row.full_deterministic_profile_net_pnl),
    markets: numeric(row.settled_markets),
    ci_low: numeric(row.ci_95_low),
    ci_high: numeric(row.ci_95_high)
  }));
  return (
    <div className="grid gap-5 xl:grid-cols-3">
      <ChartPanel title="Candidate PnL" meta="daily prospective rows" hasData={chartRows.length > 0}>
        <LineChart data={chartRows}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="date" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} width={42} />
          <Tooltip formatter={(value) => numberText(value)} />
          <Line type="monotone" dataKey="static" stroke="#17201b" dot={false} isAnimationActive={false} />
          <Line type="monotone" dataKey="dynamic" stroke="#18705b" dot={false} isAnimationActive={false} />
          <Line type="monotone" dataKey="full" stroke="#2f7fcb" dot={false} isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel title="Market Count" meta="settled markets by day" hasData={chartRows.length > 0}>
        <BarChart data={chartRows}>
          <CartesianGrid stroke="#d9ddd2" vertical={false} />
          <XAxis dataKey="date" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} width={42} />
          <Tooltip formatter={(value) => numberText(value, 0)} />
          <Bar dataKey="markets" fill="#18705b" isAnimationActive={false} />
        </BarChart>
      </ChartPanel>
      <ChartPanel title="Confidence Interval" meta="95% low/high trend" hasData={chartRows.length > 0}>
        <LineChart data={chartRows}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="date" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} width={42} />
          <Tooltip formatter={(value) => numberText(value)} />
          <Line type="monotone" dataKey="ci_low" stroke="#b3363a" dot={false} isAnimationActive={false} />
          <Line type="monotone" dataKey="ci_high" stroke="#18705b" dot={false} isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
    </div>
  );
}

function ChartPanel({ title, meta, hasData, children }: { title: string; meta: string; hasData: boolean; children: ReactElement }) {
  return (
    <Panel>
      <PanelHeader title={title} meta={meta} />
      <div className="h-64 p-3">
        {hasData ? <ResponsiveContainer width="100%" height="100%">{children}</ResponsiveContainer> : <EmptyState label="No chartable prospective rows yet" />}
      </div>
    </Panel>
  );
}

function evidenceRows(rows: ProspectiveValidationRow[], candidates: JsonRecord[]): CandidateEvidence[] {
  const latest = rows.at(-1);
  return (candidates.length ? candidates : fallbackCandidates()).map((candidate) => {
    const name = String(candidate.name ?? candidate.profile ?? "candidate");
    const pnl = candidatePnl(latest, name);
    const status = statusForCandidate(latest, name, pnl);
    const recommendation = latest?.recommendation ?? recommendationText(latest);
    return {
      candidate: name,
      version: String(candidate.candidate_version ?? candidate.frozen_since ?? "frozen"),
      status,
      latestPnl: pnl,
      pairedDelta: candidateDelta(latest, name),
      decisionGate: decisionGateForCandidate(latest, name),
      ci: latest ? `[${numberText(latest.ci_95_low)}, ${numberText(latest.ci_95_high)}]` : "collecting",
      maxDrawdown: latest?.max_drawdown ?? "collecting",
      fillModelAgreement: latest?.fill_model ? String(latest.fill_model) : "pending sensitivity",
      dataQuality: latest?.data_quality_status ?? "unknown",
      recommendation,
      lastUpdated: latest?.date ?? "not run",
      explanation: explanationForCandidate(name, status, recommendation, latest),
      latest
    };
  });
}

function apiEvidenceRows(candidates: LabCandidateEvidence[], latest?: ProspectiveValidationRow): CandidateEvidence[] {
  return candidates.map((candidate) => ({
    candidate: candidate.candidate,
    version: candidate.candidate_version ?? candidate.frozen_since ?? "frozen",
    status: candidate.status ?? "collecting",
    latestPnl: candidate.latest_test_pnl ?? "collecting",
    pairedDelta: candidate.paired_delta ?? "baseline",
    decisionGate: candidate.decision_gate ?? "RESEARCH_ONLY",
    ci:
      candidate.ci_95_low !== null && candidate.ci_95_low !== undefined && candidate.ci_95_high !== null && candidate.ci_95_high !== undefined
        ? `[${numberText(candidate.ci_95_low)}, ${numberText(candidate.ci_95_high)}]`
        : "collecting",
    maxDrawdown: candidate.max_drawdown ?? "collecting",
    fillModelAgreement: candidate.fill_model_agreement ?? "pending sensitivity",
    dataQuality: candidate.data_quality ?? "unknown",
    recommendation: candidate.recommendation ?? "collect more settled markets",
    lastUpdated: candidate.last_updated ?? "not run",
    explanation: candidate.explanation ?? `${candidate.candidate} is collecting prospective evidence.`,
    latest
  }));
}

function candidatePnl(row: ProspectiveValidationRow | undefined, candidate: string) {
  if (!row) {
    return "collecting";
  }
  if (candidate.includes("dynamic_quote_style")) {
    return row.dynamic_quote_style_net_pnl;
  }
  if (candidate.includes("full_deterministic_profile")) {
    return row.full_deterministic_profile_net_pnl;
  }
  if (candidate.includes("dynamic_safety_only")) {
    return row.dynamic_safety_only_net_pnl;
  }
  return row.static_net_pnl;
}

function candidateDelta(row: ProspectiveValidationRow | undefined, candidate: string) {
  if (!row) {
    return "collecting";
  }
  if (candidate.includes("dynamic_quote_style")) {
    return row.dynamic_quote_style_paired_delta;
  }
  if (candidate.includes("full_deterministic_profile")) {
    return row.full_deterministic_profile_paired_delta;
  }
  if (candidate.includes("dynamic_safety_only")) {
    return row.dynamic_safety_only_paired_delta;
  }
  return "baseline";
}

function decisionGateForCandidate(row: ProspectiveValidationRow | undefined, candidate: string) {
  if (!row) {
    return "RESEARCH_ONLY";
  }
  if (candidate.includes("dynamic_quote_style")) {
    return row.dynamic_quote_style_decision_gate ?? row.decision_gate ?? "RESEARCH_ONLY";
  }
  if (candidate.includes("full_deterministic_profile")) {
    return row.full_deterministic_profile_decision_gate ?? "RESEARCH_ONLY";
  }
  if (candidate.includes("dynamic_safety_only")) {
    return row.dynamic_safety_only_decision_gate ?? "RESEARCH_ONLY";
  }
  return "BASELINE_CONTROL";
}

function gateTone(gate: string | null | undefined) {
  if (gate === "PAPER_SHADOW_OK") {
    return "good";
  }
  if (gate === "REJECT") {
    return "danger";
  }
  return "warn";
}

function statusForCandidate(row: ProspectiveValidationRow | undefined, candidate: string, pnl: unknown) {
  if (!row) {
    return "collecting";
  }
  if (decisionGateForCandidate(row, candidate) === "REJECT") {
    return "rejected_by_paired_evidence";
  }
  if (row.data_quality_status && row.data_quality_status !== "healthy") {
    return "blocked";
  }
  const best = Math.max(
    numeric(row.static_net_pnl),
    numeric(row.dynamic_quote_style_net_pnl),
    numeric(row.full_deterministic_profile_net_pnl),
    numeric(row.dynamic_safety_only_net_pnl)
  );
  if (Number.isFinite(best) && numeric(pnl) === best) {
    return "candidate_leader";
  }
  return candidate.includes("static") ? "baseline" : "needs_more_evidence";
}

function explanationForCandidate(name: string, status: string, recommendation: string, row: ProspectiveValidationRow | undefined) {
  if (!row) {
    return `${name} has no prospective validation row yet. Run prospective validation before using it for research conclusions.`;
  }
  if (status === "blocked") {
    return `${name} is blocked by ${row.data_quality_status} data quality. The recommendation should not be trusted until the quality issue is resolved.`;
  }
  if (status.includes("rejected")) {
    return `${name} is rejected by paired prospective evidence for this frozen run. It remains research-only and cannot be used for live deployment.`;
  }
  return `${name} is ${status}. Recommendation: ${recommendation}. Evidence uses ${numberText(row.settled_markets, 0)} settled markets, drawdown ${numberText(row.max_drawdown)}, and CI [${numberText(row.ci_95_low)}, ${numberText(row.ci_95_high)}].`;
}

function recommendationText(row: ProspectiveValidationRow | undefined) {
  if (!row) {
    return "collect more settled markets";
  }
  const low = numeric(row.ci_95_low);
  const high = numeric(row.ci_95_high);
  if (low <= 0 && high >= 0) {
    return "evidence inconclusive; continue paper validation";
  }
  return low > 0 ? "candidate positive under current evidence" : "candidate negative under current evidence";
}

function numeric(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function GenericReport({
  title,
  rows,
  columns,
  emptyLabel
}: {
  title: string;
  rows: JsonRecord[];
  columns: ReportColumn[];
  emptyLabel: string;
}) {
  return (
    <Panel>
      <PanelHeader title={title} meta={`${rows.length} rows`} />
      {rows.length ? (
        <div className="overflow-auto">
          <table className="w-full min-w-[760px] text-left text-sm">
            <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
              <tr>{columns.map((column) => <th key={column.key} className="px-3 py-2">{column.label}</th>)}</tr>
            </thead>
            <tbody>
              {rows.slice(0, 100).map((row, index) => (
                <tr key={index} className="border-b border-line last:border-b-0">
                  {columns.map((column) => <td key={column.key} className="px-3 py-2">{compact(row[column.key])}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <EmptyState label={emptyLabel} />
      )}
    </Panel>
  );
}

function SampleSizePanel({ report }: { report?: JsonRecord | null }) {
  const stats = sampleSizeStats(report);
  const ci = confidenceIntervalText(stats);
  const requiredN = firstDefined(
    stats?.required_n_to_detect_observed_mean,
    stats?.required_n_for_plus_minus_0_10,
    stats?.required_n_for_plus_minus_0_05,
    stats?.required_n,
    stats ? "insufficient data" : undefined
  );
  return (
    <Panel>
      <PanelHeader title="Sample Size" meta="market-level confidence" />
      {stats ? (
        <>
          <div className="grid gap-3 p-4 md:grid-cols-4">
            <Metric label="N" value={firstDefined(stats.n, stats.sample_count, stats.markets, "waiting for report")} />
            <Metric label="Mean" value={firstDefined(stats.mean, "insufficient data")} />
            <Metric label="95% CI" value={ci} />
            <Metric label="Required N" value={requiredN} />
          </div>
          <div className="border-t border-line px-4 py-3 text-sm text-ink/70">
            {ci === "insufficient data"
              ? "The sample exists, but there is not enough variance/evidence to compute a stable confidence interval yet."
              : "Confidence interval is computed from settled market-level research outcomes."}
          </div>
        </>
      ) : (
        <EmptyState label="No sample-size statistics found. Run the daily research job or check research artifact access." />
      )}
    </Panel>
  );
}

function ArtifactsPanel({ artifacts, loading }: { artifacts: { artifact_id: string; path: string; kind: string }[]; loading: boolean }) {
  const [selected, setSelected] = useState<{ artifact_id: string; path: string; kind: string } | null>(null);
  const artifact = useQuery({
    queryKey: ["labs", "artifact", selected?.artifact_id],
    queryFn: () => getLabArtifact(selected?.artifact_id ?? ""),
    enabled: Boolean(selected?.artifact_id),
    retry: false
  });
  return (
    <Panel>
      <PanelHeader title="Artifacts" meta={`${artifacts.length} files`} />
      {artifacts.length ? (
        <div className="overflow-auto">
          <table className="w-full min-w-[640px] text-left text-sm">
            <tbody>
              {artifacts.slice(0, 100).map((artifact) => (
                <tr key={artifact.artifact_id} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2 font-mono text-xs">
                    <button className="text-left text-good hover:underline" onClick={() => setSelected(artifact)}>
                      {artifact.path}
                    </button>
                  </td>
                  <td className="px-3 py-2">{artifact.kind}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <EmptyState label={loading ? "Loading artifacts" : "No artifacts found"} />
      )}
      {selected ? (
        <ArtifactPreview artifact={artifact.data ?? null} loading={artifact.isLoading} error={artifact.error?.message} />
      ) : null}
    </Panel>
  );
}

function ArtifactPreview({
  artifact,
  loading,
  error
}: {
  artifact: LabArtifactPayload | null;
  loading: boolean;
  error?: string;
}) {
  return (
    <div className="border-t border-line bg-panel p-3">
      <div className="mb-2 text-xs font-semibold uppercase text-ink/50">
        {artifact?.path ?? (loading ? "Loading artifact" : "Artifact")}
      </div>
      {error ? <div className="text-sm text-danger">{error}</div> : null}
      {!error && loading ? <div className="text-sm text-ink/55">Loading artifact</div> : null}
      {!error && artifact ? (
        <pre className="max-h-96 overflow-auto border border-line bg-white p-3 text-xs leading-relaxed text-ink/75">
          {artifact.kind === "json" ? JSON.stringify(artifact.content, null, 2) : String(artifact.content ?? "")}
        </pre>
      ) : null}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="border border-line bg-panel px-3 py-3">
      <div className="truncate text-xs text-ink/50">{label}</div>
      <div className="mt-1 truncate text-lg font-semibold text-ink">{numberText(value)}</div>
    </div>
  );
}

function candidateRows(value: unknown): JsonRecord[] {
  const record = asRecord(value);
  return Array.isArray(record?.candidates) ? (record.candidates.filter(Boolean) as JsonRecord[]) : [];
}

function fallbackCandidates(): JsonRecord[] {
  return ["static_baseline", "dynamic_quote_style", "full_deterministic_profile", "dynamic_safety_only"].map((name) => ({ name, profile: name }));
}

function pointer(record: unknown, path: string): unknown {
  return path
    .split("/")
    .slice(1)
    .reduce<unknown>((current, key) => asRecord(current)?.[key], record);
}

function sampleSizeStats(report: unknown): JsonRecord | undefined {
  return (
    asRecord(pointer(report, "/result/statistics")) ??
    asRecord(pointer(report, "/statistics")) ??
    asRecord(pointer(report, "/report/result/statistics")) ??
    asRecord(pointer(report, "/sample_size/result/statistics")) ??
    asRecord(report)
  ) ?? undefined;
}

function confidenceIntervalText(stats: JsonRecord | undefined) {
  if (!stats) {
    return "waiting for report";
  }
  const low = firstDefined(stats.ci_low, stats.ci_95_low, stats.lower_ci);
  const high = firstDefined(stats.ci_high, stats.ci_95_high, stats.upper_ci);
  if (low === undefined || high === undefined || low === null || high === null) {
    return "insufficient data";
  }
  return `${numberText(low)} / ${numberText(high)}`;
}

function firstDefined(...values: unknown[]) {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : null;
}
