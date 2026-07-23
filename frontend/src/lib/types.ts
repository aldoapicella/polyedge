export type JsonRecord = Record<string, unknown>;

export type RuntimeEvent = {
  type: string;
  ts: string;
  data: JsonRecord;
};

export type ControlStatus = {
  paused: boolean;
  paused_at?: string | null;
  pause_reason?: string | null;
};

export type Snapshot = {
  status: StatusPayload;
  current_market: MarketSummary | null;
  markets: MarketSummary[];
  open_orders: OpenOrder[];
  fills: ExecutionReport[];
  latest_decisions: TradeDecision[];
  latest_execution_reports: ExecutionReport[];
};

export type StatusPayload = {
  app: string;
  execution_mode: "paper" | "live";
  started_at: string;
  now: string;
  markets: number;
  tradeable_markets: number;
  books: number;
  tracked_open_orders: number;
  control?: ControlStatus;
  kill_switch?: boolean;
  paper_fill?: PaperFillStatus | null;
  live_heartbeat_paused?: boolean;
  live_heartbeat?: JsonRecord | null;
  recorder?: JsonRecord | null;
  reference?: ReferencePrice | null;
  reports?: {
    running_job?: ReportJob | null;
    known_jobs?: number;
    store?: JsonRecord;
  };
  latest_decisions?: TradeDecision[];
  latest_execution_reports?: ExecutionReport[];
};

export type PaperFillStatus = {
  paper_maker_fills?: number;
  paper_open_resting_orders?: number;
  [key: string]: unknown;
};

export type ReferencePrice = {
  source: string;
  price: string;
  source_ts: string;
  local_ts: string;
  latency_ms: number;
  stale: boolean;
  exact_resolution_source: boolean;
  quality_flags: string[];
};

export type BookLevel = {
  price: string;
  size: string;
};

export type BookState = {
  token_id: string;
  bids: BookLevel[];
  asks: BookLevel[];
  last_trade_price?: string | null;
  exchange_ts?: string | null;
  local_ts: string;
  book_hash?: string | null;
};

export type MarketSummary = {
  market_id: string;
  market_slug?: string | null;
  question: string;
  condition_id: string;
  up_token_id: string;
  down_token_id: string;
  start_ts: string;
  end_ts: string;
  start_price?: string | null;
  status: string;
  is_active: boolean;
  is_tradeable: boolean;
  fair_value?: FairValue | null;
  chart_summary?: {
    market_id: string;
    sample_count: number;
    first_sample_ts?: string | null;
    last_sample_ts?: string | null;
    start_price?: string | null;
    q_up?: string | null;
    q_down?: string | null;
    fair_value_ts?: string | null;
  } | null;
};

export type FairValue = {
  market_id: string;
  q_up: string;
  q_down: string;
  sigma: number;
  drift_mu: number;
  model_error: string;
  computed_ts: string;
};

export type TradeDecision = {
  action: string;
  market_id: string;
  token_id?: string | null;
  outcome?: string | null;
  side?: string | null;
  price?: string | null;
  size?: string | null;
  reason: string;
  expected_edge?: string | null;
};

export type ExecutionReport = {
  order_id?: string | null;
  market_id: string;
  token_id?: string | null;
  status: string;
  filled_size: string;
  avg_price?: string | null;
  fee: string;
  local_ts: string;
  raw?: JsonRecord;
};

export type OpenOrder = {
  market_id: string;
  token_id: string;
  side: string;
  placed_ts: string;
  expires_at?: string | null;
  order_id?: string | null;
  decision: TradeDecision;
};

export type ReportJob = {
  job_id: string;
  status: string;
  source?: string;
  prefix?: string | null;
  date?: string | null;
  created_ts?: string;
  started_ts?: string | null;
  finished_ts?: string | null;
  error?: string | null;
};

export type MarketDetail = {
  market: MarketSummary;
  fair_value?: FairValue | null;
  books: {
    up?: BookState | null;
    down?: BookState | null;
  };
  decisions: TradeDecision[];
  execution_reports: ExecutionReport[];
};

export type ReportPayload = {
  job?: ReportJob;
  report?: {
    summary?: JsonRecord;
    actual_paper?: JsonRecord;
    replay_estimate?: JsonRecord;
    runtime_vs_replay?: JsonRecord;
    market_level_statistics?: JsonRecord;
    report_job?: ReportJob;
    report_metadata?: JsonRecord;
    [key: string]: unknown;
  } | null;
};

export type LabArtifact = {
  artifact_id: string;
  path: string;
  kind: "json" | "md" | string;
  size_bytes?: number | null;
  modified_ts?: string | null;
};

export type LabArtifactPayload = {
  path: string;
  kind: "json" | "markdown" | string;
  content: unknown;
};

export type LabReportBundle = {
  date?: string;
  report?: JsonRecord | null;
  audit?: JsonRecord | null;
  baseline?: JsonRecord | null;
  regimes?: JsonRecord | null;
  calibration?: JsonRecord | null;
  sample_size?: JsonRecord | null;
  artifacts?: LabArtifact[];
  detail?: string;
};

export type LabSummary = {
  generated_ts: string;
  date?: string | null;
  status?: string | null;
  recommendation?: unknown;
  sample_size?: JsonRecord | null;
  data_quality?: string | null;
  candidate_count?: number;
  prospective_rows?: number;
  research_only?: boolean;
  live_deployment_allowed?: boolean;
};

export type VenuePortfolioSnapshot = {
  status?: string;
  stage?: string;
  captured_ts?: string;
  liquid_collateral?: number;
  current_position_value?: number;
  account_equity?: number;
  starting_capital?: number | null;
  account_net_pnl?: number | null;
  gross_redeemable_value?: number;
  resolved_position_cost?: number;
  resolved_position_net_pnl?: number;
  resolved_losing_cost?: number;
  redeemable_position_count?: number;
  redeemable_winner_count?: number;
  gross_payout_is_profit?: false;
};

export type VenueCampaignRisk = {
  campaign_id?: string;
  baseline_equity?: number;
  cash_flow_adjusted_baseline?: number;
  equity_floor?: number;
  max_campaign_drawdown?: number;
  account_equity?: number;
  campaign_drawdown?: number;
  projected_equity?: number;
  projected_campaign_drawdown?: number;
  proposed_notional?: number;
  account_reconciliation_discrepancy?: number;
  maximum_reconciliation_discrepancy?: number;
  open_order_count?: number;
  unresolved_position_count?: number;
  unresolved_risk_reservation_count?: number;
  blockers?: string[];
  passed?: boolean;
};

export type VenueRiskSnapshot = {
  campaign?: VenueCampaignRisk | null;
  campaign_gate?: {
    campaign_risk_ok?: boolean;
    diagnostics_only?: boolean;
    submission_allowed?: boolean;
    blockers?: string[];
  };
  daily_turnover?: {
    conservative_loss_budget_consumed?: number;
    submitted_orders?: number;
    filled_orders?: number;
    unresolved_risk_reservations?: number;
    global_unresolved_risk_reservations?: number;
  };
  primary_risk_source?: string;
  conservative_loss_budget_consumed?: number;
  submitted_orders?: number;
  filled_orders?: number;
  unresolved_risk_reservations?: number;
  global_unresolved_risk_reservations?: number;
};

export type LabArtifactProvenance = {
  path?: string;
  source?: "funded_evidence" | "profitability_shadow" | "trained_model_storage" | "research_conservative_prior" | "api_fallback" | string;
  trust_scope?: "funded_control" | "shadow_research" | "none" | string;
  available?: boolean;
  schema_version?: string | number | null;
  valid_current_schema?: boolean;
  authoritative_ts?: string | null;
  authoritative_ts_field?: string | null;
  age_seconds?: number | null;
  freshness_window_seconds?: number;
  freshness?: "fresh" | "stale" | "unknown" | "unavailable" | string;
  fresh?: boolean;
  control_valid?: boolean;
  expires_at?: string | null;
  expired?: boolean;
  canonical_funded_state?: boolean;
  promotion_ready?: false;
  legacy_eligibility?: "current_schema" | "current_protocol" | "requires_full_validator" | "display_only_legacy" | "display_only_fallback" | "unknown_display_only" | "conservative_prior_only" | "not_applicable" | "unavailable" | string;
  validation_error?: string | null;
};

export type ProfitabilityArtifactProvenance = {
  selected_source?: "funded_evidence" | "profitability_shadow" | "api_fallback" | string;
  selection_reason?: "canonical_funded_state" | "newest_fresh_valid_manifest" | "newest_valid_manifest" | "legacy_display_only" | string;
  canonical_funded_state?: boolean;
  promotion_ready?: false;
  selected?: LabArtifactProvenance;
  candidates?: LabArtifactProvenance[];
};

export type VenueEvidenceEligibility = {
  required_protocol_version?: number;
  observed_protocol_version?: number | null;
  exact_protocol_version?: boolean;
  legacy?: boolean;
  legacy_eligibility?: "requires_full_validator" | "display_only_legacy" | string;
  counts_toward_protocol_evidence?: false;
  aggregate_promotion_ready?: false;
  validation_status?: "terminal_binding_and_full_protocol_v3_validation_required" | "ineligible_protocol_version" | string;
  reasons?: string[];
};

export type ShadowCorrectionState = {
  schema_version: 1 | number;
  campaign_id: string;
  correction_id: string;
  from: string;
  through: string;
  reason: string;
  status: "in_progress" | "failed" | "complete" | string;
  builder_git_sha?: string | null;
  started_at: string;
  completed_at?: string | null;
};

export type ShadowCorrectionVisibility = {
  journal_path: "reports/research/shadow/corrections/active.json" | string;
  available: boolean;
  status: "none" | "in_progress" | "failed" | "complete" | "invalid" | "unavailable" | string;
  blocks_promotion: boolean;
  decision: "NO-GO" | "ELIGIBILITY_UNCHANGED" | string;
  blocker?: string | null;
  validation_error: boolean;
  state?: ShadowCorrectionState | null;
};

export type VenueExecutionEvidence = {
  generated_ts: string;
  queue_position_source: "authenticated_lifecycle_plus_public_l2" | string;
  queue_position_metric: "inferred_size_ahead" | string;
  literal_fifo_rank_available: false;
  practical_target: string;
  remaining_limitation: string;
  research_only: boolean;
  strategy_promotion_allowed: boolean;
  correction?: ShadowCorrectionVisibility;
  promotion_decision?: "NO-GO" | "ELIGIBILITY_UNCHANGED" | string;
  promotion_blocker?: string | null;
  artifact_provenance?: {
    profitability?: ProfitabilityArtifactProvenance;
    latest?: LabArtifactProvenance;
    latest_attempt?: LabArtifactProvenance;
    preflight?: LabArtifactProvenance;
    redemption?: LabArtifactProvenance;
    model?: LabArtifactProvenance;
  };
  profitability?: {
    generated_at?: string;
    created_at?: string;
    expires_at?: string;
    phase?: "frozen" | "risk_repair" | "shadow_collecting" | "shadow_passed" | "evidence_collecting" | "canary_ready" | "limited_live" | "profitable_go" | "stopped_no_go" | string;
    pre_correction_phase?: string;
    status?: string;
    pre_correction_status?: string;
    effective_decision?: "NO-GO" | string;
    candidate?: {
      name?: string;
      version?: string;
      candidate_version?: string;
      config_hash?: string;
    };
    execution_model?: {
      blob_uri?: string;
      sha256?: string;
      model_version?: string;
    };
    funded_ladder?: {
      schema_version?: string;
      campaign_id?: string;
      phase?: "evidence_collecting" | "limited_live" | "profitable_go" | "stopped_no_go" | string;
      active_stage_index?: number;
      active_target_orders?: number;
      completed_checkpoints?: number[];
      human_grant_required?: boolean;
      stage_authorized?: boolean;
      terminal?: boolean;
      promotion_allowed?: false;
      maximum_calendar_days?: number;
      maximum_funded_orders?: number;
      metrics?: {
        observed_calendar_days?: number;
        cumulative_eligible_orders?: number;
        cumulative_funded_orders?: number;
        cumulative_net_pnl?: number;
        cumulative_max_drawdown?: number;
        mean_net_markout_30s?: number;
        net_markout_30s_lower_95?: number;
        markout_sample_size?: number;
        data_quality_passed?: boolean;
        unresolved_exposure?: number;
      };
      queue_model_transition?: {
        schema_version?: string;
        binding?: {
          blob_uri?: string;
          sha256?: string;
          model_version?: string;
        };
        generated_at?: string;
        training_cutoff?: string;
        training_dataset_sha256?: string;
        training_checkpoint_sha256?: string;
        model_quality_passed?: boolean;
      };
      holdout_evaluation?: {
        schema_version?: string;
        exact_order_count?: number;
        label_sample_size?: number;
        filled_order_count?: number;
        non_filled_order_count?: number;
        brier_improvement_fraction?: number;
        expected_calibration_error?: number;
        markout_sample_size?: number;
        mean_net_markout_30s?: number;
        net_markout_30s_lower_95?: number;
        holdout_net_pnl?: number;
        holdout_max_drawdown?: number;
        mean_holdout_net_pnl_per_order?: number;
        holdout_net_pnl_per_order_lower_95?: number;
        passed?: boolean;
      };
    };
    blocking_reason?: string | null;
    capital?: {
      original_starting_capital?: number;
      campaign_starting_equity?: number;
      current_equity?: number;
      campaign_net_pnl?: number;
      lifetime_net_pnl?: number;
      equity_floor?: number;
      max_campaign_drawdown?: number;
      current_drawdown?: number;
      locked_principal?: number;
      unresolved_exposure?: number;
      external_deposits?: number;
      withdrawals?: number;
    };
    shadow?: {
      clean_days?: number;
      required_clean_days?: number;
      settled_markets?: number;
      required_settled_markets?: number;
      wallet_constrained_net_pnl?: number;
      queue_conservative_net_pnl?: number;
      pnl_ci_lower_95?: number;
      positive_weekly_blocks?: number;
      required_positive_weekly_blocks?: number;
      max_drawdown?: number;
      decision_parity_rate?: number;
    };
    data_quality?: {
      status?: string;
      decision_grade_coverage?: number;
      minimum_coverage?: number;
      fatal_warnings?: number;
      blocking_warnings?: number;
      unclassified_warnings?: number;
      coverage_breakdown?: {
        start_price_capture_rate?: string | number | null;
        settlement_rate?: string | number | null;
        exact_reference_hour_coverage?: string | number | null;
        decision_metadata_coverage?: string | number | null;
        decision_grade_coverage?: string | number | null;
        final_decision_grade_coverage?: string | number | null;
        execution_field_coverage?: string | number | null;
        decision_parity_rate?: string | number | null;
        queue_snapshot_coverage?: string | number | null;
        queue_snapshot_applicable?: boolean | null;
        markout_1s_completion?: string | number | null;
        markout_1s_applicable?: boolean | null;
        markout_5s_completion?: string | number | null;
        markout_5s_applicable?: boolean | null;
        markout_30s_completion?: string | number | null;
        markout_30s_applicable?: boolean | null;
      };
    };
    gates?: Record<string, {
      passed?: boolean;
      status?: string;
      actual?: string | number | boolean | null;
      required?: string | number | boolean | null;
      reason?: string;
    }>;
    gate_metrics?: {
      promotion_allowed?: boolean;
      gates?: Array<{
        gate?: string;
        status?: string;
        actual?: string;
        required?: string;
      }>;
      metrics?: {
        observed_calendar_days?: number;
        clean_days?: number;
        settled_markets?: number;
        wallet_constrained_net_pnl?: string | number;
        queue_conservative?: boolean;
        queue_conservative_net_pnl?: string | number;
        pnl_ci_95_low?: string | number;
        consecutive_positive_weekly_blocks?: number;
        max_drawdown?: string | number;
        markout_30s_ci_low?: string | number;
        replay_runtime_parity?: boolean;
        decision_parity_rate?: string | number;
        missing_metrics?: string[];
        data_quality?: {
          registry_version?: string;
          total_events?: number;
          decision_grade_coverage?: string | number;
          coverage_breakdown?: {
            start_price_capture_rate?: string | number | null;
            settlement_rate?: string | number | null;
            exact_reference_hour_coverage?: string | number | null;
            decision_metadata_coverage?: string | number | null;
            decision_grade_coverage?: string | number | null;
            final_decision_grade_coverage?: string | number | null;
            execution_field_coverage?: string | number | null;
            decision_parity_rate?: string | number | null;
            queue_snapshot_coverage?: string | number | null;
            queue_snapshot_applicable?: boolean | null;
            markout_1s_completion?: string | number | null;
            markout_1s_applicable?: boolean | null;
            markout_5s_completion?: string | number | null;
            markout_5s_applicable?: boolean | null;
            markout_30s_completion?: string | number | null;
            markout_30s_applicable?: boolean | null;
          };
          fatal_issues?: string[];
          warnings?: Array<{
            message?: string;
            rule_id?: string;
            severity?: "informational" | "blocking" | string;
            known?: boolean;
          }>;
        };
      };
    };
    promotion_allowed?: false;
    human_authorization_required?: true;
  } | null;
  redemption?: {
    run_id?: string;
    status?: string;
    finished_ts?: string;
    dry_run?: boolean;
    redemption_enabled?: boolean;
    redemption_submitted?: boolean;
    wallet_type?: string;
    derived_wallet_match?: boolean;
    liquid_collateral_before?: number;
    liquid_collateral_after?: number;
    realized_payout?: number;
    transaction_hash?: string | null;
    zero_open_orders_confirmed?: boolean;
    portfolio?: VenuePortfolioSnapshot | null;
    recent_redemptions?: Array<{
      transaction_hash?: string | null;
      condition_id?: string | null;
      title?: string | null;
      gross_payout?: number;
      redeemed_ts?: string | null;
      attribution?: "azure_redemption_worker" | "external_or_manual" | string;
    }>;
    selection?: {
      selected_gross_payout?: number;
      available_winner_conditions?: number;
      skipped_winner_conditions?: number;
      selected?: Array<{
        condition_id?: string;
        gross_payout?: number;
        negative_risk?: boolean;
        titles?: string[];
      }>;
    };
    planned_calls?: Array<{
      purpose?: string;
      target?: string;
      condition_id?: string | null;
    }>;
  } | null;
  latest?: {
    evidence_protocol_version?: number;
    run_id?: string;
    status?: string;
    started_ts?: string;
    finished_ts?: string;
    order_submission_attempted?: boolean;
    order_submitted?: boolean;
    execution_origin?: string;
    execution_country?: string | null;
    static_egress_verified?: boolean;
    campaign_enabled?: boolean;
    submitted_order_count?: number;
    completed_probe_count?: number;
    stop_reason?: string;
    risk_at_end?: VenueRiskSnapshot;
    order?: {
      price?: number;
      size?: number;
      notional?: number;
      inferredSizeAhead?: number;
      samePricePublicSize?: number;
      betterPricePublicSize?: number;
      spread?: number | null;
    } | null;
    lifecycle?: {
      order_id?: string;
      client_to_http_ack_ms?: number | null;
      client_cancel_round_trip_ms?: number | null;
      client_to_user_cancel_ack_ms?: number | null;
      actual_matched_size?: number;
      partial_fill?: boolean;
      fully_filled?: boolean;
      fill_raced_cancellation?: boolean;
      public_touch_trade_count?: number;
      public_strict_trade_through_count?: number;
      public_trade_through_without_fill_count?: number;
      venue_status?: string;
      planned_rest_seconds?: number;
      reconciliation_complete?: boolean;
      zero_open_orders_confirmed?: boolean;
      data_gap_detected?: boolean;
      cancellation_failure?: boolean;
      markout_capture_complete?: boolean;
      matched_size_source_agreement?: boolean;
      trade_id_source_agreement?: boolean;
      related_trade_ids?: string[];
      live_user_trade_ids?: string[];
      authenticated_user_channel_reconnects?: number;
      public_market_channel_reconnects?: number;
    } | null;
    markouts?: Array<{
      fill_id?: string;
      fill_size?: number;
      horizon_seconds?: number;
      midpoint_markout_per_share?: number | null;
      executable_markout_per_share?: number | null;
      fee_per_share?: number | null;
      entry_fee_per_share?: number | null;
      hypothetical_exit_fee_per_share?: number | null;
      round_trip_fee_per_share?: number | null;
      net_midpoint_markout_per_share?: number | null;
      net_executable_markout_per_share?: number | null;
      observation_delay_ms?: number;
    }>;
    portfolio?: VenuePortfolioSnapshot | null;
    remaining_literal_fifo_limitations?: string[];
    evidence_eligibility?: VenueEvidenceEligibility;
  } | null;
  latest_attempt?: {
    run_id?: string;
    status?: string;
    finished_ts?: string;
    error?: string;
    order_submitted?: boolean;
    portfolio?: VenuePortfolioSnapshot | null;
    risk_at_end?: VenueRiskSnapshot;
  } | null;
  preflight?: {
    run_id?: string;
    status?: string;
    finished_ts?: string;
    order_submitted?: boolean;
    portfolio?: VenuePortfolioSnapshot | null;
    risk_at_end?: VenueRiskSnapshot;
  } | null;
  model?: {
    model_version?: string;
    evidence_protocol_version?: number;
    generated_at?: string;
    status?: string;
    sample_size?: number;
    label_sample_size?: number;
    positive_fills?: number;
    minimum_samples?: number;
    train_size?: number;
    test_size?: number;
    train_label_size?: number;
    test_label_size?: number;
    out_of_sample_brier_score?: number;
    naive_horizon_base_rate_brier_score?: number;
    brier_improvement_fraction?: number;
    brier_improvement_percent?: number;
    expected_calibration_error?: number;
    maximum_expected_calibration_error?: number;
    temporal_split?: string;
    reason?: string;
    promotion_allowed?: boolean;
    promotion_ready?: boolean;
    promotion_block_reason?: string;
    negative_non_fills?: number;
    excluded_observations?: number;
    legacy_protocol_observations?: number;
    net_markout_30s_sample_size?: number;
    mean_net_executable_markout_30s_per_share?: number;
    net_executable_markout_30s_lower_confidence_bound_95?: number | null;
    markout_confidence_method?: string;
    quality_gates?: {
      passed?: boolean;
      eligible_observations?: number;
      excluded_observations?: number;
      data_gap_observations?: number;
      eligible_data_gap_observations?: number;
      excluded_data_gap_observations?: number;
      cancellation_failure_observations?: number;
      filled_observations?: number;
      complete_markout_observations?: number;
      early_markout_observations?: number;
      legacy_protocol_observations?: number;
    };
    horizon_metrics?: Record<string, {
      sample_size?: number;
      positive_fills?: number;
      brier_score?: number;
    }>;
  } | null;
};

export type LabCandidateEvidence = {
  candidate: string;
  profile?: string | null;
  candidate_version?: string | null;
  config_hash?: string | null;
  frozen_since?: string | null;
  reason?: string | null;
  status?: string | null;
  latest_test_pnl?: string | number | null;
  paired_delta?: string | number | null;
  decision_gate?: string | null;
  ci_95_low?: string | number | null;
  ci_95_high?: string | number | null;
  max_drawdown?: string | number | null;
  fill_model_agreement?: string | null;
  data_quality?: string | null;
  recommendation?: string | null;
  last_updated?: string | null;
  explanation?: string | null;
  notes?: string | null;
  research_only?: boolean;
  enabled_by_default?: boolean;
  deployment_allowed?: boolean;
  live_deployment_allowed?: boolean;
};

export type LabDataQuality = {
  generated_ts: string;
  freshness?: JsonRecord | null;
  recorder?: JsonRecord | null;
  exclusions?: ExclusionRegistry;
  source?: JsonRecord;
};

export type ExclusionRegistry = {
  version: number;
  updated_at?: string | null;
  windows: ExclusionWindow[];
  error?: string;
};

export type ExclusionWindow = {
  id: string;
  start: string;
  end: string;
  end_exclusive?: string;
  reason: string;
  evidence?: string[];
  default_exclude: boolean;
};

export type LabJob = {
  job_id: string;
  job_name: string;
  status: string;
  trigger?: string;
  cron?: string | null;
  last_start?: string | null;
  last_finish?: string | null;
  duration?: string | number | null;
  exit_code?: number | null;
  output_artifact?: string | null;
  error?: string | null;
  data_quality?: string | null;
  running?: boolean;
  runnable?: boolean;
  detail?: string | null;
  execution_name?: string | null;
  execution_id?: string | null;
  research_only?: boolean;
  live_trading_enabled?: boolean;
};

export type JobExecution = {
  execution_name?: string | null;
  execution_id?: string | null;
  status: string;
  last_start?: string | null;
  last_finish?: string | null;
  duration?: number | string | null;
  running?: boolean;
  exit_code?: number | string | null;
  error?: string | null;
};

export type JobExecutionLogPayload = {
  job_id: string;
  job_name?: string;
  execution_id: string;
  logs: string[];
  log_rows?: JsonRecord[];
  artifacts?: string[];
  source?: string;
  detail?: string;
};

export type ProspectiveValidationRow = {
  date: string;
  settled_markets?: number | string | null;
  fill_model?: string | null;
  static_net_pnl?: string | number | null;
  dynamic_quote_style_net_pnl?: string | number | null;
  full_deterministic_profile_net_pnl?: string | number | null;
  dynamic_safety_only_net_pnl?: string | number | null;
  dynamic_quote_style_paired_delta?: string | number | null;
  full_deterministic_profile_paired_delta?: string | number | null;
  dynamic_safety_only_paired_delta?: string | number | null;
  best_candidate_paired_delta?: string | number | null;
  decision_gate?: string | null;
  dynamic_quote_style_decision_gate?: string | null;
  full_deterministic_profile_decision_gate?: string | null;
  dynamic_safety_only_decision_gate?: string | null;
  max_drawdown?: string | number | null;
  cancel_per_fill?: string | number | null;
  ci_95_low?: string | number | null;
  ci_95_high?: string | number | null;
  data_quality_status?: string | null;
  recommendation?: string | null;
};

export type ProspectiveValidation = {
  generated_at?: string;
  generated_ts?: string;
  correction?: ShadowCorrectionVisibility;
  promotion_decision?: "NO-GO" | "ELIGIBILITY_UNCHANGED" | string;
  promotion_blocker?: string | null;
  promotion_allowed?: boolean;
  result?: {
    status?: string;
    pre_correction_status?: string;
    decision?: "NO-GO" | string;
    blocker?: string | null;
    promotion_allowed?: boolean;
    since?: string;
    rows?: ProspectiveValidationRow[];
    frozen_candidates?: JsonRecord;
    research_only?: boolean;
    live_deployment_allowed?: boolean;
  };
};

export type QueryFilter = {
  field: string;
  op: "eq" | "ne" | "contains" | "gt" | "gte" | "lt" | "lte" | "in";
  value: unknown;
};

export type QuerySort = {
  field: string;
  direction: "asc" | "desc";
};

export type QueryRequest = {
  dataset: string;
  filters?: QueryFilter[];
  group_by?: string[];
  metrics?: string[];
  sort?: QuerySort[];
  limit?: number;
  offset?: number;
};

export type QueryColumn = {
  field: string;
  label: string;
  kind: "number" | "boolean" | "datetime" | "text" | string;
  help?: string;
};

export type QueryResult = {
  dataset: string;
  columns: QueryColumn[];
  rows: JsonRecord[];
  total_rows: number;
  returned_rows: number;
  offset: number;
  limit: number;
  truncated: boolean;
  warnings?: string[];
  source?: JsonRecord;
};

export type QueryDatasetSchema = {
  id: string;
  label: string;
  filters: string[];
  group_by: string[];
  metrics: string[];
  default_limit: number;
  max_limit: number;
};

export type QuerySchema = {
  backend: string;
  structured_only: boolean;
  generated_ts: string;
  datasets: QueryDatasetSchema[];
  operators: QueryFilter["op"][];
  output_modes: string[];
  safety: JsonRecord;
};

export type QueryTemplate = {
  id: string;
  name: string;
  description?: string;
  request: QueryRequest;
  created_ts?: string;
  updated_ts?: string;
  owner?: string;
  tags?: string[];
};

export type DataQualityTimelineEvent = {
  ts: string;
  kind: string;
  status: string;
  title: string;
  detail?: JsonRecord;
};

export type JobLogPayload = {
  job_id: string;
  job_name?: string;
  logs: string[];
  artifacts?: string[];
  detail?: string;
};

export type RuntimeConfigSection = Record<string, string | number>;

export type RuntimeConfig = {
  strategy: RuntimeConfigSection;
  risk: RuntimeConfigSection;
  paper: RuntimeConfigSection;
  read_only: Record<string, boolean | string>;
};

export type RuntimeConfigPatch = {
  strategy?: Record<string, string | number>;
  risk?: Record<string, string | number>;
  paper?: Record<string, string | number>;
};

export type ConfigChange = {
  field: string;
  old: string | number | boolean | null;
  new: string | number | boolean | null;
};

export type ConfigValidation = {
  valid: boolean;
  issues: string[];
  changes: ConfigChange[];
  current: RuntimeConfig;
  proposed: RuntimeConfig;
};

export type ConfigAuditEntry = {
  version: string;
  category: string;
  action: string;
  actor?: string | null;
  source: string;
  reason?: string | null;
  created_ts: string;
  before: RuntimeConfig;
  after: RuntimeConfig;
  metadata?: JsonRecord;
};
