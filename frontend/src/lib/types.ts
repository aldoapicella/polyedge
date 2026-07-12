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

export type VenueExecutionEvidence = {
  generated_ts: string;
  queue_position_source: "authenticated_lifecycle_plus_public_l2" | string;
  queue_position_metric: "inferred_size_ahead" | string;
  literal_fifo_rank_available: false;
  practical_target: string;
  remaining_limitation: string;
  research_only: boolean;
  strategy_promotion_allowed: boolean;
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
    order_submitted?: boolean;
    execution_origin?: string;
    execution_country?: string | null;
    static_egress_verified?: boolean;
    campaign_enabled?: boolean;
    submitted_order_count?: number;
    completed_probe_count?: number;
    stop_reason?: string;
    risk_at_end?: {
      conservative_loss_budget_consumed?: number;
      submitted_orders?: number;
      filled_orders?: number;
      unresolved_risk_reservations?: number;
      global_unresolved_risk_reservations?: number;
    };
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
      observation_delay_ms?: number;
    }>;
    portfolio?: VenuePortfolioSnapshot | null;
    remaining_literal_fifo_limitations?: string[];
  } | null;
  latest_attempt?: {
    run_id?: string;
    status?: string;
    finished_ts?: string;
    error?: string;
    order_submitted?: boolean;
    portfolio?: VenuePortfolioSnapshot | null;
    risk_at_end?: {
      unresolved_risk_reservations?: number;
      global_unresolved_risk_reservations?: number;
    };
  } | null;
  preflight?: {
    run_id?: string;
    status?: string;
    finished_ts?: string;
    order_submitted?: boolean;
    portfolio?: VenuePortfolioSnapshot | null;
    risk_at_end?: {
      unresolved_risk_reservations?: number;
      global_unresolved_risk_reservations?: number;
    };
  } | null;
  model?: {
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
  result?: {
    status?: string;
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
