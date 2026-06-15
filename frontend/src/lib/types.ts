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
  running?: boolean;
  execution_name?: string | null;
  execution_id?: string | null;
  research_only?: boolean;
  live_trading_enabled?: boolean;
};

export type ProspectiveValidationRow = {
  date: string;
  settled_markets?: number | string | null;
  fill_model?: string | null;
  static_net_pnl?: string | number | null;
  dynamic_quote_style_net_pnl?: string | number | null;
  full_deterministic_profile_net_pnl?: string | number | null;
  dynamic_safety_only_net_pnl?: string | number | null;
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
