/**
 * utils.js — Color maps, regime labels, data adapters for TW Stock Hunter Dashboard
 */

// ─── Regime Mapping ──────────────────────────────────────────────────
const REGIME_MAP = {
  normal: '常態',
  caution: '警戒',
  stress: '壓力',
  crisis: '危機',
  black_swan: '黑天鵝',
  unknown: '未知',
  // Legacy names auto-mapped
  bull: '常態',
  choppy: '警戒',
  bear: '壓力',
  no_trade: '危機',
};

const REGIME_COLORS = {
  normal: '#3fb950',
  caution: '#d29922',
  stress: '#f0883e',
  crisis: '#f85149',
  black_swan: '#8957e5',
  unknown: '#484f58',
  bull: '#3fb950',
  choppy: '#d29922',
  bear: '#f0883e',
  no_trade: '#f85149',
};

const REGIME_LEVEL = {
  normal: 1,
  caution: 2,
  stress: 3,
  crisis: 4,
  black_swan: 5,
  unknown: 0,
  bull: 1,
  choppy: 2,
  bear: 3,
  no_trade: 4,
};

const REGIME_EMOJI = {
  normal: '🟢',
  caution: '🟡',
  stress: '🟠',
  crisis: '🔴',
  black_swan: '🟣',
  unknown: '⚪',
  bull: '🟢',
  choppy: '🟡',
  bear: '🟠',
  no_trade: '🔴',
};

const GLOBAL_RISK_MAP = {
  low: '低',
  moderate: '中等',
  high: '高',
  extreme: '極高',
  neutral: '中性',
};

const CONFIDENCE_MAP = {
  high: '高',
  medium: '中',
  low: '低',
};

// ─── Signal Fusion Dimensions ────────────────────────────────────────
const SIGNAL_DIMENSIONS = [
  { key: 'technical_momentum',  label: '技術動能',   weight: 0.12 },
  { key: 'fundamentals',       label: '基本面',     weight: 0.10 },
  { key: 'revenue_quality',    label: '營收品質',   weight: 0.09 },
  { key: 'shareholders',       label: '股東結構',   weight: 0.06 },
  { key: 'pledge_ratio',       label: '質押比率',   weight: 0.07 },
  { key: 'dividends',          label: '股息殖利率', weight: 0.08 },
  { key: 'institutional_flow', label: '機構法人',   weight: 0.10 },
  { key: 'signal_quality',     label: '訊號品質',   weight: 0.08 },
  { key: 'microstructure',     label: '微結構',     weight: 0.07 },
  { key: 'news_sentiment',     label: '新聞情緒',   weight: 0.06 },
  { key: 'cross_asset',        label: '跨資產',     weight: 0.05 },
  { key: 'earnings_quality',   label: '獲利品質',   weight: 0.07 },
  { key: 'risk_management',    label: '風險管理',   weight: 0.05 },
];

const SIGNAL_LABEL_MAP = {};
SIGNAL_DIMENSIONS.forEach(d => { SIGNAL_LABEL_MAP[d.key] = d.label; });

// ─── Sector Colors ───────────────────────────────────────────────────
const SECTOR_COLORS = {
  semiconductor:   '#58a6ff',
  electronics:     '#bc8cff',
  optoelectronics: '#e3b341',
  communications:  '#39d353',
  computers:       '#79c0ff',
  components:      '#f0883e',
  financial:       '#3fb950',
  materials:       '#8b949e',
  consumer:        '#f778ba',
  industrial:      '#a5845c',
  metals:          '#c9d1d9',
  construction:    '#db6d28',
  services:        '#56d4dd',
  tpex:            '#a5d6ff',
  other:           '#484f58',
};

const SECTOR_LABELS = {
  semiconductor:   '半導體',
  electronics:     '電子',
  optoelectronics: '光電',
  communications:  '通訊',
  computers:       '電腦',
  components:      '零組件',
  financial:       '金融',
  materials:       '水泥/化工',
  consumer:        '消費',
  industrial:      '工業',
  metals:          '金屬',
  construction:    '營建',
  services:        '服務',
  tpex:            '櫃買',
  other:           '其他',
};

// ─── Grade Calculation ───────────────────────────────────────────────
function getGrade(score) {
  if (score >= 90) return 'A';
  if (score >= 80) return 'B';
  if (score >= 70) return 'C';
  if (score >= 60) return 'D';
  return 'E';
}

const GRADE_COLORS = {
  A: { bg: '#238636', text: '#ffffff' },
  B: { bg: '#2ea043', text: '#ffffff' },
  C: { bg: '#9e6a03', text: '#ffffff' },
  D: { bg: '#d29922', text: '#000000' },
  E: { bg: '#da3633', text: '#ffffff' },
};

// ─── Pipeline Stage Names ────────────────────────────────────────────
const PIPELINE_STAGES = [
  'fetch_data',
  'validate_ingested_data',
  'fetch_history',
  'detect_regime',
  'db_migrate',
  'stage1_screen',
  'stage2_deep',
  'paper_trader',
  'telegram_alerts',
  'report_generator',
  'signal_fusion',
  'portfolio_optimizer',
];

const STAGE_LABELS = {
  fetch_data: '資料擷取',
  validate_ingested_data: '資料驗證',
  fetch_history: '歷史價格',
  detect_regime: '市場狀態',
  db_migrate: '資料庫遷移',
  stage1_screen: '初階篩選',
  stage2_deep: '深度篩選',
  paper_trader: '模擬交易',
  telegram_alerts: '訊息推播',
  report_generator: '報告產生',
  signal_fusion: '訊號融合',
  portfolio_optimizer: '投組最佳化',
};

// ─── Cross-Asset Labels ─────────────────────────────────────────────
const CROSS_ASSET_LABELS = {
  taiex_futures: 'TAIEX 期貨',
  usd_twd: 'USD/TWD',
  hsi: '恆生指數',
  kweb: 'KWEB',
  vix: 'VIX',
};

const CROSS_ASSET_COLORS = {
  taiex_futures: '#58a6ff',
  usd_twd: '#f0883e',
  hsi: '#bc8cff',
  kweb: '#3fb950',
  vix: '#f85149',
};

// ─── Data Adapters ───────────────────────────────────────────────────

function adaptRegime(data) {
  if (!data) return null;
  const regime = data.regime || 'unknown';
  return {
    regime,
    label: REGIME_MAP[regime] || regime,
    color: REGIME_COLORS[regime] || '#484f58',
    emoji: REGIME_EMOJI[regime] || '⚪',
    level: REGIME_LEVEL[regime] || 0,
    daysInRegime: data.days_in_regime ?? 0,
    volatility: data.volatility ?? 0,
    globalRisk: data.global_risk ?? 'neutral',
    globalRiskLabel: GLOBAL_RISK_MAP[data.global_risk] || data.global_risk || '中性',
    confidence: data.confidence ?? 'low',
    confidenceLabel: CONFIDENCE_MAP[data.confidence] || data.confidence || '低',
    exDividendSeason: data.ex_dividend_season ?? false,
    dataQuality: data.data_quality ?? 'OK',
    timestamp: data.timestamp ?? '',
    stocksAnalyzed: data.stocks_analyzed ?? 0,
  };
}

function adaptScreening(data) {
  if (!data) return null;
  const summary = data.summary || {};
  return {
    totalScreened: summary.total_screened ?? 0,
    passed: summary.passed ?? 0,
    watchlist: summary.watchlist ?? 0,
    rejected: summary.rejected ?? 0,
    passRate: summary.total_screened > 0
      ? ((summary.passed / summary.total_screened) * 100).toFixed(1)
      : '0.0',
    candidates: data.candidates || [],
  };
}

function adaptStage2(data) {
  if (!data) return null;
  const summary = data.summary || {};
  return {
    passedStage2: summary.passed_stage2 ?? 0,
    disqualified: summary.disqualified ?? 0,
    candidates: data.candidates || [],
    disqualifiedList: data.disqualified || [],
  };
}

function adaptEnsemble(data) {
  if (!data) return null;
  return {
    date: data.date ?? '',
    ranking: data.ranking || [],
    summary: data.summary || {},
  };
}

function adaptPortfolio(data) {
  if (!data) return null;
  return {
    date: data.date ?? '',
    stockWeights: data.stock_weights || {},
    sectorAllocation: data.sector_allocation || {},
    expectedReturnAnnual: data.expected_return_annual ?? 0,
    expectedVolatilityAnnual: data.expected_volatility_annual ?? 0,
    sharpeRatio: data.sharpe_ratio ?? 0,
    optimizationMethod: data.optimization_method ?? 'none',
    smoothingApplied: data.smoothing_applied ?? false,
    nStocks: data.n_stocks ?? 0,
    constraintViolations: data.constraint_violations || [],
  };
}

function adaptPaperTrades(data) {
  if (!data || !Array.isArray(data) || data.length === 0) {
    return { empty: true, trades: [], stats: null };
  }
  const openPositions = data.filter(t => t.status === 'open');
  const closedTrades = data.filter(t => t.status === 'closed');
  const winningTrades = closedTrades.filter(t => (t.pnl_pct ?? 0) > 0);
  const losingTrades = closedTrades.filter(t => (t.pnl_pct ?? 0) <= 0);
  const totalPnl = closedTrades.reduce((s, t) => s + (t.pnl_pct ?? 0), 0);
  const winRate = closedTrades.length > 0
    ? ((winningTrades.length / closedTrades.length) * 100).toFixed(1)
    : '0.0';

  return {
    empty: false,
    trades: data,
    stats: {
      totalTrades: data.length,
      openPositions: openPositions.length,
      closedTrades: closedTrades.length,
      winningTrades: winningTrades.length,
      losingTrades: losingTrades.length,
      winRate: parseFloat(winRate),
      totalPnlPct: totalPnl,
      avgPnlPct: closedTrades.length > 0
        ? (totalPnl / closedTrades.length).toFixed(2)
        : '0.00',
    },
    openPositions,
    closedTrades,
  };
}

function adaptMarketContext(data) {
  if (!data) return null;
  const breadth = data.market_breadth || {};
  return {
    crossAssets: data.cross_assets || {},
    advanceDeclineRatio: breadth.advance_decline_ratio ?? null,
    breadthLabel: breadth.breadth_label ?? '',
    correlationMatrix: breadth.correlation_matrix || {},
    crossAssetSignal: data.cross_asset_signal ?? 0,
    signalDetails: data.signal_details || {},
    timestamp: data.timestamp ?? '',
  };
}

function adaptPipelineRun(state) {
  if (!state) return null;
  const pr = state.pipeline_run || {};
  const pipeline = state.pipeline || {};
  return {
    timestamp: pr.timestamp ?? state.last_run ?? '',
    totalElapsedSec: pr.total_elapsed_sec ?? 0,
    stagesCompleted: pr.stages_completed ?? 0,
    stagesTotal: pr.stages_total ?? 12,
    failedStage: pr.failed_stage ?? null,
    lastDate: state.last_date ?? '',
    version: state.version ?? '',
    pipeline: pipeline,
  };
}

// ─── Risk Computation (client-side) ──────────────────────────────────

function computeRiskScore(regime, portfolio, paperTrades) {
  let score = 0;

  // Concentration risk (0–3): check if any sector > 25%
  const sectorAlloc = portfolio?.sectorAllocation || {};
  const maxSectorWeight = Math.max(...Object.values(sectorAlloc), 0);
  if (maxSectorWeight > 0.40) score += 3;
  else if (maxSectorWeight > 0.30) score += 2;
  else if (maxSectorWeight > 0.25) score += 1;

  // VaR risk (0–3): estimate from paper trades drawdowns
  const trades = paperTrades?.trades || [];
  const maxDD = trades.reduce((m, t) => Math.max(m, t.max_drawdown ?? 0), 0);
  if (maxDD > 0.15) score += 3;
  else if (maxDD > 0.10) score += 2;
  else if (maxDD > 0.05) score += 1;

  // Regime risk (0–4)
  const regimeLevel = REGIME_LEVEL[regime?.regime] || 0;
  if (regimeLevel >= 5) score += 4;
  else score += Math.max(0, regimeLevel - 1);

  return Math.min(10, score);
}

function computeVarEstimate(portfolio, paperTrades) {
  // Simple historical VaR estimate from closed trade P&Ls
  const closedTrades = (paperTrades?.closedTrades || [])
    .filter(t => t.pnl_pct != null)
    .map(t => t.pnl_pct);

  if (closedTrades.length < 5) {
    return { varPct: 0, varAmount: 0, method: 'neutral', label: '資料不足' };
  }

  closedTrades.sort((a, b) => a - b);
  const idx = Math.floor(closedTrades.length * 0.05);
  const varPct = Math.abs(closedTrades[idx] || 0);

  const totalExposure = Object.values(portfolio?.stockWeights || {}).length * 500000; // rough estimate
  const varAmount = totalExposure * varPct / 100;

  return {
    varPct: varPct.toFixed(2),
    varAmount: Math.round(varAmount),
    method: 'historical_estimate',
    label: '歷史模擬（估算）',
  };
}

// ─── Formatting Helpers ──────────────────────────────────────────────

function formatPct(value, decimals = 1) {
  if (value == null || isNaN(value)) return '—';
  const sign = value > 0 ? '+' : '';
  return `${sign}${parseFloat(value).toFixed(decimals)}%`;
}

function formatNum(value, decimals = 0) {
  if (value == null || isNaN(value)) return '—';
  return parseFloat(value).toLocaleString('zh-TW', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function formatCurrency(value) {
  if (value == null || isNaN(value)) return '—';
  if (Math.abs(value) >= 1e8) return `NT$${(value / 1e8).toFixed(1)}億`;
  if (Math.abs(value) >= 1e4) return `NT$${(value / 1e4).toFixed(0)}萬`;
  return `NT$${formatNum(value)}`;
}

function formatDate(dateStr) {
  if (!dateStr) return '—';
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return dateStr;
  return d.toLocaleDateString('zh-TW', { month: 'short', day: 'numeric' });
}

function formatTime(dateStr) {
  if (!dateStr) return '—';
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return dateStr;
  return d.toLocaleString('zh-TW', {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

function formatElapsed(seconds) {
  if (seconds == null || isNaN(seconds)) return '—';
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const m = Math.floor(seconds / 60);
  const s = (seconds % 60).toFixed(0);
  return `${m}m${s}s`;
}

// ─── Sector Resolution ───────────────────────────────────────────────
// Try to determine sector from stock code + sectors mapping

const SECTOR_CODE_RANGES = [
  { min: '01', max: '06', sector: 'materials' },
  { min: '07', max: '12', sector: 'consumer' },
  { min: '13', max: '18', sector: 'industrial' },
  { min: '19', max: '21', sector: 'metals' },
  { min: '22', max: '23', sector: 'semiconductor' },
  { min: '24', max: '24', sector: 'electronics' },
  { min: '25', max: '25', sector: 'optoelectronics' },
  { min: '26', max: '26', sector: 'communications' },
  { min: '27', max: '27', sector: 'computers' },
  { min: '28', max: '30', sector: 'components' },
  { min: '31', max: '33', sector: 'financial' },
  { min: '34', max: '36', sector: 'construction' },
  { min: '37', max: '39', sector: 'services' },
  { min: '90', max: '99', sector: 'tpex' },
];

function getSectorFromCode(code) {
  if (!code) return 'other';
  // TWSE industry code is first 2 digits of the stock code
  const prefix = code.substring(0, 2);
  for (const range of SECTOR_CODE_RANGES) {
    if (prefix >= range.min && prefix <= range.max) {
      return range.sector;
    }
  }
  return 'other';
}

function getSectorLabel(sector) {
  return SECTOR_LABELS[sector] || sector || '其他';
}

function getSectorColor(sector) {
  return SECTOR_COLORS[sector] || SECTOR_COLORS.other;
}

// ─── Data Fetching ───────────────────────────────────────────────────

async function fetchJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`Failed to fetch ${url}: ${resp.status}`);
  }
  return resp.json();
}

// ─── Export ──────────────────────────────────────────────────────────
// All functions and constants are available globally (no module bundler)
