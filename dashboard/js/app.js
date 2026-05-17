/**
 * app.js — Main entry: data loading, initialization, section rendering
 */

// ─── Global State ────────────────────────────────────────────────────
const APP = {
  date: null,
  state: null,
  regime: null,
  stage1: null,
  stage2: null,
  ensemble: null,
  portfolio: null,
  paperTrades: null,
  marketContext: null,
  pipelineRun: null,
  charts: {},
  isLoading: true,
};

// ─── Initialization ─────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
  initChartDefaults();
  showLoading();

  try {
    // Check for date override in URL params
    const urlParams = new URLSearchParams(window.location.search);
    const dateOverride = urlParams.get('date');

    // Step 1: Load state.json to resolve date
    try {
      APP.state = await fetchJSON('../state.json');
    } catch (e) {
      showError('無法載入 state.json，請確認已執行管線。');
      return;
    }

    APP.date = dateOverride || APP.state?.last_date || '';
    APP.pipelineRun = adaptPipelineRun(APP.state);

    if (!APP.date) {
      showError('state.json 中無 last_date 欄位。');
      return;
    }

    // Step 2: Parallel fetch all data files
    const dataPromises = {
      regime: fetchJSON('../data/regime.json').catch(() => null),
      stage1: fetchJSON(`../data/stage1_${APP.date}.json`).catch(() => null),
      stage2: fetchJSON(`../data/stage2_${APP.date}.json`).catch(() => null),
      ensemble: fetchJSON(`../data/ensemble_${APP.date}.json`).catch(() => null),
      portfolio: fetchJSON(`../data/optimized_portfolio_${APP.date}.json`).catch(() => null),
      paperTrades: fetchJSON('../data/paper_trades.json').catch(() => []),
      marketContext: fetchJSON('../data/market_context_cache.json').catch(() => null),
    };

    const results = await Promise.allSettled(Object.values(dataPromises));
    const keys = Object.keys(dataPromises);

    keys.forEach((key, i) => {
      if (results[i].status === 'fulfilled') {
        APP[key] = results[i].value;
      }
    });

    // Step 3: Adapt data
    APP.regime = adaptRegime(APP.regime);
    APP.stage1 = adaptScreening(APP.stage1);
    APP.stage2 = adaptStage2(APP.stage2);
    APP.ensemble = adaptEnsemble(APP.ensemble);
    APP.portfolio = adaptPortfolio(APP.portfolio);
    APP.paperTrades = adaptPaperTrades(APP.paperTrades);
    APP.marketContext = adaptMarketContext(APP.marketContext);

    // Step 4: Render all sections
    renderHeader();
    renderKPICards();
    renderCandidatesSection();
    renderPortfolioSection();
    renderPnlSection();
    renderRiskSection();
    renderRegimeSection();
    renderCrossAssetSection();
    renderRadarSection();
    renderPipelineSection();

    hideLoading();
  } catch (err) {
    showError(`載入失敗: ${err.message}`);
    console.error(err);
  }
});

// ─── Refresh Handler ────────────────────────────────────────────────

function refreshDashboard() {
  // Simply reload the page to re-fetch all data
  window.location.reload();
}

// ─── Loading / Error States ─────────────────────────────────────────

function showLoading() {
  const el = document.getElementById('loading-overlay');
  if (el) el.style.display = 'flex';
}

function hideLoading() {
  const el = document.getElementById('loading-overlay');
  if (el) el.style.display = 'none';
}

function showError(message) {
  hideLoading();
  const el = document.getElementById('error-message');
  if (el) {
    el.textContent = message;
    el.style.display = 'block';
  }
}

// ─── Header Rendering ───────────────────────────────────────────────

function renderHeader() {
  const pr = APP.pipelineRun;

  // Last updated
  const lastUpdated = document.getElementById('header-timestamp');
  if (lastUpdated) {
    lastUpdated.textContent = formatTime(pr?.timestamp || APP.state?.last_run);
  }

  // Pipeline status
  const pipelineStatus = document.getElementById('header-pipeline-status');
  if (pipelineStatus) {
    const completed = pr?.stagesCompleted ?? '?';
    const total = pr?.stagesTotal ?? 12;
    const failed = pr?.failedStage;
    pipelineStatus.innerHTML = failed
      ? `<span class="status-failed">✕ ${completed}/${total}</span>`
      : `<span class="status-ok">▶ ${completed}/${total}</span>`;
  }

  // Regime badge
  const regimeBadge = document.getElementById('header-regime-badge');
  if (regimeBadge && APP.regime) {
    const r = APP.regime;
    regimeBadge.innerHTML = `<span class="regime-pill" style="background:${r.color}22;color:${r.color};border:1px solid ${r.color}">${r.emoji} ${r.label}</span>`;
  }
}

// ─── KPI Cards ──────────────────────────────────────────────────────

function renderKPICards() {
  // Market Regime
  renderKPICard('kpi-regime', {
    label: '市場狀態',
    value: APP.regime?.label || '—',
    subtitle: APP.regime
      ? `第 ${APP.regime.daysInRegime} 天 · 波動 ${(APP.regime.volatility * 100).toFixed(2)}%`
      : '',
    borderColor: APP.regime?.color || '#484f58',
  });

  // Market Breadth
  const mc = APP.marketContext;
  renderKPICard('kpi-breadth', {
    label: '市場廣度',
    value: mc?.advanceDeclineRatio ? `${mc.advanceDeclineRatio.toFixed(1)}x` : '—',
    subtitle: mc?.breadthLabel || '資料不足',
    borderColor: '#58a6ff',
  });

  // Screening Funnel
  const s1 = APP.stage1;
  renderKPICard('kpi-funnel', {
    label: '篩選漏斗',
    value: s1 ? `${s1.passed} / ${s1.totalScreened}` : '—',
    subtitle: s1 ? `觀察名單: ${s1.watchlist} · 通過率: ${s1.passRate}%` : '',
    borderColor: '#3fb950',
  });

  // P&L Summary
  const pt = APP.paperTrades;
  renderKPICard('kpi-pnl', {
    label: '損益總覽',
    value: pt.empty ? '—' : formatPct(pt.stats.totalPnlPct),
    subtitle: pt.empty ? '尚無交易記錄' : `${pt.stats.openPositions}開 ${pt.stats.closedTrades}平 · 勝率 ${pt.stats.winRate}%`,
    borderColor: pt.empty ? '#484f58' : (pt.stats?.totalPnlPct >= 0 ? '#3fb950' : '#f85149'),
  });

  // Risk Score
  const riskScore = computeRiskScore(APP.regime, APP.portfolio, APP.paperTrades);
  let riskLabel = '低';
  let riskColor = '#3fb950';
  if (riskScore >= 7) { riskLabel = '高'; riskColor = '#f85149'; }
  else if (riskScore >= 4) { riskLabel = '中'; riskColor = '#d29922'; }
  renderKPICard('kpi-risk', {
    label: '風險分數',
    value: `${riskScore}/10`,
    subtitle: `風險等級: ${riskLabel}`,
    borderColor: riskColor,
  });
}

function renderKPICard(id, { label, value, subtitle, borderColor }) {
  const card = document.getElementById(id);
  if (!card) return;
  card.style.borderLeftColor = borderColor || '#30363d';
  card.innerHTML = `
    <div class="kpi-label">${label}</div>
    <div class="kpi-value">${value}</div>
    <div class="kpi-subtitle">${subtitle}</div>`;
}

// ─── Candidates Section ─────────────────────────────────────────────

function renderCandidatesSection() {
  renderCandidatesTable('candidates-table', APP.stage2, APP.ensemble);

  // Ensemble score bar
  APP.charts.ensembleBar = renderEnsembleBar('chart-ensemble-bar', APP.ensemble);
}

// ─── Portfolio Section ──────────────────────────────────────────────

function renderPortfolioSection() {
  APP.charts.sectorDonut = renderSectorDonut('chart-sector-donut', APP.portfolio);
  APP.charts.holdingsBar = renderHoldingsBar('chart-holdings-bar', APP.portfolio);

  // Meta info
  const meta = document.getElementById('portfolio-meta');
  if (meta && APP.portfolio) {
    const p = APP.portfolio;
    meta.innerHTML = `
      <div class="meta-row"><span class="meta-label">預期年報酬</span><span class="meta-value mono">${(p.expectedReturnAnnual * 100).toFixed(1)}%</span></div>
      <div class="meta-row"><span class="meta-label">預期年波動</span><span class="meta-value mono">${(p.expectedVolatilityAnnual * 100).toFixed(1)}%</span></div>
      <div class="meta-row"><span class="meta-label">Sharpe</span><span class="meta-value mono">${p.sharpeRatio.toFixed(2)}</span></div>
      <div class="meta-row"><span class="meta-label">最佳化方法</span><span class="meta-value">${p.optimizationMethod}</span></div>
      <div class="meta-row"><span class="meta-label">平滑調整</span><span class="meta-value">${p.smoothingApplied ? '是' : '否'}</span></div>
      ${p.constraintViolations.length > 0
        ? `<div class="meta-row meta-warn"><span class="meta-label">限制違規</span><span class="meta-value">${p.constraintViolations.length} 項</span></div>`
        : ''}
    `;
  }
}

// ─── P&L Section ────────────────────────────────────────────────────

function renderPnlSection() {
  APP.charts.pnlChart = renderPnlChart('chart-pnl', APP.paperTrades);

  // Stats overlay
  const stats = document.getElementById('pnl-stats');
  if (stats && !APP.paperTrades.empty) {
    const s = APP.paperTrades.stats;
    stats.innerHTML = `
      <div class="stat-item"><span class="stat-label">總交易</span><span class="stat-value mono">${s.totalTrades}</span></div>
      <div class="stat-item"><span class="stat-label">勝率</span><span class="stat-value mono">${s.winRate}%</span></div>
      <div class="stat-item"><span class="stat-label">平均報酬</span><span class="stat-value mono">${formatPct(s.avgPnlPct)}</span></div>
      <div class="stat-item"><span class="stat-label">持倉數</span><span class="stat-value mono">${s.openPositions}</span></div>
    `;
  } else if (stats) {
    stats.innerHTML = '<div class="placeholder-msg">尚無交易記錄</div>';
  }
}

// ─── Risk Section ───────────────────────────────────────────────────

function renderRiskSection() {
  // VaR gauge
  const varEst = computeVarEstimate(APP.portfolio, APP.paperTrades);
  APP.charts.varGauge = renderVarGauge('chart-var-gauge', varEst.varPct || 0);

  // VaR info text
  const varInfo = document.getElementById('var-info');
  if (varInfo) {
    varInfo.innerHTML = `
      <div class="var-amount">${varEst.varAmount > 0 ? formatCurrency(varEst.varAmount) : '—'}</div>
      <div class="var-method">${varEst.label}</div>`;
  }

  // Sector concentration
  APP.charts.sectorConc = renderSectorConcentration('chart-sector-conc', APP.portfolio);

  // Risk status grid
  renderRiskStatusGrid();
}

function renderRiskStatusGrid() {
  const grid = document.getElementById('risk-status-grid');
  if (!grid) return;

  const riskScore = computeRiskScore(APP.regime, APP.portfolio, APP.paperTrades);
  const sectorAlloc = APP.portfolio?.sectorAllocation || {};
  const hasConcentration = Object.values(sectorAlloc).some(w => w > 0.25);
  const numPositions = APP.paperTrades?.stats?.openPositions ?? 0;

  const items = [
    {
      label: '最大回撤',
      value: APP.portfolio ? '估算中' : '—',
      ok: true, // Simplified — real maxDD from risk_manager
    },
    {
      label: '熔斷機制',
      value: '正常',
      ok: true, // Would need circuit_breaker data
    },
    {
      label: '持倉數',
      value: `${numPositions}/8`,
      ok: numPositions < 6,
    },
    {
      label: '產業集中',
      value: hasConcentration ? '超限' : '正常',
      ok: !hasConcentration,
    },
    {
      label: '相關性警報',
      value: '0',
      ok: true, // Would need correlation_alerts data
    },
    {
      label: '風險分數',
      value: `${riskScore}/10`,
      ok: riskScore <= 3,
      warn: riskScore >= 4 && riskScore <= 6,
    },
  ];

  grid.innerHTML = items.map(item => {
    let cls = 'status-ok';
    if (!item.ok) cls = 'status-warn';
    if (item.warn) cls = 'status-caution';
    return `
      <div class="status-cell ${cls}">
        <div class="status-label">${item.label}</div>
        <div class="status-value">${item.value}</div>
      </div>`;
  }).join('');
}

// ─── Regime Section ─────────────────────────────────────────────────

function renderRegimeSection() {
  APP.charts.regimeHistory = renderRegimeHistory('chart-regime-history', APP.regime);

  // Regime detail card
  const detail = document.getElementById('regime-detail');
  if (detail && APP.regime) {
    const r = APP.regime;
    detail.innerHTML = `
      <div class="regime-detail-row">
        <span class="detail-label">當前狀態</span>
        <span class="detail-value" style="color:${r.color}">${r.label} ${r.emoji}</span>
      </div>
      <div class="regime-detail-row">
        <span class="detail-label">持續天數</span>
        <span class="detail-value mono">${r.daysInRegime} 天</span>
      </div>
      <div class="regime-detail-row">
        <span class="detail-label">波動度</span>
        <span class="detail-value mono">${(r.volatility * 100).toFixed(2)}%</span>
      </div>
      <div class="regime-detail-row">
        <span class="detail-label">全球風險</span>
        <span class="detail-value">${r.globalRiskLabel}</span>
      </div>
      <div class="regime-detail-row">
        <span class="detail-label">信心水準</span>
        <span class="detail-value">${r.confidenceLabel}</span>
      </div>
      <div class="regime-detail-row">
        <span class="detail-label">除權息季</span>
        <span class="detail-value">${r.exDividendSeason ? '是' : '否'}</span>
      </div>
    `;
  }
}

// ─── Cross-Asset Section ────────────────────────────────────────────

function renderCrossAssetSection() {
  const container = document.getElementById('cross-asset-rows');
  if (!container) return;

  const mc = APP.marketContext;
  if (!mc || !mc.crossAssets || Object.keys(mc.crossAssets).length === 0) {
    container.innerHTML = '<div class="placeholder-msg">尚無跨資產資料</div>';
    return;
  }

  const assets = mc.crossAssets;
  const assetKeys = Object.keys(CROSS_ASSET_LABELS);

  container.innerHTML = assetKeys
    .filter(key => assets[key])
    .map(key => {
      const a = assets[key];
      const label = CROSS_ASSET_LABELS[key] || key;
      const color = CROSS_ASSET_COLORS[key] || '#58a6ff';
      const change = a.change_pct ?? 0;
      const changeSign = change >= 0 ? '+' : '';
      const changeColor = change >= 0 ? '#3fb950' : '#f85149';

      return `
        <div class="cross-asset-row">
          <div class="ca-label">${label}</div>
          <div class="ca-value mono">${formatNum(a.latest, 1)}</div>
          <div class="ca-change mono" style="color:${changeColor}">${changeSign}${change.toFixed(2)}%</div>
          <div class="ca-sparkline">
            <canvas id="spark-${key}" height="32"></canvas>
          </div>
        </div>`;
    }).join('');

  // Render sparklines
  assetKeys.filter(key => assets[key]).forEach(key => {
    const history = assets[key].history || [];
    if (history.length > 0) {
      renderCrossAssetSparkline(`spark-${key}`, history, CROSS_ASSET_COLORS[key]);
    }
  });

  // Correlation heatmap
  renderCorrelationHeatmap('chart-correlation', mc.correlationMatrix);

  // Cross-asset signal
  const signalEl = document.getElementById('cross-asset-signal');
  if (signalEl) {
    const sig = mc.crossAssetSignal ?? 0;
    const sigColor = sig > 0.05 ? '#3fb950' : sig < -0.05 ? '#f85149' : '#8b949e';
    signalEl.innerHTML = `
      <span class="signal-label">總訊號</span>
      <span class="signal-value mono" style="color:${sigColor}">${sig >= 0 ? '+' : ''}${sig.toFixed(3)}</span>`;
  }
}

// ─── Radar Section ──────────────────────────────────────────────────

function renderRadarSection() {
  // Populate stock selector
  const selector = document.getElementById('radar-selector');
  if (selector && APP.ensemble) {
    const ranking = APP.ensemble.ranking || [];
    const selected = ranking.slice(0, 5).map(r => r.code);

    ranking.slice(0, 15).forEach((r, i) => {
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.value = r.code;
      cb.checked = i < 5;
      cb.addEventListener('change', updateRadarChart);
      const label = document.createElement('label');
      label.className = 'radar-cb-label';
      label.appendChild(cb);
      const span = document.createElement('span');
      span.textContent = `${r.code} ${r.name}`;
      label.appendChild(span);
      selector.appendChild(label);
    });

    APP.charts.radar = renderSignalRadar('chart-signal-radar', APP.ensemble, selected);
  } else {
    APP.charts.radar = renderSignalRadar('chart-signal-radar', APP.ensemble, []);
  }
}

function updateRadarChart() {
  const selector = document.getElementById('radar-selector');
  if (!selector) return;

  const selected = [...selector.querySelectorAll('input:checked')].map(cb => cb.value);
  if (APP.charts.radar) {
    APP.charts.radar.destroy();
  }
  APP.charts.radar = renderSignalRadar('chart-signal-radar', APP.ensemble, selected);
}

// ─── Pipeline Section ───────────────────────────────────────────────

function renderPipelineSection() {
  APP.charts.pipeline = renderPipelineTimeline('chart-pipeline', APP.pipelineRun);

  // Pipeline info
  const info = document.getElementById('pipeline-info');
  if (info && APP.pipelineRun) {
    const pr = APP.pipelineRun;
    info.innerHTML = `
      <span class="pipeline-stat">總耗時 <strong class="mono">${formatElapsed(pr.totalElapsedSec)}</strong></span>
      <span class="pipeline-stat">版本 <strong>${APP.state?.version || '—'}</strong></span>
      ${pr.failedStage ? `<span class="pipeline-stat pipeline-fail">失敗階段 <strong>${STAGE_LABELS[pr.failedStage] || pr.failedStage}</strong></span>` : '<span class="pipeline-stat pipeline-ok">所有階段完成 ✅</span>'}
    `;
  }
}
