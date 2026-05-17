/**
 * charts.js — Chart.js 4.x configurations and renderers
 * All charts use the global Chart object loaded from CDN.
 */

// ─── Chart.js Global Defaults ────────────────────────────────────────
function initChartDefaults() {
  Chart.defaults.color = '#8b949e';
  Chart.defaults.borderColor = '#30363d';
  Chart.defaults.font.family = "'Noto Sans TC', 'Noto Sans SC', sans-serif";
  Chart.defaults.font.size = 12;
  Chart.defaults.plugins.legend.labels.boxWidth = 12;
  Chart.defaults.plugins.legend.labels.padding = 16;
  Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(22,27,34,0.95)';
  Chart.defaults.plugins.tooltip.borderColor = '#30363d';
  Chart.defaults.plugins.tooltip.borderWidth = 1;
  Chart.defaults.plugins.tooltip.cornerRadius = 6;
  Chart.defaults.plugins.tooltip.padding = 10;
  Chart.defaults.plugins.tooltip.titleFont = { weight: '600' };
  Chart.defaults.animation.duration = 800;
  Chart.defaults.animation.easing = 'easeOutQuart';
}

// ─── Portfolio Sector Donut ──────────────────────────────────────────

function renderSectorDonut(canvasId, portfolio) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const allocation = portfolio?.sectorAllocation || {};
  const labels = Object.keys(allocation).map(k => getSectorLabel(k));
  const data = Object.values(allocation).map(v => +(v * 100).toFixed(1));
  const colors = Object.keys(allocation).map(k => getSectorColor(k));

  if (data.length === 0) {
    return renderPlaceholder(canvasId, '尚無配置資料');
  }

  return new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{
        data,
        backgroundColor: colors,
        borderColor: '#161b22',
        borderWidth: 2,
        hoverOffset: 6,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      cutout: '65%',
      plugins: {
        legend: {
          position: 'right',
          labels: { padding: 8, font: { size: 11 } },
        },
        tooltip: {
          callbacks: {
            label: (ctx) => ` ${ctx.label}: ${ctx.raw}%`,
          },
        },
      },
    },
    plugins: [{
      id: 'centerText',
      beforeDraw(chart) {
        const { width, height, ctx: c } = chart;
        c.save();
        const nStocks = portfolio?.nStocks ?? 0;
        const sharpe = portfolio?.sharpeRatio ?? 0;
        c.font = '600 16px "Noto Sans TC"';
        c.fillStyle = '#e6edf3';
        c.textAlign = 'center';
        c.textBaseline = 'middle';
        c.fillText(`${nStocks} 檔`, width / 2, height / 2 - 10);
        c.font = '400 11px "Noto Sans TC"';
        c.fillStyle = '#8b949e';
        c.fillText(`Sharpe ${sharpe.toFixed(2)}`, width / 2, height / 2 + 10);
        c.restore();
      },
    }],
  });
}

// ─── Top Holdings Bar ────────────────────────────────────────────────

function renderHoldingsBar(canvasId, portfolio) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const weights = portfolio?.stockWeights || {};
  const entries = Object.entries(weights)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8);

  if (entries.length === 0) {
    return renderPlaceholder(canvasId, '尚無持倉資料');
  }

  const labels = entries.map(([code]) => code);
  const data = entries.map(([, w]) => +(w * 100).toFixed(1));
  const colors = entries.map(([code]) => {
    const sector = getSectorFromCode(code);
    return getSectorColor(sector);
  });

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: '權重 %',
        data,
        backgroundColor: colors,
        borderColor: colors.map(c => c + '88'),
        borderWidth: 1,
        borderRadius: 4,
        maxBarThickness: 24,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          beginAtZero: true,
          max: 10,
          grid: { color: '#21262d' },
          ticks: { callback: v => `${v}%` },
        },
        y: {
          grid: { display: false },
          ticks: { font: { family: "'Sarasa Mono SC', monospace", size: 12 } },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => ` ${ctx.label}: ${ctx.raw}%`,
          },
        },

      },
    },
  });
}

// ─── P&L Performance Chart ──────────────────────────────────────────

function renderPnlChart(canvasId, paperTrades) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  if (paperTrades.empty) {
    return renderPlaceholder(canvasId, '尚無交易記錄');
  }

  // Build cumulative P&L timeline from closed trades
  const closedTrades = paperTrades.closedTrades
    .filter(t => t.exit_date && t.pnl_pct != null)
    .sort((a, b) => a.exit_date.localeCompare(b.exit_date));

  if (closedTrades.length === 0) {
    return renderPlaceholder(canvasId, '尚無已結束交易');
  }

  let cumulative = 0;
  const labels = [];
  const cumulativeData = [];
  const perTradeData = [];

  closedTrades.forEach(t => {
    cumulative += t.pnl_pct;
    labels.push(t.exit_date);
    cumulativeData.push(parseFloat(cumulative.toFixed(2)));
    perTradeData.push(parseFloat((t.pnl_pct ?? 0).toFixed(2)));
  });

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          type: 'line',
          label: '累計損益 %',
          data: cumulativeData,
          borderColor: '#58a6ff',
          backgroundColor: 'rgba(88,166,255,0.1)',
          fill: true,
          tension: 0.3,
          pointRadius: 3,
          pointHoverRadius: 6,
          pointBackgroundColor: '#58a6ff',
          yAxisID: 'y',
          order: 1,
        },
        {
          type: 'bar',
          label: '單筆損益 %',
          data: perTradeData,
          backgroundColor: perTradeData.map(v => v >= 0 ? 'rgba(63,185,80,0.7)' : 'rgba(248,81,73,0.7)'),
          borderColor: perTradeData.map(v => v >= 0 ? '#3fb950' : '#f85149'),
          borderWidth: 1,
          borderRadius: 3,
          maxBarThickness: 16,
          yAxisID: 'y1',
          order: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: {
          grid: { color: '#21262d' },
          ticks: {
            callback: function(val, idx) {
              return formatDate(this.getLabelForValue(val));
            },
            maxRotation: 45,
            font: { size: 10 },
          },
        },
        y: {
          position: 'left',
          title: { display: true, text: '累計損益 %' },
          grid: { color: '#21262d' },
        },
        y1: {
          position: 'right',
          title: { display: true, text: '單筆損益 %' },
          grid: { display: false },
        },
      },
      plugins: {
        legend: { position: 'top' },
        tooltip: {
          callbacks: {
            afterBody: (items) => {
              const idx = items[0]?.dataIndex;
              if (idx != null && closedTrades[idx]) {
                const t = closedTrades[idx];
                return `${t.code} ${t.name}\n退出: ${t.exit_reason || '—'}`;
              }
              return '';
            },
          },
        },
      },
    },
  });
}

// ─── VaR Gauge (Doughnut + clip technique) ──────────────────────────

function renderVarGauge(canvasId, varPct) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const value = parseFloat(varPct) || 0;
  const maxVal = 5;
  const fill = Math.min(value / maxVal, 1);

  let fillColor = '#3fb950'; // green
  if (value > 3) fillColor = '#f85149'; // red
  else if (value > 1) fillColor = '#d29922'; // amber

  return new Chart(ctx, {
    type: 'doughnut',
    data: {
      datasets: [{
        data: [fill * 100, (1 - fill) * 100],
        backgroundColor: [fillColor, '#21262d'],
        borderWidth: 0,
        circumference: 180,
        rotation: 270,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      cutout: '75%',
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false },
      },
    },
    plugins: [{
      id: 'gaugeText',
      beforeDraw(chart) {
        const { width, height, ctx: c } = chart;
        c.save();
        const centerX = width / 2;
        const centerY = height * 0.72;

        c.font = '700 22px "Sarasa Mono SC", monospace';
        c.fillStyle = fillColor;
        c.textAlign = 'center';
        c.textBaseline = 'middle';
        c.fillText(`${value.toFixed(1)}%`, centerX, centerY - 6);

        c.font = '400 11px "Noto Sans TC"';
        c.fillStyle = '#8b949e';
        c.fillText('VaR (95%)', centerX, centerY + 14);
        c.restore();
      },
    }],
  });
}

// ─── Signal Fusion Radar ────────────────────────────────────────────

function renderSignalRadar(canvasId, ensembleData, selectedCodes) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const ranking = ensembleData?.ranking || [];
  if (ranking.length === 0) {
    return renderPlaceholder(canvasId, '尚無融合訊號資料');
  }

  const codesToShow = selectedCodes || ranking.slice(0, 5).map(r => r.code);
  const stocks = codesToShow
    .map(code => ranking.find(r => r.code === code))
    .filter(Boolean);

  const radarColors = ['#58a6ff', '#bc8cff', '#3fb950', '#f0883e', '#f778ba'];

  const labels = SIGNAL_DIMENSIONS.map(d => d.label);
  const datasets = stocks.map((stock, i) => {
    const signals = stock.signals || {};
    return {
      label: `${stock.code} ${stock.name}`,
      data: SIGNAL_DIMENSIONS.map(d => signals[d.key] ?? 0),
      borderColor: radarColors[i % radarColors.length],
      backgroundColor: radarColors[i % radarColors.length] + '20',
      borderWidth: 2,
      pointBackgroundColor: radarColors[i % radarColors.length],
      pointRadius: 3,
      pointHoverRadius: 6,
    };
  });

  return new Chart(ctx, {
    type: 'radar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      scales: {
        r: {
          beginAtZero: true,
          max: 100,
          grid: { color: '#30363d' },
          angleLines: { color: '#30363d' },
          pointLabels: {
            font: { size: 11, family: "'Noto Sans TC'" },
            color: '#8b949e',
          },
          ticks: {
            stepSize: 20,
            backdropColor: 'transparent',
            color: '#484f58',
          },
        },
      },
      plugins: {
        legend: {
          position: 'bottom',
          labels: { padding: 12, font: { size: 11 } },
        },
      },
    },
  });
}

// ─── Regime History Step Chart ───────────────────────────────────────

function renderRegimeHistory(canvasId, regimeData) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  // Only current regime is available — show a single-point indicator
  if (!regimeData) {
    return renderPlaceholder(canvasId, '歷史數據即將推出');
  }

  const regime = regimeData;
  const label = regime.label || REGIME_MAP[regime.regime] || '未知';
  const color = regime.color || REGIME_COLORS[regime.regime] || '#484f58';
  const days = regime.daysInRegime || 0;

  // Render as a simple indicator card instead of time series
  // (regime_history.json doesn't exist yet)
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: [label],
      datasets: [{
        label: '市場狀態',
        data: [REGIME_LEVEL[regime.regime] || 0],
        backgroundColor: [color + 'cc'],
        borderColor: [color],
        borderWidth: 2,
        borderRadius: 6,
        maxBarThickness: 80,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          min: 0,
          max: 5,
          grid: { color: '#21262d' },
          ticks: {
            stepSize: 1,
            callback: function(v) {
              const labels = ['', '常態', '警戒', '壓力', '危機', '黑天鵝'];
              return labels[v] || '';
            },
          },
        },
        x: { grid: { display: false } },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: () => `${label} (${days} 天)`,
            afterLabel: () => `波動度: ${(regime.volatility * 100).toFixed(2)}%`,
          },
        },
      },
    },
    plugins: [{
      id: 'regimeNote',
      afterDraw(chart) {
        const { width, height, ctx: c } = chart;
        c.save();
        c.font = '400 10px "Noto Sans TC"';
        c.fillStyle = '#484f58';
        c.textAlign = 'center';
        c.fillText('歷史數據即將推出', width / 2, height - 8);
        c.restore();
      },
    }],
  });
}

// ─── Cross-Asset Sparklines ─────────────────────────────────────────

function renderCrossAssetSparkline(canvasId, history, color) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const data = Array.isArray(history) ? history : [];
  if (data.length === 0) return null;

  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.map((_, i) => i),
      datasets: [{
        data,
        borderColor: color || '#58a6ff',
        backgroundColor: (color || '#58a6ff') + '15',
        fill: true,
        tension: 0.4,
        pointRadius: 0,
        borderWidth: 1.5,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: { display: false },
        y: { display: false },
      },
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      animation: { duration: 0 },
    },
  });
}

// ─── Correlation Heatmap (canvas-drawn) ─────────────────────────────

function renderCorrelationHeatmap(canvasId, correlationMatrix) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  const ctx = canvas.getContext('2d');
  const w = canvas.width = canvas.offsetWidth * 2;
  const h = canvas.height = canvas.offsetHeight * 2;
  ctx.scale(2, 2);
  const dw = canvas.offsetWidth;
  const dh = canvas.offsetHeight;

  ctx.fillStyle = '#161b22';
  ctx.fillRect(0, 0, dw, dh);

  const entries = Object.entries(correlationMatrix || {});
  if (entries.length === 0) {
    ctx.fillStyle = '#484f58';
    ctx.font = '11px "Noto Sans TC"';
    ctx.textAlign = 'center';
    ctx.fillText('無相關性資料', dw / 2, dh / 2);
    return;
  }

  const n = entries.length;
  const cellW = dw / 3;
  const cellH = dh / Math.ceil(n / 3);
  const cols = 3;

  entries.forEach(([key, value], i) => {
    const col = i % cols;
    const row = Math.floor(i / cols);
    const x = col * cellW;
    const y = row * cellH;

    // Color by value: negative=red, zero=dark, positive=blue
    let r, g, b;
    if (value >= 0) {
      const intensity = Math.min(value, 1);
      r = Math.round(88 * intensity + 33 * (1 - intensity));
      g = Math.round(166 * intensity + 38 * (1 - intensity));
      b = Math.round(255 * intensity + 45 * (1 - intensity));
    } else {
      const intensity = Math.min(Math.abs(value), 1);
      r = Math.round(248 * intensity + 33 * (1 - intensity));
      g = Math.round(81 * intensity + 38 * (1 - intensity));
      b = Math.round(73 * intensity + 45 * (1 - intensity));
    }

    ctx.fillStyle = `rgb(${r},${g},${b})`;
    ctx.fillRect(x + 2, y + 2, cellW - 4, cellH - 4);

    // Label
    const shortKey = key.replace(/_vs_/, ' ↔ ').replace(/TAIEX/, '加權').replace(/USDTWD逆相關/, '美元').replace(/VIX/, 'VIX');
    ctx.fillStyle = '#e6edf3';
    ctx.font = '9px "Noto Sans TC"';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(shortKey, x + cellW / 2, y + cellH / 2 - 7);
    ctx.font = '700 11px "Sarasa Mono SC"';
    ctx.fillText(value.toFixed(2), x + cellW / 2, y + cellH / 2 + 7);
  });
}

// ─── Pipeline Timeline Bar ──────────────────────────────────────────

function renderPipelineTimeline(canvasId, pipelineRun) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  if (!pipelineRun) {
    return renderPlaceholder(canvasId, '尚無管線執行記錄');
  }

  const totalSec = pipelineRun.totalElapsedSec || 0;
  const completed = pipelineRun.stagesCompleted || 0;
  const total = pipelineRun.stagesTotal || 12;

  // If per-stage timing isn't available, show equal splits
  const avgTime = totalSec / total;
  const labels = PIPELINE_STAGES.slice(0, total).map(s => STAGE_LABELS[s] || s);
  const data = PIPELINE_STAGES.slice(0, total).map((_, i) =>
    i < completed ? parseFloat(avgTime.toFixed(1)) : 0
  );
  const colors = PIPELINE_STAGES.slice(0, total).map((_, i) => {
    if (pipelineRun.failedStage && PIPELINE_STAGES[i] === pipelineRun.failedStage) {
      return '#f85149';
    }
    return i < completed ? '#3fb95066' : '#21262d';
  });
  const borderColors = PIPELINE_STAGES.slice(0, total).map((_, i) => {
    if (pipelineRun.failedStage && PIPELINE_STAGES[i] === pipelineRun.failedStage) {
      return '#f85149';
    }
    return i < completed ? '#3fb950' : '#30363d';
  });

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: '耗時 (秒)',
        data,
        backgroundColor: colors,
        borderColor: borderColors,
        borderWidth: 1,
        borderRadius: 4,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          beginAtZero: true,
          grid: { color: '#21262d' },
          ticks: { callback: v => `${v}s` },
        },
        y: {
          grid: { display: false },
          ticks: { font: { size: 11, family: "'Noto Sans TC'" } },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            afterBody: () => {
              return `總耗時: ${formatElapsed(totalSec)} | ${completed}/${total} 階段完成`;
            },
          },
        },
      },
    },
  });
}

// ─── Ensemble Score Bar (horizontal, top 10) ────────────────────────

function renderEnsembleBar(canvasId, ensembleData) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const ranking = (ensembleData?.ranking || []).slice(0, 10);
  if (ranking.length === 0) {
    return renderPlaceholder(canvasId, '尚無融合分數資料');
  }

  const labels = ranking.map(r => `${r.code} ${r.name}`);
  const data = ranking.map(r => +(r.ensemble_score * 100).toFixed(1));
  const colors = ranking.map(r => {
    const s = r.ensemble_score;
    if (s >= 0.75) return '#3fb950';
    if (s >= 0.60) return '#58a6ff';
    if (s >= 0.45) return '#d29922';
    return '#f85149';
  });

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Ensemble 分數',
        data,
        backgroundColor: colors.map(c => c + 'aa'),
        borderColor: colors,
        borderWidth: 1,
        borderRadius: 3,
        maxBarThickness: 20,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          beginAtZero: true,
          max: 100,
          grid: { color: '#21262d' },
          ticks: { callback: v => `${v}` },
        },
        y: {
          grid: { display: false },
          ticks: { font: { size: 11, family: "'Sarasa Mono SC', monospace" } },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            afterBody: (items) => {
              const idx = items[0]?.dataIndex;
              if (idx != null && ranking[idx]) {
                const r = ranking[idx];
                const lower = ((r.confidence_lower ?? 0) * 100).toFixed(1);
                const upper = ((r.confidence_upper ?? 0) * 100).toFixed(1);
                return `信心區間: ${lower}–${upper}`;
              }
              return '';
            },
          },
        },
      },
    },
  });
}

// ─── Sector Concentration Bar ───────────────────────────────────────

function renderSectorConcentration(canvasId, portfolio) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const allocation = portfolio?.sectorAllocation || {};
  const entries = Object.entries(allocation).sort((a, b) => b[1] - a[1]);

  if (entries.length === 0) {
    return renderPlaceholder(canvasId, '尚無產業配置資料');
  }

  const labels = entries.map(([k]) => getSectorLabel(k));
  const data = entries.map(([, v]) => +(v * 100).toFixed(1));
  const colors = entries.map(([k]) => {
    const w = allocation[k];
    return w > 0.25 ? '#f85149' : getSectorColor(k);
  });

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: '權重 %',
        data,
        backgroundColor: colors.map(c => c + 'aa'),
        borderColor: colors,
        borderWidth: 1,
        borderRadius: 3,
        maxBarThickness: 20,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          beginAtZero: true,
          max: 30,
          grid: { color: '#21262d' },
          ticks: { callback: v => `${v}%` },
        },
        y: {
          grid: { display: false },
          ticks: { font: { size: 11, family: "'Noto Sans TC'" } },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            afterBody: (items) => {
              const val = parseFloat(items[0]?.raw || 0);
              return val > 25 ? '⚠️ 超過 25% 限制' : '✅ 符合限制';
            },
          },
        },
      },
    },
    plugins: [{
      id: 'limitLine',
      afterDraw(chart) {
        const xScale = chart.scales.x;
        const x25 = xScale.getPixelForValue(25);
        const ctx2 = chart.ctx;
        ctx2.save();
        ctx2.strokeStyle = '#f8514988';
        ctx2.lineWidth = 1;
        ctx2.setLineDash([4, 4]);
        ctx2.beginPath();
        ctx2.moveTo(x25, chart.chartArea.top);
        ctx2.lineTo(x25, chart.chartArea.bottom);
        ctx2.stroke();
        ctx2.restore();
      },
    }],
  });
}

// ─── Placeholder Helper ─────────────────────────────────────────────

function renderPlaceholder(canvasId, message) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;
  const ctx = canvas.getContext('2d');
  // Draw placeholder text after chart area is ready
  setTimeout(() => {
    const w = canvas.offsetWidth;
    const h = canvas.offsetHeight;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    ctx.scale(dpr, dpr);
    ctx.fillStyle = '#484f58';
    ctx.font = '14px "Noto Sans TC"';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(message || '尚無資料', w / 2, h / 2);
  }, 100);
  return null;
}
