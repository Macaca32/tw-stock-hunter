/**
 * tables.js — Top Candidates table with sort & expand
 */

let currentSort = { field: 'combined_score', dir: 'desc' };
let allCandidates = [];
let ensembleRanking = [];

/**
 * Render the candidates table
 */
function renderCandidatesTable(containerId, stage2Data, ensembleData) {
  const container = document.getElementById(containerId);
  if (!container) return;

  const candidates = stage2Data?.candidates || [];
  const ranking = ensembleData?.ranking || [];

  if (candidates.length === 0) {
    container.innerHTML = `
      <div class="placeholder-msg">尚無候選股資料</div>`;
    return;
  }

  // Merge ensemble scores into candidates
  allCandidates = candidates.map(c => {
    const ensemble = ranking.find(r => r.code === c.code);
    return {
      ...c,
      combined_score: c.combined_score ?? 0,
      ensemble_score: ensemble?.ensemble_score ?? null,
      confidence_lower: ensemble?.confidence_lower ?? null,
      confidence_upper: ensemble?.confidence_upper ?? null,
      signals: ensemble?.signals || {},
      feature_importance: ensemble?.feature_importance_top3 || [],
      sector: getSectorFromCode(c.code),
    };
  });

  ensembleRanking = ranking;
  renderTable(container);
}

function renderTable(container) {
  const sorted = sortCandidates(allCandidates);

  const headerHtml = `
    <div class="table-header">
      <div class="table-row table-row-header">
        <div class="tc col-code" data-sort="code">股票代號 <span class="sort-icon"></span></div>
        <div class="tc col-name">名稱</div>
        <div class="tc col-score" data-sort="combined_score">綜合分數 <span class="sort-icon"></span></div>
        <div class="tc col-grade" data-sort="grade">等級 <span class="sort-icon"></span></div>
        <div class="tc col-sector" data-sort="sector">產業 <span class="sort-icon"></span></div>
        <div class="tc col-ensemble" data-sort="ensemble_score">Ensemble <span class="sort-icon"></span></div>
        <div class="tc col-price" data-sort="close">收盤價 <span class="sort-icon"></span></div>
        <div class="tc col-detail">詳情</div>
      </div>
    </div>`;

  const rowsHtml = sorted.map((c, i) => renderCandidateRow(c, i)).join('');

  container.innerHTML = `
    <div class="custom-table">
      ${headerHtml}
      <div class="table-body">
        ${rowsHtml}
      </div>
    </div>`;

  // Attach sort handlers
  container.querySelectorAll('[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const field = th.dataset.sort;
      if (currentSort.field === field) {
        currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
      } else {
        currentSort.field = field;
        currentSort.dir = 'desc';
      }
      renderTable(container);
    });
    // Update sort icon
    const icon = th.querySelector('.sort-icon');
    if (icon) {
      if (th.dataset.sort === currentSort.field) {
        icon.textContent = currentSort.dir === 'asc' ? '▲' : '▼';
      } else {
        icon.textContent = '';
      }
    }
  });

  // Attach expand handlers
  container.querySelectorAll('.row-expand-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const idx = parseInt(btn.dataset.idx);
      toggleDetailRow(container, idx);
    });
  });
}

function renderCandidateRow(c, i) {
  const grade = getGrade(c.combined_score);
  const gradeStyle = GRADE_COLORS[grade] || GRADE_COLORS.E;
  const sectorLabel = getSectorLabel(c.sector);
  const ensembleVal = c.ensemble_score != null
    ? (c.ensemble_score * 100).toFixed(1)
    : '—';

  return `
    <div class="table-row" data-idx="${i}">
      <div class="tc col-code"><span class="mono">${c.code}</span></div>
      <div class="tc col-name">${c.name || '—'}</div>
      <div class="tc col-score">
        <div class="score-bar-container">
          <div class="score-bar" style="width:${c.combined_score}%;background:${getScoreColor(c.combined_score)}"></div>
          <span class="score-text mono">${(c.combined_score ?? 0).toFixed(1)}</span>
        </div>
      </div>
      <div class="tc col-grade">
        <span class="grade-badge" style="background:${gradeStyle.bg};color:${gradeStyle.text}">${grade}</span>
      </div>
      <div class="tc col-sector">
        <span class="sector-tag" style="border-color:${getSectorColor(c.sector)}">${sectorLabel}</span>
      </div>
      <div class="tc col-ensemble mono">${ensembleVal}</div>
      <div class="tc col-price mono">${c.close != null ? formatNum(c.close, 1) : '—'}</div>
      <div class="tc col-detail">
        <button class="row-expand-btn" data-idx="${i}" title="展開詳情">▸</button>
      </div>
    </div>
    <div class="detail-row" data-detail-idx="${i}" style="display:none">
      ${renderDetailContent(c)}
    </div>`;
}

function renderDetailContent(c) {
  const checks = c.checks || {};
  const signals = c.signals || {};
  const redFlags = c.red_flags || [];

  let checksHtml = '';
  if (Object.keys(checks).length > 0) {
    checksHtml = `
      <div class="detail-section">
        <div class="detail-title">深度檢查</div>
        <div class="checks-grid">
          ${Object.entries(checks).map(([key, val]) => {
            const score = val.score ?? 0;
            const status = val.status ?? '—';
            const color = score >= 70 ? '#3fb950' : score >= 50 ? '#d29922' : '#f85149';
            return `
              <div class="check-item">
                <span class="check-label">${key}</span>
                <div class="check-bar-container">
                  <div class="check-bar" style="width:${score}%;background:${color}"></div>
                  <span class="check-score mono">${score.toFixed(0)}</span>
                </div>
                <span class="check-status">${status}</span>
              </div>`;
          }).join('')}
        </div>
      </div>`;
  }

  let signalsHtml = '';
  if (Object.keys(signals).length > 0) {
    signalsHtml = `
      <div class="detail-section">
        <div class="detail-title">訊號維度</div>
        <div class="signals-grid">
          ${SIGNAL_DIMENSIONS.map(d => {
            const val = signals[d.key];
            if (val == null) return '';
            const color = val >= 70 ? '#3fb950' : val >= 50 ? '#d29922' : '#f85149';
            return `
              <div class="signal-item">
                <span class="signal-label">${d.label}</span>
                <div class="signal-bar-container">
                  <div class="signal-bar" style="width:${val}%;background:${color}"></div>
                  <span class="signal-score mono">${val.toFixed(0)}</span>
                </div>
              </div>`;
          }).join('')}
        </div>
      </div>`;
  }

  let flagsHtml = '';
  if (redFlags.length > 0) {
    flagsHtml = `
      <div class="detail-section detail-flags">
        <div class="detail-title">🚩 警告標記</div>
        <ul class="flag-list">
          ${redFlags.map(f => `<li>${f}</li>`).join('')}
        </ul>
      </div>`;
  }

  // Microstructure
  const micro = c.microstructure || {};
  let microHtml = '';
  if (Object.keys(micro).length > 0) {
    const pattern = micro.intraday_pattern || {};
    microHtml = `
      <div class="detail-section">
        <div class="detail-title">微結構</div>
        <div class="micro-info">
          ${pattern.pattern ? `<span>型態: ${pattern.pattern}</span>` : ''}
          ${pattern.sentiment != null ? `<span>情緒: ${pattern.sentiment.toFixed(2)}</span>` : ''}
          ${pattern.confidence ? `<span>信心: ${pattern.confidence}</span>` : ''}
        </div>
      </div>`;
  }

  // Score breakdown
  const breakdown = c.score_breakdown || c.stage1_score_breakdown;
  let breakdownHtml = '';
  if (breakdown && Object.keys(breakdown).length > 0) {
    breakdownHtml = `
      <div class="detail-section">
        <div class="detail-title">分數構成</div>
        <div class="breakdown-grid">
          ${Object.entries(breakdown).map(([key, val]) => {
            const color = val >= 70 ? '#3fb950' : val >= 50 ? '#d29922' : '#f85149';
            return `
              <div class="breakdown-item">
                <span class="breakdown-label">${key}</span>
                <div class="breakdown-bar-container">
                  <div class="breakdown-bar" style="width:${val}%;background:${color}"></div>
                  <span class="breakdown-score mono">${val.toFixed(0)}</span>
                </div>
              </div>`;
          }).join('')}
        </div>
      </div>`;
  }

  return `
    <div class="detail-content">
      ${breakdownHtml}
      ${checksHtml}
      ${signalsHtml}
      ${microHtml}
      ${flagsHtml}
    </div>`;
}

function toggleDetailRow(container, idx) {
  const detailRow = container.querySelector(`[data-detail-idx="${idx}"]`);
  const expandBtn = container.querySelector(`.row-expand-btn[data-idx="${idx}"]`);
  if (!detailRow) return;

  if (detailRow.style.display === 'none') {
    detailRow.style.display = '';
    if (expandBtn) expandBtn.textContent = '▾';
  } else {
    detailRow.style.display = 'none';
    if (expandBtn) expandBtn.textContent = '▸';
  }
}

function sortCandidates(candidates) {
  const { field, dir } = currentSort;
  const sorted = [...candidates];

  sorted.sort((a, b) => {
    let aVal, bVal;

    if (field === 'grade') {
      aVal = getGrade(a.combined_score);
      bVal = getGrade(b.combined_score);
      // Sort A first
      const gradeOrder = { A: 5, B: 4, C: 3, D: 2, E: 1 };
      aVal = gradeOrder[aVal] || 0;
      bVal = gradeOrder[bVal] || 0;
    } else if (field === 'sector') {
      aVal = a.sector || 'zzz';
      bVal = b.sector || 'zzz';
      return dir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
    } else if (field === 'code') {
      aVal = a.code || '';
      bVal = b.code || '';
      return dir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
    } else {
      aVal = a[field] ?? -Infinity;
      bVal = b[field] ?? -Infinity;
    }

    return dir === 'asc' ? aVal - bVal : bVal - aVal;
  });

  return sorted;
}

function getScoreColor(score) {
  if (score >= 80) return '#3fb950';
  if (score >= 70) return '#58a6ff';
  if (score >= 60) return '#d29922';
  return '#f85149';
}
