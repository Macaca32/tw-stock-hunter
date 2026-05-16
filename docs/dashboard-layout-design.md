# Performance Dashboard UI — Layout Design Document

> **Phase**: 39 — Layout Design (pre-implementation)  
> **Date**: 2026-05-17  
> **Target**: Single-page, dark-theme, static HTML dashboard  
> **Data Sources**: `data/*.json`, `state.json`, `reports/`

---

## 1. Page-Level Architecture

The dashboard follows a **12-column CSS grid** with a fixed header and scrollable body.  
Sections are grouped into visual "panels" with consistent padding, rounded corners, and subtle borders.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  HEADER BAR  (sticky)                                                   │
│  Logo + Title | Last Updated | Pipeline Status | Regime Badge | Refresh │
├────────┬────────┬────────┬────────┬────────┬────────────────────────────┤
│        │        │        │        │        │                             │
│  REGIME│ MARKET │SCREEN  │ P&L   │ RISK   │                             │
│  CARD  │BREADTH │FUNNEL  │SUMMARY│ SCORE  │                             │
│  (1col)│(1col)  │(1col)  │(1col) │(1col)  │                             │
│        │        │        │        │        │                             │
├────────┴────────┴────────┴────────┴────────┼────────────────────────────┤
│                                             │                            │
│  TOP CANDIDATES TABLE                       │  PORTFOLIO ALLOCATION      │
│  (sortable, scrollable)                     │  (sector donut + holdings) │
│  col-span: 8                                │  col-span: 4               │
│                                             │                            │
├─────────────────────────────────────────────┼────────────────────────────┤
│                                             │                            │
│  P&L PERFORMANCE CHART                      │  RISK DASHBOARD            │
│  (area/line chart over time)                │  (VaR gauge + indicators)  │
│  col-span: 8                                │  col-span: 4               │
│                                             │                            │
│                                             │                            │
├─────────────────────────┬───────────────────┴────────────────────────────┤
│                         │                                                │
│  REGIME TREND HISTORY   │  CROSS-ASSET MONITOR                           │
│  (timeline chart)       │  (sparklines + correlation)                    │
│  col-span: 5            │  col-span: 7                                   │
│                         │                                                │
├─────────────────────────┴────────────────────────────────────────────────┤
│                                                                          │
│  SIGNAL FUSION HEATMAP  (13-dimension radar per top stock)               │
│  col-span: 12                                                           │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  PIPELINE STAGE TIMELINE  (horizontal bar chart, last run)               │
│  col-span: 12                                                           │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│  FOOTER: Data freshness notice | Thresholds summary | Version            │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Detailed ASCII Wireframe

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  🎯 TW Stock Hunter                          2026-05-17 11:30  │ ▶ 10/12 │
║                                              Last pipeline run  │ 常態 🟢 │
╠════════╦════════╦════════╦════════╦════════╦═══════════════════════════════╣
║ MARKET ║ BREADTH║SCREEN ║  P&L  ║  RISK  ║                                ║
║ REGIME ║  INDEX ║FUNNEL ║SUMMARY║ SCORE  ║                                ║
║        ║        ║        ║        ║        ║                                ║
║  常態   ║ 1.2x   ║1357    ║ +12.5% ║  ▌3/10 ║                                ║
║ 🟢     ║ ▲+0.1  ║├399    ║ 5開3平 ║  LOW   ║                                ║
║ d=12   ║ 廣度正常 ║├334    ║ WR=70% ║        ║                                ║
║        ║        ║└624    ║        ║        ║                                ║
╠════════╩════════╩════════╩════════╩════════╬═══════════════════════════════╣
║                                           ║                               ║
║  TOP CANDIDATES            ▼Score ▼Sector  ║  PORTFOLIO ALLOCATION         ║
║  ┌──────┬──────┬─────┬────┬────┬────┐     ║                               ║
║  │ Code │ Name │Score│Grd │Sec │ Δ  │     ║     ╭─────╲                    ║
║  ├──────┼──────┼─────┼────┼────┼────┤     ║    ╱  35%  ╲   半導體        ║
║  │ 2330 │台積電 │92.5 │ A  │semi│+2.1│     ║   │ 25%    │   電子          ║
║  │ 2317 │鴻海  │88.3 │ B  │elec│+1.5│     ║    ╲ 15%  ╱    金融          ║
║  │ 2454 │聯發科 │85.1 │ B  │semi│-0.3│     ║     ╲─────╱     其他          ║
║  │ ...  │ ...  │ ... │... │... │... │     ║                               ║
║  │ (scrollable, 20 rows)           │     ║  Top Holdings:                ║
║  └──────┴──────┴─────┴────┴────┴────┘     ║  2330  8.0% ████████░░       ║
║                                           ║  2317  6.0% ██████░░░░       ║
║  Ensemble Score Bar (top 10)              ║  2454  5.5% █████░░░░░       ║
║  2330 ████████████████████░ 0.92          ║  ...                          ║
║  2317 ██████████████████░░░ 0.88          ║                               ║
║  2454 █████████████████░░░░ 0.85          ║  Sharpe: 0.77  Method: scipy ║
║                                           ║                               ║
╠═══════════════════════════════════════════╬═══════════════════════════════╣
║                                           ║                               ║
║  P&L PERFORMANCE OVER TIME                ║  RISK INDICATORS              ║
║                                           ║                               ║
║  +15% ┤                          ╭──      ║  VaR (95%)                    ║
║       │                    ╭─────╯         ║  ┌────────────────────┐       ║
║   +5% ┤           ╭───────╯               ║  │ NT$45,200  1.8%   │       ║
║       │     ╭─────╯                       ║  │ Historical Sim.    │       ║
║    0% ┼─────╯                             ║  └────────────────────┘       ║
║       │                                   ║                               ║
║  -5%  ┤          ╭╮                       ║  Concentration                ║
║       │     ╭────╯╰──                     ║  半導體 35% ████████▓░       ║
║  -15% ┤─────╯                              ║  電子   25% █████░░░░       ║
║       └──┬──┬──┬──┬──┬──┬──┬──→ date       ║                               ║
║         5/10 5/11 5/12 ...                 ║  Drawdown Risk: 12.3%        ║
║                                           ║  Circuit Breaker: ✅ OK       ║
║  Cumulative P&L — Open: +5.2%             ║                               ║
║  Win/Loss bars per trade overlaid          ║  Position Limits              ║
║                                           ║  5/8 positions used          ║
║                                           ║  Sector max: 3/sector ✅     ║
╠═══════════════════════════╦═════════════════╧═══════════════════════════════╣
║                         ║                                                ║
║  REGIME HISTORY         ║  CROSS-ASSET MONITOR                           ║
║  (last 60 days)         ║                                                ║
║                         ║  TAIEX  21500 ▲+0.5%  ──╱╲──╱╲──              ║
║  crisis ┤     ╭╮        ║  USD/TWD  32.5 ▼-0.2%  ──╲╱──╲╱──            ║
║  stress ┤   ╭─╯╰─╮      ║  HSI    19500 ▼-1.2%  ──╲╱──╲──              ║
║  caution┤ ╭─╯     ╰──   ║  VIX     18.5 ▲+2.1%  ──╱╲──╱──              ║
║  normal ┤─╯           ── ║  KWEB    28.5 ▲+0.3%  ──╱─╲──╱──            ║
║         └──┬──┬──┬──┬──→ ║                                                ║
║                         ║  Correlations                                  ║
║  Color bands per regime ║  TAIEX↔HSI  0.65 │ USD↔TAIEX -0.30            ║
║  Current: 常態 (12 days)║  VIX↔TAIEX -0.45 │ Signal: +0.05              ║
║                         ║                                                ║
╠═════════════════════════╩══════════════════════════════════════════════════╣
║                                                                            ║
║  SIGNAL FUSION RADAR — Top 5 Candidates                                   ║
║                                                                            ║
║         營收品質                                                            ║
║            ╱╲                                                               ║
║  獲利能力 ╱  ╲ 技術動能                                                      ║
║          ╱ 2330╲           2317 overlay in different color                  ║
║         ╱  ●    ╲                                                          ║
║  ──────╱──────────╲────── 機構法人                                          ║
║        ╲          ╱                                                         ║
║         ╲ 股東  ╱  評價                                                      ║
║          ╲    ╱                                                              ║
║           ╲╱                                                                ║
║      新聞情緒   微結構                                                        ║
║                                                                            ║
║  Legend: ● 2330 台積電  ● 2317 鴻海  ● 2454 聯發科  ● 3008  ● 3711       ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  PIPELINE TIMELINE — Latest Run                                            ║
║                                                                            ║
║  fetch_data       ████████░░░░░░░░░░░░░░░░  8.2s                          ║
║  validate         ██░░░░░░░░░░░░░░░░░░░░░░  0.5s                          ║
║  fetch_history    ██████████████░░░░░░░░░░░ 12.1s                          ║
║  detect_regime    ████░░░░░░░░░░░░░░░░░░░░░  3.4s                          ║
║  stage1_screen    ██████████░░░░░░░░░░░░░░░  8.7s                          ║
║  stage2_deep      ████████████████░░░░░░░░░░ 13.2s                         ║
║  paper_trader     ███░░░░░░░░░░░░░░░░░░░░░░  2.1s                         ║
║  signal_fusion    █████░░░░░░░░░░░░░░░░░░░░  4.3s                          ║
║  portfolio_opt    ████░░░░░░░░░░░░░░░░░░░░░  3.0s                          ║
║  Total: 41.3s     ✅ All stages passed                                     ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Data: 2026-05-17 03:10:09 | Thresholds: v3.0 | Weights: v6.0 | v1.5      ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

## 3. Section-by-Section Design Specification

### 3.1 Header Bar (Sticky)

| Element | Description | Width |
|---------|-------------|-------|
| Logo + Title | "TW Stock Hunter" with subtitle "台股篩選儀表板" | auto |
| Last Updated | Timestamp from `state.json.last_run` | 200px |
| Pipeline Status | `stages_completed / stages_total` with pass/fail indicator | 120px |
| Regime Badge | Current regime with colored pill (常態🟢 警戒🟡 壓力🟠 危機🔴 黑天鵝⚫) | 140px |
| Refresh Button | Manual reload icon (re-fetches JSON) | 40px |

**Behavior**: Stays fixed at top on scroll. Background `rgba(13,17,23,0.95)` with backdrop blur.

---

### 3.2 KPI Card Row (5 cards)

Five compact metric cards in a flex row. Each card has:
- Icon + label (Traditional Chinese)
- Large primary value
- Subtitle / secondary stat
- Subtle left border color-coded by metric type

| Card | Data Source | Primary Value | Subtitle | Border Color |
|------|-------------|---------------|----------|-------------|
| **市場狀態** | `regime.json` | Regime label (常態/警戒/壓力/危機/黑天鵝) | `d={days_in_regime}days, vol={volatility}` | Regime color |
| **市場廣度** | `market_context_cache.json` | `advance_decline_ratio` (e.g., 1.2x) | `breadth_label` | `#58a6ff` (blue) |
| **篩選漏斗** | `stage1_{date}.json` | `{passed} / {total_screened}` | `watchlist: {n}` | `#3fb950` (green) |
| **損益總覽** | `paper_trades.json` | Cumulative P&L % | `open/closed, WR%` | Green if +, red if - |
| **風險分數** | Computed | `overall_risk_score / 10` | Risk level label | `#f0883e` → `#f85149` by score |

---

### 3.3 Top Candidates Table (col-span 8)

**Data Sources**: `stage2_{date}.json` + `ensemble_{date}.json`

| Column | Source Field | Alignment | Sortable |
|--------|-------------|-----------|----------|
| 股票代號 | `code` | center | Yes |
| 名稱 | `name` | left | No |
| 綜合分數 | `combined_score` | center | Yes (default desc) |
| 等級 | Grade derived: A/B/C/D/E | center with color badge | Yes |
| 產業 | `sector` (from sectors.py mapping) | center | Yes |
| Ensemble | `ensemble_score` | center | Yes |
| 收盤價 | `close` | right | Yes |
| 詳情 | Expandable row → show `checks` breakdown | center | No |

**Grade Badge Colors**:
| Grade | Background | Text |
|-------|-----------|------|
| A | `#238636` dark green | `#ffffff` |
| B | `#2ea043` green | `#ffffff` |
| C | `#9e6a03` amber | `#ffffff` |
| D | `#d29922` yellow | `#000000` |
| E | `#da3633` red | `#ffffff` |

**Ensemble Score Bar**: Below the table, a horizontal bar chart showing top 10 stocks' ensemble scores with confidence bands (lower–upper).

**Expandable Detail Row**: Click a row to reveal:
- `checks` breakdown as mini progress bars (dividend, pledge, shareholders, etc.)
- `microstructure` summary (pattern, sentiment)
- `red_flags` if any

---

### 3.4 Portfolio Allocation (col-span 4)

**Data Source**: `portfolio_{date}.json`

**Chart 1 — Sector Donut**:
- Chart.js doughnut chart
- 15 sectors with the color palette below
- Center text: stock count + Sharpe ratio
- Hover: sector name + weight %

**Chart 2 — Top Holdings Bar**:
- Horizontal bars showing `stock_weights` for top 8 positions
- Label: `{code} {name} {weight}%`
- Color: match sector color
- Max weight line at 8% (constraint)

**Meta Info** below charts:
| Label | Value |
|-------|-------|
| 預期年報酬 | `expected_return_annual` |
| 預期年波動 | `expected_volatility_annual` |
| Sharpe | `sharpe_ratio` |
| 最佳化方法 | `optimization_method` |
| 平滑調整 | `smoothing_applied` |

---

### 3.5 P&L Performance Chart (col-span 8)

**Data Source**: `paper_trades.json` (historical trades aggregated by date)

**Chart Type**: Dual-axis area + bar chart
- **Area (left axis)**: Cumulative P&L % over time (line filled below)
- **Bars (bottom)**: Per-trade P&L % (green above / red below zero line)

**Interactivity**:
- Tooltip: trade details (code, entry/exit, reason, P&L)
- Zoom: date range selector below chart

**Stats Overlay** (top-right corner of chart):
| Stat | Value |
|------|-------|
| 總交易次數 | `total_trades` |
| 勝率 | `win_rate` |
| 平均報酬 | `avg_pnl_pct` |
| 最大持倉 | current open count |

---

### 3.6 Risk Indicators (col-span 4)

**Data Source**: Computed from `risk_manager` outputs stored in portfolio/trade data

**Widget 1 — VaR Gauge**:
- Semi-circular gauge chart (0–5% range)
- Needle at current `var_pct`
- Color zones: green (<1%), yellow (1–3%), red (>3%)
- Below: `var_amount` in NT$

**Widget 2 — Sector Concentration Bars**:
- Horizontal stacked bars
- Each sector bar: current weight vs 25% limit line
- Over-limit sectors highlighted in red

**Widget 3 — Status Grid** (2x3 mini cards):
| Cell | Metric | OK Color | Warn Color |
|------|--------|----------|------------|
| 最大回撤 | `max_drawdown_risk` | <15% green | >15% red |
| 熔斷機制 | circuit breaker status | OK green | TRIGGERED red |
| 持倉數 | `num_positions / max_positions` | <6 green | >=6 yellow |
| 產業集中 | any sector >25% | OK green | WARN red |
| 相關性警報 | `correlation_alerts` length | 0 green | >0 red |
| 風險分數 | `overall_risk_score` | <=3 green | 4-6 yellow, >=7 red |

---

### 3.7 Regime Trend History (col-span 5)

**Data Source**: `data/regime.json` (current) + historical regime snapshots

**Chart Type**: Colored step-area chart (timeline)
- Y-axis: 5 regime levels (normal=1 → black_swan=5)
- X-axis: dates (last 60 days)
- Each day colored by regime: green / amber / orange / red / purple
- Step transitions (not interpolated) — regime is discrete
- Current position marker with pulse animation

**Annotations**:
- Min 5-day duration rule visualization
- Ex-dividend season band (Jul 15 – Aug 31) shaded

---

### 3.8 Cross-Asset Monitor (col-span 7)

**Data Source**: `data/market_context_cache.json`

**Layout**: 5 rows (TAIEX, USD/TWD, HSI, VIX, KWEB)

Each row contains:
| Element | Description |
|---------|-------------|
| Label | Asset name (Traditional Chinese) |
| Value | Latest price |
| Change | % change with arrow indicator |
| Sparkline | 20-day mini line chart |

**Correlation Matrix** (below sparklines):
- 3x3 mini heatmap: TAIEX-HSI, USD-TAIEX, VIX-TAIEX
- Color: blue (positive) → white (0) → red (negative)
- Cell values shown

**Signal Summary**:
- `cross_asset_signal` value with colored bar
- Expandable: `signal_details` from market_context

---

### 3.9 Signal Fusion Radar (col-span 12)

**Data Source**: `data/ensemble_{date}.json` → `ranking[].signals`

**Chart Type**: Radar/spider chart (Chart.js)
- 13 axes for the 13 scoring dimensions
- Up to 5 stocks overlaid with different colors
- Each stock's `signals` dict maps to radar values

**Interactivity**:
- Dropdown to select which stocks to overlay
- Hover: exact score per dimension
- Legend with stock code + name

**Feature Importance Sidebar** (if data available):
- Top 3 positive/negative contributors listed
- Mini horizontal bar chart per contributor

---

### 3.10 Pipeline Timeline (col-span 12)

**Data Source**: `state.json` → `pipeline_run`

**Chart Type**: Horizontal bar chart
- Each stage = one bar
- Bar length = `elapsed_sec`
- Color: green (success) / red (failed)
- Total time shown on the right

**Additional Info**:
- Failed stage highlighted with error message
- Hover: stage name + elapsed time
- Below: version info from `state.json.version`

---

## 4. Color Scheme & Theme

### 4.1 Base Palette (GitHub Dark Inspired)

| Token | Hex | Usage |
|-------|-----|-------|
| `--bg-primary` | `#0d1117` | Page background |
| `--bg-secondary` | `#161b22` | Card/panel backgrounds |
| `--bg-tertiary` | `#21262d` | Table header, input backgrounds |
| `--border` | `#30363d` | Card borders, dividers |
| `--text-primary` | `#e6edf3` | Headings, primary text |
| `--text-secondary` | `#8b949e` | Labels, subtitles |
| `--text-muted` | `#484f58` | Disabled, hint text |
| `--accent-blue` | `#58a6ff` | Links, active states |
| `--accent-green` | `#3fb950` | Positive, success, grade A-B |
| `--accent-amber` | `#d29922` | Warning, caution, grade C-D |
| `--accent-red` | `#f85149` | Danger, failure, grade E |
| `--accent-purple` | `#bc8cff` | Secondary accent, portfolio |

### 4.2 Regime Colors

| Regime | 中文 | Color | Hex |
|--------|------|-------|-----|
| normal | 常態 | Green | `#3fb950` |
| caution | 警戒 | Amber | `#d29922` |
| stress | 壓力 | Orange | `#f0883e` |
| crisis | 危機 | Red | `#f85149` |
| black_swan | 黑天鵝 | Purple-black | `#8957e5` |

### 4.3 Sector Colors (15 sectors)

| Sector | Color | Hex |
|--------|-------|-----|
| 半導體 semiconductor | Blue | `#58a6ff` |
| 電子 electronics | Purple | `#bc8cff` |
| 光電 optoelectronics | Yellow | `#e3b341` |
| 通訊 communications | Teal | `#39d353` |
| 電腦 computers | Sky | `#79c0ff` |
| 零組件 components | Orange | `#f0883e` |
| 金融 financial | Green | `#3fb950` |
| 水泥 materials | Gray | `#8b949e` |
| 消費 consumer | Pink | `#f778ba` |
| 工業 industrial | Brown | `#a5845c` |
| 金屬 metals | Silver | `#c9d1d9` |
| 營建 construction | Warm | `#db6d28` |
| 服務 services | Cyan | `#56d4dd` |
| 櫃買 tpex | Light blue | `#a5d6ff` |
| 其他 other | Dim gray | `#484f58` |

### 4.4 Typography

| Element | Font | Size | Weight |
|---------|------|------|--------|
| Page title | `"Noto Sans SC", sans-serif` | 20px | 700 |
| Section heading | Same | 16px | 600 |
| Card title | Same | 13px | 500 |
| Body text | `"Noto Sans TC", "Noto Sans SC", sans-serif` | 14px | 400 |
| Table data | `"Sarasa Mono SC", monospace` | 13px | 400 |
| KPI number | Same | 28px | 700 |

> **Note**: Noto Sans TC (Traditional Chinese) is preferred for labels. Noto Sans SC serves as fallback. Sarasa Mono SC for tabular/numeric data ensures column alignment.

### 4.5 Spacing & Layout

| Token | Value |
|-------|-------|
| Card padding | `16px` |
| Section gap | `16px` |
| Grid gap | `16px` |
| Card border-radius | `8px` |
| Card border | `1px solid var(--border)` |
| Card shadow | `0 0 20px rgba(0,0,0,0.3)` |

---

## 5. Tech Stack Recommendation

### 5.1 Core Stack

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| **Markup** | Vanilla HTML5 | No build step, zero dependencies |
| **Styling** | Tailwind CSS (CDN) | Rapid dark-theme styling, utility-first, CDN-only — no Node.js needed |
| **Charts** | Chart.js 4.x (CDN) | Lightweight, responsive, good radar/doughnut/line support, canvas-based (fast) |
| **Icons** | Lucide Icons (CDN) | Clean, modern, MIT-licensed |
| **Fonts** | Google Fonts CDN (Noto Sans TC, Noto Sans SC, Sarasa Mono SC) | CJK-optimized, free |

### 5.2 Why Chart.js over D3?

| Criterion | Chart.js | D3.js |
|-----------|----------|-------|
| Bundle size | ~65KB gzipped | ~90KB+ (modules) |
| Learning curve | Low — declarative config | High — imperative DOM |
| Radar chart | Built-in | Custom SVG |
| Doughnut chart | Built-in | Custom SVG |
| Time series | Built-in with adapters | Powerful but verbose |
| Animation | Automatic | Manual |
| Responsiveness | Built-in | Manual |
| **Fit for this project** | Best fit | Overkill |

Chart.js handles all required chart types (radar, doughnut, line, bar, area) natively. D3 would add complexity without proportional benefit for a static dashboard.

### 5.3 File Structure

```
dashboard/
├── index.html          # Single-page dashboard
├── css/
│   └── custom.css      # Overrides, CSS variables, animations
├── js/
│   ├── app.js          # Main entry: data loading, initialization
│   ├── charts.js       # Chart.js configurations and renderers
│   ├── tables.js       # Candidate table logic (sort, expand)
│   └── utils.js        # Date formatting, color helpers, regime mapping
├── data/               # Symlink or copy from ../data/
│   ├── regime.json
│   ├── stage1_2026-05-17.json
│   ├── stage2_2026-05-17.json
│   ├── ensemble_2026-05-17.json
│   ├── portfolio_2026-05-17.json
│   ├── paper_trades.json
│   └── market_context_cache.json
└── README.md           # Setup & usage instructions
```

> The `data/` directory in dashboard is a **symlink** to the project's `data/` directory, so the dashboard always reads the latest pipeline output without copying files.

---

## 6. Data Loading & Update Strategy

### 6.1 Loading Flow

```
Page Load
    |
    v
Load state.json --> Determine last_date
    |
    v
Parallel fetch all JSON files (Promise.all):
    ├── regime.json
    ├── stage1_{last_date}.json
    ├── stage2_{last_date}.json
    ├── ensemble_{last_date}.json
    ├── portfolio_{last_date}.json
    ├── paper_trades.json
    └── market_context_cache.json
    |
    v
Process & render each section
    |
    v
Apply animations (fade-in per section)
```

### 6.2 Date Resolution Logic

```javascript
// 1. Load state.json to get last_date
const state = await fetchJSON('data/state.json');
const date = state.last_date;  // e.g., "2026-05-17"

// 2. Construct dated filenames
const stage1Url = `data/stage1_${date}.json`;
const stage2Url = `data/stage2_${date}.json`;
// etc.
```

### 6.3 Data Adapter Layer

Each JSON file has its own schema. An adapter layer normalizes data for charts:

```javascript
// Example: regime adapter
function adaptRegime(data) {
  return {
    label: REGIME_MAP[data.regime],      // 常態, 警戒, ...
    color: REGIME_COLORS[data.regime],    // #3fb950, #d29922, ...
    daysInRegime: data.days_in_regime,
    volatility: data.volatility,
    globalRisk: data.global_risk,
    confidence: data.confidence,
  };
}
```

### 6.4 Update Strategy

| Trigger | Action |
|---------|--------|
| Page load | Fetch all JSONs, render |
| Manual refresh button | Re-fetch all, re-render with animation |
| URL param `?date=YYYY-MM-DD` | Override `last_date`, load specific date's data |

No auto-refresh / WebSocket — the dashboard is designed to be opened after a pipeline run completes.

### 6.5 Error Handling

- If a JSON file is missing (e.g., no ensemble yet): show "資料尚未產生" placeholder in that section
- If `state.json` is missing: show full-page error with instructions to run the pipeline
- Graceful degradation: each section renders independently

### 6.6 Historical Data for Charts

For the P&L and Regime History charts, we need time series:

- **P&L chart**: Aggregate `paper_trades.json` by `exit_date` (or `entry_date` for open positions). Compute cumulative P&L.
- **Regime history**: Store regime snapshots in a new file `data/regime_history.json` (array of `{date, regime, volatility}` entries). If not available, show only current regime as a single point.
- **Cross-asset sparklines**: Use `market_context_cache.json` → `cross_assets.*.history` arrays.

> **Implementation Note**: The pipeline should append to `data/regime_history.json` on each run. This is a small enhancement to `regime_detector.py` that can be added in Phase 40.

---

## 7. Responsive Behavior

The dashboard targets **desktop browsers (1440px+)** primarily, but gracefully adapts:

| Breakpoint | Behavior |
|-----------|----------|
| >=1440px | Full 12-column grid as designed |
| 1024-1439px | Reduce to 8-column, stack right panels below |
| <1024px | Single column, all sections stack vertically |
| <768px | KPI cards wrap to 2-3 per row, tables scroll horizontally |

---

## 8. Animation & Micro-interactions

| Element | Animation |
|---------|-----------|
| Section appearance | Fade-in staggered (50ms delay per section) |
| Regime badge | Subtle pulse glow (box-shadow animation) |
| KPI numbers | Count-up animation on load |
| Chart renders | Chart.js built-in easeOutQuart animation (800ms) |
| Table row hover | Background lighten + left border accent |
| Expandable rows | Slide-down with max-height transition |
| Refresh | Loading spinner → sections update sequentially |

---

## 9. Accessibility Notes

- All charts have `aria-label` descriptions
- Color is never the sole indicator (text labels accompany)
- Table rows are keyboard-navigable
- Sufficient contrast ratios (WCAG AA: 4.5:1 for text, 3:1 for large text)
- Focus visible outlines on interactive elements

---

## 10. Performance Budget

| Metric | Target |
|--------|--------|
| Total JS payload | <200KB (Chart.js ~65KB + app ~30KB gzipped) |
| Total CSS payload | <50KB (Tailwind CDN + custom) |
| First paint | <1s on local file system |
| Chart render | <500ms per chart |
| Memory | <50MB (all data in-memory) |

---

## 11. Implementation Priority (Phase 40 Roadmap)

| Step | Section | Effort |
|------|---------|--------|
| 1 | HTML skeleton + CSS variables + Tailwind CDN | 1h |
| 2 | Header bar + KPI cards | 1h |
| 3 | Top candidates table (with sort/expand) | 2h |
| 4 | Portfolio allocation (donut + bar) | 1.5h |
| 5 | P&L performance chart | 1.5h |
| 6 | Risk indicators (gauge + bars + grid) | 1.5h |
| 7 | Regime history + cross-asset monitor | 1.5h |
| 8 | Signal fusion radar | 1h |
| 9 | Pipeline timeline | 0.5h |
| 10 | Data loading + error handling + refresh | 1h |
| 11 | Animations + polish | 1h |
| **Total** | | **~13h** |
