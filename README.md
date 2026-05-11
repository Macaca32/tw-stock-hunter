# TW Stock Hunter 🦀

Quantitative stock screening pipeline for Taiwan Weighted Stock Exchange (TWSE) & Taipei Exchange (TPEx).

9-phase improvement pipeline completed. Full autonomous iteration via cron + Z.ai review.

## Architecture

```
tw-stock-hunter/
├── core/
│   ├── fetch_data.py            # TWSE Open API batch fetcher (retry + validation gate)
│   ├── fetch_history.py         # Historical price data (365-day lookback)
│   ├── fetch_institutional.py   # Institutional/margin data
│   ├── corporate_actions.py     # Backward-adjustment engine (dividends, stock splits)
│   ├── regime_detector.py       # 5-tier market regime (NORMAL→BLACK_SWAN)
│   ├── stage1_screen.py         # Quant pre-screen + composite scoring
│   ├── stage2_deep.py           # Fundamental deep-dive analysis
│   ├── paper_trader.py          # Backtest engine + survivorship bias correction
│   ├── sectors.py               # 15 sub-sectors + correlation matrix
│   ├── signal_logger.py         # Signal tracking + daily reports
│   └── telegram_alerts.py       # Telegram notifications
├── config/
│   ├── weights.json             # Scoring weights (regime-adjusted, smoothed)
│   ├── thresholds.json          # Hard filter thresholds
│   └── regime_rules.json        # Regime detection rules (v4.0)
├── data/                        # Fetched data + screening results
├── reports/                     # Daily screening reports
└── .improvement-phase.json      # Phase tracker (roadmap state)
```

## Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: DATA FETCH                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │ TWSE Daily   │    │ Price History│    │ Corporate Actions│  │
│  │ API (batch)  │    │ 365 days     │    │ dividends/splits │  │
│  │ retry+backoff│    │ adj_close    │    │ backward-adjust  │  │
│  └──────┬───────┘    └──────┬───────┘    └────────┬─────────┘  │
│         │                   │                      │           │
│  ┌──────▼───────────────────▼──────────────────────▼──────────┐│
│  │  STEP 2: REGIME DETECTION                                  ││
│  │  5-tier: NORMAL → CAUTION → STRESS → CRISIS → BLACK_SWAN   ││
│  │  • Determines position sizing (1.0x → 0.0x)                ││
│  │  • Adjusts scoring weights per regime                       ││
│  │  • 5-day min duration (BLACK_SWAN bypasses freeze)         ││
│  └────────────────────┬───────────────────────────────────────┘│
│                       │                                        │
│  ┌────────────────────▼───────────────────────────────────────┐│
│  │  STEP 3: STAGE 1 - QUANT PRE-SCREEN                        ││
│  │  • Hard filters (liquidity, data quality, delisting check) ││
│  │  • 5-dimension composite scoring (regime-adjusted weights) ││
│  │  • Weight smoothing (max 5% per-day shift)                 ││
│  │  • Sector concentration check (max 25% per sector)         ││
│  └────────────────────┬───────────────────────────────────────┘│
│                       │                                        │
│  ┌────────────────────▼───────────────────────────────────────┐│
│  │  STEP 4: STAGE 2 - FUNDAMENTAL DEEP-DIVE                   ││
│  │  • Revenue growth, margin stability, ROE                   ││
│  │  • Cash flow quality, balance sheet, earnings surprise     ││
│  │  • Management quality, announcements, pledge ratio         ││
│  └────────────────────┬───────────────────────────────────────┘│
│                       │                                        │
│  ┌────────────────────▼───────────────────────────────────────┐│
│  │  STEP 5: REPORT + SIGNAL TRACKING                           ││
│  │  • Backtest validation (raw + survivorship-bias adjusted)   ││
│  │  • Telegram alerts for top picks                            ││
│  │  • Daily report generation                                  ││
│  └────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Data Layer

### Fetch Resilience
- **Retry with exponential backoff** (3 attempts, 1s/3s/9s delays)
- **Validation gate**: Critical TWSE endpoints must return >500 records or pipeline aborts
- **Batch delays**: 100ms between API calls to avoid rate limiting

### Corporate Action Adjustment (Phase 2)
All historical prices are backward-adjusted for dividends and stock splits:

```
adjusted_price = (close - cash_dividend) / (1 + stock_dividend / 100)
```

- 401 stocks with corporate action data
- Adjustment applied from ex-dividend date backward through entire history
- All downstream modules use `adj_close` instead of raw `close`

### Price History
- **365-day lookback** (increased from 25 days in Phase 1)
- **Data quality tiers**: <240 days = low, 240-270 = medium, 270+ = high
- Minimum 240 trading days required for 300-day SMA calculations

## Regime System (Phase 3)

5-tier market regime with position sizing:

| Regime | Position Size | Description |
|--------|-------------|-------------|
| NORMAL | 1.0x | Healthy uptrend, full deployment |
| CAUTION | 0.6x | Warning signs, reduce exposure |
| STRESS | 0.3x | Elevated volatility, defensive |
| CRISIS | 0.1x | Severe stress, minimal exposure |
| BLACK_SWAN | 0.0x | Emergency, no new positions |

**Key behaviors:**
- 5-day minimum duration before regime transitions (prevents whipsaw)
- BLACK_SWAN bypasses the 5-day freeze for immediate emergency exits
- Each regime has its own stop-loss/take-profit thresholds in `regime_rules.json`

## Scoring System

### Stage 1 Weights (Phase 6: smoothed)

| Dimension | Normal | Caution | Stress | Crisis |
|-----------|--------|---------|--------|--------|
| Revenue Momentum | 25% | 20% | 15% | 10% |
| Profitability | 20% | 20% | 25% | 25% |
| Valuation | 10% | 20% | 25% | 30% |
| Institutional Flow | 25% | 20% | 20% | 20% |
| Technical Momentum | 20% | 20% | 15% | 15% |

**Weight smoothing (Phase 6):** Maximum 5% per-day weight change to avoid sudden score shifts that trigger false signals. Previous day's weights are saved and interpolated on regime transitions.

### Stage 2 Weights (fixed)
- Revenue Growth: 20%
- Margin Stability: 15%
- ROE Level: 15%
- Cash Flow Quality: 15%
- Balance Sheet: 15%
- Earnings Surprise: 10%
- Management: 10%

## Sector System (Phase 5)

15 sub-sectors (split from original 8):

| Sector | Industry Codes | Notes |
|--------|---------------|-------|
| semiconductor | 22, 23 | TSMC, UMC, etc. |
| electronics | 24 | Electrical equipment |
| optoelectronics | 25 | LED, solar, displays |
| communications | 26 | Networking, telecom |
| computers | 27 | PC, servers, peripherals |
| components | 28, 29, 30 | Passive components |
| materials | 01-06 | Cement, chemicals, plastics |
| consumer | 07-12 | Textiles, food, beverages |
| industrial | 13-18 | Machinery, auto, tools |
| metals | 19-21 | Steel, metals, shipbuilding |
| financial | 31-33 | Banks, insurance, securities |
| construction | 34-36 | Building, engineering |
| services | 37-39 | Trade, retail, tourism |
| tpex | 90-99 | OTC/TPEx stocks |
| other | — | Unclassified |

**Concentration check:** Max 25% of picks from any single sector. Correlation matrix available via `calc_sector_correlation()`.

## Backtesting & Validation

### Transaction Costs (Phase 0)
- **TWSE:** 0.6% round-trip (stamp duty + commission + exchange fee)
- **TPEx:** 0.7% round-trip

### Survivorship Bias Correction (Phase 4)
Backtest results report both raw and adjusted metrics:
- **12% return haircut** — accounts for delisted stocks not in dataset
- **8% win rate haircut** — conservative adjustment for historical delisting rates
- Adjusted fields: `avg_pnl_pct_adj`, `win_rate_adj`, `total_pnl_pct_adj`
- Tier 2 (scrape delisted stocks from MOF/MOPS) pending

### Ex-Dividend Protection
Stop-loss triggers are adjusted on ex-dividend dates to prevent mechanical price drops from triggering false exits. Corporate action handler provides `is_ex_dividend_date()` and `adjust_price_for_dividend()` helpers.

## Running

```bash
# Full pipeline
cd ~/Desktop/tw-stock-hunter

# Individual stages
python3 core/fetch_data.py --verbose
python3 core/fetch_history.py --verbose --lookback 365
python3 core/regime_detector.py --verbose
python3 core/stage1_screen.py --verbose
python3 core/stage2_deep.py --verbose
python3 core/signal_logger.py --verbose

# Backtest
python3 core/paper_trader.py

# Sector analysis
python3 core/sectors.py
```

## Cron Schedule

- **Weekdays:** Every 30 minutes, 11:00–14:00 Taipei time
- **Weekends:** Every 30 minutes, 00:00–23:30 Taipei time
- **Duplicate prevention:** Each run checks for active sub-agents before proceeding
- **Z.ai integration:** Each iteration sends changes to Z.ai for review before committing

## Improvement Phases (Completed)

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Transaction cost fix (0.3% → 0.6%/0.7%) | ✅ |
| 1 | Data quality threshold (240-day minimum) | ✅ |
| 2 | Corporate action backward-adjustment | ✅ |
| 3 | 5-tier regime system | ✅ |
| 4 | Survivorship bias correction (12% haircut) | ✅ |
| 5 | Sector refinement (15 sub-sectors) | ✅ |
| 6 | Conditional weight smoothing (5% cap) | ✅ |
| 7 | Regime freeze exemption (BLACK_SWAN) | ✅ (in Phase 3) |
| 8 | Integration testing + validation | ✅ |

## Configuration

- **`config/weights.json`** — Scoring weights per regime
- **`config/thresholds.json`** — Hard filter thresholds
- **`config/regime_rules.json`** — Regime entry thresholds, SL/TP levels, position multipliers
- **`.improvement-phase.json`** — Phase tracker for autonomous iteration

## Notes

- All prices use adjusted close (`adj_close`) for corporate actions
- TPEx stocks detected via code prefix (8xxx) and charged higher transaction costs
- Telegram alerts include regime emoji, position sizing, and top picks
- Z.ai session persisted via `.zai-chat-url` for continuous review context
