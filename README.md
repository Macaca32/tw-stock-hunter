# TW Stock Hunter

Quantitative stock screening pipeline for the **Taiwan Weighted Stock Exchange (TWSE)** and **Taipei Exchange (TPEx)** markets. Fetches daily market data, detects market regimes, screens and scores stocks across five quantitative dimensions, performs fundamental deep-dives, and delivers alerts — all orchestrated by a single `run_pipeline.py` entry point.

## Architecture

```
tw-stock-hunter/
├── core/
│   ├── fetch_data.py            # TWSE Open API batch fetcher (retry + validation gate)
│   ├── fetch_history.py         # Historical price data (365-day lookback)
│   ├── fetch_institutional.py   # Institutional/margin data
│   ├── corporate_actions.py     # Backward-adjustment engine (dividends, stock splits)
│   ├── regime_detector.py       # 5-tier market regime (NORMAL → BLACK_SWAN)
│   ├── stage1_screen.py         # Quant pre-screen + composite scoring
│   ├── stage2_deep.py           # Fundamental deep-dive analysis
│   ├── paper_trader.py          # Backtest engine + survivorship bias correction
│   ├── sectors.py               # 15 sub-sectors + correlation matrix
│   ├── signal_logger.py         # Signal tracking + daily reports
│   ├── telegram_alerts.py       # Telegram notifications
│   ├── schemas.py               # Pydantic validation models (10 schemas)
│   ├── datastore.py             # SQLite data layer (read-only + migration)
│   ├── holiday_calendar.py      # ROC holiday calendar + half-day sessions
│   └── logging_config.py        # Structured logging setup
├── learning/
│   ├── backtest.py              # Backtesting framework
│   └── calibrator.py            # Weight calibration
├── config/
│   ├── weights.json             # Scoring weights (regime-adjusted, smoothed)
│   ├── thresholds.json          # Hard filter thresholds
│   └── regime_rules.json        # Regime detection rules (v4.0)
├── scripts/
│   └── backfill_corporate_actions.py  # Historical corporate action backfill (2020-2025)
├── data/                        # Fetched data + hunter.db (SQLite)
├── reports/                     # Daily screening reports
├── tests/
│   ├── conftest.py              # Shared test fixtures
│   ├── test_core.py             # Core module tests (36 tests)
│   └── test_datastore.py        # SQLite data layer tests (44 tests)
└── run_pipeline.py              # Pipeline orchestrator (single entry point)
```

## Pipeline Flow

The pipeline is orchestrated by `run_pipeline.py`, which runs stages sequentially with fail-fast behavior — if any stage fails, all subsequent stages are skipped.

```
┌─────────────────────────────────────────────────────────────────────┐
│  run_pipeline.py --date YYYY-MM-DD -v                               │
│                                                                     │
│  1. fetch_data          TWSE Open API batch fetch (retry + backoff) │
│         │                                                            │
│  2. validate_ingested   Pydantic schema validation gate (>10%       │
│         │               validation failures → abort pipeline)       │
│         │                                                            │
│  3. fetch_history       365-day historical prices + backward adjust │
│         │                                                            │
│  4. regime_detector     5-tier market regime classification         │
│         │                                                            │
│  5. stage1_screen       Quant pre-screen (hard filters + 5-dim      │
│         │               composite scoring, regime-adjusted weights) │
│         │                                                            │
│  6. stage2_deep         Fundamental deep-dive (7 dimensions)        │
│         │                                                            │
│  7. paper_trader        Backtest simulation + survivorship bias adj │
│         │                                                            │
│  8. telegram_alerts     Telegram notifications for top picks        │
└─────────────────────────────────────────────────────────────────────┘
```

Each stage is timed and its success/failure state tracked in `PipelineResult`. After all stages complete, `state.json` is updated with the pipeline summary including stage1/stage2 results, regime, and run metadata.

## Key Features

### 5-Tier Regime System

Market conditions are classified into five regimes, each with its own position sizing, scoring weights, and stop-loss/take-profit thresholds defined in `config/regime_rules.json`:

| Regime | Position Size | Description |
|--------|:------------:|-------------|
| NORMAL | 1.0x | Healthy uptrend, full capital deployment |
| CAUTION | 0.6x | Warning signs present, reduce exposure |
| STRESS | 0.3x | Elevated volatility, defensive positioning |
| CRISIS | 0.1x | Severe market stress, minimal exposure |
| BLACK_SWAN | 0.0x | Emergency mode, no new positions allowed |

The system enforces a 5-day minimum duration before regime transitions to prevent whipsaw, except BLACK_SWAN which bypasses the freeze for immediate emergency response. Regime transitions also trigger weight smoothing (see Scoring System below).

### Corporate Action Backward Adjustment

Taiwan stocks have massive dividend yields (4-8% for many issues). On ex-dividend dates, the stock price mechanically drops by the dividend amount. Without backward adjustment, the pipeline would register these as crashes, trigger false regime signals, hit ATR stop-losses, and distort all SMA/momentum calculations.

The `CorporateActionHandler` applies backward adjustment across the entire price history:

```
adjusted_price = (close - cash_dividend) / (1 + stock_dividend / 100)
```

Data sources in priority order:
1. **TWSE /TWT49U API** — actual ex-dividend/ex-rights dates
2. **dividends_YYYY-MM-DD.json** (t187ap45_L) — dividend declarations
3. **yfinance adjusted close** — fallback for backward adjustment

All downstream modules use `adj_close` instead of raw `close`. The backfill script (`scripts/backfill_corporate_actions.py`) covers 2020-2025 using yfinance for historical corporate actions.

### Holiday-Aware Trading

The `HolidayCalendar` module handles Taiwan-specific trading schedule quirks:

- **Full closures**: National holidays (Spring Festival, Dragon Boat, Mid-Autumn, etc.)
- **Half-day sessions**: Close at 13:00 instead of 13:30 (~40-60% of normal volume), typically before long weekends
- **Makeup workdays** (補行上班日): Saturday trading sessions designated by the government
- **ROC date handling**: All TWSE API dates use Republic of China calendar format (e.g., 1150522 = 2026-05-22); the module provides `roc_date_to_iso()` and `iso_to_roc_date()` conversions
- **Holiday gap detection**: `get_holiday_gaps()` returns risk levels (high ≥ 8 days, medium ≥ 4, low) that feed into position sizing decisions

The calendar auto-loads all `holidays_*.json` files across years and prefers the `isTrading` boolean field over fragile text matching.

### SQLite Data Layer

The `datastore.py` module provides a read-optimized SQLite layer (`data/hunter.db`) sitting on top of existing JSON files:

| Table | Primary Key | Contents |
|-------|-------------|----------|
| `stocks_daily` | (stock_id, date) WITHOUT ROWID | OHLCV + adj_close/adj_volume |
| `corporate_actions` | (stock_id, ex_date) WITHOUT ROWID | cash_div, stock_div, source |
| `regime_snapshots` | date WITHOUT ROWID | regime_label, taiex_close, taiex_change_pct |
| `portfolio_history` | AUTOINCREMENT id | date, action (entry/exit), stock_id, qty, price, pnl |

Key design decisions:
- **Read-only connections** by default (`mode=ro` URI) for safety
- **WAL mode** on read-write connections for concurrent access
- **Idempotent migration**: `migrate_json_to_sqlite()` uses `INSERT OR IGNORE` so it can be run repeatedly
- **Batch query helpers**: `get_daily_history_batch()` for bulk lookups
- CLI: `python core/datastore.py --migrate --info`

### Pydantic Validation

All data entering the pipeline is validated against Pydantic models defined in `schemas.py`. The `batch_validate()` utility validates lists of records, logs the first 3 errors, and warns when more than 5% of records fail validation. Key models include:

- **`DailyStockRecord`** — TWSE daily stock data with Chinese/English key mapping and closing_price > 0 validator
- **`PERatioRecord`** — P/E, P/B, and dividend yield with alias support and empty-string-to-None coercion
- **`CompanyInfo`** / **`RevenueRecord`** — Company fundamentals with Chinese field aliases
- **`CorporateAction`** — Date format validation and source tracking
- **`PricePoint`** — Historical price with `adj_close` defaulting to `close` via model_validator
- **`Stage1Candidate`** / **`Stage2Candidate`** — Pipeline output with `extra="forbid"` to catch unexpected fields
- **`RegimeOutput`** — Regex-validated regime and risk level enums

The `normalize_keys()` function translates 21 Chinese field names to English canonical names before validation.

### Structured Logging

All modules use the centralized `logging_config.py` setup:

- Standard format: `YYYY-MM-DD HH:MM:SS [module.name] LEVEL: message`
- Verbose mode (`-v`) → DEBUG level; quiet mode → WARNING+ only
- Optional file handler (always DEBUG level for full audit trail)
- Noisy third-party loggers (urllib3, requests, yfinance) suppressed to WARNING

## Taiwan Market Specifics

### Transaction Costs

The pipeline applies realistic round-trip transaction costs that differ by market:

| Market | Round-Trip Cost | Components |
|--------|:--------------:|------------|
| TWSE | **0.6%** | Stamp duty (0.3%) + commission + exchange fee |
| TPEx | **0.7%** | Higher OTC fees + commission + exchange fee |

TPEx stocks are detected via code prefix (8xxx) and automatically charged the higher rate.

### ROC Date Handling

All TWSE API endpoints return dates in Republic of China (ROC) format. The pipeline handles conversion transparently:
- ROC year 115 = CE year 2026 (ROC year = CE year - 1911)
- Example: `1150522` → `2026-05-22`
- Used throughout `holiday_calendar.py`, `fetch_data.py`, and `corporate_actions.py`

### Stock Dividends (配股)

Taiwan companies frequently issue stock dividends in addition to cash dividends. The backward-adjustment engine handles both:
- **Cash dividend (配息)**: Subtract from close price
- **Stock dividend (配股)**: Divide by (1 + stock_dividend/100) to account for share dilution
- TWT49U data takes priority over dividend declarations for accuracy

### Half-Day Trading Sessions

Taiwan markets operate on shortened hours before certain holidays:
- **Full day**: 09:00–13:30 (6.5 hours)
- **Half day**: 09:00–13:00 (4 hours)
- The `get_session_hours()` function returns open/close times and total hours
- Volume expectations are scaled down (~40-60% of normal) on half-day sessions

## Scoring System

### Stage 1: 5-Dimension Composite Scoring

Each stock is scored across five quantitative dimensions. Weights are regime-adjusted and smoothed:

| Dimension | NORMAL | CAUTION | STRESS | CRISIS |
|-----------|:------:|:-------:|:------:|:------:|
| Revenue Momentum | 25% | 20% | 15% | 10% |
| Profitability | 20% | 20% | 25% | 25% |
| Valuation | 10% | 20% | 25% | 30% |
| Institutional Flow | 25% | 20% | 20% | 20% |
| Technical Momentum | 20% | 20% | 15% | 15% |

**Hard filters** (applied before scoring):
- Minimum liquidity threshold
- Data quality check (minimum trading history)
- Delisting warning filter
- Sector concentration check (max 25% of picks from any single sector)

**Weight smoothing** (Phase 6): Maximum 5% per-day weight change to prevent sudden score shifts that trigger false signals. Previous day's weights are saved and interpolated during regime transitions.

### Stage 2: Fundamental Deep-Dive

Seven fixed-weight dimensions for stocks that pass Stage 1:

| Dimension | Weight |
|-----------|:------:|
| Revenue Growth | 20% |
| Margin Stability | 15% |
| ROE Level | 15% |
| Cash Flow Quality | 15% |
| Balance Sheet | 15% |
| Earnings Surprise | 10% |
| Management Quality | 10% |

### O(1) Index Lookups (Phase 22)

All `score_*` functions in `stage1_screen.py` accept pre-built index dictionaries for O(1) lookups instead of O(n) linear scans. Indexes are built once per pipeline run via `_index_single()` and passed as keyword arguments:

```python
daily_index = _index_single(daily_data, code_keys=("證券代號", "Code"))
score_technical_momentum(code, daily_data, price_history=price_history,
                         daily_index=daily_index)
```

When `daily_index` is `None` (backward compatibility), functions fall back to the original O(n) scan.

## Backtesting & Validation

### Transaction Costs
- **TWSE**: 0.6% round-trip (stamp duty + commission + exchange fee)
- **TPEx**: 0.7% round-trip (higher OTC fees)

### Survivorship Bias Correction

Backtest results report both raw and bias-adjusted metrics:
- **12% return haircut** — accounts for delisted stocks not present in the dataset
- **8% win rate haircut** — conservative adjustment for historical delisting rates
- Adjusted fields: `avg_pnl_pct_adj`, `win_rate_adj`, `total_pnl_pct_adj`

### Ex-Dividend Protection

Stop-loss triggers are adjusted on ex-dividend dates to prevent mechanical price drops from triggering false exits. The `CorporateActionHandler` provides `is_ex_dividend_date()`, `adjust_price_for_dividend()`, and `should_skip_stop_check()` helpers.

## Setup

### Prerequisites

- Python 3.9+
- pip

### Install

```bash
git clone https://github.com/Macaca32/tw-stock-hunter.git
cd tw-stock-hunter
pip install -r requirements.txt
```

Dependencies:
- `requests` — TWSE API calls
- `python-dateutil` — date parsing
- `pydantic` — data validation
- `yfinance` — historical data backfill
- `pytest` — test runner

### Run the Pipeline

```bash
# Full pipeline for today
python run_pipeline.py --date 2026-05-16 -v

# The --date flag accepts any trading day in YYYY-MM-DD format
# The -v/--verbose flag enables DEBUG-level logging
```

The pipeline will:
1. Fetch fresh market data from TWSE APIs
2. Validate all ingested data against Pydantic schemas
3. Build/append 365-day price histories with backward adjustment
4. Detect the current market regime
5. Screen and score all listed stocks (Stage 1)
6. Deep-dive on top candidates (Stage 2)
7. Run paper trading simulation
8. Send Telegram alerts (if configured)

### Run Individual Stages

```bash
python core/fetch_data.py --verbose
python core/fetch_history.py --verbose --lookback 365
python core/regime_detector.py --verbose
python core/stage1_screen.py --verbose
python core/stage2_deep.py --verbose
python core/paper_trader.py
python core/sectors.py
```

### SQLite Migration

```bash
# Migrate existing JSON data to SQLite
python core/datastore.py --migrate

# View database summary
python core/datastore.py --info

# Query specific stock history
python core/datastore.py --query-daily 2330 --since 2025-01-01 --limit 30
```

## Testing

```bash
# Run all tests
pytest tests/

# Run with verbose output
pytest tests/ -v

# Run specific test module
pytest tests/test_core.py -v
pytest tests/test_datastore.py -v
```

The test suite contains **80+ tests** covering:

| Module | Tests | Coverage Area |
|--------|:-----:|---------------|
| `test_core.py` | 36 | RSI (Wilder smoothing), backward adjustment, weight smoothing, holiday calendar, holding days, ROC date conversion |
| `test_datastore.py` | 44 | SQLite migration, CRUD operations, batch queries, schema validation, idempotent migration, WAL mode |

## Configuration

| File | Purpose |
|------|---------|
| `config/weights.json` | Scoring weights per regime (Stage 1 + Stage 2) |
| `config/thresholds.json` | Hard filter thresholds (liquidity, data quality, delisting) |
| `config/regime_rules.json` | Regime entry thresholds, stop-loss/take-profit levels, position multipliers, freeze duration |

## Improvement Phases

24 phases of iterative improvement have been completed:

| Phase | Description |
|:-----:|-------------|
| 0 | Transaction cost fix — TWSE 0.6%, TPEx 0.7% round-trip |
| 1 | Data quality threshold — 240-day minimum price history |
| 2 | Corporate action backward-adjustment engine (401 stocks) |
| 3 | 5-tier regime system (NORMAL → CAUTION → STRESS → CRISIS → BLACK_SWAN) |
| 4 | Survivorship bias Tier 1 correction (12% return haircut, 8% win rate haircut) |
| 5 | Sector refinement — 6 technology sub-sectors from original 8 broad sectors |
| 6 | Conditional weight smoothing — max 5% per-day weight shift to prevent false signals |
| 7 | Regime freeze exemption — BLACK_SWAN bypasses 5-day minimum duration |
| 8 | Integration testing + validation |
| 9 | Pydantic schema enforcement — validation error rate from 44.5% to 0.2% |
| 10 | Holiday calendar with half-day sessions and makeup workday support |
| 11 | ROC date conversion consolidation and deduplication |
| 12 | Pipeline data validation gates — abort on >10% validation failures |
| 13 | Corporate action backfill — 2020-2025 historical data via yfinance |
| 14 | Sector concentration check — max 25% per sector in screened picks |
| 15 | Signal logger and daily report generation |
| 16 | Telegram alert integration with regime emoji and position sizing |
| 17 | Ex-dividend stop-loss protection to prevent false exits |
| 18 | Volume profile analysis on half-day trading sessions |
| 19 | O(n²) → O(1) scan fix for institutional flow scoring |
| 20 | Pipeline runner (`run_pipeline.py`) with fail-fast and timing |
| 21 | Unit test suite (36 tests) + structured logging |
| 22 | Full repo re-review — 12 fixes including index lookups, unknown regime handling |
| 23 | SQLite data layer — read-only view on top of JSON files, 44 new tests |
| 24 | Regression fix (daily_index parameter) + README rewrite |

## Notes

- All prices use adjusted close (`adj_close`) for corporate actions throughout the pipeline
- TPEx stocks detected via code prefix (8xxx) and charged higher transaction costs
- Telegram alerts include regime emoji, position sizing, and top picks
- The SQLite layer is read-only by default; migration is idempotent and can be run repeatedly
- `state.json` tracks the last pipeline run summary for dashboard consumption
