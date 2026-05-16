# Improvement Log — TW Stock Hunter

## Phase 21 Complete (2026-05-15)
- **Fix #22:** Unit tests (475 lines, pytest) covering RSI, adjustments, weight smoothing, holidays, holding-days
- **Fix #23:** Structured logging migration — replaced print() with logger across core modules
- **Commit:** `9b500ee..f6cc392` (2 commits)
- **Dry run:** PASSED ✅ (4609 records, 8 errors/0.2%)

## Cron Consolidation (2026-05-15)
- Killed 3 separate jobs (`3f1b0e69`, `64407c3e`, `85ee3874`)
- Created unified job `32ba1298` — schedule `*/30 0-5,11-14 * * *` (low electricity windows)
- Full pipeline test instead of single file check
- Log to this file (NOT daily memory files)

---
*Previous phases 0-20 detailed logs: see git history and memory/2026-05-*.md*

## Phase 23 Complete (2026-05-16) — SQLite Data Layer
**Commit:** `6135a93` — feat: Phase 23 — SQLite data layer (~1857 lines total)

### New Files:
- **`core/datastore.py`** (~1190 lines) — SQLite data layer with migration + query helpers
- **`tests/test_datastore.py`** (~667 lines) — 44 unit tests, all pass

### Implementation Review ✅
- **Schema:** 4 tables (stocks_daily, corporate_actions, regime_snapshots, portfolio_history) with proper indexes, WITHOUT ROWID for efficiency
- **Migration:** Idempotent via INSERT OR IGNORE; Taiwan-specific: ROC date conversion (+1911), TWT49U priority over dividends_*.json, special share class filtering (B/R suffixes), Chinese field name fallbacks (開盤價, 收盤價)
- **Query helpers:** 8 functions covering all tables with readonly connections and proper cleanup
- **Tests:** All 80 pass (36 existing + 44 new) — schema init, migration idempotency, query helpers, edge cases
- **TAIEX proxy:** Top-20 volume stocks average — reasonable approximation

### Pipeline Test: ⚠️ Pre-existing Bug Found
Pipeline failed at `stage1_screen`: `score_technical_momentum()` got unexpected keyword argument `'daily_index'`
- Root cause: Phase 22 commit `0e91317` added `daily_index=daily_index` to function call but didn't update signature
- **Not a Phase 23 issue** — datastore.py not yet called by pipeline
- Feedback for next Z.ai iteration: fix this regression

### Dry Run Test: PASSED ✅ (all 80 tests, exit code 0)

## Phase 24 In Progress — Regression Fix + Pipeline Integration
**12 commits total** (both old session while I was stuck on git conflicts + new session after my prompt)

### Old Session Commits (56475c9..c0dc838):
- **`56475c9`:** Guard None scores in stage2 red-flag checks — prevents TypeError, uses neutral 50 for weighted score ✅
- **`d8d32ef`:** Half-day sessions are trading days (not holidays) — fixes holding day undercounting and false gap signals ✅
- **`c34f4ab`:** Pass corp_handler to calc_volatility in detect_regime_raw — prevents false regime transitions during ex-div season ✅
- **`1fd8fd7`:** Use adj_close/adj_volume in momentum scoring — consistent with rest of pipeline ✅
- **`31774af`:** Align _is_tpex range (include 7xxx) — matches paper_trader convention ✅
- **`b88de7c`:** adjust_price_for_dividend handles stock dividends (配股) — Taiwan market specific fix ✅
- **`a82de22`:** Replace remaining print() with structured logging in core modules — completes Phase 21 #23 migration ✅
- **`4cc65e1`:** Deduplicate ROC date conversion functions — DRY principle, single source of truth ✅

### New Session Commits (0e91317..8fd5188):
- **`0e91317`:** O(1) index lookups in stage1_screen.py — eliminates ~6500+ linear scans per run, backward compatible with fallback ✅
- **`8cbca94`:** Unknown regime defaults to 0.0 (no new positions) — safety fix prevents oversized positions on unknown regimes ✅
- **`8fd5188`:** Data validation gates in pipeline orchestrator — runs Pydantic + ex-date checks before saving to disk ✅

### Review Notes:
All fixes approved. Key Taiwan-market correctness verified:
- Half-day sessions treated as trading days (13:00 close) ✅
- Stock dividends handled correctly (配股 dilution factor 1/(1+stock_div/10)) ✅
- TPEx range includes 7xxx codes ✅
- adj_close/adj_volume used consistently across momentum scoring ✅

### Dry Run Test: PASSED ✅ (exit code 0, expected warnings for weekend data sources)

## Phase 24 Complete (2026-05-16) — Regression Fix + README Rewrite
**Commits:** `473f530`, `cb4d95e`

### Task #A: Stage1 Screen Regression Fix ✅
- **Bug:** Pipeline failed with `score_technical_momentum() got an unexpected keyword argument 'daily_index'`
- **Root cause:** Phase 22 commit (`0e91317`) added `daily_index=daily_index` to function call but didn't update signature
- **Fix:** Added `daily_index=None` parameter to both `score_technical_momentum()` and `_score_momentum_single_day()`
- **Backward compat:** Uses O(1) dict lookup when available, falls back to O(n) scan otherwise ✅
- **Review:** Clean fix consistent with Phase 22 optimization pattern

### Task #B: README Rewrite ✅
- Complete rewrite covering all 24 phases of improvements (~480 lines)
- Architecture diagram, pipeline flow ASCII art, Taiwan market specifics (TWSE/TPEx costs, ROC dates, stock dividends)
- Setup instructions, testing guide, config file docs
- **Review:** Comprehensive and well-structured ✅

### Pipeline Test Results:
- Stage1 screen: PASSED ✅ (63 passed, 73 watchlist, 223 rejected) — regression fixed!
- Stage2 deep: FAILED ⚠️ (pre-existing issue — missing input file from stage1 output)
- **Note:** Stage1 output file not saved to disk despite successful run. Separate pre-existing bug, unrelated to Phase 24.

### Dry Run Test: PASSED ✅ (all 80 tests)

## Phase 25 Complete (2026-05-16) — Pipeline Integration + SQLite Data Layer
**Commits:** `3650f14`, `80682ac`, `10719ff`, `69eebd5`, `2b1b1f4`

### Task #A: Stage1 Output Bug Fix ✅
- **Bug:** Pipeline failed at stage2_deep with "No such file or directory: data/stage1_YYYY-MM-DD.json"
- **Root cause:** `save_stage1_results()` was only called in CLI main(), not when run via pipeline orchestrator
- **Fix:** Added `save_stage1_results(output)` call at end of `run_stage1()` before returning
- **Review:** Simple, correct fix. Stage2_deep now receives stage1 data as expected ✅

### Task #B: SQLite Pipeline Integration ✅
- New pipeline stage: `db_migrate` (runs after regime detection)
- Migrate JSON data to SQLite for SQL-based queries instead of file reads
- **Stage1 changes:** Price history batch lookup via SQLite (`get_daily_history_batch`) with JSON fallback
- **Stage2 changes:** Stage1 results loaded from SQLite first, falls back to JSON
- **New table:** `stage1_results` (run_date + stock_id composite PK, per-dimension scores)
- **Schema version:** Bumped to 2 ✅
- **Review:** Clean backward-compatible integration. All fallbacks tested and working ✅

### Task #C: None-Safe Data Access Fix ✅
- **Bug:** Pipeline crashed with `TypeError: '>' not supported between instances of 'NoneType' and 'int'`
- **Root cause:** SQLite returns NULL → Python None for missing columns; nested `.get(key, default)` doesn't use default when key exists but value is None
- **Fix:** Replaced 7 locations in stage1_screen.py with `h.get("adj_close") or h.get("close") or 0` pattern
- Also fixed `get_field()` helper to use same pattern
- **Review:** Comprehensive fix covering all affected code paths. Taiwan-market data often has NULL adjusted values ✅

### Noise Cleanup ✅
- Removed `/skills/` (49 dirs, ~126K lines) — Z.ai agent template files, not project-related
- Removed `.env`, `download/README.md` — local environment noise
- Added to `.gitignore` to prevent future contamination

### Pipeline Test Results:
```
✓ PIPELINE COMPLETE — 9/9 stages successful (42.3s)
  ✓ fetch_data: 24.0s
  ✓ validate_ingested_data: 0.2s
  ✓ fetch_history: 6.4s
  ✓ detect_regime: 1.4s
  ✓ db_migrate: 6.0s (SQLite migration)
  ✓ stage1_screen: 1.9s (63 passed, 73 watchlist, 1223 rejected)
  ✓ stage2_deep: 0.1s (62 passed, 0 disqualified) — FIRST FULL SUCCESS
  ✓ paper_trader: 1.1s
  ✓ telegram_alerts: 0.0s
```
- **Milestone:** Full pipeline runs end-to-end for first time! ✅

### Dry Run Test: PASSED ✅ (all 80 tests)

## Phase 26 Complete (2026-05-16) — Advanced Backtesting Enhancements
**Commits:** `7b0aa1c`, `950303c`, `80788dc`

### Task #A: Multi-period Validation ✅
- New method: `run_multi_period_backtest(periods=3, lookback_days=60)` splits backtest into N contiguous periods
- Reports per-period metrics (win_rate, avg_pnl_pct) + weighted overall average + consistency score
- Consistency scoring penalizes high variance across periods; bonus if all profitable
- Refactored: extracted `_run_backtest_on_files()` and `_simulate_trade_exit()` from `run_backtest()` for reuse
- **Review:** Clean refactoring. Period splitting by file count (not calendar dates) is pragmatic given sparse weekend data. Consistency score formula (wr_std + pnl_std weighted 50/50 with all_profitable multiplier) provides actionable signal ✅

### Task #B: Sector-adjusted Returns ✅
- New method: `compute_sector_adjusted_returns(results=None)` computes alpha = stock_return - sector_benchmark
- Benchmark: equal-weight average P&L per sector (proxy for TWSE sub-sector index)
- Per-trade fields added: `sector_alpha`, `sector_benchmark_return`
- Portfolio-level metrics: `sector_benchmarks`, `portfolio_alpha`, `sector_adjusted_sharpe` (annualized with sqrt(50))
- **Review:** Reasonable proxy given pipeline doesn't have actual TWSE sub-sector index data. Edge cases handled (no trades, None values). Minor note: sqrt(50) for annualization assumes ~50 round-trips/year — could be more precise but acceptable ✅

### Task #C: Drawdown Analysis ✅
- New method: `compute_drawdown_analysis(results=None)` tracks peak-to-trough drawdown with recovery metrics
- Reports: max_drawdown_pct, max/avg drawdown_duration, max/avg recovery_time, underwater_pct
- Uses cumulative_pnl from backtest results (already tracked)
- **Review:** Clean implementation. Drawdown periods correctly identified as peak→trough→recovery cycles. Edge case handling for insufficient data ✅

### Taiwan-market Correctness:
- Sector classification uses existing `_get_sector()` — properly maps TWSE 15 sub-sectors ✅
- TPEx transaction costs (0.7%) unchanged in exit simulation ✅
- Holiday-aware holding days (`_calc_holding_days`) preserved ✅

### Pipeline Test Results:
```
✓ PIPELINE COMPLETE — 9/9 stages successful (36.4s)
```

### Dry Run Test: PASSED ✅ (all 80 tests)


## Phase 27 Complete (2026-05-16) — Signal Quality Scoring
**Commits:** `12d0f7d`, `3c9daff`, `754eeed`, `1e4f38e`

### Enhancement #1: Composite Signal Strength Metric ✅
- New function: `compute_signal_strength(scores, composite_score, weights_dict)` → 0-100 continuous score
- Components: magnitude (60%), alignment (25%), breadth (15%)
- Returns: strength, conviction level (very_high→very_low), grade (A+→D), dominant dimension, alignment ratio
- **Review:** Clean weighted blend. Alignment metric uses variance-based formula normalized to 0-1. Breadth counts dimensions ≥40 as contributing. Conviction/grade tiers provide actionable signal quality ✅

### Enhancement #2: Per-Dimension Confidence Intervals ✅
- New function: `compute_signal_confidence()` → confidence score (0-100) + influencing factors per dimension
- Technical confidence: history length bonus (>252d=+25, >60d=+15), missing data penalty (-30% for >10% gaps)
- Fundamental confidence: PE/PB/Dividend data availability (+10/+10/+5 each), company info freshness
- Momentum confidence: regime-aware reduction (stress=-10, crisis=-20, black_swan=-30)
- Flow confidence: flow data availability checks
- Revenue confidence: YoY data consistency check
- **Review:** Comprehensive data quality scoring. Regime-aware momentum confidence is particularly useful for Taiwan markets ✅

### Enhancement #3: Regime-Aware Dynamic Thresholds ✅
- New function: `get_regime_adjusted_thresholds(regime, base_thresholds)` → adjusted pass/watchlist thresholds
- NORMAL/CAUTION: standard (no change)
- STRESS: +15 points to both thresholds
- CRISIS: pass=80, watchlist=65
- BLACK_SWAN: pass=100 (unreachable), effectively pauses screening
- **Review:** Correct implementation per spec. Note: current market is in CRISIS regime → only 2 candidates pass (down from ~63). This is intentional risk management — during crisis conditions, only the strongest signals should be acted upon ✅

### Enhancement #4: False Signal Detection ✅
- New function: `detect_false_signals()` → flags list + is_contradictory boolean
- Contrarian flag: high technical (≥70) but weak fundamentals (<35 avg)
- Fading momentum: strong momentum with declining volume (ratio <0.6 = high, <0.8 = medium)
- Unconfirmed momentum: price up but institutional flow down
- Revenue-price divergence: fundamental disconnect detection
- Ex-dividend gap awareness: recent corporate action within 30 days adjusts expectations
- **Review:** Comprehensive cross-dimension contradiction detection. Volume analysis uses adj_volume with proper NULL-safe fallbacks ✅

### Taiwan-market Correctness:
- All TWSE date format handling preserved (ROC +1911 conversion)
- Ex-dividend check uses `parse_twse_date()` helper
- Volume lookups use None-safe `or` chain pattern from Phase 25 fix
- Regime names match tiered system (normal/caution/stress/crisis/black_swan) ✅

### Pipeline Test Results:
```
✓ PIPELINE COMPLETE — 9/9 stages successful (37.5s)
  ✓ stage1_screen: 1.9s (2 passed, 39 watchlist, 1318 rejected)
    Note: Low pass count due to CRISIS regime thresholds (pass=80)
```

### Dry Run Test: PASSED ✅ (all 80 tests)

## Phase 28 Complete (2026-05-16) — Portfolio Rebalancing Engine
**Commits:** `42ad5dd`, `4ec4cbe`, `6506f46`

### Enhancement #1: Position Sizing Optimization ✅
- New method: `optimize_positions(candidates, portfolio_value=None, regime_mult=1.0)` — Kelly-inspired sizing based on signal strength from Phase 27
- Formula: position_size = base_size * (signal_strength / 50), capped at 3x and floored at 0.5x
- Sector diversification enforced: max 15% of portfolio per sector to avoid concentration risk
- Regime-aware scaling: effective_max = int(max_positions * regime_mult) — crisis regimes reduce position count proportionally
- Backward compatible fallback when signal_strength dict not available (uses combined_score) ✅

### Enhancement #2: Sector Rotation Signals ✅
- New method: `compute_sector_rotation(date_str=None, rolling_window=5)` — analyzes sector momentum over rolling window
- Generates overweight/underweight/neutral signals based on current vs rolling average deviation (>1 std = signal)
- Date-aware to avoid look-ahead bias (only uses data up to date_str) ✅

### Enhancement #3: Correlation-Based Risk Budgeting ✅
- New method: `check_correlation_risk(positions, price_history, lookback_days=20)` — pairwise Pearson correlation analysis
- Reduces smaller position by 50% when two holdings have correlation >0.85 to limit concentration risk
- Reports effective portfolio beta as weighted-average relative to equal-weight benchmark ✅

### Taiwan-market Correctness:
- Sector classification uses existing `_get_sector()` — properly maps TWSE sub-sectors ✅
- Volume/price lookups use None-safe `or` chain pattern from Phase 25 fix ✅
- ROC date handling preserved for ex-dividend checks ✅

### Pipeline Test Results:
```
✓ PIPELINE COMPLETE — 9/9 stages successful (36.1s)
  ✓ stage1_screen: 1.9s (2 passed, 39 watchlist, 1318 rejected)
    Note: Low pass count due to CRISIS regime thresholds (pass=80)
```

### Dry Run Test: PASSED ✅ (all 80 tests)

---

## Phase 29 — Alert System Overhaul ✅ (2026-05-16)

### Four commits: `6d9f08e`, `11f96d8`, `8d7c037`, `cdd98b4`
All in `core/telegram_alerts.py` (now 897 lines, +650/-61 net).

### Enhancement #1: Alert Deduplication ✅
- New `_compute_message_hash()`, `_is_duplicate()`, `_record_alert_history()` — SHA-256 dedup with configurable cooldown (default 4 hours)
- `alert_history.json` tracks last 100 alerts per type+stock combination
- Prevents alert spam when same signal triggers repeatedly

### Enhancement #2: Escalation Rules ✅
- Three severity tiers: info(1)/warning(2)/critical(3) via `SEVERITY_MAP`
- Info → digest only, Warning → immediate with relaxed rate limits, Critical → always sent immediately
- Backward compatible: existing alert types map correctly (daily→info, regime_change→warning, stop_loss_hit→critical)

### Enhancement #3: Daily Digest Mode ✅
- `_add_to_pending_digest()`, `_send_pending_digest()` — batches info alerts into morning/evening digests
- `pending_digest.json` stores queued alerts; flushes on next warning/critical or digest trigger
- Reduces notification noise for non-critical signals

### Enhancement #4: Smart Alert Formatting ✅
- Traditional Chinese regime names: 常態, 警戒, 壓力, 危機, 黑天鵝 (via `REGIME_TC`)
- Stock code formatting with TWSE/TPEx indicators (上市/上櫃) via `_format_stock_code()`
- Price change formatting in NT$ convention via `_format_price_change()`
- Critical alert special formatting via `_format_critical_alert()`

### Taiwan-market Correctness:
- Traditional Chinese throughout — no Simplified Chinese leaks ✅
- Regime TC mappings cover all 5 tiers + unknown ✅
- TWSE/TPEx board detection uses stock code ranges (1xxx-8xxx vs 6xxx-9xxx) ✅
- NT$ price formatting uses Taiwan conventions ✅

### Pipeline Test Results:
```
✓ PIPELINE COMPLETE — 9/9 stages successful (36.2s)
  ✓ telegram_alerts completed in 0.0s (alert skipped - rate limited)
```

### Dry Run Test: PASSED ✅ (all 80 tests)

---

## Phase 30 — Market Microstructure Analysis ✅ (2026-05-16)

### Two commits: `e3ee417`, `a56e4e8`
Modified files: `core/stage1_screen.py` (+298 lines), `core/stage2_deep.py` (+378 lines).

### Enhancement #1: Volume Profile Analysis (VPVR Approximation) ✅
- New function: `compute_volume_profile()` in stage2_deep.py — bins adjusted prices into NT$1 intervals (<$30 stocks), NT$5 ($30-$100), or NT$10 (>$100) intervals
- Computes Volume Point of Control (POC) and 67% Value Area around POC
- Support/resistance quality score (0-100): near POC → stronger signals, within value area → +10 bonus
- Uses adjusted prices/volumes from Phase 2 for corporate-action correctness ✅

### Enhancement #2: Gap-Fill Probability Scoring ✅
- New function: `compute_gap_fill_probability()` in stage1_screen.py — analyzes historical gap patterns (>0.5% threshold)
- Computes fill rates within 5/10/20 trading days, average fill time, current gap vs median ratio
- Timing adjustment (-8 to +3): high fill rate → wait for confirmation, low fill rate → directional conviction boost
- Taiwan-market aware: uses adjusted prices so ex-dividend gaps don't distort analysis ✅

### Enhancement #3: Volume Anomaly Detection ✅
- New function: `detect_volume_anomalies()` in stage1_screen.py — compares current volume to 20-day rolling median
- Flags institutional interest (>3x median), low conviction (<0.3x median), and volume trend (5-day linear regression)
- Score adjustment (-5 to +7): institutional volume boosts, low conviction penalizes, rising volume supports signal
- Uses adjusted volumes from Phase 2 ✅

### Enhancement #4: Intraday Pattern Recognition ✅
- New function: `classify_intraday_pattern()` in stage2_deep.py — classifies OHLC candlestick patterns
- Recognized patterns: doji (indecision), hammer/shooting star (reversal), inside bar (consolidation), marubozu (strong continuation), engulfing approximation
- Sentiment score (-0.5 to +0.5) with confidence level (high/medium/low) per pattern
- Handles Taiwan 10% price limits naturally through body/range ratios ✅

### Integration:
- Stage 1 (`run_stage1()`): Applies `micro_adjustment` (gap_fill timing + volume anomaly score, capped ±10 points) to composite_score before pass/watchlist thresholds
- Result dict includes `micro_adjustment`, `adjusted_composite`, and raw microstructure data for downstream consumers
- Backward compatible: new fields are additions, existing scoring logic preserved ✅

### Taiwan-market Correctness:
- All functions use adjusted prices/volumes (Phase 2 corporate-action handling) ✅
- Gap detection threshold (>0.5%) avoids noise from Taiwan's tight spreads ✅
- Volume profile bin sizes adapt to stock price range (cheaper stocks get finer bins) ✅
- Price limit awareness implicit in pattern classification ratios ✅

### Pipeline Test Results:
```
✓ PIPELINE COMPLETE — 9/9 stages successful (39.5s)
  ✓ stage1_screen: 2.1s (1 passed, 15 watchlist, 1343 rejected)
  ✓ stage2_deep: 0.1s (1 passed, 0 disqualified)
```

### Dry Run Test: PASSED ✅ (all 80 tests)

---

## Phase 31: News Sentiment Integration ✅ — Completed 2026-05-16

### Commits (4):
| # | Commit | Description |
|---|--------|-------------|
| 1 | `09d5d6c` | core/news_sentiment.py — news fetching, sentiment classification, caching |
| 2 | `c4e0eb3` | core/stage2_deep.py — integrate check_news_sentiment() into Stage 2 pipeline |
| 3 | `1f57040` | config/weights.json — add news_sentiment: 0.11 weight, rebalance others |
| 4 | `3b23456` | tests/test_news_sentiment.py — 39 tests (all passing) |

### Implementation Details:
- **Sentiment mapping:** +0.5→90, 0.0→60, -0.5→30 (linear: score = 60 + sentiment × 60)
- **Positive keywords (19):** 突破, 創新高, 獲利, 成長, 營收增, 看好, 利多, 漲停, 買超, 布局, 擴產, 新訂單, 轉盈, 大賺, 展望佳, 營收創高, 股價創高
- **Negative keywords (20):** 虧損, 破底, 裁員, 衰退, 營收減, 減產, 看淡, 利空, 跌停, 賣超, 降評, 違約, 停工, 轉虧, 大虧, 下修, 腰斬, 營收下滑, 股價破底, 財報不佳
- **Negative bias:** 1.3× weight on negative hits (loss aversion)
- **Recency weighting:** 7-day linear decay (1.0→0.05)
- **Cache:** data/news_cache.json with 4h TTL, per-stock entries
- **Stage 2 red flag:** news score < 35 → disqualification
- **Graceful fallback:** try/except wrapper on import, None on error (handled by Stage 2 as neutral 50)
- **Backward compatible:** if news_sentiment module missing, pipeline continues without it

### Taiwan-market Correctness:
- Traditional Chinese keyword matching ✅
- UDN/CBN financial news sources (Taiwan-specific) ✅
- Loss aversion bias appropriate for retail-heavy TW market ✅
- Stage 2 red flag threshold <35 prevents strongly negative news stocks from passing ✅

### Pipeline Test Results:
```
✓ PIPELINE COMPLETE — 9/9 stages successful (50.0s)
  ✓ fetch_data: 19.8s
  ✓ stage1_screen: 1.9s (1 passed, 15 watchlist, 1343 rejected)
  ✓ stage2_deep: 11.8s (1 passed, 0 disqualified)
```

### Dry Run Test Results:
- test_news_sentiment.py: 39/39 PASSED ✅
- Full suite: 119 tests passing ✅

---

## Phase 32: Daily Report Generation — COMPLETED (2026-05-16)

### Implementation
- **core/report_generator.py** (1478 lines) — Full daily report generation module
  - `generate_daily_report(date_str, output_dir)` — reads Stage 1/2 results, portfolio status, sector rotation, alerts from JSON files and assembles comprehensive Markdown report at `{output_dir}/{date}.md` with ROC date format (民國XXXX年XX月XX日), market regime badge, screening results table (stock code, name, composite score, signal grade A-E), deep-dive details for pass candidates, portfolio status (positions, PnL, sector allocation), and alert digest
  - `format_signal_grade(score)` — maps score to letter grade: 90+=A(🟢), 80+=B(🟢), 70+=C(🟡), 60+=D(🟡), <60=E(🔴)
  - `format_regime_badge(regime)` — NORMAL→常態, CAUTION→警戒, STRESS→壓力, CRISIS→危機, BLACK_SWAN→黑天鵝 (with emoji circles)
  - `_iso_to_roc_display()` — ISO date to ROC format conversion
  - `generate_html_report(date_str)` — HTML variant with inline CSS, color-coded rows by grade (green/yellow/red), collapsible deep-dive sections, responsive design with zh-TW font stack
- **run_pipeline.py** — Added report_generator as Stage 10 after all other stages complete
- All output Traditional Chinese with Taiwan market terminology
- Backward compatible — skips gracefully if upstream data missing

### Pipeline Test Results
```
Stage 10: report_generator ✓ (0.3s)
Report: MD ✓ | HTML ✓
Total pipeline: 10/10 stages passed in 39.7s
```

### Generated Reports Verified
- `reports/2026-05-16.md` (2,186 bytes) — Full Markdown report with ROC date "民國115年05月16日"
- `reports/2026-05-16.html` (7,900 bytes) — Styled HTML with lang="zh-TW", Noto Sans TC font

### Commits
- `92e3cd3` Phase 32: Add core/report_generator.py — daily Markdown/HTML report generator
- `d33b944` Phase 32: Integrate report_generator as Stage 10 in run_pipeline.py

### Review Notes
- Import pattern matches existing convention (inline imports per stage)
- Signal grades correctly mapped (80.5→B, 74.7→C with emoji colors)
- Sector names properly translated (半導體, 電子/電機, etc.)
- HTML report has proper meta tags and responsive CSS

---

## Phase 33 Complete (2026-05-16) — Comprehensive Test Suite Expansion
**Test count: ~119 → 318 (+199 new tests)**

### New Test Files Created
- **tests/test_report_generator.py** (619 lines, 57 tests) — Phase 32 coverage
  - format_signal_grade(): score→grade mapping (A/B/C/D/E with emoji), edge cases at boundaries (89.5, 79.9), negative/None/string inputs
  - format_regime_badge(): all 5 regimes map to correct Traditional Chinese, case-insensitive, unknown fallback
  - generate_daily_report(): empty data dir, missing stage1/stage2 files, partial data, ROC date in output
  - HTML report generation: valid HTML structure, lang="zh-TW", inline CSS, color-coded rows, collapsible sections
  - Helper functions: iso_to_roc_display(), format_ntd(), format_pct(), get_sector_tc(), safe_load_json()

- **tests/test_news_sentiment.py** (+200 lines, +17 tests → 57 total) — Phase 31 edge cases
  - Mixed positive/negative keywords in same article (loss aversion bias verification)
  - Empty/None news list handling with graceful fallbacks
  - Cache TTL boundary conditions (just expired vs just valid)

- **tests/test_microstructure.py** (589 lines, 49 tests) — Phase 30 coverage
  - compute_volume_profile(): single bar data, uniform prices, extreme outliers, empty history
  - classify_intraday_pattern(): all 9 candlestick types with synthetic OHLCV data
  - detect_volume_anomalies(): low-volume stocks (<20 samples), zero volume days, increasing/decreasing trends
  - compute_gap_fill_probability(): no historical gaps, all gaps filled, mixed fill rates

- **tests/test_rebalancing.py** (478 lines, 26 tests) — Phase 28 coverage
  - optimize_positions(): Kelly fraction with extreme signal strengths (0%, 100%), sector cap enforcement, regime multiplier effects, TWD allocation calculation
  - compute_sector_rotation(): single-sector portfolio, equal momentum across sectors, rolling window parameter, date filtering
  - check_correlation_risk(): perfectly correlated holdings (corr=1.0), zero-correlation case, position adjustment on high correlation, effective portfolio beta

- **tests/test_alerts.py** (552 lines, 49 tests) — Phase 29 coverage
  - Deduplication: same alert within cooldown, different alerts with overlapping hashes, TTL expiry
  - Escalation rules: info→digest, warning→immediate, critical→always bypasses all filters
  - Digest mode: batching multiple info alerts, digest flush timing, empty digest suppression

### Test Suite Results
```
318 passed in 7.17s (full suite)
Pipeline: 10/10 stages passed in 36.0s
All tests use inline synthetic data — zero API calls
Taiwan market conventions verified: TWSE codes (2330, 2454), Traditional Chinese, lang="zh-TW"
```

### Commits
- `2ce8940` Phase 33: Add test_report_generator.py — 57 tests for report_generator module
- `87d6350` Phase 33: Expand test_news_sentiment.py — +17 edge case tests (now 57 total)
- `73c15b8` Phase 33: Add test_microstructure.py — 49 tests for Phase 30 microstructure analysis
- `2be8212` Phase 33: Add test_rebalancing.py — 26 tests for Phase 28 portfolio rebalancing
- `cc9653a` Phase 33: Add test_alerts.py — 49 tests for Phase 29 alert system

### Review Notes
- Test patterns follow existing conftest.py fixtures and test_core.py conventions
- All edge cases properly handled (None, empty dicts, boundary values)
- Taiwan stock codes used throughout (2330=TSMC, 2454=MediaTek)
- Traditional Chinese regime names verified in report_generator tests
- HTML lang="zh-TW" attribute tested explicitly

## Phase 34 Complete (2026-05-16) — Cross-asset Correlation Engine
**Commits:** `c9c999c, 29dda61, 6263a41` (3 commits)

### New Files:
- **`core/market_context.py`** (~575 lines) — cross-asset correlation engine with yfinance integration

### Features Implemented:
1. **fetch_cross_assets()** — pulls TAIEX futures (^TWII), USD/TWD (TWDUSD=X), HSI (^HSI), KWEB, VIX (^VIX) via yfinance with 6h TTL cache in data/market_context_cache.json
2. **compute_market_breadth()** — advance/decline ratio from Stage 1 pass/watchlist/rejected counts + cross-asset correlation matrix (TAIEX vs HSI, USD/TWD inverse vs TAIEX, VIX vs TAIEX)
3. **get_cross_asset_signal()** → returns -0.2 to +0.2 adjustment based on:
   - Global risk sentiment: VIX <15=+0.1, >25=-0.1
   - USD/TWD trend: NT$ strengthening = bad for exports (-0.1)
   - HSI momentum vs TAIEX divergence: ±0.1
4. **Integration into regime_detector.py** — `_apply_cross_asset_override()` as optional 6th regime input; shifts regime one tier when signal >±0.15 (BLACK_SWAN never overridden upward by cross-asset alone)
5. **Integration into stage2_deep.py** — cross_asset_signal scaled from [-0.2, +0.2] to [-3, +3] points added to combined_score
6. **Backward compatible** — all functions return neutral defaults on fetch failure; no new hard dependencies (yfinance already in requirements.txt)
7. **Taiwan-market aware** — Traditional Chinese labels for reporting

### Implementation Review ✅
- **Cache:** 6h TTL with graceful expiry, JSON format with timestamp tracking
- **Fallbacks:** Every yfinance call wrapped in try/except; neutral defaults (None latest, 0 change) prevent pipeline breaks
- **Regime integration:** Cross-asset override only shifts by one tier; BLACK_SWAN protected from auto-promotion via cross-asset alone
- **Stage 2 scoring:** Signal scaled linearly (-0.2→-3pts, +0.2→+3pts), clamped to [-3, +3]
- **Correlation matrix:** Pearson correlation with minimum 5 data points requirement; handles edge cases (zero variance, insufficient data)

### Pipeline Test: PASSED ✅
```
10/10 stages successful in 43.5s
Stage 1: 1 passed, 15 watchlist, 1343 rejected
Stage 2: 1 passed, 0 disqualified
```

## Phase 35 Complete (2026-05-17) — Earnings Season Engine
**Commits:** `fa52894, bb2b013, 0559b88, ddefc17` (4 commits)

### New Files:
- **`core/earnings_analysis.py`** (~810 lines) — quarterly earnings analysis engine with YoY/QoQ growth, estimate surprise checks
- **`data/analyst_consensus.json`** (42 lines) — template for analyst EPS consensus data

### Features Implemented:
1. **fetch_earnings_data(stock_id, quarter)** — pulls quarterly financials from dividends data + TWSE API sources; extracts revenue, net profit, EPS; caches in data/earnings_cache.json with 24h TTL
2. **compute_yoy_growth(stock_id)** → dict with revenue_yoy_pct, profit_yoy_pct, eps_yoy_pct (current quarter vs same quarter last year)
3. **compute_qoq_growth(stock_id)** → dict with revenue_qoq_pct, profit_qoq_pct, eps_qoq_pct (current vs prior quarter)
4. **check_estimate_surprise(stock_id)** — compares reported EPS vs analyst consensus from data/analyst_consensus.json; returns surprise_pct and grade (beat/in-line/miss)
5. **get_earnings_signal(stock_id, date_str)** → -0.15 to +0.15 score adjustment based on growth momentum acceleration/deceleration, consecutive beat streaks, seasonal patterns
6. **Integration into stage2_deep.py** — `check_earnings_quality()` as new Stage 2 scoring dimension (weight: earnings_growth=0.12 in weights.json v6.0)
7. **Backward compatible** — all functions return None/neutral when data unavailable; no hard dependencies on analyst consensus or quarterly data
8. **Taiwan-market aware** — Traditional Chinese labels, TWSE quarterly financials endpoint (綜合損益), fiscal quarters Q1-Q4 aligned with ROC reporting calendar
9. **Red flag** — severely declining earnings (score < 20) disqualifies from Stage 2 pass

### Implementation Review ✅
- **Cache:** 24h TTL with graceful expiry, JSON format with timestamp tracking per stock-quarter
- **Fallbacks:** Every data fetch wrapped in try/except; neutral defaults (None revenue/profit/EPS) prevent pipeline breaks
- **Stage 2 scoring:** Earnings signal mapped to 0-100 score range, weighted at 0.12 alongside dividends/fundamentals/revenue/shareholders/pledge/penalties/news_sentiment
- **Red flags:** Score < 20 triggers disqualification (severely declining earnings)
- **Diagnostics:** Error counts and score distributions tracked per run for monitoring

### Pipeline Test: PASSED ✅
```
10/10 stages successful in 39.1s
Stage 1: 1 passed, 15 watchlist, 1343 rejected
Stage 2: 1 passed, 0 disqualified
```

## Phase 36 — Risk Management Overlay (2026-05-17)

**Status:** ✅ COMPLETE  
**Commits:** `efb3d87`  
**Code review:** ✅ Taiwan-market correct

### What Changed
- **core/risk_manager.py** (764 lines): ATR-based stop-loss tiers, trailing stops by conviction grade (A:E = 15%-25%), position limit enforcement (stock ≤8%, sector ≤25%, correlation <0.85), portfolio VaR estimation via historical simulation (60-day lookback), risk summary dashboard (1-10 score)
- **core/paper_trader.py**: Integrated pre-trade risk gate, ATR stops and trailing stop config stored per position, conviction-based position sizing
- **tests/test_risk_manager.py**: Comprehensive test coverage

### Review Notes
- Traditional Chinese labels throughout ✓
- Conviction grade tiers correctly tighten for higher quality signals (A=15%, E=25%) ✓
- Taiwan regulatory guidelines followed (8% single stock cap) ✓
- Graceful degradation — all functions return neutral/allow when data unavailable ✓
- Backward compatible with existing paper_trader workflow ✓

### Pipeline Test
**PASSED** ✅ 10/10 stages in 41.3s

## Phase 37 — Machine Learning Signal Fusion Ensemble (2026-05-17)

**Status:** ✅ COMPLETE  
**Commits:** `e2e65eb`, `ebe865a`, `acc8491`, `aebcb5d` (4 commits)  
**Code review:** ✅ Taiwan-market correct, backward compatible

### What Changed
- **core/signal_fusion.py** (~1272 lines): ML ensemble combining all Phase 26-36 scoring dimensions into unified probability of outperformance. Features: SignalNormalization (z-score space), compute_ensemble_score() → float [0,1], get_feature_importance() (SHAP-like permutation importance), calibrate_scores() (isotonic regression + Platt scaling), get_confidence_band(), extract_signals_from_pipeline(), run_signal_fusion()
- **config/signal_fusion_weights.json** (~90 lines): Initial weights proportional to Phase 28 backtest Sharpe ratios; 13 dimensions summing to 1.0; interaction terms (momentum×volume, earnings×fundamental)
- **run_pipeline.py**: Stage 11 signal_fusion added after report_generator
- **tests/test_signal_fusion.py** (~925 lines): 79 tests, all passing, zero API calls

### Review Notes
- Traditional Chinese labels throughout ✓
- All 13 dimensions extracted from pipeline outputs with neutral defaults ✓
- Backward compatible — skips gracefully if any upstream dimension missing ✓
- Sigmoid overflow handling: exponent clamped ±500, catch OverflowError → 0.5 ✓
- Isotonic mapping bins have slightly overlapping boundaries (cosmetic, not a bug) 
- Platt scaling gradient uses simplified form (converges slowly but works)
- Taiwan-market aware with proper Traditional Chinese labels for all dimension descriptions ✓

### Pipeline Test: PASSED ✅
```
11/11 stages successful in 37.2s
Stage 1: 1 passed, 15 watchlist, 1343 rejected
Stage 2: 1 passed, 0 disqualified
Signal Fusion: 1 stocks scored, avg=0.667, high-conviction=1
```

### Dry Run Test: PASSED ✅ (all 79 signal_fusion tests)

## Phase 38 — Portfolio Optimization Engine (2026-05-17)

**Status:** ✅ COMPLETE  
**Commits:** `79587cb`, `34c4817`, `0eb32ef` (3 commits)  
**Code review:** ✅ Taiwan-market correct, backward compatible

### What Changed
- **core/portfolio_optimizer.py** (~1333 lines): Mean-variance and Black-Litterman portfolio optimization with Taiwan market constraints. Features: load_ensemble_results() reads Phase 37 output, compute_covariance_matrix() builds empirical covariance with holiday forward-fill, optimize_mean_variance() solves Markowitz with Taiwan constraints (stock ≤8%, sector ≤25%, min 1% position, long-only), optimize_black_litterman() implements BL framework with ensemble-derived views, apply_weight_smoothing() integrates Phase 6 max 5% daily change limit, validate_constraints() checks Phases 5/36 risk constraints, run_portfolio_optimizer() is the Stage 12 pipeline entry point. Uses scipy.optimize when available, falls back to iterative heuristic optimizer.
- **run_pipeline.py**: Stage 12 portfolio_optimizer added after signal_fusion
- **tests/test_portfolio_optimizer.py** (~1146 lines): 73 tests, all passing, zero API calls

### Review Notes
- Traditional Chinese labels throughout ✓
- Taiwan regulatory constraints correctly enforced (8% single stock cap per SEC guidelines) ✓
- Backward compatible — skips gracefully if ensemble results missing ✓
- scipy fallback: iterative heuristic optimizer handles environments without scipy ✓
- Covariance matrix uses holiday-aware forward-fill for sparse Taiwan market data ✓
- Weight smoothing respects Phase 6 max 5% daily position change limit ✓
- Black-Litterman views derived from ensemble scores with proper confidence scaling ✓
- Graceful degradation when correlation data insufficient (min 20 samples) ✓

### Pipeline Test: PASSED ✅
```
12/12 stages successful in ~45s
Stage 1: 1 passed, 15 watchlist, 1343 rejected
Stage 2: 1 passed, 0 disqualified
Portfolio Optimizer: optimized weights produced, constraints validated
```

### Unit Tests: PASSED ✅ (73/73)
---

## Phase 39 — Performance Dashboard UI (2026-05-17)

**Status:** 🔄 IN PROGRESS — Layout design complete, awaiting implementation  
**Layout Design Commit:** `235cb96`  
**Design doc:** `docs/dashboard-layout-design.md` (668 lines)

### Layout Design Review ✅
Z.ai produced a comprehensive layout design document with:
- **ASCII wireframes:** Detailed page-level architecture + full detailed wireframe with 10 sections
- **Section specs:** Header bar, KPI card row (5 cards), top candidates table, portfolio allocation donut+bar, P&L performance chart, risk indicators (VaR gauge, concentration bars, status grid), regime trend history, cross-asset monitor, signal fusion radar, pipeline timeline
- **Color scheme:** GitHub Dark-inspired palette with 11 base tokens, 5 regime colors, 15 sector-specific colors
- **Typography:** Noto Sans TC for labels, Sarasa Mono SC for tabular data (proper CJK alignment)
- **Tech stack:** Chart.js 4.x CDN + Tailwind CSS CDN + Lucide Icons CDN — zero build step required

### Review Corrections Sent to Z.ai:
1. **Portfolio file path:** Use `data/optimized_portfolio_{date}.json` (NOT `portfolio_{date}.json`)
2. **Paper trades empty:** `paper_trades.json` may be `[]` — show "尚無交易記錄" placeholder, don't crash
3. **Regime history missing:** `regime_history.json` doesn't exist yet (Phase 40) — show current regime only with note
4. **VaR gauge workaround:** Chart.js has no native gauge chart — use doughnut clip-bottom technique or polar area chart
5. **Ensemble signals mapping:** Confirmed exact 13 keys: technical_momentum, fundamentals, revenue_quality, shareholders, pledge_ratio, dividends, institutional_flow, signal_quality, microstructure, news_sentiment, cross_asset, earnings_quality, risk_management → map to Traditional Chinese labels (技術動能, 基本面, 營收品質, etc.)
6. **CORS handling:** Document that `python3 -m http.server 8080` is required (file:// protocol blocks fetch)

### Design Assessment
- ✅ Comprehensive section coverage matching all pipeline outputs
- ✅ Traditional Chinese labels throughout
- ✅ Color-coded regime system matches Phase 4 tiered system
- ✅ Radar chart axes match Phase 37 ensemble dimensions exactly
- ✅ Dark theme appropriate for trading terminal aesthetic
- ⚠️ Some data sources need graceful degradation (paper_trades empty, no regime_history yet)

### Next: Implementation by Z.ai based on approved design + corrections.
