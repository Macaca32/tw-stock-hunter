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

## Phase 22 Complete (2026-05-15) — Full Repo Re-review
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
