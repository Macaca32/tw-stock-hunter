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
