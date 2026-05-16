#!/usr/bin/env python3
"""
Pipeline Runner — Orchestrates the full tw-stock-hunter pipeline

Chains: fetch_data → fetch_history → detect_regime → db_migrate → stage1_screen → stage2_deep → paper_trader → telegram_alerts → report_generator → signal_fusion → portfolio_optimizer

Usage:
    python run_pipeline.py                       # Run for today
    python run_pipeline.py --date 2026-05-12     # Run for a specific date
    python run_pipeline.py --date 2026-05-12 -v  # Verbose output
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure core/ is importable as bare modules
CORE_DIR = Path(__file__).parent / "core"
sys.path.insert(0, str(CORE_DIR))

REPO_ROOT = Path(__file__).parent
DATA_DIR = REPO_ROOT / "data"


class PipelineResult:
    """Track stage results and timing."""

    def __init__(self, date_str):
        self.date_str = date_str
        self.stages = []
        self.start_time = time.time()
        self.failed_stage = None
        self.error = None

    def add_stage(self, name, elapsed, success=True, summary=None):
        self.stages.append({
            "name": name,
            "elapsed_sec": round(elapsed, 2),
            "success": success,
            "summary": summary or {},
        })

    def mark_failed(self, stage_name, error):
        self.failed_stage = stage_name
        self.error = str(error)

    def total_elapsed(self):
        return round(time.time() - self.start_time, 2)

    def to_dict(self):
        return {
            "date": self.date_str,
            "timestamp": datetime.now().isoformat(),
            "total_elapsed_sec": self.total_elapsed(),
            "failed_stage": self.failed_stage,
            "error": self.error,
            "stages": self.stages,
        }


def _run_stage(name, func, result, verbose=False, **kwargs):
    """Run a single pipeline stage with timing and error handling."""
    if result.failed_stage is not None:
        # A previous stage failed — skip remaining stages
        return None

    if verbose:
        print(f"\n{'='*60}")
        print(f"  STAGE: {name}")
        print(f"{'='*60}")

    t0 = time.time()
    try:
        output = func(**kwargs)
        elapsed = time.time() - t0
        result.add_stage(name, elapsed, success=True)
        if verbose:
            print(f"  ✓ {name} completed in {elapsed:.1f}s")
        return output
    except Exception as e:
        elapsed = time.time() - t0
        result.add_stage(name, elapsed, success=False)
        result.mark_failed(name, e)
        if verbose:
            print(f"  ✗ {name} FAILED after {elapsed:.1f}s: {e}")
        else:
            print(f"✗ {name} FAILED: {e}")
        return None


def run_pipeline(date_str=None, verbose=False):
    """Run the full pipeline sequentially.

    Returns a PipelineResult with timing and summary data.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    result = PipelineResult(date_str)

    if verbose:
        print(f"🚀 tw-stock-hunter pipeline — {date_str}")
        print(f"   Repo: {REPO_ROOT}")

    # ── Stage 1: Fetch Data ──────────────────────────────────────────
    from fetch_data import fetch_all, save_results

    raw_data = _run_stage(
        "fetch_data",
        lambda **kw: fetch_all(verbose=verbose),
        result,
        verbose=verbose,
    )
    if raw_data is None:
        return _finalize(result, verbose)

    # Phase 22: Run Pydantic ingestion validation and ex-date validation
    # These data quality gates existed in fetch_data.main() but were bypassed
    # when the pipeline used fetch_all() directly.
    from fetch_data import validate_ingested_data, _run_ex_date_validation

    validate_ok = _run_stage(
        "validate_ingested_data",
        lambda **kw: validate_ingested_data(raw_data, verbose=verbose),
        result,
        verbose=verbose,
    )
    if validate_ok is False:
        # validate_ingested_data returns False when >10% validation failures
        result.mark_failed("validate_ingested_data", "Data quality too low (>10% validation failures)")
        return _finalize(result, verbose)

    _run_ex_date_validation(data_dir=DATA_DIR, verbose=verbose)

    save_meta = save_results(raw_data, date_str)
    if verbose:
        total_records = sum(save_meta.get("record_counts", {}).values())
        print(f"   Fetched {total_records} records across {len(save_meta.get('endpoints', []))} endpoints")

    # ── Stage 2: Fetch History ───────────────────────────────────────
    from fetch_history import build_price_history

    price_history = _run_stage(
        "fetch_history",
        lambda **kw: build_price_history(date_str=date_str, verbose=verbose),
        result,
        verbose=verbose,
    )
    if price_history is None:
        return _finalize(result, verbose)

    n_stocks = len(price_history)
    if verbose:
        print(f"   Price history: {n_stocks} stocks")

    # ── Stage 3: Detect Regime ───────────────────────────────────────
    from regime_detector import detect_regime

    regime_data = _run_stage(
        "detect_regime",
        lambda **kw: detect_regime(date_str=date_str, verbose=verbose),
        result,
        verbose=verbose,
    )
    if regime_data is None:
        return _finalize(result, verbose)

    regime = regime_data.get("regime", "unknown")
    if verbose:
        print(f"   Regime: {regime} (confidence: {regime_data.get('confidence', '?')})")

    # ── Stage 4: DB Migration ──────────────────────────────────────────
    # Phase 25: Migrate JSON files to SQLite so downstream stages can use
    # SQL queries instead of JSON file reads where beneficial.
    from datastore import migrate_json_to_sqlite, save_stage1_to_sqlite

    db_stats = _run_stage(
        "db_migrate",
        lambda **kw: migrate_json_to_sqlite(data_dir=str(DATA_DIR), verbose=verbose),
        result,
        verbose=verbose,
    )
    if verbose and db_stats:
        total_inserted = sum(v for k, v in db_stats.items() if k.endswith("_inserted"))
        print(f"   DB migration: {total_inserted} rows inserted")

    # ── Stage 5: Stage 1 Screen ──────────────────────────────────────
    from stage1_screen import run_stage1

    s1_output = _run_stage(
        "stage1_screen",
        lambda **kw: run_stage1(date_str=date_str, verbose=verbose),
        result,
        verbose=verbose,
    )
    if s1_output is None:
        return _finalize(result, verbose)

    # Phase 25: Also persist stage1 results to SQLite for SQL-based lookups
    save_stage1_to_sqlite(s1_output, data_dir=str(DATA_DIR))

    s1_summary = s1_output.get("summary", {})
    n_passed_s1 = s1_summary.get("passed", 0)
    n_watchlist = s1_summary.get("watchlist", 0)
    n_rejected = s1_summary.get("rejected", 0)
    if verbose:
        print(f"   Screened: {n_passed_s1} passed, {n_watchlist} watchlist, {n_rejected} rejected")

    # ── Stage 6: Stage 2 Deep-Dive ───────────────────────────────────
    from stage2_deep import run_stage2, save_stage2_results

    s2_output = _run_stage(
        "stage2_deep",
        lambda **kw: run_stage2(date_str=date_str, verbose=verbose),
        result,
        verbose=verbose,
    )
    if s2_output is None:
        return _finalize(result, verbose)

    s2_summary = s2_output.get("summary", {})
    n_passed_s2 = s2_summary.get("passed_stage2", 0)
    n_disqualified = s2_summary.get("disqualified", 0)

    save_stage2_results(s2_output)
    if verbose:
        print(f"   Deep-dive: {n_passed_s2} passed, {n_disqualified} disqualified")

    # ── Stage 6: Paper Trader ────────────────────────────────────────
    from paper_trader import PaperTrader
    from regime_detector import get_regime_position_mult

    def _run_paper_trader(**kw):
        trader = PaperTrader()
        regime_mult = get_regime_position_mult(regime)
        candidates = trader.load_candidates(date_str)
        if candidates:
            trader.simulate_entry(candidates, date_str, regime_mult=regime_mult)
        trader.save_trades()
        return trader.get_stats()

    trader_stats = _run_stage(
        "paper_trader",
        _run_paper_trader,
        result,
        verbose=verbose,
    )
    if verbose and trader_stats:
        print(f"   Paper trades: win_rate={trader_stats.get('win_rate', 0):.1%}, "
              f"PnL={trader_stats.get('total_pnl_pct', 0):+.2f}%")

    # ── Stage 7: Telegram Alerts ─────────────────────────────────────
    from telegram_alerts import TelegramAlerts

    def _run_telegram(**kw):
        alerts = TelegramAlerts(str(DATA_DIR))
        if alerts.should_alert("daily"):
            message = alerts.generate_alert(date_str)
            alerts.record_alert("daily")
            alerts.update_last_alert_with_regime()
            return {"alert_sent": True, "message_length": len(message) if message else 0}
        return {"alert_sent": False, "reason": "rate_limited_or_holiday"}

    alert_result = _run_stage(
        "telegram_alerts",
        _run_telegram,
        result,
        verbose=verbose,
    )
    if verbose and alert_result:
        status = "sent" if alert_result.get("alert_sent") else "skipped"
        print(f"   Alert: {status}")

    # ── Stage 10: Report Generator ───────────────────────────────────
    # Phase 32: Generate Markdown/HTML daily reports combining all
    # pipeline outputs. Runs after all other stages so it has access
    # to the full set of results. Backward compatible — if upstream
    # data is missing, the report gracefully skips those sections.
    from report_generator import generate_daily_report, generate_html_report

    def _run_report_generator(**kw):
        reports_dir = str(REPO_ROOT / "reports")
        md_report = generate_daily_report(date_str, output_dir=reports_dir)
        # Also generate HTML variant
        html_report = generate_html_report(date_str, output_dir=reports_dir)
        return {
            "md_generated": md_report is not None,
            "html_generated": html_report is not None,
            "md_length": len(md_report) if md_report else 0,
            "html_length": len(html_report) if html_report else 0,
        }

    report_result = _run_stage(
        "report_generator",
        _run_report_generator,
        result,
        verbose=verbose,
    )
    if verbose and report_result:
        md_ok = "✓" if report_result.get("md_generated") else "✗"
        html_ok = "✓" if report_result.get("html_generated") else "✗"
        print(f"   Report: MD {md_ok} | HTML {html_ok}")

    # ── Stage 11: Signal Fusion Ensemble ────────────────────────────────
    # Phase 37: Combine all scoring dimensions into unified probability
    # of outperformance. Takes stage2 results + market_context + risk_manager
    # output. Produces data/ensemble_YYYY-MM-DD.json. Backward compatible —
    # skips gracefully if any upstream dimension is missing.
    from signal_fusion import run_signal_fusion

    ensemble_result = _run_stage(
        "signal_fusion",
        lambda **kw: run_signal_fusion(date_str=date_str, verbose=verbose),
        result,
        verbose=verbose,
    )
    if verbose and ensemble_result:
        ensemble_summary = ensemble_result.get("summary", {})
        n_stocks = ensemble_summary.get("total_stocks", 0)
        avg_score = ensemble_summary.get("avg_ensemble_score", 0)
        n_high = ensemble_summary.get("high_conviction_count", 0)
        print(f"   Ensemble: {n_stocks} stocks scored, avg={avg_score:.3f}, "
              f"high-conviction={n_high}")

    # ── Stage 12: Portfolio Optimizer ──────────────────────────────────
    # Phase 38: Mean-variance / Black-Litterman portfolio optimization
    # with Taiwan market constraints. Takes signal_fusion ensemble scores
    # + price history to produce optimized portfolio weights.
    # Backward compatible — skips gracefully if ensemble results missing
    # or covariance matrix can't be computed (<30 trading days).
    from portfolio_optimizer import run_portfolio_optimizer

    portfolio_result = _run_stage(
        "portfolio_optimizer",
        lambda **kw: run_portfolio_optimizer(date_str=date_str, verbose=verbose),
        result,
        verbose=verbose,
    )
    if verbose and portfolio_result:
        n_opt = portfolio_result.get("n_stocks", 0)
        method = portfolio_result.get("optimization_method", "?")
        sharpe = portfolio_result.get("sharpe_ratio", 0)
        smoothed = "（已平滑）" if portfolio_result.get("smoothing_applied") else ""
        print(f"   Portfolio: {n_opt} stocks, method={method}{smoothed}, "
              f"sharpe={sharpe:.2f}")

    return _finalize(result, verbose)


def _finalize(result, verbose=False):
    """Update state.json and print summary."""
    # Update state.json
    state_file = REPO_ROOT / "state.json"
    try:
        state = {}
        if state_file.exists():
            with open(state_file, "r", encoding="utf-8") as f:
                state = json.load(f)

        state["last_run"] = datetime.now().isoformat()
        state["last_date"] = result.date_str

        # Fill pipeline summary from stage results
        for stage in result.stages:
            if stage["name"] == "stage1_screen" and stage.get("summary"):
                state.setdefault("pipeline", {})["stage1"] = stage["summary"]
            elif stage["name"] == "stage2_deep" and stage.get("summary"):
                state.setdefault("pipeline", {})["stage2"] = stage["summary"]
            elif stage["name"] == "detect_regime" and stage.get("summary"):
                state.setdefault("pipeline", {})["regime"] = stage["summary"].get("regime", "unknown")

        state["pipeline_run"] = {
            "timestamp": datetime.now().isoformat(),
            "total_elapsed_sec": result.total_elapsed(),
            "stages_completed": len([s for s in result.stages if s["success"]]),
            "stages_total": len(result.stages),
            "failed_stage": result.failed_stage,
        }

        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        if verbose:
            print(f"   ⚠ Failed to update state.json: {e}")

    # Print summary
    if verbose:
        total = result.total_elapsed()
        completed = len([s for s in result.stages if s["success"]])
        total_stages = len(result.stages)
        print(f"\n{'='*60}")
        if result.failed_stage:
            print(f"  ✗ PIPELINE FAILED at stage: {result.failed_stage}")
            print(f"    Error: {result.error}")
        else:
            print(f"  ✓ PIPELINE COMPLETE")
        print(f"  Stages: {completed}/{total_stages} successful")
        print(f"  Total time: {total:.1f}s")
        for s in result.stages:
            icon = "✓" if s["success"] else "✗"
            print(f"    {icon} {s['name']}: {s['elapsed_sec']:.1f}s")
        print(f"{'='*60}")
    elif result.failed_stage:
        print(f"✗ Pipeline failed at {result.failed_stage}: {result.error}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Run the full tw-stock-hunter pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Pipeline stages:
  1.  fetch_data        — Fetch TWSE data (daily, PE, revenue, etc.)
  2.  fetch_history     — Build historical price data
  3.  detect_regime     — Detect market regime
  4.  db_migrate        — Migrate JSON data to SQLite (Phase 25)
  5.  stage1_screen     — Quantitative pre-screen
  6.  stage2_deep       — Fundamental deep-dive
  7.  paper_trader      — Paper trading simulation
  8.  telegram_alerts   — Send alerts (if enabled)
  10. report_generator  — Generate Markdown/HTML daily reports (Phase 32)
  11. signal_fusion     — ML ensemble fusion of all scoring dimensions (Phase 37)
  12. portfolio_optimizer — Portfolio optimization with Taiwan constraints (Phase 38)
""",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to analyze (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output with per-stage details",
    )
    args = parser.parse_args()

    result = run_pipeline(date_str=args.date, verbose=args.verbose)

    # Exit with error code if pipeline failed
    sys.exit(1 if result.failed_stage else 0)


if __name__ == "__main__":
    main()
