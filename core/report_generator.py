#!/usr/bin/env python3
"""
Daily Report Generator — Phase 32

Produces human-readable Markdown and HTML daily reports combining all
pipeline outputs: screening results, portfolio status, sector rotation,
and alert digests.

Features:
  1. generate_daily_report(date_str, output_dir) — reads Stage 1/2
     results, portfolio status, sector rotation, alerts from JSON files
     and assembles a comprehensive Markdown report at
     {output_dir}/{date}.md with ROC date header, market regime badge,
     screening results table (stock code, name, composite score,
     signal grade A-E), deep-dive details for pass candidates,
     portfolio status (positions, PnL, sector allocation), and
     alert digest.
  2. format_signal_grade(score) — map score to letter grade:
     90+=A, 80+=B, 70+=C, 60+=D, <60=E with emoji coloring
     (A/B green, C/D yellow, E red).
  3. format_regime_badge(regime) — NORMAL=常態, CAUTION=警戒,
     STRESS=壓力, CRISIS=危機, BLACK_SWAN=黑天鵝.
  4. generate_html_report(date_str) — optional HTML variant with
     inline CSS, color-coded rows by grade, collapsible deep-dive
     sections.
  5. Integration: Added as Stage 10 in run_pipeline.py after all
     other stages complete.

All output is Traditional Chinese with Taiwan market terminology.
Backward compatible — skips gracefully if upstream data is missing.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Regime badge mapping (Traditional Chinese) ──────────────────────
REGIME_BADGE = {
    "normal": "\u5e38\u614b",        # 常態
    "caution": "\u8b66\u6212",       # 警戒
    "stress": "\u58d3\u529b",        # 壓力
    "crisis": "\u5371\u6a5f",        # 危機
    "black_swan": "\u9ed1\u5929\u9d5d",  # 黑天鵝
    "unknown": "\u672a\u77e5",       # 未知
}

REGIME_EMOJI = {
    "normal": "\U0001f7e2",      # green circle
    "caution": "\U0001f7e1",     # yellow circle
    "stress": "\U0001f7e0",      # orange circle
    "crisis": "\U0001f534",      # red circle
    "black_swan": "\U0001f6a8",  # police car light
    "unknown": "\u2753",         # question mark
}

# ── Signal grade thresholds ─────────────────────────────────────────
GRADE_THRESHOLDS = [
    (90, "A", "\U0001f7e2"),   # green circle — top tier
    (80, "B", "\U0001f7e2"),   # green circle — strong
    (70, "C", "\U0001f7e1"),   # yellow circle — moderate
    (60, "D", "\U0001f7e1"),   # yellow circle — weak
    (0,  "E", "\U0001f534"),   # red circle — avoid
]

# ── Sector name translations (Traditional Chinese) ──────────────────
SECTOR_TC = {
    "materials": "\u6c34\u6ce5/\u5316\u5de5",       # 水泥/化工
    "consumer": "\u6d88\u8cbb/\u98df\u54c1",         # 消費/食品
    "industrial": "\u6a5f\u96fb/\u5de5\u696d",       # 機電/工業
    "metals": "\u92fc\u9435/\u91d1\u5c6c",           # 鋼鐵/金屬
    "semiconductor": "\u534a\u5c0e\u9ad4",           # 半導體
    "electronics": "\u96fb\u5b50/\u96fb\u6a5f",      # 電子/電機
    "optoelectronics": "\u5149\u96fb",               # 光電
    "communications": "\u901a\u8a0a",                # 通訊
    "computers": "\u96fb\u8166/\u4f3a\u670d\u5668",  # 電腦/伺服器
    "components": "\u96f6\u7d44\u4ef6/\u88ab\u52d5",  # 零組件/被動
    "financial": "\u91d1\u878d/\u4fdd\u96aa",        # 金融/保險
    "construction": "\u71df\u5efa/\u5de5\u7a0b",      # 營建/工程
    "services": "\u8cbf\u6613/\u670d\u52d9",          # 貿易/服務
    "tpex": "\u6ac3\u8cb7/\u4e0a\u6ac3",             # 櫃買/上櫃
    "other": "\u5176\u4ed6",                          # 其他
}


def format_signal_grade(score: float) -> str:
    """Map a composite score to a letter grade with emoji.

    Grade thresholds:
        90+ → A (green), 80+ → B (green), 70+ → C (yellow),
        60+ → D (yellow), <60 → E (red)

    Args:
        score: Composite score (0-100).

    Returns:
        Formatted string like '🟢 A' or '🔴 E'.
    """
    try:
        s = float(score)
    except (TypeError, ValueError):
        return f"\U0001f534 E"

    for threshold, letter, emoji in GRADE_THRESHOLDS:
        if s >= threshold:
            return f"{emoji} {letter}"

    return f"\U0001f534 E"


def format_regime_badge(regime: str) -> str:
    """Format regime as a Traditional Chinese badge with emoji.

    Mapping: NORMAL→常態, CAUTION→警戒, STRESS→壓力,
             CRISIS→危機, BLACK_SWAN→黑天鵝

    Args:
        regime: English regime string (case-insensitive).

    Returns:
        Formatted string like '🟢 常態' or '🔴 黑天鵝'.
    """
    key = regime.lower().strip() if regime else "unknown"
    tc_name = REGIME_BADGE.get(key, regime)
    emoji = REGIME_EMOJI.get(key, "\u2753")
    return f"{emoji} {tc_name}"


def _iso_to_roc_display(iso_date_str: str) -> str:
    """Convert ISO date (YYYY-MM-DD) to ROC display format (民國XXX年XX月XX日).

    Args:
        iso_date_str: Date in YYYY-MM-DD format.

    Returns:
        ROC date string like '民國115年05月16日'.
    """
    try:
        dt = datetime.strptime(iso_date_str, "%Y-%m-%d")
        roc_year = dt.year - 1911
        return f"\u6c11\u570b{roc_year}\u5e74{dt.month:02d}\u6708{dt.day:02d}\u65e5"
        # 民國{roc_year}年{month}月{day}日
    except (ValueError, TypeError):
        return iso_date_str


def _safe_load_json(filepath: Path) -> Optional[dict]:
    """Load JSON file with graceful error handling."""
    if not filepath.exists():
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError, OSError) as e:
        logger.debug("Failed to load %s: %s", filepath, e)
        return None


def _format_ntd(amount: float) -> str:
    """Format number as NT$ with comma separation.

    Args:
        amount: Numeric value.

    Returns:
        Formatted string like 'NT$1,234,567' or 'NT$0'.
    """
    try:
        return f"NT${float(amount):,.0f}"
    except (TypeError, ValueError):
        return "NT$0"


def _format_pct(value: float, sign: bool = True) -> str:
    """Format a value as percentage string.

    Args:
        value: Numeric value (e.g., 3.5 for 3.5%).
        sign: Whether to prepend '+' for positive values.

    Returns:
        Formatted string like '+3.50%' or '-1.20%'.
    """
    try:
        v = float(value)
        if sign and v > 0:
            return f"+{v:.2f}%"
        return f"{v:.2f}%"
    except (TypeError, ValueError):
        return "N/A"


def _get_sector_tc(sector_en: str) -> str:
    """Get Traditional Chinese name for an English sector key."""
    return SECTOR_TC.get(sector_en, sector_en)


def _load_stage1(data_dir: Path, date_str: str) -> Optional[dict]:
    """Load Stage 1 screening results, trying SQLite then JSON."""
    # Try SQLite first
    try:
        from datastore import load_stage1_from_sqlite
        result = load_stage1_from_sqlite(date_str=date_str, data_dir=str(data_dir))
        if result is not None:
            return result
    except Exception:
        pass

    # Fallback to JSON
    return _safe_load_json(data_dir / f"stage1_{date_str}.json")


def _load_stage2(data_dir: Path, date_str: str) -> Optional[dict]:
    """Load Stage 2 deep-dive results."""
    return _safe_load_json(data_dir / f"stage2_{date_str}.json")


def _load_regime(data_dir: Path) -> Optional[dict]:
    """Load current market regime data."""
    return _safe_load_json(data_dir / "regime.json")


def _load_paper_trades(data_dir: Path) -> List[dict]:
    """Load paper trade ledger."""
    data = _safe_load_json(data_dir / "paper_trades.json")
    if data is None:
        return []
    if isinstance(data, list):
        return data
    return []


def _load_alert_history(data_dir: Path) -> List[dict]:
    """Load alert history for the alert digest."""
    data = _safe_load_json(data_dir / "alert_history.json")
    if data is None:
        return []
    if isinstance(data, list):
        return data
    return []


def _load_pending_digest(data_dir: Path) -> List[dict]:
    """Load pending info-level alerts for the digest."""
    data = _safe_load_json(data_dir / "pending_digest.json")
    if data is None:
        return []
    if isinstance(data, list):
        return data
    return []


def _load_sector_map(data_dir: Path) -> Dict[str, str]:
    """Load sector mapping {stock_code: sector_name}."""
    try:
        from sectors import load_sector_mapping
        return load_sector_mapping(str(data_dir))
    except ImportError:
        return {}


def _compute_portfolio_summary(trades: List[dict],
                               sector_map: Dict[str, str]) -> dict:
    """Compute portfolio summary from paper trades.

    Returns a dict with open positions, closed trades, PnL,
    and sector allocation breakdown.
    """
    open_positions = [t for t in trades if t.get("status") == "open"]
    closed_trades = [t for t in trades if t.get("status") == "closed"]

    # PnL calculations
    total_pnl = sum(t.get("pnl_pct", 0) or 0 for t in closed_trades)
    winning = [t for t in closed_trades if (t.get("pnl_pct") or 0) > 0]
    losing = [t for t in closed_trades if (t.get("pnl_pct") or 0) <= 0]
    win_rate = (len(winning) / len(closed_trades) * 100) if closed_trades else 0

    # Sector allocation of open positions
    sector_counts: Dict[str, int] = {}
    sector_pnl: Dict[str, float] = {}
    for t in closed_trades:
        sector = t.get("sector", "other")
        sector_pnl[sector] = sector_pnl.get(sector, 0) + (t.get("pnl_pct") or 0)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    return {
        "total_trades": len(trades),
        "open_positions": len(open_positions),
        "closed_trades": len(closed_trades),
        "win_rate": round(win_rate, 1),
        "total_pnl_pct": round(total_pnl, 2),
        "avg_pnl_pct": round(total_pnl / len(closed_trades), 2) if closed_trades else 0,
        "winning_trades": len(winning),
        "losing_trades": len(losing),
        "open_list": open_positions,
        "sector_allocation": sector_counts,
        "sector_pnl": sector_pnl,
    }


def generate_daily_report(date_str: str = None,
                          output_dir: str = None) -> Optional[str]:
    """Generate a comprehensive Markdown daily report.

    Reads Stage 1/2 results, portfolio status, sector rotation, and
    alerts from JSON files, then assembles a human-readable report
    saved at {output_dir}/{date}.md.

    The report includes:
      - ROC date format header (民國XXX年XX月XX日)
      - Market regime badge
      - Screening results table (stock code, name, composite score,
        signal grade A-E)
      - Deep-dive details for pass candidates
      - Portfolio status (positions, PnL, sector allocation)
      - Alert digest

    Args:
        date_str: Date in YYYY-MM-DD format. Defaults to today.
        output_dir: Directory to write the report. Defaults to
                    repo_root/reports/.

    Returns:
        The Markdown report string, or None on complete failure.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    data_dir = Path(__file__).parent.parent / "data"

    if output_dir is None:
        output_dir = str(Path(__file__).parent.parent / "reports")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # ── Load data (graceful degradation) ────────────────────────────
    s1 = _load_stage1(data_dir, date_str)
    s2 = _load_stage2(data_dir, date_str)
    regime_data = _load_regime(data_dir)
    trades = _load_paper_trades(data_dir)
    sector_map = _load_sector_map(data_dir)
    alert_history = _load_alert_history(data_dir)
    pending_digest = _load_pending_digest(data_dir)

    # ── Derive key values ───────────────────────────────────────────
    regime = regime_data.get("regime", "unknown") if regime_data else "unknown"
    regime_badge = format_regime_badge(regime)
    roc_date = _iso_to_roc_display(date_str)
    confidence = regime_data.get("confidence", "N/A") if regime_data else "N/A"
    volatility = regime_data.get("volatility", 0) if regime_data else 0
    days_in_regime = regime_data.get("days_in_regime", 0) if regime_data else 0

    # ── Build Markdown report ───────────────────────────────────────
    lines: List[str] = []

    # --- Header ---
    lines.append(f"# \u6bcf\u65e5\u7be1\u9078\u5831\u544a")  # 每日篩選報告
    lines.append("")
    lines.append(f"**{roc_date}** ({date_str})")
    lines.append("")
    lines.append(f"**\u5e02\u5834\u72c0\u614b**: {regime_badge}")
    # 市場狀態
    if regime_data:
        lines.append(f"- \u4f4d\u65bc\u7576\u524d\u72c0\u614b\u5929\u6578: {days_in_regime}")
        # 位於當前狀態天數
        lines.append(f"- \u6ce2\u52d5\u7387: {volatility:.4f}")
        # 波動率
        lines.append(f"- \u4fe1\u5fc3\u6c34\u6e96: {confidence}")
        # 信心水準
        global_risk = regime_data.get("global_risk", "N/A")
        lines.append(f"- \u5168\u7403\u98a8\u96aa: {global_risk}")
        # 全球風險
    lines.append("")

    # --- Stage 1 Summary ---
    lines.append("---")
    lines.append("")
    lines.append(f"## \u7b2c\u4e00\u968e\u6bb5\uff1a\u91cf\u5316\u521d\u7be1")  # 第一階段：量化初篩
    lines.append("")

    if s1:
        summary = s1.get("summary", {})
        total = summary.get("total_screened", 0)
        passed = summary.get("passed", 0)
        watchlist = summary.get("watchlist", 0)
        rejected = summary.get("rejected", 0)

        lines.append(f"- \u7be1\u9078\u7e3d\u6578: {total}")
        # 篩選總數
        lines.append(f"- \u901a\u904e: {passed}")
        # 通過
        lines.append(f"- \u89c0\u5bdf\u540d\u55ae: {watchlist}")
        # 觀察名單
        lines.append(f"- \u639b\u9664: {rejected}")
        # 淘汰
        lines.append("")

        # Screening results table — candidates sorted by composite_score
        candidates = s1.get("candidates", [])
        passed_candidates = [c for c in candidates if c.get("pass", c.get("composite_score", 0) >= 65)]
        passed_candidates.sort(key=lambda c: c.get("composite_score", 0), reverse=True)

        if passed_candidates:
            lines.append(f"### \u901a\u904e\u540d\u55ae")  # 通過名單
            lines.append("")
            lines.append(
                "| \u80a1\u7968\u4ee3\u865f |"    # 股票代號
                " \u540d\u7a31 |"                   # 名稱
                " \u7d9c\u5408\u8a55\u5206 |"       # 綜合評分
                " \u4fe1\u865f\u7b49\u7d1a |"       # 信號等級
                " \u6536\u76e4\u50f9 |"             # 收盤價
                " \u7522\u696d |"                   # 產業
                " |"
            )
            lines.append(
                "|:------|:-----|:--------:|:------:|------:|:-----|"
            )
            for c in passed_candidates:
                code = str(c.get("code", ""))
                name = str(c.get("name", ""))
                score = c.get("composite_score", 0)
                grade = format_signal_grade(score)
                close = c.get("close", 0)
                sector_en = sector_map.get(code, "")
                sector_display = _get_sector_tc(sector_en) if sector_en else ""
                lines.append(
                    f"| {code} | {name} | {score:.1f} | {grade} "
                    f"| {_format_ntd(close)} | {sector_display} |"
                )
            lines.append("")
    else:
        lines.append(f"*Stage 1 \u8cc7\u6599\u4e0d\u53ef\u7528\uff08\u7565\u904e\uff09*")
        # Stage 1 資料不可用（略過）
        lines.append("")

    # --- Stage 2 Deep-Dive ---
    lines.append("---")
    lines.append("")
    lines.append(f"## \u7b2c\u4e8c\u968e\u6bb5\uff1a\u57fa\u672c\u9762\u6df1\u5ea6\u6aa2\u8996")  # 第二階段：基本面深度檢視
    lines.append("")

    if s2:
        s2_summary = s2.get("summary", {})
        s2_passed = s2_summary.get("passed_stage2", 0)
        s2_disqualified = s2_summary.get("disqualified", 0)

        lines.append(f"- \u901a\u904e\u7b2c\u4e8c\u968e\u6bb5: {s2_passed}")
        # 通過第二階段
        lines.append(f"- \u5931\u683c: {s2_disqualified}")
        # 失格
        lines.append("")

        # Deep-dive details for pass candidates
        s2_candidates = s2.get("candidates", [])
        s2_candidates.sort(key=lambda c: c.get("combined_score", 0), reverse=True)

        if s2_candidates:
            lines.append(f"### \u901a\u904e\u5019\u9078\u8a73\u7d30\u8cc7\u6599")  # 通過候選詳細資料
            lines.append("")

            for c in s2_candidates:
                code = str(c.get("code", ""))
                name = str(c.get("name", ""))
                combined = c.get("combined_score", 0)
                s1_score = c.get("stage1_score", 0)
                s2_score = c.get("stage2_score", 0)
                grade = format_signal_grade(combined)

                lines.append(f"#### {code} {name} — {grade} ({combined:.1f})")
                lines.append("")
                lines.append(
                    f"- \u7b2c\u4e00\u968e\u6bb5\u8a55\u5206: {s1_score:.1f}"
                    # 第一階段評分
                    f" / \u7b2c\u4e8c\u968e\u6bb5\u8a55\u5206: {s2_score:.1f}"
                    # 第二階段評分
                )

                # Check details
                checks = c.get("checks", {})
                if checks:
                    check_labels = {
                        "dividend": "\u80a1\u5229",
                        "announcements": "\u91cd\u8981\u516c\u544a",
                        "shareholders": "\u80a1\u6771\u7d50\u69cb",
                        "pledge": "\u8cea\u62bc",
                        "penalties": "\u8655\u5206",
                        "news_sentiment": "\u65b0\u805e\u60c5\u7dd2",
                    }
                    # 股利 / 重要公告 / 股東結構 / 質押 / 處分 / 新聞情緒
                    check_parts = []
                    for key, label in check_labels.items():
                        chk = checks.get(key, {})
                        if chk:
                            score_val = chk.get("score")
                            status_val = chk.get("status", "")
                            if score_val is not None:
                                try:
                                    sv = float(score_val)
                                    g = format_signal_grade(sv)
                                    check_parts.append(f"{label}: {g} ({sv:.0f})")
                                except (TypeError, ValueError):
                                    check_parts.append(f"{label}: {status_val}")
                            else:
                                check_parts.append(f"{label}: {status_val}")
                    if check_parts:
                        lines.append(f"- {' / '.join(check_parts)}")

                # Microstructure details
                micro = c.get("microstructure", {})
                if micro:
                    vol_profile = micro.get("volume_profile", {})
                    intraday = micro.get("intraday_pattern", {})
                    poc = vol_profile.get("poc")
                    if poc is not None:
                        lines.append(
                            f"- \u6210\u4ea4\u91cf\u5256\u9762 POC: {_format_ntd(poc)}"
                            # 成交量剖面 POC
                        )
                    pattern = intraday.get("pattern", "")
                    sentiment = intraday.get("sentiment", 0)
                    if pattern and pattern != "normal":
                        lines.append(
                            f"- K\u7dda\u578b\u614b: {pattern}"
                            # K線型態
                            f" (\u60c5\u7dd2: {sentiment:+.2f})"
                            # 情緒
                        )

                # Red flags
                red_flags = c.get("red_flags", [])
                if red_flags:
                    lines.append(f"- \u8b66\u793a\u65d7\u6a19:")  # 警示旗標
                    for rf in red_flags:
                        lines.append(f"  - {rf}")

                lines.append("")

        # Disqualified list (compact)
        disqualified = s2.get("disqualified", [])
        if disqualified:
            lines.append(f"### \u5931\u683c\u540d\u55ae")  # 失格名單
            lines.append("")
            for d in disqualified[:20]:
                code = str(d.get("code", ""))
                name = str(d.get("name", ""))
                red_flags = d.get("red_flags", [])
                flag_text = "; ".join(red_flags[:2]) if red_flags else ""
                lines.append(f"- {code} {name}: {flag_text}")
            if len(disqualified) > 20:
                lines.append(f"- ...\u53ca\u5176\u4ed6 {len(disqualified) - 20} \u6a94")
                # ...及其他 N 檔
            lines.append("")
    else:
        lines.append(f"*Stage 2 \u8cc7\u6599\u4e0d\u53ef\u7528\uff08\u7565\u904e\uff09*")
        # Stage 2 資料不可用（略過）
        lines.append("")

    # --- Portfolio Status ---
    lines.append("---")
    lines.append("")
    lines.append(f"## \u6a21\u64ec\u5eab\u72c0\u614b")  # 模擬庫狀態
    lines.append("")

    portfolio = _compute_portfolio_summary(trades, sector_map)

    lines.append(f"- \u7e3d\u4ea4\u6613\u7b46\u6578: {portfolio['total_trades']}")
    # 總交易筆數
    lines.append(f"- \u6301\u5009\u4e2d: {portfolio['open_positions']}")
    # 持倉中
    lines.append(f"- \u5df2\u7d50\u7b97: {portfolio['closed_trades']}")
    # 已結算
    lines.append(
        f"- \u52dd\u7387: {portfolio['win_rate']:.1f}%"
        # 勝率
        f" ({portfolio['winning_trades']}\u52dd / {portfolio['losing_trades']}\u6557)"
        # 勝 / 敗
    )
    lines.append(f"- \u7e3d\u640d\u76ca: {_format_pct(portfolio['total_pnl_pct'])}")
    # 總損益
    lines.append(f"- \u5e73\u5747\u640d\u76ca: {_format_pct(portfolio['avg_pnl_pct'])}")
    # 平均損益
    lines.append("")

    # Open positions detail
    if portfolio["open_list"]:
        lines.append(f"### \u7576\u524d\u6301\u5009")  # 當前持倉
        lines.append("")
        lines.append(
            "| \u80a1\u7968\u4ee3\u865f |"
            # 股票代號
            " \u540d\u7a31 |"
            # 名稱
            " \u8cb7\u5165\u50f9 |"
            # 買入價
            " \u505c\u640d |"
            # 停損
            " \u505c\u5229 |"
            # 停利
            " \u7522\u696d |"
            # 產業
            " \u4fe1\u865f\u7b49\u7d1a |"
            # 信號等級
            " |"
        )
        lines.append(
            "|:------|:-----|------:|------:|------:|:-----|:------:|"
        )
        for t in portfolio["open_list"]:
            code = str(t.get("code", ""))
            name = str(t.get("name", ""))
            entry = t.get("entry_price", 0)
            stop = t.get("stop_loss", 0)
            tp = t.get("take_profit", 0)
            sector = t.get("sector", "other")
            sector_tc = _get_sector_tc(sector)
            combined = t.get("combined_score", 0)
            grade = format_signal_grade(combined)
            lines.append(
                f"| {code} | {name} | {_format_ntd(entry)} "
                f"| {_format_ntd(stop)} | {_format_ntd(tp)} "
                f"| {sector_tc} | {grade} |"
            )
        lines.append("")

    # Sector allocation
    if portfolio["sector_allocation"]:
        lines.append(f"### \u7522\u696d\u5206\u6563")  # 產業分散
        lines.append("")
        for sector, count in sorted(
            portfolio["sector_allocation"].items(),
            key=lambda x: x[1], reverse=True
        ):
            sector_tc = _get_sector_tc(sector)
            sector_pnl = portfolio["sector_pnl"].get(sector, 0)
            lines.append(
                f"- {sector_tc}: {count} \u7b46"
                # 筆
                f" (\u7d2f\u8a08\u640d\u76ca {_format_pct(sector_pnl)})"
                # 累計損益
            )
        lines.append("")

    # --- Sector Rotation ---
    lines.append("---")
    lines.append("")
    lines.append(f"## \u7522\u696d\u8f6e\u52d5\u8a0a\u865f")  # 產業輪動訊號
    lines.append("")

    sector_rotation = _load_sector_rotation(data_dir, date_str)
    if sector_rotation and sector_rotation.get("sectors"):
        overweight = sector_rotation.get("overweight_sectors", [])
        underweight = sector_rotation.get("underweight_sectors", [])
        neutral = sector_rotation.get("neutral_sectors", [])

        if overweight:
            parts = [_get_sector_tc(s) for s in overweight]
            lines.append(
                f"- \u504f\u591a: {', '.join(parts)}"
                # 偏多
            )
        if underweight:
            parts = [_get_sector_tc(s) for s in underweight]
            lines.append(
                f"- \u504f\u7a7a: {', '.join(parts)}"
                # 偏空
            )
        if neutral:
            parts = [_get_sector_tc(s) for s in neutral]
            lines.append(
                f"- \u4e2d\u6027: {', '.join(parts)}"
                # 中性
            )
        lines.append("")

        # Per-sector detail table
        sectors_data = sector_rotation.get("sectors", {})
        if sectors_data:
            lines.append(
                "| \u7522\u696d |"
                # 產業
                " \u8a0a\u865f |"
                # 訊號
                " \u7576\u524d\u5747\u5206 |"
                # 當前均分
                " \u6efe\u52d5\u5747\u5206 |"
                # 滾動均分
                " \u52d5\u80fd |"
                # 動能
                " |"
            )
            lines.append(
                "|:-----|:----:|------:|------:|------:|"
            )
            for sector_key, info in sorted(
                sectors_data.items(),
                key=lambda x: x[1].get("momentum", 0),
                reverse=True
            ):
                sector_tc = _get_sector_tc(sector_key)
                signal = info.get("signal", "neutral")
                current_avg = info.get("current_avg", 0)
                rolling_avg = info.get("rolling_avg", 0)
                momentum = info.get("momentum", 0)
                signal_tc = {
                    "overweight": "\u504f\u591a",   # 偏多
                    "underweight": "\u504f\u7a7a",   # 偏空
                    "neutral": "\u4e2d\u6027",       # 中性
                }.get(signal, signal)
                lines.append(
                    f"| {sector_tc} | {signal_tc} | {current_avg:.1f} "
                    f"| {rolling_avg:.1f} | {momentum:+.2f} |"
                )
            lines.append("")
    else:
        lines.append(f"*\u7522\u696d\u8f2a\u52d5\u8cc7\u6599\u4e0d\u53ef\u7528*")
        # 產業輪動資料不可用
        lines.append("")

    # --- Alert Digest ---
    lines.append("---")
    lines.append("")
    lines.append(f"## \u8b66\u793a\u6458\u8981")  # 警示摘要
    lines.append("")

    # Recent alerts (last 24 hours)
    recent_cutoff = datetime.now()
    from datetime import timedelta
    cutoff_24h = recent_cutoff - timedelta(hours=24)
    recent_alerts = []
    for a in alert_history:
        ts = a.get("timestamp", "")
        try:
            a_time = datetime.fromisoformat(ts)
            if a_time >= cutoff_24h:
                recent_alerts.append(a)
        except (ValueError, TypeError):
            pass

    if recent_alerts:
        lines.append(f"\u8fd1 24 \u5c0f\u6642\u8b66\u793a: {len(recent_alerts)} \u7b46")
        # 近 24 小時警示: N 筆
        lines.append("")
        # Group by type
        by_type: Dict[str, List[dict]] = {}
        for a in recent_alerts:
            a_type = a.get("type", "unknown")
            by_type.setdefault(a_type, []).append(a)

        for a_type, entries in sorted(by_type.items()):
            lines.append(f"- **{a_type}**: {len(entries)} \u7b46")
            # 筆
        lines.append("")
    else:
        lines.append(f"\u8fd1 24 \u5c0f\u6642\u7121\u8b66\u793a")
        # 近 24 小時無警示
        lines.append("")

    # Pending digest
    if pending_digest:
        lines.append(f"\u5f85\u767c\u9001\u6458\u8981: {len(pending_digest)} \u7b46")
        # 待發送摘要: N 筆
        lines.append("")

    # --- Footer ---
    lines.append("---")
    lines.append("")
    lines.append(
        f"*\u5831\u544a\u7522\u751f\u6642\u9593: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
        # 報告產生時間
    )
    lines.append(
        f"*tw-stock-hunter Phase 32 — Daily Report Generator*"
    )

    # ── Write to file ───────────────────────────────────────────────
    report_text = "\n".join(lines)
    report_file = output_path / f"{date_str}.md"

    try:
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report_text)
        logger.info("Daily report saved to %s", report_file)
    except (IOError, OSError) as e:
        logger.error("Failed to write report: %s", e)

    return report_text


def _load_sector_rotation(data_dir: Path, date_str: str) -> Optional[dict]:
    """Load or compute sector rotation data.

    Tries to use PaperTrader.compute_sector_rotation() first,
    falls back to lightweight inline calculation.
    """
    try:
        from paper_trader import PaperTrader
        trader = PaperTrader()
        return trader.compute_sector_rotation(date_str=date_str)
    except Exception as e:
        logger.debug("PaperTrader sector rotation failed: %s", e)

    # Lightweight fallback: compute from stage2 files
    try:
        return _compute_sector_rotation_lite(data_dir, date_str)
    except Exception as e:
        logger.debug("Lightweight sector rotation failed: %s", e)
        return None


def _compute_sector_rotation_lite(data_dir: Path, date_str: str,
                                  rolling_window: int = 5) -> dict:
    """Compute basic sector rotation from stage2 JSON files.

    Simplified version of PaperTrader.compute_sector_rotation() that
    doesn't require the full PaperTrader class.
    """
    sector_map = _load_sector_map(data_dir)
    stage2_files = sorted(data_dir.glob("stage2_*.json"))
    valid_files = [f for f in stage2_files
                   if f.stem.replace("stage2_", "") <= date_str]

    if not valid_files:
        return {
            "sectors": {}, "overweight_sectors": [],
            "underweight_sectors": [], "neutral_sectors": [],
            "rolling_window": rolling_window, "date": date_str,
        }

    window_files = valid_files[-rolling_window:]
    sector_scores: Dict[str, List[float]] = {}

    for filepath in window_files:
        data = _safe_load_json(filepath)
        if not data:
            continue
        for c in data.get("candidates", []):
            code = str(c.get("code", ""))
            sector = sector_map.get(code, "other")
            combined = c.get("combined_score", 0)
            try:
                sector_scores.setdefault(sector, []).append(float(combined))
            except (TypeError, ValueError):
                pass

    if not sector_scores:
        return {
            "sectors": {}, "overweight_sectors": [],
            "underweight_sectors": [], "neutral_sectors": [],
            "rolling_window": rolling_window, "date": date_str,
        }

    # Compute per-sector averages
    sectors_info = {}
    for sector, scores in sector_scores.items():
        if not scores:
            continue
        avg = sum(scores) / len(scores)
        sectors_info[sector] = {
            "current_avg": round(avg, 1),
            "rolling_avg": round(avg, 1),  # Same for lite version
            "momentum": 0.0,
            "signal": "neutral",
            "score_count": len(scores),
        }

    # Determine signals: compare against overall average
    all_avgs = [v["current_avg"] for v in sectors_info.values()]
    if all_avgs:
        overall_avg = sum(all_avgs) / len(all_avgs)
        std = (sum((a - overall_avg) ** 2 for a in all_avgs) / len(all_avgs)) ** 0.5

        overweight = []
        underweight = []
        neutral = []

        for sector, info in sectors_info.items():
            avg = info["current_avg"]
            if std > 0:
                if avg > overall_avg + std:
                    info["signal"] = "overweight"
                    info["momentum"] = round((avg - overall_avg) / std, 2)
                    overweight.append(sector)
                elif avg < overall_avg - std:
                    info["signal"] = "underweight"
                    info["momentum"] = round((avg - overall_avg) / std, 2)
                    underweight.append(sector)
                else:
                    neutral.append(sector)
            else:
                neutral.append(sector)

        return {
            "sectors": sectors_info,
            "overweight_sectors": overweight,
            "underweight_sectors": underweight,
            "neutral_sectors": neutral,
            "rolling_window": rolling_window,
            "date": date_str,
        }

    return {
        "sectors": sectors_info,
        "overweight_sectors": [],
        "underweight_sectors": [],
        "neutral_sectors": list(sectors_info.keys()),
        "rolling_window": rolling_window,
        "date": date_str,
    }


def generate_html_report(date_str: str = None,
                         output_dir: str = None) -> Optional[str]:
    """Generate an HTML variant of the daily report.

    Features:
      - Inline CSS for self-contained styling
      - Color-coded rows by signal grade (A/B green, C/D yellow, E red)
      - Collapsible deep-dive sections using <details>/<summary>
      - Responsive layout for desktop and mobile

    Args:
        date_str: Date in YYYY-MM-DD format. Defaults to today.
        output_dir: Directory to write the report. Defaults to
                    repo_root/reports/.

    Returns:
        The HTML report string, or None on complete failure.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    data_dir = Path(__file__).parent.parent / "data"

    if output_dir is None:
        output_dir = str(Path(__file__).parent.parent / "reports")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # ── Load data ───────────────────────────────────────────────────
    s1 = _load_stage1(data_dir, date_str)
    s2 = _load_stage2(data_dir, date_str)
    regime_data = _load_regime(data_dir)
    trades = _load_paper_trades(data_dir)
    sector_map = _load_sector_map(data_dir)
    alert_history = _load_alert_history(data_dir)

    regime = regime_data.get("regime", "unknown") if regime_data else "unknown"
    regime_badge = format_regime_badge(regime)
    roc_date = _iso_to_roc_display(date_str)

    # ── Grade → CSS class mapping ───────────────────────────────────
    def _grade_css_class(score: float) -> str:
        try:
            s = float(score)
        except (TypeError, ValueError):
            return "grade-e"
        if s >= 90:
            return "grade-a"
        elif s >= 80:
            return "grade-b"
        elif s >= 70:
            return "grade-c"
        elif s >= 60:
            return "grade-d"
        return "grade-e"

    # ── Build HTML ──────────────────────────────────────────────────
    html_parts: List[str] = []

    # Inline CSS
    html_parts.append("""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>\u6bcf\u65e5\u7be1\u9078\u5831\u544a — """ + roc_date + """</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: "Noto Sans TC", "Microsoft JhengHei", sans-serif;
    background: #f5f7fa; color: #333; line-height: 1.6;
    max-width: 1100px; margin: 0 auto; padding: 20px;
  }
  h1 { color: #1a237e; border-bottom: 3px solid #1a237e; padding-bottom: 10px; margin-bottom: 20px; }
  h2 { color: #283593; margin-top: 30px; margin-bottom: 15px; border-left: 4px solid #3f51b5; padding-left: 12px; }
  h3 { color: #3949ab; margin-top: 20px; margin-bottom: 10px; }
  h4 { color: #5c6bc0; margin-top: 15px; margin-bottom: 8px; }
  table { border-collapse: collapse; width: 100%; margin: 10px 0 20px; font-size: 14px; }
  th { background: #e8eaf6; color: #1a237e; font-weight: 600; }
  th, td { border: 1px solid #c5cae9; padding: 8px 12px; text-align: left; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  tr.grade-a, tr.grade-b { background: #e8f5e9; }
  tr.grade-c, tr.grade-d { background: #fff8e1; }
  tr.grade-e { background: #ffebee; }
  tr:hover { background: #e3f2fd !important; }
  .badge { display: inline-block; padding: 4px 12px; border-radius: 12px; font-weight: 600; font-size: 14px; }
  .badge-normal { background: #c8e6c9; color: #1b5e20; }
  .badge-caution { background: #fff9c4; color: #f57f17; }
  .badge-stress { background: #ffe0b2; color: #e65100; }
  .badge-crisis { background: #ffcdd2; color: #b71c1c; }
  .badge-black_swan { background: #d50000; color: #fff; }
  .badge-unknown { background: #e0e0e0; color: #616161; }
  .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 15px 0; }
  .stat-card { background: #fff; border-radius: 8px; padding: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.12); }
  .stat-card .label { color: #757575; font-size: 13px; }
  .stat-card .value { color: #1a237e; font-size: 24px; font-weight: 700; }
  .stat-card .value.positive { color: #2e7d32; }
  .stat-card .value.negative { color: #c62828; }
  details { margin: 8px 0; background: #fff; border-radius: 6px; box-shadow: 0 1px 2px rgba(0,0,0,0.1); }
  summary { padding: 10px 15px; cursor: pointer; font-weight: 600; color: #283593; }
  details[open] summary { border-bottom: 1px solid #e0e0e0; }
  .detail-content { padding: 10px 15px; }
  ul { margin: 8px 0 8px 20px; }
  li { margin: 4px 0; }
  .footer { margin-top: 30px; padding-top: 15px; border-top: 1px solid #e0e0e0; color: #9e9e9e; font-size: 12px; }
  hr { border: none; border-top: 1px solid #e0e0e0; margin: 25px 0; }
</style>
</head>
<body>
""")

    # Header
    html_parts.append(f"<h1>\u6bcf\u65e5\u7be1\u9078\u5831\u544a</h1>")
    # 每日篩選報告
    html_parts.append(f"<p><strong>{roc_date}</strong> ({date_str})</p>")

    # Regime badge
    regime_key = regime.lower().strip()
    badge_class = f"badge-{regime_key}" if regime_key in REGIME_BADGE else "badge-unknown"
    html_parts.append(
        f'<p>\u5e02\u5834\u72c0\u614b: <span class="badge {badge_class}">'
        # 市場狀態
        f"{regime_badge}</span></p>"
    )

    # Regime details
    if regime_data:
        vol = regime_data.get("volatility", 0)
        days_r = regime_data.get("days_in_regime", 0)
        conf = regime_data.get("confidence", "N/A")
        gr = regime_data.get("global_risk", "N/A")
        html_parts.append(
            f"<p>\u6ce2\u52d5\u7387: {vol:.4f} | "
            # 波動率
            f"\u4f4d\u65bc\u7576\u524d\u72c0\u614b: {days_r} \u5929 | "
            # 位於當前狀態: N 天
            f"\u4fe1\u5fc3: {conf} | "
            # 信心
            f"\u5168\u7403\u98a8\u96aa: {gr}</p>"
            # 全球風險
        )

    html_parts.append("<hr>")

    # ── Stage 1 ─────────────────────────────────────────────────────
    html_parts.append(f"<h2>\u7b2c\u4e00\u968e\u6bb5\uff1a\u91cf\u5316\u521d\u7be1</h2>")
    # 第一階段：量化初篩

    if s1:
        summary = s1.get("summary", {})
        total = summary.get("total_screened", 0)
        passed = summary.get("passed", 0)
        watchlist = summary.get("watchlist", 0)
        rejected = summary.get("rejected", 0)

        html_parts.append(f"""<div class="stats-grid">
  <div class="stat-card"><div class="label">\u7be1\u9078\u7e3d\u6578</div><div class="value">{total}</div></div>
  <div class="stat-card"><div class="label">\u901a\u904e</div><div class="value" style="color:#2e7d32">{passed}</div></div>
  <div class="stat-card"><div class="label">\u89c0\u5bdf\u540d\u55ae</div><div class="value" style="color:#f57f17">{watchlist}</div></div>
  <div class="stat-card"><div class="label">\u639b\u9664</div><div class="value" style="color:#c62828">{rejected}</div></div>
</div>""")
        # 篩選總數 / 通過 / 觀察名單 / 淘汰

        candidates = s1.get("candidates", [])
        passed_candidates = [c for c in candidates
                            if c.get("pass", c.get("composite_score", 0) >= 65)]
        passed_candidates.sort(key=lambda c: c.get("composite_score", 0), reverse=True)

        if passed_candidates:
            html_parts.append(f"<h3>\u901a\u904e\u540d\u55ae</h3>")
            # 通過名單
            html_parts.append(
                "<table><thead><tr>"
                "<th>\u80a1\u7968\u4ee3\u865f</th>"   # 股票代號
                "<th>\u540d\u7a31</th>"                 # 名稱
                "<th>\u7d9c\u5408\u8a55\u5206</th>"     # 綜合評分
                "<th>\u4fe1\u865f\u7b49\u7d1a</th>"     # 信號等級
                "<th>\u6536\u76e4\u50f9</th>"           # 收盤價
                "<th>\u7522\u696d</th>"                 # 產業
                "</tr></thead><tbody>"
            )
            for c in passed_candidates:
                code = str(c.get("code", ""))
                name = str(c.get("name", ""))
                score = c.get("composite_score", 0)
                grade = format_signal_grade(score)
                css_class = _grade_css_class(score)
                close = c.get("close", 0)
                sector_en = sector_map.get(code, "")
                sector_display = _get_sector_tc(sector_en) if sector_en else ""
                html_parts.append(
                    f'<tr class="{css_class}">'
                    f"<td>{code}</td><td>{name}</td>"
                    f'<td class="num">{score:.1f}</td>'
                    f"<td>{grade}</td>"
                    f'<td class="num">{_format_ntd(close)}</td>'
                    f"<td>{sector_display}</td></tr>"
                )
            html_parts.append("</tbody></table>")
    else:
        html_parts.append(f"<p><em>Stage 1 \u8cc7\u6599\u4e0d\u53ef\u7528</em></p>")
        # Stage 1 資料不可用

    html_parts.append("<hr>")

    # ── Stage 2 ─────────────────────────────────────────────────────
    html_parts.append(f"<h2>\u7b2c\u4e8c\u968e\u6bb5\uff1a\u57fa\u672c\u9762\u6df1\u5ea6\u6aa2\u8996</h2>")
    # 第二階段：基本面深度檢視

    if s2:
        s2_summary = s2.get("summary", {})
        s2_passed = s2_summary.get("passed_stage2", 0)
        s2_disqualified = s2_summary.get("disqualified", 0)

        html_parts.append(f"""<div class="stats-grid">
  <div class="stat-card"><div class="label">\u901a\u904e\u7b2c\u4e8c\u968e\u6bb5</div><div class="value" style="color:#2e7d32">{s2_passed}</div></div>
  <div class="stat-card"><div class="label">\u5931\u683c</div><div class="value" style="color:#c62828">{s2_disqualified}</div></div>
</div>""")
        # 通過第二階段 / 失格

        # Collapsible deep-dive sections
        s2_candidates = s2.get("candidates", [])
        s2_candidates.sort(key=lambda c: c.get("combined_score", 0), reverse=True)

        if s2_candidates:
            html_parts.append(f"<h3>\u901a\u904e\u5019\u9078\u8a73\u7d30\u8cc7\u6599</h3>")
            # 通過候選詳細資料

            for c in s2_candidates:
                code = str(c.get("code", ""))
                name = str(c.get("name", ""))
                combined = c.get("combined_score", 0)
                s1_score = c.get("stage1_score", 0)
                s2_score = c.get("stage2_score", 0)
                grade = format_signal_grade(combined)
                css_class = _grade_css_class(combined)

                html_parts.append(f"""<details>
<summary>{code} {name} — {grade} ({combined:.1f})</summary>
<div class="detail-content">
<p>\u7b2c\u4e00\u968e\u6bb5\u8a55\u5206: {s1_score:.1f} / \u7b2c\u4e8c\u968e\u6bb5\u8a55\u5206: {s2_score:.1f}</p>""")

                # Checks
                checks = c.get("checks", {})
                if checks:
                    check_labels = {
                        "dividend": "\u80a1\u5229",
                        "announcements": "\u91cd\u8981\u516c\u544a",
                        "shareholders": "\u80a1\u6771\u7d50\u69cb",
                        "pledge": "\u8cea\u62bc",
                        "penalties": "\u8655\u5206",
                        "news_sentiment": "\u65b0\u805e\u60c5\u7dd2",
                    }
                    # 股利 / 重要公告 / 股東結構 / 質押 / 處分 / 新聞情緒
                    html_parts.append("<ul>")
                    for key, label in check_labels.items():
                        chk = checks.get(key, {})
                        if chk:
                            score_val = chk.get("score")
                            status_val = chk.get("status", "")
                            if score_val is not None:
                                try:
                                    sv = float(score_val)
                                    g = format_signal_grade(sv)
                                    html_parts.append(
                                        f"<li>{label}: {g} ({sv:.0f}) — {status_val}</li>"
                                    )
                                except (TypeError, ValueError):
                                    html_parts.append(
                                        f"<li>{label}: {status_val}</li>"
                                    )
                            else:
                                html_parts.append(f"<li>{label}: {status_val}</li>")
                    html_parts.append("</ul>")

                # Microstructure
                micro = c.get("microstructure", {})
                if micro:
                    vol_profile = micro.get("volume_profile", {})
                    intraday = micro.get("intraday_pattern", {})
                    poc = vol_profile.get("poc")
                    if poc is not None:
                        html_parts.append(
                            f"<p>\u6210\u4ea4\u91cf\u5256\u9762 POC: {_format_ntd(poc)}</p>"
                            # 成交量剖面 POC
                        )
                    pattern = intraday.get("pattern", "")
                    sentiment = intraday.get("sentiment", 0)
                    if pattern and pattern != "normal":
                        html_parts.append(
                            f"<p>K\u7dda\u578b\u614b: {pattern} "
                            # K線型態
                            f"(\u60c5\u7dd2: {sentiment:+.2f})</p>"
                            # 情緒
                        )

                html_parts.append("</div></details>")

        # Disqualified (compact)
        disqualified = s2.get("disqualified", [])
        if disqualified:
            html_parts.append(f"<h3>\u5931\u683c\u540d\u55ae</h3>")
            # 失格名單
            html_parts.append("<ul>")
            for d in disqualified[:20]:
                code = str(d.get("code", ""))
                name = str(d.get("name", ""))
                red_flags = d.get("red_flags", [])
                flag_text = "; ".join(red_flags[:2]) if red_flags else ""
                html_parts.append(f"<li>{code} {name}: {flag_text}</li>")
            if len(disqualified) > 20:
                html_parts.append(
                    f"<li>...\u53ca\u5176\u4ed6 {len(disqualified) - 20} \u6a94</li>"
                    # ...及其他 N 檔
                )
            html_parts.append("</ul>")
    else:
        html_parts.append(f"<p><em>Stage 2 \u8cc7\u6599\u4e0d\u53ef\u7528</em></p>")
        # Stage 2 資料不可用

    html_parts.append("<hr>")

    # ── Portfolio Status ────────────────────────────────────────────
    html_parts.append(f"<h2>\u6a21\u64ec\u5eab\u72c0\u614b</h2>")
    # 模擬庫狀態

    portfolio = _compute_portfolio_summary(trades, sector_map)

    pnl_class = "positive" if portfolio["total_pnl_pct"] > 0 else "negative" if portfolio["total_pnl_pct"] < 0 else ""
    html_parts.append(f"""<div class="stats-grid">
  <div class="stat-card"><div class="label">\u7e3d\u4ea4\u6613\u7b46\u6578</div><div class="value">{portfolio['total_trades']}</div></div>
  <div class="stat-card"><div class="label">\u6301\u5009\u4e2d</div><div class="value">{portfolio['open_positions']}</div></div>
  <div class="stat-card"><div class="label">\u52dd\u7387</div><div class="value">{portfolio['win_rate']:.1f}%</div></div>
  <div class="stat-card"><div class="label">\u7e3d\u640d\u76ca</div><div class="value {pnl_class}">{_format_pct(portfolio['total_pnl_pct'])}</div></div>
</div>""")
    # 總交易筆數 / 持倉中 / 勝率 / 總損益

    # Open positions table
    if portfolio["open_list"]:
        html_parts.append(f"<h3>\u7576\u524d\u6301\u5009</h3>")
        # 當前持倉
        html_parts.append(
            "<table><thead><tr>"
            "<th>\u80a1\u7968\u4ee3\u865f</th>"   # 股票代號
            "<th>\u540d\u7a31</th>"                 # 名稱
            "<th>\u8cb7\u5165\u50f9</th>"           # 買入價
            "<th>\u505c\u640d</th>"                 # 停損
            "<th>\u505c\u5229</th>"                 # 停利
            "<th>\u7522\u696d</th>"                 # 產業
            "<th>\u4fe1\u865f\u7b49\u7d1a</th>"     # 信號等級
            "</tr></thead><tbody>"
        )
        for t in portfolio["open_list"]:
            code = str(t.get("code", ""))
            name = str(t.get("name", ""))
            entry = t.get("entry_price", 0)
            stop = t.get("stop_loss", 0)
            tp = t.get("take_profit", 0)
            sector = t.get("sector", "other")
            sector_tc = _get_sector_tc(sector)
            combined = t.get("combined_score", 0)
            grade = format_signal_grade(combined)
            css_class = _grade_css_class(combined)
            html_parts.append(
                f'<tr class="{css_class}">'
                f"<td>{code}</td><td>{name}</td>"
                f'<td class="num">{_format_ntd(entry)}</td>'
                f'<td class="num">{_format_ntd(stop)}</td>'
                f'<td class="num">{_format_ntd(tp)}</td>'
                f"<td>{sector_tc}</td><td>{grade}</td></tr>"
            )
        html_parts.append("</tbody></table>")

    # Sector allocation
    if portfolio["sector_allocation"]:
        html_parts.append(f"<h3>\u7522\u696d\u5206\u6563</h3>")
        # 產業分散
        html_parts.append("<ul>")
        for sector, count in sorted(
            portfolio["sector_allocation"].items(),
            key=lambda x: x[1], reverse=True
        ):
            sector_tc = _get_sector_tc(sector)
            sector_pnl = portfolio["sector_pnl"].get(sector, 0)
            pnl_cls = "positive" if sector_pnl > 0 else "negative" if sector_pnl < 0 else ""
            html_parts.append(
                f"<li>{sector_tc}: {count} \u7b46 "
                f"(\u7d2f\u8a08\u640d\u76ca <span class='{pnl_cls}'>"
                f"{_format_pct(sector_pnl)}</span>)</li>"
                # 筆 / 累計損益
            )
        html_parts.append("</ul>")

    html_parts.append("<hr>")

    # ── Sector Rotation ─────────────────────────────────────────────
    html_parts.append(f"<h2>\u7522\u696d\u8f2a\u52d5\u8a0a\u865f</h2>")
    # 產業輪動訊號

    sector_rotation = _load_sector_rotation(data_dir, date_str)
    if sector_rotation and sector_rotation.get("sectors"):
        overweight = sector_rotation.get("overweight_sectors", [])
        underweight = sector_rotation.get("underweight_sectors", [])

        if overweight:
            parts = [_get_sector_tc(s) for s in overweight]
            html_parts.append(
                f'<p>\u504f\u591a: <span style="color:#2e7d32;font-weight:600">'
                # 偏多
                f'{", ".join(parts)}</span></p>'
            )
        if underweight:
            parts = [_get_sector_tc(s) for s in underweight]
            html_parts.append(
                f'<p>\u504f\u7a7a: <span style="color:#c62828;font-weight:600">'
                # 偏空
                f'{", ".join(parts)}</span></p>'
            )

        sectors_data = sector_rotation.get("sectors", {})
        if sectors_data:
            html_parts.append(
                "<table><thead><tr>"
                "<th>\u7522\u696d</th>"      # 產業
                "<th>\u8a0a\u865f</th>"      # 訊號
                "<th>\u7576\u524d\u5747\u5206</th>"   # 當前均分
                "<th>\u6efe\u52d5\u5747\u5206</th>"   # 滾動均分
                "<th>\u52d5\u80fd</th>"      # 動能
                "</tr></thead><tbody>"
            )
            for sector_key, info in sorted(
                sectors_data.items(),
                key=lambda x: x[1].get("momentum", 0),
                reverse=True
            ):
                sector_tc = _get_sector_tc(sector_key)
                signal = info.get("signal", "neutral")
                current_avg = info.get("current_avg", 0)
                rolling_avg = info.get("rolling_avg", 0)
                momentum = info.get("momentum", 0)
                signal_tc = {
                    "overweight": "\u504f\u591a",
                    "underweight": "\u504f\u7a7a",
                    "neutral": "\u4e2d\u6027",
                }.get(signal, signal)
                # 偏多 / 偏空 / 中性
                sig_color = "#2e7d32" if signal == "overweight" else "#c62828" if signal == "underweight" else "#757575"
                html_parts.append(
                    f"<tr>"
                    f"<td>{sector_tc}</td>"
                    f'<td style="color:{sig_color};font-weight:600">{signal_tc}</td>'
                    f'<td class="num">{current_avg:.1f}</td>'
                    f'<td class="num">{rolling_avg:.1f}</td>'
                    f'<td class="num">{momentum:+.2f}</td>'
                    f"</tr>"
                )
            html_parts.append("</tbody></table>")
    else:
        html_parts.append(f"<p><em>\u7522\u696d\u8f2a\u52d5\u8cc7\u6599\u4e0d\u53ef\u7528</em></p>")
        # 產業輪動資料不可用

    html_parts.append("<hr>")

    # ── Alert Digest ────────────────────────────────────────────────
    html_parts.append(f"<h2>\u8b66\u793a\u6458\u8981</h2>")
    # 警示摘要

    from datetime import timedelta
    cutoff_24h = datetime.now() - timedelta(hours=24)
    recent_alerts = []
    for a in alert_history:
        ts = a.get("timestamp", "")
        try:
            a_time = datetime.fromisoformat(ts)
            if a_time >= cutoff_24h:
                recent_alerts.append(a)
        except (ValueError, TypeError):
            pass

    if recent_alerts:
        html_parts.append(
            f"<p>\u8fd1 24 \u5c0f\u6642\u8b66\u793a: <strong>{len(recent_alerts)}</strong> \u7b46</p>"
            # 近 24 小時警示: N 筆
        )
        by_type: Dict[str, List[dict]] = {}
        for a in recent_alerts:
            a_type = a.get("type", "unknown")
            by_type.setdefault(a_type, []).append(a)

        html_parts.append("<ul>")
        for a_type, entries in sorted(by_type.items()):
            html_parts.append(f"<li><strong>{a_type}</strong>: {len(entries)} \u7b46</li>")
            # 筆
        html_parts.append("</ul>")
    else:
        html_parts.append(f"<p>\u8fd1 24 \u5c0f\u6642\u7121\u8b66\u793a</p>")
        # 近 24 小時無警示

    # ── Footer ──────────────────────────────────────────────────────
    html_parts.append(f"""<div class="footer">
<p>\u5831\u544a\u7522\u751f\u6642\u9593: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
<p>tw-stock-hunter Phase 32 — Daily Report Generator</p>
</div>
</body>
</html>""")

    # ── Write to file ───────────────────────────────────────────────
    html_text = "\n".join(html_parts)
    html_file = output_path / f"{date_str}.html"

    try:
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_text)
        logger.info("HTML report saved to %s", html_file)
    except (IOError, OSError) as e:
        logger.error("Failed to write HTML report: %s", e)

    return html_text


def main():
    """CLI entry point for report generation."""
    import argparse

    parser = argparse.ArgumentParser(
        description="\u7522\u751f\u6bcf\u65e5\u7be1\u9078\u5831\u544a (Phase 32)",
        # 產生每日篩選報告
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="\u65e5\u671f (YYYY-MM-DD)\uff0c\u9810\u8a2d\u70ba\u4eca\u5929",
        # 日期 (YYYY-MM-DD)，預設為今天
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="\u5831\u544a\u8f38\u51fa\u76ee\u9304",
        # 報告輸出目錄
    )
    parser.add_argument(
        "--html", action="store_true",
        help="\u540c\u6642\u7522\u751f HTML \u5831\u544a",
        # 同時產生 HTML 報告
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="\u8a73\u7d30\u8f38\u51fa",
        # 詳細輸出
    )
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")

    md_report = generate_daily_report(date_str, args.output_dir)
    if md_report and args.verbose:
        print(md_report)

    if args.html:
        html_report = generate_html_report(date_str, args.output_dir)
        if html_report and args.verbose:
            print(f"HTML report generated ({len(html_report)} chars)")


if __name__ == "__main__":
    main()
