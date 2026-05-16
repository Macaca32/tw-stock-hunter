#!/usr/bin/env python3
"""
Phase 35: Earnings Season Engine
Integrates quarterly earnings reports (季報) into Stage 2 deep scoring.

Provides YoY/QoQ growth metrics, analyst surprise checks, and a composite
earnings signal that feeds into Stage 2 as a new scoring dimension.

All labels use Traditional Chinese for Taiwan-market awareness.
Backward compatible: every function returns None/neutral when data is missing.
"""

import json
import logging
import math
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Re-use helpers from the existing codebase
import sys
sys.path.insert(0, str(Path(__file__).parent))
from stage1_screen import safe_float, get_field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Taiwan fiscal quarters: Q1=1-3月, Q2=4-6月, Q3=7-9月, Q4=10-12月
QUARTER_MONTHS = {"Q1": 3, "Q2": 6, "Q3": 9, "Q4": 12}

# TWSE quarterly financials endpoint (財務報表)
TWSE_QUARTERLY_ENDPOINT = "https://openapi.twse.com.tw/v1/opendata/t187ap14_L"  # 綜合損益

# EPS field keys from TWSE dividend/financial data
EPS_KEYS_CN = ["每股盈餘", "基本每股盈餘(元)", "稀釋每股盈餘(元)"]
REVENUE_KEYS_CN = ["營業收入-當月營收", "營業收入合計", "營業收入淨額"]
PROFIT_KEYS_CN = ["本期淨利(淨損)(元)", "稅後淨利", "本期淨利"]

# Cache file location
_DATA_DIR = Path(__file__).parent.parent / "data"


# ---------------------------------------------------------------------------
#  Internal helpers
# ---------------------------------------------------------------------------

def _data_dir() -> Path:
    """Return the project data directory, creating it if needed."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    return _DATA_DIR


def _cache_path() -> Path:
    return _data_dir() / "earnings_cache.json"


def _consensus_path() -> Path:
    return _data_dir() / "analyst_consensus.json"


def _load_cache() -> Dict:
    """Load the earnings cache, returning empty dict on failure."""
    cp = _cache_path()
    if not cp.exists():
        return {}
    try:
        with open(cp, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _save_cache(cache: Dict) -> None:
    """Persist the earnings cache to disk."""
    cp = _cache_path()
    try:
        with open(cp, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logger.warning("Phase 35: failed to write earnings cache: %s", e)


def _is_cache_fresh(entry: Dict) -> bool:
    """Check whether a cache entry is within TTL."""
    ts = entry.get("_cached_at", 0)
    return (time.time() - ts) < CACHE_TTL_SECONDS


def _quarter_for_date(date_str: str) -> str:
    """Derive the fiscal quarter from a YYYY-MM-DD or YYYYMMDD date string."""
    try:
        clean = date_str.replace("-", "")[:8]
        month = int(clean[4:6])
        if month <= 3:
            return "Q1"
        elif month <= 6:
            return "Q2"
        elif month <= 9:
            return "Q3"
        else:
            return "Q4"
    except (ValueError, IndexError):
        return "Q4"  # default to latest quarter


def _year_for_date(date_str: str) -> int:
    """Derive the year from a date string."""
    try:
        return int(date_str.replace("-", "")[:4])
    except (ValueError, IndexError):
        return datetime.now().year


def _prior_quarter(quarter: str, year: int) -> Tuple[str, int]:
    """Return the (quarter, year) of the previous quarter."""
    q_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    q_num = q_order.get(quarter, 4)
    if q_num == 1:
        return "Q4", year - 1
    else:
        return f"Q{q_num - 1}", year


def _same_quarter_last_year(quarter: str, year: int) -> Tuple[str, int]:
    """Return the same quarter from the prior year."""
    return quarter, year - 1


def _extract_eps(record: Dict) -> float:
    """Extract EPS from a financial record, trying multiple field names."""
    for key in EPS_KEYS_CN:
        val = record.get(key)
        if val is not None and val != "":
            return safe_float(val, None) or 0.0
    # English fallback
    val = record.get("eps") or record.get("EPS") or record.get("earnings_per_share")
    return safe_float(val, 0.0)


def _extract_revenue(record: Dict) -> float:
    """Extract revenue from a financial record."""
    for key in REVENUE_KEYS_CN:
        val = record.get(key)
        if val is not None and val != "":
            return safe_float(val, None) or 0.0
    val = record.get("revenue") or record.get("current_month_revenue")
    return safe_float(val, 0.0)


def _extract_net_profit(record: Dict) -> float:
    """Extract net profit from a financial record."""
    for key in PROFIT_KEYS_CN:
        val = record.get(key)
        if val is not None and val != "":
            return safe_float(val, None) or 0.0
    val = record.get("net_profit") or record.get("net_income")
    return safe_float(val, 0.0)


def _pct_change(current: float, prior: float) -> Optional[float]:
    """Compute percentage change; return None if denominator is zero/negative."""
    if not prior or prior <= 0:
        return None
    return round((current - prior) / prior * 100, 2)


def _fetch_from_twse_api(stock_id: str) -> Optional[Dict]:
    """Attempt to fetch quarterly financials from TWSE API.

    Returns a dict with extracted metrics or None on failure.
    This is a best-effort fetch; failures are non-fatal.
    """
    try:
        import requests
        resp = requests.get(TWSE_QUARTERLY_ENDPOINT, timeout=15)
        if resp.status_code != 200:
            return None

        data = resp.json()
        if not isinstance(data, list):
            return None

        for record in data:
            code = get_field(record, "公司代號", "Code", "")
            if str(code).strip() == str(stock_id).strip():
                return {
                    "revenue": _extract_revenue(record),
                    "net_profit": _extract_net_profit(record),
                    "eps": _extract_eps(record),
                    "source": "twse_api",
                    "fetched_at": datetime.now().isoformat(),
                }
        return None
    except Exception as e:
        logger.debug("Phase 35: TWSE API fetch failed for %s: %r", stock_id, e)
        return None


def _find_quarterly_in_dividends(stock_id: str, quarter: str, year: int,
                                  dividends_data: list) -> Optional[Dict]:
    """Search existing dividends_*.json for EPS/profit data matching a quarter.

    The dividends data from TWSE may contain quarterly breakdowns with
    fields like 本期淨利(淨損)(元) and 每股盈餘. We attempt to match by
    year and quarter hints in the record.
    """
    for rec in dividends_data:
        code = get_field(rec, "公司代號", "Code", "")
        if str(code).strip() != str(stock_id).strip():
            continue

        # Try to match year/quarter from the record's date field
        date_val = get_field(rec, "資料年度", "year", "")
        if date_val:
            try:
                rec_year = int(str(date_val).strip()[:4])
                # ROC year: if > 1911, it's already AD; otherwise add 1911
                if rec_year < 200:
                    rec_year += 1911
                if rec_year != year:
                    continue
            except (ValueError, IndexError):
                pass

        eps = _extract_eps(rec)
        revenue = _extract_revenue(rec)
        profit = _extract_net_profit(rec)

        # Only return if at least one meaningful metric exists
        if eps > 0 or revenue > 0 or profit != 0:
            return {
                "revenue": revenue,
                "net_profit": profit,
                "eps": eps,
                "source": "dividends_json",
            }

    return None


# ---------------------------------------------------------------------------
#  Public API
# ---------------------------------------------------------------------------

def fetch_earnings_data(stock_id: str, quarter: str = "Q1",
                        date_str: str = None) -> Optional[Dict]:
    """Pull quarterly financials for a stock.

    Data sources (in priority order):
      1. Local cache (data/earnings_cache.json) with 24h TTL
      2. Existing data/dividends_*.json files
      3. TWSE API (opendata/t187ap14_L)

    Args:
        stock_id: 4-digit Taiwan stock code (e.g. "2330")
        quarter:  Target quarter ("Q1".."Q4"). Defaults to "Q1".
        date_str: Reference date (YYYY-MM-DD) to derive year/quarter
                  if quarter not explicitly given.

    Returns:
        Dict with keys: stock_id, quarter, year, revenue, net_profit, eps,
        prior_quarter_revenue, prior_quarter_profit, prior_quarter_eps,
        yoy_quarter_revenue, yoy_quarter_profit, yoy_quarter_eps,
        source, fetched_at.
        Returns None if no data is available at all.
    """
    stock_id = str(stock_id).strip().zfill(4)

    # Derive year from date_str or use current year
    if date_str:
        year = _year_for_date(date_str)
        if quarter is None or quarter == "Q1":
            quarter = _quarter_for_date(date_str)
    else:
        now = datetime.now()
        year = now.year
        if quarter is None:
            quarter = _quarter_for_date(now.strftime("%Y-%m-%d"))

    # --- Check cache first ---
    cache = _load_cache()
    cache_key = f"{stock_id}_{year}_{quarter}"
    cached = cache.get(cache_key)
    if cached and _is_cache_fresh(cached):
        logger.debug("Phase 35: cache hit for %s/%s", cache_key, quarter)
        return cached

    # --- Try dividends JSON files ---
    data_dir = _data_dir()
    dividends_data = []

    # Find latest dividends file
    div_files = sorted(data_dir.glob("dividends_*.json"))
    if div_files:
        try:
            with open(div_files[-1], "r", encoding="utf-8") as f:
                dividends_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            dividends_data = []

    result = _find_quarterly_in_dividends(stock_id, quarter, year, dividends_data)

    # --- Try TWSE API if dividends data didn't have what we need ---
    if result is None or (result.get("eps", 0) == 0 and result.get("revenue", 0) == 0):
        api_result = _fetch_from_twse_api(stock_id)
        if api_result:
            result = api_result

    if result is None:
        # No data available at all — return None (backward compatible)
        return None

    # --- Enrich with prior quarter & YoY comparison data ---
    prior_q, prior_y = _prior_quarter(quarter, year)
    yoy_q, yoy_y = _same_quarter_last_year(quarter, year)

    # Try to find prior quarter data
    prior_result = _find_quarterly_in_dividends(stock_id, prior_q, prior_y, dividends_data)
    yoy_result = _find_quarterly_in_dividends(stock_id, yoy_q, yoy_y, dividends_data)

    # Build comprehensive result
    enriched = {
        "stock_id": stock_id,
        "quarter": quarter,
        "year": year,
        "revenue": result.get("revenue", 0.0),
        "net_profit": result.get("net_profit", 0.0),
        "eps": result.get("eps", 0.0),
        "prior_quarter": prior_q,
        "prior_quarter_year": prior_y,
        "prior_quarter_revenue": prior_result.get("revenue", 0.0) if prior_result else None,
        "prior_quarter_profit": prior_result.get("net_profit", 0.0) if prior_result else None,
        "prior_quarter_eps": prior_result.get("eps", 0.0) if prior_result else None,
        "yoy_quarter": yoy_q,
        "yoy_year": yoy_y,
        "yoy_quarter_revenue": yoy_result.get("revenue", 0.0) if yoy_result else None,
        "yoy_quarter_profit": yoy_result.get("net_profit", 0.0) if yoy_result else None,
        "yoy_quarter_eps": yoy_result.get("eps", 0.0) if yoy_result else None,
        "source": result.get("source", "unknown"),
        "fetched_at": datetime.now().isoformat(),
    }

    # Cache the result
    enriched["_cached_at"] = time.time()
    cache[cache_key] = enriched
    _save_cache(cache)

    return enriched


def compute_yoy_growth(stock_id: str, quarter: str = None,
                       date_str: str = None) -> Optional[Dict]:
    """Compute Year-over-Year growth for the same quarter last year.

    Args:
        stock_id: 4-digit Taiwan stock code.
        quarter:  Override quarter (default: derived from date_str or today).
        date_str: Reference date string.

    Returns:
        Dict with revenue_yoy_pct, profit_yoy_pct, eps_yoy_pct.
        Values are None when prior-year data is unavailable.
        Returns None if no earnings data at all.
    """
    data = fetch_earnings_data(stock_id, quarter=quarter, date_str=date_str)
    if data is None:
        return None

    result = {
        "stock_id": stock_id,
        "revenue_yoy_pct": None,
        "profit_yoy_pct": None,
        "eps_yoy_pct": None,
    }

    # YoY: current vs same quarter last year
    if data.get("yoy_quarter_revenue") is not None:
        result["revenue_yoy_pct"] = _pct_change(
            data["revenue"], data["yoy_quarter_revenue"]
        )
    if data.get("yoy_quarter_profit") is not None:
        result["profit_yoy_pct"] = _pct_change(
            data["net_profit"], data["yoy_quarter_profit"]
        )
    if data.get("yoy_quarter_eps") is not None:
        result["eps_yoy_pct"] = _pct_change(
            data["eps"], data["yoy_quarter_eps"]
        )

    return result


def compute_qoq_growth(stock_id: str, quarter: str = None,
                       date_str: str = None) -> Optional[Dict]:
    """Compute Quarter-over-Quarter growth vs the prior quarter.

    Args:
        stock_id: 4-digit Taiwan stock code.
        quarter:  Override quarter.
        date_str: Reference date string.

    Returns:
        Dict with revenue_qoq_pct, profit_qoq_pct, eps_qoq_pct.
        Values are None when prior-quarter data is unavailable.
        Returns None if no earnings data at all.
    """
    data = fetch_earnings_data(stock_id, quarter=quarter, date_str=date_str)
    if data is None:
        return None

    result = {
        "stock_id": stock_id,
        "revenue_qoq_pct": None,
        "profit_qoq_pct": None,
        "eps_qoq_pct": None,
    }

    if data.get("prior_quarter_revenue") is not None:
        result["revenue_qoq_pct"] = _pct_change(
            data["revenue"], data["prior_quarter_revenue"]
        )
    if data.get("prior_quarter_profit") is not None:
        result["profit_qoq_pct"] = _pct_change(
            data["net_profit"], data["prior_quarter_profit"]
        )
    if data.get("prior_quarter_eps") is not None:
        result["eps_qoq_pct"] = _pct_change(
            data["eps"], data["prior_quarter_eps"]
        )

    return result


def check_estimate_surprise(stock_id: str, date_str: str = None) -> Optional[Dict]:
    """Compare reported EPS vs analyst consensus estimate.

    Reads analyst estimates from data/analyst_consensus.json (if available).
    This file is optional — if absent or the stock is not listed, the function
    gracefully returns a neutral result rather than failing.

    Args:
        stock_id: 4-digit Taiwan stock code.
        date_str: Reference date string (used to determine which quarter).

    Returns:
        Dict with reported_eps, consensus_eps, surprise_pct, grade.
        grade is one of: "beat", "in-line", "miss", "no_consensus".
        Returns None if no earnings data available at all.
    """
    data = fetch_earnings_data(stock_id, date_str=date_str)
    if data is None:
        return None

    reported_eps = data.get("eps", 0.0)

    # Load analyst consensus (optional file)
    consensus_data = {}
    cp = _consensus_path()
    if cp.exists():
        try:
            with open(cp, "r", encoding="utf-8") as f:
                consensus_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            consensus_data = {}

    # Look up this stock's consensus
    stock_consensus = consensus_data.get(stock_id, consensus_data.get(str(stock_id).zfill(4), {}))
    consensus_eps = safe_float(stock_consensus.get("consensus_eps"), None)

    if consensus_eps is None or consensus_eps == 0:
        return {
            "stock_id": stock_id,
            "reported_eps": reported_eps,
            "consensus_eps": None,
            "surprise_pct": None,
            "grade": "no_consensus",
        }

    surprise_pct = _pct_change(reported_eps, consensus_eps)

    # Grade: within ±3% = in-line, >3% = beat, <-3% = miss
    if surprise_pct is None:
        grade = "no_consensus"
    elif surprise_pct > 3.0:
        grade = "beat"
    elif surprise_pct < -3.0:
        grade = "miss"
    else:
        grade = "in-line"

    return {
        "stock_id": stock_id,
        "reported_eps": reported_eps,
        "consensus_eps": consensus_eps,
        "surprise_pct": surprise_pct,
        "grade": grade,
    }


def get_earnings_signal(stock_id: str, date_str: str = None) -> float:
    """Compute a composite earnings signal adjustment for Stage 2 scoring.

    The signal ranges from -0.15 to +0.15, based on:
      - Growth momentum acceleration (YoY improving vs QoQ)
      - Consecutive beat streaks (multiple quarters beating consensus)
      - Seasonal patterns (Q4 tends to be strongest in Taiwan)

    This signal is designed to be added to the Stage 2 combined score
    as a marginal adjustment, not a dominant factor.

    Args:
        stock_id: 4-digit Taiwan stock code.
        date_str: Reference date string.

    Returns:
        Float from -0.15 to +0.15. Returns 0.0 if data unavailable.
    """
    signal = 0.0

    # 1. Growth momentum (±0.07)
    yoy = compute_yoy_growth(stock_id, date_str=date_str)
    qoq = compute_qoq_growth(stock_id, date_str=date_str)

    if yoy is not None:
        # Acceleration: YoY positive AND QoQ positive = accelerating growth
        yoy_eps = yoy.get("eps_yoy_pct")
        qoq_eps = qoq.get("eps_qoq_pct") if qoq else None

        if yoy_eps is not None:
            # Base YoY signal: scale into [-0.05, +0.05]
            if yoy_eps > 50:
                signal += 0.05
            elif yoy_eps > 20:
                signal += 0.04
            elif yoy_eps > 10:
                signal += 0.03
            elif yoy_eps > 0:
                signal += 0.01
            elif yoy_eps < -20:
                signal -= 0.05
            elif yoy_eps < -10:
                signal -= 0.03
            elif yoy_eps < 0:
                signal -= 0.01

        # Acceleration bonus: QoQ improving on top of positive YoY
        if yoy_eps is not None and yoy_eps > 0 and qoq_eps is not None and qoq_eps > 0:
            signal += 0.02  # Acceleration bonus
        elif yoy_eps is not None and yoy_eps > 0 and qoq_eps is not None and qoq_eps < 0:
            signal -= 0.01  # Deceleration penalty
        elif yoy_eps is not None and yoy_eps < 0 and qoq_eps is not None and qoq_eps < 0:
            signal -= 0.02  # Deterioration penalty

    # 2. Earnings surprise / beat streak (±0.05)
    surprise = check_estimate_surprise(stock_id, date_str=date_str)
    if surprise is not None:
        grade = surprise.get("grade", "no_consensus")
        if grade == "beat":
            signal += 0.04
        elif grade == "miss":
            signal -= 0.05
        elif grade == "in-line":
            signal += 0.01  # Meeting expectations is mildly positive

        # Check for consecutive beats from consensus history
        cp = _consensus_path()
        if cp.exists():
            try:
                with open(cp, "r", encoding="utf-8") as f:
                    consensus_data = json.load(f)
                stock_cons = consensus_data.get(stock_id, consensus_data.get(str(stock_id).zfill(4), {}))
                beat_streak = safe_float(stock_cons.get("beat_streak", 0), 0)
                if beat_streak >= 3:
                    signal += 0.03  # Strong earnings momentum
                elif beat_streak >= 2:
                    signal += 0.01
            except Exception:
                pass

    # 3. Seasonal pattern (±0.03)
    # Taiwan stocks typically report strongest Q4 (year-end) and Q1 (spring rally)
    if date_str:
        quarter = _quarter_for_date(date_str)
    else:
        quarter = _quarter_for_date(datetime.now().strftime("%Y-%m-%d"))

    if quarter == "Q4":
        signal += 0.02  # Q4 earnings season tends to drive strong moves
    elif quarter == "Q1":
        signal += 0.01  # Spring rally from Q4 earnings
    elif quarter == "Q2":
        signal -= 0.01  # Q2 often weakest (slow season before Q3)

    # Clamp to [-0.15, +0.15]
    signal = max(-0.15, min(0.15, round(signal, 4)))

    return signal


# ---------------------------------------------------------------------------
#  Stage 2 integration: check_earnings_quality
# ---------------------------------------------------------------------------

def check_earnings_quality(stock_id: str, date_str: str = None) -> Tuple[Optional[float], str]:
    """Stage 2 earnings quality check (0-100 scale).

    This function is designed to be called from stage2_deep.py as a new
    scoring dimension. It synthesizes YoY growth, QoQ growth, earnings
    surprise, and growth momentum into a single 0-100 score.

    Scoring breakdown:
      - YoY growth (0-35 points): How fast earnings are growing year-over-year
      - QoQ growth (0-20 points): Sequential quarter improvement
      - Earnings surprise (0-25 points): Beat/miss vs consensus
      - Growth momentum (0-20 points): Acceleration vs deceleration

    Args:
        stock_id: 4-digit Taiwan stock code.
        date_str: Reference date string.

    Returns:
        Tuple of (score, status). Score is 0-100 or None on error.
        Status is one of: "strong_growth", "moderate_growth", "flat",
        "declining", "no_data", "error".
    """
    try:
        score = 50.0  # Neutral base

        # --- YoY Growth Component (0-35 points, centered at 17.5) ---
        yoy = compute_yoy_growth(stock_id, date_str=date_str)
        yoy_component = 17.5  # Neutral
        if yoy is not None:
            eps_yoy = yoy.get("eps_yoy_pct")
            rev_yoy = yoy.get("revenue_yoy_pct")
            if eps_yoy is not None:
                if eps_yoy > 50:
                    yoy_component = 35.0
                elif eps_yoy > 30:
                    yoy_component = 30.0
                elif eps_yoy > 15:
                    yoy_component = 25.0
                elif eps_yoy > 5:
                    yoy_component = 20.0
                elif eps_yoy > 0:
                    yoy_component = 18.0
                elif eps_yoy > -10:
                    yoy_component = 12.0
                elif eps_yoy > -20:
                    yoy_component = 7.0
                else:
                    yoy_component = 0.0

            # Revenue YoY as secondary confirmation
            if rev_yoy is not None:
                if rev_yoy > 0 and (eps_yoy is None or eps_yoy > 0):
                    yoy_component = min(35.0, yoy_component + 2.0)
                elif rev_yoy < 0 and (eps_yoy is None or eps_yoy < 0):
                    yoy_component = max(0.0, yoy_component - 3.0)

        score = score - 17.5 + yoy_component  # Replace neutral base with actual

        # --- QoQ Growth Component (0-20 points, centered at 10) ---
        qoq = compute_qoq_growth(stock_id, date_str=date_str)
        qoq_component = 10.0  # Neutral
        if qoq is not None:
            eps_qoq = qoq.get("eps_qoq_pct")
            if eps_qoq is not None:
                if eps_qoq > 30:
                    qoq_component = 20.0
                elif eps_qoq > 15:
                    qoq_component = 16.0
                elif eps_qoq > 5:
                    qoq_component = 13.0
                elif eps_qoq > 0:
                    qoq_component = 11.0
                elif eps_qoq > -10:
                    qoq_component = 8.0
                elif eps_qoq > -20:
                    qoq_component = 4.0
                else:
                    qoq_component = 0.0

        score = score - 10.0 + qoq_component

        # --- Earnings Surprise Component (0-25 points, centered at 12.5) ---
        surprise = check_estimate_surprise(stock_id, date_str=date_str)
        surprise_component = 12.5  # Neutral
        if surprise is not None:
            grade = surprise.get("grade", "no_consensus")
            surprise_pct = surprise.get("surprise_pct")
            if grade == "beat":
                if surprise_pct and surprise_pct > 10:
                    surprise_component = 25.0
                elif surprise_pct and surprise_pct > 5:
                    surprise_component = 22.0
                else:
                    surprise_component = 18.0
            elif grade == "in-line":
                surprise_component = 13.0
            elif grade == "miss":
                if surprise_pct and surprise_pct < -10:
                    surprise_component = 0.0
                elif surprise_pct and surprise_pct < -5:
                    surprise_component = 4.0
                else:
                    surprise_component = 7.0

        score = score - 12.5 + surprise_component

        # --- Growth Momentum Component (0-20 points) ---
        # Acceleration: YoY positive AND QoQ positive
        momentum_component = 10.0  # Neutral
        if yoy is not None and qoq is not None:
            eps_yoy = yoy.get("eps_yoy_pct")
            eps_qoq = qoq.get("eps_qoq_pct")
            if eps_yoy is not None and eps_qoq is not None:
                if eps_yoy > 0 and eps_qoq > 0:
                    momentum_component = 20.0  # Accelerating
                elif eps_yoy > 0 and eps_qoq < 0:
                    momentum_component = 8.0   # Decelerating but still positive
                elif eps_yoy < 0 and eps_qoq > 0:
                    momentum_component = 12.0  # Recovering
                elif eps_yoy < 0 and eps_qoq < 0:
                    momentum_component = 0.0   # Deteriorating

        score = score - 10.0 + momentum_component

        # Clamp score to [0, 100]
        score = round(max(0.0, min(100.0, score)), 2)

        # Determine status label (Traditional Chinese)
        if score >= 75:
            status = "strong_growth"
        elif score >= 55:
            status = "moderate_growth"
        elif score >= 40:
            status = "flat"
        else:
            status = "declining"

        # If all data sources returned None, mark as no_data
        if (yoy is None and qoq is None and surprise is None):
            return 50.0, "no_data"

        return score, status

    except Exception as e:
        logger.debug("Phase 35: check_earnings_quality failed for %s: %r", stock_id, e)
        return None, "error"


# ---------------------------------------------------------------------------
#  CLI for testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Phase 35: Earnings Season Engine")
    parser.add_argument("--stock", type=str, help="Stock code (e.g. 2330)")
    parser.add_argument("--date", type=str, help="Reference date (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    if args.stock:
        stock = args.stock.zfill(4)
        print(f"=== Earnings Analysis for {stock} ===")

        data = fetch_earnings_data(stock, date_str=args.date)
        print(f"\n[1] Earnings Data:")
        if data:
            for k, v in data.items():
                if not k.startswith("_"):
                    print(f"  {k}: {v}")
        else:
            print("  No data available")

        yoy = compute_yoy_growth(stock, date_str=args.date)
        print(f"\n[2] YoY Growth:")
        if yoy:
            for k, v in yoy.items():
                print(f"  {k}: {v}")

        qoq = compute_qoq_growth(stock, date_str=args.date)
        print(f"\n[3] QoQ Growth:")
        if qoq:
            for k, v in qoq.items():
                print(f"  {k}: {v}")

        surprise = check_estimate_surprise(stock, date_str=args.date)
        print(f"\n[4] Estimate Surprise:")
        if surprise:
            for k, v in surprise.items():
                print(f"  {k}: {v}")

        signal = get_earnings_signal(stock, date_str=args.date)
        print(f"\n[5] Earnings Signal: {signal:+.4f}")

        eq_score, eq_status = check_earnings_quality(stock, date_str=args.date)
        print(f"\n[6] Earnings Quality: score={eq_score}, status={eq_status}")
    else:
        print("Use --stock to specify a stock code (e.g. --stock 2330)")
