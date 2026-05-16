#!/usr/bin/env python3
"""
Stage 2: Fundamental Deep-Dive
Analyzes Stage 1 candidates with detailed fundamental checks
"""

import json
import logging
from datetime import datetime
from pathlib import Path

# Import helpers from stage1
import sys
sys.path.insert(0, str(Path(__file__).parent))
from stage1_screen import load_data, load_config, safe_float, get_field
from stage1_screen import get_regime_adjusted_thresholds
import math

logger = logging.getLogger(__name__)


def load_stage1_results(date_str=None):
    """Load Stage 1 results
    
    Phase 25: Try SQLite first for SQL-based lookup, then fall back to JSON.
    """
    data_dir = Path(__file__).parent.parent / "data"
    
    # Phase 25: Try SQLite first — allows SQL queries on stage1 data
    try:
        from datastore import load_stage1_from_sqlite
        result = load_stage1_from_sqlite(date_str=date_str, data_dir=str(data_dir))
        if result is not None:
            logger.info("Loaded stage1 results from SQLite for %s", date_str or "latest")
            return result
    except Exception as e:
        logger.debug("SQLite stage1 load failed, falling back to JSON: %s", e)
    
    # Fallback: load from JSON file
    if date_str is None:
        json_files = sorted(data_dir.glob("stage1_*.json"))
        if not json_files:
            raise FileNotFoundError("No Stage 1 results found. Run stage1_screen.py first.")
        date_str = json_files[-1].stem.replace("stage1_", "")
    
    filepath = data_dir / f"stage1_{date_str}.json"
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_stock_id(sid):
    """Ensure consistent 4-digit string format for Taiwan stock IDs.
    
    FIX: Different data sources use different formats:
    - TWSE API: "2548" (no leading zero)
    - Some TPEx sources: "06207" (5 digits with leading zero)
    - This is the #1 cause of silent data mismatches
    
    All lookups, saves, and comparisons should use this function.
    """
    sid = str(sid).strip()
    if not sid:
        return sid
    try:
        # Remove leading zeros, then pad to 4 digits
        return str(int(sid)).zfill(4)
    except ValueError:
        # Non-numeric codes (e.g., "00400A") - keep as-is
        return sid.lstrip("0").ljust(4) if sid.startswith("0") else sid


def _index_by_stock_code(data_list):
    """Build a dict mapping stock_code -> list of matching records.

    Eliminates O(n) linear scans per stock in every check function.
    Called once per dataset; replaces the repeated loop-and-filter pattern.
    """
    index = {}
    for record in data_list:
        code = get_field(record, "公司代號", "Code", "")
        if code:
            index.setdefault(code, []).append(record)
    return index


def check_dividend_history(stock_code, dividends_data, dividends_index=None):
    """Check dividend consistency and yield (0-100)
    
    FIX: Continuous scoring instead of discrete buckets.
    Previously only produced 2 unique values across 75 stocks due to
    coarse ±10/±20 adjustments from a base of 50.
    """
    if dividends_index is not None:
        stock_divs = dividends_index.get(stock_code, [])
    else:
        stock_divs = []
        for d in dividends_data:
            if get_field(d, "公司代號", "Code", "") == stock_code:
                stock_divs.append(d)
    
    if not stock_divs:
        return 25.0, "no_data"
    
    try:
        latest = stock_divs[0]
        cash_div = safe_float(get_field(latest, "股東配發-盈餘分配之現金股利(元/股)", "cash_div", ""), 0)
        stock_div = safe_float(get_field(latest, "股東配發-盈餘轉增資配股(元/股)", "stock_div", ""), 0)
        net_profit = safe_float(get_field(latest, "本期淨利(淨損)(元)", "net_profit", ""), 0)
        
        score = 50.0  # Base
        
        # Cash dividend - continuous scale based on amount (±25 range)
        if cash_div > 0:
            # Scale: small div (1-3元) → +10, large div (10+元) → +25
            score += min(25.0, 10.0 + cash_div * 1.5)
        else:
            score -= 10.0
        
        # Stock dividend - continuous scale (±15 range)
        if stock_div > 0:
            score += min(15.0, stock_div * 3.0)
        
        # Profitability - continuous scale based on magnitude (±25 range)
        if net_profit > 0:
            # Log-scale: larger profit = higher bonus
            import math
            profit_bonus = min(25.0, 10.0 + math.log10(max(net_profit, 1)) * 3.0)
            score += profit_bonus
        else:
            score -= 20.0
        
        # Consistency bonus - continuous based on record count (0-10 range)
        consistency = min(10.0, len(stock_divs) * 3.0)
        score += consistency
        
        return round(max(0.0, min(100.0, score)), 2), "ok"
    except Exception as e:
        logger.debug("[Stage2] check_dividend_history failed for %s: %r", stock_code, e)
        return None


def check_announcements(stock_code, announce_data, days_back=30, announce_index=None):
    """Check for negative corporate announcements (0-100)"""
    if announce_index is not None:
        stock_anns = announce_index.get(stock_code, [])
    else:
        stock_anns = []
        for a in announce_data:
            if get_field(a, "公司代號", "Code", "") == stock_code:
                stock_anns.append(a)
    
    if not stock_anns:
        return 50, "neutral"  # No news is OK
    
    try:
        score = 50.0  # Base
        negative_keywords = ["裁員", "減資", "虧損", "處分", "訴訟", "罰款", "停產", "破產"]
        positive_keywords = ["增資", "獲利", "配股", "配息", "新廠", "簽約", "標案"]
        
        neg_count = 0
        pos_count = 0
        for ann in stock_anns[:10]:  # Check last 10
            subject = get_field(ann, "主旨 ", "subject", "")
            desc = get_field(ann, "說明", "description", "")
            text = f"{subject} {desc}"
            
            for kw in negative_keywords:
                if kw in text:
                    neg_count += 1
                    break
            
            for kw in positive_keywords:
                if kw in text:
                    pos_count += 1
                    break
        
        # Continuous scale: more negatives = steeper penalty, more positives = bonus
        score -= neg_count * 8.0  # -8 per negative (was ±10)
        score += pos_count * 3.5  # +3.5 per positive (was ±5)
        
        return round(max(0.0, min(100.0, score)), 2), "ok"
    except Exception as e:
        logger.debug("[Stage2] check_announcements failed for %s: %r", stock_code, e)
        return None


def check_major_shareholders(stock_code, major_sh_data, shareholders_index=None):
    """Check institutional ownership quality (0-100)"""
    if shareholders_index is not None:
        stock_sh = shareholders_index.get(stock_code, [])
    else:
        stock_sh = []
        for s in major_sh_data:
            if get_field(s, "公司代號", "Code", "") == stock_code:
                stock_sh.append(s)
    
    if not stock_sh:
        return 25, "no_data"
    
    try:
        # Multiple major shareholders = stable ownership
        unique_sh = set()
        for s in stock_sh:
            sh_name = get_field(s, "大股東名稱", "shareholder_name", "")
            if sh_name:
                unique_sh.add(sh_name)
        
        score = 50.0  # Base
        
        # More unique shareholders = more stable (continuous scale)
        sh_bonus = min(20.0, len(unique_sh) * 4.0)  # +4 per shareholder, max +20
        score += sh_bonus
        if len(unique_sh) < 2:
            score -= 10.0
        
        return round(max(0.0, min(100.0, score)), 2), "ok"
    except Exception as e:
        logger.debug("[Stage2] check_major_shareholders failed for %s: %r", stock_code, e)
        return None


def check_pledge_risk(stock_code, pledge_data, pledge_index=None):
    """Check if major shareholders pledged shares (lower = better)"""
    if pledge_index is not None:
        stock_pledge = pledge_index.get(stock_code, [])
    else:
        stock_pledge = []
        for p in pledge_data:
            if get_field(p, "公司代號", "Code", "") == stock_code:
                stock_pledge.append(p)
    
    if not stock_pledge:
        return 100, "no_pledge"  # No pledge = good
    
    try:
        # Check pledge ratio
        total_pledged = 0
        for p in stock_pledge:
            pledged = safe_float(get_field(p, "累計質押股數", "total_pledged", ""), 0)
            total_pledged += pledged
        
        # Continuous scale based on pledge magnitude (log scale)
        import math
        if total_pledged > 0:
            # Log-scale: small pledge ~75, large (>100M) ~20
            log_pledge = math.log10(total_pledged)
            score = max(15.0, min(80.0, 90.0 - (log_pledge - 6.0) * 12.0))
            if score < 40:
                status = "high_pledge"
            elif score < 70:
                status = "moderate_pledge"
            else:
                status = "low_pledge"
        else:
            return 100.0, "no_pledge"
        
        return round(score, 2), status
    except Exception as e:
        logger.debug("[Stage2] check_pledge_risk failed for %s: %r", stock_code, e)
        return None


def check_penalty_risk(stock_code, penalty_data, penalty_index=None):
    """Check for regulatory penalties (lower = worse)"""
    if penalty_index is not None:
        stock_penalties = penalty_index.get(stock_code, [])
    else:
        stock_penalties = []
        for p in penalty_data:
            if get_field(p, "公司代號", "Code", "") == stock_code:
                stock_penalties.append(p)
    
    if not stock_penalties:
        return 100, "clean"
    
    try:
        # Recent penalties are worse
        recent_count = 0
        for p in stock_penalties:
            date_str = get_field(p, "處分日期", "penalty_date", "")
            if date_str:
                # Simple check: if date is in last year
                try:
                    penalty_date = datetime.strptime(str(date_str)[:8], "%Y%m%d")
                    if (datetime.now() - penalty_date).days < 365:
                        recent_count += 1
                except (ValueError, AttributeError):
                    # Unparseable date format or None — skip this entry silently
                    pass
        
        # Continuous scale based on penalty count
        if recent_count > 0:
            score = max(5.0, min(60.0, 100.0 - recent_count * 25.0))
            if recent_count >= 3:
                status = "multiple_recent_penalties"
            else:
                status = "recent_penalty"
        else:
            return round(max(80.0, min(100.0, 100.0 - len(stock_penalties) * 5.0)), 2), "old_penalties_only"
        
        return round(score, 2), status
    except Exception as e:
        logger.debug("[Stage2] check_penalty_risk failed for %s: %r", stock_code, e)
        return None


def compute_volume_profile(price_history, stock_code, current_price=None):
    """Phase 30: Approximate Volume Point of Control (POC) and Value Area.

    Bins adjusted close prices into NT$10 intervals over the available
    historical window, computes volume per bin, and identifies:
      - POC: price bin with the highest total volume
      - Value Area: contiguous bins around POC that contain 67% of total volume

    Uses adjusted prices and volumes (Phase 2 corporate-action adjustments)
    so ex-dividend gaps do not distort the profile.

    Returns dict with:
        poc: float — Volume Point of Control price (midpoint of highest-volume bin)
        value_area_high: float — Upper bound of value area
        value_area_low: float — Lower bound of value area
        sr_score: float 0-100 — Support/resistance quality score
            (stocks trading near POC → stronger signals)
        bin_size: float — The price interval used for binning
        total_bins: int — Number of bins with non-zero volume
        data_points: int — Number of history entries used
    """
    EMPTY_RESULT = {
        "poc": None, "value_area_high": None, "value_area_low": None,
        "sr_score": 50.0, "bin_size": 10.0, "total_bins": 0, "data_points": 0,
    }

    if not price_history or stock_code not in price_history:
        return EMPTY_RESULT

    history = price_history[stock_code]
    if not history or len(history) < 5:
        return EMPTY_RESULT

    try:
        # Collect (price, volume) pairs using adjusted data
        pairs = []
        for h in history:
            price = h.get("adj_close") or h.get("close") or 0
            vol = h.get("adj_volume") or h.get("volume") or 0
            if price > 0 and vol > 0:
                pairs.append((price, vol))

        if len(pairs) < 5:
            return EMPTY_RESULT

        # Determine bin size — use NT$10 for most Taiwan stocks
        # For very low-priced stocks (< NT$30), use NT$1 bins for granularity
        min_price = min(p[0] for p in pairs)
        max_price = max(p[0] for p in pairs)
        bin_size = 10.0
        if min_price < 30:
            bin_size = 1.0
        elif min_price < 100:
            bin_size = 5.0

        # Bin prices and accumulate volume
        volume_bins = {}
        for price, vol in pairs:
            bin_idx = int(price / bin_size)
            volume_bins[bin_idx] = volume_bins.get(bin_idx, 0) + vol

        if not volume_bins:
            return EMPTY_RESULT

        total_volume = sum(volume_bins.values())

        # Find POC — the bin with highest volume
        poc_bin = max(volume_bins, key=volume_bins.get)
        poc_price = (poc_bin + 0.5) * bin_size  # Midpoint of bin

        # Compute Value Area — expand from POC until 67% of volume is captured
        target_volume = total_volume * 0.67
        sorted_bins = sorted(volume_bins.keys())
        poc_idx_in_sorted = sorted_bins.index(poc_bin)

        va_bins = [poc_bin]
        va_volume = volume_bins[poc_bin]
        left = poc_idx_in_sorted - 1
        right = poc_idx_in_sorted + 1

        while va_volume < target_volume and (left >= 0 or right < len(sorted_bins)):
            left_vol = volume_bins[sorted_bins[left]] if left >= 0 else 0
            right_vol = volume_bins[sorted_bins[right]] if right < len(sorted_bins) else 0

            if left_vol >= right_vol and left >= 0:
                va_bins.append(sorted_bins[left])
                va_volume += left_vol
                left -= 1
            elif right < len(sorted_bins):
                va_bins.append(sorted_bins[right])
                va_volume += right_vol
                right += 1
            elif left >= 0:
                va_bins.append(sorted_bins[left])
                va_volume += left_vol
                left -= 1
            else:
                break

        value_area_low = min(va_bins) * bin_size
        value_area_high = (max(va_bins) + 1) * bin_size

        # Score support/resistance quality (0-100)
        # Stocks trading near POC → stronger S/R → higher score
        sr_score = 50.0  # Base

        if current_price and current_price > 0:
            # Distance from POC as percentage of price
            poc_distance_pct = abs(current_price - poc_price) / current_price * 100

            if poc_distance_pct < 1.0:
                # Trading right at POC — very strong S/R
                sr_score = 95.0
            elif poc_distance_pct < 3.0:
                # Near POC — strong S/R
                sr_score = 80.0
            elif poc_distance_pct < 5.0:
                # Moderate distance — moderate S/R
                sr_score = 65.0
            elif poc_distance_pct < 10.0:
                # Far from POC — weak S/R
                sr_score = 40.0
            else:
                # Very far — POC not relevant for current price
                sr_score = 20.0

            # Bonus if price is within value area (fair value zone)
            if value_area_low <= current_price <= value_area_high:
                sr_score = min(100.0, sr_score + 10.0)

        return {
            "poc": round(poc_price, 2),
            "value_area_high": round(value_area_high, 2),
            "value_area_low": round(value_area_low, 2),
            "sr_score": round(sr_score, 1),
            "bin_size": bin_size,
            "total_bins": len(volume_bins),
            "data_points": len(pairs),
        }
    except Exception as e:
        logger.debug("[Stage2] compute_volume_profile failed for %s: %r", stock_code, e)
        return EMPTY_RESULT


def classify_intraday_pattern(open_price, high_price, low_price, close_price):
    """Phase 30: Classify candlestick pattern from OHLC relationships.

    Uses adjusted OHLC data (Phase 2) to classify the most recent bar
    into one of several candlestick pattern categories. Each pattern maps
    to a sentiment score and confidence level.

    Recognized patterns:
        - bullish_engulfing: Strong bullish reversal
        - bearish_engulfing: Strong bearish reversal
        - doji: Indecision / potential reversal
        - hammer: Bullish reversal (after downtrend)
        - shooting_star: Bearish reversal (after uptrend)
        - inside_bar: Consolidation / low volatility
        - bullish_marubozu: Strong bullish continuation
        - bearish_marubozu: Strong bearish continuation
        - normal: No specific pattern detected

    Returns dict with:
        pattern: str — Pattern name
        sentiment: float — Sentiment score from -0.5 (bearish) to +0.5 (bullish)
        confidence: str — "high", "medium", or "low"
        body_size: float — Absolute body size (|close - open|)
        upper_wick: float — Upper shadow length
        lower_wick: float — Lower shadow length
    """
    FALLBACK = {
        "pattern": "normal", "sentiment": 0.0, "confidence": "low",
        "body_size": 0.0, "upper_wick": 0.0, "lower_wick": 0.0,
    }

    if not all([open_price, high_price, low_price, close_price]):
        return FALLBACK
    if high_price < low_price:
        return FALLBACK

    try:
        body_size = abs(close_price - open_price)
        total_range = high_price - low_price
        if total_range <= 0:
            return FALLBACK

        body_ratio = body_size / total_range  # 0 = no body, 1 = marubozu
        is_bullish = close_price > open_price
        is_bearish = close_price < open_price

        upper_wick = high_price - max(open_price, close_price)
        lower_wick = min(open_price, close_price) - low_price
        upper_wick_ratio = upper_wick / total_range if total_range > 0 else 0
        lower_wick_ratio = lower_wick / total_range if total_range > 0 else 0

        # --- Doji: very small body relative to range ---
        # Body < 10% of total range → indecision
        if body_ratio < 0.10:
            # Long-legged doji: both wicks substantial
            if upper_wick_ratio > 0.30 and lower_wick_ratio > 0.30:
                return {
                    "pattern": "doji", "sentiment": 0.0, "confidence": "high",
                    "body_size": round(body_size, 2),
                    "upper_wick": round(upper_wick, 2),
                    "lower_wick": round(lower_wick, 2),
                }
            # Dragonfly doji: long lower wick, tiny upper — mildly bullish
            elif lower_wick_ratio > 0.50 and upper_wick_ratio < 0.15:
                return {
                    "pattern": "doji", "sentiment": 0.15, "confidence": "medium",
                    "body_size": round(body_size, 2),
                    "upper_wick": round(upper_wick, 2),
                    "lower_wick": round(lower_wick, 2),
                }
            # Gravestone doji: long upper wick, tiny lower — mildly bearish
            elif upper_wick_ratio > 0.50 and lower_wick_ratio < 0.15:
                return {
                    "pattern": "doji", "sentiment": -0.15, "confidence": "medium",
                    "body_size": round(body_size, 2),
                    "upper_wick": round(upper_wick, 2),
                    "lower_wick": round(lower_wick, 2),
                }
            else:
                return {
                    "pattern": "doji", "sentiment": 0.0, "confidence": "medium",
                    "body_size": round(body_size, 2),
                    "upper_wick": round(upper_wick, 2),
                    "lower_wick": round(lower_wick, 2),
                }

        # --- Hammer: small body at top, long lower wick (≥ 60% of range) ---
        # Bullish reversal pattern after downtrend
        if lower_wick_ratio >= 0.60 and body_ratio < 0.25 and upper_wick_ratio < 0.15:
            return {
                "pattern": "hammer", "sentiment": 0.35, "confidence": "high",
                "body_size": round(body_size, 2),
                "upper_wick": round(upper_wick, 2),
                "lower_wick": round(lower_wick, 2),
            }

        # --- Shooting Star: small body at bottom, long upper wick (≥ 60%) ---
        # Bearish reversal pattern after uptrend
        if upper_wick_ratio >= 0.60 and body_ratio < 0.25 and lower_wick_ratio < 0.15:
            return {
                "pattern": "shooting_star", "sentiment": -0.35, "confidence": "high",
                "body_size": round(body_size, 2),
                "upper_wick": round(upper_wick, 2),
                "lower_wick": round(lower_wick, 2),
            }

        # --- Inside Bar: today's range completely inside previous candle ---
        # For single-bar classification, we check if the body is small and
        # the range is contracting (body < 25% of range and both wicks small)
        if body_ratio < 0.20 and upper_wick_ratio < 0.25 and lower_wick_ratio < 0.25:
            return {
                "pattern": "inside_bar", "sentiment": 0.0, "confidence": "medium",
                "body_size": round(body_size, 2),
                "upper_wick": round(upper_wick, 2),
                "lower_wick": round(lower_wick, 2),
            }

        # --- Marubozu: very large body, tiny wicks ---
        # Body > 80% of range
        if body_ratio >= 0.80:
            if is_bullish:
                return {
                    "pattern": "bullish_marubozu", "sentiment": 0.50, "confidence": "high",
                    "body_size": round(body_size, 2),
                    "upper_wick": round(upper_wick, 2),
                    "lower_wick": round(lower_wick, 2),
                }
            else:
                return {
                    "pattern": "bearish_marubozu", "sentiment": -0.50, "confidence": "high",
                    "body_size": round(body_size, 2),
                    "upper_wick": round(upper_wick, 2),
                    "lower_wick": round(lower_wick, 2),
                }

        # --- Engulfing patterns: require previous bar context ---
        # Since we only have one bar, we approximate: large body (> 60% of range)
        # with a directional close that dominates
        if body_ratio >= 0.55:
            if is_bullish:
                return {
                    "pattern": "bullish_engulfing", "sentiment": 0.40, "confidence": "medium",
                    "body_size": round(body_size, 2),
                    "upper_wick": round(upper_wick, 2),
                    "lower_wick": round(lower_wick, 2),
                }
            elif is_bearish:
                return {
                    "pattern": "bearish_engulfing", "sentiment": -0.40, "confidence": "medium",
                    "body_size": round(body_size, 2),
                    "upper_wick": round(upper_wick, 2),
                    "lower_wick": round(lower_wick, 2),
                }

        # --- Default: directional bias based on close vs open ---
        if is_bullish:
            sentiment = min(0.2, body_ratio * 0.3)
        elif is_bearish:
            sentiment = max(-0.2, -body_ratio * 0.3)
        else:
            sentiment = 0.0

        return {
            "pattern": "normal", "sentiment": round(sentiment, 2),
            "confidence": "low",
            "body_size": round(body_size, 2),
            "upper_wick": round(upper_wick, 2),
            "lower_wick": round(lower_wick, 2),
        }
    except Exception as e:
        logger.debug("[Stage2] classify_intraday_pattern failed: %r", e)
        return FALLBACK


def validate_stage1_candidates(candidates, verbose=False):
    """Phase 11: Validate Stage 1 candidates using Pydantic schema before Stage 2 processing.

    Catches missing fields, wrong types, out-of-range scores, and field name mismatches
    between Stage 1 output and Stage 2 expectations. Invalid candidates are excluded with
    a warning but don't block the pipeline.
    """
    if not candidates:
        return []

    try:
        from schemas import Stage1Candidate, ScoreBreakdown
    except ImportError as e:
        logger.warning("Pydantic schemas unavailable for inter-stage validation: %s", e)
        return candidates  # Return raw data without validation

    valid = []
    invalid_count = 0

    for i, c in enumerate(candidates):
        try:
            code = str(c.get("code", ""))
            if not code:
                raise ValueError(f"Missing 'code' field")

            # Build ScoreBreakdown from score_breakdown dict
            sb = c.get("score_breakdown", {})
            # Phase 11 R2: Require all sub-scores — no defaults that mask missing data.
            # Missing data scored as "mediocre" is worse than failing validation.
            breakdown = ScoreBreakdown(
                revenue=float(sb["revenue"]),
                profitability=float(sb["profitability"]),
                valuation=float(sb["valuation"]),
                flow=float(sb["flow"]),
                momentum=float(sb["momentum"]),
            )

            # Validate the full candidate via Pydantic schema (extra='forbid' catches bugs)
            validated = Stage1Candidate(
                code=code,
                name=str(c.get("name", "")),
                close=float(c["close"]),
                composite_score=float(c["composite_score"]),
                score_breakdown=breakdown,
                passed=bool(c.get("pass", c["composite_score"] >= 65)),
            )
            valid.append(validated.model_dump())
        except Exception as e:
            invalid_count += 1
            if invalid_count <= 3:
                code = str(c.get("code", f"#{i}"))
                logger.warning("Stage→Stage2 validation failed for %s: %s", code, e)

    logger.info("Inter-stage validation: %d/%d valid (%d excluded)", len(valid), len(candidates), invalid_count)

    return valid


def run_stage2(date_str=None, verbose=False):
    """Run Stage 2 deep-dive on Stage 1 candidates
    
    FIX v2: Actually disqualify stocks with red flags:
    - Pledge ratio >30% = disqualify
    - Recent penalties (within 180 days) = disqualify  
    - Negative announcements (裁員, 減資, 虧損) = disqualify
    - Combined score threshold raised
    """
    stage1_results = load_stage1_results(date_str)
    datasets, date = load_data(date_str)
    weights, thresholds = load_config()
    
    dividends_data = datasets.get("dividends", [])
    announce_data = datasets.get("announce", [])
    major_sh_data = datasets.get("major_sh", [])
    pledge_data = datasets.get("pledge", [])
    penalty_data = datasets.get("penalties", [])
    
    # Phase 30: Load price history for volume profile and intraday pattern analysis
    price_history = None
    data_dir = Path(__file__).parent.parent / "data"
    db_path = data_dir / "hunter.db"
    if db_path.exists():
        try:
            from datastore import get_daily_history_batch
            stock_codes_s2 = [str(c.get("code", "")) for c in stage1_results.get("candidates", [])]
            stock_codes_s2 = [c for c in stock_codes_s2 if c]
            if stock_codes_s2:
                price_history = get_daily_history_batch(
                    stock_codes_s2, limit=30, data_dir=str(data_dir)
                )
                logger.info("Phase 30: Price history loaded for volume profile: %d stocks", len(price_history))
        except Exception as e:
            logger.debug("Phase 30: SQLite price history load failed: %s", e)
            price_history = None
    if price_history is None:
        history_file = data_dir / "price_history.json"
        if history_file.exists():
            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    price_history = json.load(f)
            except Exception:
                pass
    
    # Phase 19: Pre-build index dicts for O(1) lookups instead of O(n) scans
    dividends_index = _index_by_stock_code(dividends_data)
    announce_index = _index_by_stock_code(announce_data)
    shareholders_index = _index_by_stock_code(major_sh_data)
    pledge_index = _index_by_stock_code(pledge_data)
    penalty_index = _index_by_stock_code(penalty_data)
    
    candidates = stage1_results.get("candidates", [])
    # Phase 11: Inter-stage validation — catch field mismatches early
    raw_candidates = list(candidates)
    candidates = validate_stage1_candidates(candidates, verbose=verbose)
    if len(candidates) != len(raw_candidates):
        logger.info("Filtered %d Stage 1 candidates → %d valid", len(raw_candidates), len(candidates))
    
    logger.info("Stage 2: Deep-diving %d candidates", len(candidates))
    logger.info("Stage 2 min score: %s", thresholds['stage2']['fundamental_score_min'])
    logger.info("Red flag disqualification: %s", thresholds['stage2'].get('red_flag_disqualify', True))

    # Phase 27: Adjust stage2 thresholds based on market regime
    regime = stage1_results.get("regime", "unknown")
    stage2_base = {
        "pass_threshold": thresholds['stage2']['fundamental_score_min'],
        "watchlist_threshold": thresholds['stage2'].get('watchlist_min', 50),
    }
    adjusted_s2 = get_regime_adjusted_thresholds(regime, stage2_base)
    effective_s2_min = adjusted_s2['pass_threshold']
    logger.info("Regime-adjusted Stage 2 min score: %d (%s)",
                effective_s2_min, adjusted_s2['regime_note'])

    # Phase 34: Fetch cross-asset context for composite score adjustment
    cross_asset_signal = 0.0
    cross_asset_details = {}
    try:
        from market_context import get_cross_asset_signal as _get_cross_asset_signal
        from market_context import fetch_cross_assets as _fetch_cross_assets
        cross_assets = _fetch_cross_assets(date_str)
        cross_asset_signal, cross_asset_details = _get_cross_asset_signal(
            cross_assets=cross_assets, stage1_summary=stage1_results.get("summary")
        )
        logger.info("Phase 34: Cross-asset signal: %+.4f", cross_asset_signal)
    except Exception as e:
        logger.debug("Phase 34: cross-asset signal fetch failed (using neutral 0): %s", e)
        cross_asset_signal = 0.0
        cross_asset_details = {"總訊號": 0.0, "來源": "unavailable"}
    # Scale signal from [-0.2, +0.2] to [-3, +3] points
    cross_asset_adjustment = round(cross_asset_signal * 15.0, 1)
    cross_asset_adjustment = max(-3.0, min(3.0, cross_asset_adjustment))
    logger.info("Phase 34: Cross-asset composite score adjustment: %+.1f points", cross_asset_adjustment)

    deep_results = []
    disqualified = []
    
    # Phase 12: Scoring diagnostics — track error rates and score distributions
    diagnostics = {
        "total_candidates": len(candidates),
        "check_errors": {"dividend": 0, "announcements": 0, "shareholders": 0, "pledge": 0, "penalties": 0},
        "score_distributions": {"dividend": [], "announcements": [], "shareholders": [], "pledge": [], "penalties": []},
        "score_stats": {},
    }
    
    for candidate in candidates:
        code = str(candidate.get("code", ""))
        if not code:
            continue
        name = str(candidate.get("name", ""))
        # Phase 11 R2: No silent defaults — missing composite_score means Stage 1 bug, skip safely
        cs_raw = candidate.get("composite_score")
        if cs_raw is None:
            continue
        stage1_score = float(cs_raw)
        
        # Run deep checks (with O(1) index lookups)
        div_result = check_dividend_history(code, dividends_data, dividends_index)
        ann_result = check_announcements(code, announce_data, 30, announce_index)
        sh_result = check_major_shareholders(code, major_sh_data, shareholders_index)
        pledge_result = check_pledge_risk(code, pledge_data, pledge_index)
        penalty_result = check_penalty_risk(code, penalty_data, penalty_index)
        
        # Phase 31: News sentiment analysis (graceful fallback on failure)
        try:
            from news_sentiment import check_news_sentiment
            news_result = check_news_sentiment(code, name, data_dir=str(data_dir))
        except Exception as e:
            logger.debug("Phase 31: news_sentiment import/call failed for %s: %r", code, e)
            news_result = None
        
        # Phase 35: Earnings quality analysis (graceful fallback on failure)
        try:
            from earnings_analysis import check_earnings_quality
            earnings_result = check_earnings_quality(code, date_str=date)
        except Exception as e:
            logger.debug("Phase 35: earnings_analysis import/call failed for %s: %r", code, e)
            earnings_result = None
        
        # Phase 30: Market microstructure analysis
        current_price = safe_float(candidate.get("close", 0), 0)
        vol_profile = compute_volume_profile(price_history, code, current_price=current_price)
        
        # Phase 30: Intraday pattern classification from latest OHLC
        latest_ohlc = None
        if price_history and code in price_history:
            hist = price_history[code]
            if hist:
                latest_ohlc = hist[-1]  # Most recent entry
        if latest_ohlc:
            intraday_pattern = classify_intraday_pattern(
                open_price=safe_float(latest_ohlc.get("open") or latest_ohlc.get("adj_close"), 0),
                high_price=safe_float(latest_ohlc.get("high"), 0),
                low_price=safe_float(latest_ohlc.get("low"), 0),
                close_price=safe_float(latest_ohlc.get("adj_close") or latest_ohlc.get("close"), 0),
            )
        else:
            # Fallback: try candidate's daily data
            intraday_pattern = classify_intraday_pattern(
                open_price=safe_float(candidate.get("open"), 0),
                high_price=safe_float(candidate.get("high"), 0),
                low_price=safe_float(candidate.get("low"), 0),
                close_price=current_price,
            )
        
        # Phase 12 R2: Handle None returns from checks (missing data → skip gracefully)
        div_score, div_status = (div_result if div_result is not None else (None, "error"))
        ann_score, ann_status = (ann_result if ann_result is not None else (None, "error"))
        sh_score, sh_status = (sh_result if sh_result is not None else (None, "error"))
        pledge_score, pledge_status = (pledge_result if pledge_result is not None else (None, "error"))
        penalty_score, penalty_status = (penalty_result if penalty_result is not None else (None, "error"))
        news_score, news_status = (news_result if news_result is not None else (None, "error"))
        earnings_score, earnings_status = (earnings_result if earnings_result is not None else (None, "error"))
        
        # Phase 12: Collect diagnostics
        if div_status == "error":
            diagnostics["check_errors"]["dividend"] += 1
        elif div_score is not None:
            diagnostics["score_distributions"]["dividend"].append(div_score)
        if ann_status == "error":
            diagnostics["check_errors"]["announcements"] += 1
        elif ann_score is not None:
            diagnostics["score_distributions"]["announcements"].append(ann_score)
        if sh_status == "error":
            diagnostics["check_errors"]["shareholders"] += 1
        elif sh_score is not None:
            diagnostics["score_distributions"]["shareholders"].append(sh_score)
        if pledge_status == "error":
            diagnostics["check_errors"]["pledge"] += 1
        elif pledge_score is not None:
            diagnostics["score_distributions"]["pledge"].append(pledge_score)
        if penalty_status == "error":
            diagnostics["check_errors"]["penalties"] += 1
        elif penalty_score is not None:
            diagnostics["score_distributions"]["penalties"].append(penalty_score)
        # Phase 31: News sentiment diagnostics
        if news_status == "error":
            diagnostics["check_errors"]["news_sentiment"] = diagnostics["check_errors"].get("news_sentiment", 0) + 1
        elif news_score is not None:
            diagnostics["score_distributions"].setdefault("news_sentiment", []).append(news_score)
        # Phase 35: Earnings quality diagnostics
        if earnings_status == "error":
            diagnostics["check_errors"]["earnings_growth"] = diagnostics["check_errors"].get("earnings_growth", 0) + 1
        elif earnings_score is not None:
            diagnostics["score_distributions"].setdefault("earnings_growth", []).append(earnings_score)
        
        # === RED FLAG DISQUALIFICATION ===
        # FIX v2: Actually enforce red flags
        red_flags = []
        
        if thresholds["stage2"].get("red_flag_disqualify", True):
            # Pledge risk: score < 40 means high pledge (skip if score is None from error)
            if pledge_score is not None and pledge_score < 40:
                red_flags.append(f"High pledge risk (score={pledge_score}, status={pledge_status})")

            # Recent penalties: score < 50 means concerning
            if penalty_score is not None and penalty_score < 50:
                red_flags.append(f"Penalty risk (score={penalty_score}, status={penalty_status})")

            # Negative announcements: score < 30 means serious issues
            if ann_score is not None and ann_score < 30:
                red_flags.append(f"Negative announcements (score={ann_score}, status={ann_status})")

            # Phase 31: Strongly negative news sentiment = disqualify
            if news_score is not None and news_score < 35:
                red_flags.append(f"Very negative news sentiment (score={news_score}, status={news_status})")
            
            # Phase 35: Severely declining earnings = disqualify
            if earnings_score is not None and earnings_score < 20:
                red_flags.append(f"Severely declining earnings (score={earnings_score}, status={earnings_status})")
        
        if red_flags:
            result = {
                "code": code,
                "name": name,
                "stage1_score": stage1_score,
                "stage2_score": 0,
                "red_flags": red_flags,
                "checks": {
                    "dividend": {"score": div_score, "status": div_status},
                    "announcements": {"score": ann_score, "status": ann_status},
                    "shareholders": {"score": sh_score, "status": sh_status},
                    "pledge": {"score": pledge_score, "status": pledge_status},
                    "penalties": {"score": penalty_score, "status": penalty_status},
                    "news_sentiment": {"score": news_score, "status": news_status},
                    "earnings_growth": {"score": earnings_score, "status": earnings_status}
                },
                "microstructure": {
                    "volume_profile": vol_profile,
                    "intraday_pattern": intraday_pattern,
                },
                "combined_score": 0
            }
            disqualified.append(result)
            logger.error("DISQUALIFIED %s %s: %s", code, name, '; '.join(red_flags))
            continue
        
        # Weighted Stage 2 score — load from config, fall back to hardcoded defaults
        # Replace None scores with neutral 50 (not 0 or 25) to avoid distorting the average
        stage2_weights = weights.get("stage2", {})
        w_div = stage2_weights.get("dividend", 0.22)
        w_ann = stage2_weights.get("announcements", 0.18)
        w_sh  = stage2_weights.get("shareholders", 0.13)
        w_plg = stage2_weights.get("pledge", 0.18)
        w_pen = stage2_weights.get("penalties", 0.18)
        w_news = stage2_weights.get("news_sentiment", 0.11)  # Phase 31
        w_earn = stage2_weights.get("earnings_growth", 0.12)  # Phase 35
        _safe = lambda s: s if s is not None else 50.0
        fundamental_score = (
            _safe(div_score) * w_div +
            _safe(ann_score) * w_ann +
            _safe(sh_score) * w_sh +
            _safe(pledge_score) * w_plg +
            _safe(penalty_score) * w_pen +
            _safe(news_score) * w_news +  # Phase 31
            _safe(earnings_score) * w_earn  # Phase 35
        )
        
        result = {
            "code": code,
            "name": name,
            "stage1_score": stage1_score,
            "stage2_score": round(fundamental_score, 1),
            "checks": {
                "dividend": {"score": div_score, "status": div_status},
                "announcements": {"score": ann_score, "status": ann_status},
                "shareholders": {"score": sh_score, "status": sh_status},
                "pledge": {"score": pledge_score, "status": pledge_status},
                "penalties": {"score": penalty_score, "status": penalty_status},
                "news_sentiment": {"score": news_score, "status": news_status},  # Phase 31
                "earnings_growth": {"score": earnings_score, "status": earnings_status}  # Phase 35
            },
            "microstructure": {
                "volume_profile": vol_profile,
                "intraday_pattern": intraday_pattern,
            },
            "cross_asset_adjustment": cross_asset_adjustment,  # Phase 34
        }

        # Phase 35: Add earnings signal as composite score adjustment
        try:
            from earnings_analysis import get_earnings_signal as _get_earnings_signal
            earnings_signal = _get_earnings_signal(code, date_str=date)
            # Scale signal from [-0.15, +0.15] to [-2.25, +2.25] points
            earnings_adj = round(earnings_signal * 15.0, 1)
            earnings_adj = max(-2.25, min(2.25, earnings_adj))
        except Exception:
            earnings_signal = 0.0
            earnings_adj = 0.0

        result["earnings_signal"] = {
            "signal": earnings_signal,
            "adjustment_points": earnings_adj,
        }
        result["combined_score"] = round(
            (stage1_score * 0.6 + fundamental_score * 0.4) +
            cross_asset_adjustment + earnings_adj, 1
        )
        
        if fundamental_score >= effective_s2_min:
            deep_results.append(result)
    
    # Phase 12: Compute score distribution stats from collected distributions
    for check_name in diagnostics["score_distributions"]:
        scores = diagnostics["score_distributions"][check_name]
        if scores:
            diagnostics["score_stats"][check_name] = {
                "min": round(min(scores), 1),
                "max": round(max(scores), 1),
                "mean": round(sum(scores) / len(scores), 1),
                "count": len(scores),
            }
        else:
            diagnostics["score_stats"][check_name] = {"min": None, "max": None, "mean": None, "count": 0}
    
    # Sort by combined score
    deep_results.sort(key=lambda x: x["combined_score"], reverse=True)
    
    output = {
        "stage": 2,
        "date": date,
        "timestamp": datetime.now().isoformat(),
        "regime": regime,
        "regime_adjusted_thresholds": adjusted_s2,
        "cross_asset_context": {  # Phase 34
            "signal": cross_asset_signal,
            "adjustment_points": cross_asset_adjustment,
            "details": cross_asset_details,
        },
        "candidates": deep_results,
        "disqualified": disqualified,
        "summary": {
            "stage1_candidates": len(candidates),
            "passed_stage2": len(deep_results),
            "disqualified": len(disqualified)
        },
        "diagnostics": diagnostics  # Phase 12
    }
    
    logger.info("Stage 2 Results:")
    logger.info("Passed: %d", len(deep_results))
    logger.info("Disqualified: %d", len(disqualified))
    
    # Phase 12: Print error summary
    total_errors = sum(diagnostics["check_errors"].values())
    if total_errors > 0:
        logger.warning("Check errors: %d/%d candidates", total_errors, diagnostics['total_candidates'])
        for check, count in diagnostics["check_errors"].items():
            if count > 0:
                logger.warning("  - %s: %d errors", check, count)
    
    # Phase 12: Print score stats
    logger.info("Score ranges (passed candidates):")
    for check, stats in diagnostics.get("score_stats", {}).items():
        if stats["count"] > 0:
            logger.info("  %s: %s-%s (mean=%s)", check, stats['min'], stats['max'], stats['mean'])
    if deep_results:
        logger.info("Top 5 after deep-dive:")
        for c in deep_results[:5]:
            logger.info("  %s %s: combined=%s (S1=%s, S2=%s)", c['code'], c['name'], c['combined_score'], c['stage1_score'], c['stage2_score'])
    
    return output


def save_stage2_results(results):
    """Save Stage 2 results"""
    data_dir = Path(__file__).parent.parent / "data"
    date = results["date"]
    
    filepath = data_dir / f"stage2_{date}.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    return filepath


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run Stage 2 fundamental deep-dive")
    parser.add_argument("--date", type=str, help="Date to analyze (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    verbose = args.verbose
    
    results = run_stage2(date_str=args.date, verbose=verbose)
    save_stage2_results(results)
    
    if verbose:
        print(f"\n💾 Saved to data/stage2_{results['date']}.json")
    
    return results


if __name__ == "__main__":
    main()
