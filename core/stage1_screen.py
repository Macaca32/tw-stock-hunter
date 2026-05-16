#!/usr/bin/env python3
"""
Stage 1: Quantitative Pre-Screen
Filters full TWSE universe by hard filters + composite scoring
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def load_data(date_str=None):
    """Load fetched data for a date"""
    data_dir = Path(__file__).parent.parent / "data"
    
    if date_str is None:
        # Find most recent fetch
        json_files = sorted(data_dir.glob("daily_*.json"))
        if not json_files:
            raise FileNotFoundError("No daily data found. Run fetch_data.py first.")
        date_str = json_files[-1].stem.replace("daily_", "")
    
    datasets = {}
    required = ["daily", "pe", "company", "revenue", "flow"]
    
    for name in required:
        filepath = data_dir / f"{name}_{date_str}.json"
        if filepath.exists():
            with open(filepath, 'r', encoding='utf-8') as f:
                datasets[name] = json.load(f)
        else:
            logger.warning("Missing %s_%s.json", name, date_str)
    
    # Optional datasets
    for name in ["margin", "announce", "pledge", "sanctions", "halts", "margin_susp", "dividends", "major_sh"]:
        filepath = data_dir / f"{name}_{date_str}.json"
        if filepath.exists():
            with open(filepath, 'r', encoding='utf-8') as f:
                datasets[name] = json.load(f)
    
    return datasets, date_str


def load_config():
    """Load weights and thresholds.
    
    Phase 6: Apply weight smoothing to avoid flash-trade triggers.
    Limits per-day weight changes to max_weight_change_pct (default 5%).
    """
    config_dir = Path(__file__).parent.parent / "config"
    data_dir = Path(__file__).parent.parent / "data"
    
    with open(config_dir / "weights.json", 'r') as f:
        weights = json.load(f)
    
    with open(config_dir / "thresholds.json", 'r') as f:
        thresholds = json.load(f)
    
    # Phase 6: Load previous day's weights for smoothing
    smoothing_config = weights.get("smoothing", {})
    max_change = smoothing_config.get("max_weight_change_pct", 0.05)
    
    prev_weights_file = data_dir / "weights_previous.json"
    if prev_weights_file.exists():
        try:
            with open(prev_weights_file, 'r') as f:
                prev = json.load(f)
            
            # Smooth stage1 weights
            if "stage1" in weights and "stage1" in prev:
                weights["stage1"] = _smooth_weights(
                    prev["stage1"], weights["stage1"], max_change
                )
            
            # Smooth regime weights
            if "regime_weights" in weights and "regime_weights" in prev:
                for regime in weights["regime_weights"]:
                    if regime in prev["regime_weights"] and weights["regime_weights"][regime] is not None:
                        weights["regime_weights"][regime] = _smooth_weights(
                            prev["regime_weights"][regime],
                            weights["regime_weights"][regime],
                            max_change
                        )
        except (json.JSONDecodeError, IOError):
            pass
    
    return weights, thresholds


def _smooth_weights(prev_weights, new_weights, max_change):
    """Smooth weight transition between previous and new weights.
    
    Phase 6: Limit per-day weight changes to avoid flash-trade triggers.
    Sudden weight shifts can cause large score changes that trigger
    false signals. Smooth transitions give the system time to adapt.
    
    Phase 20: Normalize FIRST to get target proportions, THEN clamp
    changes to max_allowed and redistribute excess proportionally.
    The old approach (clamp then normalize) broke intent because
    normalization after clamping would undo the clamping effect.
    
    Args:
        prev_weights: Previous day's weight dict
        new_weights: Target weight dict
        max_change: Maximum absolute change per weight (0.05 = 5pp)
    
    Returns:
        Smoothed weight dict (values sum to 1.0)
    """
    # Step 1: Normalize inputs to ensure they sum to 1.0
    def _normalize(w):
        total = sum(w.values())
        return {k: v / total for k, v in w.items()} if total > 0 else w

    prev_norm = _normalize(prev_weights)
    new_norm = _normalize(new_weights)

    # Step 2: Clamp each weight's change to max_allowed
    min_weight = 0.05  # Floor: no weight below 5%
    max_weight = 0.50  # Ceiling: no weight above 50%

    smoothed = {}
    clamped_keys = set()

    for key in new_norm:
        if key not in prev_norm:
            # New key — accept as-is, will be normalized later
            smoothed[key] = new_norm[key]
            continue

        prev_val = prev_norm[key]
        new_val = new_norm[key]
        change = new_val - prev_val

        if abs(change) <= max_change:
            smoothed[key] = new_val
        else:
            # Clamp change to max_allowed
            direction = 1 if change > 0 else -1
            smoothed[key] = prev_val + direction * max_change
            clamped_keys.add(key)

    # Step 3: Apply floor/ceiling constraints and track excess
    for key in smoothed:
        if smoothed[key] < min_weight:
            smoothed[key] = min_weight
            clamped_keys.add(key)
        elif smoothed[key] > max_weight:
            smoothed[key] = max_weight
            clamped_keys.add(key)

    # Step 4: Redistribute excess/deficit among unconstrained weights
    total = sum(smoothed.values())
    if total > 0 and abs(total - 1.0) > 1e-6:
        unconstrained = {k for k in smoothed if k not in clamped_keys}
        if unconstrained:
            diff = 1.0 - total
            per_key = diff / len(unconstrained)
            for key in unconstrained:
                smoothed[key] = max(min_weight, smoothed[key] + per_key)

    # Step 5: Final normalization to ensure sum == 1.0
    total = sum(smoothed.values())
    if total > 0:
        smoothed = {k: v / total for k, v in smoothed.items()}

    return smoothed


def parse_twse_date(date_str):
    """Parse TWSE date format YYYY/MM/DD or YYYYMMDD"""
    try:
        if '/' in date_str:
            return datetime.strptime(date_str, "%Y/%m/%d")
        else:
            return datetime.strptime(str(date_str), "%Y%m%d")
    except:
        return None


def safe_float(val, default=0.0):
    """Safely convert a value to float, handling empty strings and None"""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def get_field(stock, chinese_key, english_key, default=""):
    """Get field value trying both Chinese and English keys"""
    # Use 'or' chain instead of nested .get() — when the key exists but value is None,
    # .get() returns None (not the default). The 'or' chain falls through correctly.
    return stock.get(chinese_key) or stock.get(english_key) or default


def _index_by_stock_code(data_list, code_keys=None):
    """Build a dict mapping stock_code -> list of matching records.

    Phase 22: Pre-build O(1) lookup index to eliminate repeated linear scans.
    Same pattern as stage2_deep.py _index_by_stock_code().

    Args:
        data_list: List of dicts to index.
        code_keys: Tuple of (chinese_key, english_key) for stock code lookup.
                   Defaults to ("公司代號", "Code").
    """
    if code_keys is None:
        code_keys = ("公司代號", "Code")
    index = {}
    for record in data_list:
        if isinstance(record, dict):
            code = record.get(code_keys[0], record.get(code_keys[1], ""))
            if code:
                index.setdefault(code, []).append(record)
    return index


def _index_single(data_list, code_keys=None):
    """Build a dict mapping stock_code -> first matching record.

    Phase 22: For datasets where we only need the first match (PE, revenue, etc.),
    returns a flat dict instead of lists. Equivalent to _index_by_stock_code()
    but takes the first record per code for O(1) single-record lookups.
    """
    if code_keys is None:
        code_keys = ("公司代號", "Code")
    index = {}
    for record in data_list:
        if isinstance(record, dict):
            code = record.get(code_keys[0], record.get(code_keys[1], ""))
            if code and code not in index:
                index[code] = record
    return index


def check_hard_filters(stock, company_info, datasets, thresholds, price_history=None,
                        pledge_index=None, penalty_index=None, sanctions_index=None,
                        halts_index=None, margin_susp_index=None):
    """Check hard filters - return True if stock PASSES all filters
    
    FIX v2: Stricter filters per z.ai review:
    - ADV (Average Daily Volume) liquidity filter
    - Stricter market cap minimum (5B TWD for TWSE, 2B for TPEx)
    - Pledge ratio check (major shareholders pledged >30% = disqualify)
    - Recent penalty check
    - Revenue positivity (YoY revenue growth > -20%)

    Phase 22: Accept pre-built O(1) index dicts instead of scanning full datasets.
    Falls back to linear scan if indexes not provided (backward compatible).
    """
    stage1_thresh = thresholds["stage1"]
    
    # Get stock code (daily data uses English keys)
    stock_code = get_field(stock, "證券代號", "Code", "")
    
    # Get close price
    close_raw = get_field(stock, "收盤價", "ClosingPrice", "")
    close = safe_float(close_raw, 0)
    if close == 0:
        return False, "no_closing_price"
    
    # === LIQUIDITY FILTER (ADV in TWD) ===
    # FIX v3: Use ADV in TWD value, NOT share count.
    # A NT$500 stock trading 3M shares = NT$1.5B daily value.
    # A NT$15 stock trading 6M shares = NT$90M daily value.
    # Minimum NT$50M daily value (~US$1.5M) for institutional-grade screening.
    adv_value_threshold = stage1_thresh.get("min_adv_twd", 50000000)  # NT$50M/day default (matches thresholds.json)
    if price_history and stock_code in price_history:
        history = price_history[stock_code]
        if len(history) >= 20:
            # Calculate average daily VALUE (price × volume), not just volume
            daily_values = []
            for h in history[-20:]:
                price = h.get("adj_close") or h.get("close") or 0
                vol = h.get("adj_volume") or h.get("volume") or 0
                if price > 0 and vol > 0:
                    daily_values.append(price * vol)
            if daily_values:
                avg_daily_value = sum(daily_values) / len(daily_values)
                if avg_daily_value < adv_value_threshold:
                    return False, "insufficient_liquidity_twd"
            else:
                return False, "insufficient_liquidity_no_data"
        elif len(history) > 0:
            # Fallback: use last 5 days if we don't have 20
            daily_values = []
            for h in history[-5:]:
                price = h.get("adj_close") or h.get("close") or 0
                vol = h.get("adj_volume") or h.get("volume") or 0
                if price > 0 and vol > 0:
                    daily_values.append(price * vol)
            if daily_values:
                avg_daily_value = sum(daily_values) / len(daily_values)
                if avg_daily_value < adv_value_threshold * 0.5:
                    return False, "insufficient_liquidity_twd_short"
            else:
                return False, "insufficient_liquidity_no_data"
    
    # Market cap filter (use company info if available)
    # FIX v3: TWSE vs TPEx differentiation - stricter for TPEx
    # Phase 17: Corrected market cap calculation.
    #   實收資本額 (paid-in capital) is total NT$ amount, NOT share count.
    #   Taiwan face value = NT$10/share, so shares_outstanding = paid_in / 10.
    #   market_cap = close_price × shares_outstanding = close × (paid_in / 10)
    if company_info:
        paid_in_raw = get_field(company_info, "實收資本額", "paid_in_capital", "")
        paid_in = safe_float(paid_in_raw, 0)

        # Phase 17: Corrected formula — was close * paid_in (treated paid_in as shares)
        # Now: close * (paid_in / 10) where 10 = Taiwan face value per share
        shares_outstanding = paid_in / 10.0 if paid_in > 0 else 0
        market_cap = close * shares_outstanding

        # Determine if TPEx or TWSE (TPEx codes >= 9900 or in 6xxx-7xxx range)
        is_tpex = False
        try:
            code_int = int(stock_code)
            if code_int >= 9900 or (6000 <= code_int <= 7999):
                is_tpex = True
        except:
            pass

        # FIX v3: Higher market cap floor for position sizing safety
        # NT$50B for TWSE, NT$30B for TPEx (stricter due to higher risk)
        min_cap = stage1_thresh.get("min_market_cap_tpex" if is_tpex else "min_market_cap", 50000000000)
    else:
        # Estimate market cap from price (rough heuristic)
        market_cap = close * 1000000000  # Assume 1B shares if no data
        min_cap = stage1_thresh.get("min_market_cap", 50000000000)
    
    if market_cap < min_cap:
        return False, "market_cap_too_small"
    
    # Price range
    if close < stage1_thresh["price_floor"] or close > stage1_thresh["price_ceiling"]:
        return False, "price_out_of_range"
    
    # Listing age (from company info)
    if company_info:
        list_date_str = get_field(company_info, "上市日期", "list_date", "")
        if list_date_str:
            list_date = parse_twse_date(str(list_date_str))
            if list_date:
                years_listed = (datetime.now() - list_date).days / 365
                if years_listed < stage1_thresh["min_listing_age_years"]:
                    return False, "too_new_listing"
    
    # === PLEDGE RISK CHECK ===
    # Major shareholders with >30% pledged shares = governance red flag
    # Phase 17: Pledged shares (累計質押股數) is in SHARES, not NT$.
    #   Must compare against shares outstanding (= paid_in / 10), not raw paid_in.
    # Phase 22: O(1) index lookup instead of O(n) linear scan.
    pledge_records = pledge_index.get(stock_code, []) if pledge_index is not None else []
    if not pledge_records:
        # Fallback: linear scan if index not provided
        pledge_data = datasets.get("pledge", [])
        pledge_records = [p for p in pledge_data if get_field(p, "公司代號", "Code", "") == stock_code]
    for p in pledge_records:
        pledged = safe_float(get_field(p, "累計質押股數", "total_pledged", ""), 0)
        # If we have company info, check against shares outstanding
        if company_info and pledged > 0:
            paid_in = safe_float(get_field(company_info, "實收資本額", "paid_in_capital", ""), 0)
            if paid_in > 0:
                shares_outstanding = paid_in / 10.0  # Taiwan face value = NT$10/share
                pledge_ratio = pledged / shares_outstanding
                if pledge_ratio > 0.30:
                    return False, "high_pledge_risk"

    # === PENALTY CHECK (BINARY - ANY TWSE/FSC penalty = DISQUALIFY) ===
    # FIX v3: No sliding scale. There's no "minor financial fraud."
    # Phase 22: O(1) index lookup instead of O(n) linear scan.
    penalty_records = penalty_index.get(stock_code, []) if penalty_index is not None else []
    if not penalty_records:
        penalty_data = datasets.get("penalties", [])
        penalty_records = [p for p in penalty_data if get_field(p, "公司代號", "Code", "") == stock_code]
    for p in penalty_records:
        date_str = get_field(p, "處分日期", "penalty_date", "")
        if date_str:
            try:
                penalty_date = datetime.strptime(str(date_str)[:8], "%Y%m%d")
                days_ago = (datetime.now() - penalty_date).days
                # FIX v3: 365-day lookback, ANY penalty = disqualify
                if days_ago < 365:
                    return False, "recent_penalty"
            except (ValueError, AttributeError):
                pass

    # Check sanctions — Phase 22: O(1) index lookup
    if sanctions_index is not None:
        if stock_code in sanctions_index:
            return False, "sanctioned"
    else:
        sanctions = datasets.get("sanctions", [])
        for s in sanctions:
            if get_field(s, "證券代號", "Code", "") == stock_code:
                return False, "sanctioned"

    # Check halts — Phase 22: O(1) index lookup
    if halts_index is not None:
        if stock_code in halts_index:
            return False, "trading_halt"
    else:
        halts = datasets.get("halts", [])
        for h in halts:
            if get_field(h, "證券代號", "Code", "") == stock_code:
                return False, "trading_halt"

    # Check margin suspension — Phase 22: O(1) index lookup
    if margin_susp_index is not None:
        if stock_code in margin_susp_index:
            return False, "margin_suspended"
    else:
        margin_susp = datasets.get("margin_susp", [])
        for m in margin_susp:
            if get_field(m, "證券代號", "Code", "") == stock_code:
                return False, "margin_suspended"

    return True, "pass"


def score_revenue_momentum(stock_code, revenue_data, weights, revenue_index=None):
    """Score revenue momentum (0-100)
    
    Revenue data uses Chinese keys:
      公司代號, 營業收入-當月營收, 營業收入-去年同月增減(%), 營業收入-上月比較增減(%)

    Phase 22: Accept pre-built revenue_index for O(1) lookup.
    """
    # Phase 22: O(1) index lookup instead of O(n) linear scan
    if revenue_index is not None:
        stock_rev = revenue_index.get(stock_code)
    else:
        stock_rev = None
        for r in revenue_data:
            if get_field(r, "公司代號", "Code", "") == stock_code:
                stock_rev = r
                break
    
    if not stock_rev:
        return 25  # Neutral if no data
    
    try:
        # Revenue data has keys like:
        # "營業收入-當月營收": "173091"
        # "營業收入-去年同月增減(%)": "425.85672621217645"
        # "營業收入-上月比較增減(%)": "27.879280410771674"
        rev_yoy = safe_float(get_field(stock_rev, "營業收入-去年同月增減(%)", "rev_yoy", ""), 0)
        rev_mom = safe_float(get_field(stock_rev, "營業收入-上月比較增減(%)", "rev_mom", ""), 0)
        
        # Score based on YoY growth (primary) + MoM (secondary)
        score = 50  # Base
        
        # YoY component (±30 points)
        if rev_yoy > 50:
            score += 30
        elif rev_yoy > 20:
            score += 20
        elif rev_yoy > 0:
            score += 10
        elif rev_yoy < -20:
            score -= 30
        else:
            score -= 10
        
        # MoM component (±20 points)
        if rev_mom > 20:
            score += 20
        elif rev_mom > 5:
            score += 10
        elif rev_mom < -20:
            score -= 20
        else:
            score -= 5
        
        return max(0, min(100, score))
    except:
        return 25


def score_profitability(stock_code, pe_data, pe_index=None):
    """Score profitability based on P/E and available metrics (0-100)

    Phase 22: Accept pre-built pe_index for O(1) lookup.
    """
    # Phase 22: O(1) index lookup instead of O(n) linear scan
    if pe_index is not None:
        stock_pe = pe_index.get(stock_code)
    else:
        stock_pe = None
        for p in pe_data:
            if get_field(p, "證券代號", "Code", "") == stock_code:
                stock_pe = p
                break
    
    if not stock_pe:
        return 25
    
    try:
        # PE data uses English keys: PEratio, DividendYield, PBratio
        pe_str = get_field(stock_pe, "本益比", "PEratio", "")
        pe = float(pe_str) if pe_str else 0
        
        # P/E as proxy for profitability (lower P/E = better value for profitable companies)
        if pe > 0:
            if pe < 12:
                return 100
            elif pe < 18:
                return 75
            elif pe < 25:
                return 50
            elif pe < 35:
                return 25
            else:
                return 0
        else:
            return 0  # Negative earnings or no data
    except:
        return 25


def score_valuation(stock_code, pe_data, pe_index=None):
    """Score valuation (0-100)

    Phase 22: Accept pre-built pe_index for O(1) lookup.
    """
    # Phase 22: O(1) index lookup instead of O(n) linear scan
    if pe_index is not None:
        stock_pe = pe_index.get(stock_code)
    else:
        stock_pe = None
        for p in pe_data:
            if get_field(p, "證券代號", "Code", "") == stock_code:
                stock_pe = p
                break
    
    if not stock_pe:
        return 25
    
    try:
        # PE data uses English keys
        pe_str = get_field(stock_pe, "本益比", "PEratio", "")
        pb_str = get_field(stock_pe, "股價淨值比", "PBratio", "")
        pe = float(pe_str) if pe_str else 0
        pb = float(pb_str) if pb_str else 0
        
        # Combined P/E + P/B scoring
        pe_score = 0
        pb_score = 0
        
        if pe > 0:
            if pe < 15:
                pe_score = 100
            elif pe < 20:
                pe_score = 75
            elif pe < 30:
                pe_score = 50
            elif pe < 40:
                pe_score = 25
            else:
                pe_score = 0
        
        if pb > 0:
            if pb < 1.5:
                pb_score = 100
            elif pb < 2.5:
                pb_score = 75
            elif pb < 4:
                pb_score = 50
            else:
                pb_score = 25
        
        # Average the two
        scores = [s for s in [pe_score, pb_score] if s > 0]
        return sum(scores) / len(scores) if scores else 25
    except:
        return 25


def score_institutional_flow(stock_code, flow_data, margin_data,
                           flow_index=None, margin_index=None):
    """Score institutional flow (0-100)
    
    Uses margin trading data as proxy since institutional flow API is broken.
    Margin data has Chinese keys: 股票代號, 融資買進, 融券賣出, etc.

    Phase 22: Accept pre-built flow_index and margin_index for O(1) lookup.
    """
    # Handle case where flow_data is not a list (HTML response or empty)
    if not isinstance(flow_data, list):
        flow_data = []
    
    # Try flow data first — Phase 22: O(1) index lookup
    stock_flow = None
    if flow_index is not None:
        records = flow_index.get(stock_code, [])
        stock_flow = records[0] if records else None
    else:
        for f in flow_data:
            if isinstance(f, dict) and get_field(f, "證券代號", "Code", "") == stock_code:
                stock_flow = f
                break
    
    if stock_flow:
        try:
            foreign_net = safe_float(get_field(stock_flow, "外陸資買賣超股數", "foreign_net", ""), 0)
            trust_net = safe_float(get_field(stock_flow, "投信買賣超股數", "trust_net", ""), 0)
            total_net = safe_float(get_field(stock_flow, "三大法人買賣超股數", "total_inst_net", ""), 0)
            
            if total_net > 0:
                if foreign_net > 0 and trust_net > 0:
                    return 100
                elif foreign_net > 0 or trust_net > 0:
                    return 60
                else:
                    return 40
            else:
                if foreign_net < 0 and trust_net < 0:
                    return 0
                else:
                    return 30
        except:
            pass
    
    # Fallback: use margin data as proxy — Phase 22: O(1) index lookup
    if isinstance(margin_data, list) or margin_index is not None:
        stock_margin = None
        if margin_index is not None:
            records = margin_index.get(stock_code, [])
            stock_margin = records[0] if records else None
        else:
            for m in margin_data:
                if isinstance(m, dict) and get_field(m, "股票代號", "Code", "") == stock_code:
                    stock_margin = m
                    break
        
        if stock_margin:
            try:
                # Margin buy = bullish retail sentiment
                margin_buy = safe_float(get_field(stock_margin, "融資買進", "margin_buy", ""), 0)
                margin_sell = safe_float(get_field(stock_margin, "融券賣出", "short_sell", ""), 0)
                margin_balance = safe_float(get_field(stock_margin, "融資今日餘額", "margin_balance", ""), 0)
                short_balance = safe_float(get_field(stock_margin, "融券今日餘額", "short_balance", ""), 0)
                
                # Net margin activity
                net_margin = margin_buy - margin_sell
                
                # Short selling pressure (high short = bearish)
                short_pressure = short_balance if short_balance > 0 else 0
                
                score = 50  # Base
                
                # Net margin activity
                if net_margin > 1000000:
                    score += 20
                elif net_margin > 100000:
                    score += 10
                elif net_margin < -1000000:
                    score -= 20
                elif net_margin < -100000:
                    score -= 10
                
                # Short pressure
                if short_pressure > 1000000:
                    score -= 15  # High short pressure = bearish
                elif short_pressure < 10000:
                    score += 5   # Low short pressure = bullish
                
                return max(0, min(100, score))
            except:
                pass
    
    return 25  # Neutral if no data


def score_technical_momentum(stock_code, daily_data, pe_data=None, price_history=None,
                            daily_index=None):
    """Score technical momentum using available data (0-100)
    
    With historical data: uses 20-day MA, RSI, volume trend
    Without: falls back to single-day proxy
    Phase 22: Accept pre-built daily_index for O(1) lookup.
    """
    # Try historical data first
    if price_history and stock_code in price_history:
        history = price_history[stock_code]
        
        if len(history) >= 20:
            return _score_momentum_with_history(history)
        elif len(history) >= 5:
            return _score_momentum_short_history(history)
    
    # Fallback: single-day proxy
    return _score_momentum_single_day(stock_code, daily_data, daily_index=daily_index)


def _score_momentum_with_history(history):
    """Score momentum with 20+ days of price history"""
    try:
        closes = [h.get("adj_close") or h.get("close") or 0 for h in history[-20:]]
        volumes = [h.get("adj_volume") or h.get("volume") or 0 for h in history[-20:]]
        
        if not closes or closes[0] == 0:
            return 25
        
        score = 50  # Base
        
        # MA alignment (±20 points)
        ma5 = sum(closes[-5:]) / 5
        ma10 = sum(closes[-10:]) / 10
        ma20 = sum(closes) / 20
        current = closes[-1]
        
        if current > ma5 > ma10 > ma20:
            score += 20  # Strong bullish alignment
        elif current > ma5 > ma10:
            score += 10  # Moderate bullish
        elif current < ma5 < ma10 < ma20:
            score -= 20  # Strong bearish
        elif current < ma5 < ma10:
            score -= 10  # Moderate bearish
        
        # RSI (±15 points) — Wilder's smoothing (standard 14-period)
        period = 14
        changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [c if c > 0 else 0 for c in changes]
        losses = [abs(c) if c < 0 else 0 for c in changes]

        if len(changes) < period:
            # Not enough data for Wilder's RSI; fall back to simple average
            avg_gain = sum(gains) / len(gains) if gains else 0
            avg_loss = sum(losses) / len(losses) if losses else 0.0001
        else:
            # First average: simple mean of first `period` values
            avg_gain = sum(gains[:period]) / period
            avg_loss = sum(losses[:period]) / period
            # Wilder's exponential smoothing for remaining values
            for i in range(period, len(changes)):
                avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        avg_loss = avg_loss if avg_loss != 0 else 0.0001
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        if rsi > 70:
            score -= 10  # Overbought
        elif rsi > 60:
            score += 5
        elif rsi > 40:
            score += 5
        elif rsi > 30:
            score += 10  # Oversold bounce potential
        elif rsi < 30:
            score -= 10  # Very oversold
        
        # Volume trend (±10 points)
        recent_vol = sum(volumes[-5:]) / 5
        older_vol = sum(volumes[-10:-5]) / 5 if len(volumes) >= 10 else recent_vol
        
        if older_vol > 0:
            vol_ratio = recent_vol / older_vol
            if vol_ratio > 1.5:
                score += 10  # Increasing volume
            elif vol_ratio < 0.5:
                score -= 5  # Decreasing volume
        
        return max(0, min(100, score))
    except:
        return 25


def _score_momentum_short_history(history):
    """Score momentum with 5-19 days of price history"""
    try:
        closes = [h.get("adj_close") or h.get("close") or 0 for h in history]
        
        if not closes or closes[0] == 0:
            return 25
        
        score = 50  # Base
        current = closes[-1]
        
        # Simple trend
        if len(closes) >= 5:
            ma5 = sum(closes[-5:]) / 5
            if current > ma5:
                score += 10
            else:
                score -= 10
        
        # Overall return
        first = closes[0]
        ret = (current - first) / first if first > 0 else 0
        
        if ret > 0.05:
            score += 15
        elif ret > 0.02:
            score += 5
        elif ret < -0.05:
            score -= 15
        elif ret < -0.02:
            score -= 5
        
        return max(0, min(100, score))
    except:
        return 25


def _score_momentum_single_day(stock_code, daily_data, daily_index=None):
    """Score momentum with only single-day data (fallback)
    Phase 22: Accept pre-built daily_index for O(1) lookup.
    """
    # Phase 22: O(1) index lookup instead of O(n) linear scan
    if daily_index is not None:
        stock_daily = daily_index.get(stock_code)
    else:
        stock_daily = None
        for d in daily_data:
            if get_field(d, "證券代號", "Code", "") == stock_code:
                stock_daily = d
                break
    
    if not stock_daily:
        return 25  # Not found
    
    try:
        close = safe_float(get_field(stock_daily, "收盤價", "ClosingPrice", ""), 0)
        change = safe_float(get_field(stock_daily, "Change", "change", ""), 0)
        high = safe_float(get_field(stock_daily, "最高價", "HighestPrice", ""), 0)
        low = safe_float(get_field(stock_daily, "最低價", "LowestPrice", ""), 0)
        transactions = safe_float(get_field(stock_daily, "成交筆數", "Transaction", ""), 0)
        
        if close == 0:
            return 25
        
        score = 50  # Base
        
        # Price change component (±20 points)
        pct_change = (change / close * 100) if close > 0 else 0
        if pct_change > 3:
            score += 20
        elif pct_change > 1:
            score += 10
        elif pct_change > 0:
            score += 5
        elif pct_change < -3:
            score -= 20
        elif pct_change < -1:
            score -= 10
        
        # Intraday strength (±15 points)
        if high > low:
            day_range = high - low
            close_position = (close - low) / day_range
            if close_position > 0.75:
                score += 15
            elif close_position > 0.5:
                score += 5
            elif close_position < 0.25:
                score -= 15
            else:
                score -= 5
        
        # Volume activity (±10 points)
        if transactions > 50000:
            score += 10
        elif transactions > 10000:
            score += 5
        elif transactions < 500:
            score -= 10
        
        return max(0, min(100, score))
    except:
        return 25


def compute_gap_fill_probability(price_history, stock_code):
    """Phase 30: Analyze historical gap patterns and compute gap-fill probability.

    For each stock, analyzes adjusted close price history to identify gaps
    (today's open vs previous close) and compute:
      (a) Percentage of gaps filled within 5/10/20 trading days
      (b) Average fill time in days for gaps that did fill
      (c) Current gap size relative to historical median gap

    Uses adjusted prices (Phase 2) so ex-dividend gaps are handled correctly.
    In Taiwan, TWSE/TPEx have daily 10% price limits which affect gap behavior.

    Returns dict with:
        fill_pct_5d: float — % of historical gaps filled within 5 trading days
        fill_pct_10d: float — % filled within 10 trading days
        fill_pct_20d: float — % filled within 20 trading days
        avg_fill_days: float — Average days to fill (for gaps that filled)
        current_gap_pct: float — Current gap size as % of price (0 if no gap)
        gap_vs_median: float — Current gap / historical median gap ratio
        timing_adjustment: float — Score adjustment (-10 to +10)
            High fill probability → wait for confirmation (negative adjustment)
            Low fill probability → gap likely persists (positive for direction)
        total_gaps_analyzed: int — Number of historical gaps found
    """
    EMPTY_RESULT = {
        "fill_pct_5d": 0.0, "fill_pct_10d": 0.0, "fill_pct_20d": 0.0,
        "avg_fill_days": 0.0, "current_gap_pct": 0.0, "gap_vs_median": 0.0,
        "timing_adjustment": 0.0, "total_gaps_analyzed": 0,
    }

    if not price_history or stock_code not in price_history:
        return EMPTY_RESULT

    history = price_history[stock_code]
    if not history or len(history) < 10:
        return EMPTY_RESULT

    try:
        # Build adjusted close list with opens
        closes = []
        opens = []
        for h in history:
            c = h.get("adj_close") or h.get("close") or 0
            o = h.get("open") or c  # Fallback to close if no open
            if c > 0:
                closes.append(c)
                opens.append(o)

        if len(closes) < 10:
            return EMPTY_RESULT

        # Identify gaps: today's open vs previous close
        gap_sizes = []  # Absolute gap sizes as % of price
        gap_records = []  # (gap_pct, filled_in_N_days or None)

        for i in range(1, len(closes)):
            prev_close = closes[i - 1]
            curr_open = opens[i]
            if prev_close <= 0:
                continue

            gap_pct = (curr_open - prev_close) / prev_close * 100

            # Only count meaningful gaps (> 0.5% — skip noise)
            if abs(gap_pct) < 0.5:
                continue

            gap_sizes.append(abs(gap_pct))

            # Check if gap filled within 5/10/20 days
            # Gap up fills if price drops back to prev_close
            # Gap down fills if price rises back to prev_close
            filled_at = None
            for j in range(i, min(i + 20, len(closes))):
                if gap_pct > 0:  # Gap up — fills when close <= prev_close
                    if closes[j] <= prev_close:
                        filled_at = j - i + 1
                        break
                else:  # Gap down — fills when close >= prev_close
                    if closes[j] >= prev_close:
                        filled_at = j - i + 1
                        break

            gap_records.append({
                "gap_pct": gap_pct,
                "abs_gap_pct": abs(gap_pct),
                "filled_at": filled_at,  # None = not filled within 20 days
            })

        if not gap_records:
            return EMPTY_RESULT

        # Compute fill statistics
        total_gaps = len(gap_records)
        filled_5d = sum(1 for g in gap_records if g["filled_at"] is not None and g["filled_at"] <= 5)
        filled_10d = sum(1 for g in gap_records if g["filled_at"] is not None and g["filled_at"] <= 10)
        filled_20d = sum(1 for g in gap_records if g["filled_at"] is not None and g["filled_at"] <= 20)
        filled_ever = [g["filled_at"] for g in gap_records if g["filled_at"] is not None]

        fill_pct_5d = round(filled_5d / total_gaps * 100, 1) if total_gaps > 0 else 0
        fill_pct_10d = round(filled_10d / total_gaps * 100, 1) if total_gaps > 0 else 0
        fill_pct_20d = round(filled_20d / total_gaps * 100, 1) if total_gaps > 0 else 0
        avg_fill_days = round(sum(filled_ever) / len(filled_ever), 1) if filled_ever else 0

        # Current gap
        current_gap_pct = 0.0
        if len(closes) >= 2 and opens:
            prev_close = closes[-2] if len(closes) >= 2 else 0
            curr_open = opens[-1] if opens else 0
            if prev_close > 0:
                current_gap_pct = round((curr_open - prev_close) / prev_close * 100, 2)

        # Gap vs historical median
        gap_vs_median = 0.0
        if gap_sizes:
            sorted_gaps = sorted(gap_sizes)
            n = len(sorted_gaps)
            median_gap = sorted_gaps[n // 2] if n % 2 == 1 else (sorted_gaps[n // 2 - 1] + sorted_gaps[n // 2]) / 2
            if median_gap > 0 and abs(current_gap_pct) > 0:
                gap_vs_median = round(abs(current_gap_pct) / median_gap, 2)

        # Timing adjustment: high fill probability → wait for confirmation
        # Low fill probability → gap likely persists (directional conviction)
        timing_adjustment = 0.0
        if abs(current_gap_pct) > 0.5:
            # If gaps usually fill quickly, reduce confidence in gap direction
            if fill_pct_5d > 70:
                timing_adjustment = -8.0  # High fill rate → wait
            elif fill_pct_5d > 50:
                timing_adjustment = -4.0  # Moderate fill rate → slight caution
            elif fill_pct_10d > 60:
                timing_adjustment = -3.0  # Eventually fills → mild caution
            else:
                timing_adjustment = 3.0  # Gaps tend to persist → directional conviction

        return {
            "fill_pct_5d": fill_pct_5d,
            "fill_pct_10d": fill_pct_10d,
            "fill_pct_20d": fill_pct_20d,
            "avg_fill_days": avg_fill_days,
            "current_gap_pct": current_gap_pct,
            "gap_vs_median": gap_vs_median,
            "timing_adjustment": round(timing_adjustment, 1),
            "total_gaps_analyzed": total_gaps,
        }
    except Exception as e:
        logger.debug("[Stage1] compute_gap_fill_probability failed for %s: %r", stock_code, e)
        return EMPTY_RESULT


def detect_volume_anomalies(price_history, stock_code):
    """Phase 30: Detect volume anomalies by comparing current volume to rolling median.

    Compares the most recent trading day's volume against a 20-day rolling
    median and flags:
      (a) volume > 3x median = institutional interest signal
      (b) volume < 0.3x median = low conviction signal
      (c) relative volume trend over last 5 days (increasing/decreasing/stable)

    Uses adjusted volumes (Phase 2) for consistency.

    Returns dict with:
        relative_volume: float — Current volume / 20-day median
        anomaly_type: str — "institutional" / "low_conviction" / "normal"
        volume_trend: str — "increasing" / "decreasing" / "stable"
        trend_strength: float — Rate of volume change over last 5 days
        score_adjustment: float — Composite score adjustment (-5 to +5)
        median_volume: float — 20-day median volume
        current_volume: float — Most recent day's volume
    """
    EMPTY_RESULT = {
        "relative_volume": 1.0, "anomaly_type": "normal",
        "volume_trend": "stable", "trend_strength": 0.0,
        "score_adjustment": 0.0, "median_volume": 0, "current_volume": 0,
    }

    if not price_history or stock_code not in price_history:
        return EMPTY_RESULT

    history = price_history[stock_code]
    if not history or len(history) < 5:
        return EMPTY_RESULT

    try:
        # Extract adjusted volumes
        volumes = []
        for h in history:
            vol = h.get("adj_volume") or h.get("volume") or 0
            if vol > 0:
                volumes.append(vol)

        if len(volumes) < 5:
            return EMPTY_RESULT

        current_volume = volumes[-1]

        # Compute 20-day median (or whatever is available)
        window = min(20, len(volumes) - 1)  # Exclude current day
        if window < 5:
            return EMPTY_RESULT

        recent_volumes = volumes[-(window + 1):-1]  # Exclude current
        sorted_vols = sorted(recent_volumes)
        n = len(sorted_vols)
        median_volume = sorted_vols[n // 2] if n % 2 == 1 else (sorted_vols[n // 2 - 1] + sorted_vols[n // 2]) / 2

        if median_volume <= 0:
            return EMPTY_RESULT

        relative_volume = current_volume / median_volume

        # Determine anomaly type
        anomaly_type = "normal"
        if relative_volume > 3.0:
            anomaly_type = "institutional"  # 3x+ volume = institutional interest
        elif relative_volume < 0.3:
            anomaly_type = "low_conviction"  # < 0.3x volume = low conviction

        # Volume trend over last 5 days
        volume_trend = "stable"
        trend_strength = 0.0
        if len(volumes) >= 5:
            last5 = volumes[-5:]
            # Simple linear regression slope for trend
            x_vals = list(range(len(last5)))
            x_mean = sum(x_vals) / len(x_vals)
            y_mean = sum(last5) / len(last5)
            numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, last5))
            denominator = sum((x - x_mean) ** 2 for x in x_vals)

            if denominator > 0 and y_mean > 0:
                slope = numerator / denominator
                trend_strength = round(slope / y_mean * 100, 2)  # % change per day

                if trend_strength > 5.0:
                    volume_trend = "increasing"
                elif trend_strength < -5.0:
                    volume_trend = "decreasing"
                else:
                    volume_trend = "stable"

        # Score adjustment
        score_adjustment = 0.0

        # Institutional volume (>3x) = strong signal, boost score
        if anomaly_type == "institutional":
            score_adjustment = min(5.0, 2.0 + (relative_volume - 3.0) * 0.5)

        # Low conviction volume (<0.3x) = weak signal, reduce score
        elif anomaly_type == "low_conviction":
            score_adjustment = max(-5.0, -2.0 + (relative_volume - 0.3) * 5.0)

        # Volume trend bonus/penalty
        if volume_trend == "increasing" and anomaly_type != "low_conviction":
            score_adjustment += 1.5  # Rising volume supports the signal
        elif volume_trend == "decreasing" and anomaly_type != "institutional":
            score_adjustment -= 1.0  # Falling volume undermines the signal

        return {
            "relative_volume": round(relative_volume, 2),
            "anomaly_type": anomaly_type,
            "volume_trend": volume_trend,
            "trend_strength": trend_strength,
            "score_adjustment": round(max(-5.0, min(5.0, score_adjustment)), 1),
            "median_volume": round(median_volume, 0),
            "current_volume": round(current_volume, 0),
        }
    except Exception as e:
        logger.debug("[Stage1] detect_volume_anomalies failed for %s: %r", stock_code, e)
        return EMPTY_RESULT


def compute_signal_strength(scores, composite_score, weights_dict):
    """Phase 27: Compute continuous signal strength metric (0-100).

    Replaces binary pass/fail with nuanced signal conviction scoring.
    A stock scoring 95 should be treated differently from one scoring 55
    even if both "pass" the threshold.

    Signal strength reflects:
    - Composite magnitude (how far above/below threshold)
    - Score concentration (are all dimensions aligned, or one dominant?)
    - Breadth (how many dimensions contribute meaningfully?)

    Returns dict with:
        strength: float 0-100 (raw signal strength)
        conviction: str ("very_high" / "high" / "moderate" / "low" / "very_low")
        grade: str ("A+" / "A" / "B+" / "B" / "C" / "D")
        dominant_dimension: str (which dimension contributes most)
        alignment: float 0-1 (how aligned are all dimensions)
    """
    if not scores or composite_score <= 0:
        return {
            "strength": 0.0,
            "conviction": "very_low",
            "grade": "D",
            "dominant_dimension": "none",
            "alignment": 0.0,
        }

    # --- 1. Composite magnitude (0-100, directly from weighted score) ---
    magnitude = composite_score

    # --- 2. Score concentration / alignment ---
    # If all dimensions are close to each other, alignment is high.
    # If one dimension is very high and others very low, alignment is low.
    score_values = list(scores.values())
    if len(score_values) < 2:
        alignment = 0.5
    else:
        mean_score = sum(score_values) / len(score_values)
        variance = sum((s - mean_score) ** 2 for s in score_values) / len(score_values)
        # Max variance for 0-100 scores is 2500 (all 0 or 100).
        # Normalize to 0-1 where 1 = perfectly aligned.
        alignment = max(0.0, 1.0 - (variance / 2500.0))

    # --- 3. Breadth: how many dimensions contribute meaningfully (>40)? ---
    contributing = sum(1 for s in score_values if s >= 40)
    breadth_ratio = contributing / len(score_values) if score_values else 0

    # --- 4. Dominant dimension ---
    dominant_dimension = max(scores, key=scores.get) if scores else "none"

    # --- 5. Combined signal strength ---
    # Weighted blend: magnitude 60%, alignment 25%, breadth 15%
    strength = magnitude * 0.60 + alignment * 100 * 0.25 + breadth_ratio * 100 * 0.15
    strength = round(max(0.0, min(100.0, strength)), 1)

    # --- 6. Conviction level ---
    if strength >= 85:
        conviction = "very_high"
    elif strength >= 70:
        conviction = "high"
    elif strength >= 50:
        conviction = "moderate"
    elif strength >= 30:
        conviction = "low"
    else:
        conviction = "very_low"

    # --- 7. Grade ---
    if strength >= 90:
        grade = "A+"
    elif strength >= 80:
        grade = "A"
    elif strength >= 70:
        grade = "B+"
    elif strength >= 55:
        grade = "B"
    elif strength >= 40:
        grade = "C"
    else:
        grade = "D"

    return {
        "strength": strength,
        "conviction": conviction,
        "grade": grade,
        "dominant_dimension": dominant_dimension,
        "alignment": round(alignment, 3),
    }


def compute_signal_confidence(stock_code, scores, price_history=None,
                              company_info=None, pe_index=None,
                              flow_data=None, margin_data=None,
                              regime="unknown", margin_index=None,
                              daily_index=None, revenue_index=None):
    """Phase 27: Compute per-dimension confidence scores (0-100).

    Confidence reflects data quality and consistency for each signal component.
    High confidence = score is reliable; low confidence = score may be noisy.

    Rules per dimension:
    - Technical: Higher confidence when price history > 1 year and low missing data
    - Fundamental: Higher confidence when recent financials available (within last quarter)
    - Momentum: Lower confidence during high-volatility regimes (stress/crisis/black_swan)
    - Volume: Lower confidence if ADV is borderline (< NT$100M)
    - Revenue: Higher confidence when YoY data available and consistent

    Returns dict mapping each dimension name to a confidence dict:
        {dimension: {"confidence": float 0-100, "factors": [str list of influencing factors]}}
    """
    confidence = {}

    # --- Technical confidence ---
    tech_factors = []
    tech_conf = 50.0  # Base

    if price_history and stock_code in price_history:
        history = price_history[stock_code]
        n_days = len(history)

        # History length bonus
        if n_days >= 252:  # ~1 year of trading days
            tech_conf += 25.0
            tech_factors.append("1yr+_history")
        elif n_days >= 60:
            tech_conf += 15.0
            tech_factors.append("60d+_history")
        elif n_days >= 20:
            tech_conf += 5.0
            tech_factors.append("20d+_history")
        else:
            tech_conf -= 10.0
            tech_factors.append("short_history")

        # Missing data penalty
        if n_days >= 20:
            closes = [h.get("adj_close") or h.get("close") or 0 for h in history[-20:]]
            zeros = sum(1 for c in closes if c == 0)
            missing_ratio = zeros / 20.0
            if missing_ratio > 0.1:
                tech_conf -= missing_ratio * 30.0
                tech_factors.append(f"missing_data_{missing_ratio:.0%}")
            else:
                tech_conf += 10.0
                tech_factors.append("complete_data")
    else:
        # No price history at all — single-day fallback used
        tech_conf -= 20.0
        tech_factors.append("no_price_history")

    confidence["momentum"] = {
        "confidence": round(max(0.0, min(100.0, tech_conf)), 1),
        "factors": tech_factors,
    }

    # --- Fundamental confidence (profitability + valuation) ---
    fund_factors = []
    fund_conf = 50.0

    # PE data availability
    has_pe = False
    if pe_index is not None:
        pe_record = pe_index.get(stock_code)
        if pe_record:
            has_pe = True
            pe_str = pe_record.get("PEratio") or pe_record.get("本益比") or ""
            pb_str = pe_record.get("PBratio") or pe_record.get("股價淨值比") or ""
            div_str = pe_record.get("DividendYield") or pe_record.get("殖利率(%)") or ""

            if pe_str and pe_str != "":
                fund_conf += 10.0
            if pb_str and pb_str != "":
                fund_conf += 10.0
            if div_str and div_str != "":
                fund_conf += 5.0

            if all(s and s != "" for s in [pe_str, pb_str]):
                fund_factors.append("full_pe_data")
            else:
                fund_factors.append("partial_pe_data")
        else:
            fund_conf -= 20.0
            fund_factors.append("no_pe_data")
    else:
        fund_conf -= 20.0
        fund_factors.append("no_pe_index")

    # Company info freshness — if available, assume recent financials
    if company_info:
        fund_conf += 15.0
        fund_factors.append("company_info_available")
    else:
        fund_conf -= 10.0
        fund_factors.append("no_company_info")

    confidence["profitability"] = {
        "confidence": round(max(0.0, min(100.0, fund_conf)), 1),
        "factors": fund_factors,
    }
    confidence["valuation"] = {
        "confidence": round(max(0.0, min(100.0, fund_conf - 5.0)), 1),
        "factors": list(fund_factors),
    }

    # --- Momentum-specific regime confidence ---
    # Already built as "momentum" above, but adjust for regime
    mom_conf = confidence["momentum"]["confidence"]
    mom_factors = list(confidence["momentum"]["factors"])

    if regime in ("stress", "crisis", "black_swan"):
        mom_conf -= 20.0
        mom_factors.append(f"low_confidence_regime:{regime}")
    elif regime == "caution":
        mom_conf -= 5.0
        mom_factors.append("caution_regime")
    elif regime == "normal":
        mom_conf += 5.0
        mom_factors.append("normal_regime")

    confidence["momentum"] = {
        "confidence": round(max(0.0, min(100.0, mom_conf)), 1),
        "factors": mom_factors,
    }

    # --- Volume / Flow confidence ---
    flow_factors = []
    flow_conf = 50.0

    # Check if institutional flow data is available (primary source)
    has_inst_flow = False
    if flow_data is not None and isinstance(flow_data, list) and len(flow_data) > 0:
        has_inst_flow = True
        flow_conf += 20.0
        flow_factors.append("inst_flow_available")

    # Check if margin data is available (fallback source)
    if margin_index is not None:
        records = margin_index.get(stock_code, [])
        if records:
            flow_conf += 10.0
            flow_factors.append("margin_data_available")
        else:
            flow_conf -= 5.0
            flow_factors.append("no_margin_data")

    if not has_inst_flow and margin_index is None:
        flow_conf -= 20.0
        flow_factors.append("no_flow_or_margin_data")

    # ADV borderline check — lower confidence if ADV is near threshold
    if price_history and stock_code in price_history:
        history = price_history[stock_code]
        if len(history) >= 20:
            daily_values = []
            for h in history[-20:]:
                price = h.get("adj_close") or h.get("close") or 0
                vol = h.get("adj_volume") or h.get("volume") or 0
                if price > 0 and vol > 0:
                    daily_values.append(price * vol)
            if daily_values:
                adv = sum(daily_values) / len(daily_values)
                if adv < 100_000_000:  # NT$100M borderline
                    flow_conf -= 15.0
                    flow_factors.append("borderline_adv")
                else:
                    flow_conf += 10.0
                    flow_factors.append("healthy_adv")

    confidence["flow"] = {
        "confidence": round(max(0.0, min(100.0, flow_conf)), 1),
        "factors": flow_factors,
    }

    # --- Revenue confidence ---
    rev_factors = []
    rev_conf = 50.0

    if revenue_index is not None:
        rev_record = revenue_index.get(stock_code)
        if rev_record:
            rev_conf += 20.0
            # Check YoY data consistency
            yoy_str = rev_record.get("營業收入-去年同月增減(%)") or rev_record.get("rev_yoy") or ""
            mom_str = rev_record.get("營業收入-上月比較增減(%)") or rev_record.get("rev_mom") or ""
            if yoy_str and yoy_str != "":
                rev_conf += 10.0
                rev_factors.append("yoy_available")
            if mom_str and mom_str != "":
                rev_conf += 5.0
                rev_factors.append("mom_available")
            # Check for contradictory signals (YoY up but MoM down sharply, or vice versa)
            try:
                yoy_val = float(yoy_str) if yoy_str else 0
                mom_val = float(mom_str) if mom_str else 0
                if (yoy_val > 10 and mom_val < -10) or (yoy_val < -10 and mom_val > 10):
                    rev_conf -= 10.0
                    rev_factors.append("contradictory_yoy_mom")
                else:
                    rev_factors.append("consistent_yoy_mom")
            except (ValueError, TypeError):
                rev_factors.append("unparseable_revenue")
        else:
            rev_conf -= 25.0
            rev_factors.append("no_revenue_data")
    else:
        rev_conf -= 25.0
        rev_factors.append("no_revenue_index")

    confidence["revenue"] = {
        "confidence": round(max(0.0, min(100.0, rev_conf)), 1),
        "factors": rev_factors,
    }

    return confidence


def get_regime_adjusted_thresholds(regime, base_thresholds):
    """Phase 27: Adjust pass/fail thresholds dynamically based on market regime.

    Regime-aware thresholds ensure only the strongest signals survive
    in adverse market conditions:
    - NORMAL/CAUTION: Standard thresholds (no adjustment)
    - STRESS: Raise minimum score by 15 points
    - CRISIS: Require exceptional scores (>80)
    - BLACK_SWAN: Pause screening entirely (effectively impossible threshold)

    Args:
        regime: Current market regime string
        base_thresholds: Dict with 'pass_threshold' and 'watchlist_threshold'

    Returns:
        Adjusted thresholds dict with same keys plus 'regime_adjustment' metadata.
    """
    base_pass = base_thresholds.get("pass_threshold", 60)
    base_watch = base_thresholds.get("watchlist_threshold", 45)

    if regime in ("normal", "caution", "unknown"):
        # Standard thresholds — no adjustment
        return {
            "pass_threshold": base_pass,
            "watchlist_threshold": base_watch,
            "regime_adjustment": 0,
            "regime_note": f"Standard thresholds ({regime})",
        }
    elif regime == "stress":
        # Raise by 15 points — only strongest signals survive
        adjusted_pass = min(100, base_pass + 15)
        adjusted_watch = min(100, base_watch + 15)
        return {
            "pass_threshold": adjusted_pass,
            "watchlist_threshold": adjusted_watch,
            "regime_adjustment": 15,
            "regime_note": "Stress regime: thresholds raised +15pts",
        }
    elif regime == "crisis":
        # Require exceptional scores (>80)
        adjusted_pass = max(base_pass, 80)
        adjusted_watch = max(base_watch, 65)
        return {
            "pass_threshold": adjusted_pass,
            "watchlist_threshold": adjusted_watch,
            "regime_adjustment": adjusted_pass - base_pass,
            "regime_note": "Crisis regime: exceptional scores required (>80)",
        }
    elif regime == "black_swan":
        # Pause screening — effectively impossible threshold
        return {
            "pass_threshold": 100,  # Unreachable
            "watchlist_threshold": 95,
            "regime_adjustment": 100 - base_pass,
            "regime_note": "BLACK SWAN: screening paused (no new positions)",
        }
    else:
        # Unknown regime — use standard thresholds
        return {
            "pass_threshold": base_pass,
            "watchlist_threshold": base_watch,
            "regime_adjustment": 0,
            "regime_note": f"Unknown regime '{regime}': using standard thresholds",
        }


def detect_false_signals(scores, price_history=None, stock_code=None,
                         datasets=None, dividends_index=None):
    """Phase 27: Detect contradictory signals across dimensions.

    Flags candidates that show contradictory signals which may indicate
    false positives or require extra caution:
    - High technical score but weak fundamentals → "contrarian" flag
    - Strong momentum but declining volume → "fading_momentum" warning
    - Recent corporate action (ex-dividend within 30 days) → adjust expectations
    - Revenue momentum diverging from price momentum → "divergence" flag
    - Low confidence dimensions scoring high → "unreliable_signal" flag

    Args:
        scores: Dict of dimension scores (revenue, profitability, valuation, flow, momentum)
        price_history: Optional price history dict for volume analysis
        stock_code: Stock code for data lookups
        datasets: Optional datasets dict for corporate action checks
        dividends_index: Optional index for dividend data lookups

    Returns:
        Dict with "flags" (list of warning dicts) and "is_contradictory" (bool).
        Each flag has: type, severity, dimension, message.
    """
    flags = []

    technical_score = scores.get("momentum", 50)
    fundamental_avg = (scores.get("profitability", 50) + scores.get("valuation", 50)) / 2.0
    momentum_score = technical_score
    flow_score = scores.get("flow", 50)
    revenue_score = scores.get("revenue", 50)

    # --- 1. Contrarian flag: high technical but weak fundamentals ---
    # If momentum is strong but fundamental measures are poor, the signal may be
    # a speculative spike rather than a sustainable trend.
    if technical_score >= 70 and fundamental_avg < 35:
        flags.append({
            "type": "contrarian",
            "severity": "high",
            "dimensions": ["momentum", "profitability", "valuation"],
            "message": (f"High technical ({technical_score:.0f}) but weak fundamentals "
                       f"(avg {fundamental_avg:.0f}) — likely speculative, not sustainable"),
        })
    elif technical_score >= 60 and fundamental_avg < 40:
        flags.append({
            "type": "contrarian",
            "severity": "medium",
            "dimensions": ["momentum", "profitability", "valuation"],
            "message": (f"Technical ({technical_score:.0f}) outpacing fundamentals "
                       f"(avg {fundamental_avg:.0f}) — watch for reversal"),
        })

    # --- 2. Fading momentum: strong momentum but declining volume ---
    # Price going up but volume declining = distribution / weakening trend.
    if momentum_score >= 60 and price_history and stock_code and stock_code in price_history:
        history = price_history[stock_code]
        if len(history) >= 10:
            try:
                recent_volumes = [h.get("adj_volume") or h.get("volume") or 0 for h in history[-5:]]
                older_volumes = [h.get("adj_volume") or h.get("volume") or 0 for h in history[-10:-5]]
                recent_avg = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
                older_avg = sum(older_volumes) / len(older_volumes) if older_volumes else 1

                if older_avg > 0 and recent_avg > 0:
                    vol_ratio = recent_avg / older_avg
                    if vol_ratio < 0.6:  # Volume dropped > 40%
                        flags.append({
                            "type": "fading_momentum",
                            "severity": "high",
                            "dimensions": ["momentum", "flow"],
                            "message": (f"Strong momentum ({momentum_score:.0f}) but volume declining "
                                       f"(ratio={vol_ratio:.2f}) — distribution suspected"),
                        })
                    elif vol_ratio < 0.8:
                        flags.append({
                            "type": "fading_momentum",
                            "severity": "medium",
                            "dimensions": ["momentum", "flow"],
                            "message": (f"Momentum ({momentum_score:.0f}) with softening volume "
                                       f"(ratio={vol_ratio:.2f}) — trend may weaken"),
                        })
            except (ValueError, TypeError, ZeroDivisionError):
                pass

    # --- 3. Strong momentum but weak flow → institutional not confirming ---
    if momentum_score >= 65 and flow_score < 30:
        flags.append({
            "type": "unconfirmed_momentum",
            "severity": "medium",
            "dimensions": ["momentum", "flow"],
            "message": (f"Strong momentum ({momentum_score:.0f}) but weak institutional flow "
                       f"({flow_score:.0f}) — retail-driven, less reliable"),
        })

    # --- 4. Revenue-price divergence ---
    # Revenue declining but price momentum strong → fundamental disconnect.
    if revenue_score < 30 and momentum_score >= 60:
        flags.append({
            "type": "revenue_price_divergence",
            "severity": "high",
            "dimensions": ["revenue", "momentum"],
            "message": (f"Declining revenue ({revenue_score:.0f}) but strong price momentum "
                       f"({momentum_score:.0f}) — earnings may not support price"),
        })
    elif revenue_score >= 70 and momentum_score < 30:
        flags.append({
            "type": "revenue_price_divergence",
            "severity": "medium",
            "dimensions": ["revenue", "momentum"],
            "message": (f"Strong revenue ({revenue_score:.0f}) but weak price momentum "
                       f"({momentum_score:.0f}) — market not yet recognizing value"),
        })

    # --- 5. Recent corporate action (ex-dividend within 30 days) ---
    # Ex-dividend causes an artificial price gap that distorts momentum scores.
    if stock_code and datasets:
        # Check dividends data for recent ex-dividend dates
        div_data = datasets.get("dividends", [])
        if div_data:
            div_records = []
            if dividends_index is not None:
                div_records = dividends_index.get(stock_code, [])
            else:
                for d in div_data:
                    if isinstance(d, dict) and get_field(d, "公司代號", "Code", "") == stock_code:
                        div_records.append(d)

            for d in div_records[:5]:  # Check last 5 records
                try:
                    # Try to find date field
                    date_str = d.get("日期") or d.get("date") or d.get("ex_dividend_date") or ""
                    cash_div = safe_float(d.get("股東配發-盈餘分配之現金股利(元/股)") or
                                         d.get("cash_div") or "0", 0)
                    if date_str and cash_div > 0:
                        # Parse date
                        parsed_date = parse_twse_date(str(date_str))
                        if parsed_date:
                            days_since = (datetime.now() - parsed_date).days
                            if 0 <= days_since <= 30:
                                flags.append({
                                    "type": "recent_ex_dividend",
                                    "severity": "medium",
                                    "dimensions": ["momentum"],
                                    "message": (f"Ex-dividend {days_since}d ago (NT${cash_div:.1f}/share) — "
                                               f"price gap may distort momentum score"),
                                })
                                break  # Only flag once
                except (ValueError, TypeError):
                    pass

    # --- Determine overall contradictory status ---
    high_severity_count = sum(1 for f in flags if f.get("severity") == "high")
    is_contradictory = high_severity_count >= 1 or len(flags) >= 3

    return {
        "flags": flags,
        "is_contradictory": is_contradictory,
        "flag_count": len(flags),
        "high_severity_count": high_severity_count,
    }


def _validate_candidates(candidates, verbose=True):
    """Phase 9: Validate Stage 1 candidates via Pydantic schema.
    
    Catches missing fields (e.g., close=0), wrong types, and score out-of-range
    before downstream modules receive corrupted data. Soft filter — invalid
    records are excluded with a warning but don't block the pipeline.
    """
    if not candidates:
        return []
    
    try:
        from schemas import Stage1Candidate, ScoreBreakdown
    except ImportError as e:
        logger.warning("Pydantic schemas unavailable for validation: %s", e)
        return candidates  # Return raw data without validation
    
    valid = []
    invalid_count = 0
    
    for c in candidates:
        try:
            # Build ScoreBreakdown from score_breakdown dict
            sb = c.get("score_breakdown", {})
            breakdown = ScoreBreakdown(
                revenue=float(sb.get("revenue", 25)),
                profitability=float(sb.get("profitability", 25)),
                valuation=float(sb.get("valuation", 25)),
                flow=float(sb.get("flow", 25)),
                momentum=float(sb.get("momentum", 25)),
            )
            
            # Validate the full candidate
            validated = Stage1Candidate(
                code=str(c["code"]),
                name=c.get("name", ""),
                close=float(c["close"]),
                composite_score=float(c["composite_score"]),
                score_breakdown=breakdown,
                passed=bool(c.get("pass", c["composite_score"] >= 65)),
            )
            valid.append(validated.model_dump())
        except Exception as e:
            invalid_count += 1
            if invalid_count <= 3:  # Log first few, not all
                code = c.get("code", "??")
                logger.warning("Candidate validation failed for %s: %s", code, e)
    
    logger.info("Stage 1 output validated: %d/%d valid (%d excluded)", len(valid), len(candidates), invalid_count)
    
    return valid


def run_stage1(date_str=None, verbose=False):
    """Run Stage 1 screening"""
    datasets, date = load_data(date_str)
    weights, thresholds = load_config()
    
    daily_data = datasets.get("daily", [])
    pe_data = datasets.get("pe", [])
    revenue_data = datasets.get("revenue", [])
    flow_data = datasets.get("flow", [])
    company_data = datasets.get("company", [])
    margin_data = datasets.get("margin", [])
    
    # Load price history if available
    # Phase 25: Try SQLite batch query first for efficiency, fall back to JSON
    price_history = None
    data_dir = Path(__file__).parent.parent / "data"
    db_path = data_dir / "hunter.db"

    if db_path.exists():
        try:
            from datastore import get_daily_history_batch
            # Get all stock codes from daily data for batch lookup
            stock_codes = [get_field(s, "證券代號", "Code", "") for s in daily_data]
            stock_codes = [c for c in stock_codes if c]  # filter empty
            if stock_codes:
                raw_history = get_daily_history_batch(
                    stock_codes, limit=30, data_dir=str(data_dir)
                )
                # Convert to price_history format: {stock_id: [entries]}
                # SQLite returns {stock_id: [{date, open, high, low, close, volume, adj_close, adj_volume}]}
                # which matches the format expected by check_hard_filters and score_technical_momentum
                price_history = raw_history
                logger.info("Price history loaded from SQLite: %d stocks", len(price_history))
        except Exception as e:
            logger.debug("SQLite price history load failed, falling back to JSON: %s", e)
            price_history = None

    if price_history is None:
        history_file = data_dir / "price_history.json"
        if history_file.exists():
            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    price_history = json.load(f)
                logger.info("Price history loaded from JSON: %d stocks", len(price_history))
            except:
                pass
    
    # Load regime if available
    regime = "unknown"
    regime_file = Path(__file__).parent.parent / "data" / "regime.json"
    if regime_file.exists():
        try:
            with open(regime_file, 'r', encoding='utf-8') as f:
                regime_data = json.load(f)
                regime = regime_data.get("regime", "unknown")
            logger.info("Market regime: %s", regime)
        except:
            pass
    
    # Build company info lookup (company data uses Chinese keys)
    company_lookup = {}
    for c in company_data:
        code = get_field(c, "公司代號", "Code", "")
        company_lookup[code] = c

    # Phase 22: Pre-build O(1) index dicts for all lookup-heavy datasets.
    # Eliminates O(n) linear scans per stock in check_hard_filters() and
    # all scoring functions. Same pattern as stage2_deep.py.
    pe_index = _index_single(pe_data, code_keys=("證券代號", "Code"))
    revenue_index = _index_single(revenue_data, code_keys=("公司代號", "Code"))
    flow_index = _index_by_stock_code(flow_data, code_keys=("證券代號", "Code"))
    margin_index = _index_by_stock_code(margin_data, code_keys=("股票代號", "Code"))
    pledge_index = _index_by_stock_code(datasets.get("pledge", []), code_keys=("公司代號", "Code"))
    penalty_index = _index_by_stock_code(datasets.get("penalties", []), code_keys=("公司代號", "Code"))
    sanctions_index = _index_by_stock_code(datasets.get("sanctions", []), code_keys=("證券代號", "Code"))
    halts_index = _index_by_stock_code(datasets.get("halts", []), code_keys=("證券代號", "Code"))
    margin_susp_index = _index_by_stock_code(datasets.get("margin_susp", []), code_keys=("證券代號", "Code"))
    daily_index = _index_single(daily_data, code_keys=("證券代號", "Code"))
    # Phase 27: dividends index for false signal detection (ex-dividend checks)
    dividends_index = _index_by_stock_code(datasets.get("dividends", []), code_keys=("公司代號", "Code"))
    logger.info(
        "Phase 22: Built index dicts (pe=%d, revenue=%d, flow=%d, margin=%d)",
        len(pe_index), len(revenue_index), len(flow_index), len(margin_index),
    )

    candidates = []
    watchlist = []
    rejected = []
    
    logger.info("Stage 1: Screening %d stocks", len(daily_data))
    logger.info("Pass threshold: %s", thresholds['stage1']['pass_threshold'])
    logger.info("Watchlist threshold: %s", thresholds['stage1']['watchlist_threshold'])

    # Phase 27: Adjust thresholds based on current market regime
    adjusted_thresholds = get_regime_adjusted_thresholds(regime, thresholds['stage1'])
    effective_pass = adjusted_thresholds['pass_threshold']
    effective_watch = adjusted_thresholds['watchlist_threshold']
    logger.info("Regime-adjusted thresholds: pass=%d, watch=%d (%s)",
                effective_pass, effective_watch, adjusted_thresholds['regime_note'])

    for stock in daily_data:
        # Daily data uses English keys
        code = get_field(stock, "證券代號", "Code", "")
        name = get_field(stock, "證券名稱", "Name", "")
        
        # Get company info for this stock
        company_info = company_lookup.get(code)
        
        # Hard filters (pass company_info for market cap calc, price_history for ADV)
        # Phase 22: Pass pre-built O(1) index dicts
        passes, reason = check_hard_filters(
            stock, company_info, datasets, thresholds, price_history=price_history,
            pledge_index=pledge_index, penalty_index=penalty_index,
            sanctions_index=sanctions_index, halts_index=halts_index,
            margin_susp_index=margin_susp_index,
        )
        if not passes:
            rejected.append({"code": code, "name": name, "reason": reason})
            continue
        
        # Score each dimension — Phase 22: Pass O(1) index dicts
        scores = {
            "revenue": score_revenue_momentum(code, revenue_data, weights, revenue_index=revenue_index),
            "profitability": score_profitability(code, pe_data, pe_index=pe_index),
            "valuation": score_valuation(code, pe_data, pe_index=pe_index),
            "flow": score_institutional_flow(code, flow_data, margin_data,
                                             flow_index=flow_index, margin_index=margin_index),
            "momentum": score_technical_momentum(code, daily_data, price_history=price_history,
                                                daily_index=daily_index)
        }
        
        # Weighted composite
        stage1_weights = weights["stage1"]
        composite = (
            scores["revenue"] * stage1_weights["revenue_momentum"] +
            scores["profitability"] * stage1_weights["profitability"] +
            scores["valuation"] * stage1_weights["valuation"] +
            scores["flow"] * stage1_weights["institutional_flow"] +
            scores["momentum"] * stage1_weights["technical_momentum"]
        )

        # Phase 27: Compute signal strength (continuous conviction metric)
        signal_strength = compute_signal_strength(scores, round(composite, 1), stage1_weights)

        # Phase 27: Compute per-dimension confidence scores
        signal_confidence = compute_signal_confidence(
            code, scores, price_history=price_history,
            company_info=company_info, pe_index=pe_index,
            flow_data=flow_data, margin_data=margin_data,
            regime=regime, margin_index=margin_index,
            daily_index=daily_index, revenue_index=revenue_index,
        )

        # Phase 27: Detect false/contradictory signals
        false_signals = detect_false_signals(
            scores, price_history=price_history, stock_code=code,
            datasets=datasets, dividends_index=dividends_index,
        )

        # Phase 30: Market microstructure analysis
        gap_fill = compute_gap_fill_probability(price_history, code)
        vol_anomaly = detect_volume_anomalies(price_history, code)

        # Phase 30: Apply microstructure adjustments to composite score
        micro_adjustment = 0.0
        micro_adjustment += gap_fill.get("timing_adjustment", 0.0)
        micro_adjustment += vol_anomaly.get("score_adjustment", 0.0)
        # Cap total microstructure adjustment to ±10 points
        micro_adjustment = max(-10.0, min(10.0, micro_adjustment))
        adjusted_composite = composite + micro_adjustment

        result = {
            "code": code,
            "name": name,
            "close": float(get_field(stock, "收盤價", "ClosingPrice", 0)),
            "composite_score": round(adjusted_composite, 1),
            "composite_raw": round(composite, 1),  # Before microstructure adjustments
            "score_breakdown": scores,
            "signal_strength": signal_strength,
            "signal_confidence": signal_confidence,
            "false_signals": false_signals,
            "microstructure": {
                "gap_fill": gap_fill,
                "volume_anomaly": vol_anomaly,
                "micro_adjustment": round(micro_adjustment, 1),
            },
            "pass": adjusted_composite >= effective_pass
        }
        
        if adjusted_composite >= effective_pass:
            candidates.append(result)
        elif adjusted_composite >= effective_watch:
            watchlist.append(result)
        else:
            rejected.append(result)
    
    # Sort by composite score
    candidates.sort(key=lambda x: x["composite_score"], reverse=True)
    watchlist.sort(key=lambda x: x["composite_score"], reverse=True)
    
    # Phase 9: Validate output via Pydantic schema
    valid_candidates = _validate_candidates(candidates, verbose=verbose)
    
    output = {
        "stage": 1,
        "date": date,
        "timestamp": datetime.now().isoformat(),
        "regime": regime,
        "regime_adjusted_thresholds": adjusted_thresholds,
        "candidates": valid_candidates,
        "watchlist": watchlist,
        "rejected_count": len(rejected),
        "summary": {
            "total_screened": len(daily_data),
            "passed": len(candidates),
            "watchlist": len(watchlist),
            "rejected": len(rejected)
        }
    }
    
    logger.info("Results:")
    logger.info("Passed: %d", len(valid_candidates))
    logger.info("Watchlist: %d", len(watchlist))
    logger.info("Rejected: %d", len(rejected))
    if valid_candidates:
        logger.info("Top 5 candidates:")
        for c in valid_candidates[:5]:
            logger.info("  %s %s: %s", c['code'], c['name'], c['composite_score'])
    
    # Phase 25: Persist results to disk so downstream stages (stage2_deep) can
    # load them via load_stage1_results(). Previously save_stage1_results() was
    # only called in the CLI main(), meaning the pipeline path never wrote the
    # file — causing "No such file or directory: data/stage1_YYYY-MM-DD.json".
    save_stage1_results(output)
    
    return output


def save_stage1_results(results):
    """Save Stage 1 results and current weights for smoothing.
    
    Phase 6: Save current weights so next run can smooth transitions.
    """
    data_dir = Path(__file__).parent.parent / "data"
    date = results["date"]
    
    filepath = data_dir / f"stage1_{date}.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    # Phase 6: Save current weights for next-day smoothing
    try:
        weights, _ = load_config()
        weights_file = data_dir / "weights_previous.json"
        with open(weights_file, 'w', encoding='utf-8') as f:
            json.dump({
                "stage1": weights.get("stage1", {}),
                "regime_weights": weights.get("regime_weights", {}),
                "saved_at": datetime.now().isoformat(),
            }, f, ensure_ascii=False, indent=2)
    except Exception:
        pass  # Don't fail the pipeline if weight save fails
    
    return filepath


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run Stage 1 screening")
    parser.add_argument("--date", type=str, help="Date to screen (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    verbose = args.verbose
    
    results = run_stage1(date_str=args.date, verbose=verbose)
    save_stage1_results(results)
    
    if verbose:
        print(f"\n💾 Saved to data/stage1_{results['date']}.json")
    
    return results


if __name__ == "__main__":
    main()
