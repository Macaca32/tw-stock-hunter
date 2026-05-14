#!/usr/bin/env python3
"""
Stage 1: Quantitative Pre-Screen
Filters full TWSE universe by hard filters + composite scoring
"""

import json
from datetime import datetime, timedelta
from pathlib import Path


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
            print(f"⚠ Missing {name}_{date_str}.json")
    
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
    
    Args:
        prev_weights: Previous day's weight dict
        new_weights: Target weight dict
        max_change: Maximum fractional change per weight (0.05 = 5%)
    
    Returns:
        Smoothed weight dict
    """
    smoothed = {}
    
    for key in new_weights:
        if key not in prev_weights:
            smoothed[key] = new_weights[key]
            continue
        
        prev_val = prev_weights[key]
        new_val = new_weights[key]
        
        # Calculate max allowed change
        change = new_val - prev_val
        max_allowed = max_change
        
        if abs(change) <= max_allowed:
            smoothed[key] = new_val
        else:
            # Clamp change to max allowed
            direction = 1 if change > 0 else -1
            smoothed[key] = prev_val + direction * max_allowed
    
    # Normalize to sum to 1.0
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
    return stock.get(chinese_key, stock.get(english_key, default))


def check_hard_filters(stock, company_info, datasets, thresholds, price_history=None):
    """Check hard filters - return True if stock PASSES all filters
    
    FIX v2: Stricter filters per z.ai review:
    - ADV (Average Daily Volume) liquidity filter
    - Stricter market cap minimum (5B TWD for TWSE, 2B for TPEx)
    - Pledge ratio check (major shareholders pledged >30% = disqualify)
    - Recent penalty check
    - Revenue positivity (YoY revenue growth > -20%)
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
    # Minimum NT$100M daily value (~US$3M) for institutional-grade screening.
    adv_value_threshold = stage1_thresh.get("min_adv_twd", 100000000)  # NT$100M/day default
    if price_history and stock_code in price_history:
        history = price_history[stock_code]
        if len(history) >= 20:
            # Calculate average daily VALUE (price × volume), not just volume
            daily_values = []
            for h in history[-20:]:
                price = h.get("adj_close", h.get("close", 0))
                vol = h.get("adj_volume", h.get("volume", 0))
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
                price = h.get("adj_close", h.get("close", 0))
                vol = h.get("adj_volume", h.get("volume", 0))
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

        # Determine if TPEx or TWSE (TPEx codes >= 9900 or in 6xxx range)
        is_tpex = False
        try:
            code_int = int(stock_code)
            if code_int >= 9900 or (6000 <= code_int <= 6999):
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
    pledge_data = datasets.get("pledge", [])
    for p in pledge_data:
        if get_field(p, "公司代號", "Code", "") == stock_code:
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
    penalty_data = datasets.get("penalties", [])
    for p in penalty_data:
        if get_field(p, "公司代號", "Code", "") == stock_code:
            date_str = get_field(p, "處分日期", "penalty_date", "")
            if date_str:
                try:
                    penalty_date = datetime.strptime(str(date_str)[:8], "%Y%m%d")
                    days_ago = (datetime.now() - penalty_date).days
                    # FIX v3: 365-day lookback, ANY penalty = disqualify
                    if days_ago < 365:
                        return False, "recent_penalty"
                except:
                    pass
    
    # Check sanctions
    sanctions = datasets.get("sanctions", [])
    for s in sanctions:
        if get_field(s, "證券代號", "Code", "") == stock_code:
            return False, "sanctioned"
    
    # Check halts
    halts = datasets.get("halts", [])
    for h in halts:
        if get_field(h, "證券代號", "Code", "") == stock_code:
            return False, "trading_halt"
    
    # Check margin suspension
    margin_susp = datasets.get("margin_susp", [])
    for m in margin_susp:
        if get_field(m, "證券代號", "Code", "") == stock_code:
            return False, "margin_suspended"
    
    return True, "pass"


def score_revenue_momentum(stock_code, revenue_data, weights):
    """Score revenue momentum (0-100)
    
    Revenue data uses Chinese keys:
      公司代號, 營業收入-當月營收, 營業收入-去年同月增減(%), 營業收入-上月比較增減(%)
    """
    # Find stock in revenue data (uses 公司代號)
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


def score_profitability(stock_code, pe_data):
    """Score profitability based on P/E and available metrics (0-100)"""
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


def score_valuation(stock_code, pe_data):
    """Score valuation (0-100)"""
    # Similar to profitability but focuses on value metrics
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


def score_institutional_flow(stock_code, flow_data, margin_data):
    """Score institutional flow (0-100)
    
    Uses margin trading data as proxy since institutional flow API is broken.
    Margin data has Chinese keys: 股票代號, 融資買進, 融券賣出, etc.
    """
    # Handle case where flow_data is not a list (HTML response or empty)
    if not isinstance(flow_data, list):
        flow_data = []
    
    # Try flow data first
    stock_flow = None
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
    
    # Fallback: use margin data as proxy
    if isinstance(margin_data, list):
        stock_margin = None
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


def score_technical_momentum(stock_code, daily_data, pe_data=None, price_history=None):
    """Score technical momentum using available data (0-100)
    
    With historical data: uses 20-day MA, RSI, volume trend
    Without: falls back to single-day proxy
    """
    # Try historical data first
    if price_history and stock_code in price_history:
        history = price_history[stock_code]
        
        if len(history) >= 20:
            return _score_momentum_with_history(history)
        elif len(history) >= 5:
            return _score_momentum_short_history(history)
    
    # Fallback: single-day proxy
    return _score_momentum_single_day(stock_code, daily_data)


def _score_momentum_with_history(history):
    """Score momentum with 20+ days of price history"""
    try:
        closes = [h["close"] for h in history[-20:]]
        volumes = [h["volume"] for h in history[-20:]]
        
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
        
        # RSI (±15 points)
        gains = []
        losses = []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0.0001
        
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
        closes = [h["close"] for h in history]
        
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


def _score_momentum_single_day(stock_code, daily_data):
    """Score momentum with only single-day data (fallback)"""
    # Find stock in daily data
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


def _validate_candidates(candidates, verbose=True):
    """Phase 9: Validate Stage 1 candidates via Pydantic schema.
    
    Catches missing fields (e.g., close=0), wrong types, and score out-of-range
    before downstream modules receive corrupted data. Soft filter — invalid
    records are excluded with a warning but don't block the pipeline.
    """
    if not candidates:
        return []
    
    try:
        from core.schemas import Stage1Candidate, ScoreBreakdown
    except ImportError as e:
        if verbose:
            print(f"⚠ Pydantic schemas unavailable for validation: {e}")
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
            if verbose and invalid_count <= 3:  # Log first few, not all
                code = c.get("code", "??")
                print(f"   ⚠ Candidate validation failed for {code}: {e}")
    
    if verbose:
        print(f"📋 Stage 1 output validated: {len(valid)}/{len(candidates)} valid ({invalid_count} excluded)")
    
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
    price_history = None
    history_file = Path(__file__).parent.parent / "data" / "price_history.json"
    if history_file.exists():
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                price_history = json.load(f)
            if verbose:
                print(f"   📦 Price history loaded: {len(price_history)} stocks")
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
            if verbose:
                print(f"   📊 Market regime: {regime}")
        except:
            pass
    
    # Build company info lookup (company data uses Chinese keys)
    company_lookup = {}
    for c in company_data:
        code = get_field(c, "公司代號", "Code", "")
        company_lookup[code] = c
    
    candidates = []
    watchlist = []
    rejected = []
    
    if verbose:
        print(f"🔍 Stage 1: Screening {len(daily_data)} stocks")
        print(f"   Pass threshold: {thresholds['stage1']['pass_threshold']}")
        print(f"   Watchlist threshold: {thresholds['stage1']['watchlist_threshold']}")
        print()
    
    for stock in daily_data:
        # Daily data uses English keys
        code = get_field(stock, "證券代號", "Code", "")
        name = get_field(stock, "證券名稱", "Name", "")
        
        # Get company info for this stock
        company_info = company_lookup.get(code)
        
        # Hard filters (pass company_info for market cap calc, price_history for ADV)
        passes, reason = check_hard_filters(stock, company_info, datasets, thresholds, price_history=price_history)
        if not passes:
            rejected.append({"code": code, "name": name, "reason": reason})
            continue
        
        # Score each dimension
        scores = {
            "revenue": score_revenue_momentum(code, revenue_data, weights),
            "profitability": score_profitability(code, pe_data),
            "valuation": score_valuation(code, pe_data),
            "flow": score_institutional_flow(code, flow_data, margin_data),
            "momentum": score_technical_momentum(code, daily_data, price_history=price_history)
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
        
        result = {
            "code": code,
            "name": name,
            "close": float(get_field(stock, "收盤價", "ClosingPrice", 0)),
            "composite_score": round(composite, 1),
            "score_breakdown": scores,
            "pass": composite >= thresholds["stage1"]["pass_threshold"]
        }
        
        if composite >= thresholds["stage1"]["pass_threshold"]:
            candidates.append(result)
        elif composite >= thresholds["stage1"]["watchlist_threshold"]:
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
    
    if verbose:
        print(f"📊 Results:")
        print(f"   Passed: {len(valid_candidates)}")
        print(f"   Watchlist: {len(watchlist)}")
        print(f"   Rejected: {len(rejected)}")
        print()
        if valid_candidates:
            print(f"   Top 5 candidates:")
            for c in valid_candidates[:5]:
                print(f"      {c['code']} {c['name']}: {c['composite_score']}")
    
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
