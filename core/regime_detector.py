#!/usr/bin/env python3
"""
Market Regime Detector - Detect bull/choppy/bear regimes

FIX v3:
- NO_TRADE regime state added
- Minimum 5-day regime duration (anti-whipsaw)
- 50/150/300-day SMA for proper cycle coverage
- Transition logic with hysteresis
- Data quality assertions
"""

import json
import math
from datetime import datetime
from pathlib import Path

# FIX v3: Import corporate action handler for ex-dividend awareness
try:
    from corporate_actions import CorporateActionHandler
except ImportError:
    CorporateActionHandler = None

BASE = "https://openapi.twse.com.tw/v1"


def load_price_history():
    """Load merged price history from TWSE API"""
    history_file = Path(__file__).parent.parent / "data" / "price_history.json"
    
    if not history_file.exists():
        return {}
    
    with open(history_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_previous_regime():
    """Load previously saved regime for transition logic"""
    regime_file = Path(__file__).parent.parent / "data" / "regime.json"
    
    if not regime_file.exists():
        return None
    
    with open(regime_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def calc_sma(prices, period):
    """Calculate Simple Moving Average from price history"""
    if not prices or len(prices) < period:
        return None
    return sum(p.get("close", 0) for p in prices[-period:]) / period


def calc_market_breadth(prices, lookback=20):
    """Calculate market breadth: % of stocks above their SMA.
    
    Returns 0.0-1.0. Below 0.3 = extreme risk.
    """
    if not prices:
        return 0.5
    
    above = 0
    total = 0
    
    for code, history in prices.items():
        if len(history) >= lookback:
            sma = calc_sma(history, lookback)
            current = history[-1].get("close", 0)
            if sma > 0 and current > 0:
                total += 1
                if current > sma:
                    above += 1
    
    if total == 0:
        return 0.5
    
    return above / total


def calc_volatility(prices, lookback=20, corp_handler=None):
    """Calculate market volatility using daily returns std dev.
    
    FIX v3: Skip ex-dividend dates to avoid false volatility spikes.
    Ex-dividend drops are mechanical, not market-driven.
    """
    if not prices:
        return 0.0
    
    # Use top 20 stocks by volume
    stock_avg_vol = {}
    for code, history in prices.items():
        if len(history) >= 20:
            recent = history[-20:]
            avg_vol = sum(h.get("volume", 0) for h in recent) / len(recent)
            stock_avg_vol[code] = avg_vol
    
    top_stocks = sorted(stock_avg_vol.items(), key=lambda x: x[1], reverse=True)[:20]
    
    if not top_stocks:
        return 0.0
    
    all_returns = []
    for code, _ in top_stocks:
        history = prices[code]
        if len(history) >= 2:
            for i in range(1, len(history)):
                prev = history[i-1].get("close", 0)
                curr = history[i].get("close", 0)
                date_str = history[i].get("date", "")
                
                # FIX v3: Skip ex-dividend dates
                if corp_handler and corp_handler.is_ex_dividend_date(code, date_str):
                    continue
                
                if prev > 0:
                    all_returns.append((curr - prev) / prev)
    
    if not all_returns:
        return 0.0
    
    mean = sum(all_returns) / len(all_returns)
    variance = sum((r - mean) ** 2 for r in all_returns) / len(all_returns)
    vol = variance ** 0.5
    
    return vol if vol == vol else 0.0


def check_ex_dividend_season():
    """Check if we're in ex-dividend season (July-August)."""
    now = datetime.now()
    if (now.month == 7 and now.day >= 15) or (now.month == 8):
        return True
    return False


def assess_global_risk(prices, corp_handler=None):
    """Assess global risk using market breadth and volatility.
    
    Returns: 'low', 'moderate', 'high', or 'extreme'
    """
    if not prices:
        return "moderate"
    
    breadth = calc_market_breadth(prices, lookback=20)
    vol = calc_volatility(prices, corp_handler=corp_handler)
    
    # VIX-equivalent proxy: high vol + low breadth = extreme
    if breadth < 0.25 and vol > 0.03:
        return "extreme"
    elif breadth < 0.35 or vol > 0.025:
        return "high"
    elif breadth > 0.7 and vol < 0.015:
        return "low"
    else:
        return "moderate"


def detect_regime_raw(prices, config, corp_handler=None):
    """Detect raw regime signal without transition logic.
    
    Uses 50/150/300-day SMAs for proper cycle coverage.
    Returns: raw regime string
    """
    if not prices:
        return "unknown"
    
    # Get top stocks by volume for representative sample
    stock_avg_vol = {}
    for code, history in prices.items():
        if len(history) >= 20:
            recent = history[-20:]
            avg_vol = sum(h.get("volume", 0) for h in recent) / len(recent)
            stock_avg_vol[code] = avg_vol
    
    top_stocks = sorted(stock_avg_vol.items(), key=lambda x: x[1], reverse=True)[:20]
    
    if not top_stocks:
        return "unknown"
    
    # Calculate average position relative to SMAs
    above_50 = 0
    above_150 = 0
    above_300 = 0
    total = 0
    
    for code, _ in top_stocks:
        history = prices[code]
        current = history[-1].get("close", 0)
        
        if current <= 0:
            continue
        
        if len(history) >= 50:
            sma50 = calc_sma(history, 50)
            if sma50 > 0 and current > sma50:
                above_50 += 1
            total += 1
        
        if len(history) >= 150:
            sma150 = calc_sma(history, 150)
            if sma150 > 0 and current > sma150:
                above_150 += 1
        
        if len(history) >= 300:
            sma300 = calc_sma(history, 300)
            if sma300 > 0 and current > sma300:
                above_300 += 1
    
    if total == 0:
        return "unknown"
    
    ratio_50 = above_50 / total
    ratio_150 = above_150 / total if total > 0 else 0.5
    ratio_300 = above_300 / total if total > 0 else 0.5
    
    vol = calc_volatility(prices)
    global_risk = assess_global_risk(prices)
    
    # === NO_TRADE REGIME ===
    # FIX v3: Mandatory no-trade state
    if global_risk == "extreme" and ratio_50 < 0.3:
        return "no_trade"
    if vol > 0.035 and ratio_50 < 0.35:
        return "no_trade"
    
    # Bull regime: above all SMAs
    if ratio_50 > 0.6 and ratio_150 > 0.5:
        if vol > 0.025:
            return "volatile_bull"
        return "bull"
    
    # Bear regime: below all SMAs
    if ratio_50 < 0.4 and ratio_150 < 0.45:
        if vol > 0.025:
            return "volatile_bear"
        return "bear"
    
    # Mixed signals
    if ratio_50 > 0.5:
        return "cautious_bull"
    else:
        return "cautious_bear"


def apply_transition_logic(raw_regime, prev_regime_data, config):
    """Apply transition logic with minimum duration and hysteresis.
    
    FIX v3: Prevent rapid oscillation between regimes.
    Minimum 5 trading days per regime before allowing state change.
    """
    min_duration = config.get("min_regime_duration_days", 5)
    
    if prev_regime_data is None:
        return raw_regime, 1
    
    prev_regime = prev_regime_data.get("regime", "unknown")
    days_in_regime = prev_regime_data.get("days_in_regime", 1)
    
    # If we haven't been in the current regime long enough, stay
    if days_in_regime < min_duration:
        return prev_regime, days_in_regime + 1
    
    # Allow transition
    if raw_regime != prev_regime:
        return raw_regime, 1
    
    # Same regime, increment counter
    return raw_regime, days_in_regime + 1


def detect_regime(date_str=None, verbose=False):
    """Detect current market regime using TWSE data only.
    
    FIX v3: With transition logic, NO_TRADE state, data quality checks,
    and corporate action awareness (ex-dividend date handling).
    """
    data_dir = Path(__file__).parent.parent / "data"
    
    # FIX v3: Initialize corporate action handler
    corp_handler = None
    try:
        from corporate_actions import CorporateActionHandler
        corp_handler = CorporateActionHandler(str(data_dir))
    except ImportError:
        pass
    
    # Load price history
    prices = load_price_history()
    
    # === DATA QUALITY ASSERTION ===
    if not prices:
        return {
            "regime": "unknown",
            "reason": "No price history available. Run fetch_history.py first.",
            "volatility": 0,
            "trend": "unknown",
            "long_term_trend": "neutral",
            "ex_dividend_season": False,
            "global_risk": "neutral",
            "data_quality": "FAIL",
            "confidence": "low"
        }
    
    # Check data quality: need sufficient history
    min_days_50 = 0
    min_days_150 = 0
    min_days_300 = 0
    
    for code, history in prices.items():
        min_days_50 = max(min_days_50, len(history))
        if len(history) >= 150:
            min_days_150 += 1
        if len(history) >= 300:
            min_days_300 += 1
    
    data_quality = "OK"
    confidence = "high"
    
    if min_days_50 < 50:
        data_quality = "INSUFFICIENT_50D"
        confidence = "low"
    elif min_days_50 < 100:
        confidence = "medium"
    
    # Load previous regime for transition logic
    prev_regime = load_previous_regime()
    
    # Load config
    config_file = data_dir.parent / "config" / "regime_rules.json"
    config = {}
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
    
    # Get raw regime signal
    raw_regime = detect_regime_raw(prices, config, corp_handler=corp_handler)
    
    # Apply transition logic
    regime, days_in_regime = apply_transition_logic(raw_regime, prev_regime, config)
    
    # Volatility (with ex-dividend awareness)
    volatility = calc_volatility(prices, corp_handler=corp_handler)
    
    # Ex-dividend season check
    ex_div = check_ex_dividend_season()
    
    # Global risk (with ex-dividend awareness)
    global_risk = assess_global_risk(prices, corp_handler=corp_handler)
    
    result = {
        "regime": regime,
        "raw_regime": raw_regime,
        "days_in_regime": days_in_regime,
        "volatility": round(volatility, 4),
        "ex_dividend_season": ex_div,
        "global_risk": global_risk,
        "data_quality": data_quality,
        "confidence": confidence,
        "timestamp": datetime.now().isoformat(),
        "stocks_analyzed": len(prices),
        "max_data_days": min_days_50
    }
    
    if verbose:
        print(f"🔍 Market Regime: {regime}")
        print(f"   Raw signal: {raw_regime}")
        print(f"   Days in regime: {days_in_regime}")
        print(f"   Volatility: {volatility:.4f}")
        print(f"   Global risk: {global_risk}")
        print(f"   Data quality: {data_quality} (confidence: {confidence})")
        print(f"   Stocks analyzed: {len(prices)}")
        print(f"   Max data days: {min_days_50}")
    
    return result


def get_regime_weights(regime):
    """Get adjusted weights based on regime."""
    config_file = Path(__file__).parent.parent / "config" / "weights.json"
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        regime_config = config.get("regime_weights", {})
        
        if regime in ("bull", "volatile_bull", "cautious_bull"):
            return regime_config.get("bull_momentum", {})
        elif regime in ("bear", "volatile_bear", "cautious_bear"):
            return regime_config.get("bear_defensive", {})
        elif regime == "no_trade":
            # NO_TRADE: don't generate any signals
            return None
        else:
            return regime_config.get("choppy_neutral", {})
    
    return None


def get_regime_position_mult(regime):
    """Get position size multiplier based on regime."""
    if regime == "no_trade":
        return 0.0  # No new positions
    
    config_file = Path(__file__).parent.parent / "config" / "regime_rules.json"
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        regimes = config.get("regimes", {})
        
        if regime in ("bull", "volatile_bull", "cautious_bull"):
            return regimes.get("bull_momentum", {}).get("adjustments", {}).get("position_size_mult", 1.0)
        elif regime in ("bear", "volatile_bear", "cautious_bear"):
            return regimes.get("bear_defensive", {}).get("adjustments", {}).get("position_size_mult", 0.4)
        else:
            return regimes.get("choppy_neutral", {}).get("adjustments", {}).get("position_size_mult", 0.6)
    
    return 0.6


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Detect market regime")
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    result = detect_regime(date_str=args.date, verbose=args.verbose)
    
    # Save regime
    regime_file = Path(__file__).parent.parent / "data" / "regime.json"
    with open(regime_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    if args.verbose:
        print(f"\n💾 Saved to data/regime.json")
    
    return result


if __name__ == "__main__":
    main()
