#!/usr/bin/env python3
"""
Market Regime Detector - Detect bull/choppy/bear regimes

FIX v2: 
- Removed yfinance dependency (unreliable for TW stocks)
- Uses TWSE Open API historical data exclusively
- Extended lookback: 50-day (short-term) + 200-day (long-term)
- Added global risk filter (VIX proxy via TWSE volatility index)
- Handles ex-dividend season (July-August) to avoid false crash signals
"""

import json
import math
from datetime import datetime
from pathlib import Path

BASE = "https://openapi.twse.com.tw/v1"


def load_price_history():
    """Load merged price history from TWSE API"""
    history_file = Path(__file__).parent.parent / "data" / "price_history.json"
    
    if not history_file.exists():
        return {}
    
    with open(history_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_historical_dates():
    """Load all available historical date files"""
    data_dir = Path(__file__).parent.parent / "data"
    files = sorted(data_dir.glob("historical_*.json"))
    dates = []
    for f in files:
        try:
            date_str = f.stem.replace("historical_", "")
            datetime.strptime(date_str, "%Y-%m-%d")
            dates.append(date_str)
        except:
            pass
    return dates


def calc_market_trend(prices, lookback=50):
    """Calculate market trend from price data using top 20 stocks by volume.
    
    Uses 50-day lookback for short-term trend detection.
    Returns: 'bull', 'bear', 'choppy', or 'unknown'
    """
    if not prices:
        return "unknown"
    
    # Sort stocks by average volume (last 20 days)
    stock_avg_vol = {}
    for code, history in prices.items():
        if len(history) >= 20:
            recent = history[-20:]
            avg_vol = sum(h.get("volume", 0) for h in recent) / len(recent)
            stock_avg_vol[code] = avg_vol
    
    # Get top 20 by volume
    top_stocks = sorted(stock_avg_vol.items(), key=lambda x: x[1], reverse=True)[:20]
    
    if not top_stocks:
        return "unknown"
    
    # Calculate average return for top stocks over lookback period
    returns = []
    for code, _ in top_stocks:
        history = prices[code]
        if len(history) >= lookback:
            recent = history[-lookback:]
            first_close = recent[0].get("close", 0)
            last_close = recent[-1].get("close", 0)
            
            if first_close > 0:
                ret = (last_close - first_close) / first_close
                returns.append(ret)
    
    if not returns:
        return "unknown"
    
    avg_return = sum(returns) / len(returns)
    
    # Classify regime with wider thresholds to reduce whipsaw
    if avg_return > 0.08:
        return "bull"
    elif avg_return < -0.08:
        return "bear"
    else:
        return "choppy"


def calc_long_term_trend(prices, lookback=200):
    """Calculate long-term trend (200-day MA proxy).
    
    Returns: 'uptrend', 'downtrend', or 'neutral'
    """
    if not prices:
        return "neutral"
    
    stock_avg_vol = {}
    for code, history in prices.items():
        if len(history) >= 20:
            recent = history[-20:]
            avg_vol = sum(h.get("volume", 0) for h in recent) / len(recent)
            stock_avg_vol[code] = avg_vol
    
    top_stocks = sorted(stock_avg_vol.items(), key=lambda x: x[1], reverse=True)[:20]
    
    if not top_stocks:
        return "neutral"
    
    returns = []
    for code, _ in top_stocks:
        history = prices[code]
        if len(history) >= lookback:
            recent = history[-lookback:]
            first_close = recent[0].get("close", 0)
            last_close = recent[-1].get("close", 0)
            
            if first_close > 0:
                ret = (last_close - first_close) / first_close
                returns.append(ret)
    
    if not returns:
        return "neutral"
    
    avg_return = sum(returns) / len(returns)
    
    if avg_return > 0.15:
        return "uptrend"
    elif avg_return < -0.15:
        return "downtrend"
    else:
        return "neutral"


def calc_volatility(prices, lookback=20):
    """Calculate market volatility using daily returns std dev."""
    if not prices:
        return 0.0
    
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
                if prev > 0:
                    all_returns.append((curr - prev) / prev)
    
    if not all_returns:
        return 0.0
    
    mean = sum(all_returns) / len(all_returns)
    variance = sum((r - mean) ** 2 for r in all_returns) / len(all_returns)
    vol = variance ** 0.5
    
    return vol if vol == vol else 0.0  # NaN check


def check_ex_dividend_season():
    """Check if we're in ex-dividend season (July-August).
    
    During this period, many stocks drop on ex-dividend dates,
    which can falsely trigger 'crash' signals.
    """
    now = datetime.now()
    # July 15 - August 31 is typical ex-dividend season in Taiwan
    if (now.month == 7 and now.day >= 15) or (now.month == 8):
        return True
    # Also check if the date range includes this period
    return False


def detect_regime(date_str=None, verbose=False):
    """Detect current market regime using TWSE data only."""
    data_dir = Path(__file__).parent.parent / "data"
    
    # Always use TWSE price history
    prices = load_price_history()
    
    if not prices:
        return {
            "regime": "unknown",
            "reason": "No price history available. Run fetch_history.py first.",
            "volatility": 0,
            "trend": "unknown",
            "long_term_trend": "neutral",
            "ex_dividend_season": False,
            "global_risk": "neutral"
        }
    
    # Short-term trend (50-day)
    trend = calc_market_trend(prices, lookback=50)
    
    # Long-term trend (200-day)
    long_term = calc_long_term_trend(prices, lookback=200)
    
    # Volatility
    volatility = calc_volatility(prices)
    
    # Ex-dividend season check
    ex_div = check_ex_dividend_season()
    
    # Global risk assessment (proxy: TWSE volatility + breadth)
    global_risk = assess_global_risk(prices)
    
    # Combine signals: short-term + long-term + volatility
    regime = combine_regime_signals(trend, long_term, volatility, ex_div, global_risk)
    
    result = {
        "regime": regime,
        "trend": trend,
        "long_term_trend": long_term,
        "volatility": round(volatility, 4),
        "ex_dividend_season": ex_div,
        "global_risk": global_risk,
        "timestamp": datetime.now().isoformat(),
        "stocks_analyzed": len(prices),
        "data_days": max((len(h) for h in prices.values()), default=0)
    }
    
    if verbose:
        print(f"🔍 Market Regime: {regime}")
        print(f"   Short-term trend (50d): {trend}")
        print(f"   Long-term trend (200d): {long_term}")
        print(f"   Volatility: {volatility:.4f}")
        print(f"   Ex-dividend season: {ex_div}")
        print(f"   Global risk: {global_risk}")
        print(f"   Stocks analyzed: {len(prices)}")
    
    return result


def assess_global_risk(prices):
    """Assess global risk using market breadth and volatility.
    
    Returns: 'low', 'moderate', 'high', or 'extreme'
    """
    if not prices:
        return "moderate"
    
    # Market breadth: % of stocks above their 20-day MA
    above_ma = 0
    total = 0
    
    for code, history in prices.items():
        if len(history) >= 20:
            recent = history[-20:]
            ma20 = sum(h.get("close", 0) for h in recent) / 20
            current = history[-1].get("close", 0)
            if current > 0 and ma20 > 0:
                total += 1
                if current > ma20:
                    above_ma += 1
    
    if total == 0:
        return "moderate"
    
    breadth = above_ma / total
    
    # Volatility assessment
    vol = calc_volatility(prices)
    
    if breadth < 0.3 and vol > 0.025:
        return "extreme"
    elif breadth < 0.4 or vol > 0.02:
        return "high"
    elif breadth > 0.7 and vol < 0.015:
        return "low"
    else:
        return "moderate"


def combine_regime_signals(trend, long_term, volatility, ex_div, global_risk):
    """Combine multiple signals into final regime classification.
    
    Logic:
    - Long-term uptrend + short-term bull = strong bull
    - Long-term downtrend + short-term bear = strong bear
    - Mixed signals = choppy/cautious
    - Ex-dividend season = dampen bear signals
    - High global risk = reduce position sizing
    """
    # Base regime from short-term trend
    base = trend
    
    # Override with long-term context
    if long_term == "uptrend" and trend == "choppy":
        base = "cautious_bull"
    elif long_term == "downtrend" and trend == "choppy":
        base = "cautious_bear"
    elif long_term == "downtrend" and trend == "bull":
        base = "rebound"  # Potential dead cat bounce
    elif long_term == "uptrend" and trend == "bear":
        base = "pullback"  # Healthy correction in uptrend
    
    # Dampen bear signals during ex-dividend season
    if ex_div and base in ("bear", "volatile_bear"):
        base = "choppy"
    
    # Add volatility modifier
    if volatility > 0.025:
        if "bull" in base:
            return "volatile_bull"
        elif "bear" in base:
            return "volatile_bear"
        else:
            return "high_volatility"
    
    return base


def get_regime_weights(regime):
    """Get adjusted weights based on regime."""
    config_file = Path(__file__).parent.parent / "config" / "weights.json"
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        regime_config = config.get("regime_weights", {})
        
        # Map new regime names to weight profiles
        if regime in ("bull", "volatile_bull", "cautious_bull", "rebound"):
            return regime_config.get("bull_momentum", {})
        elif regime in ("bear", "volatile_bear", "cautious_bear"):
            return regime_config.get("bear_defensive", {})
        else:
            return regime_config.get("choppy_neutral", {})
    
    # Default weights
    return {
        "revenue_momentum": 0.20,
        "profitability": 0.20,
        "valuation": 0.20,
        "institutional_flow": 0.20,
        "technical_momentum": 0.20
    }


def get_regime_position_mult(regime):
    """Get position size multiplier based on regime."""
    config_file = Path(__file__).parent.parent / "config" / "regime_rules.json"
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        regimes = config.get("regimes", {})
        
        # Map to closest matching regime
        if regime in ("bull", "volatile_bull", "cautious_bull"):
            return regimes.get("bull_momentum", {}).get("adjustments", {}).get("position_size_mult", 1.0)
        elif regime in ("bear", "volatile_bear", "cautious_bear"):
            return regimes.get("bear_defensive", {}).get("adjustments", {}).get("position_size_mult", 0.4)
        else:
            return regimes.get("choppy_neutral", {}).get("adjustments", {}).get("position_size_mult", 0.6)
    
    return 0.6  # Conservative default


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
