#!/usr/bin/env python3
"""
Market Regime Detector - Detect bull/choppy/bear regimes

Uses yfinance for reliable price data when TWSE API returns stale snapshots.
"""

import json
import time
from datetime import datetime
from pathlib import Path

try:
    import yfinance as yf
    import pandas as pd
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False
    print("⚠️  yfinance not available")


def load_price_history():
    """Load merged price history from TWSE API"""
    history_file = Path(__file__).parent.parent / "data" / "price_history.json"
    
    if not history_file.exists():
        return {}
    
    with open(history_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def fetch_yf_prices(stock_codes, lookback_days=20):
    """Fetch prices from yfinance (fallback)"""
    if not YF_AVAILABLE:
        return {}
    
    # Limit to top 50 stocks to avoid rate limits
    if len(stock_codes) > 50:
        stock_codes = stock_codes[:50]
    
    symbols = [f"{code}.TW" for code in stock_codes]
    
    all_prices = {}
    
    # Fetch in smaller batches with delays
    batch_size = 10
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        try:
            data = yf.download(
                batch,
                period=f"{lookback_days}d",
                group_by="ticker",
                progress=False,
                auto_adjust=True
            )
            
            if not data.empty:
                for symbol in batch:
                    try:
                        stock_data = data[symbol]
                        code = symbol.replace(".TW", "")
                        
                        all_prices[code] = []
                        for date_obj, row in stock_data.iterrows():
                            close = row.get("Close", 0)
                            volume = row.get("Volume", 0)
                            
                            # Skip if close is NaN
                            if close != close:  # NaN check
                                continue
                            
                            all_prices[code].append({
                                "date": date_obj.strftime("%Y-%m-%d"),
                                "close": float(close),
                                "volume": float(volume) if volume == volume else 0,
                                "open": float(row.get("Open", 0)) if row.get("Open", 0) == row.get("Open", 0) else 0,
                                "high": float(row.get("High", 0)) if row.get("High", 0) == row.get("High", 0) else 0,
                                "low": float(row.get("Low", 0)) if row.get("Low", 0) == row.get("Low", 0) else 0
                            })
                    except:
                        pass
            
            time.sleep(2)  # Rate limit delay
            
        except Exception as e:
            print(f"yfinance batch error: {e}")
            time.sleep(5)  # Longer delay on error
    
    return all_prices


def calc_market_trend(prices, lookback=20):
    """Calculate market trend from price data
    
    Uses top 20 stocks by volume as market proxy
    """
    if not prices:
        return "unknown"
    
    # Sort stocks by average volume
    stock_avg_vol = {}
    for code, history in prices.items():
        if len(history) >= lookback:
            avg_vol = sum(h["volume"] for h in history[-lookback:]) / len(history[-lookback:])
            # Skip if volume is NaN
            if avg_vol == avg_vol:  # NaN check
                stock_avg_vol[code] = avg_vol
    
    # Get top 20 by volume
    top_stocks = sorted(stock_avg_vol.items(), key=lambda x: x[1], reverse=True)[:20]
    
    if not top_stocks:
        return "unknown"
    
    # Calculate average return for top stocks
    returns = []
    for code, _ in top_stocks:
        history = prices[code]
        if len(history) >= lookback:
            recent = history[-lookback:]
            first_close = recent[0]["close"]
            last_close = recent[-1]["close"]
            
            # Skip if either close is NaN or 0
            if first_close > 0 and first_close == first_close and last_close == last_close:
                ret = (last_close - first_close) / first_close
                returns.append(ret)
    
    if not returns:
        return "unknown"
    
    avg_return = sum(returns) / len(returns)
    
    # Classify regime
    if avg_return > 0.05:
        return "bull"
    elif avg_return < -0.05:
        return "bear"
    else:
        return "choppy"


def calc_volatility(prices, lookback=20):
    """Calculate market volatility"""
    if not prices:
        return 0.0
    
    # Use top 20 stocks
    stock_avg_vol = {}
    for code, history in prices.items():
        if len(history) >= lookback:
            avg_vol = sum(h["volume"] for h in history[-lookback:]) / len(history[-lookback:])
            # Skip if volume is NaN
            if avg_vol == avg_vol:  # NaN check
                stock_avg_vol[code] = avg_vol
    
    top_stocks = sorted(stock_avg_vol.items(), key=lambda x: x[1], reverse=True)[:20]
    
    if not top_stocks:
        return 0.0
    
    # Calculate daily returns
    all_returns = []
    for code, _ in top_stocks:
        history = prices[code]
        if len(history) >= 2:
            for i in range(1, len(history)):
                prev = history[i-1]["close"]
                curr = history[i]["close"]
                # Skip if either is NaN or 0
                if prev > 0 and prev == prev and curr == curr:
                    all_returns.append((curr - prev) / prev)
    
    if not all_returns:
        return 0.0
    
    # Standard deviation of returns
    mean = sum(all_returns) / len(all_returns)
    variance = sum((r - mean) ** 2 for r in all_returns) / len(all_returns)
    return variance ** 0.5


def detect_regime(date_str=None, verbose=False):
    """Detect current market regime"""
    # Always use yfinance for regime detection since TWSE API returns stale snapshots
    if YF_AVAILABLE:
        if verbose:
            print("   📡 Using yfinance for regime detection")
        
        # Get stock codes from daily data
        data_dir = Path(__file__).parent.parent / "data"
        daily_file = data_dir / f"daily_{date_str or datetime.now().strftime('%Y-%m-%d')}.json"
        
        if daily_file.exists():
            with open(daily_file, 'r', encoding='utf-8') as f:
                daily_data = json.load(f)
            
            stock_codes = [d.get("Code", "") for d in daily_data if d.get("Code")]
            prices = fetch_yf_prices(stock_codes, lookback_days=20)
            
            # Save yfinance data to price_history.json for use by other modules
            history_file = data_dir / "price_history.json"
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(prices, f, ensure_ascii=False)
            
            if verbose:
                print(f"   💾 Saved yfinance price history for {len(prices)} stocks")
        else:
            prices = {}
    else:
        # Fallback to TWSE data
        prices = load_price_history()
    
    if not prices:
        return {
            "regime": "unknown",
            "reason": "No price history available",
            "volatility": 0,
            "trend": "unknown"
        }
    
    trend = calc_market_trend(prices)
    volatility = calc_volatility(prices)
    
    # Handle NaN volatility
    if volatility != volatility:  # NaN check
        volatility = 0.0
    
    # Refine regime based on volatility
    if volatility > 0.03:
        if trend == "bull":
            regime = "volatile_bull"
        elif trend == "bear":
            regime = "volatile_bear"
        else:
            regime = "choppy"
    else:
        regime = trend
    
    result = {
        "regime": regime,
        "trend": trend,
        "volatility": round(volatility, 4),
        "timestamp": datetime.now().isoformat(),
        "stocks_analyzed": len(prices),
        "data_days": max(len(h) for h in prices.values()) if prices else 0
    }
    
    if verbose:
        print(f"🔍 Market Regime: {regime}")
        print(f"   Trend: {trend}")
        print(f"   Volatility: {volatility:.4f}")
        print(f"   Stocks: {len(prices)}")
    
    return result


def get_regime_weights(regime):
    """Get adjusted weights based on regime"""
    config_file = Path(__file__).parent.parent / "config" / "weights.json"
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        regime_config = config.get("regime_weights", {})
        
        if regime == "bull" or regime == "volatile_bull":
            return regime_config.get("bull_momentum", {})
        elif regime == "bear" or regime == "volatile_bear":
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
