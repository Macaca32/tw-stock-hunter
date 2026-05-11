#!/usr/bin/env python3
"""
Historical Data Fetcher - Pull N days of daily data for momentum calculations

Uses TWSE Open API as primary source, falls back to yfinance when needed.
Phase 2: Integrates backward price adjustment for corporate actions.
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add parent dir for imports
sys.path.insert(0, str(Path(__file__).parent))

try:
    import yfinance as yf
    import pandas as pd
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False
    print("⚠️  yfinance not available - install with: pip install yfinance")

from corporate_actions import CorporateActionHandler
from holiday_calendar import HolidayCalendar, get_holiday_gaps as _get_holiday_gaps_fn, is_trading_day as _is_trading_day_fn

BASE = "https://openapi.twse.com.tw/v1"
ENDPOINT = "/exchangeReport/STOCK_DAY_ALL"
RATE_LIMIT = 0.3
TIMEOUT = 15


def safe_float(val, default=0.0):
    """Safely convert to float"""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def tw_date_to_iso(date_str):
    """Convert TW date string (YYYYMMDD) to ISO format"""
    try:
        return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except:
        return None


def fetch_from_twse(dates, output_dir=None, verbose=False):
    """Fetch historical data from TWSE Open API"""
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)
    
    all_data = {}
    
    for date_str in dates:
        tw_date = date_str.replace("-", "")
        
        if verbose:
            print(f"  → Fetching {date_str}...")
        
        try:
            import requests
            r = requests.get(
                f"{BASE}{ENDPOINT}",
                params={"date": tw_date},
                timeout=TIMEOUT
            )
            
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    all_data[date_str] = data
                    if verbose:
                        print(f"    ✓ {len(data)} stocks")
                    
                    # Save individual file
                    filepath = output_dir / f"historical_{date_str}.json"
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False)
                else:
                    if verbose:
                        print(f"    ⚠ No data (holiday?)")
            else:
                if verbose:
                    print(f"    ⚠ HTTP {r.status_code}")
        except Exception as e:
            if verbose:
                print(f"    ✗ Error: {e}")
        
        time.sleep(RATE_LIMIT)
    
    return all_data


def fetch_from_yfinance(stock_codes, dates, output_dir=None, verbose=False):
    """Fetch historical data from yfinance (fallback)"""
    if not YF_AVAILABLE:
        return {}
    
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)
    
    # Convert codes to yfinance symbols
    symbols = [f"{code}.TW" for code in stock_codes]
    
    if verbose:
        print(f"  → Fetching {len(symbols)} stocks from yfinance...")
    
    all_data = {}
    
    # Fetch in batches to avoid overwhelming yfinance
    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        try:
            # Use download for efficiency
            data = yf.download(
                batch,
                period="30d",
                group_by="ticker",
                progress=False,
                auto_adjust=True
            )
            
            if not data.empty:
                # Extract data for each stock
                for symbol in batch:
                    try:
                        stock_data = data[symbol]
                        
                        # Convert to our format
                        for date_obj, row in stock_data.iterrows():
                            date_iso = date_obj.strftime("%Y-%m-%d")
                            if date_iso not in dates:
                                continue
                            
                            close = row.get("Close", 0)
                            # Skip if close is NaN
                            if close != close:  # NaN check
                                continue
                            
                            if date_iso not in all_data:
                                all_data[date_iso] = []
                            
                            all_data[date_iso].append({
                                "Code": symbol.replace(".TW", ""),
                                "Name": "",  # yfinance doesn't provide names easily
                                "OpeningPrice": row.get("Open", 0) if row.get("Open", 0) == row.get("Open", 0) else 0,
                                "HighestPrice": row.get("High", 0) if row.get("High", 0) == row.get("High", 0) else 0,
                                "LowestPrice": row.get("Low", 0) if row.get("Low", 0) == row.get("Low", 0) else 0,
                                "ClosingPrice": float(row.get("Close", 0)) if row.get("Close", 0) == row.get("Close", 0) else 0,
                                "TradeVolume": row.get("Volume", 0) if row.get("Volume", 0) == row.get("Volume", 0) else 0
                            })
                    except:
                        pass
            
            time.sleep(1)  # Rate limit
            
        except Exception as e:
            if verbose:
                print(f"    ✗ Batch error: {e}")
    
    if verbose:
        print(f"  ✓ Fetched {sum(len(v) for v in all_data.values())} stock-day records")
    
    return all_data


def get_trading_dates(start_date, end_date, verbose=False):
    """Get list of trading dates (skip weekends AND holidays).

    Survivorship Bias Tier 2: Uses TWSE holiday calendar to avoid phantom
    trading days. Without this, SMA windows count LNY gaps as missing data,
    inflating volatility and distorting momentum signals.
    """
    start_iso = start_date.strftime("%Y-%m-%d")
    end_iso = end_date.strftime("%Y-%m-%d")

    try:
        from pathlib import Path
        data_dir = str(Path(__file__).parent.parent / "data")
        cal = HolidayCalendar(data_dir)
        dates = cal.get_trading_days_in_range(start_iso, end_iso)

        # Show holiday gaps for awareness
        gaps = cal.get_holiday_gaps(start_iso, end_iso)
        if gaps and verbose:
            print(f"   📅 Holiday gaps detected: {len(gaps)}")
            for g in gaps:
                print(f"      {g['start']} → {g['end']} ({g['length']} days): {g.get('reason', '?')}")
    except Exception as e:
        if verbose:
            print(f"   ⚠ Holiday calendar unavailable ({e}), using weekend-only filter")
        # Fallback to weekend-only filter
        dates = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

    return dates


def build_price_history(date_str=None, lookback_days=365, verbose=False):
    """Build price history for all stocks over lookback period.
    
    Default: 365 days (≈1 full trading year, ~245 trading days).
    Need at least 240 days for 300-day SMA to be reliable.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    end_date = datetime.strptime(date_str, "%Y-%m-%d")
    start_date = end_date - timedelta(days=lookback_days * 1.2)  # Extra buffer for holidays
    
    dates = get_trading_dates(start_date, end_date, verbose)
    
    if verbose:
        print(f"📅 Building price history for {len(dates)} trading days")
    
    # Check cache first
    data_dir = Path(__file__).parent.parent / "data"
    cached = []
    for d in dates:
        filepath = data_dir / f"historical_{d}.json"
        if filepath.exists():
            cached.append(d)
    
    if cached and verbose:
        print(f"   📦 {len(cached)} days cached")
    
    # Fetch missing dates
    to_fetch = [d for d in dates if d not in cached]
    if to_fetch:
        if verbose:
            print(f"   📡 Fetching {len(to_fetch)} new days")
        
        # Try TWSE first
        twse_data = fetch_from_twse(to_fetch, output_dir=data_dir, verbose=verbose)
        
        # If TWSE failed or returned duplicate data, fall back to yfinance
        if not twse_data or all(len(v) == 0 for v in twse_data.values()):
            if verbose:
                print("   ⚠️  TWSE returned no data, falling back to yfinance")
            
            # Get stock codes from existing data
            stock_codes = []
            if data_dir.exists():
                daily_file = data_dir / f"daily_{date_str}.json"
                if daily_file.exists():
                    with open(daily_file, 'r', encoding='utf-8') as f:
                        daily_data = json.load(f)
                        stock_codes = [d.get("Code", "") for d in daily_data if d.get("Code")]
            
            if stock_codes:
                yf_data = fetch_from_yfinance(stock_codes, to_fetch, output_dir=data_dir, verbose=verbose)
                twse_data = yf_data
        
        # Merge TWSE data
        for d, stocks in twse_data.items():
            filepath = data_dir / f"historical_{d}.json"
            if not filepath.exists():
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(stocks, f, ensure_ascii=False)
    
    # Merge all data
    all_prices = {}
    for d in dates:
        filepath = data_dir / f"historical_{d}.json"
        if filepath.exists():
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for stock in data:
                    code = stock.get("Code", "")
                    if code not in all_prices:
                        all_prices[code] = []
                    
                    # Use safe_float for all numeric fields
                    all_prices[code].append({
                        "date": d,
                        "close": safe_float(stock.get("ClosingPrice", ""), 0),
                        "volume": safe_float(stock.get("TradeVolume", ""), 0),
                        "open": safe_float(stock.get("OpeningPrice", ""), 0),
                        "high": safe_float(stock.get("HighestPrice", ""), 0),
                        "low": safe_float(stock.get("LowestPrice", ""), 0)
                    })

    # Phase 2: Apply backward adjustment for corporate actions
    if verbose:
        print(f"   🔄 Applying backward adjustment for corporate actions...")
    
    ca_handler = CorporateActionHandler(data_dir=str(data_dir))
    ca_summary = ca_handler.summary()
    if verbose:
        print(f"      Corporate actions loaded: {ca_summary['twt49u_actions']} (TWT49U) + {ca_summary['declaration_actions']} (declarations)")
    
    adjusted_prices = {}
    adjusted_count = 0
    for code, prices in all_prices.items():
        adjusted = ca_handler.backward_adjust_prices(prices, code)
        adjusted_prices[code] = adjusted
        # Count stocks that actually had adjustments applied
        if any(p.get("cumulative_factor", 1.0) != 1.0 for p in adjusted):
            adjusted_count += 1
    
    if verbose:
        print(f"      Adjusted {adjusted_count}/{len(adjusted_prices)} stocks had corporate actions in range")
    
    all_prices = adjusted_prices
    
    # Save merged history (with backward-adjusted prices)
    history_file = data_dir / "price_history.json"
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(all_prices, f, ensure_ascii=False)
    
    if verbose:
        print(f"\n✅ Price history built for {len(all_prices)} stocks")
        print(f"   Saved to data/price_history.json (backward-adjusted)")
        print(f"   Note: Prices include 'adj_close' field for corporate-action-adjusted prices")
    
    return all_prices


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Build historical price data")
    parser.add_argument("--date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--lookback", type=int, default=20, help="Days of history")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--force-yf", action="store_true", help="Force yfinance fallback")
    args = parser.parse_args()
    
    build_price_history(
        date_str=args.date,
        lookback_days=args.lookback,
        verbose=args.verbose
    )


if __name__ == "__main__":
    main()
