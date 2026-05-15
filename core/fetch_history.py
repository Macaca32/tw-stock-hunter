#!/usr/bin/env python3
"""
Historical Data Fetcher - Pull N days of daily data for momentum calculations

Uses TWSE Open API as primary source, falls back to yfinance when needed.
Phase 2: Integrates backward price adjustment for corporate actions.
"""

import json
import sys
import time
import logging
import warnings
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# Add parent dir for imports
sys.path.insert(0, str(Path(__file__).parent))

try:
    import yfinance as yf
    import pandas as pd
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False
    logger.warning("yfinance not available - install with: pip install yfinance")

from corporate_actions import CorporateActionHandler
from holiday_calendar import HolidayCalendar, get_holiday_gaps as _get_holiday_gaps_fn, is_trading_day as _is_trading_day_fn

BASE = "https://openapi.twse.com.tw/v1"
ENDPOINT = "/exchangeReport/STOCK_DAY_ALL"
RATE_LIMIT = 0.3
TIMEOUT = 15


def _validate_twse_history_response(data, date_str, verbose=False):
    """Validate TWSE historical data API response.

    Phase 20: Catch malformed responses at ingestion boundary.
    Returns validated list of records, or None if unusable.
    """
    if data is None:
        logger.warning("%s: response is None", date_str)
        return None

    # Reject HTML that slipped through
    if isinstance(data, str):
        if "<!DOCTYPE" in data[:100] or "<html" in data[:100].lower():
            logger.warning("%s: received HTML instead of JSON", date_str)
            return None
        logger.warning("%s: unexpected string response", date_str)
        return None

    # Reject dicts with error keys
    if isinstance(data, dict):
        if "error" in data or "Error" in data:
            err_msg = data.get("error", data.get("Error", "unknown"))
            logger.warning("%s: API error — %s", date_str, err_msg)
            return None
        # Unwrap {"data": [...]} if present
        if "data" in data and isinstance(data["data"], list):
            data = data["data"]
        else:
            logger.warning("%s: unexpected dict structure — %s", date_str, list(data.keys())[:5])
            return None

    # Expected: list of stock records
    if not isinstance(data, list):
        logger.warning("%s: expected list, got %s", date_str, type(data).__name__)
        return None

    # Empty list is valid (holiday)
    if len(data) == 0:
        return data

    # Spot-check first record structure
    if isinstance(data[0], dict):
        first = data[0]
        has_expected_fields = (
            "Code" in first or "證券代號" in first or
            "ClosingPrice" in first or "收盤價" in first
        )
        if not has_expected_fields:
            warnings.warn(
                f"TWSE history {date_str}: first record missing expected fields: "
                f"{list(first.keys())[:6]}"
            )
    return data


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
        
        logger.debug("Fetching %s...", date_str)
        
        try:
            import requests
            r = requests.get(
                f"{BASE}{ENDPOINT}",
                params={"date": tw_date},
                timeout=TIMEOUT
            )
            
            if r.status_code == 200:
                data = r.json()
                # Phase 20: Validate response structure at ingestion boundary
                validated = _validate_twse_history_response(data, date_str, verbose=verbose)
                if validated is None:
                    logger.warning("%s: response validation failed", date_str)
                elif isinstance(validated, list) and validated:
                    all_data[date_str] = validated
                    logger.info("%s: %d stocks", date_str, len(validated))
                    
                    # Save individual file
                    filepath = output_dir / f"historical_{date_str}.json"
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(validated, f, ensure_ascii=False)
                else:
                    logger.warning("%s: No data (holiday?)", date_str)
            else:
                logger.warning("%s: HTTP %d", date_str, r.status_code)
        except Exception as e:
            logger.error("%s: Error: %s", date_str, e)
        
        time.sleep(RATE_LIMIT)
    
    return all_data


def fetch_from_yfinance(stock_codes, dates, output_dir=None, verbose=False):
    """Fetch historical data from yfinance (fallback).

    Phase 10 R8: yfinance .TW symbols are unreliable. This function now:
    - Has per-batch timeout (30s total) to avoid hanging the pipeline
    - Logs failures clearly and returns empty dict on any error
    """
    if not YF_AVAILABLE:
        return {}

    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "data"

    # Convert codes to yfinance symbols
    symbols = [f"{code}.TW" for code in stock_codes]

    logger.debug("Fetching %d stocks from yfinance (fallback, may be slow)...", len(symbols))

    all_data = {}

    # Fetch in batches to avoid overwhelming yfinance
    batch_size = 50
    total_timeout = 30  # seconds per batch
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]

        try:
            import signal

            class _YfTimeout(Exception):
                pass

            def _handler(signum, frame):
                raise _YfTimeout()

            old_handler = signal.signal(signal.SIGALRM, _handler)
            signal.alarm(total_timeout)

            try:
                data = yf.download(
                    batch,
                    period="30d",
                    group_by="ticker",
                    progress=False,
                    auto_adjust=True
                )
            except _YfTimeout:
                logger.error("Batch %d timed out after %ds", i // batch_size + 1, total_timeout)
                return all_data
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

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
            logger.error("Batch error: %s", e)

    logger.info("Fetched %d stock-day records from yfinance", sum(len(v) for v in all_data.values()))

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
        if gaps:
            logger.info("Holiday gaps detected: %d", len(gaps))
            for g in gaps:
                logger.info("  %s → %s (%d days): %s", g['start'], g['end'], g['length'], g.get('reason', '?'))
    except Exception as e:
        logger.warning("Holiday calendar unavailable (%s), using weekend-only filter", e)
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
    
    logger.info("Building price history for %d trading days", len(dates))
    
    # Check cache first
    data_dir = Path(__file__).parent.parent / "data"
    cached = []
    for d in dates:
        filepath = data_dir / f"historical_{d}.json"
        if filepath.exists():
            cached.append(d)
    
    if cached:
        logger.info("%d days cached", len(cached))
    
    # Fetch missing dates
    to_fetch = [d for d in dates if d not in cached]
    if to_fetch:
        logger.info("Fetching %d new days", len(to_fetch))
        
        # Try TWSE first
        twse_data = fetch_from_twse(to_fetch, output_dir=data_dir, verbose=verbose)
        
        # If TWSE failed or returned duplicate data, fall back to yfinance
        if not twse_data or all(len(v) == 0 for v in twse_data.values()):
            logger.warning("TWSE returned no data, falling back to yfinance")
            
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
    logger.info("Applying backward adjustment for corporate actions...")
    
    ca_handler = CorporateActionHandler(data_dir=str(data_dir))
    ca_summary = ca_handler.summary()
    logger.info("Corporate actions loaded: %s (TWT49U) + %s (declarations)", ca_summary['twt49u_actions'], ca_summary['declaration_actions'])
    
    adjusted_prices = {}
    adjusted_count = 0
    for code, prices in all_prices.items():
        adjusted = ca_handler.backward_adjust_prices(prices, code)
        adjusted_prices[code] = adjusted
        # Count stocks that actually had adjustments applied
        if any(p.get("cumulative_factor", 1.0) != 1.0 for p in adjusted):
            adjusted_count += 1
    
    logger.info("Adjusted %d/%d stocks had corporate actions in range", adjusted_count, len(adjusted_prices))
    
    all_prices = adjusted_prices
    
    # Save merged history (with backward-adjusted prices)
    history_file = data_dir / "price_history.json"
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(all_prices, f, ensure_ascii=False)
    
    logger.info("Price history built for %d stocks", len(all_prices))
    logger.info("Saved to data/price_history.json (backward-adjusted)")
    logger.info("Note: Prices include 'adj_close' field for corporate-action-adjusted prices")
    
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
