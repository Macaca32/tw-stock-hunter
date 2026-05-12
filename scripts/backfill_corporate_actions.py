#!/usr/bin/env python3
"""
Backfill Corporate Actions (2020-2025) from yfinance

TWSE TWT49U API returns HTML, not JSON. The current corporate action handler
uses estimated ex-dates (+7 days from shareholder meeting), which is inaccurate
for historical data spanning 2020-2025.

yfinance has actual ex-dividend dates and amounts going back years for .TW stocks.
This script fetches those actions and merges them into the existing cache format
so CorporateActionHandler can use them alongside declaration data.

Usage:
    python3 scripts/backfill_corporate_actions.py [--codes CODES...] [--years 2020,2021,...] [--dry-run]
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# --------------------------------------------------------------------------- #
#  yfinance import with graceful fallback
# --------------------------------------------------------------------------- #
try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip install yfinance")
    sys.exit(1)


def get_stock_codes(data_dir: Path):
    """Get all stock codes from daily data files."""
    codes = set()
    for f in sorted(data_dir.glob("daily_*.json"))[-3:]:  # last 3 days
        try:
            with open(f, "r", encoding="utf-8") as fh:
                records = json.load(fh)
            for r in records:
                code = str(r.get("證券代號", "") or r.get("公司代號", ""))
                if code and not code.endswith(("B", "R")):
                    codes.add(code.zfill(4))
        except (json.JSONDecodeError, IOError):
            continue
    return sorted(codes)


def fetch_actions_yfinance(ticker_symbol: str, start_year: int = 2020, end_year: int = None) -> list:
    """Fetch corporate actions for a single ticker from yfinance.
    
    Returns list of dicts with date, cash_div (cash dividend per share), stock_split ratio.
    Each record represents an ex-dividend date.
    """
    if end_year is None:
        end_year = datetime.now().year + 1
    
    try:
        ticker = yf.Ticker(ticker_symbol)
        actions = ticker.actions
        
        if actions is None or len(actions) == 0:
            return []
        
        result = []
        # Filter to date range and convert to our format
        for _, row in actions.iterrows():
            dt = str(row.name).split(" ")[0]  # YYYY-MM-DD
            year = int(dt[:4])
            
            if year < start_year or year > end_year:
                continue
            
            cash_div = float(row.get("Dividends", 0)) or 0.0
            stock_split = float(row.get("Stock Splits", 0)) or 0.0
            
            # Only keep records with actual dividends or splits
            if cash_div > 0 or stock_split > 0:
                result.append({
                    "date": dt,
                    "cash_div": round(cash_div, 4),
                    "stock_split": round(stock_split, 4),
                })
        
        return sorted(result, key=lambda x: x["date"])
    except Exception as e:
        print(f"  ⚠ {ticker_symbol}: yfinance error — {e}", file=sys.stderr)
        return []


def backfill(codes=None, years=None, dry_run=False):
    """Run the backfill process."""
    data_dir = Path(__file__).parent.parent / "data"
    
    if codes is None:
        codes = get_stock_codes(data_dir)
    
    year_list = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
    if years:
        year_list = sorted([int(y) for y in years])
    
    start_year = min(year_list)
    end_year = max(year_list)
    
    cache_file = data_dir / "twt49u_ex_dividend.json"
    
    # Load existing cache (TWT49U from API if available, or previous backfill)
    existing = {}
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    
    print(f"Phase 13: Corporate Action Backfill ({start_year}-{end_year})")
    print(f"Stocks to process: {len(codes)}")
    print(f"Existing cache: {sum(len(v) for v in existing.values())} actions for {len(existing)} stocks")
    
    # Track stats
    total_stocks = 0
    total_actions = 0
    new_actions = 0
    skipped = 0
    
    for i, code in enumerate(codes):
        symbol = f"{code}.TW"
        
        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(codes)} stocks... ({total_stocks} with data, {total_actions} actions)")
        
        # Rate limit yfinance
        time.sleep(0.3)
        
        actions = fetch_actions_yfinance(symbol, start_year=start_year, end_year=end_year)
        
        if not actions:
            skipped += 1
            continue
        
        total_stocks += 1
        stock_total = len(actions)
        total_actions += stock_total
        
        # Convert to cache format (TWT49U-compatible dict)
        formatted = []
        for a in actions:
            record = {
                "date": a["date"],
                "cash_div": a["cash_div"],
                "stock_div": 0.0,  # yfinance doesn't separate stock dividends vs cash; stock_split is different
                "ref_price": 0.0,
                "rights_value": 0.0,
                "source": "yfinance_backfill",
            }
            
            # If there's a stock split > 1, it means shares increased
            # e.g., 2.0 = 2-for-1 (doubles shares). Convert to Taiwan convention.
            if a["stock_split"] > 1:
                # stock_split ratio: 2.0 means each share becomes 2 shares
                # In Taiwan, stock_div of 10 means 1 new share per 10 old shares = 1.1x total
                # We need to convert: split_ratio → stock_div equivalent
                # stock_div / 10 = (split_ratio - 1), so stock_div = (split_ratio - 1) * 10
                record["stock_div"] = round((a["stock_split"] - 1.0) * 10, 4)
            
            formatted.append(record)
        
        if not dry_run:
            existing[code] = formatted
    
    print(f"\nBackfill complete:")
    print(f"  Stocks with data: {total_stocks}/{len(codes)}")
    print(f"  Total actions collected: {total_actions}")
    print(f"  Skipped (no actions): {skipped}")
    
    if not dry_run and existing:
        # Atomic write
        tmp_file = cache_file.with_suffix(".tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        tmp_file.rename(cache_file)
        print(f"  Saved to {cache_file}")
    
    return total_stocks, total_actions


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Backfill corporate actions from yfinance")
    parser.add_argument("--codes", nargs="+", help="Stock codes to process (default: all)")
    parser.add_argument("--years", default=None, help="Comma-separated years to include (e.g., 2020,2021)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write cache file")
    
    args = parser.parse_args()
    
    codes_list = None
    if args.codes:
        codes_list = [c.zfill(4) for c in args.codes]
    
    years = None
    if args.years:
        years = args.years.split(",")
    
    total_stocks, total_actions = backfill(codes=codes_list, years=years, dry_run=args.dry_run)
    
    print(f"\n{'='*60}")
    print(f"Backfill summary: {total_stocks} stocks, {total_actions} corporate actions")
    if args.dry_run:
        print("(Dry run — no cache written)")


if __name__ == "__main__":
    main()
