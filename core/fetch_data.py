#!/usr/bin/env python3
"""
TWSE Data Fetcher — Batch fetch all daily data from TWSE Open API
Usage: python3 core/fetch_data.py [--date YYYY-MM-DD] [--verbose]
"""

import requests
import time
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

BASE = "https://openapi.twse.com.tw/v1"
RATE_LIMIT_DELAY = 0.4  # seconds between requests
TIMEOUT = 15
MAX_RETRIES = 3  # retries per endpoint
RETRY_BACKOFF = 2  # exponential backoff multiplier
BATCH_DELAY = 2.0  # delay between batches of 5 requests (rate limit protection)

# All endpoints to fetch
ENDPOINTS = {
    "daily": "/exchangeReport/STOCK_DAY_ALL",
    "pe": "/exchangeReport/BWIBBU_ALL",
    "margin": "/exchangeReport/MI_MARGN",
    "company": "/opendata/t187ap03_L",
    "revenue": "/opendata/t187ap05_L",
    "announce": "/opendata/t187ap04_L",
    "insider": "/opendata/t11sb10_q1",
    "pledge": "/opendata/t187ap09_L",
    "transfers": "/opendata/t187ap12_L",
    "penalties": "/opendata/t187ap22_L",
    "ctrl_change": "/opendata/t187ap24_L",
    "limits": "/exchangeReport/TWT84U",
    "halts": "/exchangeReport/TWTAWU",
    "margin_susp": "/exchangeReport/BFI84U",
    "sanctions": "/announcement/punish",
    "dividends": "/opendata/t187ap45_L",
    "major_sh": "/opendata/t187ap02_L",
    "holidays": "/holidaySchedule/holidaySchedule",
}

# Institutional flow needs date param
FLOW_ENDPOINT = "/fund/T86"


def get_last_trading_day():
    """Get last trading day (skip weekends)"""
    d = datetime.now() - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d


def fetch_endpoint(name, path, params=None, verbose=False):
    """Fetch a single endpoint with retry logic and exponential backoff"""
    url = f"{BASE}{path}"
    
    for attempt in range(MAX_RETRIES):
        try:
            if verbose:
                print(f"  → {name} ({url})")
            
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                content_type = r.headers.get('content-type', '')
                # Check if response is JSON
                if 'application/json' in content_type or r.text.strip().startswith('{') or r.text.strip().startswith('['):
                    try:
                        data = r.json()
                        if verbose:
                            count = len(data) if isinstance(data, list) else 'OK'
                            print(f"    ✓ {name}: {count} records")
                        return data
                    except json.JSONDecodeError:
                        if verbose:
                            print(f"    ⚠ {name}: JSON decode failed")
                        return []
                else:
                    # HTML response - likely auth issue or wrong params
                    if verbose:
                        print(f"    ⚠ {name}: Non-JSON response ({content_type})")
                    return []
            else:
                if verbose:
                    print(f"    ⚠ {name}: HTTP {r.status_code}")
                return []
        except requests.exceptions.ConnectionResetError as e:
            wait = RETRY_BACKOFF ** (attempt + 1)
            if verbose:
                print(f"    ✗ {name}: Connection reset (attempt {attempt+1}/{MAX_RETRIES}), retrying in {wait}s...")
            time.sleep(wait)
        except requests.exceptions.Timeout as e:
            wait = RETRY_BACKOFF ** (attempt + 1)
            if verbose:
                print(f"    ✗ {name}: Timeout (attempt {attempt+1}/{MAX_RETRIES}), retrying in {wait}s...")
            time.sleep(wait)
        except Exception as e:
            wait = RETRY_BACKOFF ** (attempt + 1)
            if verbose:
                print(f"    ✗ {name}: {e} (attempt {attempt+1}/{MAX_RETRIES}), retrying in {wait}s...")
            time.sleep(wait)
    
    if verbose:
        print(f"    ✗ {name}: All {MAX_RETRIES} retries exhausted")
    return []


def fetch_all(date_str=None, verbose=False):
    """Fetch all daily data and return as dict"""
    results = {}
    failed = []
    
    if date_str is None:
        last_day = get_last_trading_day()
        date_str = last_day.strftime("%Y-%m-%d")
        date_param = last_day.strftime("%Y%m%d")
    else:
        date_param = date_str.replace("-", "")
    
    if verbose:
        print(f"📡 Fetching TWSE data for {date_str}")
        print(f"   Rate limit: {RATE_LIMIT_DELAY}s between requests, {BATCH_DELAY}s between batches")
        print()
    
    # Fetch regular endpoints in batches with delays
    endpoint_list = list(ENDPOINTS.items())
    batch_size = 5
    for i in range(0, len(endpoint_list), batch_size):
        batch = endpoint_list[i:i+batch_size]
        for name, path in batch:
            results[name] = fetch_endpoint(name, path, verbose=verbose)
            time.sleep(RATE_LIMIT_DELAY)
        # Delay between batches to avoid rate limiting
        if i + batch_size < len(endpoint_list):
            time.sleep(BATCH_DELAY)
    
    # Fetch institutional flow with date param
    flow_params = {
        "response": "json",
        "date": date_param,
        "selectType": "ALLBUT0999"
    }
    results["flow"] = fetch_endpoint("flow", FLOW_ENDPOINT, params=flow_params, verbose=verbose)
    
    # Track failed endpoints (empty lists for critical endpoints)
    critical = ["daily", "pe", "company", "revenue"]
    for name in critical:
        if not results.get(name):
            failed.append(name)
    
    # Summary
    success_count = len(results) - len(failed)
    if verbose:
        print(f"\n✅ Fetched {success_count}/{len(ENDPOINTS)+1} endpoints")
        if failed:
            print(f"   ⚠ Critical failures: {', '.join(failed)}")
    
    return results


def save_results(results, date_str=None):
    """Save fetched data to data/ directory"""
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    # Save each dataset separately for efficiency
    for name, data in results.items():
        filepath = data_dir / f"{name}_{date_str}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    # Save metadata
    meta = {
        "date": date_str,
        "fetched_at": datetime.now().isoformat(),
        "endpoints": list(results.keys()),
        "record_counts": {k: len(v) if isinstance(v, list) else 'N/A' for k, v in results.items()}
    }
    meta_path = data_dir / f"fetch_meta_{date_str}.json"
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    
    return meta


def validate_data(results, verbose=False):
    """Validate that critical data was fetched successfully"""
    critical_min = {
        "daily": 500,      # Should have ~1300+ stocks
        "pe": 500,         # ~1000+
        "company": 500,   # ~1000+
        "revenue": 500,   # ~1000+
    }
    
    issues = []
    for name, minimum in critical_min.items():
        data = results.get(name, [])
        if not data or (isinstance(data, list) and len(data) < minimum):
            issues.append(f"{name}: expected >={minimum}, got {len(data) if isinstance(data, list) else 0}")
    
    if issues:
        if verbose:
            print(f"\n❌ Data validation FAILED:")
            for issue in issues:
                print(f"   - {issue}")
            print(f"   → Pipeline should NOT proceed with insufficient data")
        return False, issues
    
    if verbose:
        print(f"\n✅ Data validation PASSED — all critical endpoints have sufficient data")
    return True, []


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fetch TWSE daily data")
    parser.add_argument("--date", type=str, help="Date to fetch (YYYY-MM-DD), defaults to last trading day")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to disk")
    parser.add_argument("--validate-only", action="store_true", help="Only validate existing data, don't fetch")
    args = parser.parse_args()
    
    verbose = args.verbose or args.dry_run
    
    # Validate-only mode: check existing data files
    if args.validate_only:
        data_dir = Path(__file__).parent.parent / "data"
        date_str = args.date or datetime.now().strftime("%Y-%m-%d")
        
        results = {}
        for name in ["daily", "pe", "company", "revenue"]:
            filepath = data_dir / f"{name}_{date_str}.json"
            if filepath.exists():
                with open(filepath, 'r', encoding='utf-8') as f:
                    results[name] = json.load(f)
            else:
                results[name] = []
        
        ok, issues = validate_data(results, verbose=True)
        sys.exit(0 if ok else 1)
    
    results = fetch_all(date_str=args.date, verbose=verbose)
    
    # Validate before saving
    ok, issues = validate_data(results, verbose=verbose)
    if not ok:
        print(f"\n❌ Aborting save — data validation failed")
        sys.exit(1)
    
    if not args.dry_run:
        meta = save_results(results, date_str=args.date)
        if verbose:
            print(f"\n💾 Saved to data/ directory")
            print(f"   Records: {meta['record_counts']}")
    
    return results


if __name__ == "__main__":
    main()
