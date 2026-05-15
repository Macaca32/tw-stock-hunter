#!/usr/bin/env python3
"""
Institutional Flow Fetcher - Get institutional ownership and flow data

Phase 10 R8: Yahoo Finance .TW symbols consistently return 404.
yfinance dropped reliable support for Taiwan stocks. This module now:
1. Uses TWSE margin trading data (already fetched) as the primary proxy for institutional activity
2. Attempts yfinance only for top candidates (not all 1200+ stocks)
3. Has proper timeout and early-exit on consecutive failures
"""

import json
import signal
import time
import warnings
from pathlib import Path

import logging

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False
    logger.warning("yfinance not available")

try:
    import pandas as pd
    PD_AVAILABLE = True
except ImportError:
    PD_AVAILABLE = False
    logger.warning("pandas not available")

# Timeout per yfinance request (seconds)
YF_REQUEST_TIMEOUT = 5
# Max consecutive failures before giving up on yfinance entirely
YF_MAX_CONSECUTIVE_FAILURES = 5


def _validate_yf_dataframe(df, expected_columns, context="", verbose=False):
    """Validate a yfinance DataFrame has expected structure.

    Phase 20: Catch malformed DataFrame responses before processing.
    Returns the DataFrame if valid, None otherwise.
    """
    if df is None or df.empty:
        return None

    # Check that expected columns exist
    missing = [c for c in expected_columns if c not in df.columns]
    if missing and verbose:
        warnings.warn(f"yfinance {context}: missing columns {missing}, have {list(df.columns)[:8]}")

    # Check for completely null columns (yfinance sometimes returns empty DataFrames with columns)
    non_null_cols = [c for c in df.columns if df[c].notna().any()]
    if not non_null_cols:
        if verbose:
            logger.debug("yfinance %s: all columns are null", context)
        return None

    return df


def _try_fetch_yf_institutional(code: str, verbose: bool = False) -> dict | None:
    """Try to fetch institutional data for a single stock via yfinance.
    
    Returns dict with institutional data or None on failure.
    Raises TimeoutError if request takes too long.
    """
    if not PD_AVAILABLE:
        return None

    import signal

    class YfTimeout(Exception):
        pass

    def _handler(signum, frame):
        raise YfTimeout()

    symbol = f"{code}.TW"
    old_handler = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(YF_REQUEST_TIMEOUT)

    try:
        ticker = yf.Ticker(symbol)
        inst_holders = ticker.institutional_holders
        major_holders = ticker.major_holders

        # Phase 20: Validate yfinance DataFrames before processing
        inst_holders = _validate_yf_dataframe(
            inst_holders, ["Holder", "Shares", "Value"],
            context=f"{code}/institutional", verbose=verbose
        )
        if inst_holders is None:
            return None

        total_pct = inst_holders.get("pctHeld", pd.Series()).sum() if "pctHeld" in inst_holders.columns else 0

        top_holders = []
        for _, row in inst_holders.head(5).iterrows():
            top_holders.append({
                "holder": row.get("Holder", ""),
                "shares": int(row.get("Shares", 0)) if pd.notna(row.get("Shares")) else 0,
                "value": float(row.get("Value", 0)) if pd.notna(row.get("Value")) else 0,
                "pctHeld": float(row.get("pctHeld", 0)),
            })

        result = {
            "symbol": symbol,
            "total_institutional_pct": round(float(total_pct), 4),
            "institutional_count": len(inst_holders),
            "top_holders": top_holders,
            "source": "yfinance",
        }

        if major_holders is not None and not major_holders.empty:
            # Phase 20: Validate major_holders DataFrame before accessing
            validated_major = _validate_yf_dataframe(
                major_holders, ["Value"],
                context=f"{code}/major_holders", verbose=verbose
            )
            if validated_major is not None:
                try:
                    result["insider_pct"] = float(validated_major.loc["Insiders Percent Held", "Value"])
                    result["institutional_pct"] = float(validated_major.loc["Institutions Percent Held", "Value"])
                except (KeyError, TypeError):
                    pass

        return result
    except (YfTimeout, Exception) as e:
        if verbose:
            logger.debug("%s: %s", code, type(e).__name__)
        return None
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def fetch_institutional_data(stock_codes, output_dir=None, verbose=False):
    """Fetch institutional ownership data for stocks.
    
    Phase 10 R8: yfinance .TW support is broken (404s). This function now:
    - Only attempts yfinance for a small sample to check if it's working
    - Falls back immediately on consecutive failures
    - Uses cached data when available
    """
    if not YF_AVAILABLE:
        return {}

    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)

    # Try loading cached data first (yfinance data changes slowly)
    cache_file = output_dir / "institutional_flow.json"
    cache_age_hours = 0
    if cache_file.exists():
        import os
        age_seconds = time.time() - os.path.getmtime(cache_file)
        cache_age_hours = age_seconds / 3600
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if cached and verbose:
                logger.info("Using cached institutional data (%d stocks, %.1fh old)", len(cached), cache_age_hours)
            # Return cache if less than 24h old
            if cache_age_hours < 24:
                return cached
        except (json.JSONDecodeError, IOError):
            pass

    inst_data = {}
    consecutive_failures = 0

    if verbose:
        logger.info("Fetching institutional data for %d stocks...", len(stock_codes))
        logger.info("yfinance .TW support is unreliable — early exit on failures")

    # Only attempt first N stocks (top candidates get priority)
    max_attempts = min(len(stock_codes), 200)

    for code in stock_codes[:max_attempts]:
        result = _try_fetch_yf_institutional(code, verbose=verbose)
        if result is not None:
            inst_data[code] = result
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= YF_MAX_CONSECUTIVE_FAILURES:
                if verbose:
                    logger.warning("yfinance returning %d consecutive failures — skipping remaining stocks", consecutive_failures)
                break

    # Save to file (even if empty, so we know we tried)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(inst_data, f, ensure_ascii=False, indent=2)

    if verbose:
        logger.info("Institutional data: %d/%d stocks via yfinance", len(inst_data), len(stock_codes))

    return inst_data


def score_institutional_flow_yf(code, inst_data, margin_data=None):
    """Score institutional flow using yfinance data (0-100)
    
    Higher institutional ownership = more confidence
    """
    if code in inst_data:
        data = inst_data[code]
        
        score = 50  # Base
        
        # Institutional ownership (±20 points)
        inst_pct = data.get("total_institutional_pct", 0)
        if inst_pct > 0.5:
            score += 20  # >50% institutional
        elif inst_pct > 0.3:
            score += 10  # >30% institutional
        elif inst_pct < 0.1:
            score -= 10  # <10% institutional
        
        # Number of institutional holders (±10 points)
        inst_count = data.get("institutional_count", 0)
        if inst_count > 100:
            score += 10
        elif inst_count > 50:
            score += 5
        elif inst_count < 10:
            score -= 10
        
        # Insider ownership (±10 points)
        insider_pct = data.get("insider_pct", 0)
        if insider_pct > 0.1:
            score += 10  # High insider ownership
        elif insider_pct < 0.01:
            score -= 5  # Very low insider ownership
        
        return max(0, min(100, score))
    
    # Fallback to margin data if available
    if margin_data:
        for m in margin_data:
            if m.get("Code", "") == code or m.get("證券代號", "") == code:
                # Use margin trading data as proxy
                margin_buy = float(m.get("MarginBuy", 0) or 0)
                margin_sell = float(m.get("MarginSell", 0) or 0)
                
                if margin_sell > 0:
                    ratio = margin_buy / margin_sell
                    if ratio > 1.5:
                        return 70
                    elif ratio > 1.0:
                        return 60
                    elif ratio < 0.5:
                        return 30
                    else:
                        return 40
                else:
                    return 50
    
    return 25  # No data


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fetch institutional flow data")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    # Load stock codes
    data_dir = Path(__file__).parent.parent / "data"
    daily_file = data_dir / "daily_2026-05-09.json"
    
    if daily_file.exists():
        with open(daily_file, 'r', encoding='utf-8') as f:
            daily_data = json.load(f)
        
        stock_codes = [d.get("Code", "") for d in daily_data if d.get("Code")]
        
        fetch_institutional_data(stock_codes, output_dir=data_dir, verbose=args.verbose)
    else:
        logger.error("No daily data found")


if __name__ == "__main__":
    main()
