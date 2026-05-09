#!/usr/bin/env python3
"""
Institutional Flow Fetcher - Get institutional ownership and flow data

Uses yfinance as primary source since TWSE Open API flow endpoints are broken.
"""

import json
import time
from pathlib import Path

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False
    print("⚠️  yfinance not available")


def fetch_institutional_data(stock_codes, output_dir=None, verbose=False):
    """Fetch institutional ownership data for stocks"""
    if not YF_AVAILABLE:
        return {}
    
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)
    
    inst_data = {}
    
    if verbose:
        print(f"  → Fetching institutional data for {len(stock_codes)} stocks...")
    
    # Fetch in batches
    batch_size = 20
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i+batch_size]
        
        for code in batch:
            symbol = f"{code}.TW"
            try:
                ticker = yf.Ticker(symbol)
                
                # Get institutional holders
                inst_holders = ticker.institutional_holders
                major_holders = ticker.major_holders
                
                if inst_holders is not None and not inst_holders.empty:
                    # Calculate total institutional ownership
                    total_pct = inst_holders['pctHeld'].sum() if 'pctHeld' in inst_holders.columns else 0
                    
                    # Get top holders
                    top_holders = []
                    for _, row in inst_holders.head(5).iterrows():
                        top_holders.append({
                            "holder": row.get("Holder", ""),
                            "shares": row.get("Shares", 0),
                            "value": row.get("Value", 0),
                            "pctHeld": row.get("pctHeld", 0)
                        })
                    
                    inst_data[code] = {
                        "symbol": symbol,
                        "total_institutional_pct": round(total_pct, 4),
                        "institutional_count": len(inst_holders),
                        "top_holders": top_holders,
                        "insider_pct": 0,
                        "institutional_float_pct": 0
                    }
                    
                    # Get major holders if available
                    if major_holders is not None:
                        try:
                            inst_data[code]["insider_pct"] = float(major_holders.loc["Insiders Percent Held", "Value"]) if "Insiders Percent Held" in major_holders.index else 0
                            inst_data[code]["institutional_pct"] = float(major_holders.loc["Institutions Percent Held", "Value"]) if "Institutions Percent Held" in major_holders.index else 0
                        except:
                            pass
            
            except Exception as e:
                if verbose:
                    print(f"    ✗ {code}: {e}")
            
            time.sleep(0.5)  # Rate limit
        
        if verbose:
            print(f"    ✓ Batch {i//batch_size + 1} done")
    
    # Save to file
    filepath = output_dir / "institutional_flow.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(inst_data, f, ensure_ascii=False, indent=2)
    
    if verbose:
        print(f"  ✓ Saved institutional data for {len(inst_data)} stocks")
    
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
        print("❌ No daily data found")


if __name__ == "__main__":
    main()
