#!/usr/bin/env python3
"""
Sector Classification for Taiwan stocks

FIX v3: Use actual TWSE industry codes from company data, not hardcoded ranges.
The previous sector mapping was garbage — most 3xxx-8xxx codes are NOT
"hotels" or "education". They're mostly electronics/semiconductors.

TWSE 產業別 codes map to:
  01-03: 水泥/玻璃/化學 (Cement/Glass/Chemicals)
  04-06: 塑膠/橡膠/紙類 (Plastics/Rubber/Paper)
  07-09: 紡織/成衣/食品 (Textiles/Apparel/Food)
  10-12: 飲料/皮革/木材 (Beverages/Leather/Wood)
  13-15: 電器/電子/電機 (Appliances/Electronics/Electrical)
  16-18: 機械/汽車/工具 (Machinery/Auto/Tools)
  19-21: 鋼鐵/金屬/造船 (Steel/Metals/Shipbuilding)
  22-24: 航運/半導體/電子零組件 (Shipping/Semiconductor/Electronics)
  25-27: 光電/通訊/電腦 (Optoelectronics/Communications/Computers)
  28-30: 零組件/被動元件/被動元件 (Components/Passive)
  31-33: 金融/保險/證券 (Finance/Insurance/Securities)
  34-36: 營建/營造/工程 (Construction/Engineering)
  37-39: 貿易/百貨/觀光 (Trade/Retail/Tourism)
  90+: 櫃買 (OTC/TPEx)
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# TWSE industry code to sector mapping
# Phase 5: Refined 12-15 sub-sectors for better diversification tracking.
# Previous mapping grouped all tech (22-30) into one bucket = 60% of market.
# Now split into semiconductors, electronics, communications, etc.
INDUSTRY_TO_SECTOR = {
    # Materials (unchanged)
    "01": "materials", "02": "materials", "03": "materials",
    "04": "materials", "05": "materials", "06": "materials",
    # Consumer (unchanged)
    "07": "consumer", "08": "consumer", "09": "consumer",
    "10": "consumer", "11": "consumer", "12": "consumer",
    # Industrial (unchanged)
    "13": "industrial", "14": "industrial", "15": "industrial",
    "16": "industrial", "17": "industrial", "18": "industrial",
    # Metals/Heavy (unchanged)
    "19": "metals", "20": "metals", "21": "metals",
    # Technology — Phase 5: Split into 6 sub-sectors
    "22": "semiconductor",    # 半導體
    "23": "semiconductor",    # 電子零組件 (mostly semiconductor equipment)
    "24": "electronics",      # 電機 (electrical equipment)
    "25": "optoelectronics",  # 光電 (LED, solar, displays)
    "26": "communications",   # 通訊 (networking, telecom)
    "27": "computers",        # 電腦 (PC, servers, peripherals)
    "28": "components",       # 零組件 (passive components)
    "29": "components",       # 被動元件
    "30": "components",       # 被動元件 (duplicate code)
    # Financial (unchanged)
    "31": "financial", "32": "financial", "33": "financial",
    # Construction (unchanged)
    "34": "construction", "35": "construction", "36": "construction",
    # Services (unchanged)
    "37": "services", "38": "services", "39": "services",
    # OTC/TPEx
    "90": "tpex", "91": "tpex", "92": "tpex", "93": "tpex",
    "94": "tpex", "95": "tpex", "96": "tpex", "97": "tpex",
    "98": "tpex", "99": "tpex",
}


def load_sector_mapping(data_dir=None):
    """Load sector mapping from company data.
    
    Returns dict: {stock_code: sector_name}
    """
    data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent / "data"
    result = {}
    
    # Find the latest company data file
    company_files = sorted(data_dir.glob("company_*.json"))
    if not company_files:
        return result
    
    latest = company_files[-1]
    try:
        with open(latest, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return result
    
    for item in data:
        code = item.get("公司代號", "")
        industry = item.get("產業別", "")
        
        if code and industry:
            sector = INDUSTRY_TO_SECTOR.get(industry, "other")
            result[code] = sector
    
    return result


def get_sector(stock_code, sector_map=None):
    """Get sector for a stock code.
    
    Args:
        stock_code: Stock code (e.g., "2330")
        sector_map: Pre-loaded sector mapping dict. If None, loads from file.
    
    Returns:
        Sector name string (e.g., "technology", "financial", "materials")
    """
    if sector_map is None:
        sector_map = load_sector_mapping()
    
    return sector_map.get(stock_code, "other")


def get_sector_summary(sector_map=None):
    """Get summary of sector distribution.
    
    Returns dict: {sector: count}
    """
    if sector_map is None:
        sector_map = load_sector_mapping()
    
    from collections import Counter
    return dict(Counter(sector_map.values()).most_common())


def calc_sector_correlation(prices, sector_map=None, lookback=60):
    """Calculate sector correlation matrix.
    
    Phase 5: Correlation matrix helps detect when multiple picks
    are effectively the same bet (e.g., semiconductor + electronics).
    
    Args:
        prices: Dict {code: [{date, close, ...}, ...]}
        sector_map: Dict {code: sector_name}
        lookback: Days of returns to use for correlation
    
    Returns:
        Dict {sector_a: {sector_b: correlation}}
    """
    if sector_map is None:
        sector_map = load_sector_mapping()
    
    # Calculate sector returns (average of constituent stocks)
    sector_returns = {}
    
    for code, history in prices.items():
        sector = sector_map.get(code, "other")
        if len(history) < lookback:
            continue
        
        # Get daily returns
        returns = []
        for i in range(1, min(lookback, len(history))):
            prev = history[i-1].get("adj_close", history[i-1].get("close", 0))
            curr = history[i].get("adj_close", history[i].get("close", 0))
            if prev > 0:
                returns.append((curr - prev) / prev)
        
        if not returns:
            continue
        
        if sector not in sector_returns:
            sector_returns[sector] = []
        sector_returns[sector].append(returns)
    
    # Average returns per sector
    sector_avg_returns = {}
    for sector, return_lists in sector_returns.items():
        if not return_lists:
            continue
        # Align by length
        min_len = min(len(r) for r in return_lists)
        if min_len < 10:
            continue
        avg = [sum(r[:min_len]) / len(r[:min_len]) for r in return_lists]
        sector_avg_returns[sector] = avg
    
    # Calculate correlation matrix
    sectors = list(sector_avg_returns.keys())
    matrix = {}
    
    for a in sectors:
        matrix[a] = {}
        for b in sectors:
            if a == b:
                matrix[a][b] = 1.0
                continue
            
            # Pearson correlation
            ra = sector_avg_returns[a]
            rb = sector_avg_returns.get(b, [])
            
            if not rb or len(ra) < 10 or len(rb) < 10:
                matrix[a][b] = 0.0
                continue
            
            min_len = min(len(ra), len(rb))
            mean_a = sum(ra[:min_len]) / min_len
            mean_b = sum(rb[:min_len]) / min_len
            
            cov = sum((ra[i] - mean_a) * (rb[i] - mean_b) for i in range(min_len)) / min_len
            std_a = sum((x - mean_a) ** 2 for x in ra[:min_len]) ** 0.5 / min_len ** 0.5
            std_b = sum((x - mean_b) ** 2 for x in rb[:min_len]) ** 0.5 / min_len ** 0.5
            
            if std_a > 0 and std_b > 0:
                matrix[a][b] = round(cov / (std_a * std_b), 3)
            else:
                matrix[a][b] = 0.0
    
    return matrix


def check_sector_concentration(candidates, sector_map=None, max_sector_pct=0.25):
    """Check if candidates are too concentrated in any single sector.
    
    Phase 5: Prevent over-concentration in correlated sectors.
    
    Args:
        candidates: List of candidate dicts with 'code' field
        sector_map: Dict {code: sector_name}
        max_sector_pct: Max fraction of candidates from one sector
    
    Returns:
        (ok, details) - ok is bool, details has sector breakdown
    """
    if sector_map is None:
        sector_map = load_sector_mapping()
    
    if not candidates:
        return True, {}
    
    from collections import Counter
    sector_counts = Counter()
    
    for cand in candidates:
        code = cand.get("code", "")
        sector = sector_map.get(code, "other")
        sector_counts[sector] += 1
    
    total = len(candidates)
    details = {sector: round(count / total, 2) for sector, count in sector_counts.items()}
    
    # Check concentration
    max_sector = max(sector_counts.values()) if sector_counts else 0
    max_pct = max_sector / total
    
    ok = max_pct <= max_sector_pct
    
    return ok, details


def main():
    """Test sector mapping."""
    sector_map = load_sector_mapping()
    
    logger.info("Loaded sector mapping for %d stocks", len(sector_map))
    logger.info("Sector distribution:")
    summary = get_sector_summary(sector_map)
    for sector, count in summary.items():
        logger.info("  %s: %d", sector, count)
    
    # Test specific stocks
    test_stocks = ["2330", "2317", "2881", "2882", "2301", "2454"]
    logger.info("Test stocks:")
    for code in test_stocks:
        sector = get_sector(code, sector_map)
        logger.info("  %s: %s", code, sector)
    
    return sector_map


if __name__ == "__main__":
    main()
