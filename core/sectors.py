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
from pathlib import Path

# TWSE industry code to sector mapping
# Grouped for position concentration limits
INDUSTRY_TO_SECTOR = {
    # Materials
    "01": "materials", "02": "materials", "03": "materials",
    "04": "materials", "05": "materials", "06": "materials",
    # Consumer
    "07": "consumer", "08": "consumer", "09": "consumer",
    "10": "consumer", "11": "consumer", "12": "consumer",
    # Industrial
    "13": "industrial", "14": "industrial", "15": "industrial",
    "16": "industrial", "17": "industrial", "18": "industrial",
    # Metals/Heavy
    "19": "metals", "20": "metals", "21": "metals",
    # Technology (largest group - semiconductors + electronics)
    "22": "technology", "23": "technology", "24": "technology",
    "25": "technology", "26": "technology", "27": "technology",
    "28": "technology", "29": "technology", "30": "technology",
    # Financial
    "31": "financial", "32": "financial", "33": "financial",
    # Construction
    "34": "construction", "35": "construction", "36": "construction",
    # Services
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


def main():
    """Test sector mapping."""
    sector_map = load_sector_mapping()
    
    print(f"Loaded sector mapping for {len(sector_map)} stocks")
    print("\nSector distribution:")
    summary = get_sector_summary(sector_map)
    for sector, count in summary.items():
        print(f"  {sector}: {count}")
    
    # Test specific stocks
    test_stocks = ["2330", "2317", "2881", "2882", "2301", "2454"]
    print("\nTest stocks:")
    for code in test_stocks:
        sector = get_sector(code, sector_map)
        print(f"  {code}: {sector}")
    
    return sector_map


if __name__ == "__main__":
    main()
