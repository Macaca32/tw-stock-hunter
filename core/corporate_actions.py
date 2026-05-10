#!/usr/bin/env python3
"""
Corporate Action Handler - Ex-dividend awareness for Taiwan stocks

FIX v3: CRITICAL - Corporate action handling prevents false signals.
Taiwan has massive dividend yields (4-8% for many stocks).
On ex-date, the stock drops 4-8% by market mechanics.
Without this, the system will:
- Register a 4-8% "crash" → trigger bear regime signals
- Hit ATR stop on the gap down
- Log a "losing trade" that was actually a dividend capture

Data sources:
- dividends_YYYY-MM-DD.json (TWSE dividend declarations)
- price_history.json (for ex-date estimation)

Usage:
    from corporate_actions import CorporateActionHandler
    handler = CorporateActionHandler()
    handler.is_ex_dividend_date("2330", "2026-07-20")
"""

import json
from datetime import datetime, timedelta
from pathlib import Path


class CorporateActionHandler:
    """Handles corporate actions (ex-dividend dates, stock splits, etc.)"""
    
    def __init__(self, data_dir=None):
        self.data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent / "data"
        self.dividend_data = self._load_dividend_data()
        self._ex_div_cache = {}
    
    def _load_dividend_data(self):
        """Load dividend data from TWSE files.
        
        Returns dict: {stock_code: [{"date": ..., "cash_div": ..., "stock_div": ...}, ...]}
        """
        result = {}
        
        # Find the latest dividend file
        dividend_files = sorted(self.data_dir.glob("dividends_*.json"))
        if not dividend_files:
            return result
        
        latest = dividend_files[-1]
        try:
            with open(latest, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            return result
        
        for item in data:
            code = item.get("公司代號", "")
            if not code:
                continue
            
            # Get cash dividend per share
            cash_div = 0.0
            for key in ["股東配發-盈餘分配之現金股利(元/股)", 
                       "股東配發-法定盈餘公積發放之現金(元/股)",
                       "股東配發-資本公積發放之現金(元/股)"]:
                val = item.get(key, 0)
                try:
                    cash_div += float(val)
                except (ValueError, TypeError):
                    pass
            
            if cash_div <= 0:
                continue
            
            # Get stock dividend
            stock_div = 0.0
            for key in ["股東配發-盈餘轉增資配股(元/股)",
                       "股東配發-法定盈餘公積轉增資配股(元/股)",
                       "股東配發-資本公積轉增資配股(元/股)"]:
                val = item.get(key, 0)
                try:
                    stock_div += float(val)
                except (ValueError, TypeError):
                    pass
            
            # Estimate ex-dividend date (typically ~1 week after 股東會日期)
            # 股東會日期 is in ROC format (e.g., "1150522" = 2026-05-22)
            sh_date_str = item.get("股東會日期", "")
            ex_date = self._estimate_ex_date(sh_date_str)
            
            if ex_date:
                if code not in result:
                    result[code] = []
                result[code].append({
                    "date": ex_date,
                    "cash_div": cash_div,
                    "stock_div": stock_div,
                    "total_div": cash_div + stock_div,
                    "status": item.get("決議（擬議）進度", "unknown")
                })
        
        return result
    
    def _estimate_ex_date(self, sh_date_str):
        """Estimate ex-dividend date from shareholder meeting date.
        
        Ex-date is typically 1-2 weeks after the shareholder meeting.
        ROC date format: "1150522" = 2026-05-22
        """
        if not sh_date_str or len(sh_date_str) < 7:
            return None
        
        try:
            # ROC year = year - 1911
            roc_year = int(sh_date_str[:3])
            month = int(sh_date_str[3:5])
            day = int(sh_date_str[5:7])
            
            year = roc_year + 1911
            sh_date = datetime(year, month, day)
            
            # Ex-date is typically 7-14 days after shareholder meeting
            # Use 10 days as a reasonable estimate
            ex_date = sh_date + timedelta(days=10)
            return ex_date.strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            return None
    
    def is_ex_dividend_date(self, stock_code, date_str):
        """Check if a stock has an ex-dividend date on the given date.
        
        Returns: True if ex-dividend, False otherwise
        """
        cache_key = (stock_code, date_str)
        if cache_key in self._ex_div_cache:
            return self._ex_div_cache[cache_key]
        
        actions = self.dividend_data.get(stock_code, [])
        for action in actions:
            if action["date"] == date_str:
                self._ex_div_cache[cache_key] = True
                return True
        
        self._ex_div_cache[cache_key] = False
        return False
    
    def get_ex_dividend_amount(self, stock_code, date_str):
        """Get the cash dividend amount for a stock on a given date.
        
        Returns: dividend amount (float) or None
        """
        actions = self.dividend_data.get(stock_code, [])
        for action in actions:
            if action["date"] == date_str:
                return action.get("cash_div", 0)
        return None
    
    def get_upcoming_ex_dividends(self, stock_code, days_ahead=30):
        """Get upcoming ex-dividend dates for a stock.
        
        Returns: list of upcoming ex-dividend actions
        """
        actions = self.dividend_data.get(stock_code, [])
        today = datetime.now().strftime("%Y-%m-%d")
        cutoff = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        upcoming = []
        for action in actions:
            action_date = action.get("date", "")
            if today <= action_date <= cutoff:
                upcoming.append(action)
        
        return upcoming
    
    def should_skip_stop_check(self, stock_code, date_str):
        """Check if stop loss should be skipped due to ex-dividend date.
        
        FIX v3: On ex-dividend dates, the price drop is mechanical,
        not a market signal. Skip stop checks to avoid false exits.
        
        Returns: True if stop check should be skipped
        """
        return self.is_ex_dividend_date(stock_code, date_str)
    
    def adjust_price_for_dividend(self, price, stock_code, date_str):
        """Adjust price for ex-dividend drop.
        
        On ex-date, the stock price drops by the dividend amount.
        This adjustment prevents false crash signals.
        
        Returns: adjusted price (or original if no ex-dividend)
        """
        div_amount = self.get_ex_dividend_amount(stock_code, date_str)
        
        if div_amount is not None and div_amount > 0:
            return price + div_amount  # Add back the dividend drop
        
        return price
    
    def get_dividend_yield_impact(self, stock_code, current_price):
        """Estimate dividend yield impact for a stock.
        
        Returns: estimated annual dividend yield as percentage
        """
        upcoming = self.get_upcoming_ex_dividends(stock_code, days_ahead=365)
        
        if not upcoming or current_price <= 0:
            return 0.0
        
        total_div = sum(a.get("cash_div", 0) for a in upcoming)
        yield_pct = (total_div / current_price) * 100
        
        return round(yield_pct, 2)
    
    def get_ex_dividend_dates_for_stock(self, stock_code):
        """Get all ex-dividend dates for a stock.
        
        Returns: list of dates
        """
        actions = self.dividend_data.get(stock_code, [])
        return [a["date"] for a in actions if a.get("date")]
    
    def get_all_ex_dividend_dates(self):
        """Get all ex-dividend dates across all stocks.
        
        Returns: dict {date: [stock_codes]}
        """
        result = {}
        for code, actions in self.dividend_data.items():
            for action in actions:
                date = action.get("date")
                if date:
                    if date not in result:
                        result[date] = []
                    result[date].append(code)
        return result


def main():
    """Test corporate action handler."""
    handler = CorporateActionHandler()
    
    print(f"Loaded dividend data for {len(handler.dividend_data)} stocks")
    
    # Test with a known stock
    test_code = "2330"
    if test_code in handler.dividend_data:
        actions = handler.dividend_data[test_code]
        print(f"\n{test_code} dividend actions:")
        for a in actions:
            print(f"  {a['date']}: cash={a['cash_div']}, stock={a['stock_div']}")
    
    # Test ex-dividend check
    test_date = "2026-07-20"
    is_ex = handler.is_ex_dividend_date(test_code, test_date)
    print(f"\nIs {test_code} ex-dividend on {test_date}? {is_ex}")
    
    return handler


if __name__ == "__main__":
    main()
