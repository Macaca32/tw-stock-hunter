#!/usr/bin/env python3
"""
Corporate Action Handler — Backward-Adjustment Engine for Taiwan Stocks

Phase 2: Full backward-adjustment engine using TWSE /TWT49U API.

Taiwan stocks have massive dividend yields (4-8% for many stocks).
On ex-date, the stock drops 4-8% by market mechanics. Without backward
adjustment, the system will:
- Register a 4-8% "crash" → trigger bear regime signals
- Hit ATR stop on the gap down
- Log a "losing trade" that was actually a dividend capture
- Distort all SMA/momentum calculations on unadjusted prices

Data sources (in priority order):
1. TWSE /TWT49U API — actual ex-dividend/ex-rights dates
2. dividends_YYYY-MM-DD.json (t187ap45_L) — dividend amounts
3. yfinance adjusted close — fallback for backward adjustment

Usage:
    from corporate_actions import CorporateActionHandler
    handler = CorporateActionHandler()
    handler.is_ex_dividend_date("2330", "2026-07-20")
    adjusted_prices = handler.backward_adjust_prices(prices, "2330")
"""

import json
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any


# TWSE Open API base
TWSE_BASE = "https://openapi.twse.com.tw/v1"
TWSE_TIMEOUT = 15
TWSE_RATE_LIMIT = 0.3  # seconds between requests


def roc_date_to_iso(roc_date_str: str) -> Optional[str]:
    """Convert ROC date string (e.g., '1150522') to ISO format (e.g., '2026-05-22').
    
    ROC year = year - 1911, so 115 = 2026.
    """
    if not roc_date_str or len(roc_date_str) < 7:
        return None
    try:
        roc_year = int(roc_date_str[:3])
        month = int(roc_date_str[3:5])
        day = int(roc_date_str[5:7])
        year = roc_year + 1911
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        return None


def iso_to_roc_date(iso_date_str: str) -> Optional[str]:
    """Convert ISO date string to ROC date string."""
    if not iso_date_str:
        return None
    try:
        dt = datetime.strptime(iso_date_str, "%Y-%m-%d")
        return f"{dt.year - 1911:03d}{dt.month:02d}{dt.day:02d}"
    except ValueError:
        return None


class CorporateActionHandler:
    """Handles corporate actions with backward price adjustment.
    
    Supports:
    - Cash dividends (現金股利)
    - Stock dividends (股票股利 / 配股)
    - Stock splits (拆股) — inferred from stock dividend ratios
    """

    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent / "data"
        self.dividend_data: Dict[str, List[dict]] = {}
        self.ex_dividend_data: Dict[str, List[dict]] = {}  # From TWT49U
        self._ex_div_cache: Dict[tuple, bool] = {}
        self._loaded = False

        # Phase 15: Holiday calendar for ex-date validation
        self.holiday_calendar = None
        try:
            from holiday_calendar import HolidayCalendar
            self.holiday_calendar = HolidayCalendar(str(self.data_dir))
        except ImportError:
            pass

        self._load_all_data()

    # ------------------------------------------------------------------ #
    #  Data loading
    # ------------------------------------------------------------------ #

    def _load_all_data(self):
        """Load all corporate action data sources."""
        self._load_dividend_declarations()
        self._load_twt49u_ex_dividend()
        self._loaded = True

    def _load_dividend_declarations(self):
        """Load dividend declaration data from TWSE t187ap45_L files.
        
        Returns dict: {stock_code: [{"date": ..., "cash_div": ..., "stock_div": ...}, ...]}
        """
        result: Dict[str, List[dict]] = {}

        dividend_files = sorted(self.data_dir.glob("dividends_*.json"))
        if not dividend_files:
            return

        # Load ALL dividend files (not just latest) for historical coverage
        for dividend_file in dividend_files:
            try:
                with open(dividend_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError):
                continue

            for item in data:
                code = item.get("公司代號", "")
                if not code:
                    continue

                # Skip special shares (e.g., '1101B')
                if code.endswith("B") or code.endswith("R"):
                    continue

                # Cash dividend per share (sum all sources)
                cash_div = 0.0
                for key in [
                    "股東配發-盈餘分配之現金股利(元/股)",
                    "股東配發-法定盈餘公積發放之現金(元/股)",
                    "股東配發-資本公積發放之現金(元/股)",
                ]:
                    val = item.get(key, 0)
                    try:
                        cash_div += float(val)
                    except (ValueError, TypeError):
                        pass

                # Stock dividend per share (sum all sources)
                stock_div = 0.0
                for key in [
                    "股東配發-盈餘轉增資配股(元/股)",
                    "股東配發-法定盈餘公積轉增資配股(元/股)",
                    "股東配發-資本公積轉增資配股(元/股)",
                ]:
                    val = item.get(key, 0)
                    try:
                        stock_div += float(val)
                    except (ValueError, TypeError):
                        pass

                # Skip if no dividend at all
                if cash_div <= 0 and stock_div <= 0:
                    continue

                # Estimate ex-dividend date from shareholder meeting date
                # Taiwan convention: ex-date is typically 1-3 days BEFORE payment date
                # Payment date is typically 1-2 weeks after shareholder meeting
                sh_date_str = item.get("股東會日期", "")
                ex_date = self._estimate_ex_date(sh_date_str)

                if ex_date:
                    if code not in result:
                        result[code] = []
                    # Avoid duplicates
                    existing_dates = {a["date"] for a in result[code]}
                    if ex_date not in existing_dates:
                        result[code].append({
                            "date": ex_date,
                            "cash_div": round(cash_div, 4),
                            "stock_div": round(stock_div, 4),
                            "source": "dividend_declaration",
                            "status": item.get("決議（擬議）進度", "unknown"),
                            "dividend_year": item.get("股利年度", ""),
                        })

        self.dividend_data = result

    def _load_twt49u_ex_dividend(self):
        """Load actual ex-dividend/ex-rights data from TWSE /TWT49U API.
        
        The /TWT49U endpoint provides actual 除權除息 (ex-rights/ex-dividend) data
        with precise ex-dates, cash dividends, stock dividends, and reference prices.
        
        Falls back to cached files if API is unavailable.
        """
        result: Dict[str, List[dict]] = {}

        # Try loading from cache first
        cache_file = self.data_dir / "twt49u_ex_dividend.json"
        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    result = json.load(f)
                if result:
                    self.ex_dividend_data = result
                    return
            except (json.JSONDecodeError, IOError):
                pass

        # Fetch from TWSE API
        try:
            data = self._fetch_twt49u()
            if data:
                result = self._parse_twt49u(data)
                # Cache the result
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
        except Exception as e:
            # Silently fail — we still have dividend declarations as fallback
            pass

        self.ex_dividend_data = result

    def _fetch_twt49u(self) -> Optional[List[dict]]:
        """Fetch ex-dividend data from TWSE /TWT49U endpoint.
        
        Returns list of raw records or None on failure.
        """
        # TWSE Open API v1 endpoint for 除權除息
        url = f"{TWSE_BASE}/exchangeReport/TWT49U"

        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=TWSE_TIMEOUT)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list):
                        return data
            except (requests.RequestException, json.JSONDecodeError):
                pass
            time.sleep(1)

        return None

    def _parse_twt49u(self, raw_data: List[dict]) -> Dict[str, List[dict]]:
        """Parse raw TWT49U data into structured corporate actions.
        
        TWT49U fields (Chinese):
        - 有價證券代號: stock code
        - 除權除息日: ex-dividend/ex-rights date
        - 每股現金股利: cash dividend per share
        - 每股股票股利: stock dividend per share
        - 參考價格: reference price (theoretical ex-date price)
        - 權值: rights value
        """
        result: Dict[str, List[dict]] = {}

        for item in raw_data:
            code = item.get("有價證券代號", item.get("公司代號", ""))
            if not code:
                continue

            # Parse ex-date (ROC format)
            ex_date_roc = item.get("除權除息日", "")
            ex_date = roc_date_to_iso(ex_date_roc)
            if not ex_date:
                continue

            # Cash dividend
            cash_div = 0.0
            for key in ["每股現金股利", "現金股利"]:
                val = item.get(key, 0)
                try:
                    cash_div = float(val)
                    break
                except (ValueError, TypeError):
                    pass

            # Stock dividend
            stock_div = 0.0
            for key in ["每股股票股利", "股票股利"]:
                val = item.get(key, 0)
                try:
                    stock_div = float(val)
                    break
                except (ValueError, TypeError):
                    pass

            # Reference price (theoretical ex-date price)
            ref_price = 0.0
            for key in ["參考價格", "參考價"]:
                val = item.get(key, 0)
                try:
                    ref_price = float(val)
                    break
                except (ValueError, TypeError):
                    pass

            # Rights value (權值) — useful for assessing impact
            rights_value = 0.0
            for key in ["權值", "淨權值"]:
                val = item.get(key, 0)
                try:
                    rights_value = float(val)
                    break
                except (ValueError, TypeError):
                    pass

            if code not in result:
                result[code] = []
            result[code].append({
                "date": ex_date,
                "cash_div": round(cash_div, 4),
                "stock_div": round(stock_div, 4),
                "ref_price": round(ref_price, 2),
                "rights_value": round(rights_value, 2),
                "source": "twt49u",
            })

        return result

    # ------------------------------------------------------------------ #
    #  Ex-date estimation (fallback when TWT49U data unavailable)
    # ------------------------------------------------------------------ #

    def _estimate_ex_date(self, sh_date_str: str) -> Optional[str]:
        """Estimate ex-dividend date from shareholder meeting date.
        
        Taiwan market convention:
        - Shareholder meeting (股東會) is held in May (annual) or after quarter end
        - Ex-dividend date (除息日) is typically 1-3 days before payment date
        - Payment date is typically 7-14 days after shareholder meeting
        - Therefore: ex-date ≈ shareholder meeting + 5-12 days
        
        Using +7 days as the median estimate. This is an approximation;
        actual ex-dates from TWT49U API take priority.
        """
        if not sh_date_str or len(sh_date_str) < 7:
            return None

        try:
            roc_year = int(sh_date_str[:3])
            month = int(sh_date_str[3:5])
            day = int(sh_date_str[5:7])
            year = roc_year + 1911
            sh_date = datetime(year, month, day)

            # Improved estimate: 7 days after shareholder meeting
            # (previous +10 was too late for many stocks)
            ex_date = sh_date + timedelta(days=7)
            return ex_date.strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            return None

    # ------------------------------------------------------------------ #
    #  Query interface
    # ------------------------------------------------------------------ #

    def get_actions_for_stock(self, stock_code: str) -> List[dict]:
        """Get all corporate actions for a stock, merging TWT49U and declaration data.
        
        TWT49U data takes priority (actual ex-dates). Declaration data fills gaps.
        """
        actions = []

        # TWT49U data (priority — actual ex-dates)
        for action in self.ex_dividend_data.get(stock_code, []):
            actions.append({**action, "_priority": 1})

        # Declaration data (fallback — estimated ex-dates)
        for action in self.dividend_data.get(stock_code, []):
            # Only add if no TWT49U entry for the same date
            existing_dates = {a["date"] for a in actions if a.get("_priority") == 1}
            if action["date"] not in existing_dates:
                actions.append({**action, "_priority": 2})

        # Sort by date
        actions.sort(key=lambda a: a["date"])
        return actions

    def is_ex_dividend_date(self, stock_code: str, date_str: str) -> bool:
        """Check if a stock has an ex-dividend date on the given date."""
        cache_key = (stock_code, date_str)
        if cache_key in self._ex_div_cache:
            return self._ex_div_cache[cache_key]

        actions = self.get_actions_for_stock(stock_code)
        found = any(a["date"] == date_str for a in actions)
        self._ex_div_cache[cache_key] = found
        return found

    def get_action_on_date(self, stock_code: str, date_str: str) -> Optional[dict]:
        """Get the corporate action for a stock on a specific date."""
        actions = self.get_actions_for_stock(stock_code)
        for action in actions:
            if action["date"] == date_str:
                return action
        return None

    def get_ex_dividend_amount(self, stock_code: str, date_str: str) -> Optional[float]:
        """Get the cash dividend amount for a stock on a given date."""
        action = self.get_action_on_date(stock_code, date_str)
        if action:
            return action.get("cash_div", 0)
        return None

    def get_stock_dividend_ratio(self, stock_code: str, date_str: str) -> Optional[float]:
        """Get the stock dividend ratio for a stock on a given date.
        
        Stock dividend in 元 (face value). 1元 = 0.1 shares per share
        (since Taiwan face value is 10 TWD). So stock_div of 3 means
        3/10 = 0.3 shares per share (30% increase in share count).
        """
        action = self.get_action_on_date(stock_code, date_str)
        if action:
            return action.get("stock_div", 0)
        return None

    # ------------------------------------------------------------------ #
    #  Phase 15: Ex-date validation (holiday-aware)
    # ------------------------------------------------------------------ #

    def validate_ex_dates(self) -> List[dict]:
        """Validate all ex-dividend dates against holiday calendar.

        Checks:
        - Ex-dates must fall on trading days (not weekends/holidays)
        - True duplicate records (same date + same amounts from different sources)
          — Taiwan stocks commonly have BOTH cash and stock dividends on the same
            ex-date; only flag as duplicate if amounts also match.
        - Non-official source ex-dates on non-trading days get higher severity
          because TWT49U (official) conflicts likely mean calendar gaps, not bad data.

        Returns list of validation issues found.
        """
        issues: List[dict] = []

        if not self.holiday_calendar:
            return [{"type": "warning", "message": "Holiday calendar unavailable — skip ex-date validation"}]

        all_codes = set(
            list(self.ex_dividend_data.keys()) + list(self.dividend_data.keys())
        )

        for code in sorted(all_codes):
            actions = self.get_actions_for_stock(code)
            # Track (date, cash_div, stock_div) tuples for true duplicate detection.
            # Taiwan stocks commonly have multiple action types on the same ex-date
            # (e.g., cash + stock dividend), so date alone is not a duplicate key.
            seen_actions: Dict[tuple, str] = {}

            for action in actions:
                ex_date = action["date"]
                source = action.get("source", "unknown")
                cash_div = float(action.get("cash_div", 0) or 0)
                stock_div = float(action.get("stock_div", 0) or 0)

                # Check 1: True duplicate — same date AND same amounts from different source
                action_key = (ex_date, round(cash_div, 4), round(stock_div, 4))
                if action_key in seen_actions:
                    prev_source = seen_actions[action_key]
                    issues.append({
                        "type": "warning",
                        "stock_code": code,
                        "date": ex_date,
                        "message": f"Exact duplicate ex-date record: {source} vs {prev_source}",
                        "severity": "low",
                    })
                else:
                    seen_actions[action_key] = source

                # Check 2: Ex-date must be a trading day
                if not self.holiday_calendar.is_trading_day(ex_date):
                    if source == "twt49u":
                        # Official TWSE data — likely calendar is missing a make-up workday
                        issues.append({
                            "type": "warning",
                            "stock_code": code,
                            "date": ex_date,
                            "source": source,
                            "message": f"TWT49U ex-date {ex_date} not in calendar — likely missing make-up workday",
                            "severity": "medium",
                        })
                    else:
                        # Non-official source (estimated from declaration) — more likely genuinely wrong
                        issues.append({
                            "type": "warning",
                            "stock_code": code,
                            "date": ex_date,
                            "source": source,
                            "message": f"Estimated ex-date {ex_date} is not a trading day",
                            "severity": "high",
                        })

        return issues

    # ------------------------------------------------------------------ #
    #  Backward price adjustment engine
    # ------------------------------------------------------------------ #

    def backward_adjust_prices(
        self,
        prices: List[dict],
        stock_code: str,
    ) -> List[dict]:
        """Apply backward adjustment to historical price data.
        
        Backward adjustment means: when a stock goes ex-dividend, we adjust
        ALL historical prices BEFORE the ex-date so that the price chart
        is continuous. This is the standard practice in financial data.
        
        For cash dividends:
            adjustment_factor = (price_before_ex - cash_div) / price_before_ex
            All prices before ex-date are multiplied by this factor.
        
        For stock dividends (配股):
            Taiwan convention: stock_div is in 元 (face value), face value = 10 TWD.
            If stock_div = 3 (3元), that means 3/10 = 0.3 shares per share.
            adjustment_factor = 1 / (1 + stock_div/10)
            All prices before ex-date are multiplied by this factor.
        
        Combined (cash + stock dividend):
            adjustment_factor = (price_before_ex - cash_div) / (price_before_ex * (1 + stock_div/10))
        
        Args:
            prices: List of price dicts with 'date' and 'close' keys, sorted by date.
            stock_code: Stock code to look up corporate actions.
        
        Returns:
            Adjusted price list with 'adj_close' field added.
        """
        if not prices:
            return prices

        # Get all corporate actions for this stock within the price range
        actions = self.get_actions_for_stock(stock_code)
        if not actions:
            # No actions — just copy close to adj_close
            for p in prices:
                p["adj_close"] = p.get("close", 0)
            return prices

        # Filter actions to those within our price range
        price_dates = {p["date"] for p in prices}
        relevant_actions = [a for a in actions if a["date"] in price_dates]

        if not relevant_actions:
            for p in prices:
                p["adj_close"] = p.get("close", 0)
            return prices

        # Sort actions by date (descending) for backward adjustment
        relevant_actions.sort(key=lambda a: a["date"], reverse=True)

        # Build date-to-action lookup
        action_by_date: Dict[str, dict] = {}
        for action in relevant_actions:
            action_by_date[action["date"]] = action

        # Backward adjustment: iterate from newest to oldest.
        # cumulative_factor starts at 1.0 (most recent prices are already adjusted).
        #
        # KEY: On the ex-date itself, the market price is ALREADY the post-dividend
        # price. We should NOT adjust the ex-date price. The adjustment factor is
        # computed on the ex-date but only applied to prices BEFORE it.
        cumulative_factor = 1.0

        # Two-pass approach:
        # Pass 1: compute adjustment factors for each ex-date using day-before close
        # Pass 2: apply cumulative factors to all prices
        factors_by_date: Dict[str, float] = {}
        for i in range(1, len(prices)):  # i = ex-date index
            if prices[i]["date"] in action_by_date:
                action = action_by_date[prices[i]["date"]]
                cash_div = action.get("cash_div", 0) or 0
                stock_div = action.get("stock_div", 0) or 0
                day_before_close = prices[i - 1].get("close", 0)

                if day_before_close > 0 and (cash_div > 0 or stock_div > 0):
                    # Taiwan convention: stock_div is in 元 (face value)
                    # 1元 = 0.1 shares per share (face value = 10 TWD)
                    stock_ratio = stock_div / 10.0

                    if cash_div > 0 and stock_div > 0:
                        adj_price = (day_before_close - cash_div) / (1.0 + stock_ratio)
                        factor = adj_price / day_before_close if adj_price > 0 else 1.0
                    elif cash_div > 0:
                        adj_price = day_before_close - cash_div
                        factor = adj_price / day_before_close if adj_price > 0 else 1.0
                    else:
                        factor = 1.0 / (1.0 + stock_ratio)

                    factors_by_date[prices[i]["date"]] = factor

        # Pass 2: apply cumulative adjustment from newest to oldest
        cumulative_factor = 1.0
        adjusted = []
        for p in reversed(prices):
            date = p["date"]
            close = p.get("close", 0)
            volume = p.get("volume", None)

            adj_close = round(close * cumulative_factor, 4) if close > 0 else 0
            # Volume adjustment: inverse of price factor (more shares → higher raw volume)
            adj_volume = round(volume / cumulative_factor) if volume is not None and cumulative_factor > 0 else volume

            result = {
                **p,
                "adj_close": adj_close,
                "cumulative_factor": round(cumulative_factor, 6),
            }
            if volume is not None:
                result["adj_volume"] = adj_volume

            adjusted.append(result)

            # Update cumulative factor for ALL PRIOR prices (ex-date factors apply before this point)
            if date in factors_by_date:
                cumulative_factor *= factors_by_date[date]

        # Reverse back to chronological order
        adjusted.reverse()
        return adjusted

    def adjust_price_for_dividend(self, price: float, stock_code: str, date_str: str) -> float:
        """Adjust a single price for ex-dividend drop.

        On ex-date, the stock price drops by the dividend amount.
        This adjustment prevents false crash signals.

        For cash dividends: add back the cash_div to the price.
        For stock dividends: the price is diluted by (1 + stock_div/10).
        We reverse this by multiplying by (1 + stock_div/10).

        Returns: adjusted price (or original if no ex-dividend)
        """
        action = self.get_action_on_date(stock_code, date_str)

        if action:
            cash_div = action.get("cash_div", 0) or 0
            stock_div = action.get("stock_div", 0) or 0

            if cash_div > 0 and stock_div > 0:
                # Combined: reverse both cash drop and dilution
                return (price + cash_div) * (1.0 + stock_div / 10.0)
            elif cash_div > 0:
                return price + cash_div  # Add back the dividend drop
            elif stock_div > 0:
                # Stock dividend dilutes price; reverse by multiplying
                return price * (1.0 + stock_div / 10.0)

        return price

    # ------------------------------------------------------------------ #
    #  Convenience methods
    # ------------------------------------------------------------------ #

    def should_skip_stop_check(self, stock_code: str, date_str: str) -> bool:
        """Check if stop loss should be skipped due to ex-dividend date.
        
        On ex-dividend dates, the price drop is mechanical,
        not a market signal. Skip stop checks to avoid false exits.
        """
        return self.is_ex_dividend_date(stock_code, date_str)

    def get_dividend_yield_impact(self, stock_code: str, current_price: float) -> float:
        """Estimate dividend yield impact for a stock.
        
        Returns: estimated annual dividend yield as percentage.
        """
        actions = self.get_actions_for_stock(stock_code)

        if not actions or current_price <= 0:
            return 0.0

        # Sum cash dividends for actions in the next 365 days
        today = datetime.now().strftime("%Y-%m-%d")
        cutoff = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

        total_div = 0.0
        for action in actions:
            action_date = action.get("date", "")
            if today <= action_date <= cutoff:
                total_div += action.get("cash_div", 0) or 0

        yield_pct = (total_div / current_price) * 100
        return round(yield_pct, 2)

    def get_ex_dividend_dates_for_stock(self, stock_code: str) -> List[str]:
        """Get all ex-dividend dates for a stock."""
        actions = self.get_actions_for_stock(stock_code)
        return [a["date"] for a in actions if a.get("date")]

    def get_all_ex_dividend_dates(self) -> Dict[str, List[str]]:
        """Get all ex-dividend dates across all stocks.
        
        Returns: dict {date: [stock_codes]}
        """
        result: Dict[str, List[str]] = {}
        all_codes = set(
            list(self.ex_dividend_data.keys()) + list(self.dividend_data.keys())
        )

        for code in all_codes:
            actions = self.get_actions_for_stock(code)
            for action in actions:
                date = action.get("date")
                if date:
                    if date not in result:
                        result[date] = []
                    result[date].append(code)

        return result

    def summary(self) -> dict:
        """Return a summary of loaded corporate action data."""
        twt49u_stocks = len(self.ex_dividend_data)
        decl_stocks = len(self.dividend_data)
        twt49u_actions = sum(len(v) for v in self.ex_dividend_data.values())
        decl_actions = sum(len(v) for v in self.dividend_data.values())

        return {
            "twt49u_stocks": twt49u_stocks,
            "twt49u_actions": twt49u_actions,
            "declaration_stocks": decl_stocks,
            "declaration_actions": decl_actions,
            "loaded": self._loaded,
        }


def main():
    """Test corporate action handler."""
    handler = CorporateActionHandler()

    summary = handler.summary()
    print(f"Corporate Action Handler Summary:")
    print(f"  TWT49U stocks: {summary['twt49u_stocks']}")
    print(f"  TWT49U actions: {summary['twt49u_actions']}")
    print(f"  Declaration stocks: {summary['declaration_stocks']}")
    print(f"  Declaration actions: {summary['declaration_actions']}")

    # Test with a known stock
    test_code = "2330"
    actions = handler.get_actions_for_stock(test_code)
    if actions:
        print(f"\n{test_code} corporate actions:")
        for a in actions[:5]:
            print(f"  {a['date']}: cash={a.get('cash_div', 0)}, stock={a.get('stock_div', 0)}, source={a.get('source', '?')}")

    # Test backward adjustment with sample data
    print(f"\nBackward adjustment test:")
    sample_prices = [
        {"date": "2026-06-01", "close": 280.0},
        {"date": "2026-06-02", "close": 282.0},
        {"date": "2026-06-03", "close": 278.0},  # Ex-dividend day (cash=8)
        {"date": "2026-06-04", "close": 275.0},
        {"date": "2026-06-05", "close": 277.0},
    ]
    adjusted = handler.backward_adjust_prices(sample_prices, test_code)
    for p in adjusted:
        print(f"  {p['date']}: close={p['close']}, adj_close={p['adj_close']}, factor={p.get('cumulative_factor', 1.0)}")

    return handler


if __name__ == "__main__":
    main()
