#!/usr/bin/env python3
"""
Taiwan Stock Exchange Holiday Calendar

Survivorship Bias Tier 2: Properly handles TWSE holidays so that
trading-day calculations don't assume phantom trading days.

Without this, SMA windows count calendar days during LNY (5-7 day gap)
as "missing data" rather than non-trading days, causing:
- Inflated volatility readings around holiday gaps
- Incorrect MA crossovers when a 20-day window spans a 1-week holiday
- Backtest P&L errors from assuming fills on closed-market days

Data source: TWSE /holidaySchedule/holidaySchedule API (ROC date format).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


def roc_date_to_iso(roc_date_str: str) -> Optional[str]:
    """Convert ROC date string (e.g., '1150522') to ISO format (e.g., '2026-05-22').

    ROC year = year - 1911, so 115 = 2026.
    """
    if not roc_date_str or len(roc_date_str) < 7:
        return None
    try:
        roc_year = int(str(roc_date_str)[:3])
        month = int(str(roc_date_str)[3:5])
        day = int(str(roc_date_str)[5:7])
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


class HolidayCalendar:
    """Manages TWSE trading holidays.

    Loads holiday schedule from cached fetch_data files or fetches fresh.
    Distinguishes between full market closures and "settlement only" days.
    """

    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent / "data"
        # Set of ISO dates when market is fully closed (no trading)
        self._closed_dates: set = set()
        # Set of ISO dates with partial/no trading but not full closure
        self._partial_dates: set = set()  # Half-day sessions
        # Make-up workdays — Saturdays that are actually trading days
        self._makeup_workdays: set = set()
        # Mapping date -> holiday name for debugging/logging
        self._holiday_names: dict = {}
        self._raw_holidays: list = []
        self._load_holidays()

    def _load_holidays(self):
        """Load holidays from cached data files.

        Uses the most recent holidays_*.json file from fetch_data.py runs.
        Phase 9: Use isTrading field from TWSE API instead of fragile text matching.
        Also handles make-up workdays (補行上班日) where market opens on Saturday,
        half-day sessions (僅上午交易), and typhoon days (颱風天).

        Cross-year fix: Load ALL holiday files in data_dir, merge by date to catch
        CNY gaps that span year boundaries (e.g., Dec 31 → Jan 2).
        """
        # Load ALL holiday files (not just latest) for cross-year coverage
        holiday_files = sorted(self.data_dir.glob("holidays_*.json"))
        if not holiday_files:
            return

        all_raw: list = []
        seen_dates: set = set()
        for filepath in holiday_files:
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for entry in data:
                    roc_date = str(entry.get("Date", ""))
                    if roc_date not in seen_dates:
                        all_raw.append(entry)
                        seen_dates.add(roc_date)
            except (json.JSONDecodeError, IOError):
                continue

        self._raw_holidays = all_raw

        for entry in self._raw_holidays:
            roc_date = str(entry.get("Date", ""))
            iso_date = roc_date_to_iso(roc_date)
            if not iso_date:
                continue

            name = entry.get("Name", "")
            description = entry.get("Description", "")
            combined_text = f"{name} {description}".lower()

            self._holiday_names[iso_date] = name

            # Phase 9: Prefer isTrading field from TWSE API if available
            is_trading = entry.get("isTrading", None)
            if is_trading == "N":
                # Explicitly marked as non-trading day
                self._closed_dates.add(iso_date)
            elif is_trading == "Y":
                # Marked as trading — check for half-day or make-up workday
                if "僅上午交易" in combined_text or "half" in combined_text:
                    self._partial_dates.add(iso_date)  # Half-day session
                # else: normal trading day, no action needed
            elif is_trading is None:
                # Fallback to text matching when isTrading field missing
                if "市場無交易" in combined_text or "無交易" in combined_text:
                    self._closed_dates.add(iso_date)
                elif "開始交易日" in combined_text or "最後交易日" in combined_text:
                    pass  # Boundary markers, not holidays
                else:
                    self._closed_dates.add(iso_date)
            
            # Handle make-up workdays (補行上班日) — Saturday becomes trading day
            if "補行上班" in name or "調整上班" in description or "makeup" in combined_text:
                try:
                    dt = datetime.strptime(iso_date, "%Y-%m-%d")
                    # Make-up workdays are typically on Saturdays (weekday 5)
                    # We track them separately since is_trading_day checks weekday >= 5 first
                    self._makeup_workdays.add(iso_date)
                except ValueError:
                    pass

    def is_trading_day(self, date_str: str) -> bool:
        """Check if a given ISO date is a TWSE trading day.

        Phase 9: Also handles make-up workdays (Saturdays designated as working days).
        
        Args:
            date_str: Date in YYYY-MM-DD format.

        Returns:
            True if it's a normal trading day (weekday and not a holiday).
        """
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return False

        # Check if this Saturday is a make-up workday (補行上班日)
        if date_str in self._makeup_workdays:
            return True

        # Weekends are never trading days (except make-up workdays handled above)
        if dt.weekday() >= 5:
            return False

        # Check holidays
        if date_str in self._closed_dates or date_str in self._partial_dates:
            return False

        return True

    def is_holiday(self, date_str: str) -> bool:
        """Check if a given ISO date is a TWSE holiday."""
        return date_str in self._closed_dates or date_str in self._partial_dates

    def is_half_day(self, date_str: str) -> bool:
        """Check if a given ISO date has a half-day session (morning only).
        
        Half-days typically close at 13:00 instead of normal hours.
        Volume on half-days is typically 40-60% of normal → affects liquidity scoring.
        """
        return date_str in self._partial_dates

    def get_session_hours(self, date_str: str) -> dict:
        """Get trading session schedule for a date.
        
        Returns:
            Dict with 'type' (closed/half_day/full_day), 'open', 'close', 'hours'.
        """
        if not self.is_trading_day(date_str):
            return {"type": "closed", "hours": 0}
        if date_str in self._partial_dates:
            return {
                "type": "half_day",
                "open": "09:00",
                "close": "13:00",
                "hours": 4.0,
            }
        return {
            "type": "full_day",
            "open": "09:00",
            "close": "13:30",  # TWSE normal close (plus afternoon session)
            "hours": 6.5,
        }

    def get_holiday_name(self, date_str: str) -> Optional[str]:
        """Get the name of the holiday for a given date."""
        return self._holiday_names.get(date_str)

    def get_trading_days_in_range(
        self, start_date: str, end_date: str
    ) -> list:
        """Get list of trading days between two dates (inclusive).

        Args:
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.

        Returns:
            List of ISO date strings for trading days.
        """
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            return []

        result = []
        current = start
        while current <= end:
            iso = current.strftime("%Y-%m-%d")
            if self.is_trading_day(iso):
                result.append(iso)
            current += timedelta(days=1)

        return result

    def count_trading_days_in_range(self, start_date: str, end_date: str) -> int:
        """Count trading days between two dates."""
        return len(self.get_trading_days_in_range(start_date, end_date))

    def get_holidays_for_year(self, year: int) -> list:
        """Get all holidays for a given Gregorian year.

        Args:
            year: Gregorian year (e.g., 2026).

        Returns:
            List of dicts with 'date', 'name' keys.
        """
        result = []
        for date_str, name in self._holiday_names.items():
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                if dt.year == year:
                    result.append({"date": date_str, "name": name})
            except ValueError:
                continue
        return sorted(result, key=lambda x: x["date"])

    def next_trading_day(self, from_date: str) -> Optional[str]:
        """Get the next trading day after a given date.

        Args:
            from_date: Starting date in YYYY-MM-DD format (exclusive).

        Returns:
            ISO date string of next trading day, or None if not found within 60 days.
        """
        try:
            dt = datetime.strptime(from_date, "%Y-%m-%d") + timedelta(days=1)
        except ValueError:
            return None

        for i in range(60):
            check_date = dt + timedelta(days=i)
            iso = check_date.strftime("%Y-%m-%d")
            if self.is_trading_day(iso):
                return iso
        return None

    def prev_trading_day(self, from_date: str) -> Optional[str]:
        """Get the previous trading day before a given date.

        Args:
            from_date: Starting date in YYYY-MM-DD format (exclusive).

        Returns:
            ISO date string of previous trading day, or None if not found within 60 days.
        """
        try:
            dt = datetime.strptime(from_date, "%Y-%m-%d") - timedelta(days=1)
        except ValueError:
            return None

        for i in range(60):
            check_date = dt - timedelta(days=i)
            iso = check_date.strftime("%Y-%m-%d")
            if self.is_trading_day(iso):
                return iso
        return None

    def get_holiday_gaps(self, start_date: str, end_date: str) -> list:
        """Find holiday gaps (consecutive non-trading days > 2 days).

        Useful for identifying LNY and other extended closures that affect
        moving average calculations.
        
        Phase 9 R7: Also returns gap info useful for position sizing —
        long gaps (>5 days) should trigger reduced position sizes (Z.ai recommendation).

        Returns:
            List of dicts with 'start', 'end', 'length', 'reason', 'risk_level'.
        """
        all_days = self.get_trading_days_in_range(start_date, end_date)
        if not all_days:
            return []

        gaps = []
        try:
            full_start = datetime.strptime(start_date, "%Y-%m-%d")
            full_end = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            return []

        current = full_start
        gap_start = None
        while current <= full_end:
            iso = current.strftime("%Y-%m-%d")
            if not self.is_trading_day(iso):
                # Only count weekdays as part of gaps (weekends are expected)
                dt = datetime.strptime(iso, "%Y-%m-%d")
                if dt.weekday() < 5:  # Weekday but closed = holiday gap
                    if gap_start is None:
                        gap_start = iso
            else:
                # Trading day — end any open gap
                if gap_start is not None:
                    gap_end = (datetime.strptime(iso, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                    gap_length = (datetime.strptime(gap_end, "%Y-%m-%d") - datetime.strptime(gap_start, "%Y-%m-%d")).days + 1
                    if gap_length > 2:
                        reason = self._holiday_names.get(gap_start, "Unknown holiday")
                        # Phase 9 R7: Risk level for position sizing
                        if gap_length >= 8:
                            risk_level = "high"   # LNY-level — reduce positions by 50%
                        elif gap_length >= 4:
                            risk_level = "medium" # Reduce positions by 25%
                        else:
                            risk_level = "low"
                        gaps.append({
                            "start": gap_start,
                            "end": gap_end,
                            "length": gap_length,
                            "reason": reason,
                            "risk_level": risk_level,
                        })
                    gap_start = None

            current += timedelta(days=1)

        # Handle gap at end of range
        if gap_start is not None:
            gap_end = full_end.strftime("%Y-%m-%d")
            gap_length = (datetime.strptime(gap_end, "%Y-%m-%d") - datetime.strptime(gap_start, "%Y-%m-%d")).days + 1
            reason = self._holiday_names.get(gap_start, "Unknown holiday")
            if gap_length >= 8:
                risk_level = "high"
            elif gap_length >= 4:
                risk_level = "medium"
            else:
                risk_level = "low"
            gaps.append({
                "start": gap_start,
                "end": gap_end,
                "length": gap_length,
                "reason": reason,
                "risk_level": risk_level,
            })

        return gaps

    def summary(self) -> dict:
        """Return a summary of loaded holiday data."""
        current_year = datetime.now().year
        year_holidays = self.get_holidays_for_year(current_year)
        return {
            "total_raw_entries": len(self._raw_holidays),
            "closed_dates_count": len(self._closed_dates),
            "partial_dates_count": len(self._partial_dates),
            "makeup_workdays_count": len(self._makeup_workdays),
            "current_year_holidays": len(year_holidays),
            "sample_holidays": year_holidays[:5],
        }


# Module-level singleton for convenience
_calendar: Optional[HolidayCalendar] = None


def get_calendar(data_dir: Optional[str] = None) -> HolidayCalendar:
    """Get a shared HolidayCalendar instance."""
    global _calendar
    if _calendar is None:
        _calendar = HolidayCalendar(data_dir=data_dir)
    return _calendar


# Convenience functions for use in other modules
def is_trading_day(date_str: str, data_dir: Optional[str] = None) -> bool:
    """Check if a date is a TWSE trading day (module-level convenience)."""
    return get_calendar(data_dir).is_trading_day(date_str)


def get_holiday_gaps(start_date: str, end_date: str, data_dir: Optional[str] = None) -> list:
    """Get holiday gaps in a date range."""
    return get_calendar(data_dir).get_holiday_gaps(start_date, end_date)


if __name__ == "__main__":
    import sys

    cal = HolidayCalendar()
    summary = cal.summary()
    print(f"Holiday Calendar Summary:")
    print(f"  Raw entries: {summary['total_raw_entries']}")
    print(f"  Closed dates: {summary['closed_dates_count']}")
    print(f"  Current year holidays: {summary['current_year_holidays']}")

    # Show current year holidays
    for h in summary["sample_holidays"]:
        print(f"    {h['date']}: {h['name']}")

    # Test trading day checks
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"\n  Today ({today}): {'Trading' if cal.is_trading_day(today) else 'Closed'}")
    print(f"  Yesterday ({yesterday}): {'Trading' if cal.is_trading_day(yesterday) else 'Closed'}")

    # Show holiday gaps for current year
    start_y = f"{datetime.now().year}-01-01"
    end_y = f"{datetime.now().year}-12-31"
    gaps = cal.get_holiday_gaps(start_y, end_y)
    if gaps:
        print(f"\n  Holiday gaps in {datetime.now().year}:")
        for g in gaps:
            print(f"    {g['start']} → {g['end']} ({g['length']} days): {g['reason']}")
