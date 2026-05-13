#!/usr/bin/env python3
"""
Telegram Alerts - Send screening results to Telegram

Features:
- Top candidates alert
- Regime change alert
- Significant score changes
- Daily summary
- Rate limiting to avoid spam
"""

import json
from datetime import date, datetime
from pathlib import Path


class TelegramAlerts:
    def __init__(self, data_dir=None):
        self.data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent / "data"
        self.reports_dir = Path(__file__).parent.parent / "reports"
        self.last_alert_file = self.data_dir / "last_alert.json"
        self.cooldown_minutes = 30  # Min time between alerts

        # Phase 15: Holiday calendar for alert suppression on non-trading days
        self.holiday_calendar = None
        try:
            from core.holiday_calendar import HolidayCalendar
            self.holiday_calendar = HolidayCalendar(str(self.data_dir))
        except ImportError:
            pass
    
    def should_alert(self, alert_type="daily"):
        """Check if we should send an alert (rate limiting + holiday suppression)

        Phase 15: Suppress daily alerts on non-trading days (weekends/holidays)
        to avoid sending stale or irrelevant information.
        Critical alerts and heartbeat checks ALWAYS bypass suppression.
        """
        # ── Holiday suppression ──────────────────────────────────
        NEVER_SUPPRESS = {"regime_change", "crisis", "black_swan", "heartbeat"}

        if alert_type not in NEVER_SUPPRESS:
            today = date.today().isoformat()  # ISO string for is_trading_day()
            if self.holiday_calendar and not self.holiday_calendar.is_trading_day(today):
                return False

        if not self.last_alert_file.exists():
            return True
        
        with open(self.last_alert_file, 'r') as f:
            last_alert = json.load(f)
        
        last_time = datetime.fromisoformat(last_alert.get("timestamp", ""))
        now = datetime.now()
        
        if (now - last_time).total_seconds() < self.cooldown_minutes * 60:
            return False
        
        return True
    
    def record_alert(self, alert_type="daily"):
        """Record that an alert was sent"""
        self.last_alert_file.parent.mkdir(exist_ok=True)
        
        with open(self.last_alert_file, 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "type": alert_type
            }, f)
    
    def format_top_candidates(self, candidates, limit=5):
        """Format top candidates for Telegram (no markdown tables)"""
        lines = []
        
        for i, c in enumerate(candidates[:limit], 1):
            code = c.get("code", "")
            name = c.get("name", "")
            combined = c.get("combined_score", c.get("composite_score", 0))
            s1 = c.get("stage1_score", "")
            s2 = c.get("stage2_score", "")
            
            lines.append(f"{i}. {code} {name}")
            lines.append(f"   Score: {combined} (S1:{s1} S2:{s2})")
            
            # Add checks if available
            checks = c.get("checks", {})
            if checks:
                pledge = checks.get("pledge", {}).get("status", "")
                penalties = checks.get("penalties", {}).get("status", "")
                if pledge or penalties:
                    lines.append(f"   ✅ Pledge: {pledge} | Penalties: {penalties}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def format_regime(self, regime_data):
        """Format regime info for Telegram"""
        regime = regime_data.get("regime", "unknown")
        trend = regime_data.get("trend", "unknown")
        volatility = regime_data.get("volatility", 0)
        
        emoji = {
            "normal": "🐂",
            "caution": "📊",
            "stress": "⚠️",
            "crisis": "🐻",
            "black_swan": "🚨",
            "unknown": "❓"
        }
        
        return f"{emoji.get(regime, '❓')} Regime: {regime}\n   Trend: {trend} | Volatility: {volatility:.4f}"
    
    def format_daily_summary(self, report):
        """Format daily summary for Telegram"""
        date = report.get("date", "unknown")
        stage1 = report.get("stage1", {})
        stage2 = report.get("stage2", {})
        trading = report.get("trading_stats", {})
        
        # Get candidates from top_10 or candidates
        candidates = stage2.get("top_10", stage2.get("candidates", []))
        
        lines = [
            f"📊 TW Stock Hunter - {date}",
            f"",
            f"Stage 1: {stage1.get('passed', 0)} passed / {stage1.get('total_screened', 0)} screened",
            f"Stage 2: {stage2.get('passed', 0)} passed / {stage2.get('disqualified', 0)} disqualified",
            f"",
            f"Top Candidates:",
            self.format_top_candidates(candidates),
        ]
        
        # Add trading stats if available
        if trading.get("completed", 0) > 0:
            lines.append(f"Trading Stats:")
            lines.append(f"   Win rate: {trading.get('win_rate', 0)}%")
            lines.append(f"   Avg P&L: {trading.get('avg_pnl_pct', 0)}%")
            lines.append(f"   Profit factor: {trading.get('profit_factor', 0)}")
            lines.append("")
        
        return "\n".join(lines)
    
    def check_regime_change(self):
        """Check if regime changed since last alert"""
        regime_file = self.data_dir / "regime.json"
        if not regime_file.exists():
            return None
        
        with open(regime_file, 'r') as f:
            current_regime = json.load(f)
        
        if not self.last_alert_file.exists():
            return None
        
        with open(self.last_alert_file, 'r') as f:
            last_alert = json.load(f)
        
        last_regime = last_alert.get("regime", "")
        current = current_regime.get("regime", "")
        
        if last_regime and current != last_regime:
            return {
                "old": last_regime,
                "new": current,
                "data": current_regime
            }
        
        return None
    
    def generate_alert(self, date_str=None):
        """Generate alert message"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Load report
        report_file = self.reports_dir / f"report_{date_str}.json"
        if not report_file.exists():
            return None
        
        with open(report_file, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        # Check regime change
        regime_change = self.check_regime_change()
        
        message = self.format_daily_summary(report)
        
        if regime_change:
            message = f"⚠️ Regime Change: {regime_change['old']} → {regime_change['new']}\n\n" + message
        
        return message
    
    def update_last_alert_with_regime(self):
        """Update last alert with current regime"""
        regime_file = self.data_dir / "regime.json"
        if regime_file.exists():
            with open(regime_file, 'r') as f:
                regime = json.load(f)
            
            self.last_alert_file.parent.mkdir(exist_ok=True)
            with open(self.last_alert_file, 'w') as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "regime": regime.get("regime", "unknown")
                }, f)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate Telegram alerts")
    parser.add_argument("--date", type=str, help="Date to alert (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    alerts = TelegramAlerts()
    message = alerts.generate_alert(date_str=args.date)
    
    if message:
        if args.verbose:
            print("📱 Telegram Alert:")
            print(message)
        alerts.record_alert()
        alerts.update_last_alert_with_regime()
    else:
        if args.verbose:
            print("❌ No alert generated")
    
    return message


if __name__ == "__main__":
    main()
