#!/usr/bin/env python3
"""
Telegram Alerts - Send screening results to Telegram

Features:
- Top candidates alert
- Regime change alert
- Significant score changes
- Daily summary
- Rate limiting to avoid spam
- Phase 20: Actual message delivery via Telegram Bot API
- Phase 29: Alert Deduplication - suppress duplicates within configurable cooldown
- Phase 29: Escalation Rules - severity levels (info/warning/critical) with routing
"""

import hashlib
import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# -- Phase 29: Severity mapping for alert types --
# info=1: goes to daily digest only (new_entry, take_profit)
# warning=2: sent immediately, batched (regime_change, signal_detected)
# critical=3: sent immediately with special formatting (stop_loss_hit)
SEVERITY_MAP = {
    "new_entry": "info",
    "take_profit": "info",
    "regime_change": "warning",
    "signal_detected": "warning",
    "stop_loss_hit": "critical",
    # Legacy alert types default to info severity
    "daily": "info",
    "crisis": "warning",
    "black_swan": "warning",
    "heartbeat": "info",
}

SEVERITY_LEVEL = {"info": 1, "warning": 2, "critical": 3}


class TelegramAlerts:
    def __init__(self, data_dir=None, dedup_cooldown_hours: float = 4.0):
        self.data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent / "data"
        self.reports_dir = Path(__file__).parent.parent / "reports"
        self.last_alert_file = self.data_dir / "last_alert.json"
        self.cooldown_minutes = 30  # Min time between alerts

        # Phase 29: Alert deduplication - track recent alerts in history file
        self.alert_history_file = self.data_dir / "alert_history.json"
        self.dedup_cooldown_hours = dedup_cooldown_hours  # Default 4 hours

        # Phase 20: Telegram Bot API credentials from environment
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

        # Phase 15: Holiday calendar for alert suppression on non-trading days
        self.holiday_calendar = None
        try:
            from holiday_calendar import HolidayCalendar
            self.holiday_calendar = HolidayCalendar(str(self.data_dir))
        except ImportError:
            pass

    def send_message(self, message: str) -> bool:
        """Send a message via Telegram Bot API.

        Phase 20: Actual HTTP POST to Telegram's sendMessage endpoint.
        Returns True on success, False on failure or missing credentials.
        Gracefully degrades if TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
        are not configured - prints a warning but does not raise.
        """
        if not self.bot_token or not self.chat_id:
            logger.warning("Telegram credentials not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
            return False

        import requests

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML",
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                return True
            else:
                logger.warning("Telegram API error: HTTP %d - %s", response.status_code, response.text[:200])
                return False
        except requests.exceptions.Timeout:
            logger.warning("Telegram API: request timed out")
            return False
        except Exception as e:
            logger.warning("Failed to send Telegram message: %s", e)
            return False

    def deliver_alert(self, date_str=None, alert_type="daily") -> bool:
        """Generate and deliver an alert via Telegram.

        Phase 20: End-to-end delivery - generate_alert() + send_message().
        Respects rate limiting via should_alert(). Records the alert on
        success so cooldown logic works correctly.

        Phase 29: Now routes through the escalation system. Info-level
        alerts are handled silently (future digest). Warning/critical go
        immediately. Backward compatible - existing callers using
        alert_type="daily" still work.

        Returns True if the alert was sent (or queued), False otherwise.
        """
        severity = self._get_severity(alert_type)

        message = self.generate_alert(date_str)
        if not message:
            return False

        # Phase 29: Deduplication check
        stock_code = self._extract_stock_code(message, alert_type)
        message_hash = self._compute_message_hash(message)
        if self._is_duplicate(alert_type, stock_code, message_hash):
            logger.info("Alert deduplicated: type=%s stock=%s", alert_type, stock_code)
            return False

        # Phase 29: Route based on severity
        if severity == "info":
            # Info alerts are sent normally but tagged as info severity.
            # Phase 29 digest mode (next commit) will batch these instead.
            if not self.should_alert(alert_type):
                return False
            success = self.send_message(message)
            if success:
                self.record_alert(alert_type)
                self._record_alert_history(alert_type, stock_code, message_hash)
                self.update_last_alert_with_regime()
            return success

        elif severity == "warning":
            # Warning alerts: sent immediately, bypass normal rate limiting
            success = self.send_message(message)
            if success:
                self.record_alert(alert_type)
                self._record_alert_history(alert_type, stock_code, message_hash)
                self.update_last_alert_with_regime()
            return success

        elif severity == "critical":
            # Critical alerts: always sent immediately with special formatting
            formatted = self._format_critical_alert(message, alert_type, stock_code)
            success = self.send_message(formatted)
            if success:
                self.record_alert(alert_type)
                self._record_alert_history(alert_type, stock_code, message_hash)
                self.update_last_alert_with_regime()
            return success

        # Fallback (should not reach here)
        if not self.should_alert(alert_type):
            return False
        success = self.send_message(message)
        if success:
            self.record_alert(alert_type)
            self.update_last_alert_with_regime()
        return success

    def should_alert(self, alert_type="daily"):
        """Check if we should send an alert (rate limiting + holiday suppression)

        Phase 15: Suppress daily alerts on non-trading days (weekends/holidays)
        to avoid sending stale or irrelevant information.
        Critical alerts and heartbeat checks ALWAYS bypass suppression.
        """
        # -- Holiday suppression --
        NEVER_SUPPRESS = {"regime_change", "crisis", "black_swan", "heartbeat", "stop_loss_hit"}

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

    # ================================================================== #
    #  Phase 29: Alert Deduplication
    # ================================================================== #

    def _compute_message_hash(self, message: str) -> str:
        """Compute a hash of the alert message for dedup comparison.

        Uses SHA-256 truncated to 16 hex chars for compact storage.
        Two alerts with the same substantive content produce the same hash.
        """
        return hashlib.sha256(message.encode("utf-8")).hexdigest()[:16]

    def _load_alert_history(self) -> List[Dict]:
        """Load alert history from data/alert_history.json.

        Returns list of dicts with keys:
        alert_id, timestamp, type, stock_code, message_hash
        """
        if not self.alert_history_file.exists():
            return []
        try:
            with open(self.alert_history_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []

    def _save_alert_history(self, history: List[Dict]):
        """Persist alert history, pruning entries older than cooldown."""
        self.alert_history_file.parent.mkdir(exist_ok=True)
        cutoff = datetime.now() - timedelta(hours=self.dedup_cooldown_hours)
        pruned = [
            entry for entry in history
            if self._parse_timestamp(entry.get("timestamp", "")) > cutoff
        ]
        with open(self.alert_history_file, 'w', encoding='utf-8') as f:
            json.dump(pruned, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _parse_timestamp(ts: str) -> datetime:
        """Parse an ISO timestamp string, falling back to epoch on failure."""
        try:
            return datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            return datetime.min

    def _is_duplicate(self, alert_type: str, stock_code: str,
                      message_hash: str) -> bool:
        """Check if this alert is a duplicate within the cooldown window.

        An alert is considered a duplicate if another alert with the same
        type + stock_code + message_hash exists within dedup_cooldown_hours.

        Args:
            alert_type: The alert category (e.g., 'regime_change', 'new_entry')
            stock_code: Stock code string (empty string for market-level alerts)
            message_hash: SHA-256 hash of the message content

        Returns:
            True if a matching alert was recently sent (duplicate), False otherwise.
        """
        history = self._load_alert_history()
        cutoff = datetime.now() - timedelta(hours=self.dedup_cooldown_hours)

        for entry in history:
            entry_time = self._parse_timestamp(entry.get("timestamp", ""))
            if entry_time <= cutoff:
                continue
            if (entry.get("type") == alert_type
                    and entry.get("stock_code") == stock_code
                    and entry.get("message_hash") == message_hash):
                return True

        return False

    def _record_alert_history(self, alert_type: str, stock_code: str,
                             message_hash: str):
        """Record an alert in the dedup history."""
        history = self._load_alert_history()
        entry = {
            "alert_id": f"{alert_type}_{stock_code}_{message_hash[:8]}_{int(datetime.now().timestamp())}",
            "timestamp": datetime.now().isoformat(),
            "type": alert_type,
            "stock_code": stock_code,
            "message_hash": message_hash,
        }
        history.append(entry)
        self._save_alert_history(history)

    def _extract_stock_code(self, message: str, alert_type: str) -> str:
        """Best-effort extraction of a stock code from the alert message.

        For market-level alerts like regime_change, returns empty string.
        For stock-specific alerts, tries to extract a 4-digit TWSE code.
        """
        # Market-level alert types have no single stock code
        MARKET_LEVEL = {"regime_change", "daily", "crisis", "black_swan", "heartbeat"}
        if alert_type in MARKET_LEVEL:
            return ""

        # Try to find a 4-digit stock code pattern in the message
        match = re.search(r'\b(\d{4})\b', message)
        return match.group(1) if match else ""

    # ================================================================== #
    #  Phase 29: Escalation Rules
    # ================================================================== #

    def _get_severity(self, alert_type: str) -> str:
        """Get the severity level for an alert type.

        Maps alert types to severity: info, warning, or critical.
        Unknown alert types default to 'info' for safety (won't spam).

        Severity routing:
        - info (1): daily digest only (new_entry, take_profit, daily)
        - warning (2): sent immediately, batched (regime_change, signal_detected)
        - critical (3): sent immediately with special formatting (stop_loss_hit)
        """
        return SEVERITY_MAP.get(alert_type, "info")

    def get_severity_level(self, alert_type: str) -> int:
        """Get numeric severity level for an alert type.

        Returns:
            1 for info, 2 for warning, 3 for critical. Default is 1.
        """
        severity = self._get_severity(alert_type)
        return SEVERITY_LEVEL.get(severity, 1)

    def _format_critical_alert(self, message: str, alert_type: str,
                               stock_code: str = "") -> str:
        """Format a critical-level alert with special urgency markers.

        Critical alerts get:
        - Double red circle emoji header
        - UPPERCASE severity tag
        - Timestamp for audit trail
        - Blank line separators for visibility
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = "\U0001f534\U0001f534 CRITICAL ALERT"
        if stock_code:
            header += f" - {stock_code}"
        header += f" [{alert_type}]"

        return (
            f"{header}\n"
            f"  {now_str}\n"
            f"\n"
            f"{message}\n"
            f"\n"
            f"-- End of Critical Alert --"
        )

    # ================================================================== #
    #  Formatting methods (unchanged - backward compatible)
    # ================================================================== #

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
                    lines.append(f"   Pledge: {pledge} | Penalties: {penalties}")

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
        report_date = report.get("date", "unknown")
        stage1 = report.get("stage1", {})
        stage2 = report.get("stage2", {})
        trading = report.get("trading_stats", {})

        # Get candidates from top_10 or candidates
        candidates = stage2.get("top_10", stage2.get("candidates", []))

        lines = [
            f"📊 TW Stock Hunter - {report_date}",
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

    # ================================================================== #
    #  Phase 29: Convenience method for sending typed alerts
    # ================================================================== #

    def send_typed_alert(self, alert_type: str, message: str,
                         stock_code: str = "",
                         date_str: str = None) -> bool:
        """Send a typed alert through the full Phase 29 pipeline.

        This is the recommended entry point for programmatic alert sending.
        It applies deduplication and severity escalation.

        Args:
            alert_type: One of the known types (new_entry, regime_change,
                        stop_loss_hit, take_profit, signal_detected, etc.)
            message: The alert message body
            stock_code: Optional stock code for dedup and formatting
            date_str: Optional date string (defaults to today)

        Returns:
            True if alert was sent, False if suppressed or failed.
        """
        severity = self._get_severity(alert_type)
        message_hash = self._compute_message_hash(message)

        # Deduplication check
        if self._is_duplicate(alert_type, stock_code, message_hash):
            logger.info("Typed alert deduplicated: type=%s stock=%s", alert_type, stock_code)
            return False

        # Route by severity
        if severity == "info":
            if not self.should_alert(alert_type):
                return False
            success = self.send_message(message)
            if success:
                self._record_alert_history(alert_type, stock_code, message_hash)
            return success

        elif severity == "warning":
            success = self.send_message(message)
            if success:
                self._record_alert_history(alert_type, stock_code, message_hash)
            return success

        elif severity == "critical":
            formatted = self._format_critical_alert(message, alert_type, stock_code)
            success = self.send_message(formatted)
            if success:
                self._record_alert_history(alert_type, stock_code, message_hash)
            return success

        return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate and deliver Telegram alerts")
    parser.add_argument("--date", type=str, help="Date to alert (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--dry-run", action="store_true", help="Generate alert but don't send")
    args = parser.parse_args()

    alerts = TelegramAlerts()
    message = alerts.generate_alert(date_str=args.date)

    if message:
        if args.verbose or args.dry_run:
            print("📱 Telegram Alert:")
            print(message)

        if args.dry_run:
            print("\n(dry-run: message not sent)")
        else:
            sent = alerts.deliver_alert(date_str=args.date)
            if sent:
                print("✅ Alert delivered via Telegram")
            else:
                print("⚠ Alert not delivered (rate-limited, deduplicated, holiday, or missing credentials)")
    else:
        if args.verbose:
            print("❌ No alert generated")

    return message


if __name__ == "__main__":
    main()
