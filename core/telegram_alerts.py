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
- Phase 29: Daily Digest Mode - batch info alerts into morning/evening digests
- Phase 29: Smart Alert Formatting - Taiwan market conventions, Traditional Chinese
"""

import hashlib
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
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

# -- Phase 29: Regime name translations (Traditional Chinese) --
REGIME_TC = {
    "normal": "\u5e38\u614b",       # 常態
    "caution": "\u8b66\u6212",      # 警戒
    "stress": "\u58d3\u529b",       # 壓力
    "crisis": "\u5371\u6a5f",       # 危機
    "black_swan": "\u9ed1\u5929\u9d5d",  # 黑天鵝
    "unknown": "\u672a\u77e5",      # 未知
}

# -- Phase 29: Sector indicators for stock codes --
# TWSE stocks: 1xxx-8xxx (TWSE 上市)
# TPEx stocks: 6xxx-9xxx (櫃買 上櫃)
SECTOR_INDICATORS = {
    "twse": "\u4e0a\u5e02",  # 上市
    "tpex": "\u4e0a\u6ac3",  # 上櫃
}


class TelegramAlerts:
    def __init__(self, data_dir=None, dedup_cooldown_hours: float = 4.0):
        self.data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent / "data"
        self.reports_dir = Path(__file__).parent.parent / "reports"
        self.last_alert_file = self.data_dir / "last_alert.json"
        self.cooldown_minutes = 30  # Min time between alerts

        # Phase 29: Alert deduplication - track recent alerts in history file
        self.alert_history_file = self.data_dir / "alert_history.json"
        self.dedup_cooldown_hours = dedup_cooldown_hours  # Default 4 hours

        # Phase 29: Daily digest - pending info-level alerts
        self.pending_digest_file = self.data_dir / "pending_digest.json"

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

        # Phase 29: Lazy-loaded sector mapping
        self._sector_map = None

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

        Phase 29: Now routes through the escalation and digest system.
        Info-level alerts are batched into the daily digest. Warning/critical
        go immediately. Backward compatible - existing callers using
        alert_type="daily" still work (daily -> info -> digest).

        Returns True if the alert was sent (or queued for digest), False otherwise.
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
            # Info alerts go to digest - not sent immediately
            self._add_to_pending_digest(alert_type, stock_code, message)
            # Still record for rate-limiting / dedup purposes
            self.record_alert(alert_type)
            self._record_alert_history(alert_type, stock_code, message_hash)
            self.update_last_alert_with_regime()
            logger.info("Info alert queued for digest: type=%s", alert_type)
            return True  # Queued successfully

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
            # Phase 29: Use smart stock code formatting
            formatted_code = self._format_stock_code(stock_code)
            header += f" - {formatted_code}"
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
    #  Phase 29: Daily Digest Mode
    # ================================================================== #

    def _load_pending_digest(self) -> List[Dict]:
        """Load pending info-level alerts from data/pending_digest.json."""
        if not self.pending_digest_file.exists():
            return []
        try:
            with open(self.pending_digest_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []

    def _save_pending_digest(self, pending: List[Dict]):
        """Persist pending digest alerts."""
        self.pending_digest_file.parent.mkdir(exist_ok=True)
        with open(self.pending_digest_file, 'w', encoding='utf-8') as f:
            json.dump(pending, f, ensure_ascii=False, indent=2)

    def _add_to_pending_digest(self, alert_type: str, stock_code: str,
                               message: str):
        """Add an info-level alert to the pending digest queue.

        Info alerts are not sent immediately. They are accumulated in
        data/pending_digest.json and flushed at digest times
        (morning 08:00 and evening 17:00 Taipei time).
        """
        pending = self._load_pending_digest()
        entry = {
            "timestamp": datetime.now().isoformat(),
            "type": alert_type,
            "stock_code": stock_code,
            "message": message,
        }
        pending.append(entry)
        self._save_pending_digest(pending)

    def should_send_digest(self) -> Optional[str]:
        """Check if it's time to send a digest (morning or evening).

        Morning digest: 08:00 Taipei time (UTC+8)
        Evening digest: 17:00 Taipei time (UTC+8)

        Returns:
            'morning', 'evening', or None if not a digest time.
            Uses a 10-minute window (HH:00 to HH:10) to account for
            scheduling jitter.
        """
        taipei_tz = timezone(timedelta(hours=8))
        now_taipei = datetime.now(taipei_tz)
        hour = now_taipei.hour
        minute = now_taipei.minute

        if hour == 8 and minute < 10:
            return "morning"
        elif hour == 17 and minute < 10:
            return "evening"
        return None

    def flush_digest(self, digest_type: str = "morning") -> bool:
        """Flush pending info alerts as a formatted digest message.

        Args:
            digest_type: 'morning' or 'evening' - affects header.

        Returns:
            True if the digest was sent successfully or there was nothing
            to send, False on send failure.
        """
        pending = self._load_pending_digest()
        if not pending:
            logger.info("No pending digest alerts to flush")
            return True

        # Build the digest message
        lines = self._format_digest_header(digest_type, len(pending))

        # Group pending alerts by type for cleaner presentation
        by_type: Dict[str, List[Dict]] = {}
        for entry in pending:
            t = entry.get("type", "unknown")
            by_type.setdefault(t, []).append(entry)

        for alert_type, entries in by_type.items():
            lines.append(f"")
            lines.append(f"-- {alert_type} ({len(entries)} items) --")
            for entry in entries:
                ts = entry.get("timestamp", "")[:16]  # Trim seconds
                code = entry.get("stock_code", "")
                # Phase 29: Smart stock code formatting in digest
                code_label = f" [{self._format_stock_code(code)}]" if code else ""
                # Show first 80 chars of message to keep digest readable
                msg_preview = entry.get("message", "")[:80]
                if len(entry.get("message", "")) > 80:
                    msg_preview += "..."
                lines.append(f"  * {ts}{code_label}: {msg_preview}")

        lines.append(f"")
        lines.append(f"-- Total: {len(pending)} alerts | {digest_type} digest --")

        message = "\n".join(lines)
        success = self.send_message(message)

        if success:
            # Clear the pending digest
            self._save_pending_digest([])
            logger.info("Digest flushed: type=%s alerts=%d", digest_type, len(pending))
        return success

    def _format_digest_header(self, digest_type: str, count: int) -> List[str]:
        """Build the header lines for a digest message.

        Args:
            digest_type: 'morning' or 'evening'
            count: Number of pending alerts in the digest
        """
        taipei_tz = timezone(timedelta(hours=8))
        now_taipei = datetime.now(taipei_tz)
        date_str = now_taipei.strftime("%Y-%m-%d")

        if digest_type == "morning":
            emoji = "\U0001f305"  # sunrise
            label = "Morning Digest"
        else:
            emoji = "\U0001f306"  # sunset
            label = "Evening Digest"

        lines = [
            f"{emoji} TW Stock Hunter {label}",
            f"  {date_str}",
            f"  Pending alerts: {count}",
        ]
        return lines

    def check_and_flush_digest(self) -> bool:
        """Convenience method: check if it's digest time and flush if so.

        Returns True if a digest was sent, False otherwise.
        """
        digest_type = self.should_send_digest()
        if digest_type is None:
            return False
        return self.flush_digest(digest_type)

    # ================================================================== #
    #  Phase 29: Smart Alert Formatting - Taiwan Market Conventions
    # ================================================================== #

    def _get_sector_map(self) -> Dict[str, str]:
        """Lazy-load the sector mapping from sectors module."""
        if self._sector_map is None:
            try:
                from sectors import load_sector_mapping
                self._sector_map = load_sector_mapping(str(self.data_dir))
            except ImportError:
                self._sector_map = {}
        return self._sector_map

    def _format_stock_code(self, code: str) -> str:
        """Format a stock code with sector indicator.

        Taiwan market convention: show the listing board alongside the code.
        TWSE (上市) stocks: 1xxx-5xxx, 8xxx
        TPEx (上櫃) stocks: 6xxx-9xxx

        Example: 2330 -> 2330(上市)  or  6170 -> 6170(上櫃)
        """
        if not code or not code.isdigit():
            return code

        try:
            code_int = int(code)
        except ValueError:
            return code

        # TPEx range: 6000-7999 and >= 9900
        if (6000 <= code_int <= 7999) or code_int >= 9900:
            board = SECTOR_INDICATORS["tpex"]
        else:
            board = SECTOR_INDICATORS["twse"]

        # Add sector tag from sectors module
        sector_map = self._get_sector_map()
        sector = sector_map.get(code, "")

        if sector:
            return f"{code}({board}) [{sector}]"
        return f"{code}({board})"

    def _format_price_change(self, current_price: float,
                             previous_price: float = 0.0,
                             change_pct: float = 0.0) -> str:
        """Format a price change using Taiwan market conventions.

        Shows NT$ amount as primary indicator, percentage as secondary.
        This matches how Taiwanese investors discuss price movements:
        "漲了3.5元" (up NT$3.5) rather than just "+1.2%".

        Args:
            current_price: Current price in NT$
            previous_price: Previous close price in NT$ (0 if unavailable)
            change_pct: Percentage change (used if previous_price unavailable)

        Returns:
            Formatted string like "NT$985.0 (+3.5 / +0.36%)" or
            "NT$985.0 (+0.36%)"
        """
        if previous_price > 0:
            change_ntd = current_price - previous_price
            if change_pct == 0.0 and previous_price > 0:
                change_pct = (change_ntd / previous_price) * 100

            sign = "+" if change_ntd >= 0 else ""
            return f"NT${current_price:.1f} ({sign}{change_ntd:.1f} / {sign}{change_pct:.2f}%)"
        else:
            sign = "+" if change_pct >= 0 else ""
            return f"NT${current_price:.1f} ({sign}{change_pct:.2f}%)"

    def _format_regime_tc(self, regime: str) -> str:
        """Format regime name in Traditional Chinese for Telegram display.

        Maps English regime names to Traditional Chinese equivalents
        commonly used in Taiwanese financial media:
          normal -> 常態, caution -> 警戒, stress -> 壓力,
          crisis -> 危機, black_swan -> 黑天鵝
        """
        return REGIME_TC.get(regime, regime)

    # ================================================================== #
    #  Formatting methods - enhanced with Phase 29 smart formatting
    # ================================================================== #

    def format_top_candidates(self, candidates, limit=5):
        """Format top candidates for Telegram (no markdown tables)

        Phase 29: Now includes sector indicator on stock codes and
        NT$ price formatting when close price is available.
        """
        lines = []

        for i, c in enumerate(candidates[:limit], 1):
            code = c.get("code", "")
            name = c.get("name", "")
            combined = c.get("combined_score", c.get("composite_score", 0))
            s1 = c.get("stage1_score", "")
            s2 = c.get("stage2_score", "")

            # Phase 29: Smart stock code formatting with board indicator
            formatted_code = self._format_stock_code(code)

            lines.append(f"{i}. {formatted_code} {name}")
            lines.append(f"   Score: {combined} (S1:{s1} S2:{s2})")

            # Phase 29: Show price in NT$ when available
            close = c.get("close") or c.get("adj_close")
            if close and float(close) > 0:
                prev_close = float(c.get("prev_close", 0))
                change_pct = float(c.get("change_pct", 0))
                if prev_close > 0 or change_pct != 0:
                    price_str = self._format_price_change(
                        float(close), prev_close, change_pct)
                else:
                    price_str = f"NT${float(close):.1f}"
                lines.append(f"   {price_str}")

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
        """Format regime info for Telegram

        Phase 29: Uses Traditional Chinese regime names for display.
        """
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

        # Phase 29: Traditional Chinese regime name
        regime_tc = self._format_regime_tc(regime)

        return (f"{emoji.get(regime, '❓')} 市場狀態: {regime_tc} ({regime})\n"
                f"   趨勢: {trend} | 波動率: {volatility:.4f}")

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
        """Generate alert message

        Phase 29: Regime change notifications now use Traditional Chinese
        names for better readability on Telegram.
        """
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
            # Phase 29: Use Traditional Chinese for regime names
            old_tc = self._format_regime_tc(regime_change['old'])
            new_tc = self._format_regime_tc(regime_change['new'])
            message = (
                f"⚠️ 市場狀態變更: {old_tc} → {new_tc}\n"
                f"   ({regime_change['old']} → {regime_change['new']})\n\n"
                + message
            )

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
        It applies deduplication, severity escalation, and digest routing.

        Args:
            alert_type: One of the known types (new_entry, regime_change,
                        stop_loss_hit, take_profit, signal_detected, etc.)
            message: The alert message body
            stock_code: Optional stock code for dedup and formatting
            date_str: Optional date string (defaults to today)

        Returns:
            True if alert was sent or queued, False if suppressed or failed.
        """
        severity = self._get_severity(alert_type)
        message_hash = self._compute_message_hash(message)

        # Deduplication check
        if self._is_duplicate(alert_type, stock_code, message_hash):
            logger.info("Typed alert deduplicated: type=%s stock=%s", alert_type, stock_code)
            return False

        # Route by severity
        if severity == "info":
            self._add_to_pending_digest(alert_type, stock_code, message)
            self._record_alert_history(alert_type, stock_code, message_hash)
            logger.info("Info alert queued for digest: type=%s stock=%s", alert_type, stock_code)
            return True

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
    parser.add_argument("--flush-digest", action="store=True",
                        help="Flush pending info alerts as a digest")
    parser.add_argument("--digest-type", choices=["morning", "evening"],
                        default="morning", help="Digest type for --flush-digest")
    args = parser.parse_args()

    alerts = TelegramAlerts()

    if args.flush_digest:
        sent = alerts.flush_digest(args.digest_type)
        if sent:
            print(f"✅ {args.digest_type.capitalize()} digest sent")
        else:
            print(f"⚠ {args.digest_type.capitalize()} digest not sent")
        return

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
                print("✅ Alert delivered via Telegram (or queued for digest)")
            else:
                print("⚠ Alert not delivered (rate-limited, deduplicated, holiday, or missing credentials)")
    else:
        if args.verbose:
            print("❌ No alert generated")

    return message


if __name__ == "__main__":
    main()
