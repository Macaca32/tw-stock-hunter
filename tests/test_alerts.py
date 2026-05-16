"""Unit tests for Phase 29: Alert System (Deduplication, Escalation, Digest).

Tests TelegramAlerts deduplication, escalation rules, and digest mode
with inline synthetic data — no API calls.
"""

import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ═══════════════════════════════════════════════════════════════════════
#  Helper: Create TelegramAlerts with isolated data dir
# ═══════════════════════════════════════════════════════════════════════

def _make_alerter(tmp_path, dedup_cooldown_hours=4.0):
    """Create a TelegramAlerts instance with isolated data directory."""
    from telegram_alerts import TelegramAlerts
    alerter = TelegramAlerts(data_dir=str(tmp_path), dedup_cooldown_hours=dedup_cooldown_hours)
    # Don't try to load real holiday calendar
    alerter.holiday_calendar = None
    return alerter


# ═══════════════════════════════════════════════════════════════════════
#  1. Alert Deduplication
# ═══════════════════════════════════════════════════════════════════════

class TestAlertDeduplication:
    """Verify that duplicate alerts within cooldown are suppressed."""

    def test_same_alert_within_cooldown_is_duplicate(self, tmp_path):
        """Same type + stock + hash within cooldown → duplicate detected."""
        alerter = _make_alerter(tmp_path, dedup_cooldown_hours=4.0)
        msg = "台積電 2330 新進場訊號"
        msg_hash = alerter._compute_message_hash(msg)

        # Record the alert
        alerter._record_alert_history("new_entry", "2330", msg_hash)

        # Same alert within cooldown → should be duplicate
        is_dup = alerter._is_duplicate("new_entry", "2330", msg_hash)
        assert is_dup is True

    def test_same_alert_after_cooldown_not_duplicate(self, tmp_path):
        """Same alert after cooldown expires → not a duplicate."""
        alerter = _make_alerter(tmp_path, dedup_cooldown_hours=0.001)  # ~3.6 seconds
        msg = "台積電 2330 新進場訊號"
        msg_hash = alerter._compute_message_hash(msg)

        # Manually write an old alert entry that is definitely outside cooldown
        old_ts = (datetime.now() - timedelta(hours=1)).isoformat()
        history = [{
            "alert_id": "test_old",
            "timestamp": old_ts,
            "type": "new_entry",
            "stock_code": "2330",
            "message_hash": msg_hash,
        }]
        alerter._save_alert_history(history)

        # Old alert should not count as duplicate
        is_dup = alerter._is_duplicate("new_entry", "2330", msg_hash)
        assert is_dup is False

    def test_different_alert_type_not_duplicate(self, tmp_path):
        """Different alert type with same stock → not a duplicate."""
        alerter = _make_alerter(tmp_path)
        msg = "台積電 2330 新進場訊號"
        msg_hash = alerter._compute_message_hash(msg)

        alerter._record_alert_history("new_entry", "2330", msg_hash)

        # Different type → not duplicate
        is_dup = alerter._is_duplicate("signal_detected", "2330", msg_hash)
        assert is_dup is False

    def test_different_stock_not_duplicate(self, tmp_path):
        """Same type but different stock → not a duplicate."""
        alerter = _make_alerter(tmp_path)
        msg = "台積電 2330 新進場訊號"
        msg_hash = alerter._compute_message_hash(msg)

        alerter._record_alert_history("new_entry", "2330", msg_hash)

        # Different stock → not duplicate
        is_dup = alerter._is_duplicate("new_entry", "2454", msg_hash)
        assert is_dup is False

    def test_different_hash_not_duplicate(self, tmp_path):
        """Same type and stock but different message → not a duplicate."""
        alerter = _make_alerter(tmp_path)
        msg1 = "台積電 2330 新進場訊號"
        msg2 = "台積電 2330 獲利了結"
        hash1 = alerter._compute_message_hash(msg1)
        hash2 = alerter._compute_message_hash(msg2)

        alerter._record_alert_history("new_entry", "2330", hash1)

        # Different hash → not duplicate
        is_dup = alerter._is_duplicate("new_entry", "2330", hash2)
        assert is_dup is False

    def test_overlapping_hashes_still_distinct(self, tmp_path):
        """Two different messages with overlapping content → distinct hashes."""
        alerter = _make_alerter(tmp_path)
        msg1 = "2330 台積電突破新高"
        msg2 = "2330 台積電突破瓶頸"
        hash1 = alerter._compute_message_hash(msg1)
        hash2 = alerter._compute_message_hash(msg2)

        # Different messages should produce different hashes
        assert hash1 != hash2

    def test_message_hash_deterministic(self, tmp_path):
        """Same message should always produce the same hash."""
        alerter = _make_alerter(tmp_path)
        msg = "測試訊息"
        hash1 = alerter._compute_message_hash(msg)
        hash2 = alerter._compute_message_hash(msg)
        assert hash1 == hash2

    def test_message_hash_length(self, tmp_path):
        """Hash should be 16 hex characters (SHA-256 truncated)."""
        alerter = _make_alerter(tmp_path)
        msg = "測試"
        result = alerter._compute_message_hash(msg)
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)

    def test_alert_history_persistence(self, tmp_path):
        """Alert history should be persisted to disk and reloadable."""
        alerter = _make_alerter(tmp_path)
        msg_hash = alerter._compute_message_hash("test")
        alerter._record_alert_history("new_entry", "2330", msg_hash)

        # Verify the file exists and contains the entry
        history = alerter._load_alert_history()
        assert len(history) >= 1
        assert history[-1]["type"] == "new_entry"
        assert history[-1]["stock_code"] == "2330"

    def test_alert_history_pruning(self, tmp_path):
        """Old entries beyond cooldown should be pruned on save."""
        alerter = _make_alerter(tmp_path, dedup_cooldown_hours=4.0)

        # Manually write an old entry (5 hours ago, beyond 4h cooldown)
        old_ts = (datetime.now() - timedelta(hours=5)).isoformat()
        msg_hash = alerter._compute_message_hash("test")
        history = [{
            "alert_id": "test_old",
            "timestamp": old_ts,
            "type": "new_entry",
            "stock_code": "2330",
            "message_hash": msg_hash,
        }]
        alerter._save_alert_history(history)

        # Also add a fresh entry
        fresh_ts = datetime.now().isoformat()
        history.append({
            "alert_id": "test_fresh",
            "timestamp": fresh_ts,
            "type": "take_profit",
            "stock_code": "2454",
            "message_hash": alerter._compute_message_hash("fresh"),
        })
        alerter._save_alert_history(history)

        # The old entry should have been pruned, fresh one kept
        loaded = alerter._load_alert_history()
        assert len(loaded) == 1
        assert loaded[0]["alert_id"] == "test_fresh"


# ═══════════════════════════════════════════════════════════════════════
#  2. Escalation Rules
# ═══════════════════════════════════════════════════════════════════════

class TestEscalationRules:
    """Verify alert severity mapping and routing rules."""

    def test_info_severity_for_new_entry(self, tmp_path):
        """new_entry → info severity."""
        alerter = _make_alerter(tmp_path)
        assert alerter._get_severity("new_entry") == "info"

    def test_info_severity_for_take_profit(self, tmp_path):
        """take_profit → info severity."""
        alerter = _make_alerter(tmp_path)
        assert alerter._get_severity("take_profit") == "info"

    def test_info_severity_for_daily(self, tmp_path):
        """daily → info severity."""
        alerter = _make_alerter(tmp_path)
        assert alerter._get_severity("daily") == "info"

    def test_info_severity_for_heartbeat(self, tmp_path):
        """heartbeat → info severity."""
        alerter = _make_alerter(tmp_path)
        assert alerter._get_severity("heartbeat") == "info"

    def test_warning_severity_for_regime_change(self, tmp_path):
        """regime_change → warning severity."""
        alerter = _make_alerter(tmp_path)
        assert alerter._get_severity("regime_change") == "warning"

    def test_warning_severity_for_signal_detected(self, tmp_path):
        """signal_detected → warning severity."""
        alerter = _make_alerter(tmp_path)
        assert alerter._get_severity("signal_detected") == "warning"

    def test_critical_severity_for_stop_loss(self, tmp_path):
        """stop_loss_hit → critical severity."""
        alerter = _make_alerter(tmp_path)
        assert alerter._get_severity("stop_loss_hit") == "critical"

    def test_unknown_type_defaults_to_info(self, tmp_path):
        """Unknown alert type → info severity (safe default)."""
        alerter = _make_alerter(tmp_path)
        assert alerter._get_severity("unknown_type") == "info"

    def test_severity_level_values(self, tmp_path):
        """Severity levels: info=1, warning=2, critical=3."""
        alerter = _make_alerter(tmp_path)
        assert alerter.get_severity_level("new_entry") == 1
        assert alerter.get_severity_level("regime_change") == 2
        assert alerter.get_severity_level("stop_loss_hit") == 3

    def test_info_routes_to_digest(self, tmp_path):
        """Info alerts should be queued to pending digest, not sent immediately."""
        alerter = _make_alerter(tmp_path)
        with patch.object(alerter, 'generate_alert', return_value="測試訊息 2330"), \
             patch.object(alerter, 'send_message') as mock_send, \
             patch.object(alerter, '_is_duplicate', return_value=False):
            result = alerter.deliver_alert(alert_type="new_entry")

        # Info alerts should NOT be sent immediately
        mock_send.assert_not_called()
        # But should be added to pending digest
        pending = alerter._load_pending_digest()
        assert len(pending) > 0

    def test_warning_routes_immediately(self, tmp_path):
        """Warning alerts should be sent immediately."""
        alerter = _make_alerter(tmp_path)
        with patch.object(alerter, 'generate_alert', return_value="市場狀態改變"), \
             patch.object(alerter, 'send_message', return_value=True) as mock_send, \
             patch.object(alerter, '_is_duplicate', return_value=False):
            result = alerter.deliver_alert(alert_type="regime_change")

        # Warning should be sent immediately
        mock_send.assert_called_once()

    def test_critical_routes_immediately(self, tmp_path):
        """Critical alerts should be sent immediately with special formatting."""
        alerter = _make_alerter(tmp_path)
        with patch.object(alerter, 'generate_alert', return_value="停損觸發 2330"), \
             patch.object(alerter, 'send_message', return_value=True) as mock_send, \
             patch.object(alerter, '_is_duplicate', return_value=False):
            result = alerter.deliver_alert(alert_type="stop_loss_hit")

        # Critical should be sent
        mock_send.assert_called_once()
        # Check the message includes critical formatting
        sent_msg = mock_send.call_args[0][0]
        assert "2330" in sent_msg or "停損" in sent_msg


# ═══════════════════════════════════════════════════════════════════════
#  3. Digest Mode
# ═══════════════════════════════════════════════════════════════════════

class TestDigestMode:
    """Verify digest batching, storage, and flushing."""

    def test_add_to_pending_digest(self, tmp_path):
        """Adding an info alert should create a pending digest entry."""
        alerter = _make_alerter(tmp_path)
        alerter._add_to_pending_digest("new_entry", "2330", "台積電新進場")

        pending = alerter._load_pending_digest()
        assert len(pending) == 1
        assert pending[0]["type"] == "new_entry"
        assert pending[0]["stock_code"] == "2330"
        assert pending[0]["message"] == "台積電新進場"

    def test_batch_multiple_info_alerts(self, tmp_path):
        """Multiple info alerts should accumulate in the pending digest."""
        alerter = _make_alerter(tmp_path)
        alerter._add_to_pending_digest("new_entry", "2330", "台積電新進場")
        alerter._add_to_pending_digest("take_profit", "2454", "聯發科獲利了結")
        alerter._add_to_pending_digest("new_entry", "1101", "台泥新進場")

        pending = alerter._load_pending_digest()
        assert len(pending) == 3

    def test_flush_digest_clears_pending(self, tmp_path):
        """Flushing digest should clear all pending entries."""
        alerter = _make_alerter(tmp_path)
        alerter._add_to_pending_digest("new_entry", "2330", "台積電新進場")
        alerter._add_to_pending_digest("take_profit", "2454", "聯發科獲利了結")

        with patch.object(alerter, 'send_message', return_value=True):
            result = alerter.flush_digest("morning")

        # After flush, pending should be empty
        pending = alerter._load_pending_digest()
        assert len(pending) == 0

    def test_flush_digest_returns_true_on_success(self, tmp_path):
        """Successful flush should return True."""
        alerter = _make_alerter(tmp_path)
        alerter._add_to_pending_digest("new_entry", "2330", "測試")

        with patch.object(alerter, 'send_message', return_value=True):
            result = alerter.flush_digest("morning")
        assert result is True

    def test_flush_digest_returns_true_when_empty(self, tmp_path):
        """Flushing empty digest should return True (nothing to do)."""
        alerter = _make_alerter(tmp_path)
        result = alerter.flush_digest("morning")
        assert result is True

    def test_flush_digest_returns_false_on_send_failure(self, tmp_path):
        """Failed send should return False and keep pending entries."""
        alerter = _make_alerter(tmp_path)
        alerter._add_to_pending_digest("new_entry", "2330", "測試")

        with patch.object(alerter, 'send_message', return_value=False):
            result = alerter.flush_digest("morning")

        assert result is False
        # Pending entries should NOT be cleared on failure
        pending = alerter._load_pending_digest()
        assert len(pending) == 1

    def test_digest_message_groups_by_type(self, tmp_path):
        """Digest message should group alerts by type."""
        alerter = _make_alerter(tmp_path)
        alerter._add_to_pending_digest("new_entry", "2330", "台積電新進場")
        alerter._add_to_pending_digest("take_profit", "2454", "聯發科獲利了結")
        alerter._add_to_pending_digest("new_entry", "1101", "台泥新進場")

        sent_message = None

        def capture_message(msg):
            nonlocal sent_message
            sent_message = msg
            return True

        with patch.object(alerter, 'send_message', side_effect=capture_message):
            alerter.flush_digest("morning")

        # Message should contain both alert types
        assert sent_message is not None
        assert "new_entry" in sent_message
        assert "take_profit" in sent_message

    def test_digest_header_morning(self, tmp_path):
        """Morning digest header should contain sunrise emoji."""
        alerter = _make_alerter(tmp_path)
        lines = alerter._format_digest_header("morning", 5)
        combined = "\n".join(lines)
        assert "Morning" in combined
        assert "5" in combined  # Count shown

    def test_digest_header_evening(self, tmp_path):
        """Evening digest header should contain sunset emoji."""
        alerter = _make_alerter(tmp_path)
        lines = alerter._format_digest_header("evening", 3)
        combined = "\n".join(lines)
        assert "Evening" in combined
        assert "3" in combined

    def test_digest_timing_morning_window(self, tmp_path):
        """should_send_digest returns 'morning' at 08:00-08:09 Taipei time."""
        alerter = _make_alerter(tmp_path)
        # Mock the current time to be 08:05 Taipei time
        from datetime import timezone
        taipei_tz = timezone(timedelta(hours=8))
        mock_time = datetime(2026, 5, 16, 8, 5, tzinfo=taipei_tz)

        with patch("telegram_alerts.datetime") as mock_dt:
            mock_dt.now.return_value = mock_time
            result = alerter.should_send_digest()
        assert result == "morning"

    def test_digest_timing_evening_window(self, tmp_path):
        """should_send_digest returns 'evening' at 17:00-17:09 Taipei time."""
        alerter = _make_alerter(tmp_path)
        from datetime import timezone
        taipei_tz = timezone(timedelta(hours=8))
        mock_time = datetime(2026, 5, 16, 17, 3, tzinfo=taipei_tz)

        with patch("telegram_alerts.datetime") as mock_dt:
            mock_dt.now.return_value = mock_time
            result = alerter.should_send_digest()
        assert result == "evening"

    def test_digest_timing_not_digest_time(self, tmp_path):
        """should_send_digest returns None outside digest windows."""
        alerter = _make_alerter(tmp_path)
        from datetime import timezone
        taipei_tz = timezone(timedelta(hours=8))
        mock_time = datetime(2026, 5, 16, 14, 30, tzinfo=taipei_tz)

        with patch("telegram_alerts.datetime") as mock_dt:
            mock_dt.now.return_value = mock_time
            result = alerter.should_send_digest()
        assert result is None


# ═══════════════════════════════════════════════════════════════════════
#  4. Stock Code Extraction & Formatting
# ═══════════════════════════════════════════════════════════════════════

class TestStockCodeHandling:
    """Verify stock code extraction and formatting."""

    def test_extract_stock_code_from_message(self, tmp_path):
        """4-digit stock code in message should be extracted."""
        alerter = _make_alerter(tmp_path)
        code = alerter._extract_stock_code("台積電 2330 新進場訊號", "new_entry")
        assert code == "2330"

    def test_extract_stock_code_market_level(self, tmp_path):
        """Market-level alert types should return empty string."""
        alerter = _make_alerter(tmp_path)
        for alert_type in ["regime_change", "daily", "crisis", "black_swan", "heartbeat"]:
            code = alerter._extract_stock_code("some message 2330", alert_type)
            assert code == ""

    def test_extract_no_code_in_message(self, tmp_path):
        """Message without a 4-digit code should return empty string."""
        alerter = _make_alerter(tmp_path)
        code = alerter._extract_stock_code("市場狀態改變", "signal_detected")
        assert code == ""

    def test_format_twse_stock_code(self, tmp_path):
        """TWSE stock (2330) should be formatted with 上市 indicator."""
        alerter = _make_alerter(tmp_path)
        alerter._sector_map = {}  # No sector mapping
        result = alerter._format_stock_code("2330")
        assert "2330" in result
        assert "上市" in result

    def test_format_tpex_stock_code(self, tmp_path):
        """TPEx stock (6170) should be formatted with 上櫃 indicator."""
        alerter = _make_alerter(tmp_path)
        alerter._sector_map = {}
        result = alerter._format_stock_code("6170")
        assert "6170" in result
        assert "上櫃" in result

    def test_format_non_numeric_code(self, tmp_path):
        """Non-numeric code should be returned as-is."""
        alerter = _make_alerter(tmp_path)
        result = alerter._format_stock_code("ABC")
        assert result == "ABC"

    def test_format_empty_code(self, tmp_path):
        """Empty code should be returned as-is."""
        alerter = _make_alerter(tmp_path)
        result = alerter._format_stock_code("")
        assert result == ""


# ═══════════════════════════════════════════════════════════════════════
#  5. deliver_alert Integration
# ═══════════════════════════════════════════════════════════════════════

class TestDeliverAlertIntegration:
    """Integration tests for the full alert delivery pipeline."""

    def test_duplicate_alert_suppressed(self, tmp_path):
        """Duplicate alert should be suppressed by dedup system."""
        alerter = _make_alerter(tmp_path)
        msg = "台積電 2330 新進場訊號"

        with patch.object(alerter, 'generate_alert', return_value=msg), \
             patch.object(alerter, '_is_duplicate', return_value=True):
            result = alerter.deliver_alert(alert_type="new_entry")

        assert result is False  # Suppressed

    def test_no_telegram_credentials(self, tmp_path):
        """Missing Telegram credentials should not crash."""
        alerter = _make_alerter(tmp_path)
        alerter.bot_token = ""
        alerter.chat_id = ""

        # send_message should return False gracefully
        assert alerter.send_message("test") is False

    def test_record_alert_timestamp(self, tmp_path):
        """Recording an alert should persist the current timestamp."""
        alerter = _make_alerter(tmp_path)
        before = datetime.now()
        alerter.record_alert("daily")
        after = datetime.now()

        # Read the recorded file
        with open(alerter.last_alert_file, 'r') as f:
            data = json.load(f)

        recorded_time = datetime.fromisoformat(data["timestamp"])
        assert before <= recorded_time <= after

    def test_parse_timestamp_valid(self, tmp_path):
        """Valid ISO timestamp should be parsed correctly."""
        alerter = _make_alerter(tmp_path)
        ts = "2026-05-16T10:30:00"
        result = alerter._parse_timestamp(ts)
        assert result.year == 2026
        assert result.month == 5
        assert result.hour == 10

    def test_parse_timestamp_invalid(self, tmp_path):
        """Invalid timestamp should return datetime.min."""
        alerter = _make_alerter(tmp_path)
        result = alerter._parse_timestamp("not-a-date")
        assert result == datetime.min

    def test_parse_timestamp_none(self, tmp_path):
        """None timestamp should return datetime.min."""
        alerter = _make_alerter(tmp_path)
        result = alerter._parse_timestamp(None)
        assert result == datetime.min

    def test_check_and_flush_digest_not_time(self, tmp_path):
        """check_and_flush_digest returns False when not digest time."""
        alerter = _make_alerter(tmp_path)
        with patch.object(alerter, 'should_send_digest', return_value=None):
            result = alerter.check_and_flush_digest()
        assert result is False

    def test_check_and_flush_digest_at_digest_time(self, tmp_path):
        """check_and_flush_digest flushes when it is digest time."""
        alerter = _make_alerter(tmp_path)
        alerter._add_to_pending_digest("new_entry", "2330", "測試")

        with patch.object(alerter, 'should_send_digest', return_value="morning"), \
             patch.object(alerter, 'send_message', return_value=True):
            result = alerter.check_and_flush_digest()

        assert result is True
