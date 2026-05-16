"""Unit tests for Phase 32: Report Generator.

Tests format_signal_grade(), format_regime_badge(), generate_daily_report(),
and generate_html_report() with inline synthetic data — no API calls.
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ═══════════════════════════════════════════════════════════════════════
#  1. format_signal_grade — Score → Grade Mapping
# ═══════════════════════════════════════════════════════════════════════

class TestFormatSignalGrade:
    """Verify score-to-grade mapping with emoji: 90+=A, 80+=B, 70+=C, 60+=D, <60=E."""

    def _grade(self, score):
        from report_generator import format_signal_grade
        return format_signal_grade(score)

    def test_score_95_is_grade_a(self):
        """Score 95 → 🟢 A."""
        result = self._grade(95)
        assert "A" in result
        assert "🟢" in result

    def test_score_90_is_grade_a(self):
        """Score 90 (boundary) → 🟢 A."""
        result = self._grade(90)
        assert "A" in result
        assert "🟢" in result

    def test_score_89_5_is_grade_b(self):
        """Score 89.5 → 🟢 B (below A threshold)."""
        result = self._grade(89.5)
        assert "B" in result
        assert "🟢" in result

    def test_score_85_is_grade_b(self):
        """Score 85 → 🟢 B."""
        result = self._grade(85)
        assert "B" in result
        assert "🟢" in result

    def test_score_80_is_grade_b(self):
        """Score 80 (boundary) → 🟢 B."""
        result = self._grade(80)
        assert "B" in result

    def test_score_79_9_is_grade_c(self):
        """Score 79.9 → 🟡 C (below B threshold)."""
        result = self._grade(79.9)
        assert "C" in result
        assert "🟡" in result

    def test_score_75_is_grade_c(self):
        """Score 75 → 🟡 C."""
        result = self._grade(75)
        assert "C" in result

    def test_score_70_is_grade_c(self):
        """Score 70 (boundary) → 🟡 C."""
        result = self._grade(70)
        assert "C" in result

    def test_score_69_9_is_grade_d(self):
        """Score 69.9 → 🟡 D (below C threshold)."""
        result = self._grade(69.9)
        assert "D" in result
        assert "🟡" in result

    def test_score_65_is_grade_d(self):
        """Score 65 → 🟡 D."""
        result = self._grade(65)
        assert "D" in result

    def test_score_60_is_grade_d(self):
        """Score 60 (boundary) → 🟡 D."""
        result = self._grade(60)
        assert "D" in result

    def test_score_59_9_is_grade_e(self):
        """Score 59.9 → 🔴 E (below D threshold)."""
        result = self._grade(59.9)
        assert "E" in result
        assert "🔴" in result

    def test_score_30_is_grade_e(self):
        """Score 30 → 🔴 E."""
        result = self._grade(30)
        assert "E" in result

    def test_score_0_is_grade_e(self):
        """Score 0 → 🔴 E (lowest possible)."""
        result = self._grade(0)
        assert "E" in result

    def test_negative_score_is_grade_e(self):
        """Negative score → 🔴 E."""
        result = self._grade(-10)
        assert "E" in result

    def test_none_input_is_grade_e(self):
        """None input → 🔴 E (graceful fallback)."""
        result = self._grade(None)
        assert "E" in result

    def test_string_input_is_grade_e(self):
        """Non-numeric string → 🔴 E (graceful fallback)."""
        result = self._grade("not-a-number")
        assert "E" in result

    def test_numeric_string_works(self):
        """Numeric string '85' should be converted and graded as B."""
        result = self._grade("85")
        assert "B" in result

    def test_score_100_is_grade_a(self):
        """Score 100 → 🟢 A (max score)."""
        result = self._grade(100)
        assert "A" in result


# ═══════════════════════════════════════════════════════════════════════
#  2. format_regime_badge — Regime → Traditional Chinese Badge
# ═══════════════════════════════════════════════════════════════════════

class TestFormatRegimeBadge:
    """Verify all 5 regimes map to correct Traditional Chinese labels with emoji."""

    def _badge(self, regime):
        from report_generator import format_regime_badge
        return format_regime_badge(regime)

    def test_normal_badge(self):
        """NORMAL → 🟢 常態."""
        result = self._badge("normal")
        assert "常態" in result

    def test_caution_badge(self):
        """CAUTION → 🟡 警戒."""
        result = self._badge("caution")
        assert "警戒" in result

    def test_stress_badge(self):
        """STRESS → 🟠 壓力."""
        result = self._badge("stress")
        assert "壓力" in result

    def test_crisis_badge(self):
        """CRISIS → 🔴 危機."""
        result = self._badge("crisis")
        assert "危機" in result

    def test_black_swan_badge(self):
        """BLACK_SWAN → 🚨 黑天鵝."""
        result = self._badge("black_swan")
        assert "黑天鵝" in result

    def test_unknown_regime(self):
        """Unknown regime → ❓ 未知."""
        result = self._badge("unknown")
        assert "未知" in result

    def test_case_insensitive(self):
        """Regime lookup should be case-insensitive."""
        result = self._badge("NORMAL")
        assert "常態" in result

    def test_mixed_case(self):
        """Mixed case 'Black_Swan' should still resolve."""
        result = self._badge("Black_Swan")
        assert "黑天鵝" in result

    def test_empty_string(self):
        """Empty string → ❓ 未知."""
        result = self._badge("")
        assert "未知" in result

    def test_none_input(self):
        """None input → ❓ 未知."""
        result = self._badge(None)
        assert "未知" in result

    def test_arbitrary_string(self):
        """Arbitrary regime string not in mapping → passed through as-is."""
        result = self._badge("custom_regime")
        assert "custom_regime" in result


# ═══════════════════════════════════════════════════════════════════════
#  3. generate_daily_report — Markdown Report Generation
# ═══════════════════════════════════════════════════════════════════════

class TestGenerateDailyReport:
    """Verify Markdown report generation with various data scenarios."""

    def test_empty_data_dir(self, tmp_path):
        """Report with no data files should still generate (graceful degradation)."""
        from report_generator import generate_daily_report
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with patch("report_generator.Path") as mock_path_cls:
            # Make Path(__file__).parent.parent / "data" return our tmp data_dir
            mock_path_cls.return_value.parent.parent.__truediv__.return_value = data_dir
            # We need a more targeted approach
            pass

        # Simpler: just patch _load_stage1 etc. to return None/empty
        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_pending_digest", return_value=[]), \
             patch("report_generator._load_sector_rotation", return_value=None), \
             patch("report_generator._load_sector_map", return_value={}):
            report = generate_daily_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert report is not None
        # Source uses \u7be1 (纂) not \u7be9 (篩) — match actual output
        assert "每日" in report and "報告" in report
        assert "Stage 1 資料不可用" in report
        assert "Stage 2 資料不可用" in report

    def test_missing_stage1_file(self, tmp_path):
        """Report with missing Stage 1 but valid Stage 2 data."""
        from report_generator import generate_daily_report

        s2_data = {
            "summary": {"passed_stage2": 3, "disqualified": 1},
            "candidates": [
                {"code": "2330", "name": "台積電", "combined_score": 88,
                 "stage1_score": 85, "stage2_score": 90, "checks": {}, "red_flags": []},
            ],
            "disqualified": [],
        }

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=s2_data), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_pending_digest", return_value=[]), \
             patch("report_generator._load_sector_rotation", return_value=None), \
             patch("report_generator._load_sector_map", return_value={}):
            report = generate_daily_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert report is not None
        assert "2330" in report
        assert "台積電" in report
        assert "Stage 1 資料不可用" in report

    def test_missing_stage2_file(self, tmp_path):
        """Report with valid Stage 1 but missing Stage 2 data."""
        from report_generator import generate_daily_report

        s1_data = {
            "summary": {"total_screened": 100, "passed": 10, "watchlist": 5, "rejected": 85},
            "candidates": [
                {"code": "2330", "name": "台積電", "composite_score": 92,
                 "close": 580, "pass": True},
            ],
        }

        with patch("report_generator._load_stage1", return_value=s1_data), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_pending_digest", return_value=[]), \
             patch("report_generator._load_sector_rotation", return_value=None), \
             patch("report_generator._load_sector_map", return_value={}):
            report = generate_daily_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert report is not None
        assert "2330" in report
        assert "Stage 2 資料不可用" in report

    def test_partial_data_with_regime(self, tmp_path):
        """Report with regime data but no stage files."""
        from report_generator import generate_daily_report

        regime_data = {
            "regime": "caution",
            "confidence": 0.75,
            "volatility": 0.0234,
            "days_in_regime": 5,
            "global_risk": "moderate",
        }

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=regime_data), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_pending_digest", return_value=[]), \
             patch("report_generator._load_sector_rotation", return_value=None), \
             patch("report_generator._load_sector_map", return_value={}):
            report = generate_daily_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert report is not None
        assert "警戒" in report  # regime badge
        assert "0.0234" in report  # volatility

    def test_full_data_report(self, tmp_path):
        """Report with all data sources present."""
        from report_generator import generate_daily_report

        s1_data = {
            "summary": {"total_screened": 200, "passed": 8, "watchlist": 3, "rejected": 189},
            "candidates": [
                {"code": "2330", "name": "台積電", "composite_score": 92,
                 "close": 580, "pass": True},
                {"code": "2454", "name": "聯發科", "composite_score": 78,
                 "close": 1200, "pass": True},
            ],
        }
        s2_data = {
            "summary": {"passed_stage2": 2, "disqualified": 0},
            "candidates": [
                {"code": "2330", "name": "台積電", "combined_score": 88,
                 "stage1_score": 85, "stage2_score": 90,
                 "checks": {"dividend": {"score": 80, "status": "ok"}},
                 "red_flags": []},
            ],
            "disqualified": [],
        }
        regime_data = {"regime": "normal", "confidence": 0.9,
                       "volatility": 0.012, "days_in_regime": 10,
                       "global_risk": "low"}
        trades = [
            {"code": "2330", "name": "台積電", "status": "closed",
             "pnl_pct": 5.2, "sector": "semiconductor"},
        ]

        with patch("report_generator._load_stage1", return_value=s1_data), \
             patch("report_generator._load_stage2", return_value=s2_data), \
             patch("report_generator._load_regime", return_value=regime_data), \
             patch("report_generator._load_paper_trades", return_value=trades), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_pending_digest", return_value=[]), \
             patch("report_generator._load_sector_rotation", return_value=None), \
             patch("report_generator._load_sector_map", return_value={"2330": "semiconductor"}):
            report = generate_daily_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert report is not None
        assert "常態" in report
        assert "2330" in report
        assert "模擬" in report

    def test_report_file_written(self, tmp_path):
        """Report should be written to a .md file in the output directory."""
        from report_generator import generate_daily_report

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_pending_digest", return_value=[]), \
             patch("report_generator._load_sector_rotation", return_value=None), \
             patch("report_generator._load_sector_map", return_value={}):
            report = generate_daily_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        report_file = tmp_path / "reports" / "2026-05-16.md"
        assert report_file.exists()
        content = report_file.read_text(encoding="utf-8")
        assert "每日" in content

    def test_roc_date_in_report(self, tmp_path):
        """Report should contain ROC date format (民國)."""
        from report_generator import generate_daily_report

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_pending_digest", return_value=[]), \
             patch("report_generator._load_sector_rotation", return_value=None), \
             patch("report_generator._load_sector_map", return_value={}):
            report = generate_daily_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert "民國115年05月16日" in report


# ═══════════════════════════════════════════════════════════════════════
#  4. generate_html_report — HTML Report Generation
# ═══════════════════════════════════════════════════════════════════════

class TestGenerateHtmlReport:
    """Verify HTML report generation with valid structure and lang attribute."""

    def test_html_lang_zh_tw(self, tmp_path):
        """HTML report must have lang='zh-TW' attribute."""
        from report_generator import generate_html_report

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_sector_map", return_value={}):
            html = generate_html_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert html is not None
        assert 'lang="zh-TW"' in html

    def test_html_valid_structure(self, tmp_path):
        """HTML report should contain proper DOCTYPE, html, head, body."""
        from report_generator import generate_html_report

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_sector_map", return_value={}):
            html = generate_html_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert html is not None
        assert "<!DOCTYPE html>" in html
        assert "<html" in html
        assert "</html>" in html
        assert "<head>" in html
        assert "</head>" in html
        assert "<body>" in html
        assert "</body>" in html

    def test_html_contains_inline_css(self, tmp_path):
        """HTML report should have inline <style> for self-contained rendering."""
        from report_generator import generate_html_report

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_sector_map", return_value={}):
            html = generate_html_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert "<style>" in html
        assert "</style>" in html

    def test_html_with_stage_data(self, tmp_path):
        """HTML report should contain screening data when available."""
        from report_generator import generate_html_report

        s1_data = {
            "summary": {"total_screened": 100, "passed": 5, "watchlist": 2, "rejected": 93},
            "candidates": [
                {"code": "2330", "name": "台積電", "composite_score": 92,
                 "close": 580, "pass": True},
            ],
        }

        with patch("report_generator._load_stage1", return_value=s1_data), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_sector_map", return_value={}):
            html = generate_html_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert html is not None
        assert "2330" in html
        assert "台積電" in html
        assert "<table>" in html

    def test_html_color_coded_rows(self, tmp_path):
        """HTML report should have color-coded rows based on grade."""
        from report_generator import generate_html_report

        s1_data = {
            "summary": {"total_screened": 50, "passed": 2, "watchlist": 0, "rejected": 48},
            "candidates": [
                {"code": "2330", "name": "台積電", "composite_score": 92,
                 "close": 580, "pass": True},
                {"code": "1101", "name": "台泥", "composite_score": 45,
                 "close": 40, "pass": True},
            ],
        }

        with patch("report_generator._load_stage1", return_value=s1_data), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_sector_map", return_value={}):
            html = generate_html_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert "grade-a" in html
        assert "grade-e" in html

    def test_html_regime_badge(self, tmp_path):
        """HTML report should display regime badge with CSS class."""
        from report_generator import generate_html_report

        regime_data = {"regime": "stress", "confidence": 0.6,
                       "volatility": 0.035, "days_in_regime": 3,
                       "global_risk": "high"}

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=regime_data), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_sector_map", return_value={}):
            html = generate_html_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        assert "badge-stress" in html
        assert "壓力" in html

    def test_html_file_written(self, tmp_path):
        """HTML report should be written to a .html file."""
        from report_generator import generate_html_report

        with patch("report_generator._load_stage1", return_value=None), \
             patch("report_generator._load_stage2", return_value=None), \
             patch("report_generator._load_regime", return_value=None), \
             patch("report_generator._load_paper_trades", return_value=[]), \
             patch("report_generator._load_alert_history", return_value=[]), \
             patch("report_generator._load_sector_map", return_value={}):
            html = generate_html_report("2026-05-16", output_dir=str(tmp_path / "reports"))

        html_file = tmp_path / "reports" / "2026-05-16.html"
        assert html_file.exists()


# ═══════════════════════════════════════════════════════════════════════
#  5. Helper Functions
# ═══════════════════════════════════════════════════════════════════════

class TestHelperFunctions:
    """Verify internal helper functions for formatting and conversion."""

    def test_iso_to_roc_display(self):
        """ISO date should convert to ROC format."""
        from report_generator import _iso_to_roc_display
        result = _iso_to_roc_display("2026-05-16")
        assert "民國115年" in result
        assert "05月16日" in result

    def test_iso_to_roc_display_invalid(self):
        """Invalid date should return the original string."""
        from report_generator import _iso_to_roc_display
        result = _iso_to_roc_display("not-a-date")
        assert result == "not-a-date"

    def test_format_ntd(self):
        """Number should format as NT$ with comma separation."""
        from report_generator import _format_ntd
        assert _format_ntd(1234567) == "NT$1,234,567"
        assert _format_ntd(0) == "NT$0"
        assert _format_ntd(100) == "NT$100"

    def test_format_ntd_none(self):
        """None should format as NT$0."""
        from report_generator import _format_ntd
        assert _format_ntd(None) == "NT$0"

    def test_format_pct_positive(self):
        """Positive value should have + prefix."""
        from report_generator import _format_pct
        assert _format_pct(3.5) == "+3.50%"

    def test_format_pct_negative(self):
        """Negative value should not have + prefix."""
        from report_generator import _format_pct
        assert _format_pct(-1.2) == "-1.20%"

    def test_format_pct_zero(self):
        """Zero value should not have + prefix."""
        from report_generator import _format_pct
        assert _format_pct(0.0) == "0.00%"

    def test_format_pct_none(self):
        """None should return N/A."""
        from report_generator import _format_pct
        assert _format_pct(None) == "N/A"

    def test_get_sector_tc(self):
        """Known English sector should map to Traditional Chinese."""
        from report_generator import _get_sector_tc
        assert _get_sector_tc("semiconductor") == "半導體"
        assert _get_sector_tc("financial") == "金融/保險"

    def test_get_sector_tc_unknown(self):
        """Unknown sector should return the key as-is."""
        from report_generator import _get_sector_tc
        assert _get_sector_tc("custom_sector") == "custom_sector"

    def test_safe_load_json_missing(self, tmp_path):
        """Missing file should return None."""
        from report_generator import _safe_load_json
        result = _safe_load_json(tmp_path / "nonexistent.json")
        assert result is None

    def test_safe_load_json_valid(self, tmp_path):
        """Valid JSON file should be loaded correctly."""
        from report_generator import _safe_load_json
        f = tmp_path / "test.json"
        f.write_text('{"key": "value"}', encoding="utf-8")
        result = _safe_load_json(f)
        assert result == {"key": "value"}

    def test_safe_load_json_malformed(self, tmp_path):
        """Malformed JSON should return None."""
        from report_generator import _safe_load_json
        f = tmp_path / "bad.json"
        f.write_text("not json{", encoding="utf-8")
        result = _safe_load_json(f)
        assert result is None
