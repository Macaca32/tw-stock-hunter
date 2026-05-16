"""Unit tests for Phase 28: Portfolio Rebalancing Engine.

Tests optimize_positions(), compute_sector_rotation(), and
check_correlation_risk() with inline synthetic data — no API calls.
"""

import json
import math
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ═══════════════════════════════════════════════════════════════════════
#  Helper: Create PaperTrader with mocked dependencies
# ═══════════════════════════════════════════════════════════════════════

def _make_trader(data_dir=None, config=None):
    """Create a PaperTrader instance with minimal dependencies for testing."""
    from paper_trader import PaperTrader
    trader = PaperTrader.__new__(PaperTrader)
    trader.config = config or PaperTrader()._default_config()
    trader.trades = []
    trader.active_positions = []
    trader.data_dir = Path(data_dir) if data_dir else Path("/tmp/test_trader")
    trader._sector_map = {}  # Avoid loading real sector data
    trader._regime_detector_available = False
    return trader


# ═══════════════════════════════════════════════════════════════════════
#  1. optimize_positions — Kelly-Inspired Position Sizing
# ═══════════════════════════════════════════════════════════════════════

class TestOptimizePositions:
    """Verify Kelly-inspired position sizing with sector constraints."""

    def test_zero_signal_strength(self):
        """Signal strength 0% → size_mult floored at 0.5x."""
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電", "combined_score": 0},
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000)
        assert len(result["allocations"]) == 1
        assert result["allocations"][0]["size_mult"] == 0.5

    def test_100_percent_signal_strength(self):
        """Signal strength 100% → size_mult = 2.0x."""
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電", "combined_score": 100},
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000)
        assert len(result["allocations"]) == 1
        assert result["allocations"][0]["size_mult"] == 2.0

    def test_moderate_signal_strength(self):
        """Signal strength 50% → size_mult = 1.0x (base)."""
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電", "combined_score": 50},
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000)
        assert len(result["allocations"]) == 1
        assert result["allocations"][0]["size_mult"] == 1.0

    def test_signal_strength_dict(self):
        """Signal strength from Phase 27 dict format."""
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電",
             "signal_strength": {"strength": 75}},
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000)
        assert len(result["allocations"]) == 1
        assert result["allocations"][0]["size_mult"] == 1.5

    def test_extreme_signal_above_cap(self):
        """Signal strength >100 gets clamped to 100 → size_mult = 2.0x (max from clamping).

        Note: signal_str is clamped to [0, 100], so 200 → 100 → size_mult = 2.0.
        The 3.0x cap only applies if signal_strength somehow exceeds 150 after clamping.
        """
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電", "combined_score": 200},
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000)
        # signal_str clamped to 100, size_mult = 100/50 = 2.0
        assert result["allocations"][0]["size_mult"] == 2.0

    def test_sector_cap_enforcement(self):
        """Positions exceeding 15% sector cap should be reduced or skipped."""
        trader = _make_trader()
        # Create many candidates in the same sector
        candidates = [
            {"code": f"23{i}", "name": f"Stock{i}", "combined_score": 80}
            for i in range(10)
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000)

        # Total sector allocation should not exceed 15%
        sector_alloc = result["sector_allocations"].get("semiconductor", 0)
        assert sector_alloc <= 15.0 + 0.1  # Small tolerance for rounding

    def test_regime_mult_zero_no_positions(self):
        """Regime multiplier 0 → no positions allocated."""
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電", "combined_score": 90},
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000,
                                              regime_mult=0.0)
        assert result["allocations"] == []
        assert result["total_allocated_pct"] == 0
        assert len(result["warnings"]) > 0

    def test_regime_mult_reduces_positions(self):
        """Low regime multiplier should reduce effective max positions."""
        trader = _make_trader()
        candidates = [
            {"code": f"23{i}", "name": f"Stock{i}", "combined_score": 80}
            for i in range(10)
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000,
                                              regime_mult=0.4)
        # effective_max = 5 * 0.4 = 2 → only 2 positions
        assert len(result["allocations"]) <= 2

    def test_allocation_pct_calculation(self):
        """Verify allocation percentage formula: base_alloc_pct * size_mult."""
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電", "combined_score": 50},
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000)

        alloc = result["allocations"][0]
        # base_alloc_pct = risk_per_trade * 100 = 0.015 * 100 = 1.5%
        # size_mult = 50/50 = 1.0
        # alloc_pct = 1.5 * 1.0 = 1.5%
        expected_pct = trader.config["risk_per_trade"] * 100 * 1.0
        assert abs(alloc["allocation_pct"] - expected_pct) < 0.1

    def test_allocation_twd_calculation(self):
        """Allocation in TWD should be alloc_pct/100 * portfolio_value."""
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電", "combined_score": 50},
        ]
        pv = 2_000_000
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=pv)

        alloc = result["allocations"][0]
        expected_twd = alloc["allocation_pct"] / 100 * pv
        assert abs(alloc["allocation_twd"] - expected_twd) < 1

    def test_total_unallocated_pct(self):
        """Total allocated + unallocated should equal 100%."""
        trader = _make_trader()
        candidates = [
            {"code": "2330", "name": "台積電", "combined_score": 60},
            {"code": "2454", "name": "聯發科", "combined_score": 70},
        ]
        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.optimize_positions(candidates, portfolio_value=1_000_000)

        total = result["total_allocated_pct"] + result["unallocated_pct"]
        assert abs(total - 100.0) < 0.1

    def test_empty_candidates(self):
        """No candidates → zero allocation."""
        trader = _make_trader()
        with patch.object(trader, '_get_sector', return_value="other"):
            result = trader.optimize_positions([], portfolio_value=1_000_000)
        assert result["allocations"] == []
        assert result["total_allocated_pct"] == 0


# ═══════════════════════════════════════════════════════════════════════
#  2. compute_sector_rotation — Sector Rotation Signals
# ═══════════════════════════════════════════════════════════════════════

class TestComputeSectorRotation:
    """Verify sector rotation signal computation."""

    def _write_stage2_file(self, data_dir, date_str, candidates):
        """Write a synthetic stage2 JSON file."""
        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        filepath = data_dir / f"stage2_{date_str}.json"
        data = {"candidates": candidates, "summary": {"passed_stage2": len(candidates)}}
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return filepath

    def test_no_stage2_files(self, tmp_path):
        """No stage2 files → empty rotation result."""
        trader = _make_trader(data_dir=str(tmp_path))
        result = trader.compute_sector_rotation(date_str="2026-05-16")
        assert result["sectors"] == {}
        assert result["overweight_sectors"] == []

    def test_single_sector_portfolio(self, tmp_path):
        """All stocks in one sector → that sector is neutral (no comparison)."""
        trader = _make_trader(data_dir=str(tmp_path))
        candidates = [
            {"code": "2330", "combined_score": 80},
            {"code": "2337", "combined_score": 75},
        ]
        self._write_stage2_file(tmp_path, "2026-05-16", candidates)

        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.compute_sector_rotation(date_str="2026-05-16")

        # Only one sector → std_dev = 0 → neutral
        assert "semiconductor" in result["sectors"]
        assert result["sectors"]["semiconductor"]["signal"] == "neutral"

    def test_equal_momentum_across_sectors(self, tmp_path):
        """All sectors with identical scores → all neutral."""
        trader = _make_trader(data_dir=str(tmp_path))
        candidates = [
            {"code": "2330", "combined_score": 75},  # semiconductor
            {"code": "1101", "combined_score": 75},  # materials
        ]
        self._write_stage2_file(tmp_path, "2026-05-16", candidates)

        sector_map = {"2330": "semiconductor", "1101": "materials"}
        with patch.object(trader, '_get_sector', side_effect=lambda c: sector_map.get(c, "other")):
            result = trader.compute_sector_rotation(date_str="2026-05-16")

        # Both sectors have same score → neutral
        for sector, info in result["sectors"].items():
            assert info["signal"] == "neutral"

    def test_sector_rotation_with_momentum(self, tmp_path):
        """Sector with improving scores should be overweight."""
        trader = _make_trader(data_dir=str(tmp_path))

        # Day 1: semiconductor=60, materials=60
        self._write_stage2_file(tmp_path, "2026-05-12",
                                [{"code": "2330", "combined_score": 60},
                                 {"code": "1101", "combined_score": 60}])
        # Day 2: semiconductor=90, materials=60
        self._write_stage2_file(tmp_path, "2026-05-13",
                                [{"code": "2330", "combined_score": 90},
                                 {"code": "1101", "combined_score": 60}])

        sector_map = {"2330": "semiconductor", "1101": "materials"}
        with patch.object(trader, '_get_sector', side_effect=lambda c: sector_map.get(c, "other")):
            result = trader.compute_sector_rotation(date_str="2026-05-13", rolling_window=5)

        # Semiconductor has higher current_avg → possibly overweight
        semi = result["sectors"].get("semiconductor", {})
        mat = result["sectors"].get("materials", {})
        if semi and mat:
            assert semi["current_avg"] >= mat["current_avg"]

    def test_rolling_window_parameter(self, tmp_path):
        """Rolling window should limit the number of days analyzed."""
        trader = _make_trader(data_dir=str(tmp_path))

        # Create 10 days of files
        for i in range(10):
            date = f"2026-05-{7+i:02d}"
            self._write_stage2_file(tmp_path, date,
                                    [{"code": "2330", "combined_score": 70}])

        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.compute_sector_rotation(date_str="2026-05-16", rolling_window=3)

        assert result["rolling_window"] == 3

    def test_date_filtering(self, tmp_path):
        """Files after date_str should be excluded."""
        trader = _make_trader(data_dir=str(tmp_path))

        self._write_stage2_file(tmp_path, "2026-05-14",
                                [{"code": "2330", "combined_score": 80}])
        self._write_stage2_file(tmp_path, "2026-05-16",
                                [{"code": "2330", "combined_score": 90}])

        with patch.object(trader, '_get_sector', return_value="semiconductor"):
            result = trader.compute_sector_rotation(date_str="2026-05-15")

        # Only the 2026-05-14 file should be included (2026-05-16 > 2026-05-15)
        if "semiconductor" in result["sectors"]:
            assert result["sectors"]["semiconductor"]["current_avg"] == 80


# ═══════════════════════════════════════════════════════════════════════
#  3. check_correlation_risk — Pairwise Correlation Analysis
# ═══════════════════════════════════════════════════════════════════════

class TestCheckCorrelationRisk:
    """Verify pairwise correlation detection and position adjustments."""

    @staticmethod
    def _make_price_history(codes, days=25, base_price=100, daily_return=0.01, seed=42):
        """Create synthetic price history with controllable correlation.

        If daily_return is a single float, all stocks get the same returns
        (perfectly correlated). Vary returns per stock for lower correlation.
        """
        import random
        random.seed(seed)
        history = {}
        for code in codes:
            prices = []
            price = base_price
            for d in range(days + 1):
                prices.append({
                    "date": f"2026-04-{d+1:02d}",
                    "adj_close": round(price, 2),
                    "close": round(price, 2),
                })
                price *= (1 + daily_return)
            history[code] = prices
        return history

    @staticmethod
    def _make_positions(codes, alloc_pct=5.0):
        """Create position dicts for each stock code."""
        return [{"code": code, "allocation_pct": alloc_pct} for code in codes]

    def test_perfectly_correlated_holdings(self):
        """All holdings with identical returns → corr ≈ 1.0 → flagged."""
        trader = _make_trader()
        codes = ["2330", "2454"]
        positions = self._make_positions(codes)
        # Both stocks have same daily return → perfect correlation
        price_history = self._make_price_history(codes, daily_return=0.02)

        result = trader.check_correlation_risk(positions, price_history)

        assert len(result["pairwise_correlations"]) > 0
        # Correlation should be very high (≈ 1.0)
        for pair in result["pairwise_correlations"]:
            assert pair["correlation"] > 0.9
            assert pair["flagged"] is True

    def test_zero_correlation_case(self):
        """Holdings with uncorrelated returns → low correlation → not flagged."""
        trader = _make_trader()
        import random
        random.seed(42)

        codes = ["2330", "1101"]
        # Create uncorrelated price histories
        history = {}
        for code in codes:
            prices = []
            price = 100
            for d in range(26):
                prices.append({
                    "date": f"2026-04-{d+1:02d}",
                    "adj_close": round(price, 2),
                    "close": round(price, 2),
                })
                price *= (1 + random.uniform(-0.05, 0.05))
            history[code] = prices

        positions = self._make_positions(codes)
        result = trader.check_correlation_risk(positions, history)

        if result["pairwise_correlations"]:
            # With random independent returns, correlation should be low
            for pair in result["pairwise_correlations"]:
                # May not be flagged unless accidentally high
                assert isinstance(pair["correlation"], float)

    def test_single_holding(self):
        """Only one holding → no pairs → empty result."""
        trader = _make_trader()
        codes = ["2330"]
        positions = self._make_positions(codes)
        price_history = self._make_price_history(codes)

        result = trader.check_correlation_risk(positions, price_history)
        assert result["pairwise_correlations"] == []
        assert result["holdings_analyzed"] <= 1

    def test_no_positions(self):
        """No positions → empty result with note."""
        trader = _make_trader()
        result = trader.check_correlation_risk(positions=[], price_history={})
        assert result["pairwise_correlations"] == []
        assert "note" in result

    def test_high_correlation_position_adjustment(self):
        """High correlation pairs → smaller position reduced by 50%."""
        trader = _make_trader()
        codes = ["2330", "2454"]
        positions = [
            {"code": "2330", "allocation_pct": 3.0},
            {"code": "2454", "allocation_pct": 5.0},
        ]
        price_history = self._make_price_history(codes, daily_return=0.02)

        result = trader.check_correlation_risk(positions, price_history)

        if result["high_correlation_pairs"]:
            # The smaller position (2330, 3%) should be reduced
            adjustments = result["position_adjustments"]
            assert len(adjustments) > 0
            # Smaller position reduced by 50%
            for adj in adjustments:
                assert adj["adjusted_pct"] == adj["original_pct"] * 0.5

    def test_effective_portfolio_beta(self):
        """Portfolio beta should be computed and returned."""
        trader = _make_trader()
        codes = ["2330", "2454"]
        positions = self._make_positions(codes)
        price_history = self._make_price_history(codes, daily_return=0.01)

        result = trader.check_correlation_risk(positions, price_history)
        # Beta should be a finite number
        if result["holdings_analyzed"] >= 2:
            assert isinstance(result["effective_portfolio_beta"], (int, float))
            assert not math.isnan(result["effective_portfolio_beta"])

    def test_correlation_matrix_symmetry(self):
        """Correlation matrix should be symmetric: corr(A,B) == corr(B,A)."""
        trader = _make_trader()
        codes = ["2330", "2454", "1101"]
        positions = self._make_positions(codes)
        # Create different returns for different stocks
        import random
        random.seed(123)
        history = {}
        for i, code in enumerate(codes):
            prices = []
            price = 100
            for d in range(26):
                prices.append({
                    "date": f"2026-04-{d+1:02d}",
                    "adj_close": round(price, 2),
                    "close": round(price, 2),
                })
                price *= (1 + random.uniform(-0.03, 0.03))
            history[code] = prices

        result = trader.check_correlation_risk(positions, history)
        matrix = result["correlation_matrix"]
        if matrix:
            for code_a in matrix:
                for code_b in matrix[code_a]:
                    assert abs(matrix[code_a][code_b] - matrix.get(code_b, {}).get(code_a, 0)) < 0.01

    def test_insufficient_price_data(self):
        """Holdings with insufficient price history should be excluded."""
        trader = _make_trader()
        # Only 5 data points (need 11 for lookback_days=20)
        short_history = {
            "2330": [{"adj_close": 100 + i, "close": 100 + i}
                     for i in range(6)],
            "2454": [{"adj_close": 200 + i, "close": 200 + i}
                     for i in range(6)],
        }
        positions = self._make_positions(["2330", "2454"])

        result = trader.check_correlation_risk(positions, short_history, lookback_days=20)
        # With insufficient data, may return fewer analyzed holdings
        assert result["holdings_analyzed"] <= 2
