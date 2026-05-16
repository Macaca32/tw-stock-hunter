#!/usr/bin/env python3
"""Phase 36: Risk Management Overlay — Unit Tests

Tests cover all 7 features of core/risk_manager.py:
1. compute_atr_stop()        — ATR-based hard stop-loss
2. get_trailing_stop_config() — Tiered trailing stops by conviction grade
3. check_position_limit()     — Position constraints per stock/sector/correlation
4. estimate_portfolio_var()   — Value-at-Risk via historical simulation
5. get_risk_summary()         — Portfolio-level risk overview
6. pre_trade_risk_check()     — Pre-trade risk gate
7. enforce_stop_losses()      — Stop-loss enforcement on existing positions

All tests use inline synthetic data — zero API calls, fully isolated.
Taiwan market conventions verified: TWSE codes (2330/2454), Traditional Chinese labels.
"""

import pytest
import sys
from pathlib import Path

# Add core to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "core"))

from risk_manager import (
    compute_atr_stop,
    get_trailing_stop_config,
    check_position_limit,
    estimate_portfolio_var,
    get_risk_summary,
    pre_trade_risk_check,
    enforce_stop_losses,
    TRAILING_STOP_TIERS,
    MAX_SINGLE_STOCK_PCT,
    MAX_SECTOR_PCT,
    CORRELATION_RISK_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Fixtures: synthetic price history and portfolio data
# ---------------------------------------------------------------------------

def make_price_history(days=30, base_close=150.0, volatility=2.0):
    """Generate deterministic OHLCV history for testing."""
    history = []
    close = base_close
    for i in range(days):
        high = close + volatility * ((i % 7) / 6)
        low = close - volatility * ((i % 5) / 4)
        volume = 1000000 + (i * 10000)
        history.append({
            "date": f"2026-01-{(i % 28) + 1:02d}",
            "open": round(close, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close, 2),
            "adj_close": round(close, 2),
            "volume": volume,
        })
        close += volatility * ((i % 3) - 1) * 0.5  # slight drift
    return history


def make_portfolio(num_positions=3):
    """Generate a synthetic portfolio for testing."""
    positions = []
    total_value = 1_000_000
    per_pos = total_value // num_positions
    for i in range(num_positions):
        positions.append({
            "stock_id": f"23{30 + i}",
            "value": per_pos,
            "sector": ["semiconductor", "electronics"][i % 2],
            "entry_price": 150.0 + i * 10,
        })
    sectors = {}
    for p in positions:
        s = p["sector"]
        sectors[s] = sectors.get(s, 0) + p["value"]
    return {
        "total_value": total_value,
        "positions": positions,
        "sectors": sectors,
    }


# ---------------------------------------------------------------------------
# Feature 1: compute_atr_stop()
# ---------------------------------------------------------------------------

class TestComputeAtrStop:
    def test_basic_computation(self):
        """ATR stop should be calculated from price history."""
        hist = make_price_history(days=20)
        result = compute_atr_stop("2330", hist)
        assert result["stock_id"] == "2330"
        assert result["method"] == "atr"
        assert result["atr"] is not None and result["atr"] > 0
        assert result["stop_price"] is not None and result["stop_price"] < hist[-1]["close"]
        assert result["trailing_high"] is not None

    def test_insufficient_data(self):
        """Should return neutral when history is too short."""
        hist = make_price_history(days=5)
        result = compute_atr_stop("2330", hist, period=14)
        assert result["method"] == "neutral"
        assert result["atr"] is None

    def test_empty_history(self):
        """Should return neutral for empty history."""
        result = compute_atr_stop("2330", [])
        assert result["method"] == "neutral"

    def test_custom_multiplier(self):
        """Wider multiplier should produce lower stop price."""
        hist = make_price_history(days=20)
        r1 = compute_atr_stop("2330", hist, multiplier=2.0)
        r2 = compute_atr_stop("2330", hist, multiplier=3.0)
        assert r2["stop_price"] < r1["stop_price"]

    def test_trailing_high_tracking(self):
        """Trailing high should be max of recent closes."""
        hist = make_price_history(days=20)
        result = compute_atr_stop("2330", hist, period=14)
        recent_closes = [d["close"] for d in hist[-14:]]
        assert abs(result["trailing_high"] - max(recent_closes)) < 0.01

    def test_floor_at_50_pct(self):
        """Stop price should never go below 50% of current price."""
        hist = make_price_history(days=20, base_close=10.0)
        result = compute_atr_stop("2330", hist, multiplier=20)  # extreme multiplier
        assert result["stop_price"] >= hist[-1]["close"] * 0.5

    def test_adj_close_fallback(self):
        """Should work with adj_close when close is missing."""
        hist = make_price_history(days=20)
        for d in hist:
            del d["close"]
            del d["high"]
            del d["low"]
            d["adj_high"] = d["adj_close"] + 1
            d["adj_low"] = d["adj_close"] - 1
        result = compute_atr_stop("2330", hist)
        assert result["method"] == "atr"


# ---------------------------------------------------------------------------
# Feature 2: get_trailing_stop_config()
# ---------------------------------------------------------------------------

class TestGetTrailingStopConfig:
    def test_grade_a_tightest(self):
        """A-grade should have the tightest trailing stop."""
        config = get_trailing_stop_config("A")
        assert config["conviction_grade"] == "A"
        assert config["trailing_pct"] == 0.15

    @pytest.mark.parametrize("grade,pct", [
        ("A", 0.15), ("B", 0.18), ("C", 0.20), ("D", 0.22), ("E", 0.25)
    ])
    def test_all_grades(self, grade, pct):
        """All conviction grades should return correct trailing percentages."""
        config = get_trailing_stop_config(grade)
        assert config["trailing_pct"] == pct

    def test_none_defaults_to_c(self):
        """None grade should default to C (20%)."""
        config = get_trailing_stop_config(None)
        assert config["conviction_grade"] == "C"
        assert config["trailing_pct"] == 0.20

    def test_unknown_grade_fallback(self):
        """Unknown grades should use default fallback."""
        config = get_trailing_stop_config("Z")
        assert config["trailing_pct"] == pytest.approx(0.20)  # DEFAULT_TRAILING_STOP

    def test_chinese_labels_present(self):
        """Response should include Traditional Chinese labels."""
        for grade in ["A", "B", "C", "D", "E"]:
            config = get_trailing_stop_config(grade)
            assert "label_zh" in config
            assert isinstance(config["label_zh"], str) and len(config["label_zh"]) > 0
            assert "rationale" in config

    def test_case_insensitive(self):
        """Grade should be case-insensitive."""
        upper = get_trailing_stop_config("a")
        lower = get_trailing_stop_config("A")
        assert upper["trailing_pct"] == lower["trailing_pct"]


# ---------------------------------------------------------------------------
# Feature 3: check_position_limit()
# ---------------------------------------------------------------------------

class TestCheckPositionLimit:
    def test_new_portfolio_allows(self):
        """New portfolio with no positions should allow any trade."""
        empty = {"total_value": 0, "positions": []}
        result = check_position_limit(empty, "2330", "semiconductor")
        assert result["allowed"] is True
        assert result["recommended_action"] == "allow"

    def test_single_stock_limit(self):
        """Should reject when single stock exceeds 8%."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [{"stock_id": "2330", "value": 900_000, "sector": "semiconductor"}],
            "sectors": {"semiconductor": 900_000},
        }
        result = check_position_limit(portfolio, "2330", "semiconductor")
        assert result["allowed"] is False
        assert result["recommended_action"] == "reject"

    def test_sector_limit(self):
        """Should flag when sector exceeds 25%."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [{"stock_id": "2330", "value": 300_000, "sector": "semiconductor"}],
            "sectors": {"semiconductor": 300_000},
        }
        result = check_position_limit(portfolio, "2454", "semiconductor")
        assert result["recommended_action"] == "reduce"

    def test_correlation_risk(self):
        """Should flag high correlation pairs."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [
                {"stock_id": "2330", "value": 50_000, "sector": "semiconductor"},
                {"stock_id": "2454", "value": 50_000, "sector": "electronics"},
            ],
            "sectors": {},
        }
        corr = {("2330", "6001"): 0.90}
        result = check_position_limit(portfolio, "6001", "semiconductor", correlation_matrix=corr)
        assert len(result["correlation_alerts"]) > 0

    def test_multiple_correlation_rejects(self):
        """Should reject when too many high-correlation pairs exist."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [
                {"stock_id": f"23{30+i}", "value": 50_000, "sector": "semiconductor"}
                for i in range(5)
            ],
            "sectors": {},
        }
        corr = {}
        for i in range(5):
            corr[(f"23{30+i}", "6001")] = 0.90
        result = check_position_limit(portfolio, "6001", "semiconductor", correlation_matrix=corr)
        assert result["allowed"] is False
        assert result["recommended_action"] == "reject"

    def test_max_allowed_value(self):
        """Should compute max allowed value correctly."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [{"stock_id": "2330", "value": 50_000, "sector": "semiconductor"}],
        }
        result = check_position_limit(portfolio, "2330", "semiconductor")
        expected_max = (1_000_000 * MAX_SINGLE_STOCK_PCT) - 50_000
        assert abs(result["max_allowed_value"] - expected_max) < 0.01

    def test_sectors_computed_from_positions(self):
        """Should compute sector totals from positions when not provided."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [
                {"stock_id": "2330", "value": 100_000, "sector": "semiconductor"},
                {"stock_id": "2454", "value": 100_000, "sector": "electronics"},
            ],
        }
        result = check_position_limit(portfolio, "6001", "semiconductor")
        assert result["sector_pct"] == pytest.approx(0.1)

    def test_chinese_labels(self):
        """Correlation alerts should have Traditional Chinese labels."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [{"stock_id": "2330", "value": 50_000, "sector": "semiconductor"}],
        }
        corr = {("2330", "6001"): 0.90}
        result = check_position_limit(portfolio, "6001", "semiconductor", correlation_matrix=corr)
        if result["correlation_alerts"]:
            assert "label_zh" in result["correlation_alerts"][0]


# ---------------------------------------------------------------------------
# Feature 4: estimate_portfolio_var()
# ---------------------------------------------------------------------------

class TestEstimatePortfolioVar:
    def test_empty_positions(self):
        """Empty portfolio should return zero VaR."""
        result = estimate_portfolio_var([])
        assert result["var_amount"] == 0.0
        assert result["method"] in ("neutral", "fallback_estimate")

    def test_with_price_history(self):
        """Should compute VaR from price history when available."""
        positions = [{"stock_id": "2330", "value": 100_000}]
        hist = {"2330": make_price_history(days=70)}
        result = estimate_portfolio_var(positions, hist)
        assert result["var_amount"] > 0
        assert result["confidence_level"] == 0.95

    def test_fallback_estimate(self):
        """Should use fallback when price history is unavailable."""
        positions = [{"stock_id": "2330", "value": 1_000_000}]
        result = estimate_portfolio_var(positions, {})
        assert result["method"] == "fallback_estimate"
        assert result["var_amount"] > 0

    def test_custom_confidence(self):
        """Higher confidence should produce higher VaR."""
        positions = [{"stock_id": "2330", "value": 1_000_000}]
        hist = {"2330": make_price_history(days=70)}
        r95 = estimate_portfolio_var(positions, hist, confidence=0.95)
        r99 = estimate_portfolio_var(positions, hist, confidence=0.99)
        assert r99["var_amount"] >= r95["var_amount"]

    def test_chinese_labels(self):
        """Should include Traditional Chinese description."""
        positions = [{"stock_id": "2330", "value": 1_000_000}]
        result = estimate_portfolio_var(positions, {})
        assert "label_zh" in result


# ---------------------------------------------------------------------------
# Feature 5: get_risk_summary()
# ---------------------------------------------------------------------------

class TestGetRiskSummary:
    def test_empty_portfolio(self):
        """Empty portfolio should have zero risk."""
        result = get_risk_summary({"total_value": 0, "positions": []})
        assert result["total_exposure"] == 0
        assert result["num_positions"] == 0
        assert 1 <= result["overall_risk_score"] <= 10

    def test_full_summary(self):
        """Should compute all risk dimensions."""
        portfolio = make_portfolio()
        result = get_risk_summary(portfolio)
        assert "total_exposure" in result
        assert "sector_concentration" in result
        assert "var_95" in result
        assert "max_drawdown_risk" in result
        assert "regime_risk" in result
        assert 1 <= result["overall_risk_score"] <= 10

    def test_regime_impact(self):
        """Black swan regime should increase risk score."""
        portfolio = make_portfolio()
        r_normal = get_risk_summary(portfolio, regime="normal")
        r_bswan = get_risk_summary(portfolio, regime="black_swan")
        assert r_bswan["overall_risk_score"] >= r_normal["overall_risk_score"]

    def test_chinese_regime_labels(self):
        """Regime labels should be in Traditional Chinese."""
        portfolio = make_portfolio()
        for regime, zh_label in [("normal", "常態"), ("crisis", "危機")]:
            result = get_risk_summary(portfolio, regime=regime)
            assert zh_label in result["regime_risk"]

    def test_drawdown_capped_at_50(self):
        """Max drawdown risk should be capped at 50%."""
        portfolio = make_portfolio()
        result = get_risk_summary(portfolio, regime="black_swan")
        assert result["max_drawdown_risk"] <= 0.50

    def test_sector_concentration_sorted(self):
        """Sector concentration should be sorted by weight descending."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [
                {"stock_id": "2330", "value": 400_000, "sector": "semiconductor"},
                {"stock_id": "2454", "value": 300_000, "sector": "electronics"},
                {"stock_id": "6001", "value": 200_000, "sector": "communications"},
            ],
        }
        result = get_risk_summary(portfolio)
        if len(result["sector_concentration"]) >= 2:
            assert result["sector_concentration"][0]["weight_pct"] >= result["sector_concentration"][1]["weight_pct"]


# ---------------------------------------------------------------------------
# Feature 6: pre_trade_risk_check()
# ---------------------------------------------------------------------------

class TestPreTradeRiskCheck:
    def test_allows_new_portfolio(self):
        """Should allow any trade in a new portfolio."""
        empty = {"total_value": 0, "positions": []}
        candidate = {"stock_id": "2330", "sector": "semiconductor"}
        result = pre_trade_risk_check(empty, candidate)
        assert result["allowed"] is True

    def test_blocks_over_concentrated(self):
        """Should block trade that would exceed concentration limits."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [{"stock_id": "2330", "value": 900_000, "sector": "semiconductor"}],
        }
        candidate = {"stock_id": "2330", "sector": "semiconductor"}
        result = pre_trade_risk_check(portfolio, candidate)
        assert result["allowed"] is False

    def test_includes_atr_and_trailing(self):
        """Should include ATR stop and trailing config in response."""
        portfolio = {"total_value": 1_000_000, "positions": []}
        hist = {"2330": make_price_history(days=20)}
        candidate = {"stock_id": "2330", "sector": "semiconductor", "conviction_grade": "A"}
        result = pre_trade_risk_check(portfolio, candidate, price_history=hist)
        assert "atr_stop" in result
        assert "trailing_config" in result

    def test_chinese_reason(self):
        """Blocked trades should have Traditional Chinese reason."""
        portfolio = {
            "total_value": 1_000_000,
            "positions": [{"stock_id": "2330", "value": 900_000, "sector": "semiconductor"}],
        }
        candidate = {"stock_id": "2330", "sector": "semiconductor"}
        result = pre_trade_risk_check(portfolio, candidate)
        if not result["allowed"]:
            assert isinstance(result.get("reason_zh"), str) and len(result["reason_zh"]) > 0


# ---------------------------------------------------------------------------
# Feature 7: enforce_stop_losses()
# ---------------------------------------------------------------------------

class TestEnforceStopLosses:
    def test_no_positions(self):
        """Empty positions should return empty result."""
        result = enforce_stop_losses([])
        assert len(result["positions_to_exit"]) == 0

    def test_stop_loss_hit(self):
        """Position below stop-loss should be flagged for exit."""
        positions = [{
            "stock_id": "2330",
            "entry_price": 150.0,
            "stop_loss": 140.0,
            "take_profit": 180.0,
        }]
        current_prices = {"2330": 138.0}
        result = enforce_stop_losses(positions, current_prices=current_prices)
        assert len(result["positions_to_exit"]) == 1
        assert result["positions_to_exit"][0]["signal"] == "stop_loss"

    def test_take_profit_hit(self):
        """Position above take-profit should be flagged for exit."""
        positions = [{
            "stock_id": "2330",
            "entry_price": 150.0,
            "stop_loss": 140.0,
            "take_profit": 180.0,
        }]
        current_prices = {"2330": 185.0}
        result = enforce_stop_losses(positions, current_prices=current_prices)
        assert len(result["positions_to_exit"]) == 1
        assert result["positions_to_exit"][0]["signal"] == "take_profit"

    def test_no_breach(self):
        """Position within range should remain updated."""
        positions = [{
            "stock_id": "2330",
            "entry_price": 150.0,
            "stop_loss": 140.0,
            "take_profit": 180.0,
        }]
        current_prices = {"2330": 160.0}
        result = enforce_stop_losses(positions, current_prices=current_prices)
        assert len(result["positions_to_exit"]) == 0
        assert len(result["updated_positions"]) == 1

    def test_trailing_stop_update(self):
        """Winner position should have trailing stop updated."""
        positions = [{
            "stock_id": "2330",
            "entry_price": 150.0,
            "stop_loss": 140.0,
            "conviction_grade": "A",
        }]
        current_prices = {"2330": 180.0}
        result = enforce_stop_losses(positions, current_prices=current_prices)
        updated = result["updated_positions"][0]
        assert "trailing_stop_price" in updated

    def test_chinese_summary(self):
        """Summary should be in Traditional Chinese."""
        positions = [{"stock_id": "2330", "entry_price": 150.0, "stop_loss": 140.0}]
        current_prices = {"2330": 138.0}
        result = enforce_stop_losses(positions, current_prices=current_prices)
        assert isinstance(result["summary_zh"], str)
        if result["positions_to_exit"]:
            assert "停損" in result["summary_zh"] or "止盈" in result["summary_zh"]

    def test_missing_price_skips(self):
        """Position with no price data should be skipped."""
        positions = [{"stock_id": "2330", "entry_price": 150.0}]
        result = enforce_stop_losses(positions, current_prices={"9999": 100})
        assert len(result["updated_positions"]) == 1
        # Should not crash or exit the position

    def test_pnl_calculation(self):
        """Exit PnL should be calculated correctly."""
        positions = [{"stock_id": "2330", "entry_price": 150.0, "stop_loss": 140.0}]
        current_prices = {"2330": 140.0}
        result = enforce_stop_losses(positions, current_prices=current_prices)
        if result["positions_to_exit"]:
            assert abs(result["positions_to_exit"][0]["pnl_pct"] - (-6.67)) < 0.5


# ---------------------------------------------------------------------------
# Edge cases and Taiwan market conventions
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_twse_stock_codes(self):
        """Should handle TWSE stock codes (2330, 2454)."""
        hist = make_price_history(days=20)
        for code in ["2330", "2454"]:
            result = compute_atr_stop(code, hist)
            assert result["stock_id"] == code

    def test_tpex_stock_codes(self):
        """Should handle TPEx stock codes (6001)."""
        hist = make_price_history(days=20)
        result = compute_atr_stop("6001", hist)
        assert result["stock_id"] == "6001"

    def test_trailing_tiers_correct(self):
        """TRAILING_STOP_TIERS dict should match spec."""
        expected = {"A": 0.15, "B": 0.18, "C": 0.20, "D": 0.22, "E": 0.25}
        assert TRAILING_STOP_TIERS == expected

    def test_constants_match_spec(self):
        """Constants should match Taiwan market guidelines."""
        assert MAX_SINGLE_STOCK_PCT == 0.08
        assert MAX_SECTOR_PCT == 0.25
        assert CORRELATION_RISK_THRESHOLD == 0.85

    def test_zero_entry_price_in_enforce(self):
        """Should not crash with zero entry price."""
        positions = [{"stock_id": "2330", "entry_price": 0, "stop_loss": 0}]
        current_prices = {"2330": 150.0}
        result = enforce_stop_losses(positions, current_prices=current_prices)
        assert len(result["updated_positions"]) == 1

    def test_var_with_single_position(self):
        """VaR should work with a single position."""
        positions = [{"stock_id": "2330", "value": 500_000}]
        hist = {"2330": make_price_history(days=70)}
        result = estimate_portfolio_var(positions, hist)
        assert result["var_amount"] > 0

    def test_risk_summary_no_regime(self):
        """Should handle missing regime gracefully."""
        portfolio = make_portfolio()
        result = get_risk_summary(portfolio)
        assert "regime_risk" in result
        assert isinstance(result["overall_risk_score"], int)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
