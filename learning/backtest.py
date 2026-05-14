#!/usr/bin/env python3
"""
Backtesting Framework - Test strategies against historical data

Features:
- Walk-forward validation
- Weight optimization
- Threshold tuning
- Regime-aware testing
- Sharpe ratio, max drawdown, win rate metrics

Phase 17: ATR-based stop loss aligned with live paper trading.
Previously used hardcoded -5%/+10% stops which inflated backtest
metrics (Sharpe 9.33, 70.8% win rate were not representative).
Now uses same 2.5x ATR + 0.5x gap buffer + TWSE/TPEx transaction
costs as paper_trader.py for backtest/live consistency.
"""

import json
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# Ensure parent dir is on path for core module imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def _safe_float(val, default=0.0):
    """Safely convert to float"""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


class Backtester:
    # Phase 17: ATR stop parameters — MUST match paper_trader.py defaults
    ATR_STOP_MULT = 2.5
    ATR_GAP_BUFFER_MULT = 0.5
    ATR_PERIOD = 14
    MAX_HOLDING_DAYS = 20
    TAKE_PROFIT_RR = 2.0  # 2:1 reward-to-risk
    TWSE_ROUNTRIP_COST = 0.006   # 0.6%
    TPEx_ROUNTRIP_COST = 0.007   # 0.7%
    FALLBACK_STOP_PCT = 0.03     # -3% if ATR unavailable

    def __init__(self):
        self.data_dir = Path(__file__).parent.parent / "data"
        self.config_dir = Path(__file__).parent.parent / "config"
        self.results = []

    def load_price_history(self):
        """Load price history"""
        history_file = self.data_dir / "price_history.json"
        if not history_file.exists():
            return {}

        with open(history_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def load_weights(self):
        """Load current weights"""
        weights_file = self.config_dir / "weights.json"
        if not weights_file.exists():
            return self._default_weights()

        with open(weights_file, 'r') as f:
            return json.load(f)

    def _default_weights(self):
        return {
            "stage1": {
                "revenue_momentum": 0.10,
                "profitability": 0.30,
                "valuation": 0.25,
                "institutional_flow": 0.10,
                "technical_momentum": 0.25
            }
        }

    # ------------------------------------------------------------------ #
    #  Phase 17: ATR-based stop loss — aligned with paper_trader.py
    # ------------------------------------------------------------------ #

    @staticmethod
    def _is_tpex(stock_code):
        """Determine if a stock trades on TPEx (上櫃).

        TPEx stocks: codes >= 9900, or 6000-7999 range.
        """
        try:
            code_int = int(stock_code)
            return code_int >= 9900 or (6000 <= code_int <= 7999)
        except (ValueError, TypeError):
            return False

    def _get_transaction_cost(self, stock_code):
        """Get round-trip transaction cost based on market."""
        if self._is_tpex(stock_code):
            return self.TPEx_ROUNTRIP_COST
        return self.TWSE_ROUNTRIP_COST

    def calc_atr(self, stock_history, period=14):
        """Calculate Average True Range for a stock.

        Phase 17: Same ATR logic as paper_trader.py for consistency.
        """
        if not stock_history or len(stock_history) < period + 1:
            return None

        true_ranges = []
        for i in range(1, len(stock_history)):
            high = _safe_float(stock_history[i].get("high", 0))
            low = _safe_float(stock_history[i].get("low", 0))
            prev_close = _safe_float(stock_history[i-1].get("close", 0))

            if high > 0 and low > 0 and prev_close > 0:
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )
                true_ranges.append(tr)

        if not true_ranges:
            return None

        return sum(true_ranges[-period:]) / min(period, len(true_ranges))

    def _get_atr_stop(self, entry_price, atr_value):
        """Calculate ATR-based stop loss.

        Phase 17: Same formula as paper_trader.py:
            stop = entry - (ATR * 2.5) - (ATR * 0.5)
        Falls back to fixed % if ATR unavailable.

        Returns (stop_price, take_profit_price) tuple.
        """
        if atr_value is not None and atr_value > 0:
            stop = entry_price - (atr_value * self.ATR_STOP_MULT) - (atr_value * self.ATR_GAP_BUFFER_MULT)

            # Minimum 2% below entry (same as paper_trader)
            min_stop = entry_price * 0.98
            stop = min(stop, min_stop)

            risk_per_share = entry_price - stop
            take_profit = entry_price + risk_per_share * self.TAKE_PROFIT_RR
            return round(stop, 2), round(take_profit, 2)
        else:
            # Fallback: fixed percentage
            stop = entry_price * (1 - self.FALLBACK_STOP_PCT)
            risk_per_share = entry_price - stop
            take_profit = entry_price + risk_per_share * self.TAKE_PROFIT_RR
            return round(stop, 2), round(take_profit, 2)

    def simulate_trade(self, entry_price, exit_price, stock_code="", commission_rate=None):
        """Simulate a single trade.

        Phase 17: Uses market-specific transaction cost if stock_code
        is provided, otherwise falls back to provided commission_rate.
        """
        if entry_price == 0:
            return None

        gross_pnl = (exit_price - entry_price) / entry_price
        # Phase 17: Use market-specific cost when stock_code available
        if commission_rate is None:
            commission_rate = self._get_transaction_cost(stock_code)
        net_pnl = gross_pnl - commission_rate

        return {
            "entry": entry_price,
            "exit": exit_price,
            "gross_pnl_pct": round(gross_pnl * 100, 2),
            "net_pnl_pct": round(net_pnl * 100, 2),
            "win": net_pnl > 0
        }

    def test_weights(self, weights, price_history, candidates, lookback=20):
        """Test a set of weights against historical data.

        Phase 17: Uses ATR-based stops instead of hardcoded -5%/+10%.
        Stop loss = entry - (2.5×ATR + 0.5×ATR gap buffer)
        Take profit = entry + risk × 2.0 (2:1 reward-to-risk)
        Transaction costs = TWSE 0.6% / TPEx 0.7% round-trip
        Max holding = 20 trading days (same as paper_trader default)
        """
        trades = []

        for candidate in candidates:
            code = candidate["code"]

            # Use price history if available
            if code in price_history:
                history = price_history[code]
                if len(history) < lookback:
                    continue

                # Entry price — use adj_close for consistency with live pipeline
                entry_price = history[0].get("adj_close", history[0].get("close", 0))
                if entry_price <= 0:
                    continue

                # Phase 17: ATR-based stop loss (same as paper_trader.py)
                atr = self.calc_atr(history, period=self.ATR_PERIOD)
                stop_loss, take_profit = self._get_atr_stop(entry_price, atr)

                # Scan forward for exit
                exit_price = None
                exit_reason = None
                holding_days = 0

                for day in history[1:]:
                    price = day.get("adj_close", day.get("close", 0))
                    holding_days += 1

                    if price <= 0:
                        continue

                    if price <= stop_loss:
                        exit_price = stop_loss
                        exit_reason = "stop_loss"
                        break
                    elif price >= take_profit:
                        exit_price = take_profit
                        exit_reason = "take_profit"
                        break

                    # Max holding days (same as paper_trader default)
                    if holding_days >= self.MAX_HOLDING_DAYS:
                        exit_price = price
                        exit_reason = "max_holding_days"
                        break

                if exit_price is None:
                    # Use last available price
                    last_day = history[-1]
                    exit_price = last_day.get("adj_close", last_day.get("close", 0))
                    exit_reason = "end_of_data"

                if exit_price <= 0:
                    continue
            else:
                # No price history — skip rather than simulate with random noise.
                # Phase 17: Removed random-noise simulation that produced
                # meaningless backtest results for stocks without history.
                continue

            trade = self.simulate_trade(entry_price, exit_price, stock_code=code)
            if trade:
                trade["code"] = code
                trade["holding_days"] = holding_days if code in price_history else lookback
                trade["stop_type"] = "atr" if (code in price_history and self.calc_atr(price_history.get(code, []))) else "fallback_pct"
                trade["exit_reason"] = exit_reason if code in price_history else "simulated"
                trades.append(trade)

        return self._calc_metrics(trades)

    def _calc_metrics(self, trades):
        """Calculate trading metrics with statistical significance checks.

        FIX v2: Add sample size warnings, confidence intervals,
        and proper Sharpe calculation.
        """
        if not trades:
            return {
                "total_trades": 0,
                "win_rate": 0,
                "avg_pnl": 0,
                "total_pnl": 0,
                "sharpe_ratio": 0,
                "max_drawdown": 0,
                "profit_factor": 0,
                "statistical_warning": "No trades"
            }

        winning = [t for t in trades if t["win"]]
        losing = [t for t in trades if not t["win"]]

        win_rate = len(winning) / len(trades) * 100
        avg_pnl = sum(t["net_pnl_pct"] for t in trades) / len(trades)
        total_pnl = sum(t["net_pnl_pct"] for t in trades)

        # Sharpe ratio (simplified, annualized)
        pnls = [t["net_pnl_pct"] for t in trades]
        mean_pnl = sum(pnls) / len(pnls)
        std_pnl = (sum((p - mean_pnl) ** 2 for p in pnls) / len(pnls)) ** 0.5
        sharpe = (mean_pnl / std_pnl * math.sqrt(252)) if std_pnl > 0 else 0

        # Max drawdown
        cumulative = 0
        peak = 0
        max_dd = 0
        for pnl in pnls:
            cumulative += pnl
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd

        # Profit factor
        gross_profit = sum(t["net_pnl_pct"] for t in winning) if winning else 0
        gross_loss = abs(sum(t["net_pnl_pct"] for t in losing)) if losing else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        # FIX v2: Statistical significance warnings
        n = len(trades)
        warning = None

        if n < 10:
            warning = f"CRITICAL: Only {n} trades. Results are statistically meaningless. Need >=30 trades for reliable metrics."
        elif n < 30:
            warning = f"WARNING: Only {n} trades. Metrics have high uncertainty. Need >=30 trades for reliable Sharpe ratio."
        elif n < 50:
            warning = f"NOTE: {n} trades. Metrics are preliminary. Consider >=50 trades for walk-forward validation."

        # Confidence interval for win rate (Wilson score interval)
        if n >= 10:
            z = 1.96  # 95% confidence
            denom = 1 + z**2 / n
            center = (win_rate/100 + z**2 / (2*n)) / denom
            margin = z * math.sqrt((win_rate/100 * (1 - win_rate/100) + z**2 / (4*n)) / n) / denom
            ci_lower = round((center - margin) * 100, 1)
            ci_upper = round((center + margin) * 100, 1)
        else:
            ci_lower = None
            ci_upper = None

        # Phase 17: Note which stop type was used
        atr_trades = [t for t in trades if t.get("stop_type") == "atr"]
        fallback_trades = [t for t in trades if t.get("stop_type") == "fallback_pct"]

        return {
            "total_trades": n,
            "win_rate": round(win_rate, 1),
            "win_rate_ci_95": f"{ci_lower}-{ci_upper}%" if ci_lower else "N/A",
            "avg_pnl": round(avg_pnl, 2),
            "total_pnl": round(total_pnl, 2),
            "sharpe_ratio": round(sharpe, 2),
            "sharpe_reliable": n >= 30,
            "max_drawdown": round(max_dd, 2),
            "profit_factor": round(profit_factor, 2) if profit_factor != float('inf') else "inf",
            "winning_trades": len(winning),
            "losing_trades": len(losing),
            "statistical_warning": warning,
            "stop_type_breakdown": {
                "atr_stops": len(atr_trades),
                "fallback_pct_stops": len(fallback_trades),
            },
        }

    def walk_forward_test(self, weights, price_history, train_days=20, test_days=10):
        """Walk-forward validation"""
        all_dates = sorted(set(
            date for history in price_history.values()
            for date in [h["date"] for h in history]
        ))

        if len(all_dates) < train_days + test_days:
            return {"error": "Insufficient data for walk-forward test"}

        windows = []
        i = 0
        while i + train_days + test_days <= len(all_dates):
            train_dates = all_dates[i:i+train_days]
            test_dates = all_dates[i+train_days:i+train_days+test_days]
            windows.append((train_dates, test_dates))
            i += test_days  # Non-overlapping test windows

        results = []
        for train_dates, test_dates in windows:
            # Get candidates for training period
            train_candidates = self._get_candidates_for_dates(train_dates)

            if not train_candidates:
                continue

            # Test on test period
            test_metrics = self.test_weights(
                weights,
                price_history,
                train_candidates,
                lookback=len(test_dates)
            )

            test_metrics["train_dates"] = f"{train_dates[0]} to {train_dates[-1]}"
            test_metrics["test_dates"] = f"{test_dates[0]} to {test_dates[-1]}"
            results.append(test_metrics)

        # Aggregate results
        if results:
            avg_win_rate = sum(r["win_rate"] for r in results) / len(results)
            avg_sharpe = sum(r["sharpe_ratio"] for r in results) / len(results)
            avg_pnl = sum(r["avg_pnl"] for r in results) / len(results)

            return {
                "windows_tested": len(results),
                "avg_win_rate": round(avg_win_rate, 1),
                "avg_sharpe": round(avg_sharpe, 2),
                "avg_pnl": round(avg_pnl, 2),
                "consistent_win_rate": sum(1 for r in results if r["win_rate"] > 50) / len(results) * 100,
                "window_results": results
            }

        return {"error": "No valid test windows"}

    def _get_candidates_for_dates(self, dates):
        """Get candidates for specific dates"""
        candidates = []

        for date_str in dates:
            stage2_file = self.data_dir / f"stage2_{date_str}.json"
            if stage2_file.exists():
                with open(stage2_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    candidates.extend(data.get("candidates", []))

        return candidates

    def optimize_weights(self, price_history, candidates, iterations=100):
        """Optimize weights using grid search"""
        import random

        best_metrics = None
        best_weights = None

        for _ in range(iterations):
            # Generate random weights
            w1 = random.uniform(0.05, 0.40)
            w2 = random.uniform(0.05, 0.40)
            w3 = random.uniform(0.05, 0.40)
            w4 = random.uniform(0.05, 0.40)
            w5 = 1.0 - w1 - w2 - w3 - w4

            # Ensure all weights are positive
            if w5 <= 0:
                continue

            test_weights = {
                "stage1": {
                    "revenue_momentum": round(w1, 2),
                    "profitability": round(w2, 2),
                    "valuation": round(w3, 2),
                    "institutional_flow": round(w4, 2),
                    "technical_momentum": round(w5, 2)
                }
            }

            # Test these weights
            metrics = self.test_weights(test_weights["stage1"], price_history, candidates)

            # Score based on Sharpe + win rate - drawdown
            score = metrics["sharpe_ratio"] + (metrics["win_rate"] / 100) - (metrics["max_drawdown"] / 100)

            if best_metrics is None or score > best_metrics["score"]:
                best_metrics = {**metrics, "score": score}
                best_weights = test_weights

        return {
            "best_weights": best_weights,
            "metrics": best_metrics
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Backtesting framework")
    parser.add_argument("--mode", choices=["test", "walk-forward", "optimize"], default="test")
    parser.add_argument("--iterations", type=int, default=100, help="Optimization iterations")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    backtester = Backtester()
    prices = backtester.load_price_history()
    weights = backtester.load_weights()

    # Load recent candidates
    stage2_files = sorted(backtester.data_dir.glob("stage2_*.json"))
    if not stage2_files:
        print("No Stage 2 data found")
        return

    # Use most recent candidates
    with open(stage2_files[-1], 'r', encoding='utf-8') as f:
        data = json.load(f)
    candidates = data.get("candidates", [])

    if args.mode == "test":
        metrics = backtester.test_weights(weights["stage1"], prices, candidates)

        if args.verbose:
            print("Weight Test Results (Phase 17: ATR-based stops):")
            for key, value in metrics.items():
                print(f"   {key}: {value}")

    elif args.mode == "walk-forward":
        results = backtester.walk_forward_test(weights["stage1"], prices)

        if args.verbose:
            print("Walk-Forward Test Results:")
            for key, value in results.items():
                if key != "window_results":
                    print(f"   {key}: {value}")

    elif args.mode == "optimize":
        results = backtester.optimize_weights(prices, candidates, args.iterations)

        if args.verbose:
            print("Optimization Results:")
            print(f"   Best weights: {results['best_weights']}")
            print(f"   Metrics:")
            for key, value in results["metrics"].items():
                print(f"     {key}: {value}")

    return results if "results" in locals() else metrics


if __name__ == "__main__":
    main()
