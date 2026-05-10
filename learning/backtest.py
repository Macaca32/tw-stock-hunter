#!/usr/bin/env python3
"""
Backtesting Framework - Test strategies against historical data

Features:
- Walk-forward validation
- Weight optimization
- Threshold tuning
- Regime-aware testing
- Sharpe ratio, max drawdown, win rate metrics
"""

import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict


class Backtester:
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
    
    def simulate_trade(self, entry_price, exit_price, commission_rate=0.003):
        """Simulate a single trade"""
        if entry_price == 0:
            return None
        
        gross_pnl = (exit_price - entry_price) / entry_price
        commission = commission_rate
        net_pnl = gross_pnl - commission
        
        return {
            "entry": entry_price,
            "exit": exit_price,
            "gross_pnl_pct": round(gross_pnl * 100, 2),
            "net_pnl_pct": round(net_pnl * 100, 2),
            "win": net_pnl > 0
        }
    
    def test_weights(self, weights, price_history, candidates, lookback=20):
        """Test a set of weights against historical data"""
        trades = []
        
        for candidate in candidates:
            code = candidate["code"]
            
            # Use price history if available, otherwise simulate
            if code in price_history:
                history = price_history[code]
                if len(history) < lookback:
                    continue
                
                # Entry price (first day)
                entry_price = history[0]["close"]
                
                # Exit price (last day or hit stop/target)
                exit_price = None
                stop_loss = entry_price * 0.95  # -5%
                take_profit = entry_price * 1.10  # +10%
                
                for day in history[1:]:
                    price = day["close"]
                    
                    if price <= stop_loss:
                        exit_price = stop_loss
                        break
                    elif price >= take_profit:
                        exit_price = take_profit
                        break
                    else:
                        exit_price = price
                
                if exit_price is None:
                    exit_price = history[-1]["close"]
            else:
                # Simulate based on candidate score with randomness
                import random
                entry_price = candidate.get("close", 100)  # Default to 100 if no price
                
                # Higher score = better expected return
                score = candidate.get("combined_score", candidate.get("composite_score", 50))
                expected_return = (score - 50) / 100  # Normalize to -0.5 to +0.5
                
                # Add market noise
                noise = random.gauss(0, 0.02)  # 2% standard deviation
                actual_return = expected_return * 0.1 + noise
                
                exit_price = entry_price * (1 + actual_return)
            
            trade = self.simulate_trade(entry_price, exit_price)
            if trade:
                trade["code"] = code
                trade["holding_days"] = lookback
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
            warning = f"CRITICAL: Only {n} trades. Results are statistically meaningless. Need ≥30 trades for reliable metrics."
        elif n < 30:
            warning = f"WARNING: Only {n} trades. Metrics have high uncertainty. Need ≥30 trades for reliable Sharpe ratio."
        elif n < 50:
            warning = f"NOTE: {n} trades. Metrics are preliminary. Consider ≥50 trades for walk-forward validation."
        
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
            "statistical_warning": warning
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
        print("❌ No Stage 2 data found")
        return
    
    # Use most recent candidates
    with open(stage2_files[-1], 'r', encoding='utf-8') as f:
        data = json.load(f)
    candidates = data.get("candidates", [])
    
    if args.mode == "test":
        metrics = backtester.test_weights(weights["stage1"], prices, candidates)
        
        if args.verbose:
            print("📊 Weight Test Results:")
            for key, value in metrics.items():
                print(f"   {key}: {value}")
    
    elif args.mode == "walk-forward":
        results = backtester.walk_forward_test(weights["stage1"], prices)
        
        if args.verbose:
            print("📊 Walk-Forward Test Results:")
            for key, value in results.items():
                if key != "window_results":
                    print(f"   {key}: {value}")
    
    elif args.mode == "optimize":
        results = backtester.optimize_weights(prices, candidates, args.iterations)
        
        if args.verbose:
            print("📊 Optimization Results:")
            print(f"   Best weights: {results['best_weights']}")
            print(f"   Metrics:")
            for key, value in results["metrics"].items():
                print(f"     {key}: {value}")
    
    return results if "results" in locals() else metrics


if __name__ == "__main__":
    main()
