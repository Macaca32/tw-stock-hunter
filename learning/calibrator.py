#!/usr/bin/env python3
"""
Learning Module - Self-improvement loop for weight calibration

Features:
- Weight calibration based on signal performance
- Walk-forward validation before applying changes
- Regime-aware adjustments
- Feature importance tracking
"""

import json
import math
from datetime import datetime
from pathlib import Path


class WeightCalibrator:
    def __init__(self):
        self.config_dir = Path(__file__).parent.parent / "config"
        self.data_dir = Path(__file__).parent.parent / "data"
        self.weights_file = self.config_dir / "weights.json"
        self.learning_log = self.data_dir / "learning_log.json"
    
    def load_weights(self):
        """Load current weights"""
        with open(self.weights_file, 'r') as f:
            return json.load(f)
    
    def save_weights(self, weights):
        """Save updated weights"""
        weights["updated"] = datetime.now().strftime("%Y-%m-%d")
        with open(self.weights_file, 'w') as f:
            json.dump(weights, f, indent=2, ensure_ascii=False)
    
    def load_signals(self):
        """Load completed signals from paper trading"""
        signal_file = self.data_dir / "signal_log.json"
        if not signal_file.exists():
            return []
        
        with open(signal_file, 'r') as f:
            signals = json.load(f)
        
        # Return only completed signals
        return [s for s in signals if s.get("exit_price") is not None]
    
    def load_paper_trades(self):
        """Load paper trading results"""
        trades_file = self.data_dir / "paper_trades.json"
        if not trades_file.exists():
            return []
        
        with open(trades_file, 'r') as f:
            return json.load(f)
    
    def calculate_dimension_performance(self, signals):
        """Calculate win rate for each scoring dimension"""
        if not signals:
            return {}
        
        # Group by score ranges
        dimension_stats = {
            "revenue": {"high": [], "low": []},
            "profitability": {"high": [], "low": []},
            "valuation": {"high": [], "low": []},
            "flow": {"high": [], "low": []},
            "momentum": {"high": [], "low": []}
        }
        
        for signal in signals:
            scores = signal.get("stage_scores", {})
            win = signal.get("win", False)
            
            for dim in dimension_stats:
                score = scores.get(dim, 50)
                if score >= 70:
                    dimension_stats[dim]["high"].append(win)
                elif score <= 30:
                    dimension_stats[dim]["low"].append(win)
        
        # Calculate win rates
        results = {}
        for dim, stats in dimension_stats.items():
            high_wr = sum(stats["high"]) / len(stats["high"]) if stats["high"] else 0
            low_wr = sum(stats["low"]) / len(stats["low"]) if stats["low"] else 0
            
            results[dim] = {
                "high_score_win_rate": round(high_wr * 100, 1),
                "low_score_win_rate": round(low_wr * 100, 1),
                "edge": round((high_wr - low_wr) * 100, 1),
                "sample_size": len(stats["high"]) + len(stats["low"])
            }
        
        return results
    
    def walk_forward_validate(self, new_weights, min_win_rate=45):
        """Validate weight changes using walk-forward testing"""
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        
        from learning.backtest import Backtester
        
        backtester = Backtester()
        prices = backtester.load_price_history()
        
        if not prices:
            return {"valid": False, "reason": "No price history"}
        
        # Load recent candidates
        stage2_files = sorted(self.data_dir.glob("stage2_*.json"))
        if not stage2_files:
            return {"valid": False, "reason": "No Stage 2 data"}
        
        with open(stage2_files[-1], 'r', encoding='utf-8') as f:
            data = json.load(f)
        candidates = data.get("candidates", [])
        
        # Test new weights
        metrics = backtester.test_weights(new_weights, prices, candidates)
        
        # Validate
        if metrics["total_trades"] < 10:
            return {"valid": False, "reason": "Insufficient trades for validation"}
        
        if metrics["win_rate"] < min_win_rate:
            return {
                "valid": False,
                "reason": f"Win rate {metrics['win_rate']}% below threshold {min_win_rate}%",
                "metrics": metrics
            }
        
        if metrics["sharpe_ratio"] < 0:
            return {
                "valid": False,
                "reason": f"Negative Sharpe ratio {metrics['sharpe_ratio']}",
                "metrics": metrics
            }
        
        return {
            "valid": True,
            "metrics": metrics
        }
    
    def calibrate_weights(self, min_signals=20):
        """Calibrate weights based on signal performance
        
        Only runs if we have enough completed signals
        """
        signals = self.load_signals()
        trades = self.load_paper_trades()
        
        # Combine signals and trades
        all_data = signals + trades
        
        if len(all_data) < min_signals:
            return {
                "status": "insufficient_data",
                "signals_needed": min_signals - len(all_data),
                "current_signals": len(all_data)
            }
        
        # Calculate dimension performance
        dimension_perf = self.calculate_dimension_performance(all_data)
        
        # Adjust weights based on edge
        weights = self.load_weights()
        stage1_weights = weights.get("stage1", {})
        
        for dim, perf in dimension_perf.items():
            key = dim if dim != "flow" else "institutional_flow"
            if key in stage1_weights:
                current = stage1_weights[key]
                edge = perf["edge"] / 100  # Normalize to 0-1
                
                # Only adjust if we have enough samples
                if perf["sample_size"] < 5:
                    continue
                
                # Increase weight for dimensions with higher edge
                adjustment = edge * 0.1
                stage1_weights[key] = round(min(0.40, max(0.05, current + adjustment)), 2)
        
        # Normalize weights to sum to 1.0
        total = sum(stage1_weights.values())
        for key in stage1_weights:
            stage1_weights[key] = round(stage1_weights[key] / total, 2)
        
        # Walk-forward validate
        validation = self.walk_forward_validate(stage1_weights)
        
        if not validation["valid"]:
            return {
                "status": "validation_failed",
                "reason": validation["reason"],
                "current_weights": stage1_weights,
                "metrics": validation.get("metrics", {})
            }
        
        # Apply validated weights
        self.save_weights(weights)
        
        # Log calibration
        self._log_calibration(dimension_perf, stage1_weights, validation["metrics"])
        
        return {
            "status": "calibrated",
            "signals_used": len(all_data),
            "dimension_performance": dimension_perf,
            "new_weights": stage1_weights,
            "validation_metrics": validation["metrics"]
        }
    
    def _log_calibration(self, dimension_perf, new_weights, metrics):
        """Log calibration event"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": "weight_calibration",
            "dimension_performance": dimension_perf,
            "new_weights": new_weights,
            "validation_metrics": metrics
        }
        
        # Load existing log
        entries = []
        if self.learning_log.exists():
            with open(self.learning_log, 'r') as f:
                entries = json.load(f)
        
        entries.append(log_entry)
        
        with open(self.learning_log, 'w') as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
    
    def get_feature_importance(self):
        """Track feature importance over time"""
        signals = self.load_signals()
        
        if not signals:
            return {}
        
        # Calculate correlation between each dimension score and P&L
        importance = {}
        
        for dim in ["revenue", "profitability", "valuation", "flow", "momentum"]:
            scores = [s.get("stage_scores", {}).get(dim, 50) for s in signals]
            pnls = [s.get("pnl_pct", 0) for s in signals if s.get("pnl_pct") is not None]
            
            if len(scores) != len(pnls) or len(scores) < 5:
                continue
            
            # Simple correlation
            mean_score = sum(scores) / len(scores)
            mean_pnl = sum(pnls) / len(pnls)
            
            cov = sum((scores[i] - mean_score) * (pnls[i] - mean_pnl) for i in range(len(scores))) / len(scores)
            std_score = (sum((s - mean_score) ** 2 for s in scores) / len(scores)) ** 0.5
            std_pnl = (sum((p - mean_pnl) ** 2 for p in pnls) / len(pnls)) ** 0.5
            
            if std_score > 0 and std_pnl > 0:
                correlation = cov / (std_score * std_pnl)
                importance[dim] = round(correlation, 3)
        
        return importance


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Calibrate scoring weights")
    parser.add_argument("--min-signals", type=int, default=20, help="Minimum signals for calibration")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    calibrator = WeightCalibrator()
    result = calibrator.calibrate_weights(min_signals=args.min_signals)
    
    if args.verbose:
        print(f"Calibration Result:")
        print(f"  Status: {result['status']}")
        if result['status'] == 'calibrated':
            print(f"  Signals used: {result['signals_used']}")
            print(f"  New weights: {result['new_weights']}")
            print(f"  Validation: {result['validation_metrics']}")
        elif result['status'] == 'validation_failed':
            print(f"  Reason: {result['reason']}")
            print(f"  Current weights: {result['current_weights']}")
        else:
            print(f"  Signals needed: {result['signals_needed']}")
    
    return result


if __name__ == "__main__":
    main()
