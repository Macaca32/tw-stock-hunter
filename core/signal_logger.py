#!/usr/bin/env python3
"""
Signal Logger - Track all signals for self-improvement loop
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class SignalLogger:
    def __init__(self, data_dir=None):
        self.data_dir = data_dir or Path(__file__).parent.parent / "data"
        self.signal_file = self.data_dir / "signal_log.json"
        self.signals = self._load()
    
    def _load(self):
        """Load existing signals"""
        if self.signal_file.exists():
            with open(self.signal_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []
    
    def save(self):
        """Save signals to disk"""
        with open(self.signal_file, 'w', encoding='utf-8') as f:
            json.dump(self.signals, f, ensure_ascii=False, indent=2)
    
    def log_signal(self, code, name, entry_price, stage_scores, composite, 
                   regime="unknown", catalyst=None):
        """Log a new signal"""
        signal = {
            "signal_id": f"{code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "code": code,
            "name": name,
            "entry_price": entry_price,
            "stage_scores": stage_scores,
            "composite": composite,
            "holding_days": 0,
            "exit_price": None,
            "exit_reason": None,
            "pnl_pct": None,
            "max_drawdown": None,
            "regime_at_entry": regime,
            "catalyst_present": catalyst,
            "win": None,
            "timestamp": datetime.now().isoformat()
        }
        self.signals.append(signal)
        self.save()
        return signal["signal_id"]
    
    def update_exit(self, signal_id, exit_price, exit_reason, holding_days, 
                    max_drawdown=None):
        """Update signal with exit info"""
        for signal in self.signals:
            if signal["signal_id"] == signal_id:
                signal["exit_price"] = exit_price
                signal["exit_reason"] = exit_reason
                signal["holding_days"] = holding_days
                signal["max_drawdown"] = max_drawdown
                
                # Calculate P&L
                if signal["entry_price"] and exit_price:
                    pnl = (exit_price - signal["entry_price"]) / signal["entry_price"]
                    signal["pnl_pct"] = round(pnl * 100, 2)
                    signal["win"] = pnl > 0
                
                self.save()
                return True
        return False
    
    def get_stats(self):
        """Get trading statistics"""
        completed = [s for s in self.signals if s["exit_price"] is not None]
        if not completed:
            return {"total_signals": len(self.signals), "completed": 0}
        
        wins = [s for s in completed if s["win"]]
        losses = [s for s in completed if not s["win"]]
        
        win_rate = len(wins) / len(completed) if completed else 0
        avg_win = sum(s["pnl_pct"] for s in wins) / len(wins) if wins else 0
        avg_loss = sum(s["pnl_pct"] for s in losses) / len(losses) if losses else 0
        
        return {
            "total_signals": len(self.signals),
            "completed": len(completed),
            "win_rate": round(win_rate * 100, 1),
            "avg_win_pct": round(avg_win, 2),
            "avg_loss_pct": round(avg_loss, 2),
            "profit_factor": round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else float('inf'),
            "avg_holding_days": round(sum(s["holding_days"] for s in completed) / len(completed), 1)
        }


def generate_report(date_str=None, verbose=False):
    """Generate daily screening report"""
    from stage1_screen import load_data, load_config
    from stage2_deep import load_stage1_results, run_stage2
    
    # Load data
    datasets, date = load_data(date_str)
    stage1_results = load_stage1_results(date_str)
    stage2_results = run_stage2(date_str, verbose=False)
    
    # Load signal logger stats
    logger = SignalLogger()
    stats = logger.get_stats()
    
    # Generate report
    report = {
        "date": date,
        "timestamp": datetime.now().isoformat(),
        "stage1": {
            "total_screened": stage1_results["summary"]["total_screened"],
            "passed": stage1_results["summary"]["passed"],
            "watchlist": stage1_results["summary"]["watchlist"],
            "rejected": stage1_results["summary"]["rejected"],
            "top_10": stage1_results["candidates"][:10]
        },
        "stage2": {
            "passed": stage2_results["summary"]["passed_stage2"],
            "disqualified": stage2_results["summary"]["disqualified"],
            "top_10": stage2_results["candidates"][:10]
        },
        "trading_stats": stats,
        "market_regime": "unknown"  # Will be updated by regime detector
    }
    
    # Save report
    data_dir = Path(__file__).parent.parent / "reports"
    data_dir.mkdir(exist_ok=True)
    
    report_file = data_dir / f"report_{date}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    if verbose:
        logger.info("Daily Report for %s", date)
        logger.info("Stage 1: %d passed / %d screened", stage1_results['summary']['passed'], stage1_results['summary']['total_screened'])
        logger.info("Stage 2: %d passed / %d disqualified", stage2_results['summary']['passed_stage2'], stage2_results['summary']['disqualified'])
        logger.info("Trading Stats: %s", stats)
    
    return report


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate daily screening report")
    parser.add_argument("--date", type=str, help="Date to report (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    generate_report(date_str=args.date, verbose=args.verbose)


if __name__ == "__main__":
    main()
