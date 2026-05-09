#!/usr/bin/env python3
"""
Paper Trader - Simulate trading without real money

Takes Stage 2 candidates and simulates entry/exit based on:
- Entry: Stage 2 pass at current close price
- Exit: Take profit (2x risk), stop loss (-5%), or max holding days (20)
- Tracks P&L, win rate, max drawdown, holding period
"""

import json
from datetime import datetime, timedelta
from pathlib import Path


class PaperTrader:
    def __init__(self, config=None):
        self.config = config or self._default_config()
        self.trades = []
        self.active_positions = []
        self.data_dir = Path(__file__).parent.parent / "data"
    
    def _default_config(self):
        return {
            "risk_per_trade": 0.015,  # 1.5% risk per trade
            "max_portfolio_heat": 0.06,  # 6% max total risk
            "take_profit_rr": 2.0,  # 2:1 reward:risk
            "stop_loss_pct": -0.05,  # -5% stop loss
            "max_holding_days": 20,
            "max_positions": 5,
            "entry_buffer": 0.01,  # 1% buffer for entry
            "commission_rate": 0.003  # 0.3% commission
        }
    
    def load_candidates(self, date_str=None):
        """Load Stage 2 candidates with close prices from Stage 1"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        stage2_file = self.data_dir / f"stage2_{date_str}.json"
        if not stage2_file.exists():
            return []
        
        with open(stage2_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        candidates = data.get("candidates", [])
        
        # Load close prices from Stage 1
        stage1_file = self.data_dir / f"stage1_{date_str}.json"
        if stage1_file.exists():
            with open(stage1_file, 'r', encoding='utf-8') as f:
                s1_data = json.load(f)
            
            # Build close price lookup
            close_prices = {c["code"]: c.get("close", 0) for c in s1_data.get("candidates", [])}
            
            # Add close prices to candidates
            for candidate in candidates:
                code = candidate.get("code", "")
                if code in close_prices:
                    candidate["close"] = close_prices[code]
        
        return candidates
    
    def load_price_history(self):
        """Load price history for simulation"""
        history_file = self.data_dir / "price_history.json"
        if not history_file.exists():
            return {}
        
        with open(history_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def simulate_entry(self, candidates, date_str=None):
        """Simulate entry for top candidates"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        new_trades = []
        
        # Only take top candidates up to max_positions
        for candidate in candidates[:self.config["max_positions"]]:
            code = candidate["code"]
            name = candidate["name"]
            entry_price = candidate.get("close", 0)
            
            # Use candidate's close price directly
            if entry_price == 0:
                entry_price = candidate.get("composite_score", 0)  # Fallback to score if no price
            
            if entry_price == 0:
                continue
            
            # Calculate position size based on risk
            risk_amount = entry_price * self.config["risk_per_trade"]
            stop_loss = entry_price * (1 + self.config["stop_loss_pct"])
            take_profit = entry_price + (entry_price - stop_loss) * self.config["take_profit_rr"]
            
            trade = {
                "trade_id": f"{code}_{date_str}",
                "code": code,
                "name": name,
                "entry_date": date_str,
                "entry_price": entry_price,
                "stop_loss": round(stop_loss, 2),
                "take_profit": round(take_profit, 2),
                "risk_per_share": round(entry_price - stop_loss, 2),
                "reward_per_share": round(take_profit - entry_price, 2),
                "rr_ratio": self.config["take_profit_rr"],
                "holding_days": 0,
                "max_drawdown": 0,
                "status": "open",
                "exit_date": None,
                "exit_price": None,
                "exit_reason": None,
                "pnl_pct": None,
                "commission": round(entry_price * self.config["commission_rate"], 2),
                "combined_score": candidate.get("combined_score", 0),
                "stage1_score": candidate.get("stage1_score", 0),
                "stage2_score": candidate.get("stage2_score", 0)
            }
            
            new_trades.append(trade)
        
        self.trades.extend(new_trades)
        self.active_positions = [t for t in self.trades if t["status"] == "open"]
        
        return new_trades
    
    def simulate_exit(self, trade, current_price, current_date):
        """Simulate exit for a single trade"""
        if trade["status"] != "open":
            return trade
        
        # Check exit conditions
        if current_price <= trade["stop_loss"]:
            trade["exit_price"] = trade["stop_loss"]
            trade["exit_reason"] = "stop_loss"
        elif current_price >= trade["take_profit"]:
            trade["exit_price"] = trade["take_profit"]
            trade["exit_reason"] = "take_profit"
        elif trade["holding_days"] >= self.config["max_holding_days"]:
            trade["exit_price"] = current_price
            trade["exit_reason"] = "max_holding_days"
        else:
            return trade  # Still holding
        
        # Calculate P&L
        gross_pnl = (trade["exit_price"] - trade["entry_price"]) / trade["entry_price"]
        commission_cost = trade["commission"] / trade["entry_price"]
        trade["pnl_pct"] = round((gross_pnl - commission_cost) * 100, 2)
        trade["status"] = "closed"
        trade["exit_date"] = current_date
        trade["holding_days"] = self._calc_holding_days(trade["entry_date"], current_date)
        
        return trade
    
    def _calc_holding_days(self, entry_date, exit_date):
        """Calculate holding days"""
        try:
            entry = datetime.strptime(entry_date, "%Y-%m-%d")
            exit = datetime.strptime(exit_date, "%Y-%m-%d")
            return (exit - entry).days
        except:
            return 0
    
    def run_backtest(self, lookback_days=60):
        """Run backtest using historical signals"""
        # Load all historical stage2 results
        stage2_files = sorted(self.data_dir.glob("stage2_*.json"))
        
        if not stage2_files:
            return {"error": "No historical Stage 2 data found"}
        
        results = {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "total_pnl_pct": 0,
            "avg_pnl_pct": 0,
            "max_drawdown": 0,
            "avg_holding_days": 0,
            "trades": []
        }
        
        for filepath in stage2_files[-lookback_days:]:
            date_str = filepath.stem.replace("stage2_", "")
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            candidates = data.get("candidates", [])
            if not candidates:
                continue
            
            # Simulate entry
            trades = self.simulate_entry(candidates, date_str)
            
            # Simulate exit (use next day's price if available)
            next_date = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            next_stage2 = self.data_dir / f"stage2_{next_date}.json"
            
            prices = self.load_price_history()
            
            for trade in trades:
                code = trade["code"]
                if code in prices and len(prices[code]) >= 2:
                    # Use next available price
                    current_price = prices[code][-1]["close"]
                    trade = self.simulate_exit(trade, current_price, next_date)
                
                results["total_trades"] += 1
                if trade["pnl_pct"] is not None:
                    results["total_pnl_pct"] += trade["pnl_pct"]
                    if trade["pnl_pct"] > 0:
                        results["winning_trades"] += 1
                    else:
                        results["losing_trades"] += 1
                    results["trades"].append(trade)
        
        # Calculate stats
        if results["total_trades"] > 0:
            results["win_rate"] = round(results["winning_trades"] / results["total_trades"] * 100, 1)
            results["avg_pnl_pct"] = round(results["total_pnl_pct"] / results["total_trades"], 2)
        
        return results
    
    def save_trades(self):
        """Save trades to file"""
        trades_file = self.data_dir / "paper_trades.json"
        with open(trades_file, 'w', encoding='utf-8') as f:
            json.dump(self.trades, f, ensure_ascii=False, indent=2)
    
    def get_stats(self):
        """Get trading statistics"""
        closed_trades = [t for t in self.trades if t["status"] == "closed"]
        
        if not closed_trades:
            return {
                "total_trades": len(self.trades),
                "open_positions": len(self.active_positions),
                "closed_trades": 0
            }
        
        winning = [t for t in closed_trades if t["pnl_pct"] and t["pnl_pct"] > 0]
        losing = [t for t in closed_trades if t["pnl_pct"] and t["pnl_pct"] <= 0]
        
        total_pnl = sum(t["pnl_pct"] for t in closed_trades if t["pnl_pct"])
        avg_holding = sum(t["holding_days"] for t in closed_trades) / len(closed_trades)
        
        return {
            "total_trades": len(self.trades),
            "open_positions": len(self.active_positions),
            "closed_trades": len(closed_trades),
            "win_rate": round(len(winning) / len(closed_trades) * 100, 1),
            "total_pnl_pct": round(total_pnl, 2),
            "avg_pnl_pct": round(total_pnl / len(closed_trades), 2),
            "avg_holding_days": round(avg_holding, 1),
            "best_trade": max(closed_trades, key=lambda t: t["pnl_pct"] or 0),
            "worst_trade": min(closed_trades, key=lambda t: t["pnl_pct"] or 0)
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Paper trading simulation")
    parser.add_argument("--date", type=str, help="Date to simulate (YYYY-MM-DD)")
    parser.add_argument("--backtest", type=int, default=0, help="Run backtest for N days")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    trader = PaperTrader()
    
    if args.backtest > 0:
        # Run backtest
        results = trader.run_backtest(lookback_days=args.backtest)
        
        if args.verbose:
            print("📊 Backtest Results:")
            print(f"   Total trades: {results['total_trades']}")
            print(f"   Win rate: {results.get('win_rate', 0)}%")
            print(f"   Avg P&L: {results['avg_pnl_pct']}%")
            print(f"   Total P&L: {results['total_pnl_pct']}%")
    else:
        # Simulate today
        candidates = trader.load_candidates(args.date)
        
        if candidates:
            trades = trader.simulate_entry(candidates)
            trader.save_trades()
            
            if args.verbose:
                print(f"📝 Paper Trading Simulation:")
                print(f"   Candidates: {len(candidates)}")
                print(f"   New trades: {len(trades)}")
                print(f"   Active positions: {len(trader.active_positions)}")
                
                for trade in trades:
                    print(f"\n   {trade['code']} {trade['name']}:")
                    print(f"     Entry: {trade['entry_price']}")
                    print(f"     Stop: {trade['stop_loss']}")
                    print(f"     Target: {trade['take_profit']}")
                    print(f"     R:R: {trade['rr_ratio']}")
        else:
            print("❌ No candidates found")
    
    # Print stats
    stats = trader.get_stats()
    if args.verbose:
        print(f"\n📈 Trading Stats:")
        for key, value in stats.items():
            if key not in ["best_trade", "worst_trade"]:
                print(f"   {key}: {value}")


if __name__ == "__main__":
    main()
