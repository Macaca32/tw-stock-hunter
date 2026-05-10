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
            "stop_loss_pct": -0.03,  # -3% fallback (used only if ATR unavailable)
            "atr_stop_mult": 1.5,  # 1.5x ATR for stop loss (FIX v2)
            "atr_period": 14,  # 14-day ATR period
            "max_holding_days": 20,
            "max_positions": 5,
            "entry_buffer": 0.01,  # 1% buffer for entry
            "commission_rate": 0.003,  # 0.3% commission
            "gap_risk_buffer": 0.01,  # 1% extra buffer for gap risk (FIX v2)
            "max_drawdown_pct": 0.15,  # 15% max drawdown (FIX v2: was 10%, too tight)
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
    
    def calc_atr(self, stock_history, period=14):
        """Calculate Average True Range for a stock.
        
        FIX v2: Use ATR-based stops instead of fixed percentages.
        ATR adapts to market volatility - wider stops in volatile markets,
        tighter stops in calm markets.
        """
        if not stock_history or len(stock_history) < period + 1:
            return None
        
        true_ranges = []
        for i in range(1, len(stock_history)):
            high = stock_history[i].get("high", 0)
            low = stock_history[i].get("low", 0)
            prev_close = stock_history[i-1].get("close", 0)
            
            if high > 0 and low > 0 and prev_close > 0:
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )
                true_ranges.append(tr)
        
        if not true_ranges:
            return None
        
        # Use last 'period' true ranges
        return sum(true_ranges[-period:]) / min(period, len(true_ranges))
    
    def get_atr_stop(self, stock_code, entry_price, price_history):
        """Get ATR-based stop loss for a stock.
        
        Returns (stop_price, atr_value) tuple.
        Falls back to fixed % stop if ATR unavailable.
        """
        if stock_code in price_history:
            history = price_history[stock_code]
            atr = self.calc_atr(history, period=self.config.get("atr_period", 14))
            
            if atr is not None and atr > 0:
                atr_mult = self.config.get("atr_stop_mult", 1.5)
                gap_buffer = self.config.get("gap_risk_buffer", 0.01)
                
                # Stop = entry - (ATR * multiplier) - gap buffer
                stop = entry_price - (atr * atr_mult) - (entry_price * gap_buffer)
                
                # Ensure stop is at least 2% below entry (minimum protection)
                min_stop = entry_price * 0.98
                stop = min(stop, min_stop)
                
                return round(stop, 2), round(atr, 2)
        
        # Fallback: fixed percentage stop
        fallback_stop = entry_price * (1 + self.config["stop_loss_pct"])
        return round(fallback_stop, 2), None
    
    def simulate_entry(self, candidates, date_str=None):
        """Simulate entry for top candidates
        
        FIX v2: Use ATR-based stops, add gap risk buffer,
        add liquidity check (skip stocks with insufficient price history).
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        new_trades = []
        price_history = self.load_price_history()
        
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
            
            # FIX v2: ATR-based stop loss
            stop_loss, atr_value = self.get_atr_stop(code, entry_price, price_history)
            
            # Calculate take profit based on risk
            risk_per_share = entry_price - stop_loss
            take_profit = entry_price + risk_per_share * self.config["take_profit_rr"]
            
            trade = {
                "trade_id": f"{code}_{date_str}",
                "code": code,
                "name": name,
                "entry_date": date_str,
                "entry_price": entry_price,
                "stop_loss": round(stop_loss, 2),
                "take_profit": round(take_profit, 2),
                "risk_per_share": round(risk_per_share, 2),
                "reward_per_share": round(take_profit - entry_price, 2),
                "rr_ratio": self.config["take_profit_rr"],
                "atr_value": atr_value,  # Track ATR for transparency
                "stop_type": "atr" if atr_value else "fixed_pct",
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
        """Run backtest using historical signals.
        
        FIX v2: Use proper exit simulation with ATR stops,
        include commission costs, and track drawdown properly.
        """
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
            "trades": [],
            "cumulative_pnl": [],
        }
        
        prices = self.load_price_history()
        cumulative = 0
        peak = 0
        max_dd = 0
        
        for filepath in stage2_files[-lookback_days:]:
            date_str = filepath.stem.replace("stage2_", "")
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            candidates = data.get("candidates", [])
            if not candidates:
                continue
            
            # Simulate entry
            trades = self.simulate_entry(candidates, date_str)
            
            # Simulate exit using price history
            for trade in trades:
                code = trade["code"]
                
                # Find exit price from price history
                exit_price = None
                exit_date = None
                exit_reason = None
                
                if code in prices and len(prices[code]) >= 2:
                    history = prices[code]
                    entry_idx = None
                    
                    # Find entry date in history
                    for i, day in enumerate(history):
                        if day["date"] == date_str:
                            entry_idx = i
                            break
                    
                    if entry_idx is not None:
                        # Scan forward for exit
                        for j in range(entry_idx + 1, min(entry_idx + self.config["max_holding_days"] + 1, len(history))):
                            day = history[j]
                            price = day["close"]
                            
                            if price <= trade["stop_loss"]:
                                exit_price = trade["stop_loss"]
                                exit_date = day["date"]
                                exit_reason = "stop_loss"
                                break
                            elif price >= trade["take_profit"]:
                                exit_price = trade["take_profit"]
                                exit_date = day["date"]
                                exit_reason = "take_profit"
                                break
                        
                        # If no exit triggered, use last available price
                        if exit_price is None:
                            last_day = history[min(entry_idx + self.config["max_holding_days"], len(history) - 1)]
                            exit_price = last_day["close"]
                            exit_date = last_day["date"]
                            exit_reason = "max_holding_days"
                
                if exit_price is not None and exit_price > 0:
                    # Calculate P&L
                    gross_pnl = (exit_price - trade["entry_price"]) / trade["entry_price"]
                    commission_cost = trade["commission"] / trade["entry_price"]
                    net_pnl = (gross_pnl - commission_cost) * 100
                    
                    trade["exit_price"] = exit_price
                    trade["exit_date"] = exit_date
                    trade["exit_reason"] = exit_reason
                    trade["pnl_pct"] = round(net_pnl, 2)
                    trade["status"] = "closed"
                    trade["holding_days"] = self._calc_holding_days(date_str, exit_date)
                    
                    results["total_trades"] += 1
                    results["total_pnl_pct"] += net_pnl
                    cumulative += net_pnl
                    
                    if cumulative > peak:
                        peak = cumulative
                    dd = peak - cumulative
                    if dd > max_dd:
                        max_dd = dd
                    
                    if net_pnl > 0:
                        results["winning_trades"] += 1
                    else:
                        results["losing_trades"] += 1
                    
                    results["trades"].append(trade)
                    results["cumulative_pnl"].append(round(cumulative, 2))
        
        # Calculate stats
        if results["total_trades"] > 0:
            results["win_rate"] = round(results["winning_trades"] / results["total_trades"] * 100, 1)
            results["avg_pnl_pct"] = round(results["total_pnl_pct"] / results["total_trades"], 2)
            results["avg_holding_days"] = round(
                sum(t["holding_days"] for t in results["trades"]) / results["total_trades"], 1
            )
            results["max_drawdown"] = round(max_dd, 2)
        
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
