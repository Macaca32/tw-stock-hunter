#!/usr/bin/env python3
"""
Paper Trader - Simulate trading without real money

Takes Stage 2 candidates and simulates entry/exit based on:
- Entry: Stage 2 pass at current close price
- Exit: Take profit (2x risk), stop loss (-5%), or max holding days (20)
- Tracks P&L, win rate, max drawdown, holding period

Phase 26: Advanced backtesting enhancements:
  1. Multi-period validation (run_multi_period_backtest)
  2. Sector-adjusted returns (compute_sector_adjusted_returns)
  3. Drawdown analysis (compute_drawdown_analysis)

Phase 28: Portfolio Rebalancing Engine:
  1. Position sizing optimization (optimize_positions) — Kelly-inspired sizing
     based on Phase 27 signal strength with sector diversification constraints
  2. Sector rotation signals (compute_sector_rotation) — rolling-window
     relative strength analysis across sectors
  3. Correlation-based risk budgeting (check_correlation_risk) — pairwise
     correlation check with effective portfolio beta reporting
All additions are backward compatible — existing API unchanged.
"""

import json
import logging
import math
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


class PaperTrader:
    def __init__(self, config=None):
        self.config = config or self._default_config()
        self.trades = []
        self.active_positions = []
        self.data_dir = Path(__file__).parent.parent / "data"

        # FIX v3: Initialize corporate action handler
        self.corp_handler = None
        try:
            from corporate_actions import CorporateActionHandler
            self.corp_handler = CorporateActionHandler(str(self.data_dir))
        except ImportError:
            pass  # Corporate action handling unavailable

        # Phase 10 R8: Initialize holiday calendar for gap-aware position sizing
        self.holiday_calendar = None
        try:
            from holiday_calendar import HolidayCalendar
            self.holiday_calendar = HolidayCalendar(str(self.data_dir))
        except ImportError:
            pass

        # Phase 17: Use detect_regime_from_prices (accepts pre-filtered data)
        # Old code imported detect_regime(date_str, verbose) which cannot accept
        # pre-filtered price data — caused signature mismatch in backtest.
        self._detect_regime_from_prices = None
        self._regime_detector_available = False
        try:
            from regime_detector import detect_regime_from_prices
            if callable(detect_regime_from_prices):
                self._detect_regime_from_prices = detect_regime_from_prices
                self._regime_detector_available = True
        except ImportError:
            logger.debug("regime_detector not available — regime defaults to normal")
        except Exception as e:
            logger.warning(f"regime_detector import failed: {e}")

    def _default_config(self):
        return {
            "risk_per_trade": 0.015,  # 1.5% risk per trade
            "max_portfolio_heat": 0.06,  # 6% max total risk
            "take_profit_rr": 2.0,  # 2:1 reward:risk
            "stop_loss_pct": -0.03,  # -3% fallback (used only if ATR unavailable)
            "atr_stop_mult": 2.5,  # 2.5x ATR for stop loss (FIX v3: Taiwan mid-caps need wider stops)
            "atr_period": 14,  # 14-day ATR period
            "max_holding_days": 20,
            "max_positions": 5,
            "entry_buffer": 0.01,  # 1% buffer for entry
            "twse_roundtrip_cost": 0.006,  # 0.6% round-trip for TWSE (0.3% tax sell + 0.14% comm × 2)
            "tpex_roundtrip_cost": 0.007,  # 0.7% round-trip for TPEx (includes wider spreads/slippage on lower-liquidity stocks)
            "gap_risk_buffer_atr_mult": 0.5,  # FIX v3: 0.5x ATR for gap buffer (was flat 1%)
            "max_drawdown_pct": 0.20,  # 20% max drawdown (FIX v3: momentum strategy needs room)
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

    def get_atr_stop(self, stock_code, entry_price, price_history, override_atr_mult=None):
        """Get ATR-based stop loss for a stock.

        Returns (stop_price, atr_value) tuple.
        Falls back to fixed % stop if ATR unavailable.

        Args:
            override_atr_mult: Optional custom ATR multiplier (e.g., wider stops on post-holiday days).
        """
        if stock_code in price_history:
            history = price_history[stock_code]
            atr = self.calc_atr(history, period=self.config.get("atr_period", 14))

            if atr is not None and atr > 0:
                atr_mult = override_atr_mult or self.config.get("atr_stop_mult", 2.5)
                gap_mult = self.config.get("gap_risk_buffer_atr_mult", 0.5)

                # FIX v3: Gap buffer = 0.5x ATR (not flat 1%)
                # Stop = entry - (ATR * multiplier) - (ATR * gap_mult)
                stop = entry_price - (atr * atr_mult) - (atr * gap_mult)

                # Ensure stop is at least 2% below entry (minimum protection)
                min_stop = entry_price * 0.98
                stop = min(stop, min_stop)

                return round(stop, 2), round(atr, 2)

        # Fallback: fixed percentage stop
        fallback_stop = entry_price * (1 + self.config["stop_loss_pct"])
        return round(fallback_stop, 2), None

    def simulate_entry(self, candidates, date_str=None, regime_mult=1.0):
        """Simulate entry for top candidates

        FIX v3: Use ATR-based stops, add gap risk buffer,
        add liquidity check (skip stocks with insufficient price history),
        enforce sector concentration limits (max 3 per sector).
        Phase 14: regime_mult scales position count based on market regime.
            1.0 = normal, 0.75 = caution, 0.3 = stress, 0.0 = crisis/black_swan
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        # Phase 14 R2: Skip all entries when regime says zero positions
        if regime_mult <= 0.0:
            return []

        new_trades = []
        price_history = self.load_price_history()

        # FIX v3: Sector concentration tracking
        sector_counts = {}  # sector -> count of positions
        max_per_sector = self.config.get("max_positions_per_sector", 3)

        # Phase 10 R8: Holiday-aware position sizing + post-holiday volatility
        holiday_risk_factor = self._get_holiday_risk_factor(date_str)
        is_post_holiday, post_holiday_gap = self._is_post_holiday(date_str)

        # Phase 14 R2: Scale max_positions by regime multiplier.
        # Don't force min(1,...) — let the math work so stress/crisis actually reduce positions.
        effective_max = int(self.config["max_positions"] * regime_mult)
        if effective_max <= 0:
            return []  # No new entries in this regime

        # Only take top candidates up to effective_max positions
        for candidate in candidates[:effective_max]:
            code = candidate["code"]
            name = candidate["name"]

            # Phase 14 R2: Use adj_close consistently for P&L alignment with live pipeline
            entry_price = candidate.get("adj_close") or candidate.get("close", 0)

            # FIX: Stage 2 candidates may not have 'close' field
            # Look up from price history if missing
            if entry_price == 0 and code in price_history:
                for day in price_history[code]:
                    if day.get("date") == date_str:
                        entry_price = day.get("adj_close", day.get("close", 0))
                        break
                # If date not found, use latest available
                if entry_price == 0 and price_history[code]:
                    entry_price = price_history[code][-1].get("adj_close", price_history[code][-1].get("close", 0))

            if entry_price == 0:
                continue

            # FIX v3: Check sector concentration
            sector = self._get_sector(code)
            current_sector_count = sector_counts.get(sector, 0)

            if current_sector_count >= max_per_sector:
                # Skip this candidate - sector already has max positions
                continue

            # FIX v2: ATR-based stop loss
            is_post_holiday, post_holiday_gap = self._is_post_holiday(date_str)
            atr_mult = self.config.get("atr_stop_mult", 2.5)
            # Phase 10 R8: Widen stops by 30% on first trading day after long gap (post-holiday volatility spike)
            # Phase 17: Normal weekend gap = 3 days (Fri→Mon), so only widen for
            # gaps > 3 days (i.e., 4+ day gaps like LNY, national holidays)
            if is_post_holiday and post_holiday_gap >= 4:
                atr_mult *= 1.3
            stop_loss, atr_value = self.get_atr_stop(
                code, entry_price, price_history,
                override_atr_mult=atr_mult if is_post_holiday else None
            )

            # Calculate take profit based on risk
            risk_per_share = entry_price - stop_loss
            take_profit = entry_price + risk_per_share * self.config["take_profit_rr"]

            trade = {
                "trade_id": f"{code}_{date_str}",
                "code": code,
                "name": name,
                "sector": sector,
                "entry_date": date_str,
                "entry_price": entry_price,
                "stop_loss": round(stop_loss, 2),
                "take_profit": round(take_profit, 2),
                "risk_per_share": round(risk_per_share, 2),
                "reward_per_share": round(take_profit - entry_price, 2),
                "rr_ratio": self.config["take_profit_rr"],
                "atr_value": atr_value,
                "stop_type": "atr" if atr_value else "fixed_pct",
                # Phase 10 R8: Holiday-aware metadata
                "holiday_risk_factor": round(holiday_risk_factor, 2),
                "is_post_holiday": is_post_holiday,
                "post_holiday_gap_days": post_holiday_gap,
                "holding_days": 0,
                "max_drawdown": 0,
                "status": "open",
                "exit_date": None,
                "exit_price": None,
                "exit_reason": None,
                "pnl_pct": None,
                "commission": round(entry_price * self._get_transaction_cost(code), 2),
                "combined_score": candidate.get("combined_score", 0),
                "stage1_score": candidate.get("stage1_score", 0),
                "stage2_score": candidate.get("stage2_score", 0),
                # Phase 14: Regime-aware metadata
                "regime_mult": round(regime_mult, 2),
            }

            new_trades.append(trade)
            sector_counts[sector] = current_sector_count + 1

        self.trades.extend(new_trades)
        self.active_positions = [t for t in self.trades if t["status"] == "open"]

        return new_trades

    def _is_tpex(self, stock_code):
        """Determine if a stock trades on TPEx (上櫃).

        TPEx stocks: codes >= 9900, or 6000-6999, or 7000-7999 range.
        TWSE stocks: 1xxx, 2xxx, 3xxx, 5xxx, 8xxx, etc.

        Note: 7xxx range added per Z.ai review - many TPEx stocks exist there.
        9xxx includes 興櫃 stocks but they're filtered out by market cap thresholds.
        """
        try:
            code_int = int(stock_code)
            return code_int >= 9900 or (6000 <= code_int <= 7999)
        except (ValueError, TypeError):
            return False

    def _get_transaction_cost(self, stock_code):
        """Get round-trip transaction cost based on market (TWSE vs TPEx).

        TWSE: ~0.6% round-trip (0.3% 證交稅 sell + ~0.14% brokerage comm each way)
        TPEx: ~0.7% round-trip (same explicit costs + buffer for wider bid-ask spreads
               and lower liquidity on TPEx-listed stocks)

        Note: Settlement fees (集保結算所) are included in brokerage commission,
        not charged separately. Minimum NT$20 commission not modeled (assumes
        position sizes well above minimum threshold).
        """
        if self._is_tpex(stock_code):
            return self.config.get("tpex_roundtrip_cost", 0.007)
        return self.config.get("twse_roundtrip_cost", 0.006)

    def _get_sector(self, stock_code):
        """Get sector for a stock code.

        FIX v3: Use TWSE industry codes from company data via sectors module.
        Falls back to 'other' if sector data not available.
        """
        # Import sectors module
        try:
            from sectors import get_sector, load_sector_mapping
            # Load sector mapping once and cache it
            if not hasattr(self, '_sector_map'):
                self._sector_map = load_sector_mapping(str(self.data_dir))
            return get_sector(stock_code, self._sector_map)
        except ImportError:
            return "other"
        except:
            return "unknown"

    # ------------------------------------------------------------------ #
    #  Phase 10 R8: Holiday-aware helpers
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    #  Phase 14: Regime-aware position sizing + backtest alignment
    # ------------------------------------------------------------------ #

    def _get_regime(self, price_history, date_str=None):
        """Detect market regime as of a specific date.

        CRITICAL: Only uses price data UP TO date_str to avoid look-ahead bias.
        Returns (regime_name, position_mult) tuple. Falls back to ('normal', 1.0)
        if regime detection is unavailable or fails.
        """
        if not self._regime_detector_available:
            return 'normal', 1.0

        try:
            # Parse cutoff date
            cutoff = datetime.strptime(date_str, "%Y-%m-%d").date()

            # ── CRITICAL FIX: Filter to only data on or before date_str ──
            filtered_prices = {}
            min_samples_needed = 20

            for stock_id, entries in price_history.items():
                if not isinstance(entries, list):
                    continue

                past_entries = []
                for entry in entries:
                    try:
                        entry_date = datetime.strptime(entry.get("date", ""), "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        continue
                    if entry_date <= cutoff:
                        past_entries.append(entry)

                if len(past_entries) >= min_samples_needed:
                    filtered_prices[stock_id] = past_entries

            if not filtered_prices:
                return 'normal', 1.0

            # Phase 17: Call detect_regime_from_prices with pre-filtered data.
            # This function accepts the price dict directly, unlike detect_regime()
            # which expects (date_str, verbose) and loads its own data.
            # Also load previous regime for transition logic if available.
            prev_regime_data = None
            regime_file = self.data_dir / "regime.json"
            if regime_file.exists():
                try:
                    with open(regime_file, 'r', encoding='utf-8') as f:
                        prev_regime_data = json.load(f)
                except (json.JSONDecodeError, IOError):
                    pass

            regime_name = self._detect_regime_from_prices(
                filtered_prices,
                prev_regime_data=prev_regime_data,
            )
            position_mult = self._get_regime_position_mult(regime_name)
            return regime_name, position_mult
        except Exception as e:
            logger.debug(f"Regime detection failed: {e}")
            return 'normal', 1.0

    def _get_regime_position_mult(self, regime):
        """Get position size multiplier based on market regime.

        ALIGNED WITH PHASE 3 (regime_detector.py + weights.json v4.0).
        Read from config first, fallback to hardcoded defaults that match Phase 3.

        NORMAL:   1.0x (full sizing)
        CAUTION:  0.6x (reduce 40%)
        STRESS:   0.3x (reduce 70%)
        CRISIS:   0.1x (90% reduction, keep minimal exposure)
        BLACK_SWAN: 0.0x (exit everything)
        """
        # Try config first for single source of truth
        mult = self.config.get("regime_position_multipliers", {}).get(regime.lower())
        if mult is not None:
            return float(mult)

        # Defaults MUST match Phase 3 regime_detector.py
        # Phase 22: Unknown regime defaults to 0.0 (no new positions) for safety.
        # Previously defaulted to 1.0 which could cause unwanted trading on data failures.
        DEFAULTS = {
            'normal': 1.0,
            'caution': 0.6,
            'stress': 0.3,
            'crisis': 0.1,
            'black_swan': 0.0,
        }
        return DEFAULTS.get(regime.lower(), 0.0)

    # ------------------------------------------------------------------ #
    #  Phase 10 R8: Holiday-aware helpers
    # ------------------------------------------------------------------ #

    def _is_post_holiday(self, date_str):
        """Check if the given date is a post-holiday trading day (first trading day after a gap).

        Phase 10 R8: Post-holiday days have wider volatility spikes.
        Phase 17: Fixed off-by-one — use proper calendar-day gap calculation
        instead of the broken 'days_back + 1 - 1' which was a no-op.

        Returns (is_post_holiday: bool, gap_days: int) tuple.
        gap_days = calendar days between date_str and the previous trading day.
        Normal weekend (Mon after Fri): gap_days = 3 (Fri→Sat→Sun→Mon)
        Extended LNY (Wed after prev Wed): gap_days = 7
        Only flagged as post-holiday when gap > 3 (extended break).
        """
        if not self.holiday_calendar:
            return False, 0

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return False, 0

        # Walk backward to find the previous trading day
        prev_date = dt - timedelta(days=1)
        steps = 0
        while steps < 20:  # Safety limit
            iso = prev_date.strftime("%Y-%m-%d")
            if self.holiday_calendar.is_trading_day(iso):
                # Phase 17: Direct calendar-day gap between the two dates
                gap_days = (dt - prev_date).days
                return gap_days > 3, max(0, gap_days)
            prev_date -= timedelta(days=1)
            steps += 1

        return False, 0

    def _get_holiday_risk_factor(self, date_str):
        """Get position size adjustment factor based on upcoming holiday gaps.

        Phase 10 R8: Reduce position sizes when holding over long gaps.
        Returns a multiplier (0.5-1.0) applied to risk_per_trade.
        """
        if not self.holiday_calendar:
            return 1.0

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return 1.0

        # Look ahead up to max_holding_days for upcoming gaps
        look_ahead = self.config.get("max_holding_days", 20)
        end_date = (dt + timedelta(days=look_ahead)).strftime("%Y-%m-%d")
        gaps = self.holiday_calendar.get_holiday_gaps(date_str, end_date)

        if not gaps:
            return 1.0

        # Find the most impactful gap during holding period
        worst_risk = "low"
        for gap in gaps:
            risk = gap.get("risk_level", "low")
            length = gap.get("length", 0)
            if risk == "high" or length >= 8:
                return 0.5   # LNY-level: reduce by 50%
            elif risk == "medium" or length >= 4:
                worst_risk = "medium"

        if worst_risk == "medium":
            return 0.75  # Reduce by 25%

        return 1.0

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
        # Use market-specific round-trip cost (TWSE 0.6% / TPEx 0.7%)
        roundtrip_cost = self._get_transaction_cost(trade["code"])
        trade["pnl_pct"] = round((gross_pnl - roundtrip_cost) * 100, 2)
        trade["status"] = "closed"
        trade["exit_date"] = current_date
        trade["holding_days"] = self._calc_holding_days(trade["entry_date"], current_date)

        return trade

    def _calc_holding_days(self, entry_date, exit_date):
        """Calculate holding days (Phase 16: trading days only, not calendar days).

        During LNY gaps (5-8 consecutive non-trading weekdays), counting calendar
days would inflate holding period and trigger premature max_holding_days exits.
        E.g., a position held across LNY could show 12 calendar days but only
        ~6 actual trading days.
        """
        try:
            if self.holiday_calendar:
                return self.holiday_calendar.count_trading_days_in_range(entry_date, exit_date)
            # Fallback: calendar days (less accurate but safe)
            entry = datetime.strptime(entry_date, "%Y-%m-%d")
            exit_dt = datetime.strptime(exit_date, "%Y-%m-%d")
            return (exit_dt - entry).days
        except Exception:
            return 0

    # ------------------------------------------------------------------ #
    #  Phase 26: Multi-period validation
    # ------------------------------------------------------------------ #

    def run_multi_period_backtest(self, periods=3, lookback_days=60):
        """Run forward tests across multiple time periods for consistency validation.

        Phase 26: Splits the available backtest date range into N contiguous
        periods and runs a separate forward test on each. This reveals whether
        the strategy works consistently across different market conditions or
        just happened to catch one lucky stretch.

        Args:
            periods: Number of periods to split the data into (default 3).
                     Period 0 = most recent, Period N-1 = oldest.
            lookback_days: Total lookback window (passed to run_backtest).

        Returns:
            Dict with per-period results, overall average, and consistency score.
        """
        stage2_files = sorted(self.data_dir.glob("stage2_*.json"))

        if not stage2_files:
            return {"error": "No historical Stage 2 data found"}

        files_window = stage2_files[-lookback_days:]
        n_files = len(files_window)

        if n_files < periods:
            logger.warning(
                "Not enough data files (%d) for %d periods; using %d period(s)",
                n_files, periods, n_files,
            )
            periods = max(1, n_files)

        chunk_size = n_files // periods
        period_results = []

        for p in range(periods):
            start_idx = p * chunk_size
            end_idx = (p + 1) * chunk_size if p < periods - 1 else n_files
            period_files = files_window[start_idx:end_idx]

            if not period_files:
                continue

            start_date = period_files[0].stem.replace("stage2_", "")
            end_date = period_files[-1].stem.replace("stage2_", "")

            if p == 0:
                label = f"recent ({start_date} to {end_date})"
            elif p == periods - 1:
                label = f"oldest ({start_date} to {end_date})"
            else:
                label = f"mid ({start_date} to {end_date})"

            result = self._run_backtest_on_files(period_files)
            result["label"] = label
            result["start_date"] = start_date
            result["end_date"] = end_date
            period_results.append(result)

        # Compute overall average (weighted by trade count)
        total_trades = sum(r.get("total_trades", 0) for r in period_results)
        if total_trades > 0:
            overall_avg = {
                "total_trades": total_trades,
                "win_rate": round(
                    sum(r.get("win_rate", 0) * r.get("total_trades", 0)
                        for r in period_results) / total_trades, 1),
                "avg_pnl_pct": round(
                    sum(r.get("avg_pnl_pct", 0) * r.get("total_trades", 0)
                        for r in period_results) / total_trades, 2),
                "avg_holding_days": round(
                    sum(r.get("avg_holding_days", 0) * r.get("total_trades", 0)
                        for r in period_results) / total_trades, 1),
            }
        else:
            overall_avg = {"total_trades": 0, "win_rate": 0, "avg_pnl_pct": 0, "avg_holding_days": 0}

        # Consistency analysis
        win_rates = [r.get("win_rate", 0) for r in period_results if r.get("total_trades", 0) > 0]
        pnls = [r.get("avg_pnl_pct", 0) for r in period_results if r.get("total_trades", 0) > 0]

        consistency = {}
        if len(win_rates) >= 2:
            wr_mean = sum(win_rates) / len(win_rates)
            wr_std = math.sqrt(sum((x - wr_mean) ** 2 for x in win_rates) / len(win_rates))
            pnl_mean = sum(pnls) / len(pnls)
            pnl_std = math.sqrt(sum((x - pnl_mean) ** 2 for x in pnls) / len(pnls))
            all_profitable = all(p > 0 for p in pnls)

            wr_penalty = min(1.0, wr_std / 20.0)
            pnl_penalty = min(1.0, pnl_std / 5.0)
            score = round(max(0, 100 * (1.0 - 0.5 * wr_penalty - 0.5 * pnl_penalty)
                              * (1.2 if all_profitable else 0.6)), 1)
            score = min(100, score)

            consistency = {
                "win_rate_std": round(wr_std, 1),
                "avg_pnl_std": round(pnl_std, 2),
                "all_periods_profitable": all_profitable,
                "score": score,
            }
        else:
            consistency = {
                "win_rate_std": 0, "avg_pnl_std": 0,
                "all_periods_profitable": len(pnls) > 0 and all(p > 0 for p in pnls),
                "score": 0,
                "note": "Not enough periods with trades for consistency analysis",
            }

        return {"periods": period_results, "overall_average": overall_avg, "consistency": consistency}

    def _run_backtest_on_files(self, file_list):
        """Core backtest loop operating on a specific list of stage2 files.

        Phase 26: Extracted from run_backtest so that multi-period validation
        can call it on subsets of files. Same logic as run_backtest but takes
        an explicit file list instead of using glob + lookback_days.
        """
        results = {
            "total_trades": 0, "winning_trades": 0, "losing_trades": 0,
            "total_pnl_pct": 0, "avg_pnl_pct": 0, "max_drawdown": 0,
            "avg_holding_days": 0, "trades": [], "cumulative_pnl": [],
        }

        prices = self.load_price_history()
        cumulative = 0
        peak = 0
        max_dd = 0

        for filepath in file_list:
            date_str = filepath.stem.replace("stage2_", "")

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning("Failed to load %s: %s", filepath, e)
                continue

            candidates = data.get("candidates", [])
            if not candidates:
                continue

            if not hasattr(self, '_regime_cache'):
                self._regime_cache = {}

            if date_str not in self._regime_cache:
                regime, regime_mult = self._get_regime(prices, date_str)
                self._regime_cache[date_str] = (regime, regime_mult)
            else:
                regime, regime_mult = self._regime_cache[date_str]

            trades = self.simulate_entry(candidates, date_str, regime_mult=regime_mult)

            for trade in trades:
                code = trade["code"]
                exit_price, exit_date, exit_reason = self._simulate_trade_exit(
                    trade, code, date_str, prices
                )

                if exit_price is not None and exit_price > 0:
                    gross_pnl = (exit_price - trade["entry_price"]) / trade["entry_price"]
                    roundtrip_cost = self._get_transaction_cost(code)
                    net_pnl = (gross_pnl - roundtrip_cost) * 100

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

        if results["total_trades"] > 0:
            results["win_rate"] = round(results["winning_trades"] / results["total_trades"] * 100, 1)
            results["avg_pnl_pct"] = round(results["total_pnl_pct"] / results["total_trades"], 2)
            results["avg_holding_days"] = round(
                sum(t["holding_days"] for t in results["trades"]) / results["total_trades"], 1)
            results["max_drawdown"] = round(max_dd, 2)

        return results

    def _simulate_trade_exit(self, trade, code, date_str, prices):
        """Simulate the exit for a single trade given price history.

        Phase 26: Extracted from run_backtest for reuse in multi-period validation.
        Returns (exit_price, exit_date, exit_reason) tuple.
        """
        exit_price = None
        exit_date = None
        exit_reason = None

        if code not in prices or len(prices[code]) < 2:
            return exit_price, exit_date, exit_reason

        history = prices[code]
        entry_idx = None

        for i, day in enumerate(history):
            if day["date"] == date_str:
                entry_idx = i
                break

        if entry_idx is None:
            best_idx = None
            for i, day in enumerate(history):
                if day["date"] <= date_str:
                    best_idx = i
                else:
                    break
            entry_idx = best_idx

        if entry_idx is None:
            return exit_price, exit_date, exit_reason

        max_trade_days = self.config["max_holding_days"]
        trading_days_counted = 0

        for j in range(entry_idx + 1, len(history)):
            day = history[j]
            day_date = day["date"]

            if self.holiday_calendar and not self.holiday_calendar.is_trading_day(day_date):
                logger.debug(
                    f"Backtest: {code} skipping non-trading day {day_date} "
                    f"(likely typhoon closure)"
                )
                continue

            trading_days_counted += 1
            price = day.get("adj_close") or day.get("close") or 0

            is_ex_div = False
            if self.corp_handler:
                is_ex_div = self.corp_handler.is_ex_dividend_date(code, day_date)

            if is_ex_div:
                adj_price = self.corp_handler.adjust_price_for_dividend(price, code, day_date)
                if adj_price <= trade["stop_loss"]:
                    return trade["stop_loss"], day_date, "stop_loss"
                elif adj_price >= trade["take_profit"]:
                    return trade["take_profit"], day_date, "take_profit"
                continue

            if price <= trade["stop_loss"]:
                return trade["stop_loss"], day_date, "stop_loss"
            elif price >= trade["take_profit"]:
                return trade["take_profit"], day_date, "take_profit"

            if trading_days_counted >= max_trade_days:
                return price, day_date, "max_holding_days"

        # No exit triggered — use last available price
        last_day_idx = entry_idx + self.config["max_holding_days"]
        if self.holiday_calendar and last_day_idx >= len(history):
            last_day_idx = len(history) - 1
        elif not self.holiday_calendar:
            last_day_idx = min(last_day_idx, len(history) - 1)

        last_day = history[last_day_idx]
        exit_price = last_day.get("adj_close") or last_day.get("close") or 0
        return exit_price, last_day["date"], "max_holding_days"

    def run_backtest(self, lookback_days=60):
        """Run backtest using historical signals.

        FIX v2: Use proper exit simulation with ATR stops,
        include commission costs, and track drawdown properly.

        Phase 26: Refactored inner loop into _simulate_trade_exit() and
        _run_backtest_on_files() for reuse in multi-period validation.
        This method remains backward compatible — same signature, same output.
        """
        stage2_files = sorted(self.data_dir.glob("stage2_*.json"))

        if not stage2_files:
            return {"error": "No historical Stage 2 data found"}

        files_window = stage2_files[-lookback_days:]
        results = self._run_backtest_on_files(files_window)

        # Phase 4: Survivorship bias correction (Tier 1)
        survivorship_bias = {
            "tier": "1 - Conservative haircut",
            "return_adjustment": -0.12,
            "win_rate_adjustment": -0.08,
            "reason": "Backtest uses only currently-listed stocks, missing delisted failures",
            "tier2_pending": "Scrape delisted stock data from MOF/MOPS historical archives",
        }

        if results["total_trades"] > 0:
            raw_return = results["avg_pnl_pct"]
            raw_win_rate = results["win_rate"]
            results["avg_pnl_pct_adj"] = round(raw_return * (1 + survivorship_bias["return_adjustment"]), 2)
            results["win_rate_adj"] = round(
                max(0, raw_win_rate * (1 + survivorship_bias["win_rate_adjustment"])), 1
            )
            results["total_pnl_pct_adj"] = round(
                results["total_pnl_pct"] * (1 + survivorship_bias["return_adjustment"]), 2
            )

        results["survivorship_bias"] = survivorship_bias

        return results

    # ------------------------------------------------------------------ #
    #  Phase 26: Sector-adjusted returns
    # ------------------------------------------------------------------ #

    def compute_sector_adjusted_returns(self, results=None):
        """Compute sector-adjusted returns (alpha) for each trade and portfolio.

        Phase 26: Compares each stock's return against its sector benchmark
        (equal-weight average of all trades in the same sector within the same
        backtest period). Reports alpha = stock_return - sector_return for each
        holding, and a portfolio-level sector-adjusted Sharpe ratio.

        The sector benchmark is computed as the simple average P&L% of all
        closed trades in the same sector. This serves as a proxy for the
        TWSE sub-sector index return — using actual index data would be more
        accurate but requires data that is not currently available in the
        pipeline.

        Args:
            results: Output from run_backtest(). If None, runs a fresh backtest
                     with default lookback_days=60.

        Returns:
            The same results dict with additional fields:
            - Each trade gets "sector_alpha" and "sector_benchmark_return"
            - results gets "sector_adjusted" dict with portfolio-level metrics
        """
        if results is None:
            results = self.run_backtest()

        if results.get("total_trades", 0) == 0:
            results["sector_adjusted"] = {
                "sector_benchmarks": {},
                "portfolio_alpha": 0,
                "sector_adjusted_sharpe": 0,
                "note": "No trades to analyze",
            }
            return results

        # Build sector benchmark: equal-weight average P&L per sector
        sector_pnls = {}  # sector -> [pnl_pct, ...]
        for trade in results.get("trades", []):
            sector = trade.get("sector", "other")
            pnl = trade.get("pnl_pct", 0)
            if pnl is not None:
                sector_pnls.setdefault(sector, []).append(pnl)

        sector_benchmarks = {}
        for sector, pnls in sector_pnls.items():
            sector_benchmarks[sector] = round(sum(pnls) / len(pnls), 2)

        # Compute alpha per trade
        alphas = []
        for trade in results.get("trades", []):
            sector = trade.get("sector", "other")
            benchmark = sector_benchmarks.get(sector, 0)
            pnl = trade.get("pnl_pct") or 0
            alpha = round(pnl - benchmark, 2)
            trade["sector_alpha"] = alpha
            trade["sector_benchmark_return"] = benchmark
            alphas.append(alpha)

        # Portfolio-level alpha (average of all trade alphas)
        portfolio_alpha = round(sum(alphas) / len(alphas), 2) if alphas else 0

        # Sector-adjusted Sharpe ratio: mean(alpha) / std(alpha)
        # Annualized assuming ~252 trading days, ~5 trades per rebalance cycle
        # (conservative annualization factor: sqrt(50) for ~50 round-trips/year)
        if len(alphas) >= 2:
            mean_alpha = sum(alphas) / len(alphas)
            std_alpha = math.sqrt(sum((a - mean_alpha) ** 2 for a in alphas) / len(alphas))
            if std_alpha > 0:
                sharpe = (mean_alpha / std_alpha) * math.sqrt(50)
                sector_adjusted_sharpe = round(sharpe, 2)
            else:
                sector_adjusted_sharpe = 0
        else:
            sector_adjusted_sharpe = 0

        results["sector_adjusted"] = {
            "sector_benchmarks": sector_benchmarks,
            "portfolio_alpha": portfolio_alpha,
            "sector_adjusted_sharpe": sector_adjusted_sharpe,
        }

        return results

    # ------------------------------------------------------------------ #
    #  Phase 26: Drawdown analysis
    # ------------------------------------------------------------------ #

    def compute_drawdown_analysis(self, results=None):
        """Compute detailed drawdown analysis from backtest results.

        Phase 26: Tracks and reports maximum drawdown, average drawdown
        duration, and recovery time from peak to new peak. Adds these
        metrics to the backtest summary output alongside existing
        win_rate/avg_pnl_pct.

        A drawdown period is defined as: peak -> trough -> recovery (new peak).
        Duration = trading sessions from peak to trough.
        Recovery = trading sessions from trough to new peak.

        Args:
            results: Output from run_backtest(). If None, runs a fresh backtest
                     with default lookback_days=60.

        Returns:
            The same results dict with an added "drawdown_analysis" field
            containing max_drawdown_pct, max/avg drawdown duration,
            max/avg recovery time, drawdown_periods list, and underwater stats.
        """
        if results is None:
            results = self.run_backtest()

        cumulative_pnl = results.get("cumulative_pnl", [])

        if not cumulative_pnl or len(cumulative_pnl) < 2:
            results["drawdown_analysis"] = {
                "max_drawdown_pct": 0,
                "max_drawdown_duration": 0,
                "avg_drawdown_duration": 0,
                "max_recovery_time": 0,
                "avg_recovery_time": 0,
                "drawdown_periods": [],
                "underwater_count": 0,
                "underwater_pct": 0,
                "note": "Insufficient data for drawdown analysis",
            }
            return results

        # Track equity curve and find drawdown periods
        # A drawdown period starts when cumulative P&L drops below a previous
        # peak and ends when it recovers to that peak level (or higher).
        drawdown_periods = []
        peak_idx = 0
        peak_value = cumulative_pnl[0]
        in_drawdown = False
        dd_start_idx = None
        trough_idx = None
        trough_value = 0

        for i in range(1, len(cumulative_pnl)):
            val = cumulative_pnl[i]

            if val >= peak_value:
                # New peak reached
                if in_drawdown:
                    dd_pct = round(peak_value - trough_value, 2)
                    duration = trough_idx - dd_start_idx
                    recovery_time = i - trough_idx
                    drawdown_periods.append({
                        "peak_idx": dd_start_idx,
                        "trough_idx": trough_idx,
                        "recovery_idx": i,
                        "drawdown_pct": dd_pct,
                        "duration": duration,
                        "recovery_time": recovery_time,
                    })
                    in_drawdown = False

                peak_value = val
                peak_idx = i
            else:
                # Below peak — in a drawdown
                if not in_drawdown:
                    in_drawdown = True
                    dd_start_idx = peak_idx
                    trough_idx = i
                    trough_value = val
                elif val < trough_value:
                    trough_idx = i
                    trough_value = val

        # Handle unclosed drawdown (still in drawdown at end of data)
        if in_drawdown:
            dd_pct = round(peak_value - trough_value, 2)
            duration = trough_idx - dd_start_idx
            drawdown_periods.append({
                "peak_idx": dd_start_idx,
                "trough_idx": trough_idx,
                "recovery_idx": None,  # No recovery yet
                "drawdown_pct": dd_pct,
                "duration": duration,
                "recovery_time": None,
            })

        # Compute summary statistics
        if drawdown_periods:
            max_dd = max(drawdown_periods, key=lambda d: d["drawdown_pct"])
            durations = [d["duration"] for d in drawdown_periods if d["duration"] > 0]
            recoveries = [d["recovery_time"] for d in drawdown_periods
                          if d["recovery_time"] is not None]

            # Count underwater sessions: sessions where cumulative P&L < previous peak
            underwater = 0
            running_peak = cumulative_pnl[0]
            for val in cumulative_pnl[1:]:
                if val < running_peak:
                    underwater += 1
                else:
                    running_peak = val

            results["drawdown_analysis"] = {
                "max_drawdown_pct": max_dd["drawdown_pct"],
                "max_drawdown_duration": max_dd["duration"],
                "avg_drawdown_duration": round(
                    sum(durations) / len(durations), 1) if durations else 0,
                "max_recovery_time": max(recoveries) if recoveries else 0,
                "avg_recovery_time": round(
                    sum(recoveries) / len(recoveries), 1) if recoveries else 0,
                "drawdown_periods": drawdown_periods,
                "underwater_count": underwater,
                "underwater_pct": round(
                    underwater / max(1, len(cumulative_pnl) - 1) * 100, 1),
            }
        else:
            # No drawdown periods found (monotonically increasing equity)
            results["drawdown_analysis"] = {
                "max_drawdown_pct": 0,
                "max_drawdown_duration": 0,
                "avg_drawdown_duration": 0,
                "max_recovery_time": 0,
                "avg_recovery_time": 0,
                "drawdown_periods": [],
                "underwater_count": 0,
                "underwater_pct": 0,
            }

        return results

    # ------------------------------------------------------------------ #
    #  Phase 28: Portfolio Rebalancing Engine
    # ------------------------------------------------------------------ #

    def optimize_positions(self, candidates, portfolio_value=None,
                           date_str=None, regime_mult=1.0):
        """Optimize position sizes based on signal strength and sector constraints.

        Phase 28: Instead of equal-weight positions, size each position based on
        signal strength (from Phase 27's compute_signal_strength). Higher
        conviction signals get larger allocations using Kelly-inspired sizing:
            position_size = base_size * (signal_strength / 50)
        Capped at 3x base_size and floored at 0.5x base_size.

        Also enforces sector diversification: max 15% of portfolio per sector
        to avoid concentration risk.

        Args:
            candidates: List of candidate dicts (from Stage 2). Each should
                        have a "signal_strength" field (from Phase 27) or a
                        "combined_score" as fallback.
            portfolio_value: Total portfolio value. If None, uses 1,000,000 TWD
                            as default paper trading capital.
            date_str: Date string (YYYY-MM-DD). Defaults to today.
            regime_mult: Regime multiplier for position scaling (from Phase 14).

        Returns:
            Dict with:
                allocations: list of {code, name, sector, allocation_pct,
                            allocation_twd, size_mult, signal_strength}
                sector_allocations: dict of {sector: total_pct}
                total_allocated_pct: float
                unallocated_pct: float
                warnings: list of str
        """
        if portfolio_value is None:
            portfolio_value = 1_000_000  # Default paper trading capital (TWD)

        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        # Phase 14 R2: Skip all entries when regime says zero positions
        if regime_mult <= 0.0:
            return {
                "allocations": [],
                "sector_allocations": {},
                "total_allocated_pct": 0,
                "unallocated_pct": 100,
                "warnings": ["Regime multiplier is zero — no positions allocated"],
            }

        max_positions = self.config.get("max_positions", 5)
        max_sector_pct = self.config.get("max_sector_portfolio_pct", 0.15)
        risk_per_trade = self.config.get("risk_per_trade", 0.015)

        # Effective max positions scaled by regime
        effective_max = int(max_positions * regime_mult)
        if effective_max <= 0:
            return {
                "allocations": [],
                "sector_allocations": {},
                "total_allocated_pct": 0,
                "unallocated_pct": 100,
                "warnings": ["Effective max positions is zero after regime scaling"],
            }

        candidates = candidates[:effective_max]

        # Step 1: Compute Kelly-inspired size multiplier for each candidate
        raw_allocations = []
        for candidate in candidates:
            code = candidate.get("code", "")
            name = candidate.get("name", "")

            # Extract signal strength: prefer Phase 27's signal_strength dict,
            # fall back to combined_score (0-100 range)
            signal_info = candidate.get("signal_strength", None)
            if isinstance(signal_info, dict) and "strength" in signal_info:
                signal_str = signal_info["strength"]
            else:
                signal_str = candidate.get("combined_score", 50)

            # Clamp signal strength to valid range
            signal_str = max(0, min(100, float(signal_str)))

            # Kelly-inspired sizing: base_size * (signal_strength / 50)
            # At signal_strength=50 (moderate), size_mult = 1.0x (base)
            # At signal_strength=100 (very high), size_mult = 2.0x
            # Floor at 0.5x (signal_strength=25) and cap at 3.0x (signal_strength=150)
            size_mult = signal_str / 50.0
            size_mult = max(0.5, min(3.0, size_mult))

            # Base allocation: equal weight adjusted by risk_per_trade
            # base_size = risk_per_trade * portfolio_value per position
            base_alloc_pct = risk_per_trade * 100  # e.g., 1.5% per trade
            alloc_pct = base_alloc_pct * size_mult

            sector = self._get_sector(code)

            raw_allocations.append({
                "code": code,
                "name": name,
                "sector": sector,
                "signal_strength": round(signal_str, 1),
                "size_mult": round(size_mult, 2),
                "raw_alloc_pct": round(alloc_pct, 2),
            })

        # Step 2: Enforce sector diversification — max 15% per sector
        sector_running = {}  # sector -> total pct allocated
        warnings = []
        final_allocations = []

        for alloc in raw_allocations:
            sector = alloc["sector"]
            current_sector_pct = sector_running.get(sector, 0.0)

            # Check if adding this position would exceed sector limit
            proposed_pct = current_sector_pct + alloc["raw_alloc_pct"]

            if proposed_pct > max_sector_pct * 100:
                # Reduce allocation to fit within sector limit
                available_pct = max(0, max_sector_pct * 100 - current_sector_pct)

                if available_pct <= 0:
                    # Sector already at limit — skip this position
                    warnings.append(
                        f"Sector '{sector}' at {current_sector_pct:.1f}% limit, "
                        f"skipping {alloc['code']} {alloc['name']}"
                    )
                    continue

                # Scale down allocation to fit
                original_pct = alloc["raw_alloc_pct"]
                alloc["raw_alloc_pct"] = round(available_pct, 2)
                alloc["size_mult"] = round(
                    alloc["size_mult"] * (available_pct / original_pct), 2
                ) if original_pct > 0 else alloc["size_mult"]
                warnings.append(
                    f"Sector '{sector}' limit: reduced {alloc['code']} "
                    f"from {original_pct:.1f}% to {available_pct:.1f}%"
                )

            # Apply allocation
            final_pct = alloc["raw_alloc_pct"]
            sector_running[sector] = sector_running.get(sector, 0.0) + final_pct

            final_allocations.append({
                "code": alloc["code"],
                "name": alloc["name"],
                "sector": alloc["sector"],
                "signal_strength": alloc["signal_strength"],
                "size_mult": alloc["size_mult"],
                "allocation_pct": round(final_pct, 2),
                "allocation_twd": round(final_pct / 100 * portfolio_value, 0),
            })

        total_allocated = sum(a["allocation_pct"] for a in final_allocations)

        return {
            "allocations": final_allocations,
            "sector_allocations": {s: round(p, 2) for s, p in sector_running.items()},
            "total_allocated_pct": round(total_allocated, 2),
            "unallocated_pct": round(100 - total_allocated, 2),
            "portfolio_value": portfolio_value,
            "regime_mult": round(regime_mult, 2),
            "warnings": warnings,
        }

    def compute_sector_rotation(self, date_str=None, rolling_window=5):
        """Analyze sector rotation signals based on relative strength/weakness.

        Phase 28: Compares average signal scores across sectors over a rolling
        window (last N trading days). Generates rotation signals:
        - "overweight": sectors with improving momentum (rising average scores)
        - "underweight": sectors with declining momentum (falling average scores)
        - "neutral": sectors with stable scores

        The rotation signal is determined by comparing the current period's
        average score against the rolling window average. A sector is
        "overweight" if its current score exceeds the rolling average by more
        than one standard deviation, "underweight" if it falls below by more
        than one standard deviation, and "neutral" otherwise.

        Args:
            date_str: Reference date (YYYY-MM-DD). Defaults to today.
                      Only uses data up to this date to avoid look-ahead bias.
            rolling_window: Number of recent trading days to analyze (default 5).

        Returns:
            Dict with:
                sectors: dict of {sector: {signal, current_avg, rolling_avg,
                         momentum, score_count}}
                overweight_sectors: list of sector names
                underweight_sectors: list of sector names
                neutral_sectors: list of sector names
                rolling_window: int
                date: str
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        # Collect stage2 files for the rolling window period
        stage2_files = sorted(self.data_dir.glob("stage2_*.json"))
        if not stage2_files:
            return {
                "sectors": {},
                "overweight_sectors": [],
                "underweight_sectors": [],
                "neutral_sectors": [],
                "rolling_window": rolling_window,
                "date": date_str,
                "note": "No historical Stage 2 data found",
            }

        # Filter files up to date_str and take the rolling window
        cutoff = date_str
        valid_files = [f for f in stage2_files
                       if f.stem.replace("stage2_", "") <= cutoff]

        if not valid_files:
            return {
                "sectors": {},
                "overweight_sectors": [],
                "underweight_sectors": [],
                "neutral_sectors": [],
                "rolling_window": rolling_window,
                "date": date_str,
                "note": "No data files on or before the given date",
            }

        # Take the last `rolling_window` files
        window_files = valid_files[-rolling_window:]

        # Collect per-day per-sector scores
        # Structure: {sector: [day_avg_score, ...]} — one entry per day
        sector_daily_scores = {}
        sector_stock_counts = {}

        for filepath in window_files:
            file_date = filepath.stem.replace("stage2_", "")

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError):
                continue

            candidates = data.get("candidates", [])
            if not candidates:
                continue

            # Group candidates by sector for this day
            day_sector_scores = {}  # sector -> [score, ...]
            for cand in candidates:
                code = cand.get("code", "")
                sector = self._get_sector(code)

                # Use signal_strength if available (Phase 27), else combined_score
                signal_info = cand.get("signal_strength", None)
                if isinstance(signal_info, dict) and "strength" in signal_info:
                    score = signal_info["strength"]
                else:
                    score = cand.get("combined_score", 0)

                score = float(score) if score else 0.0
                day_sector_scores.setdefault(sector, []).append(score)

            # Compute daily average per sector
            for sector, scores in day_sector_scores.items():
                avg = sum(scores) / len(scores) if scores else 0
                sector_daily_scores.setdefault(sector, []).append(avg)
                sector_stock_counts[sector] = max(
                    sector_stock_counts.get(sector, 0), len(scores)
                )

        if not sector_daily_scores:
            return {
                "sectors": {},
                "overweight_sectors": [],
                "underweight_sectors": [],
                "neutral_sectors": [],
                "rolling_window": rolling_window,
                "date": date_str,
                "note": "No sector scores found in rolling window",
            }

        # Compute rotation signals per sector
        sector_results = {}
        overweight = []
        underweight = []
        neutral = []

        for sector, daily_scores in sector_daily_scores.items():
            if not daily_scores:
                continue

            current_avg = daily_scores[-1]  # Most recent day
            rolling_avg = sum(daily_scores) / len(daily_scores)

            # Compute standard deviation for threshold
            if len(daily_scores) >= 2:
                variance = sum((s - rolling_avg) ** 2 for s in daily_scores) / len(daily_scores)
                std_dev = math.sqrt(variance)
            else:
                std_dev = 5.0  # Default threshold for single-day data

            # Momentum: current vs rolling average
            momentum = current_avg - rolling_avg

            # Determine signal based on standard deviation bands
            if std_dev > 0 and momentum > std_dev:
                signal = "overweight"
                overweight.append(sector)
            elif std_dev > 0 and momentum < -std_dev:
                signal = "underweight"
                underweight.append(sector)
            else:
                signal = "neutral"
                neutral.append(sector)

            sector_results[sector] = {
                "signal": signal,
                "current_avg": round(current_avg, 2),
                "rolling_avg": round(rolling_avg, 2),
                "momentum": round(momentum, 2),
                "std_dev": round(std_dev, 2),
                "days_in_window": len(daily_scores),
                "stock_count": sector_stock_counts.get(sector, 0),
            }

        return {
            "sectors": sector_results,
            "overweight_sectors": overweight,
            "underweight_sectors": underweight,
            "neutral_sectors": neutral,
            "rolling_window": rolling_window,
            "date": date_str,
        }

    def check_correlation_risk(self, positions=None, price_history=None,
                               lookback_days=20, correlation_threshold=0.85):
        """Check pairwise correlation between holdings and compute effective portfolio beta.

        Phase 28: Computes pairwise Pearson correlation between all holdings using
        their price history over the last `lookback_days` trading days. If any two
        holdings have correlation > `correlation_threshold` (default 0.85), the
        smaller position is reduced by 50% to limit concentration risk.

        Also reports effective portfolio beta as a risk metric. Portfolio beta is
        computed as the weighted-average beta of each holding relative to the
        equal-weight portfolio of all holdings (since a market index benchmark
        is not directly available in the paper trading system).

        Args:
            positions: List of position dicts (each must have 'code' and
                       optionally 'allocation_pct'). If None, uses
                       self.active_positions.
            price_history: Dict of {code: [{date, close, ...}, ...]}. If None,
                           loads from data directory.
            lookback_days: Number of days of price history for correlation
                           computation (default 20).
            correlation_threshold: Correlation above which positions are flagged
                                   for reduction (default 0.85).

        Returns:
            Dict with:
                pairwise_correlations: list of {pair, correlation, flagged}
                high_correlation_pairs: list of pairs with corr > threshold
                position_adjustments: list of {code, original_pct, adjusted_pct,
                                      reason}
                effective_portfolio_beta: float
                holdings_analyzed: int
                correlation_matrix: dict of {code: {code: correlation}}
        """
        if positions is None:
            positions = self.active_positions

        if price_history is None:
            price_history = self.load_price_history()

        if not positions or not price_history:
            return {
                "pairwise_correlations": [],
                "high_correlation_pairs": [],
                "position_adjustments": [],
                "effective_portfolio_beta": 0,
                "holdings_analyzed": 0,
                "correlation_matrix": {},
                "note": "No positions or price history available",
            }

        # Step 1: Extract daily returns for each holding
        holding_codes = []
        holding_returns = {}  # code -> [daily_return, ...]
        holding_allocs = {}   # code -> allocation_pct

        for pos in positions:
            code = pos.get("code", "")
            if not code or code not in price_history:
                continue

            history = price_history[code]
            if len(history) < lookback_days + 1:
                continue

            # Take last lookback_days + 1 entries to compute lookback_days returns
            recent = history[-(lookback_days + 1):]

            # Compute daily returns
            returns = []
            for i in range(1, len(recent)):
                prev = recent[i - 1].get("adj_close", recent[i - 1].get("close", 0))
                curr = recent[i].get("adj_close", recent[i].get("close", 0))
                if prev > 0:
                    returns.append((curr - prev) / prev)

            if len(returns) < 10:
                # Not enough return data for reliable correlation
                continue

            holding_codes.append(code)
            holding_returns[code] = returns
            holding_allocs[code] = pos.get("allocation_pct", 0)

        if len(holding_codes) < 2:
            return {
                "pairwise_correlations": [],
                "high_correlation_pairs": [],
                "position_adjustments": [],
                "effective_portfolio_beta": 0,
                "holdings_analyzed": len(holding_codes),
                "correlation_matrix": {},
                "note": "Fewer than 2 holdings with sufficient price data",
            }

        # Step 2: Compute pairwise correlations
        correlation_matrix = {}
        pairwise_list = []
        high_corr_pairs = []

        for i, code_a in enumerate(holding_codes):
            correlation_matrix.setdefault(code_a, {})[code_a] = 1.0

            for j in range(i + 1, len(holding_codes)):
                code_b = holding_codes[j]

                returns_a = holding_returns[code_a]
                returns_b = holding_returns[code_b]

                # Align by length
                min_len = min(len(returns_a), len(returns_b))
                if min_len < 10:
                    corr = 0.0
                else:
                    ra = returns_a[:min_len]
                    rb = returns_b[:min_len]

                    mean_a = sum(ra) / min_len
                    mean_b = sum(rb) / min_len

                    cov = sum((ra[k] - mean_a) * (rb[k] - mean_b)
                              for k in range(min_len)) / min_len
                    std_a = math.sqrt(sum((x - mean_a) ** 2 for x in ra) / min_len)
                    std_b = math.sqrt(sum((x - mean_b) ** 2 for x in rb) / min_len)

                    if std_a > 0 and std_b > 0:
                        corr = cov / (std_a * std_b)
                    else:
                        corr = 0.0

                corr = round(max(-1.0, min(1.0, corr)), 3)

                correlation_matrix.setdefault(code_a, {})[code_b] = corr
                correlation_matrix.setdefault(code_b, {})[code_a] = corr

                flagged = corr > correlation_threshold
                pair_info = {
                    "pair": f"{code_a}/{code_b}",
                    "code_a": code_a,
                    "code_b": code_b,
                    "correlation": corr,
                    "flagged": flagged,
                }
                pairwise_list.append(pair_info)

                if flagged:
                    high_corr_pairs.append(pair_info)

        # Step 3: Reduce smaller position for high-correlation pairs
        position_adjustments = []
        adjusted_allocs = dict(holding_allocs)  # Copy original allocations

        for pair in high_corr_pairs:
            code_a = pair["code_a"]
            code_b = pair["code_b"]

            alloc_a = adjusted_allocs.get(code_a, 0)
            alloc_b = adjusted_allocs.get(code_b, 0)

            # Identify the smaller position
            if alloc_a <= alloc_b:
                smaller_code, smaller_alloc = code_a, alloc_a
                larger_code = code_b
            else:
                smaller_code, smaller_alloc = code_b, alloc_b
                larger_code = code_a

            # Reduce the smaller position by 50%
            new_alloc = round(smaller_alloc * 0.5, 2)
            adjusted_allocs[smaller_code] = new_alloc

            position_adjustments.append({
                "code": smaller_code,
                "original_pct": smaller_alloc,
                "adjusted_pct": new_alloc,
                "reduction_reason": (
                    f"Correlation {pair['correlation']} with {larger_code} "
                    f"exceeds threshold {correlation_threshold}"
                ),
            })

        # Step 4: Compute effective portfolio beta
        # Beta of each holding vs equal-weight portfolio of all holdings
        # Portfolio return = equal-weight average of all holding returns
        min_len = min(len(r) for r in holding_returns.values())
        all_returns = {code: rets[:min_len] for code, rets in holding_returns.items()}

        # Compute portfolio returns (equal-weight)
        portfolio_rets = []
        for k in range(min_len):
            day_avg = sum(all_returns[code][k] for code in holding_codes) / len(holding_codes)
            portfolio_rets.append(day_avg)

        # Compute beta for each holding: Cov(stock, portfolio) / Var(portfolio)
        mean_port = sum(portfolio_rets) / min_len
        var_port = sum((r - mean_port) ** 2 for r in portfolio_rets) / min_len

        weighted_beta = 0.0
        total_weight = 0.0

        for code in holding_codes:
            returns = all_returns[code]
            mean_stock = sum(returns) / min_len

            cov = sum((returns[k] - mean_stock) * (portfolio_rets[k] - mean_port)
                      for k in range(min_len)) / min_len

            beta = cov / var_port if var_port > 0 else 1.0

            # Weight beta by allocation
            weight = adjusted_allocs.get(code, 0)
            if weight <= 0:
                # If no allocation_pct, use equal weight
                weight = 1.0 / len(holding_codes)

            weighted_beta += beta * weight
            total_weight += weight

        effective_beta = round(weighted_beta / total_weight, 2) if total_weight > 0 else 1.0

        return {
            "pairwise_correlations": pairwise_list,
            "high_correlation_pairs": high_corr_pairs,
            "position_adjustments": position_adjustments,
            "effective_portfolio_beta": effective_beta,
            "holdings_analyzed": len(holding_codes),
            "correlation_matrix": correlation_matrix,
            "lookback_days": lookback_days,
            "correlation_threshold": correlation_threshold,
        }

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
    parser.add_argument("--multi-period", dest="multi_period", type=int, default=0,
                        help="Run multi-period validation with N periods")
    parser.add_argument("--sector-adjusted", dest="sector_adjusted", action="store_true",
                        help="Include sector-adjusted returns analysis")
    parser.add_argument("--drawdown-analysis", dest="drawdown_analysis", action="store_true",
                        help="Include detailed drawdown analysis")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    trader = PaperTrader()

    if args.multi_period > 0:
        # Phase 26: Multi-period validation
        results = trader.run_multi_period_backtest(
            periods=args.multi_period,
            lookback_days=args.backtest or 60,
        )

        if args.verbose:
            print("Multi-Period Backtest Results:")
            for period in results.get("periods", []):
                label = period.get("label", "unknown")
                print(f"\n  {label}:")
                print(f"    Trades: {period.get('total_trades', 0)}")
                print(f"    Win rate: {period.get('win_rate', 0)}%")
                print(f"    Avg P&L: {period.get('avg_pnl_pct', 0)}%")

            overall = results.get("overall_average", {})
            consistency = results.get("consistency", {})
            print(f"\n  Overall average:")
            print(f"    Win rate: {overall.get('win_rate', 0)}%")
            print(f"    Avg P&L: {overall.get('avg_pnl_pct', 0)}%")
            print(f"  Consistency score: {consistency.get('score', 0)}/100")
            print(f"  All periods profitable: {consistency.get('all_periods_profitable', False)}")

    elif args.backtest > 0:
        # Run backtest
        results = trader.run_backtest(lookback_days=args.backtest)

        # Phase 26: Optional sector-adjusted enhancement
        if args.sector_adjusted:
            results = trader.compute_sector_adjusted_returns(results)

        # Phase 26: Optional drawdown analysis
        if args.drawdown_analysis:
            results = trader.compute_drawdown_analysis(results)

        if args.verbose:
            print("Backtest Results:")
            print(f"   Total trades: {results['total_trades']}")
            print(f"   Win rate: {results.get('win_rate', 0)}%")
            print(f"   Avg P&L: {results['avg_pnl_pct']}%")
            print(f"   Total P&L: {results['total_pnl_pct']}%")

            if "sector_adjusted" in results:
                sa = results["sector_adjusted"]
                print(f"\n   Sector-Adjusted Metrics:")
                print(f"     Portfolio alpha: {sa.get('portfolio_alpha', 0)}%")
                print(f"     Sector-adj Sharpe: {sa.get('sector_adjusted_sharpe', 0)}")
                for sector, bench in sa.get("sector_benchmarks", {}).items():
                    print(f"     {sector} benchmark: {bench}%")

            if "drawdown_analysis" in results:
                da = results["drawdown_analysis"]
                print(f"\n   Drawdown Analysis:")
                print(f"     Max drawdown: {da.get('max_drawdown_pct', 0)}%")
                print(f"     Max DD duration: {da.get('max_drawdown_duration', 0)} sessions")
                print(f"     Avg DD duration: {da.get('avg_drawdown_duration', 0)} sessions")
                print(f"     Max recovery time: {da.get('max_recovery_time', 0)} sessions")
                print(f"     Avg recovery time: {da.get('avg_recovery_time', 0)} sessions")
                print(f"     Underwater: {da.get('underwater_pct', 0)}% of sessions")
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
