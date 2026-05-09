# TW Stock Hunter 🦀

Quantitative stock screening pipeline for Taiwan Weighted Stock Exchange (TWSE).

## Architecture

```
core/
├── fetch_data.py       # TWSE Open API batch fetcher
├── fetch_history.py    # Historical price data (20+ days)
├── regime_detector.py  # Market regime detection (bull/choppy/bear)
├── stage1_screen.py    # Quant pre-screen + composite scoring
├── stage2_deep.py      # Fundamental deep-dive analysis
└── signal_logger.py    # Signal tracking + daily reports

config/
├── weights.json        # Scoring weights (regime-adjusted)
├── thresholds.json     # Hard filter thresholds
└── regime_rules.json   # Regime detection rules

data/                   # Fetched data + results
reports/                # Daily screening reports
```

## Pipeline

1. **Fetch** → TWSE Open API (daily data, PE, revenue, margin, dividends, etc.)
2. **History** → 25-day price history for momentum calculations
3. **Regime** → Detect bull/choppy/bear market conditions
4. **Stage 1** → Hard filters + 5-dimension composite scoring
5. **Stage 2** → Fundamental deep-dive (dividends, announcements, pledge, penalties)
6. **Report** → Generate daily report + track signals

## Scoring Dimensions

| Dimension | Weight | Data Source |
|-----------|--------|-------------|
| Revenue Momentum | 10% | TWSE revenue API |
| Profitability | 30% | P/E ratio |
| Valuation | 25% | P/E + P/B |
| Institutional Flow | 10% | Margin data proxy |
| Technical Momentum | 25% | Price history + single-day |

## Running

```bash
# Full pipeline
python3 core/fetch_data.py --verbose
python3 core/fetch_history.py --verbose --lookback 25
python3 core/regime_detector.py --verbose
python3 core/stage1_screen.py --verbose
python3 core/stage2_deep.py --verbose
python3 core/signal_logger.py --verbose

# Or run individual stages
python3 core/stage1_screen.py --date 2026-05-09 --verbose
```

## Cron Schedule

- **Daily 15:30 TWT (Mon-Fri)**: Full pipeline run
- **Results**: Auto-reported to Telegram group

## Known Issues

- Institutional flow API returns HTML → using margin data as proxy
- Historical price data limited by TWSE API → momentum uses single-day proxy
- Regime detector shows "choppy" due to limited price variation

## Next Steps

- [ ] Find reliable historical price source (yfinance fallback)
- [ ] Add backtesting framework
- [ ] Set up paper trading mode
- [ ] Add Telegram alerts for top signals
- [ ] Implement self-improvement weight calibration
