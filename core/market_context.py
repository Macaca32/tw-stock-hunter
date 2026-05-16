#!/usr/bin/env python3
"""
Phase 34: Cross-Asset Correlation Engine

Integrates cross-market data into regime detection and signal quality scoring
for the Taiwan stock screening pipeline.

Features:
- fetch_cross_assets(): Pull TAIEX futures, USD/TWD, HSI, KWEB, VIX via yfinance
  with 6h TTL cache in data/market_context_cache.json
- compute_market_breadth(): Advance/decline ratio from Stage 1 pass/watchlist/skipped
  counts + cross-asset correlation matrix
- get_cross_asset_signal(): Returns -0.2 to +0.2 adjustment based on global risk
  sentiment, USD/TWD trend, and HSI momentum vs TAIEX divergence

All functions return neutral defaults if fetch fails — backward compatible,
no new hard dependencies beyond yfinance (already in requirements.txt).
"""

import json
import logging
import math
import time
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------

CACHE_TTL_SECONDS = 6 * 3600  # 6 hours


def _cache_path():
    """Return the cache file path."""
    return Path(__file__).parent.parent / "data" / "market_context_cache.json"


def _read_cache():
    """Read the cache file if it exists and is not expired."""
    path = _cache_path()
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            cached = json.load(f)
        ts = cached.get("timestamp", 0)
        if time.time() - ts < CACHE_TTL_SECONDS:
            return cached.get("data", {})
        # Expired
        return None
    except (json.JSONDecodeError, OSError, KeyError):
        return None


def _write_cache(data):
    """Write data to the cache file with current timestamp."""
    path = _cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        payload = {
            "timestamp": time.time(),
            "updated_utc": datetime.utcnow().isoformat(),
            "data": data,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        logger.warning("Phase 34: cache write failed: %s", exc)


# ---------------------------------------------------------------------------
# yfinance helpers (graceful fallback)
# ---------------------------------------------------------------------------

def _fetch_ticker(ticker: str, period: str = "5d"):
    """Fetch recent daily data for a single yfinance ticker.

    Returns a list of dicts with 'date', 'close', 'open', 'high', 'low'
    or an empty list on failure.
    """
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period=period, auto_adjust=True)
        if hist.empty:
            return []
        rows = []
        for idx, row in hist.iterrows():
            rows.append({
                "date": idx.strftime("%Y-%m-%d"),
                "open": float(row.get("Open", 0)),
                "high": float(row.get("High", 0)),
                "low": float(row.get("Low", 0)),
                "close": float(row.get("Close", 0)),
            })
        return rows
    except Exception as exc:
        logger.debug("Phase 34: yfinance fetch failed for %s: %s", ticker, exc)
        return []


# ---------------------------------------------------------------------------
# Ticker constants — yfinance symbols
# ---------------------------------------------------------------------------

# TAIEX futures (台指期) — TX is the mini TAIEX future on TAIFEX
TICKER_TAIEX_FUTURES = "^TWII"         # TAIEX index (proxy for futures)
TICKER_USDTWD = "TWDUSD=X"             # USD/TWD exchange rate
TICKER_HSI = "^HSI"                    # Hang Seng Index (恆生指數)
TICKER_KWEB = "KWEB"                   # KraneShares CSI China Internet ETF
TICKER_VIX = "^VIX"                    # CBOE Volatility Index


# ---------------------------------------------------------------------------
# fetch_cross_assets
# ---------------------------------------------------------------------------

def fetch_cross_assets(date_str=None):
    """Pull cross-asset data from yfinance with 6-hour TTL cache.

    Assets fetched:
    - TAIEX futures (TF) proxy via ^TWII
    - USD/TWD exchange rate (TWDUSD=X)
    - Hang Seng Index (^HSI)
    - KWEB — China Internet ETF
    - VIX (^VIX)

    Returns dict with keys: taiex_futures, usd_twd, hsi, kweb, vix
    Each sub-dict has: latest (float), change_pct (float), history (list)
    Returns neutral defaults if any fetch fails.
    """
    # Check cache first
    cached = _read_cache()
    if cached is not None:
        logger.info("Phase 34: Using cached cross-asset data (TTL 6h)")
        return cached

    # Fetch all tickers
    tickers = {
        "taiex_futures": TICKER_TAIEX_FUTURES,
        "usd_twd": TICKER_USDTWD,
        "hsi": TICKER_HSI,
        "kweb": TICKER_KWEB,
        "vix": TICKER_VIX,
    }

    result = {}
    for key, ticker in tickers.items():
        history = _fetch_ticker(ticker, period="5d")
        if history and len(history) >= 1:
            latest = history[-1]["close"]
            if len(history) >= 2:
                prev_close = history[-2]["close"]
                change_pct = ((latest - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
            else:
                change_pct = 0.0
            result[key] = {
                "latest": round(latest, 4),
                "change_pct": round(change_pct, 4),
                "history": history,
                "source": "yfinance",
                "fetched_at": datetime.utcnow().isoformat(),
            }
        else:
            # Neutral default — missing data should not break the pipeline
            result[key] = {
                "latest": None,
                "change_pct": 0.0,
                "history": [],
                "source": "unavailable",
                "fetched_at": None,
            }
            logger.info("Phase 34: %s (%s) data unavailable, using neutral default", key, ticker)

    # Write cache
    _write_cache(result)
    logger.info("Phase 34: Fetched and cached cross-asset data for %d assets", len(tickers))

    return result


# ---------------------------------------------------------------------------
# compute_market_breadth
# ---------------------------------------------------------------------------

def compute_market_breadth(stage1_summary=None, cross_assets=None):
    """Compute market breadth indicators and cross-asset correlation matrix.

    Market breadth:
    - Advance/decline ratio derived from Stage 1 pass/watchlist/rejected counts
      (approximation: more passes = advancing market, more rejections = declining)
    - If stage1_summary is not provided, returns neutral defaults.

    Cross-asset correlation matrix:
    - TAIEX vs HSI (Hong Kong linkage)
    - USD/TWD inverse vs TAIEX (stronger NT$ = headwind for exporters)
    - VIX vs TAIEX (global risk-off / risk-on)

    Args:
        stage1_summary: Dict from Stage 1 output with keys:
            'passed', 'watchlist', 'rejected' counts.
        cross_assets: Dict from fetch_cross_assets().

    Returns:
        Dict with keys:
            advance_decline_ratio: float (0.0-2.0, neutral=1.0)
            breadth_label: str (Traditional Chinese)
            correlation_matrix: dict of {pair: correlation_coefficient}
            data_available: bool
    """
    neutral = {
        "advance_decline_ratio": 1.0,
        "breadth_label": "中性（無資料）",
        "correlation_matrix": {
            "TAIEX_vs_HSI": 0.0,
            "USDTWD逆相關_vs_TAIEX": 0.0,
            "VIX_vs_TAIEX": 0.0,
        },
        "data_available": False,
    }

    # --- Market breadth from Stage 1 counts ---
    ad_ratio = 1.0
    breadth_label = "中性（無資料）"

    if stage1_summary and isinstance(stage1_summary, dict):
        passed = stage1_summary.get("passed", 0)
        watchlist = stage1_summary.get("watchlist", 0)
        rejected = stage1_summary.get("rejected", 0)
        total = passed + watchlist + rejected

        if total > 0:
            # Advance/decline proxy: (passed + watchlist) / rejected
            # Higher ratio = broader market participation (bullish breadth)
            advancing = passed + watchlist
            if rejected > 0:
                ad_ratio = round(advancing / rejected, 3)
            else:
                ad_ratio = 2.0  # Cap at 2.0

            # Traditional Chinese labels for breadth interpretation
            if ad_ratio >= 1.5:
                breadth_label = "市場廣度強勁（多數股上漲）"
            elif ad_ratio >= 1.0:
                breadth_label = "市場廣度正常"
            elif ad_ratio >= 0.5:
                breadth_label = "市場廣度偏弱（多數股下跌）"
            else:
                breadth_label = "市場廣度極弱（嚴重下跌）"

    # --- Cross-asset correlation matrix ---
    if cross_assets is None:
        cross_assets = {}

    corr_matrix = {
        "TAIEX_vs_HSI": 0.0,
        "USDTWD逆相關_vs_TAIEX": 0.0,
        "VIX_vs_TAIEX": 0.0,
    }

    data_available = False

    # Compute correlations if we have sufficient history (at least 3 overlapping days)
    taiex_hist = cross_assets.get("taiex_futures", {}).get("history", [])
    hsi_hist = cross_assets.get("hsi", {}).get("history", [])
    usd_twd_hist = cross_assets.get("usd_twd", {}).get("history", [])
    vix_hist = cross_assets.get("vix", {}).get("history", [])

    def _daily_returns(hist):
        """Compute daily % returns from a history list."""
        rets = []
        for i in range(1, len(hist)):
            prev = hist[i - 1]["close"]
            curr = hist[i]["close"]
            if prev > 0 and curr > 0:
                rets.append((curr - prev) / prev)
        return rets

    def _align_returns(hist_a, hist_b):
        """Align two history lists by date and return paired returns."""
        a_by_date = {h["date"]: h["close"] for h in hist_a if h.get("close")}
        b_by_date = {h["date"]: h["close"] for h in hist_b if h.get("close")}
        common_dates = sorted(set(a_by_date.keys()) & set(b_by_date.keys()))

        if len(common_dates) < 3:
            return [], []

        a_closes = [a_by_date[d] for d in common_dates]
        b_closes = [b_by_date[d] for d in common_dates]

        a_rets = []
        b_rets = []
        for i in range(1, len(a_closes)):
            if a_closes[i - 1] > 0 and b_closes[i - 1] > 0:
                a_rets.append((a_closes[i] - a_closes[i - 1]) / a_closes[i - 1])
                b_rets.append((b_closes[i] - b_closes[i - 1]) / b_closes[i - 1])

        return a_rets, b_rets

    def _pearson(x, y):
        """Compute Pearson correlation coefficient."""
        n = min(len(x), len(y))
        if n < 3:
            return 0.0
        x = x[:n]
        y = y[:n]
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n)) / n
        std_x = (sum((xi - mean_x) ** 2 for xi in x) / n) ** 0.5
        std_y = (sum((yi - mean_y) ** 2 for yi in y) / n) ** 0.5
        if std_x == 0 or std_y == 0:
            return 0.0
        return round(cov / (std_x * std_y), 4)

    # TAIEX vs HSI
    a_rets, b_rets = _align_returns(taiex_hist, hsi_hist)
    if a_rets:
        corr_matrix["TAIEX_vs_HSI"] = _pearson(a_rets, b_rets)
        data_available = True

    # USD/TWD inverse vs TAIEX
    # Stronger NT$ (lower USD/TWD) should correlate with weaker TAIEX for export-driven Taiwan
    a_rets, b_rets = _align_returns(taiex_hist, usd_twd_hist)
    if a_rets:
        # Invert the USD/TWD returns: when NT$ strengthens, USD/TWD drops
        inv_usd_twd_rets = [-r for r in b_rets]
        corr_matrix["USDTWD逆相關_vs_TAIEX"] = _pearson(a_rets, inv_usd_twd_rets)
        data_available = True

    # VIX vs TAIEX
    a_rets, b_rets = _align_returns(taiex_hist, vix_hist)
    if a_rets:
        corr_matrix["VIX_vs_TAIEX"] = _pearson(a_rets, b_rets)
        data_available = True

    return {
        "advance_decline_ratio": ad_ratio,
        "breadth_label": breadth_label,
        "correlation_matrix": corr_matrix,
        "data_available": data_available,
    }


# ---------------------------------------------------------------------------
# get_cross_asset_signal
# ---------------------------------------------------------------------------

def get_cross_asset_signal(cross_assets=None, stage1_summary=None):
    """Compute a cross-asset signal adjustment for composite scores.

    Returns a float from -0.2 to +0.2 based on:
    1. Global risk sentiment (VIX level):
       - VIX < 15 → +0.1 (complacent/risk-on)
       - VIX > 25 → -0.1 (fear/risk-off)
       - In between → scaled proportionally
    2. USD/TWD trend:
       - NT$ strengthening (USD/TWD falling) → bad for exporters → -0.1
       - NT$ weakening (USD/TWD rising) → good for exporters → +0.05
       - Stable → 0.0
    3. HSI momentum vs TAIEX divergence:
       - HSI falling while TAIEX rising → divergence → -0.05 (regional weakness)
       - HSI rising while TAIEX falling → potential catch-up → +0.05
       - Aligned → 0.0

    The final signal is the sum of all three components, clamped to [-0.2, +0.2].
    Returns 0.0 if data is unavailable (backward compatible).

    Args:
        cross_assets: Dict from fetch_cross_assets(). If None, returns 0.0.
        stage1_summary: Optional Stage 1 summary for breadth context.

    Returns:
        signal: float in [-0.2, +0.2]
        details: dict with per-component breakdown (Traditional Chinese labels)
    """
    if not cross_assets:
        return 0.0, {"總訊號": 0.0, "來源": "無跨市場資料"}

    details = {}
    total_signal = 0.0

    # --- Component 1: VIX (全球風險情緒) ---
    vix_data = cross_assets.get("vix", {})
    vix_level = vix_data.get("latest")
    vix_signal = 0.0

    if vix_level is not None and vix_level > 0:
        if vix_level < 15:
            # Low VIX = risk-on environment, supportive for stocks
            vix_signal = +0.1
        elif vix_level > 25:
            # High VIX = risk-off, headwind for stocks
            vix_signal = -0.1
        else:
            # Scale linearly between 15 and 25
            # At VIX=15: +0.1, at VIX=25: -0.1
            vix_signal = round(+0.1 - (vix_level - 15) * 0.02, 4)

        details["全球風險情緒"] = {
            "VIX": vix_level,
            "訊號": vix_signal,
            "說明": "低波動=偏多" if vix_level < 15 else ("高波動=偏空" if vix_level > 25 else "波動中性"),
        }
    else:
        details["全球風險情緒"] = {"VIX": None, "訊號": 0.0, "說明": "無VIX資料"}

    total_signal += vix_signal

    # --- Component 2: USD/TWD trend (匯率趨勢) ---
    usd_twd_data = cross_assets.get("usd_twd", {})
    usd_twd_change = usd_twd_data.get("change_pct", 0.0)
    usd_twd_level = usd_twd_data.get("latest")
    fx_signal = 0.0

    if usd_twd_level is not None:
        # NT$ strengthening (USD/TWD falling) is BAD for Taiwan exporters
        # NT$ weakening (USD/TWD rising) is GOOD for Taiwan exporters
        if usd_twd_change < -0.5:
            # NT$ strengthening significantly → headwind for exporters
            fx_signal = -0.1
        elif usd_twd_change > 0.5:
            # NT$ weakening → tailwind for exporters
            fx_signal = +0.05
        else:
            # Stable exchange rate → neutral
            fx_signal = 0.0

        details["匯率趨勢"] = {
            "USD_TWD": usd_twd_level,
            "變動_pct": usd_twd_change,
            "訊號": fx_signal,
            "說明": "台幣升值不利出口" if usd_twd_change < -0.5 else (
                "台幣貶值有利出口" if usd_twd_change > 0.5 else "匯率穩定"
            ),
        }
    else:
        details["匯率趨勢"] = {"USD_TWD": None, "訊號": 0.0, "說明": "無匯率資料"}

    total_signal += fx_signal

    # --- Component 3: HSI vs TAIEX divergence (港股動能分歧) ---
    hsi_data = cross_assets.get("hsi", {})
    taiex_data = cross_assets.get("taiex_futures", {})
    hsi_change = hsi_data.get("change_pct", 0.0)
    taiex_change = taiex_data.get("change_pct", 0.0)
    hsi_available = hsi_data.get("latest") is not None
    divergence_signal = 0.0

    if hsi_available:
        divergence = hsi_change - taiex_change

        if divergence < -2.0:
            # HSI significantly underperforming TAIEX → regional weakness
            divergence_signal = -0.05
        elif divergence > 2.0:
            # HSI significantly outperforming TAIEX → regional strength / catch-up potential
            divergence_signal = +0.05
        else:
            divergence_signal = 0.0

        details["港股動能分歧"] = {
            "HSI變動_pct": hsi_change,
            "TAIEX變動_pct": taiex_change,
            "分歧值": round(divergence, 2),
            "訊號": divergence_signal,
            "說明": "港股明顯弱於台股（區域性弱勢）" if divergence < -2.0 else (
                "港股明顯強於台股（區域性強勢）" if divergence > 2.0 else "台港走勢一致"
            ),
        }
    else:
        details["港股動能分歧"] = {"HSI": None, "訊號": 0.0, "說明": "無港股資料"}

    total_signal += divergence_signal

    # --- Clamp to [-0.2, +0.2] ---
    total_signal = max(-0.2, min(0.2, round(total_signal, 4)))
    details["總訊號"] = total_signal
    details["來源"] = "yfinance"

    return total_signal, details


# ---------------------------------------------------------------------------
# Convenience: get full market context for a date
# ---------------------------------------------------------------------------

def get_market_context(date_str=None, stage1_summary=None):
    """Fetch all cross-asset context for a given date.

    This is the main entry point for other modules (regime_detector, stage2_deep).

    Returns dict with:
        cross_assets: raw fetch_cross_assets() output
        market_breadth: compute_market_breadth() output
        cross_asset_signal: float from get_cross_asset_signal()
        signal_details: per-component breakdown
        timestamp: ISO timestamp
    """
    cross_assets = fetch_cross_assets(date_str)
    market_breadth = compute_market_breadth(stage1_summary=stage1_summary, cross_assets=cross_assets)
    signal, signal_details = get_cross_asset_signal(cross_assets=cross_assets, stage1_summary=stage1_summary)

    return {
        "cross_assets": cross_assets,
        "market_breadth": market_breadth,
        "cross_asset_signal": signal,
        "signal_details": signal_details,
        "timestamp": datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """CLI entry point for market_context module."""
    import argparse
    parser = argparse.ArgumentParser(description="Phase 34: Cross-Asset Correlation Engine")
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    context = get_market_context(date_str=args.date)

    if args.verbose:
        print("=== Phase 34: 跨市場關聯引擎 ===")
        print(f"時間: {context['timestamp']}")

        # Cross-assets summary
        print("\n--- 跨市場資產 ---")
        for key, info in context["cross_assets"].items():
            label_map = {
                "taiex_futures": "台指期",
                "usd_twd": "美元/台幣",
                "hsi": "恆生指數",
                "kweb": "KWEB（中國ETF）",
                "vix": "VIX恐慌指數",
            }
            label = label_map.get(key, key)
            latest = info.get("latest")
            change = info.get("change_pct", 0.0)
            if latest is not None:
                print(f"  {label}: {latest} ({change:+.2f}%)")
            else:
                print(f"  {label}: 無資料")

        # Market breadth
        print("\n--- 市場廣度 ---")
        breadth = context["market_breadth"]
        print(f"  漲跌比: {breadth['advance_decline_ratio']:.3f}")
        print(f"  標籤: {breadth['breadth_label']}")

        # Correlation matrix
        print("\n--- 跨資產相關性 ---")
        for pair, corr in breadth["correlation_matrix"].items():
            print(f"  {pair}: {corr:+.4f}")

        # Cross-asset signal
        print("\n--- 跨資產訊號 ---")
        print(f"  總訊號: {context['cross_asset_signal']:+.4f}")
        for key, val in context["signal_details"].items():
            if isinstance(val, dict):
                signal_val = val.get("訊號", val)
                desc = val.get("說明", "")
                print(f"  {key}: {signal_val:+.4f} ({desc})")

    return context


if __name__ == "__main__":
    main()
