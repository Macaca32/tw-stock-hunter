#!/usr/bin/env python3
"""
Phase 38: Portfolio Optimization Engine

Mean-variance / Black-Litterman portfolio optimization with Taiwan market
constraints. Integrates as Stage 12 in run_pipeline.py after signal_fusion.

All labels use Traditional Chinese for Taiwan-market awareness.
Backward compatible: every function returns neutral defaults when data is missing
or numpy/scipy unavailable.

Features:
1. load_ensemble_results()        — Read Phase 37 ensemble output
2. compute_covariance_matrix()     — Empirical covariance with holiday forward-fill
3. optimize_mean_variance()        — Markowitz with Taiwan-specific constraints
4. optimize_black_litterman()      — BL framework with ensemble-derived views
5. apply_weight_smoothing()        — Phase 6 integration: max 5% daily change
6. validate_constraints()          — Check Phases 5/36 constraints
7. run_portfolio_optimizer()       — Pipeline Stage 12 entry point

Author: Z.ai GLM-5.1 (Phase 38 implementation)
"""

import json
import logging
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Taiwan market constraints (matching Phase 36 risk_manager)
MAX_SINGLE_STOCK_PCT = 0.08   # <=8% per stock
MAX_SECTOR_PCT = 0.25         # <=25% per sector
CORRELATION_RISK_THRESHOLD = 0.85  # pairwise correlation above this triggers flag
MIN_POSITION_PCT = 0.01       # >=1% or 0 (no tiny positions)
MAX_DAILY_WEIGHT_CHANGE = 0.05  # Phase 6: max 5% per-day weight changes

# Minimum trading days for covariance computation
MIN_TRADING_DAYS = 30
DEFAULT_LOOKBACK_DAYS = 60

# Risk-free rate approximation (Taiwan 1-year govt bond ~1.1%)
RISK_FREE_RATE_DAILY = 0.011 / 252
RISK_FREE_RATE_ANNUAL = 0.011

# Cache / data directories
_DATA_DIR = Path(__file__).parent.parent / "data"
_CONFIG_DIR = Path(__file__).parent.parent / "config"

# Taiwan market labels (Traditional Chinese)
OPTIMIZATION_LABELS_ZH = {
    "mean_variance": "均值-變異數最佳化",
    "black_litterman": "Black-Litterman 模型",
    "heuristic": "啟發式迭代最佳化（無scipy）",
    "equal_weight": "等權重（數據不足）",
    "no_change": "權重未變動",
    "smoothed": "已套用平滑化（每日最大變動5%）",
    "constraint_violation": "約束條件違反",
    "insufficient_data": "數據不足",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _data_dir() -> Path:
    """Return the project data directory."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    return _DATA_DIR


def _config_dir() -> Path:
    """Return the project config directory."""
    return _CONFIG_DIR


def _try_import_numpy():
    """Try importing numpy, return None if unavailable."""
    try:
        import numpy as np
        return np
    except ImportError:
        return None


def _try_import_scipy():
    """Try importing scipy.optimize, return None if unavailable."""
    try:
        from scipy import optimize
        return optimize
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Feature 1: load_ensemble_results
# ---------------------------------------------------------------------------

def load_ensemble_results(date_str: str) -> Dict[str, Any]:
    """Load ensemble results from Phase 37 signal_fusion output.

    Reads data/ensemble_YYYY-MM-DD.json and returns sorted candidates
    with ensemble_score, confidence bands, signals dict, and top-3
    feature importance.

    Falls back gracefully if file is missing or corrupt.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        Dict with keys:
            date: str
            candidates: list of dicts sorted by ensemble_score DESC, each with:
                code, name, ensemble_score, confidence_lower, confidence_upper,
                signals, feature_importance_top3, label_zh
            total_stocks: int
            loaded: bool — whether file was successfully loaded
            error: str or None — error message if loading failed
    """
    result = {
        "date": date_str,
        "candidates": [],
        "total_stocks": 0,
        "loaded": False,
        "error": None,
    }

    ensemble_path = _data_dir() / f"ensemble_{date_str}.json"
    if not ensemble_path.exists():
        result["error"] = f"ensemble_{date_str}.json 不存在"
        logger.debug("Phase 38: %s not found", ensemble_path)
        return result

    try:
        with open(ensemble_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        result["error"] = f"讀取失敗: {e}"
        logger.warning("Phase 38: failed to load %s: %s", ensemble_path, e)
        return result

    # Extract stocks — handle both "stocks" and "ranking" keys
    stocks = data.get("ranking", data.get("stocks", []))
    if not stocks:
        result["error"] = "無候選股票"
        return result

    # Build sorted candidates
    candidates = []
    for stock in stocks:
        code = stock.get("code", "")
        name = stock.get("name", "")
        ensemble_score = stock.get("ensemble_score", 0.5)
        conf_lower = stock.get("confidence_lower", ensemble_score - 0.05)
        conf_upper = stock.get("confidence_upper", ensemble_score + 0.05)
        signals = stock.get("signals", {})
        top3 = stock.get("feature_importance_top3", [])
        label_zh = stock.get("label_zh", "")

        candidates.append({
            "code": code,
            "name": name,
            "ensemble_score": ensemble_score,
            "confidence_lower": conf_lower,
            "confidence_upper": conf_upper,
            "signals": signals,
            "feature_importance_top3": top3,
            "label_zh": label_zh,
        })

    # Sort by ensemble_score descending
    candidates.sort(key=lambda x: x["ensemble_score"], reverse=True)

    result["candidates"] = candidates
    result["total_stocks"] = len(candidates)
    result["loaded"] = True

    return result


# ---------------------------------------------------------------------------
# Feature 2: compute_covariance_matrix
# ---------------------------------------------------------------------------

def compute_covariance_matrix(
    stock_ids: List[str],
    price_history: Dict[str, List[Dict]],
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Dict[str, Any]:
    """Build empirical covariance matrix from adjusted close prices.

    Computes mean daily returns and covariance matrix. Handles Taiwan-market
    quirks: sparse data on weekends/holidays via forward-fill, minimum 30
    trading days requirement.

    Uses numpy if available, falls back to pure Python for compatibility.

    Args:
        stock_ids: List of stock codes to include
        price_history: Dict mapping stock_id -> list of daily OHLCV dicts
            Each dict should have: date, close or adj_close
        lookback_days: Number of trading days to use (default 60)

    Returns:
        Dict with keys:
            stock_ids: list — stock codes in matrix order
            means: dict — stock_id -> mean daily return
            cov_matrix: list of lists — covariance matrix (stock_ids x stock_ids)
            n_days: int — actual number of trading days used
            method: str — "numpy" or "pure_python"
            sufficient_data: bool — whether minimum 30 days was met
            label_zh: str
    """
    result = {
        "stock_ids": stock_ids,
        "means": {},
        "cov_matrix": [],
        "n_days": 0,
        "method": "pure_python",
        "sufficient_data": False,
        "label_zh": "",
    }

    if not stock_ids:
        return result

    # Extract adjusted close prices for each stock
    price_series = {}
    for sid in stock_ids:
        history = price_history.get(sid, [])
        if not history:
            continue

        # Take the last lookback_days entries
        recent = history[-lookback_days:] if len(history) > lookback_days else history

        # Forward-fill: handle weekends/holidays by filling gaps
        closes = []
        for day in recent:
            close = day.get("adj_close") or day.get("close") or 0
            closes.append(close)

        # Forward-fill zeros/None (weekends, holidays, missing data)
        for i in range(len(closes)):
            if closes[i] == 0 and i > 0 and closes[i - 1] > 0:
                closes[i] = closes[i - 1]

        # Remove leading zeros
        while closes and closes[0] == 0:
            closes.pop(0)

        if len(closes) >= 2:
            price_series[sid] = closes

    if len(price_series) < 1:
        result["label_zh"] = "無可用價格數據"
        return result

    # Align all series to the same length (shortest common length)
    min_len = min(len(v) for v in price_series.values())
    n_days = min_len - 1  # Returns have one fewer element

    if n_days < MIN_TRADING_DAYS:
        result["n_days"] = n_days
        result["label_zh"] = f"交易日不足（需{MIN_TRADING_DAYS}天，僅{n_days}天）"
        return result

    # Compute daily returns
    returns_dict = {}
    for sid, closes in price_series.items():
        aligned = closes[-min_len:]
        daily_returns = []
        for i in range(1, len(aligned)):
            prev = aligned[i - 1]
            curr = aligned[i]
            if prev > 0:
                daily_returns.append((curr - prev) / prev)
            else:
                daily_returns.append(0.0)
        returns_dict[sid] = daily_returns

    # Build aligned stock list (only those with returns)
    valid_ids = [sid for sid in stock_ids if sid in returns_dict]
    if not valid_ids:
        result["label_zh"] = "無有效報酬率數據"
        return result

    n_stocks = len(valid_ids)
    n_obs = len(returns_dict[valid_ids[0]])

    # Compute means
    means = {}
    for sid in valid_ids:
        ret = returns_dict[sid]
        means[sid] = sum(ret) / len(ret) if ret else 0.0
    result["means"] = means

    # Try numpy path
    np = _try_import_numpy()
    if np is not None:
        result["method"] = "numpy"
        # Build returns matrix (n_obs x n_stocks)
        ret_matrix = np.array([returns_dict[sid] for sid in valid_ids]).T  # (n_obs, n_stocks)
        cov = np.cov(ret_matrix, rowvar=False)  # (n_stocks, n_stocks)
        # Handle single stock case
        if n_stocks == 1:
            cov = np.array([[cov]])
        result["cov_matrix"] = cov.tolist()
    else:
        # Pure Python fallback
        result["method"] = "pure_python"
        # Compute covariance matrix manually
        ret_lists = [returns_dict[sid] for sid in valid_ids]
        cov_matrix = []

        for i in range(n_stocks):
            row = []
            ri = ret_lists[i]
            mean_i = means[valid_ids[i]]
            for j in range(n_stocks):
                rj = ret_lists[j]
                mean_j = means[valid_ids[j]]
                # Covariance
                cov_ij = sum(
                    (ri[k] - mean_i) * (rj[k] - mean_j)
                    for k in range(n_obs)
                ) / (n_obs - 1) if n_obs > 1 else 0.0
                row.append(round(cov_ij, 10))
            cov_matrix.append(row)

        result["cov_matrix"] = cov_matrix

    result["stock_ids"] = valid_ids
    result["n_days"] = n_obs
    result["sufficient_data"] = True
    result["label_zh"] = f"共變異矩陣：{n_stocks}檔股票，{n_obs}個交易日（{result['method']}）"

    return result


# ---------------------------------------------------------------------------
# Feature 3: optimize_mean_variance
# ---------------------------------------------------------------------------

def optimize_mean_variance(
    expected_returns: Dict[str, float],
    cov_matrix: Any,  # list of lists or numpy array
    stock_ids: List[str],
    constraints: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Solve the Markowitz mean-variance optimization with Taiwan constraints.

    Constraints:
    - Stock cap: w_i <= 8% (Phase 36 risk_manager)
    - Sector caps: max 25% per sector (Phase 5)
    - Minimum position size: >=1% or 0 (no tiny positions)
    - Long-only (no shorting — Taiwan retail focus)
    - Weights sum to 1.0

    Uses scipy.optimize.minimize if available; falls back to iterative
    heuristic optimizer otherwise.

    Args:
        expected_returns: Dict mapping stock_id -> expected daily return
        cov_matrix: Covariance matrix (list of lists or numpy array)
        stock_ids: Stock codes in same order as cov_matrix rows/cols
        constraints: Optional dict with:
            max_stock_pct: float (default 0.08)
            max_sector_pct: float (default 0.25)
            min_position_pct: float (default 0.01)
            sector_map: Dict mapping stock_id -> sector name

    Returns:
        Dict with keys:
            weights: Dict[str, float] — stock_id -> optimized weight
            expected_return_annual: float — annualized expected return
            expected_volatility_annual: float — annualized expected volatility
            sharpe_ratio: float — risk-adjusted return
            method: str — "scipy" or "heuristic"
            constraint_violations: list — any violations found
            label_zh: str
    """
    # Default constraints
    cons = {
        "max_stock_pct": MAX_SINGLE_STOCK_PCT,
        "max_sector_pct": MAX_SECTOR_PCT,
        "min_position_pct": MIN_POSITION_PCT,
        "sector_map": {},
    }
    if constraints:
        cons.update(constraints)

    result = {
        "weights": {},
        "expected_return_annual": 0.0,
        "expected_volatility_annual": 0.0,
        "sharpe_ratio": 0.0,
        "method": "equal_weight",
        "constraint_violations": [],
        "label_zh": "",
    }

    n = len(stock_ids)
    if n == 0:
        result["label_zh"] = "無股票可最佳化"
        return result

    # Build expected returns vector and cov_matrix as list of lists
    mu = [expected_returns.get(sid, 0.0) for sid in stock_ids]

    # Convert cov_matrix to list-of-lists if needed
    if isinstance(cov_matrix, list):
        sigma = cov_matrix
    else:
        # Might be numpy array
        try:
            sigma = cov_matrix.tolist()
        except AttributeError:
            sigma = [[0.001]]  # fallback

    # If single stock, return max weight
    if n == 1:
        w = cons["max_stock_pct"]
        result["weights"] = {stock_ids[0]: round(w, 4)}
        ann_ret = mu[0] * 252
        ann_vol = math.sqrt(sigma[0][0] * 252) if sigma and sigma[0] else 0.0
        result["expected_return_annual"] = round(ann_ret, 6)
        result["expected_volatility_annual"] = round(ann_vol, 6)
        result["sharpe_ratio"] = round((ann_ret - RISK_FREE_RATE_ANNUAL) / ann_vol, 4) if ann_vol > 0 else 0.0
        result["method"] = "single_stock"
        result["label_zh"] = f"單一股票：{stock_ids[0]}，權重{w*100:.0f}%"
        return result

    # Try scipy optimization
    scipy_opt = _try_import_scipy()
    np = _try_import_numpy()

    if scipy_opt is not None and np is not None:
        result.update(_optimize_scipy(mu, sigma, n, stock_ids, cons, np, scipy_opt))
    else:
        result.update(_optimize_heuristic(mu, sigma, n, stock_ids, cons))

    # Compute portfolio metrics from result weights
    weights_list = [result["weights"].get(sid, 0.0) for sid in stock_ids]
    ann_ret, ann_vol, sharpe = _compute_portfolio_metrics(weights_list, mu, sigma)
    result["expected_return_annual"] = round(ann_ret, 6)
    result["expected_volatility_annual"] = round(ann_vol, 6)
    result["sharpe_ratio"] = round(sharpe, 4)

    # Label
    result["label_zh"] = (
        f"{OPTIMIZATION_LABELS_ZH.get(result['method'], result['method'])}："
        f"{n}檔股票，年化報酬{ann_ret*100:.2f}%，年化波動{ann_vol*100:.2f}%，"
        f"夏普值{sharpe:.2f}"
    )

    return result


def _optimize_scipy(mu, sigma, n, stock_ids, cons, np, optimize):
    """Scipy-based mean-variance optimization."""
    mu_np = np.array(mu)
    sigma_np = np.array(sigma)

    max_stock = cons["max_stock_pct"]
    min_pos = cons["min_position_pct"]

    # Objective: minimize portfolio variance for given return (maximize Sharpe)
    def neg_sharpe(w):
        w = np.array(w)
        port_ret = np.dot(w, mu_np) * 252
        port_vol = np.sqrt(np.dot(w, np.dot(sigma_np, w)) * 252)
        if port_vol < 1e-10:
            return 0.0
        return -(port_ret - RISK_FREE_RATE_ANNUAL) / port_vol

    # Bounds: long-only, min_pos to max_stock
    bounds = [(min_pos, max_stock) for _ in range(n)]

    # Constraint: weights sum to 1.0
    sum_constraint = {"type": "eq", "fun": lambda w: np.sum(w) - 1.0}

    # Sector constraints (if sector_map provided)
    sector_map = cons.get("sector_map", {})
    sector_constraints = []
    if sector_map:
        sectors = set(sector_map.values())
        for sector in sectors:
            sector_indices = [i for i, sid in enumerate(stock_ids)
                              if sector_map.get(sid) == sector]
            if sector_indices:
                sector_constraints.append({
                    "type": "ineq",
                    "fun": lambda w, idx=sector_indices: cons["max_sector_pct"] - sum(w[i] for i in idx),
                })

    all_constraints = [sum_constraint] + sector_constraints

    # Initial guess: equal weight (capped by max_stock)
    w0 = np.full(n, min(1.0 / n, max_stock))
    w0 = w0 / w0.sum()  # normalize

    try:
        opt_result = optimize.minimize(
            neg_sharpe,
            w0,
            method="SLSQP",
            bounds=bounds,
            constraints=all_constraints,
            options={"maxiter": 500, "ftol": 1e-9},
        )

        if opt_result.success:
            weights = opt_result.x
        else:
            # Try with relaxed bounds (no minimum position)
            bounds_relaxed = [(0.0, max_stock) for _ in range(n)]
            opt_result2 = optimize.minimize(
                neg_sharpe,
                w0,
                method="SLSQP",
                bounds=bounds_relaxed,
                constraints=all_constraints,
                options={"maxiter": 500, "ftol": 1e-9},
            )
            weights = opt_result2.x if opt_result2.success else w0
    except Exception as e:
        logger.debug("Phase 38: scipy optimization failed: %s", e)
        weights = w0

    # Build weights dict, zero out tiny positions
    weight_dict = {}
    for i, sid in enumerate(stock_ids):
        w = float(weights[i])
        if w < min_pos:
            w = 0.0
        weight_dict[sid] = round(w, 6)

    # Re-normalize
    total = sum(weight_dict.values())
    if total > 0:
        weight_dict = {k: round(v / total, 6) for k, v in weight_dict.items()}

    return {"weights": weight_dict, "method": "scipy"}


def _optimize_heuristic(mu, sigma, n, stock_ids, cons):
    """Iterative heuristic optimizer when scipy is unavailable.

    Uses a simple gradient-ascent approach on the Sharpe ratio,
    respecting position limits.
    """
    max_stock = cons["max_stock_pct"]
    min_pos = cons["min_position_pct"]

    # Start with equal weights, capped by max_stock
    w = [min(1.0 / n, max_stock)] * n
    total = sum(w)
    w = [wi / total for wi in w]

    best_sharpe = -999
    best_w = list(w)

    # Iterative improvement: shift weight toward higher Sharpe contributors
    for iteration in range(200):
        # Compute marginal Sharpe contribution for each stock
        port_ret = sum(w[i] * mu[i] for i in range(n))
        port_var = sum(
            w[i] * w[j] * sigma[i][j]
            for i in range(n) for j in range(n)
        )
        port_vol = math.sqrt(port_var) if port_var > 0 else 1e-10
        current_sharpe = (port_ret * 252 - RISK_FREE_RATE_ANNUAL) / (port_vol * math.sqrt(252)) if port_vol > 1e-10 else 0.0

        if current_sharpe > best_sharpe:
            best_sharpe = current_sharpe
            best_w = list(w)

        # Compute gradient: marginal return contribution
        gradients = []
        for i in range(n):
            # d(port_ret)/d(w_i) = mu_i
            # d(port_var)/d(w_i) = 2 * sum(w_j * sigma[i][j])
            d_ret = mu[i]
            d_var = 2 * sum(w[j] * sigma[i][j] for j in range(n))
            d_vol = d_var / (2 * port_vol) if port_vol > 1e-10 else 0.0
            # d(Sharpe)/d(w_i) approx
            grad = (d_ret * 252 * port_vol * math.sqrt(252) -
                    (port_ret * 252 - RISK_FREE_RATE_ANNUAL) * d_vol * math.sqrt(252)) / (port_vol * math.sqrt(252)) ** 2
            gradients.append(grad)

        # Shift weight: small step toward gradient direction
        step = 0.005
        for i in range(n):
            if gradients[i] > 0:
                w[i] += step * min(gradients[i] / (max(abs(g) for g in gradients) + 1e-10), 1.0)
            else:
                w[i] -= step * min(abs(gradients[i]) / (max(abs(g) for g in gradients) + 1e-10), 1.0)

        # Apply constraints
        for i in range(n):
            w[i] = max(0.0, min(max_stock, w[i]))

        # Re-normalize
        total = sum(w)
        if total > 0:
            w = [wi / total for wi in w]

    # Zero out positions below minimum
    weight_dict = {}
    for i, sid in enumerate(stock_ids):
        wi = best_w[i]
        if wi < min_pos:
            wi = 0.0
        weight_dict[sid] = round(wi, 6)

    # Re-normalize
    total = sum(weight_dict.values())
    if total > 0:
        weight_dict = {k: round(v / total, 6) for k, v in weight_dict.items()}

    return {"weights": weight_dict, "method": "heuristic"}


def _compute_portfolio_metrics(weights_list, mu, sigma):
    """Compute annualized return, volatility, and Sharpe ratio.

    Args:
        weights_list: List of weights in same order as mu and sigma
        mu: List of expected daily returns
        sigma: Covariance matrix (list of lists)

    Returns:
        (annual_return, annual_volatility, sharpe_ratio)
    """
    n = len(weights_list)
    if n == 0:
        return 0.0, 0.0, 0.0

    daily_ret = sum(weights_list[i] * mu[i] for i in range(n))

    daily_var = 0.0
    for i in range(n):
        for j in range(n):
            if i < len(sigma) and j < len(sigma[i]):
                daily_var += weights_list[i] * weights_list[j] * sigma[i][j]

    ann_ret = daily_ret * 252
    ann_vol = math.sqrt(daily_var * 252) if daily_var > 0 else 0.0
    sharpe = (ann_ret - RISK_FREE_RATE_ANNUAL) / ann_vol if ann_vol > 1e-10 else 0.0

    return ann_ret, ann_vol, sharpe


# ---------------------------------------------------------------------------
# Feature 4: optimize_black_litterman
# ---------------------------------------------------------------------------

def optimize_black_litterman(
    expected_returns: Dict[str, float],
    cov_matrix: Any,
    stock_ids: List[str],
    risk_aversion: float = 2.5,
    views: Optional[Dict[str, Dict]] = None,
    market_cap_weights: Optional[Dict[str, float]] = None,
    tau: float = 0.05,
    constraints: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Black-Litterman portfolio optimization framework.

    Combines market equilibrium returns with investor views (derived from
    signal_fusion ensemble scores) using the BL formula.

    Args:
        expected_returns: Dict mapping stock_id -> expected daily return
        cov_matrix: Covariance matrix (list of lists)
        stock_ids: Stock codes in matrix order
        risk_aversion: Risk aversion parameter delta (default 2.5)
        views: Optional investor views as dict:
            {stock_id: {"return": float, "confidence": 0-1}}
            Derived from signal_fusion ensemble scores if not provided.
        market_cap_weights: Market equilibrium weights (from Phase 6
            weight smoothing output if available). Equal weight if None.
        tau: Uncertainty in prior (default 0.05)
        constraints: Same as optimize_mean_variance constraints

    Returns:
        Dict with same keys as optimize_mean_variance
    """
    cons = {
        "max_stock_pct": MAX_SINGLE_STOCK_PCT,
        "max_sector_pct": MAX_SECTOR_PCT,
        "min_position_pct": MIN_POSITION_PCT,
        "sector_map": {},
    }
    if constraints:
        cons.update(constraints)

    result = {
        "weights": {},
        "expected_return_annual": 0.0,
        "expected_volatility_annual": 0.0,
        "sharpe_ratio": 0.0,
        "method": "black_litterman",
        "constraint_violations": [],
        "label_zh": "",
    }

    n = len(stock_ids)
    if n == 0:
        result["label_zh"] = "無股票可最佳化"
        return result

    # Convert cov_matrix to list of lists
    if isinstance(cov_matrix, list):
        sigma = cov_matrix
    else:
        try:
            sigma = cov_matrix.tolist()
        except AttributeError:
            sigma = [[0.001]]

    np = _try_import_numpy()

    # Market equilibrium weights
    if market_cap_weights:
        w_mkt = [market_cap_weights.get(sid, 0.0) for sid in stock_ids]
        total_w = sum(w_mkt)
        if total_w > 0:
            w_mkt = [w / total_w for w in w_mkt]
        else:
            w_mkt = [1.0 / n] * n
    else:
        w_mkt = [1.0 / n] * n

    # Implied equilibrium returns: Pi = delta * Sigma * w_mkt
    # (using daily covariance and returns)
    pi = []
    for i in range(n):
        implied_ret = risk_aversion * sum(sigma[i][j] * w_mkt[j] for j in range(n))
        pi.append(implied_ret)

    # If no views provided, use ensemble-derived views
    if views is None:
        views = {}
        for sid in stock_ids:
            ret = expected_returns.get(sid, 0.0)
            # Map ensemble expected return to a BL view with moderate confidence
            views[sid] = {"return": ret, "confidence": 0.3}

    # Build BL posterior returns
    # BL formula: E[r] = [(tau*Sigma)^-1 + P'Omega^-1P]^-1 * [(tau*Sigma)^-1 * Pi + P'Omega^-1 * Q]
    # Simplified for absolute views on each stock:
    #   mu_BL = [(tau*Sigma)^-1 + Omega^-1]^-1 * [(tau*Sigma)^-1 * Pi + Omega^-1 * Q]

    if np is not None:
        try:
            sigma_np = np.array(sigma)
            pi_np = np.array(pi)

            # Build view matrix P (identity for absolute views) and view returns Q
            view_ids = [sid for sid in stock_ids if sid in views]
            if not view_ids:
                bl_returns = pi
            else:
                # Omega: uncertainty of views (diagonal matrix)
                # omega_ii = (1 - confidence) * variance
                omega_diag = []
                q_vec = []
                for sid in stock_ids:
                    view = views.get(sid, {})
                    conf = view.get("confidence", 0.3)
                    view_return = view.get("return", pi[stock_ids.index(sid)] if sid in stock_ids else 0.0)
                    omega_diag.append((1.0 - conf) * sigma[stock_ids.index(sid)][stock_ids.index(sid)] if conf < 1.0 else 1e-10)
                    q_vec.append(view_return)

                omega_np = np.diag(omega_diag)
                tau_sigma_inv = np.linalg.inv(tau * sigma_np + 1e-10 * np.eye(n))
                omega_inv = np.linalg.inv(omega_np + 1e-10 * np.eye(n))

                # BL posterior
                m1 = tau_sigma_inv + omega_inv
                m2 = tau_sigma_inv @ pi_np + omega_inv @ np.array(q_vec)
                bl_returns = np.linalg.solve(m1, m2).tolist()
        except Exception as e:
            logger.debug("Phase 38: numpy BL failed, using weighted average: %s", e)
            bl_returns = _bl_weighted_average(pi, expected_returns, views, stock_ids)
    else:
        bl_returns = _bl_weighted_average(pi, expected_returns, views, stock_ids)

    # Now run mean-variance optimization with BL returns
    bl_expected = {sid: bl_returns[i] for i, sid in enumerate(stock_ids)}
    mv_result = optimize_mean_variance(bl_expected, sigma, stock_ids, cons)

    result["weights"] = mv_result["weights"]
    result["expected_return_annual"] = mv_result["expected_return_annual"]
    result["expected_volatility_annual"] = mv_result["expected_volatility_annual"]
    result["sharpe_ratio"] = mv_result["sharpe_ratio"]
    result["constraint_violations"] = mv_result["constraint_violations"]
    result["method"] = "black_litterman"

    result["label_zh"] = (
        f"Black-Litterman 模型：{n}檔股票，年化報酬"
        f"{mv_result['expected_return_annual']*100:.2f}%，夏普值{mv_result['sharpe_ratio']:.2f}"
    )

    return result


def _bl_weighted_average(pi, expected_returns, views, stock_ids):
    """Simplified BL: weighted average of equilibrium and view returns."""
    bl_returns = []
    for i, sid in enumerate(stock_ids):
        view = views.get(sid, {})
        conf = view.get("confidence", 0.3)
        view_ret = view.get("return", expected_returns.get(sid, 0.0))
        # Weighted average: (1 - conf) * equilibrium + conf * view
        bl_ret = (1.0 - conf) * pi[i] + conf * view_ret
        bl_returns.append(bl_ret)
    return bl_returns


# ---------------------------------------------------------------------------
# Feature 5: apply_weight_smoothing
# ---------------------------------------------------------------------------

def apply_weight_smoothing(
    old_weights: Dict[str, float],
    new_weights: Dict[str, float],
    max_daily_change: float = MAX_DAILY_WEIGHT_CHANGE,
) -> Dict[str, Any]:
    """Limit per-day weight changes to avoid flash-trade triggers.

    Phase 6 integration: interpolates between old and new weights,
    capping each weight's change at max_daily_change (default 5%).
    Normalizes after smoothing.

    Args:
        old_weights: Previous day's weight dict {stock_id: weight}
        new_weights: Target weight dict {stock_id: weight}
        max_daily_change: Maximum absolute change per weight (0.05 = 5pp)

    Returns:
        Dict with keys:
            smoothed_weights: Dict[str, float] — adjusted weights
            changes: Dict[str, float] — actual per-stock weight changes
            max_change_hit: list — stock_ids where cap was applied
            smoothing_applied: bool — whether any smoothing was needed
            label_zh: str
    """
    # Normalize inputs
    def _normalize(w):
        total = sum(w.values())
        return {k: v / total for k, v in w.items()} if total > 0 else w

    old_norm = _normalize(old_weights)
    new_norm = _normalize(new_weights)

    smoothed = {}
    changes = {}
    max_change_hit = []

    all_keys = set(old_norm.keys()) | set(new_norm.keys())

    for key in all_keys:
        old_val = old_norm.get(key, 0.0)
        new_val = new_norm.get(key, 0.0)
        delta = new_val - old_val

        if abs(delta) <= max_daily_change:
            smoothed[key] = new_val
        else:
            # Clamp change
            direction = 1.0 if delta > 0 else -1.0
            smoothed[key] = old_val + direction * max_daily_change
            max_change_hit.append(key)

        changes[key] = round(smoothed[key] - old_val, 6)

    # Final normalization
    total = sum(smoothed.values())
    if total > 0:
        smoothed = {k: round(v / total, 6) for k, v in smoothed.items()}

    smoothing_applied = len(max_change_hit) > 0

    # Label
    if not smoothing_applied:
        label_zh = OPTIMIZATION_LABELS_ZH["no_change"]
    else:
        label_zh = f"{OPTIMIZATION_LABELS_ZH['smoothed']}，{len(max_change_hit)}檔觸及上限"

    return {
        "smoothed_weights": smoothed,
        "changes": changes,
        "max_change_hit": max_change_hit,
        "smoothing_applied": smoothing_applied,
        "label_zh": label_zh,
    }


# ---------------------------------------------------------------------------
# Feature 6: validate_constraints
# ---------------------------------------------------------------------------

def validate_constraints(
    weights: Dict[str, float],
    sector_map: Optional[Dict[str, str]] = None,
    correlation_matrix: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Check all constraints from Phases 5/36.

    Validates:
    - Single stock <= 8%
    - Sector caps <= 25% per sub-sector
    - Correlation risk: pairs with corr > 0.85 flagged
    - Total = 1.0 (within 0.1% tolerance)

    Args:
        weights: Dict mapping stock_id -> weight
        sector_map: Dict mapping stock_id -> sector name
        correlation_matrix: Optional pairwise correlation dict.
            Format: {(id_a, id_b): corr} or {"id_a-id_b": corr}

    Returns:
        Dict with keys:
            valid: bool — all constraints satisfied
            violations: list of dicts with violation details
            sector_allocation: Dict[str, float] — sector -> total weight
            total_weight: float — sum of all weights
            correlation_flags: list — high-correlation pairs
            label_zh: str
    """
    violations = []
    correlation_flags = []

    if not weights:
        return {
            "valid": False,
            "violations": [{"type": "no_weights", "label_zh": "無權重數據"}],
            "sector_allocation": {},
            "total_weight": 0.0,
            "correlation_flags": [],
            "label_zh": "無權重數據",
        }

    # Check single stock cap
    for sid, w in weights.items():
        if w > MAX_SINGLE_STOCK_PCT + 0.001:  # small tolerance
            violations.append({
                "type": "single_stock_cap",
                "stock_id": sid,
                "weight": round(w, 4),
                "limit": MAX_SINGLE_STOCK_PCT,
                "label_zh": f"單一持股超限：{sid} 權重{w*100:.1f}% > {MAX_SINGLE_STOCK_PCT*100:.0f}%",
            })

    # Check sector caps
    sector_allocation = {}
    if sector_map:
        for sid, w in weights.items():
            sector = sector_map.get(sid, "other")
            sector_allocation[sector] = sector_allocation.get(sector, 0.0) + w

        for sector, total_w in sector_allocation.items():
            if total_w > MAX_SECTOR_PCT + 0.001:
                violations.append({
                    "type": "sector_cap",
                    "sector": sector,
                    "weight": round(total_w, 4),
                    "limit": MAX_SECTOR_PCT,
                    "label_zh": f"產業別超限：{sector} 權重{total_w*100:.1f}% > {MAX_SECTOR_PCT*100:.0f}%",
                })
    else:
        # No sector map — allocate to "unknown"
        sector_allocation = {"unknown": sum(weights.values())}

    # Check correlation risk
    if correlation_matrix and weights:
        stock_ids = [sid for sid in weights if weights[sid] > 0]
        for i, sid_a in enumerate(stock_ids):
            for sid_b in stock_ids[i + 1:]:
                # Try different key formats
                pair_key = (sid_a, sid_b)
                alt_key = f"{sid_a}-{sid_b}"
                alt_key2 = (sid_b, sid_a)
                alt_key3 = f"{sid_b}-{sid_a}"

                corr = None
                for key in [pair_key, alt_key, alt_key2, alt_key3]:
                    if key in correlation_matrix:
                        corr = correlation_matrix[key]
                        break

                if corr is not None and isinstance(corr, (int, float)):
                    if abs(corr) > CORRELATION_RISK_THRESHOLD:
                        correlation_flags.append({
                            "pair": [sid_a, sid_b],
                            "correlation": round(abs(corr), 3),
                            "label_zh": f"高度相關：{sid_a} ↔ {sid_b} (ρ={abs(corr):.2f})",
                        })

    if len(correlation_flags) > 2:
        violations.append({
            "type": "correlation_risk",
            "count": len(correlation_flags),
            "label_zh": f"高相關持股過多：{len(correlation_flags)}對",
        })

    # Check total weight
    total_weight = sum(weights.values())
    if abs(total_weight - 1.0) > 0.001:
        violations.append({
            "type": "total_weight",
            "weight": round(total_weight, 4),
            "label_zh": f"權重總和={total_weight*100:.1f}%（應為100%）",
        })

    valid = len(violations) == 0
    n_violations = len(violations)

    if valid:
        label_zh = "所有約束條件通過"
    else:
        label_zh = f"{n_violations}項約束條件違反"

    return {
        "valid": valid,
        "violations": violations,
        "sector_allocation": {k: round(v, 4) for k, v in sector_allocation.items()},
        "total_weight": round(total_weight, 4),
        "correlation_flags": correlation_flags,
        "label_zh": label_zh,
    }


# ---------------------------------------------------------------------------
# Feature 7: run_portfolio_optimizer (Pipeline Stage 12)
# ---------------------------------------------------------------------------

def run_portfolio_optimizer(date_str: str = None, verbose: bool = False) -> Dict[str, Any]:
    """Main entry point for pipeline Stage 12: Portfolio Optimization.

    Loads ensemble results from Phase 37, computes covariance and expected
    returns, runs mean-variance optimization (primary) with Black-Litterman
    as alternative when views available, applies weight smoothing from
    previous day's weights, validates all constraints, and saves optimized
    portfolio.

    Backward compatible: skips gracefully if ensemble results missing or
    covariance matrix can't be computed (<30 trading days).

    Args:
        date_str: Date string in YYYY-MM-DD format (default: today)
        verbose: Whether to print detailed progress

    Returns:
        Dict with optimized portfolio data (same structure as saved JSON)
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    result = {
        "stage": 12,
        "date": date_str,
        "timestamp": datetime.now().isoformat(),
        "stock_weights": {},
        "sector_allocation": {},
        "expected_return_annual": 0.0,
        "expected_volatility_annual": 0.0,
        "sharpe_ratio": 0.0,
        "constraint_violations": [],
        "smoothing_applied": False,
        "optimization_method": "none",
        "n_stocks": 0,
        "label_zh": "",
    }

    # Step 1: Load ensemble results from Phase 37
    ensemble = load_ensemble_results(date_str)
    if not ensemble.get("loaded"):
        logger.warning("Phase 38: 無法載入整合評分結果，跳過投資組合最佳化")
        result["label_zh"] = f"跳過：{ensemble.get('error', '未知原因')}"
        return result

    candidates = ensemble.get("candidates", [])
    if not candidates:
        result["label_zh"] = "跳過：無候選股票"
        return result

    if verbose:
        print(f"  Phase 38: {len(candidates)} candidates loaded")

    # Step 2: Load price history
    price_history = _load_price_history(date_str)
    if not price_history:
        logger.warning("Phase 38: 無法載入價格歷史，跳過投資組合最佳化")
        result["label_zh"] = "跳過：無價格歷史數據"
        return result

    # Step 3: Get stock IDs from top candidates
    # Limit to top candidates with reasonable ensemble scores
    top_candidates = [c for c in candidates if c.get("ensemble_score", 0) >= 0.35]
    if not top_candidates:
        top_candidates = candidates[:10]  # fallback: top 10
    stock_ids = [c["code"] for c in top_candidates]

    if verbose:
        print(f"  Phase 38: Optimizing for {len(stock_ids)} stocks")

    # Step 4: Compute covariance matrix
    cov_result = compute_covariance_matrix(stock_ids, price_history)
    if not cov_result.get("sufficient_data"):
        logger.warning("Phase 38: 共變異矩陣數據不足，使用等權重")
        # Fall back to equal weight
        equal_w = 1.0 / len(stock_ids) if stock_ids else 0.0
        result["stock_weights"] = {sid: round(equal_w, 4) for sid in stock_ids}
        result["optimization_method"] = "equal_weight"
        result["n_stocks"] = len(stock_ids)
        result["label_zh"] = OPTIMIZATION_LABELS_ZH["equal_weight"]
        _save_portfolio(result, date_str)
        return result

    # Step 5: Build expected returns from ensemble scores
    expected_returns = {}
    for c in top_candidates:
        # Map ensemble_score (0-1) to expected daily return
        # Use a simple linear mapping: score 0.5 → 0% daily, 0.8 → ~0.1% daily
        score = c.get("ensemble_score", 0.5)
        expected_returns[c["code"]] = (score - 0.5) * 0.002  # scale factor

    # Step 6: Load sector map
    sector_map = _load_sector_map()
    constraints = {
        "sector_map": sector_map,
    }

    # Step 7: Run mean-variance optimization
    valid_ids = cov_result["stock_ids"]
    # Filter expected_returns to match valid_ids
    filtered_returns = {sid: expected_returns.get(sid, 0.0) for sid in valid_ids}

    mv_result = optimize_mean_variance(
        filtered_returns,
        cov_result["cov_matrix"],
        valid_ids,
        constraints=constraints,
    )

    # Step 7b: Also try Black-Litterman with ensemble-derived views
    views = {}
    for c in top_candidates:
        if c["code"] in valid_ids:
            score = c.get("ensemble_score", 0.5)
            confidence = c.get("ensemble_score", 0.5)  # Use score as confidence proxy
            views[c["code"]] = {
                "return": (score - 0.5) * 0.002,
                "confidence": min(confidence, 0.8),
            }

    # Load previous weights for BL equilibrium
    prev_weights = _load_previous_weights(date_str)

    bl_result = optimize_black_litterman(
        filtered_returns,
        cov_result["cov_matrix"],
        valid_ids,
        views=views,
        market_cap_weights=prev_weights,
        constraints=constraints,
    )

    # Use BL result if Sharpe is better, otherwise MV
    if bl_result.get("sharpe_ratio", 0) > mv_result.get("sharpe_ratio", 0):
        opt_result = bl_result
    else:
        opt_result = mv_result

    # Step 8: Apply weight smoothing from previous day
    smoothing_result = apply_weight_smoothing(
        prev_weights if prev_weights else {},
        opt_result["weights"],
    )

    final_weights = smoothing_result["smoothed_weights"]

    # Step 9: Validate constraints
    validation = validate_constraints(
        final_weights,
        sector_map=sector_map,
    )

    # Build result
    result["stock_weights"] = final_weights
    result["sector_allocation"] = validation.get("sector_allocation", {})
    result["expected_return_annual"] = opt_result.get("expected_return_annual", 0.0)
    result["expected_volatility_annual"] = opt_result.get("expected_volatility_annual", 0.0)
    result["sharpe_ratio"] = opt_result.get("sharpe_ratio", 0.0)
    result["constraint_violations"] = validation.get("violations", [])
    result["smoothing_applied"] = smoothing_result.get("smoothing_applied", False)
    result["optimization_method"] = opt_result.get("method", "unknown")
    result["n_stocks"] = len([w for w in final_weights.values() if w > 0])

    # Label
    method_label = OPTIMIZATION_LABELS_ZH.get(result["optimization_method"], result["optimization_method"])
    smoothing_label = "（已平滑）" if result["smoothing_applied"] else ""
    result["label_zh"] = (
        f"{method_label}{smoothing_label}：{result['n_stocks']}檔，"
        f"年化報酬{result['expected_return_annual']*100:.2f}%，"
        f"夏普值{result['sharpe_ratio']:.2f}"
    )

    # Step 10: Save
    _save_portfolio(result, date_str)

    if verbose:
        print(f"  Phase 38: {result['label_zh']}")
        if result["constraint_violations"]:
            for v in result["constraint_violations"]:
                print(f"    ⚠ {v.get('label_zh', v)}")

    return result


# ---------------------------------------------------------------------------
# Internal: data loading helpers
# ---------------------------------------------------------------------------

def _load_price_history(date_str: str) -> Dict[str, List[Dict]]:
    """Load price history from SQLite or JSON files.

    Two-tier loading strategy matching stage2_deep.py pattern:
    1. SQLite first (datastore)
    2. JSON fallback (price_history.json)
    """
    price_history = None
    data_dir = _data_dir()

    # Try SQLite first
    db_path = data_dir / "hunter.db"
    if db_path.exists():
        try:
            from datastore import get_daily_history_batch
            # Get list of stock codes from ensemble results
            ensemble = load_ensemble_results(date_str)
            if ensemble.get("loaded"):
                codes = [c["code"] for c in ensemble.get("candidates", [])]
                if codes:
                    price_history = get_daily_history_batch(
                        codes, limit=DEFAULT_LOOKBACK_DAYS + 10,
                        data_dir=str(data_dir),
                    )
        except Exception:
            price_history = None

    # JSON fallback
    if price_history is None:
        history_file = data_dir / "price_history.json"
        if history_file.exists():
            try:
                with open(history_file, "r", encoding="utf-8") as f:
                    price_history = json.load(f)
            except (json.JSONDecodeError, IOError):
                price_history = None

    return price_history or {}


def _load_sector_map() -> Dict[str, str]:
    """Load sector mapping from core/sectors.py."""
    try:
        from sectors import load_sector_mapping
        return load_sector_mapping(data_dir=str(_data_dir()))
    except Exception:
        return {}


def _load_previous_weights(date_str: str) -> Optional[Dict[str, float]]:
    """Load previous day's portfolio weights for smoothing.

    Looks for data/portfolio_weights_YYYY-MM-DD.json from the most recent
    date before date_str. Falls back to data/weights_previous.json.
    """
    data_dir = _data_dir()

    # Try to find most recent portfolio weights file
    weight_files = sorted(data_dir.glob("optimized_portfolio_*.json"))
    # Filter to dates before current date_str
    prev_files = [f for f in weight_files if f.stem.replace("optimized_portfolio_", "") < date_str]

    if prev_files:
        latest = prev_files[-1]
        try:
            with open(latest, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("stock_weights", {})
        except (json.JSONDecodeError, IOError):
            pass

    # Fallback: try weights_previous.json (from Stage 1 smoothing)
    prev_weights_file = data_dir / "weights_previous.json"
    if prev_weights_file.exists():
        try:
            with open(prev_weights_file, "r", encoding="utf-8") as f:
                prev = json.load(f)
            # weights_previous.json has stage1/regime_weights format
            # Return stage1 weights if available
            if "stage1" in prev:
                return prev["stage1"]
        except (json.JSONDecodeError, IOError):
            pass

    return None


def _save_portfolio(result: Dict, date_str: str) -> None:
    """Save optimized portfolio to data/optimized_portfolio_YYYY-MM-DD.json."""
    output_path = _data_dir() / f"optimized_portfolio_{date_str}.json"
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("Phase 38: saved portfolio to %s", output_path)
    except IOError as e:
        logger.warning("Phase 38: failed to save portfolio: %s", e)
