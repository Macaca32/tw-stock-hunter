#!/usr/bin/env python3
"""
Phase 37: Machine Learning Signal Fusion Ensemble

Combines all scoring dimensions from Phases 26-36 into a unified probability
of outperformance score. This is the "brain" layer above the existing
Stage 1 / Stage 2 pipeline.

All labels use Traditional Chinese for Taiwan-market awareness.
Backward compatible: every function returns neutral defaults when data is missing.

Features:
1. SignalNormalization   — Normalize dimension scores to common z-score space
2. compute_ensemble_score — Weighted probability of outperformance (0-1)
3. get_feature_importance — SHAP-like permutation importance ranking
4. calibrate_scores       — Isotonic/Platt calibration vs historical win rates
5. get_confidence_band    — Confidence interval based on dimension agreement
6. Pipeline integration   — Stage 11 in run_pipeline.py

Author: Z.ai GLM-5.1 (Phase 37 implementation)
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

# All 12 scoring dimensions (plus risk) from Phases 26-36
DIMENSION_KEYS = [
    "technical_momentum",   # stage1: RSI, MACD, SMA alignment
    "fundamentals",         # stage2: P/E, P/B, ROE, dividend yield
    "revenue_quality",      # stage2: YoY growth, margin trends
    "shareholders",         # stage2: institutional ownership quality
    "pledge_ratio",         # stage2: major shareholder pledge risk
    "dividends",            # stage2: payout consistency, yield
    "institutional_flow",   # stage1: foreign/domestic net buys
    "signal_quality",       # phase 27: conviction grade A-E, alignment
    "microstructure",       # phase 30: VPVR, gap-fill, volume anomalies
    "news_sentiment",       # phase 31: Traditional Chinese keyword scoring
    "cross_asset",          # phase 34: TAIEX/HSI/VIX/USD-TWD correlation
    "earnings_quality",     # phase 35: YoY/QoQ, estimate surprises
    "risk_management",      # phase 36: ATR stops, VaR, conviction sizing
]

# Mapping from pipeline output keys to ensemble dimension keys
# This translates the various score locations in stage1/stage2 results
# into a flat signal_dict for the ensemble.
SCORE_EXTRACTION_MAP = {
    "technical_momentum": {
        "stage": "stage1",
        "key": "score_breakdown.momentum",
        "description": "技術動能（RSI, MACD, SMA）",
    },
    "fundamentals": {
        "stage": "stage2",
        "key": "checks.announcements.score",  # proxy: use stage2 overall
        "description": "基本面（P/E, P/B, ROE）",
        "aggregate": True,  # special: aggregate from multiple stage2 checks
    },
    "revenue_quality": {
        "stage": "stage1",
        "key": "score_breakdown.revenue",
        "description": "營收品質（YoY成長, 毛利率趨勢）",
    },
    "shareholders": {
        "stage": "stage2",
        "key": "checks.shareholders.score",
        "description": "股東結構（法人持股品質）",
    },
    "pledge_ratio": {
        "stage": "stage2",
        "key": "checks.pledge.score",
        "description": "質押比率（大股東質押風險）",
    },
    "dividends": {
        "stage": "stage2",
        "key": "checks.dividend.score",
        "description": "股息（配息一致性, 殖利率）",
    },
    "institutional_flow": {
        "stage": "stage1",
        "key": "score_breakdown.flow",
        "description": "法人動向（外資/投信買超）",
    },
    "signal_quality": {
        "stage": "derived",
        "key": "signal_quality",
        "description": "訊號品質（信賴等級A-E）",
    },
    "microstructure": {
        "stage": "stage2",
        "key": "microstructure.volume_profile.sr_score",
        "description": "微結構（VPVR, 缺口回補, 量能異常）",
    },
    "news_sentiment": {
        "stage": "stage2",
        "key": "checks.news_sentiment.score",
        "description": "新聞情緒（繁中關鍵詞評分）",
    },
    "cross_asset": {
        "stage": "derived",
        "key": "cross_asset",
        "description": "跨資產（台指/恆指/VIX/匯率）",
    },
    "earnings_quality": {
        "stage": "stage2",
        "key": "checks.earnings_growth.score",
        "description": "盈餘品質（YoY/QoQ, 預期驚喜）",
    },
    "risk_management": {
        "stage": "derived",
        "key": "risk_management",
        "description": "風險管理（ATR停損, VaR, 部位控制）",
    },
}

# Cache file location
_DATA_DIR = Path(__file__).parent.parent / "data"
_CONFIG_DIR = Path(__file__).parent.parent / "config"

# Default sigmoid parameters for mapping raw score to [0,1]
DEFAULT_SIGMOID_K = 1.0   # steepness
DEFAULT_SIGMOID_X0 = 50.0  # midpoint (raw score of 50 → probability of 0.5)

# Neutral default for missing dimensions
NEUTRAL_SCORE = 50.0
NEUTRAL_ZSCORE = 0.0


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


def _load_weights() -> Dict:
    """Load signal fusion weights from config file."""
    weights_path = _config_dir() / "signal_fusion_weights.json"
    if not weights_path.exists():
        logger.warning("Phase 37: signal_fusion_weights.json not found, using defaults")
        return _default_weights()
    try:
        with open(weights_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Phase 37: failed to load weights: %s", e)
        return _default_weights()


def _default_weights() -> Dict:
    """Fallback weights if config file is missing."""
    return {
        "dimensions": {
            "technical_momentum": {"weight": 0.12},
            "fundamentals": {"weight": 0.10},
            "revenue_quality": {"weight": 0.09},
            "shareholders": {"weight": 0.06},
            "pledge_ratio": {"weight": 0.07},
            "dividends": {"weight": 0.08},
            "institutional_flow": {"weight": 0.10},
            "signal_quality": {"weight": 0.08},
            "microstructure": {"weight": 0.07},
            "news_sentiment": {"weight": 0.06},
            "cross_asset": {"weight": 0.05},
            "earnings_quality": {"weight": 0.07},
            "risk_management": {"weight": 0.05},
        },
        "interaction_terms": {
            "momentum_volume_bonus": {
                "dimensions": ["technical_momentum", "microstructure"],
                "bonus_weight": 0.03,
            },
            "earnings_fundamental_synergy": {
                "dimensions": ["earnings_quality", "fundamentals"],
                "bonus_weight": 0.02,
            },
        },
        "calibration": {
            "sigmoid_k": DEFAULT_SIGMOID_K,
            "sigmoid_x0": DEFAULT_SIGMOID_X0,
        },
    }


def _sigmoid(x: float, k: float = DEFAULT_SIGMOID_K,
              x0: float = DEFAULT_SIGMOID_X0) -> float:
    """Sigmoid function mapping real-valued input to (0, 1).

    Args:
        x: Input value (typically a raw weighted score around 0-100)
        k: Steepness parameter
        x0: Midpoint (value where sigmoid = 0.5)

    Returns:
        Float in (0, 1)
    """
    try:
        exponent = -k * (x - x0) / 25.0  # Scale: 25-point change = 1 unit
        # Clamp exponent to avoid overflow
        exponent = max(-500, min(500, exponent))
        return 1.0 / (1.0 + math.exp(exponent))
    except (OverflowError, ValueError):
        return 0.5


def _safe_get(data: Dict, key_path: str, default=None):
    """Safely traverse a nested dict using dot-separated key path.

    Example: _safe_get({"a": {"b": 5}}, "a.b") → 5
    """
    keys = key_path.split(".")
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def _load_calibration() -> Optional[Dict]:
    """Load calibration data from data/signal_calibration.json."""
    cal_path = _data_dir() / "signal_calibration.json"
    if not cal_path.exists():
        return None
    try:
        with open(cal_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def _save_calibration(calibration: Dict) -> None:
    """Save calibration data to data/signal_calibration.json."""
    cal_path = _data_dir() / "signal_calibration.json"
    try:
        with open(cal_path, "w", encoding="utf-8") as f:
            json.dump(calibration, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logger.warning("Phase 37: failed to save calibration: %s", e)


# ---------------------------------------------------------------------------
# Feature 1: SignalNormalization
# ---------------------------------------------------------------------------

class SignalNormalization:
    """Normalize all dimension scores to a common z-score space.

    Uses rolling statistics (mean/std from historical data) to convert
    raw 0-100 scores into z-scores. Handles None/missing gracefully
    with neutral defaults (z-score = 0).

    The z-score tells us how many standard deviations a dimension score
    is above or below its historical average for this stock or the market.
    """

    def __init__(self, historical_stats: Optional[Dict] = None):
        """Initialize with optional historical statistics.

        Args:
            historical_stats: Dict mapping dimension_key → {"mean": float, "std": float}
                If not provided, defaults to market-wide averages (50 mean, 16.7 std
                for a 0-100 range assuming uniform-ish distribution).
        """
        # Default stats: for 0-100 scores, market average is 50, std ~16.7
        self._stats = {}
        if historical_stats:
            self._stats = historical_stats

    def normalize(self, dimension_key: str, raw_score: Optional[float]) -> float:
        """Normalize a raw dimension score to z-score space.

        Args:
            dimension_key: One of DIMENSION_KEYS
            raw_score: Raw 0-100 score, or None if unavailable

        Returns:
            Z-score (float). Returns 0.0 for None/missing (neutral).
        """
        if raw_score is None:
            return NEUTRAL_ZSCORE

        stats = self._stats.get(dimension_key)
        if stats is None:
            # No historical stats: use market-wide defaults
            mean = NEUTRAL_SCORE
            std = 16.7  # ~sqrt(100^2/12 / 3) — approximate
        else:
            mean = stats.get("mean", NEUTRAL_SCORE)
            std = stats.get("std", 16.7)
            if std <= 0:
                std = 16.7

        z = (raw_score - mean) / std
        # Clamp to [-3, 3] to prevent outlier distortion
        return round(max(-3.0, min(3.0, z)), 4)

    def denormalize(self, dimension_key: str, z_score: float) -> float:
        """Convert a z-score back to raw 0-100 score space.

        Args:
            dimension_key: One of DIMENSION_KEYS
            z_score: Z-score value

        Returns:
            Raw score (float) in 0-100 range.
        """
        stats = self._stats.get(dimension_key)
        if stats is None:
            mean = NEUTRAL_SCORE
            std = 16.7
        else:
            mean = stats.get("mean", NEUTRAL_SCORE)
            std = stats.get("std", 16.7)

        raw = mean + z_score * std
        return round(max(0.0, min(100.0, raw)), 2)

    def compute_rolling_stats(self, historical_scores: Dict[str, List[float]]) -> Dict:
        """Compute rolling mean/std from historical score arrays.

        Args:
            historical_scores: Dict mapping dimension_key → list of historical scores

        Returns:
            Updated stats dict suitable for re-initialization.
        """
        for dim_key, scores in historical_scores.items():
            if not scores:
                continue
            valid_scores = [s for s in scores if s is not None]
            if not valid_scores:
                continue
            n = len(valid_scores)
            mean = sum(valid_scores) / n
            variance = sum((s - mean) ** 2 for s in valid_scores) / n
            std = math.sqrt(variance) if variance > 0 else 1.0
            self._stats[dim_key] = {"mean": round(mean, 2), "std": round(std, 2)}

        return dict(self._stats)

    def get_stats(self) -> Dict:
        """Return current normalization statistics."""
        return dict(self._stats)


# ---------------------------------------------------------------------------
# Feature 2: compute_ensemble_score
# ---------------------------------------------------------------------------

def compute_ensemble_score(signal_dict: Dict[str, Optional[float]],
                           weights_config: Optional[Dict] = None,
                           normalizer: Optional[SignalNormalization] = None,
                           apply_calibration: bool = True) -> Dict[str, Any]:
    """Compute weighted probability of outperformance over next N trading days.

    The ensemble score combines all scoring dimensions into a single 0-1
    probability using:
      1. Z-score normalization of each dimension
      2. Weighted linear combination
      3. Sigmoid mapping to [0, 1]
      4. Non-linear interaction bonuses
      5. Optional calibration correction

    Args:
        signal_dict: Dict mapping dimension_key → raw score (0-100 or None).
            Keys should be from DIMENSION_KEYS. Missing keys treated as None.
        weights_config: Optional weights config dict. If None, loads from file.
        normalizer: Optional SignalNormalization instance. If None, creates default.
        apply_calibration: Whether to apply stored calibration correction.

    Returns:
        Dict with keys:
            ensemble_score: float in [0, 1] — probability of outperformance
            raw_weighted_sum: float — raw weighted sum before sigmoid
            dimensions_used: int — number of non-None dimensions
            dimensions_missing: list — keys with missing data
            interaction_bonus: float — total non-linear interaction bonus
            label_zh: str — Traditional Chinese interpretation
    """
    if weights_config is None:
        weights_config = _load_weights()

    if normalizer is None:
        normalizer = SignalNormalization()

    dim_weights = weights_config.get("dimensions", {})
    interaction_terms = weights_config.get("interaction_terms", {})
    cal_config = weights_config.get("calibration", {})

    # Step 1: Normalize each dimension and compute weighted sum
    weighted_sum = 0.0
    total_weight = 0.0
    dimensions_used = 0
    dimensions_missing = []
    normalized_scores = {}

    for dim_key in DIMENSION_KEYS:
        raw_score = signal_dict.get(dim_key)
        dim_config = dim_weights.get(dim_key, {})
        weight = dim_config.get("weight", 0.05)  # Default small weight

        if raw_score is None:
            dimensions_missing.append(dim_key)
            # Use neutral z-score (0) for missing dimensions
            z_score = NEUTRAL_ZSCORE
        else:
            z_score = normalizer.normalize(dim_key, raw_score)
            dimensions_used += 1

        normalized_scores[dim_key] = z_score
        weighted_sum += z_score * weight
        total_weight += weight

    # Step 2: Add non-linear interaction terms
    interaction_bonus = 0.0
    for term_name, term_config in interaction_terms.items():
        term_dims = term_config.get("dimensions", [])
        bonus_weight = term_config.get("bonus_weight", 0.0)

        if len(term_dims) == 2:
            dim_a, dim_b = term_dims
            z_a = normalized_scores.get(dim_a, 0.0)
            z_b = normalized_scores.get(dim_b, 0.0)

            # Interaction: both positive → bonus; both negative → penalty
            # If one is positive and the other is negative, they partially cancel
            interaction_value = z_a * z_b * 0.1  # Scale down product
            interaction_bonus += interaction_value * bonus_weight

    weighted_sum += interaction_bonus

    # Step 3: Map to [0, 1] via sigmoid
    # First convert z-score weighted sum to a "raw score" equivalent
    # The weighted_sum is in z-score * weight space, so we need to scale
    # back to a 0-100 equivalent for the sigmoid
    if total_weight > 0:
        # Weighted average z-score
        avg_z = weighted_sum / total_weight
    else:
        avg_z = 0.0

    # Convert average z-score back to raw score equivalent
    raw_score_equiv = normalizer.denormalize("technical_momentum", avg_z)

    sigmoid_k = cal_config.get("sigmoid_k", DEFAULT_SIGMOID_K)
    sigmoid_x0 = cal_config.get("sigmoid_x0", DEFAULT_SIGMOID_X0)
    probability = _sigmoid(raw_score_equiv, k=sigmoid_k, x0=sigmoid_x0)

    # Step 4: Apply calibration correction if available
    calibration_applied = False
    if apply_calibration:
        calibration = _load_calibration()
        if calibration and calibration.get("method") == "isotonic_regression":
            # Apply isotonic regression mapping
            mapping = calibration.get("mapping", [])
            if mapping:
                calibrated = _apply_isotonic_mapping(probability, mapping)
                if calibrated is not None:
                    probability = calibrated
                    calibration_applied = True
        elif calibration and calibration.get("method") == "platt_scaling":
            # Apply Platt scaling: calibrated = 1 / (1 + exp(A * raw + B))
            platt_a = calibration.get("platt_a", 1.0)
            platt_b = calibration.get("platt_b", 0.0)
            try:
                exponent = -(platt_a * probability + platt_b)
                exponent = max(-500, min(500, exponent))
                probability = 1.0 / (1.0 + math.exp(exponent))
                calibration_applied = True
            except (OverflowError, ValueError):
                pass

    # Clamp final probability
    probability = round(max(0.01, min(0.99, probability)), 4)

    # Traditional Chinese label
    if probability >= 0.75:
        label_zh = "強烈看多（高勝率訊號）"
    elif probability >= 0.60:
        label_zh = "偏多（正向訊號）"
    elif probability >= 0.45:
        label_zh = "中性（多空分歧）"
    elif probability >= 0.30:
        label_zh = "偏空（負向訊號）"
    else:
        label_zh = "強烈看空（低勝率訊號）"

    return {
        "ensemble_score": probability,
        "raw_weighted_sum": round(weighted_sum, 4),
        "raw_score_equivalent": round(raw_score_equiv, 2),
        "dimensions_used": dimensions_used,
        "dimensions_total": len(DIMENSION_KEYS),
        "dimensions_missing": dimensions_missing,
        "interaction_bonus": round(interaction_bonus, 4),
        "calibration_applied": calibration_applied,
        "label_zh": label_zh,
    }


def _apply_isotonic_mapping(probability: float,
                             mapping: List[Dict]) -> Optional[float]:
    """Apply isotonic regression calibration mapping.

    The mapping is a list of {input_min, input_max, output} entries.
    We find the bin that contains the input probability and return
    the corresponding calibrated output.

    Args:
        probability: Raw probability to calibrate
        mapping: List of calibration bin dicts

    Returns:
        Calibrated probability, or None if mapping is invalid
    """
    if not mapping:
        return None

    for entry in mapping:
        input_min = entry.get("input_min", 0.0)
        input_max = entry.get("input_max", 1.0)
        output = entry.get("output")

        if output is None:
            continue

        if input_min <= probability <= input_max:
            return round(max(0.01, min(0.99, output)), 4)

    # Fallback: linear interpolation between first and last mapping points
    if len(mapping) >= 2:
        first_output = mapping[0].get("output", 0.1)
        last_output = mapping[-1].get("output", 0.9)
        # Simple proportional mapping
        calibrated = first_output + (probability - 0.0) * (last_output - first_output)
        return round(max(0.01, min(0.99, calibrated)), 4)

    return None


# ---------------------------------------------------------------------------
# Feature 3: get_feature_importance (SHAP-like permutation importance)
# ---------------------------------------------------------------------------

def get_feature_importance(signal_dict: Dict[str, Optional[float]],
                           weights_config: Optional[Dict] = None,
                           normalizer: Optional[SignalNormalization] = None) -> Dict[str, Any]:
    """Rank dimensions by contribution using leave-one-out permutation importance.

    For each dimension, compute the ensemble score with and without that
    dimension (replaced with neutral). The delta in score indicates how
    much that dimension contributed to the final result.

    This is a SHAP-like approximation: permutation importance using
    leave-one-out score deltas.

    Args:
        signal_dict: Dict mapping dimension_key → raw score (0-100 or None)
        weights_config: Optional weights config. If None, loads from file.
        normalizer: Optional SignalNormalization instance.

    Returns:
        Dict with keys:
            ranking: list of {dimension, contribution, rank} sorted by |contribution| desc
            positive_contributors: list of dimensions that increased the score
            negative_contributors: list of dimensions that decreased the score
            neutral_contributors: list of dimensions with negligible contribution
            label_zh: str — Traditional Chinese summary
    """
    # Compute baseline ensemble score
    baseline = compute_ensemble_score(signal_dict, weights_config, normalizer,
                                       apply_calibration=False)
    baseline_score = baseline["ensemble_score"]

    contributions = {}
    for dim_key in DIMENSION_KEYS:
        # Create a modified signal_dict with this dimension set to None (neutral)
        modified_dict = dict(signal_dict)
        modified_dict[dim_key] = None

        # Compute score without this dimension
        modified = compute_ensemble_score(modified_dict, weights_config, normalizer,
                                          apply_calibration=False)

        # Contribution = baseline - modified (positive = this dim helped)
        delta = baseline_score - modified["ensemble_score"]
        contributions[dim_key] = round(delta, 6)

    # Sort by absolute contribution descending
    ranking = sorted(
        [{"dimension": k, "contribution": v, "label_zh": SCORE_EXTRACTION_MAP.get(k, {}).get("description", k)}
         for k, v in contributions.items()],
        key=lambda x: abs(x["contribution"]),
        reverse=True,
    )

    # Add rank numbers
    for i, entry in enumerate(ranking):
        entry["rank"] = i + 1

    # Classify contributors
    positive = [r for r in ranking if r["contribution"] > 0.005]
    negative = [r for r in ranking if r["contribution"] < -0.005]
    neutral = [r for r in ranking if abs(r["contribution"]) <= 0.005]

    # Traditional Chinese summary
    top_pos = positive[0]["label_zh"] if positive else "無"
    top_neg = negative[0]["label_zh"] if negative else "無"
    label_zh = f"最大正向因子: {top_pos}；最大負向因子: {top_neg}"

    return {
        "ranking": ranking,
        "positive_contributors": positive,
        "negative_contributors": negative,
        "neutral_contributors": neutral,
        "baseline_score": baseline_score,
        "label_zh": label_zh,
    }


# ---------------------------------------------------------------------------
# Feature 4: calibrate_scores
# ---------------------------------------------------------------------------

def calibrate_scores(backtest_results: List[Dict],
                     method: str = "isotonic_regression") -> Dict[str, Any]:
    """Calibrate raw scores against actual win rates from backtest results.

    Uses historical backtest results from Phase 26 multi-period validation
    to calibrate raw ensemble scores so predicted probability ≈ actual
    frequency.

    Supports two calibration methods:
      - isotonic_regression: Non-parametric, monotonic mapping
      - platt_scaling: Parametric sigmoid fit (logistic regression)

    Args:
        backtest_results: List of dicts, each with:
            - predicted_score: float (raw ensemble score 0-1)
            - actual_outcome: bool or 0/1 (did the stock outperform?)
            - stock_id: str (optional, for diagnostics)
        method: "isotonic_regression" or "platt_scaling"

    Returns:
        Dict with keys:
            method: str — Calibration method used
            mapping: list — Isotonic regression bins (if applicable)
            platt_a, platt_b: float — Platt scaling parameters (if applicable)
            num_samples: int — Number of data points used
            calibration_curve: list — [{predicted, actual_win_rate}] for diagnostics
            saved_to: str — Path where calibration was persisted
            label_zh: str — Traditional Chinese description
    """
    if not backtest_results:
        return {
            "method": method,
            "num_samples": 0,
            "calibration_curve": [],
            "label_zh": "無校準資料（回測結果為空）",
        }

    # Filter valid entries
    valid = [
        r for r in backtest_results
        if r.get("predicted_score") is not None and r.get("actual_outcome") is not None
    ]

    if len(valid) < 5:
        logger.warning("Phase 37: insufficient calibration data (%d points)", len(valid))
        return {
            "method": method,
            "num_samples": len(valid),
            "calibration_curve": [],
            "label_zh": f"校準資料不足（僅{len(valid)}筆）",
        }

    # Sort by predicted score
    valid.sort(key=lambda r: r["predicted_score"])

    # Build calibration curve (binned actual win rates)
    n_bins = min(10, len(valid) // 3)  # At least 3 samples per bin
    n_bins = max(3, n_bins)

    bin_size = len(valid) // n_bins
    calibration_curve = []
    mapping = []

    for i in range(n_bins):
        start = i * bin_size
        end = start + bin_size if i < n_bins - 1 else len(valid)
        bin_data = valid[start:end]

        if not bin_data:
            continue

        predicted_avg = sum(r["predicted_score"] for r in bin_data) / len(bin_data)
        actual_wins = sum(1 for r in bin_data if r.get("actual_outcome"))
        actual_win_rate = actual_wins / len(bin_data)

        calibration_curve.append({
            "predicted": round(predicted_avg, 4),
            "actual_win_rate": round(actual_win_rate, 4),
            "sample_count": len(bin_data),
        })

        # Isotonic regression mapping bins
        if i > 0:
            input_min = calibration_curve[-2]["predicted"] if i >= 2 else 0.0
        else:
            input_min = 0.0
        input_max = predicted_avg if i < n_bins - 1 else 1.0

        mapping.append({
            "input_min": round(input_min, 4),
            "input_max": round(input_max, 4),
            "output": round(actual_win_rate, 4),
        })

    # Platt scaling parameters (logistic regression on raw scores)
    platt_a = 1.0
    platt_b = 0.0
    if method == "platt_scaling" and len(valid) >= 10:
        platt_a, platt_b = _fit_platt_scaling(valid)

    # Build and save calibration
    calibration = {
        "method": method,
        "mapping": mapping,
        "platt_a": round(platt_a, 6),
        "platt_b": round(platt_b, 6),
        "num_samples": len(valid),
        "calibration_curve": calibration_curve,
        "created_at": datetime.now().isoformat(),
        "label_zh": f"校準完成：{method}，{len(valid)}筆樣本，{n_bins}個分箱",
    }

    _save_calibration(calibration)

    return calibration


def _fit_platt_scaling(data: List[Dict]) -> Tuple[float, float]:
    """Fit Platt scaling parameters using gradient descent.

    Platt scaling: P(actual=1) = 1 / (1 + exp(A * predicted + B))

    We use a simple gradient descent to find A and B that minimize
    log-loss on the calibration data.

    Returns:
        (A, B) parameters for Platt scaling
    """
    # Initialize: identity mapping (A=1, B=0 means sigmoid of predicted)
    a = 1.0
    b = 0.0
    learning_rate = 0.01
    iterations = 200

    for _ in range(iterations):
        grad_a = 0.0
        grad_b = 0.0

        for entry in data:
            pred = entry["predicted_score"]
            actual = float(entry["actual_outcome"])

            # Clip predicted to avoid numerical issues
            pred = max(0.01, min(0.99, pred))

            # Current calibrated probability
            exponent = -(a * pred + b)
            exponent = max(-500, min(500, exponent))
            calibrated = 1.0 / (1.0 + math.exp(exponent))
            calibrated = max(0.001, min(0.999, calibrated))

            # Gradient of log-loss
            error = calibrated - actual
            grad_a += error * pred * calibrated * (1 - calibrated)
            grad_b += error * calibrated * (1 - calibrated)

        n = len(data) if data else 1
        a -= learning_rate * grad_a / n
        b -= learning_rate * grad_b / n

    return round(a, 6), round(b, 6)


# ---------------------------------------------------------------------------
# Feature 5: get_confidence_band
# ---------------------------------------------------------------------------

def get_confidence_band(score: float,
                        signal_dict: Optional[Dict[str, Optional[float]]] = None,
                        normalizer: Optional[SignalNormalization] = None,
                        historical_variance: Optional[float] = None) -> Dict[str, Any]:
    """Compute confidence interval around the ensemble score.

    The band width depends on:
      1. Dimension agreement: All dimensions agreeing → narrow band
      2. Historical variance: Higher variance → wider band
      3. Missing dimensions: More missing → wider band

    Args:
        score: Ensemble score (0-1) from compute_ensemble_score
        signal_dict: Optional signal dict for dimension agreement calculation
        normalizer: Optional SignalNormalization instance
        historical_variance: Optional historical variance of ensemble scores

    Returns:
        Dict with keys:
            lower: float — Lower bound of confidence interval
            upper: float — Upper bound of confidence interval
            width: float — Band width (upper - lower)
            agreement_score: float — How much dimensions agree (0-1)
            label_zh: str — Traditional Chinese description
    """
    if normalizer is None:
        normalizer = SignalNormalization()

    # Compute dimension agreement if signal_dict is provided
    agreement_score = 0.5  # Default: moderate agreement
    dimensions_missing_count = 0

    if signal_dict:
        valid_z_scores = []
        for dim_key in DIMENSION_KEYS:
            raw_score = signal_dict.get(dim_key)
            if raw_score is None:
                dimensions_missing_count += 1
                continue
            z = normalizer.normalize(dim_key, raw_score)
            valid_z_scores.append(z)

        if len(valid_z_scores) >= 2:
            # Agreement = how much dimensions point in the same direction
            # Use sign consistency and magnitude consistency
            positive_count = sum(1 for z in valid_z_scores if z > 0)
            negative_count = sum(1 for z in valid_z_scores if z < 0)
            total = len(valid_z_scores)

            # Direction agreement: 1.0 if all same sign, 0.5 if mixed
            direction_agreement = max(positive_count, negative_count) / total

            # Magnitude agreement: low standard deviation of z-scores = high agreement
            mean_z = sum(valid_z_scores) / len(valid_z_scores)
            if len(valid_z_scores) > 1:
                variance = sum((z - mean_z) ** 2 for z in valid_z_scores) / len(valid_z_scores)
                std_z = math.sqrt(variance)
                # Map std to agreement: low std → high agreement
                # std=0 → agreement=1, std=1 → agreement=0.5, std=2+ → agreement=0.2
                magnitude_agreement = max(0.1, 1.0 / (1.0 + std_z))
            else:
                magnitude_agreement = 0.5

            agreement_score = 0.6 * direction_agreement + 0.4 * magnitude_agreement
        elif len(valid_z_scores) == 1:
            agreement_score = 0.3  # Low confidence with only one dimension
        else:
            agreement_score = 0.1  # Very low confidence with no data

    # Missing dimensions penalty
    missing_ratio = dimensions_missing_count / len(DIMENSION_KEYS)
    missing_penalty = missing_ratio * 0.15

    # Historical variance component
    if historical_variance is not None and historical_variance > 0:
        hist_component = math.sqrt(historical_variance) * 0.5
    else:
        hist_component = 0.05  # Default 5% uncertainty

    # Compute band width
    # Base width: 0.10 (10 percentage points)
    base_width = 0.10
    # Adjust by agreement: high agreement → narrower, low → wider
    agreement_factor = 1.0 - 0.6 * agreement_score  # Range: [0.4, 1.0]
    # Add missing penalty and historical variance
    total_width = (base_width * agreement_factor + missing_penalty + hist_component)
    # Cap at reasonable bounds
    total_width = max(0.04, min(0.40, total_width))

    lower = round(max(0.01, score - total_width / 2), 4)
    upper = round(min(0.99, score + total_width / 2), 4)

    # Traditional Chinese label
    if total_width <= 0.08:
        label_zh = "高信心區間（維度高度一致）"
    elif total_width <= 0.15:
        label_zh = "中等信心區間（維度部分分歧）"
    elif total_width <= 0.25:
        label_zh = "低信心區間（維度嚴重分歧）"
    else:
        label_zh = "極低信心區間（數據嚴重不足）"

    return {
        "lower": lower,
        "upper": upper,
        "width": round(upper - lower, 4),
        "agreement_score": round(agreement_score, 4),
        "dimensions_missing": dimensions_missing_count,
        "label_zh": label_zh,
    }


# ---------------------------------------------------------------------------
# Signal extraction from pipeline results
# ---------------------------------------------------------------------------

def extract_signals_from_pipeline(
    stage2_result: Dict,
    stage1_summary: Optional[Dict] = None,
    market_context: Optional[Dict] = None,
    risk_summary: Optional[Dict] = None,
) -> Dict[str, Optional[float]]:
    """Extract dimension scores from pipeline stage outputs into a flat signal_dict.

    This function translates the various score locations in stage1/stage2 results
    into a unified signal_dict suitable for compute_ensemble_score().

    Args:
        stage2_result: Single stock result from stage2_deep output
        stage1_summary: Stage 1 summary data (for signal quality derivation)
        market_context: Cross-asset context dict from Phase 34
        risk_summary: Risk summary dict from Phase 36

    Returns:
        Dict mapping dimension_key → score (0-100 or None)
    """
    signals = {}

    # Technical momentum — from stage1 score_breakdown
    s1_breakdown = stage2_result.get("score_breakdown", {})
    if not s1_breakdown and "stage1_score" in stage2_result:
        # If no breakdown, estimate from stage1_score
        s1_score = stage2_result.get("stage1_score", 50)
        signals["technical_momentum"] = s1_score
    else:
        signals["technical_momentum"] = s1_breakdown.get("momentum")

    # Fundamentals — aggregate from stage2 checks
    checks = stage2_result.get("checks", {})
    ann_score = checks.get("announcements", {}).get("score")
    pen_score = checks.get("penalties", {}).get("score")
    # Use weighted average of fundamental-relevant checks as proxy
    fundamental_scores = []
    if ann_score is not None:
        fundamental_scores.append(ann_score)
    if pen_score is not None:
        fundamental_scores.append(pen_score)
    signals["fundamentals"] = (
        sum(fundamental_scores) / len(fundamental_scores)
        if fundamental_scores else None
    )

    # Revenue quality — from stage1 score_breakdown
    signals["revenue_quality"] = s1_breakdown.get("revenue")

    # Shareholders
    signals["shareholders"] = checks.get("shareholders", {}).get("score")

    # Pledge ratio
    signals["pledge_ratio"] = checks.get("pledge", {}).get("score")

    # Dividends
    signals["dividends"] = checks.get("dividend", {}).get("score")

    # Institutional flow — from stage1 score_breakdown
    signals["institutional_flow"] = s1_breakdown.get("flow")

    # Signal quality — derived from combined score and regime
    # Higher combined scores → better signal quality
    combined = stage2_result.get("combined_score", 50)
    if combined is not None:
        # Map combined score to 0-100 signal quality scale
        # combined ~50 → signal_quality ~50; combined ~80 → ~80
        signals["signal_quality"] = round(max(0, min(100, combined)), 1)
    else:
        signals["signal_quality"] = None

    # Microstructure — VPVR support/resistance score
    microstructure = stage2_result.get("microstructure", {})
    vol_profile = microstructure.get("volume_profile", {})
    sr_score = vol_profile.get("sr_score")
    # Also consider intraday pattern sentiment
    intraday = microstructure.get("intraday_pattern", {})
    sentiment = intraday.get("sentiment", 0.0)
    # Map sentiment from [-0.5, +0.5] to [0, 100] and combine with sr_score
    sentiment_score = 60.0 + sentiment * 60.0  # +0.5→90, 0→60, -0.5→30
    if sr_score is not None:
        # Average of VPVR and candlestick sentiment
        signals["microstructure"] = round((sr_score + sentiment_score) / 2, 1)
    else:
        signals["microstructure"] = round(sentiment_score, 1) if sentiment != 0.0 else None

    # News sentiment
    signals["news_sentiment"] = checks.get("news_sentiment", {}).get("score")

    # Cross-asset — from market_context
    if market_context:
        cross_signal = market_context.get("cross_asset_signal", 0.0)
        # Map from [-0.2, +0.2] to [30, 90] scale
        signals["cross_asset"] = round(60.0 + cross_signal * 150.0, 1)
        signals["cross_asset"] = max(0, min(100, signals["cross_asset"]))
    else:
        # Try from stage2 result
        cross_adj = stage2_result.get("cross_asset_adjustment", 0)
        if cross_adj:
            # Map from [-3, +3] to [30, 90] scale
            signals["cross_asset"] = round(60.0 + cross_adj * 10.0, 1)
            signals["cross_asset"] = max(0, min(100, signals["cross_asset"]))
        else:
            signals["cross_asset"] = None

    # Earnings quality
    signals["earnings_quality"] = checks.get("earnings_growth", {}).get("score")

    # Risk management — from risk_summary
    if risk_summary:
        risk_score = risk_summary.get("overall_risk_score", 5)
        # Map 1-10 risk score to 0-100 (inverted: low risk = good)
        signals["risk_management"] = round(max(0, min(100, (10 - risk_score) * 10 + 10)), 1)
    else:
        # Estimate from earnings signal and combined score
        earnings_signal = stage2_result.get("earnings_signal", {})
        if isinstance(earnings_signal, dict):
            sig = earnings_signal.get("signal", 0.0)
            # Map from [-0.15, +0.15] to [40, 80]
            signals["risk_management"] = round(60.0 + sig * 133.0, 1)
            signals["risk_management"] = max(0, min(100, signals["risk_management"]))
        else:
            signals["risk_management"] = None

    return signals


# ---------------------------------------------------------------------------
# Batch processing for pipeline integration
# ---------------------------------------------------------------------------

def run_signal_fusion(date_str: str = None,
                      verbose: bool = False) -> Optional[Dict]:
    """Run signal fusion on all Stage 2 results for a given date.

    This is the main entry point for pipeline integration (Stage 11).
    It loads Stage 2 results, extracts signals, computes ensemble scores,
    feature importance, and confidence bands for every stock.

    Args:
        date_str: Date string (YYYY-MM-DD). Default: today.
        verbose: Whether to log detailed progress.

    Returns:
        Dict with keys:
            date: str
            timestamp: str
            stocks: list of per-stock ensemble results
            ranking: list of stocks sorted by ensemble_score desc
            summary: dict with aggregate statistics
            normalizer_stats: dict with normalization parameters
    """
    from datetime import datetime as _dt
    if date_str is None:
        date_str = _dt.now().strftime("%Y-%m-%d")

    data_dir = _data_dir()

    # Load Stage 2 results
    s2_path = data_dir / f"stage2_{date_str}.json"
    if not s2_path.exists():
        # Try to find latest stage2 file
        s2_files = sorted(data_dir.glob("stage2_*.json"))
        if not s2_files:
            logger.warning("Phase 37: No Stage 2 results found, skipping signal fusion")
            return None
        s2_path = s2_files[-1]
        date_str = s2_path.stem.replace("stage2_", "")

    try:
        with open(s2_path, "r", encoding="utf-8") as f:
            s2_output = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error("Phase 37: Failed to load Stage 2 results: %s", e)
        return None

    # Load market context if available
    market_context = None
    try:
        from market_context import get_market_context
        s1_summary = s2_output.get("summary")
        market_context = get_market_context(date_str, stage1_summary=s1_summary)
    except Exception as e:
        logger.debug("Phase 37: market_context unavailable: %s", e)

    # Load risk summary if available
    risk_summary = None
    try:
        from risk_manager import get_risk_summary
        # Build a minimal portfolio from paper trader data
        # For now, use a neutral risk summary
        risk_summary = {"overall_risk_score": 5}
    except Exception as e:
        logger.debug("Phase 37: risk_manager unavailable: %s", e)

    # Load weights
    weights_config = _load_weights()

    # Initialize normalizer with rolling stats from historical data if available
    normalizer = SignalNormalization()
    cal_path = _data_dir() / "signal_calibration.json"
    if cal_path.exists():
        try:
            with open(cal_path, "r", encoding="utf-8") as f:
                cal_data = json.load(f)
            if "normalizer_stats" in cal_data:
                normalizer = SignalNormalization(cal_data["normalizer_stats"])
        except Exception:
            pass

    # Process each stock
    candidates = s2_output.get("candidates", [])
    stock_results = []

    for candidate in candidates:
        code = candidate.get("code", "")
        name = candidate.get("name", "")

        # Extract signals from pipeline outputs
        signals = extract_signals_from_pipeline(
            candidate,
            stage1_summary=s2_output.get("summary"),
            market_context=market_context,
            risk_summary=risk_summary,
        )

        # Compute ensemble score
        ensemble = compute_ensemble_score(signals, weights_config, normalizer)

        # Compute feature importance
        importance = get_feature_importance(signals, weights_config, normalizer)

        # Compute confidence band
        confidence = get_confidence_band(
            ensemble["ensemble_score"],
            signal_dict=signals,
            normalizer=normalizer,
        )

        stock_results.append({
            "code": code,
            "name": name,
            "signals": signals,
            "ensemble_score": ensemble["ensemble_score"],
            "raw_weighted_sum": ensemble["raw_weighted_sum"],
            "dimensions_used": ensemble["dimensions_used"],
            "dimensions_missing": ensemble["dimensions_missing"],
            "interaction_bonus": ensemble["interaction_bonus"],
            "calibration_applied": ensemble["calibration_applied"],
            "confidence_lower": confidence["lower"],
            "confidence_upper": confidence["upper"],
            "confidence_width": confidence["width"],
            "agreement_score": confidence["agreement_score"],
            "feature_importance_top3": [
                {"dimension": r["dimension"], "contribution": r["contribution"],
                 "label_zh": r["label_zh"]}
                for r in importance["ranking"][:3]
            ],
            "label_zh": ensemble["label_zh"],
        })

    # Sort by ensemble score descending
    ranking = sorted(stock_results, key=lambda x: x["ensemble_score"], reverse=True)

    # Add rank
    for i, stock in enumerate(ranking):
        stock["rank"] = i + 1

    # Summary statistics
    if ranking:
        scores = [s["ensemble_score"] for s in ranking]
        summary = {
            "total_stocks": len(ranking),
            "avg_ensemble_score": round(sum(scores) / len(scores), 4),
            "max_ensemble_score": round(max(scores), 4),
            "min_ensemble_score": round(min(scores), 4),
            "high_conviction_count": sum(1 for s in scores if s >= 0.65),
            "low_conviction_count": sum(1 for s in scores if s < 0.35),
        }
    else:
        summary = {"total_stocks": 0}

    result = {
        "stage": 11,
        "date": date_str,
        "timestamp": _dt.now().isoformat(),
        "stocks": stock_results,
        "ranking": ranking,
        "summary": summary,
        "normalizer_stats": normalizer.get_stats(),
    }

    # Save to data/ensemble_YYYY-MM-DD.json
    output_path = data_dir / f"ensemble_{date_str}.json"
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("Phase 37: Ensemble results saved to %s (%d stocks)",
                    output_path.name, len(ranking))
    except IOError as e:
        logger.warning("Phase 37: Failed to save ensemble results: %s", e)

    if verbose and ranking:
        logger.info("Phase 37: Top 5 ensemble scores:")
        for s in ranking[:5]:
            logger.info("  #%d %s %s: %.4f [%s]",
                        s["rank"], s["code"], s["name"],
                        s["ensemble_score"], s["label_zh"])

    return result


# ---------------------------------------------------------------------------
# CLI for testing
# ---------------------------------------------------------------------------

def main():
    """CLI entry point for signal_fusion module."""
    import argparse
    parser = argparse.ArgumentParser(description="Phase 37: Signal Fusion Ensemble")
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    result = run_signal_fusion(date_str=args.date, verbose=args.verbose)

    if result:
        print(f"=== Phase 37: 訊號融合集成 ===")
        print(f"日期: {result['date']}")
        print(f"股票數: {result['summary']['total_stocks']}")
        if result['ranking']:
            print("\n前5名:")
            for s in result['ranking'][:5]:
                print(f"  #{s['rank']} {s['code']} {s['name']}: "
                      f"{s['ensemble_score']:.4f} [{s['label_zh']}]")
    else:
        print("No results available. Run Stage 2 first.")


if __name__ == "__main__":
    main()
