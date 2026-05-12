#!/usr/bin/env python3
"""
Stage 2: Fundamental Deep-Dive
Analyzes Stage 1 candidates with detailed fundamental checks
"""

import json
import logging
from datetime import datetime
from pathlib import Path

# Import helpers from stage1
import sys
sys.path.insert(0, str(Path(__file__).parent))
from stage1_screen import load_data, load_config, safe_float, get_field
import math

logger = logging.getLogger(__name__)


def load_stage1_results(date_str=None):
    """Load Stage 1 results"""
    data_dir = Path(__file__).parent.parent / "data"
    
    if date_str is None:
        json_files = sorted(data_dir.glob("stage1_*.json"))
        if not json_files:
            raise FileNotFoundError("No Stage 1 results found. Run stage1_screen.py first.")
        date_str = json_files[-1].stem.replace("stage1_", "")
    
    filepath = data_dir / f"stage1_{date_str}.json"
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_stock_id(sid):
    """Ensure consistent 4-digit string format for Taiwan stock IDs.
    
    FIX: Different data sources use different formats:
    - TWSE API: "2548" (no leading zero)
    - Some TPEx sources: "06207" (5 digits with leading zero)
    - This is the #1 cause of silent data mismatches
    
    All lookups, saves, and comparisons should use this function.
    """
    sid = str(sid).strip()
    if not sid:
        return sid
    try:
        # Remove leading zeros, then pad to 4 digits
        return str(int(sid)).zfill(4)
    except ValueError:
        # Non-numeric codes (e.g., "00400A") - keep as-is
        return sid.lstrip("0").ljust(4) if sid.startswith("0") else sid


def check_dividend_history(stock_code, dividends_data):
    """Check dividend consistency and yield (0-100)
    
    FIX: Continuous scoring instead of discrete buckets.
    Previously only produced 2 unique values across 75 stocks due to
    coarse ±10/±20 adjustments from a base of 50.
    """
    stock_divs = []
    for d in dividends_data:
        if get_field(d, "公司代號", "Code", "") == stock_code:
            stock_divs.append(d)
    
    if not stock_divs:
        return 25.0, "no_data"
    
    try:
        latest = stock_divs[0]
        cash_div = safe_float(get_field(latest, "股東配發-盈餘分配之現金股利(元/股)", "cash_div", ""), 0)
        stock_div = safe_float(get_field(latest, "股東配發-盈餘轉增資配股(元/股)", "stock_div", ""), 0)
        net_profit = safe_float(get_field(latest, "本期淨利(淨損)(元)", "net_profit", ""), 0)
        
        score = 50.0  # Base
        
        # Cash dividend - continuous scale based on amount (±25 range)
        if cash_div > 0:
            # Scale: small div (1-3元) → +10, large div (10+元) → +25
            score += min(25.0, 10.0 + cash_div * 1.5)
        else:
            score -= 10.0
        
        # Stock dividend - continuous scale (±15 range)
        if stock_div > 0:
            score += min(15.0, stock_div * 3.0)
        
        # Profitability - continuous scale based on magnitude (±25 range)
        if net_profit > 0:
            # Log-scale: larger profit = higher bonus
            import math
            profit_bonus = min(25.0, 10.0 + math.log10(max(net_profit, 1)) * 3.0)
            score += profit_bonus
        else:
            score -= 20.0
        
        # Consistency bonus - continuous based on record count (0-10 range)
        consistency = min(10.0, len(stock_divs) * 3.0)
        score += consistency
        
        return round(max(0.0, min(100.0, score)), 2), "ok"
    except Exception as e:
        logger.warning("[Stage2] check_dividend_history failed for %s: %r", stock_code, e)
        return 25.0, "error"


def check_announcements(stock_code, announce_data, days_back=30):
    """Check for negative corporate announcements (0-100)"""
    stock_anns = []
    for a in announce_data:
        if get_field(a, "公司代號", "Code", "") == stock_code:
            stock_anns.append(a)
    
    if not stock_anns:
        return 50, "neutral"  # No news is OK
    
    try:
        score = 50.0  # Base
        negative_keywords = ["裁員", "減資", "虧損", "處分", "訴訟", "罰款", "停產", "破產"]
        positive_keywords = ["增資", "獲利", "配股", "配息", "新廠", "簽約", "標案"]
        
        neg_count = 0
        pos_count = 0
        for ann in stock_anns[:10]:  # Check last 10
            subject = get_field(ann, "主旨 ", "subject", "")
            desc = get_field(ann, "說明", "description", "")
            text = f"{subject} {desc}"
            
            for kw in negative_keywords:
                if kw in text:
                    neg_count += 1
                    break
            
            for kw in positive_keywords:
                if kw in text:
                    pos_count += 1
                    break
        
        # Continuous scale: more negatives = steeper penalty, more positives = bonus
        score -= neg_count * 8.0  # -8 per negative (was ±10)
        score += pos_count * 3.5  # +3.5 per positive (was ±5)
        
        return round(max(0.0, min(100.0, score)), 2), "ok"
    except Exception as e:
        logger.warning("[Stage2] check_announcements failed for %s: %r", stock_code, e)
        return 50, "error"


def check_major_shareholders(stock_code, major_sh_data):
    """Check institutional ownership quality (0-100)"""
    stock_sh = []
    for s in major_sh_data:
        if get_field(s, "公司代號", "Code", "") == stock_code:
            stock_sh.append(s)
    
    if not stock_sh:
        return 25, "no_data"
    
    try:
        # Multiple major shareholders = stable ownership
        unique_sh = set()
        for s in stock_sh:
            sh_name = get_field(s, "大股東名稱", "shareholder_name", "")
            if sh_name:
                unique_sh.add(sh_name)
        
        score = 50.0  # Base
        
        # More unique shareholders = more stable (continuous scale)
        sh_bonus = min(20.0, len(unique_sh) * 4.0)  # +4 per shareholder, max +20
        score += sh_bonus
        if len(unique_sh) < 2:
            score -= 10.0
        
        return round(max(0.0, min(100.0, score)), 2), "ok"
    except Exception as e:
        logger.warning("[Stage2] check_major_shareholders failed for %s: %r", stock_code, e)
        return 25, "error"


def check_pledge_risk(stock_code, pledge_data):
    """Check if major shareholders pledged shares (lower = better)"""
    stock_pledge = []
    for p in pledge_data:
        if get_field(p, "公司代號", "Code", "") == stock_code:
            stock_pledge.append(p)
    
    if not stock_pledge:
        return 100, "no_pledge"  # No pledge = good
    
    try:
        # Check pledge ratio
        total_pledged = 0
        for p in stock_pledge:
            pledged = safe_float(get_field(p, "累計質押股數", "total_pledged", ""), 0)
            total_pledged += pledged
        
        # Continuous scale based on pledge magnitude (log scale)
        import math
        if total_pledged > 0:
            # Log-scale: small pledge ~75, large (>100M) ~20
            log_pledge = math.log10(total_pledged)
            score = max(15.0, min(80.0, 90.0 - (log_pledge - 6.0) * 12.0))
            if score < 40:
                status = "high_pledge"
            elif score < 70:
                status = "moderate_pledge"
            else:
                status = "low_pledge"
        else:
            return 100.0, "no_pledge"
        
        return round(score, 2), status
    except Exception as e:
        logger.warning("[Stage2] check_pledge_risk failed for %s: %r", stock_code, e)
        return 50, "error"


def check_penalty_risk(stock_code, penalty_data):
    """Check for regulatory penalties (lower = worse)"""
    stock_penalties = []
    for p in penalty_data:
        if get_field(p, "公司代號", "Code", "") == stock_code:
            stock_penalties.append(p)
    
    if not stock_penalties:
        return 100, "clean"
    
    try:
        # Recent penalties are worse
        recent_count = 0
        for p in stock_penalties:
            date_str = get_field(p, "處分日期", "penalty_date", "")
            if date_str:
                # Simple check: if date is in last year
                try:
                    penalty_date = datetime.strptime(str(date_str)[:8], "%Y%m%d")
                    if (datetime.now() - penalty_date).days < 365:
                        recent_count += 1
                except ValueError:
                    # Unparseable date format — skip this entry silently
                    pass
        
        # Continuous scale based on penalty count
        if recent_count > 0:
            score = max(5.0, min(60.0, 100.0 - recent_count * 25.0))
            if recent_count >= 3:
                status = "multiple_recent_penalties"
            else:
                status = "recent_penalty"
        else:
            return round(max(80.0, min(100.0, 100.0 - len(stock_penalties) * 5.0)), 2), "old_penalties_only"
        
        return round(score, 2), status
    except Exception as e:
        logger.warning("[Stage2] check_penalty_risk failed for %s: %r", stock_code, e)
        return 50, "error"


def validate_stage1_candidates(candidates, verbose=False):
    """Phase 11: Validate Stage 1 candidates using Pydantic schema before Stage 2 processing.

    Catches missing fields, wrong types, out-of-range scores, and field name mismatches
    between Stage 1 output and Stage 2 expectations. Invalid candidates are excluded with
    a warning but don't block the pipeline.
    """
    if not candidates:
        return []

    try:
        from core.schemas import Stage1Candidate, ScoreBreakdown
    except ImportError as e:
        if verbose:
            print(f"⚠ Pydantic schemas unavailable for inter-stage validation: {e}")
        return candidates  # Return raw data without validation

    valid = []
    invalid_count = 0

    for i, c in enumerate(candidates):
        try:
            code = str(c.get("code", ""))
            if not code:
                raise ValueError(f"Missing 'code' field")

            # Build ScoreBreakdown from score_breakdown dict
            sb = c.get("score_breakdown", {})
            # Phase 11 R2: Require all sub-scores — no defaults that mask missing data.
            # Missing data scored as "mediocre" is worse than failing validation.
            breakdown = ScoreBreakdown(
                revenue=float(sb["revenue"]),
                profitability=float(sb["profitability"]),
                valuation=float(sb["valuation"]),
                flow=float(sb["flow"]),
                momentum=float(sb["momentum"]),
            )

            # Validate the full candidate via Pydantic schema (extra='forbid' catches bugs)
            validated = Stage1Candidate(
                code=code,
                name=str(c.get("name", "")),
                close=float(c["close"]),
                composite_score=float(c["composite_score"]),
                score_breakdown=breakdown,
                passed=bool(c.get("pass", c["composite_score"] >= 65)),
            )
            valid.append(validated.model_dump())
        except Exception as e:
            invalid_count += 1
            if verbose and invalid_count <= 3:
                code = str(c.get("code", f"#{i}"))
                print(f"   ⚠ Stage→Stage2 validation failed for {code}: {e}")

    if verbose:
        print(f"📋 Inter-stage validation: {len(valid)}/{len(candidates)} valid ({invalid_count} excluded)")

    return valid


def run_stage2(date_str=None, verbose=False):
    """Run Stage 2 deep-dive on Stage 1 candidates
    
    FIX v2: Actually disqualify stocks with red flags:
    - Pledge ratio >30% = disqualify
    - Recent penalties (within 180 days) = disqualify  
    - Negative announcements (裁員, 減資, 虧損) = disqualify
    - Combined score threshold raised
    """
    stage1_results = load_stage1_results(date_str)
    datasets, date = load_data(date_str)
    weights, thresholds = load_config()
    
    dividends_data = datasets.get("dividends", [])
    announce_data = datasets.get("announce", [])
    major_sh_data = datasets.get("major_sh", [])
    pledge_data = datasets.get("pledge", [])
    penalty_data = datasets.get("penalties", [])
    
    candidates = stage1_results.get("candidates", [])
    # Phase 11: Inter-stage validation — catch field mismatches early
    raw_candidates = list(candidates)
    candidates = validate_stage1_candidates(candidates, verbose=verbose)
    if verbose and len(candidates) != len(raw_candidates):
        print(f"   Filtered {len(raw_candidates)} Stage 1 candidates → {len(candidates)} valid")
    
    if verbose:
        print(f"🔬 Stage 2: Deep-diving {len(candidates)} candidates")
        print(f"   Stage 2 min score: {thresholds['stage2']['fundamental_score_min']}")
        print(f"   Red flag disqualification: {thresholds['stage2'].get('red_flag_disqualify', True)}")
        print()
    
    deep_results = []
    disqualified = []
    
    # Phase 12: Scoring diagnostics — track error rates and score distributions
    diagnostics = {
        "total_candidates": len(candidates),
        "check_errors": {"dividend": 0, "announcements": 0, "shareholders": 0, "pledge": 0, "penalties": 0},
        "score_distributions": {"dividend": [], "announcements": [], "shareholders": [], "pledge": [], "penalties": []},
        "score_stats": {},
    }
    
    for candidate in candidates:
        code = str(candidate.get("code", ""))
        if not code:
            continue
        name = str(candidate.get("name", ""))
        # Phase 11 R2: No silent defaults — missing composite_score means Stage 1 bug, skip safely
        cs_raw = candidate.get("composite_score")
        if cs_raw is None:
            continue
        stage1_score = float(cs_raw)
        
        # Run deep checks
        div_score, div_status = check_dividend_history(code, dividends_data)
        ann_score, ann_status = check_announcements(code, announce_data)
        sh_score, sh_status = check_major_shareholders(code, major_sh_data)
        pledge_score, pledge_status = check_pledge_risk(code, pledge_data)
        penalty_score, penalty_status = check_penalty_risk(code, penalty_data)
        
        # Phase 12: Collect diagnostics
        if div_status == "error":
            diagnostics["check_errors"]["dividend"] += 1
        if ann_status == "error":
            diagnostics["check_errors"]["announcements"] += 1
        if sh_status == "error":
            diagnostics["check_errors"]["shareholders"] += 1
        if pledge_status == "error":
            diagnostics["check_errors"]["pledge"] += 1
        if penalty_status == "error":
            diagnostics["check_errors"]["penalties"] += 1
        
        # === RED FLAG DISQUALIFICATION ===
        # FIX v2: Actually enforce red flags
        red_flags = []
        
        if thresholds["stage2"].get("red_flag_disqualify", True):
            # Pledge risk: score < 40 means high pledge
            if pledge_score < 40:
                red_flags.append(f"High pledge risk (score={pledge_score}, status={pledge_status})")
            
            # Recent penalties: score < 50 means concerning
            if penalty_score < 50:
                red_flags.append(f"Penalty risk (score={penalty_score}, status={penalty_status})")
            
            # Negative announcements: score < 30 means serious issues
            if ann_score < 30:
                red_flags.append(f"Negative announcements (score={ann_score}, status={ann_status})")
        
        if red_flags:
            result = {
                "code": code,
                "name": name,
                "stage1_score": stage1_score,
                "stage2_score": 0,
                "red_flags": red_flags,
                "checks": {
                    "dividend": {"score": div_score, "status": div_status},
                    "announcements": {"score": ann_score, "status": ann_status},
                    "shareholders": {"score": sh_score, "status": sh_status},
                    "pledge": {"score": pledge_score, "status": pledge_status},
                    "penalties": {"score": penalty_score, "status": penalty_status}
                },
                "combined_score": 0
            }
            disqualified.append(result)
            if verbose:
                print(f"   ❌ DISQUALIFIED {code} {name}: {'; '.join(red_flags)}")
            continue
        
        # Weighted Stage 2 score
        stage2_weights = weights.get("stage2", {})
        fundamental_score = (
            div_score * 0.25 +
            ann_score * 0.20 +
            sh_score * 0.15 +
            pledge_score * 0.20 +
            penalty_score * 0.20
        )
        
        result = {
            "code": code,
            "name": name,
            "stage1_score": stage1_score,
            "stage2_score": round(fundamental_score, 1),
            "checks": {
                "dividend": {"score": div_score, "status": div_status},
                "announcements": {"score": ann_score, "status": ann_status},
                "shareholders": {"score": sh_score, "status": sh_status},
                "pledge": {"score": pledge_score, "status": pledge_status},
                "penalties": {"score": penalty_score, "status": penalty_status}
            },
            "combined_score": round((stage1_score * 0.6 + fundamental_score * 0.4), 1)
        }
        
        if fundamental_score >= thresholds["stage2"]["fundamental_score_min"]:
            deep_results.append(result)
    
    # Phase 12: Compute score distribution stats
    for check_name in diagnostics["score_distributions"]:
        scores = [r["checks"][check_name]["score"] for r in deep_results + disqualified if check_name in r.get("checks", {})]
        if scores:
            diagnostics["score_stats"][check_name] = {
                "min": round(min(scores), 1),
                "max": round(max(scores), 1),
                "mean": round(sum(scores) / len(scores), 1),
                "count": len(scores),
            }
        else:
            diagnostics["score_stats"][check_name] = {"min": None, "max": None, "mean": None, "count": 0}
    
    # Sort by combined score
    deep_results.sort(key=lambda x: x["combined_score"], reverse=True)
    
    output = {
        "stage": 2,
        "date": date,
        "timestamp": datetime.now().isoformat(),
        "candidates": deep_results,
        "disqualified": disqualified,
        "summary": {
            "stage1_candidates": len(candidates),
            "passed_stage2": len(deep_results),
            "disqualified": len(disqualified)
        },
        "diagnostics": diagnostics  # Phase 12
    }
    
    if verbose:
        print(f"📊 Stage 2 Results:")
        print(f"   Passed: {len(deep_results)}")
        print(f"   Disqualified: {len(disqualified)}")
        
        # Phase 12: Print error summary
        total_errors = sum(diagnostics["check_errors"].values())
        if total_errors > 0:
            print(f"   ⚠ Check errors: {total_errors}/{diagnostics['total_candidates']} candidates")
            for check, count in diagnostics["check_errors"].items():
                if count > 0:
                    print(f"      - {check}: {count} errors")
        
        # Phase 12: Print score stats
        print(f"   Score ranges (passed candidates):")
        for check, stats in diagnostics.get("score_stats", {}).items():
            if stats["count"] > 0:
                print(f"      {check}: {stats['min']}-{stats['max']} (mean={stats['mean']})")
        print()
        if deep_results:
            print(f"   Top 5 after deep-dive:")
            for c in deep_results[:5]:
                print(f"      {c['code']} {c['name']}: combined={c['combined_score']} (S1={c['stage1_score']}, S2={c['stage2_score']})")
    
    return output


def save_stage2_results(results):
    """Save Stage 2 results"""
    data_dir = Path(__file__).parent.parent / "data"
    date = results["date"]
    
    filepath = data_dir / f"stage2_{date}.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    return filepath


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run Stage 2 fundamental deep-dive")
    parser.add_argument("--date", type=str, help="Date to analyze (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    verbose = args.verbose
    
    results = run_stage2(date_str=args.date, verbose=verbose)
    save_stage2_results(results)
    
    if verbose:
        print(f"\n💾 Saved to data/stage2_{results['date']}.json")
    
    return results


if __name__ == "__main__":
    main()
