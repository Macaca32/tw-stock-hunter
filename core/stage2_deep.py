#!/usr/bin/env python3
"""
Stage 2: Fundamental Deep-Dive
Analyzes Stage 1 candidates with detailed fundamental checks
"""

import json
from datetime import datetime
from pathlib import Path

# Import helpers from stage1
import sys
sys.path.insert(0, str(Path(__file__).parent))
from stage1_screen import load_data, load_config, safe_float, get_field


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


def check_dividend_history(stock_code, dividends_data):
    """Check dividend consistency and yield (0-100)"""
    stock_divs = []
    for d in dividends_data:
        if get_field(d, "公司代號", "Code", "") == stock_code:
            stock_divs.append(d)
    
    if not stock_divs:
        return 25, "no_data"
    
    try:
        # Check most recent dividend
        latest = stock_divs[0]
        cash_div = safe_float(get_field(latest, "股東配發-盈餘分配之現金股利(元/股)", "cash_div", ""), 0)
        stock_div = safe_float(get_field(latest, "股東配發-盈餘轉增資配股(元/股)", "stock_div", ""), 0)
        
        # Net profit
        net_profit = safe_float(get_field(latest, "本期淨利(淨損)(元)", "net_profit", ""), 0)
        
        score = 50  # Base
        
        # Cash dividend consistency
        if cash_div > 0:
            score += 20
        else:
            score -= 10
        
        # Stock dividend (growth signal)
        if stock_div > 0:
            score += 10
        
        # Profitability
        if net_profit > 0:
            score += 20
        else:
            score -= 20
        
        # Multiple dividend records = consistency
        if len(stock_divs) >= 3:
            score += 10
        
        return max(0, min(100, score)), "ok"
    except:
        return 25, "error"


def check_announcements(stock_code, announce_data, days_back=30):
    """Check for negative corporate announcements (0-100)"""
    stock_anns = []
    for a in announce_data:
        if get_field(a, "公司代號", "Code", "") == stock_code:
            stock_anns.append(a)
    
    if not stock_anns:
        return 50, "neutral"  # No news is OK
    
    try:
        score = 50  # Base
        negative_keywords = ["裁員", "減資", "虧損", "處分", "訴訟", "罰款", "停產", "破產"]
        positive_keywords = ["增資", "獲利", "配股", "配息", "新廠", "簽約", "標案"]
        
        for ann in stock_anns[:10]:  # Check last 10
            subject = get_field(ann, "主旨 ", "subject", "")
            desc = get_field(ann, "說明", "description", "")
            text = f"{subject} {desc}"
            
            # Negative signals
            for kw in negative_keywords:
                if kw in text:
                    score -= 10
                    break
            
            # Positive signals
            for kw in positive_keywords:
                if kw in text:
                    score += 5
                    break
        
        return max(0, min(100, score)), "ok"
    except:
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
        
        score = 50  # Base
        
        # More unique shareholders = more stable
        if len(unique_sh) >= 5:
            score += 20
        elif len(unique_sh) >= 3:
            score += 10
        elif len(unique_sh) < 2:
            score -= 10
        
        return max(0, min(100, score)), "ok"
    except:
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
        
        # High pledge = higher risk
        if total_pledged > 100000000:
            return 20, "high_pledge"
        elif total_pledged > 10000000:
            return 40, "moderate_pledge"
        elif total_pledged > 0:
            return 70, "low_pledge"
        else:
            return 100, "no_pledge"
    except:
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
                except:
                    pass
        
        if recent_count >= 3:
            return 0, "multiple_recent_penalties"
        elif recent_count >= 1:
            return 40, "recent_penalty"
        else:
            return 80, "old_penalties_only"
    except:
        return 50, "error"


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
    
    if verbose:
        print(f"🔬 Stage 2: Deep-diving {len(candidates)} candidates")
        print(f"   Stage 2 min score: {thresholds['stage2']['fundamental_score_min']}")
        print(f"   Red flag disqualification: {thresholds['stage2'].get('red_flag_disqualify', True)}")
        print()
    
    deep_results = []
    disqualified = []
    
    for candidate in candidates:
        code = candidate["code"]
        name = candidate["name"]
        stage1_score = candidate["composite_score"]
        
        # Run deep checks
        div_score, div_status = check_dividend_history(code, dividends_data)
        ann_score, ann_status = check_announcements(code, announce_data)
        sh_score, sh_status = check_major_shareholders(code, major_sh_data)
        pledge_score, pledge_status = check_pledge_risk(code, pledge_data)
        penalty_score, penalty_status = check_penalty_risk(code, penalty_data)
        
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
        }
    }
    
    if verbose:
        print(f"📊 Stage 2 Results:")
        print(f"   Passed: {len(deep_results)}")
        print(f"   Disqualified: {len(disqualified)}")
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
