"""
Sensitivity analysis for task-specific weights.

Perturbs primary weights by ±25% to test robustness of conclusions.
"""

import json
from pathlib import Path
import pandas as pd
import numpy as np
from sufficiency_scoring import compute_sufficiency, classify_readiness, load_weights

CONFIG_DIR = Path(__file__).parent.parent / "config"

def perturb_weights(weights, task, perturbation=0.25):
    """
    Perturb the highest-weighted metric by ±perturbation.
    Returns dict with baseline, low, and high weight sets.
    """
    task_weights = weights[task].copy()
    
    # Find primary (highest) weight
    primary_metric = max(task_weights, key=task_weights.get)
    primary_value = task_weights[primary_metric]
    
    # Create perturbed versions
    low_weights = task_weights.copy()
    high_weights = task_weights.copy()
    
    low_weights[primary_metric] = primary_value * (1 - perturbation)
    high_weights[primary_metric] = primary_value * (1 + perturbation)
    
    # Renormalize
    for w_dict in [low_weights, high_weights]:
        total = sum(w_dict.values())
        for k in w_dict:
            w_dict[k] /= total
    
    return {
        "baseline": task_weights,
        "low": low_weights,
        "high": high_weights,
        "primary_metric": primary_metric
    }

def run_sensitivity_analysis(df, perturbation=0.25):
    """Run sensitivity analysis across all tasks."""
    weights = load_weights()
    tasks = ["early_warning", "compliance_reporting", "cross_border"]
    
    results = {
        "summary": [],
        "detail": []
    }
    
    for task in tasks:
        perturbed = perturb_weights(weights, task, perturbation)
        
        # Compute scores under each weight scenario
        scenarios = {}
        for scenario_name, scenario_weights in [
            ("baseline", perturbed["baseline"]),
            ("low", perturbed["low"]),
            ("high", perturbed["high"])
        ]:
            scenario_scores = []
            for idx, row in df.iterrows():
                # Create modified weights dict
                modified_weights = {task: scenario_weights}
                score, _ = compute_sufficiency(row, task, modified_weights)
                scenario_scores.append(score)
            scenarios[scenario_name] = scenario_scores
        
        # Compute statistics
        baseline_ready = sum(1 for s in scenarios["baseline"] if classify_readiness(s) == "Ready") / len(df) * 100
        low_ready = sum(1 for s in scenarios["low"] if classify_readiness(s) == "Ready") / len(df) * 100
        high_ready = sum(1 for s in scenarios["high"] if classify_readiness(s) == "Ready") / len(df) * 100
        
        results["summary"].append({
            "task": task,
            "primary_metric": perturbed["primary_metric"],
            "baseline_mean": np.mean(scenarios["baseline"]),
            "low_mean": np.mean(scenarios["low"]),
            "high_mean": np.mean(scenarios["high"]),
            "baseline_ready_pct": baseline_ready,
            "low_ready_pct": low_ready,
            "high_ready_pct": high_ready,
            "range_pp": high_ready - low_ready
        })
        
        # Store detailed scores
        for i, dataset_id in enumerate(df["dataset_id"]):
            results["detail"].append({
                "dataset_id": dataset_id,
                "task": task,
                "baseline_score": scenarios["baseline"][i],
                "low_score": scenarios["low"][i],
                "high_score": scenarios["high"][i]
            })
    
    return results

def main():
    # Load data with scores
    data_path = Path(__file__).parent.parent / "data" / "outputs" / "sufficiency_scores.csv"
    df = pd.read_csv(data_path)
    
    print("Running sensitivity analysis (±25% weight perturbation)...")
    
    results = run_sensitivity_analysis(df, perturbation=0.25)
    
    # Save results
    summary_df = pd.DataFrame(results["summary"])
    detail_df = pd.DataFrame(results["detail"])
    
    output_dir = Path(__file__).parent.parent / "data" / "outputs"
    summary_df.to_csv(output_dir / "sensitivity_summary.csv", index=False)
    detail_df.to_csv(output_dir / "sensitivity_detail.csv", index=False)
    
    print("\n=== SENSITIVITY ANALYSIS RESULTS ===")
    print("\nSummary (±25% perturbation of primary weight):")
    print(summary_df.to_string(index=False))
    
    print("\nInterpretation:")
    for _, row in summary_df.iterrows():
        print(f"\n{row['task'].upper().replace('_', ' ')}:")
        print(f"  Primary metric: {row['primary_metric']}")
        print(f"  Ready % range: {row['low_ready_pct']:.1f}% - {row['high_ready_pct']:.1f}%")
        print(f"  Range: {row['range_pp']:.1f} percentage points")
        if row['range_pp'] < 5:
            print(f"  → ROBUST: Conclusions stable under weight variation")
        else:
            print(f"  → SENSITIVE: Consider reporting with confidence bands")
    
    return results

if __name__ == "__main__":
    main()
