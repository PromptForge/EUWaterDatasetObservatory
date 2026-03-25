"""
Climate Adaptation Sensitivity Analysis

For each climate task (drought_early_warning, climate_infrastructure, nbs_monitoring):
  1. Identifies the highest-weighted dimension
  2. Perturbs that weight by ±25%, renormalizes remaining weights
     (0-weight dimensions stay at 0 after renormalization)
  3. Recomputes sufficiency scores and readiness categories
  4. Records: task, primary_weight, baseline_ready%, low_ready%, high_ready%, range_pp

Inputs:
  data/outputs_climate/climate_prepared_records.csv
  config/climate_weights.json

Outputs:
  data/outputs_climate/climate_sensitivity_summary.csv
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import sys

sys.path.insert(0, str(Path(__file__).parent))

from sufficiency_scoring import classify_readiness

# Import the same dimension scoring functions used in score_climate_data.py
from score_climate_data import METRIC_FUNCTIONS, compute_climate_task_score

ROOT       = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
OUTPUT_DIR = ROOT / "data" / "outputs_climate"


def load_climate_weights() -> dict:
    with open(CONFIG_DIR / "climate_weights.json") as f:
        return json.load(f)


def perturb_weights(task_weights: dict, perturbation: float = 0.25) -> dict:
    """
    Perturb the highest non-zero weight by ±perturbation.
    Renormalizes all weights (0-weight dimensions remain 0).
    Returns: {"baseline": ..., "low": ..., "high": ..., "primary_metric": ...}
    """
    # Find primary: highest non-zero weight
    non_zero = {k: v for k, v in task_weights.items() if v > 0}
    if not non_zero:
        return {"baseline": task_weights.copy(), "low": task_weights.copy(),
                "high": task_weights.copy(), "primary_metric": "none"}

    primary = max(non_zero, key=non_zero.get)
    base_val = task_weights[primary]

    low_w  = task_weights.copy()
    high_w = task_weights.copy()

    low_w[primary]  = base_val * (1 - perturbation)
    high_w[primary] = base_val * (1 + perturbation)

    # Renormalize so all weights sum to 1.00 (0-weights stay 0)
    for w_dict in [low_w, high_w]:
        total = sum(w_dict.values())
        if total > 0:
            for k in w_dict:
                w_dict[k] = round(w_dict[k] / total, 6)

    return {
        "baseline":       task_weights.copy(),
        "low":            low_w,
        "high":           high_w,
        "primary_metric": primary,
    }


def score_with_weights(df: pd.DataFrame, task: str, weights: dict) -> pd.Series:
    """Recompute sufficiency scores for all rows under the given weights."""
    # Build a fake climate_weights dict with this task's weights
    task_weights_wrap = {task: weights}
    scores = []
    for _, row in df.iterrows():
        score, _ = compute_climate_task_score(row, task, task_weights_wrap)
        scores.append(score)
    return pd.Series(scores, index=df.index)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("CLIMATE SENSITIVITY ANALYSIS (±25% weight perturbation)")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    # ── Load prepared data ───────────────────────────────────────────────────
    prepared_path = OUTPUT_DIR / "climate_prepared_records.csv"
    if not prepared_path.exists():
        print(f"ERROR: {prepared_path} not found. Run score_climate_data.py first.")
        return

    df = pd.read_csv(prepared_path, low_memory=False)
    print(f"\nLoaded {len(df)} prepared records")

    if len(df) == 0:
        print("No data to analyse.")
        return

    # ── Load climate weights ─────────────────────────────────────────────────
    climate_weights = load_climate_weights()
    tasks = list(climate_weights.keys())
    print(f"Climate tasks: {tasks}\n")

    results = []

    for task in tasks:
        print(f"\n--- {task} ---")
        task_w = climate_weights[task]

        perturbed = perturb_weights(task_w, perturbation=0.25)
        primary   = perturbed["primary_metric"]
        print(f"  Primary weight: {primary} = {task_w.get(primary, 0):.2f}")

        # Compute scores under each scenario
        base_scores = score_with_weights(df, task, perturbed["baseline"])
        low_scores  = score_with_weights(df, task, perturbed["low"])
        high_scores = score_with_weights(df, task, perturbed["high"])

        def _ready_pct(s: pd.Series) -> float:
            return float((s.apply(classify_readiness) == "Ready").mean() * 100)
        def _partial_pct(s: pd.Series) -> float:
            return float((s.apply(classify_readiness) == "Partial").mean() * 100)
        def _insufficient_pct(s: pd.Series) -> float:
            return float((s.apply(classify_readiness) == "Insufficient").mean() * 100)

        baseline_ready = _ready_pct(base_scores)
        low_ready      = _ready_pct(low_scores)
        high_ready     = _ready_pct(high_scores)
        range_pp       = high_ready - low_ready

        row = {
            "task":                    task,
            "primary_metric":          primary,
            "primary_weight_baseline": round(task_w.get(primary, 0), 4),
            "primary_weight_low":      round(perturbed["low"].get(primary, 0), 4),
            "primary_weight_high":     round(perturbed["high"].get(primary, 0), 4),
            "baseline_mean_score":     round(float(base_scores.mean()), 4),
            "low_mean_score":          round(float(low_scores.mean()),  4),
            "high_mean_score":         round(float(high_scores.mean()), 4),
            "baseline_ready_pct":      round(baseline_ready, 2),
            "low_ready_pct":           round(low_ready,      2),
            "high_ready_pct":          round(high_ready,     2),
            "range_pp":                round(range_pp,       2),
            "robust":                  abs(range_pp) < 5.0,
        }
        results.append(row)

        print(f"  Baseline mean : {row['baseline_mean_score']:.4f}")
        print(f"  Low mean      : {row['low_mean_score']:.4f}")
        print(f"  High mean     : {row['high_mean_score']:.4f}")
        print(f"  Ready %   — baseline: {baseline_ready:.1f}%  "
              f"low: {low_ready:.1f}%  high: {high_ready:.1f}%")
        print(f"  Range (Δ pp)  : {range_pp:.2f}  "
              f"→ {'ROBUST' if abs(range_pp) < 5 else 'SENSITIVE'}")

    summary_df = pd.DataFrame(results)
    summary_df.to_csv(OUTPUT_DIR / "climate_sensitivity_summary.csv", index=False)
    print(f"\nSaved climate_sensitivity_summary.csv")

    print("\n=== SENSITIVITY SUMMARY ===")
    print(summary_df[["task", "primary_metric", "baseline_ready_pct",
                       "low_ready_pct", "high_ready_pct", "range_pp"]].to_string(index=False))

    print("\nINTERPRETATION:")
    for _, row in summary_df.iterrows():
        msg = ("ROBUST: Conclusions stable under weight variation"
               if row["robust"] else
               "SENSITIVE: Consider reporting with confidence bands")
        print(f"  {row['task']}: primary={row['primary_metric']}, "
              f"range={row['range_pp']:.1f}pp → {msg}")

    return summary_df


if __name__ == "__main__":
    main()
