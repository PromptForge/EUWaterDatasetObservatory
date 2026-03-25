"""
Climate Adaptation Impact Proxy & Priority Fixes

Computes the same 5-component impact proxy used in the original paper,
then derives priority scores P(d,t) = I(d) × (1 − S(d,t)) for each
climate task.

Also identifies the single metadata dimension fix that would yield the
largest sufficiency gain per dataset-task pair.

Inputs:
  data/outputs_climate/climate_sufficiency_scores.csv
  data/outputs_climate/climate_prepared_records.csv
  config/climate_weights.json

Outputs:
  data/outputs_climate/climate_priority_fixes.csv
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import sys

sys.path.insert(0, str(Path(__file__).parent))

from impact_proxy import compute_impact, compute_priority
from score_climate_data import METRIC_FUNCTIONS, compute_climate_task_score

ROOT       = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
OUTPUT_DIR = ROOT / "data" / "outputs_climate"


def load_climate_weights() -> dict:
    with open(CONFIG_DIR / "climate_weights.json") as f:
        return json.load(f)


def best_single_fix(row: pd.Series, task: str, climate_weights: dict) -> tuple[str, float]:
    """
    For a dataset-task pair, identify which single dimension improvement
    (to score 1.0) would yield the largest sufficiency gain.

    Gain formula: weight[d] × (1 − current_score[d])
    Only considers dimensions with weight > 0.
    """
    task_w = climate_weights[task]
    best_dim  = "none"
    best_gain = 0.0

    for dim, weight in task_w.items():
        if weight <= 0:
            continue
        fn = METRIC_FUNCTIONS.get(dim)
        if fn is None:
            continue
        try:
            current_score = fn(row)
        except Exception:
            current_score = 0.0
        gain = weight * (1.0 - current_score)
        if gain > best_gain:
            best_gain = gain
            best_dim  = dim

    return best_dim, round(best_gain, 4)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("CLIMATE IMPACT PROXY & PRIORITY FIXES")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    # ── Load data ────────────────────────────────────────────────────────────
    scores_path   = OUTPUT_DIR / "climate_sufficiency_scores.csv"
    prepared_path = OUTPUT_DIR / "climate_prepared_records.csv"

    if not scores_path.exists():
        print(f"ERROR: {scores_path} not found. Run score_climate_data.py first.")
        return
    if not prepared_path.exists():
        print(f"ERROR: {prepared_path} not found. Run score_climate_data.py first.")
        return

    scores_df   = pd.read_csv(scores_path,   low_memory=False)
    prepared_df = pd.read_csv(prepared_path, low_memory=False)
    climate_weights = load_climate_weights()

    print(f"\nLoaded {len(scores_df)} scored records, {len(prepared_df)} prepared records")

    if len(scores_df) == 0:
        print("No data to process.")
        return

    # Merge prepared columns into scores_df.
    # prepared_df has both dataset_id and dataset_uri; use dataset_uri as key.
    # Drop dataset_id to avoid confusion, keep dataset_uri as the join key.
    prepared_df = prepared_df.drop(columns=["dataset_id"], errors="ignore")
    merge_cols  = [c for c in prepared_df.columns
                   if c not in scores_df.columns or c == "dataset_uri"]
    # Ensure merge_cols has no duplicates
    merge_cols  = list(dict.fromkeys(merge_cols))
    merged_df   = scores_df.merge(prepared_df[merge_cols], on="dataset_uri", how="left")

    # ── Compute impact proxy ─────────────────────────────────────────────────
    print("\n=== Computing impact proxy ===")
    impacts = []
    for _, row in merged_df.iterrows():
        impact_val, components = compute_impact(row)
        impacts.append({
            "dataset_uri":        row["dataset_uri"],
            "impact_score":       round(impact_val, 4),
            **{f"impact_{k}": round(v, 4) for k, v in components.items()},
        })
    impact_df = pd.DataFrame(impacts)
    merged_df = merged_df.merge(impact_df, on="dataset_uri", how="left")
    print(f"  Impact scores: mean={merged_df['impact_score'].mean():.3f}, "
          f"min={merged_df['impact_score'].min():.3f}, "
          f"max={merged_df['impact_score'].max():.3f}")

    # ── Compute priority and best fix per task ────────────────────────────────
    print("\n=== Computing priority scores and best fixes ===")
    tasks = list(climate_weights.keys())

    all_priority_rows = []
    for task in tasks:
        score_col    = f"{task}_score"
        priority_col = f"{task}_priority"

        if score_col not in merged_df.columns:
            print(f"  WARNING: {score_col} not in scores — skipping {task}")
            continue

        merged_df[priority_col] = merged_df.apply(
            lambda r: compute_priority(r["impact_score"], r[score_col]),
            axis=1
        )

        # Compute best single fix for each dataset
        best_fixes = merged_df.apply(
            lambda r: best_single_fix(r, task, climate_weights),
            axis=1
        )
        merged_df[f"{task}_best_fix_dim"]  = best_fixes.apply(lambda x: x[0])
        merged_df[f"{task}_best_fix_gain"] = best_fixes.apply(lambda x: x[1])

        # Top-20 priority fixes
        top20 = merged_df.nlargest(20, priority_col).copy()
        top20["task"]          = task
        top20["priority_rank"] = range(1, len(top20) + 1)

        # Select columns for output
        out_cols = [
            "task", "priority_rank", "dataset_uri", "domain",
            "impact_score", score_col, priority_col,
            f"{task}_best_fix_dim", f"{task}_best_fix_gain",
        ]
        # Add title if available
        for col in ["title"]:
            if col in top20.columns:
                out_cols.insert(3, col)
                break
        out_cols = [c for c in out_cols if c in top20.columns]

        all_priority_rows.append(top20[out_cols])
        print(f"\n  [{task}] top priority fix (rank 1):")
        r1 = top20.iloc[0]
        print(f"    URI      : {str(r1['dataset_uri'])[:65]}")
        print(f"    Impact   : {r1['impact_score']:.4f}")
        print(f"    Suf score: {r1[score_col]:.4f}")
        print(f"    Priority : {r1[priority_col]:.4f}")
        print(f"    Best fix : {r1[f'{task}_best_fix_dim']} "
              f"(gain={r1[f'{task}_best_fix_gain']:.4f})")

    if all_priority_rows:
        priority_df = pd.concat(all_priority_rows, ignore_index=True)
        priority_df.to_csv(OUTPUT_DIR / "climate_priority_fixes.csv", index=False)
        print(f"\nSaved climate_priority_fixes.csv ({len(priority_df)} rows, "
              f"top-20 per task × {len(tasks)} tasks)")
    else:
        print("No priority rows generated.")

    # ── Quick summary by task ─────────────────────────────────────────────────
    print("\n=== PRIORITY SUMMARY BY TASK ===")
    for task in tasks:
        pcol = f"{task}_priority"
        if pcol in merged_df.columns:
            fix_col = f"{task}_best_fix_dim"
            top_fix = (merged_df[fix_col].value_counts().idxmax()
                       if fix_col in merged_df.columns else "n/a")
            print(f"  {task}:")
            print(f"    Mean priority      : {merged_df[pcol].mean():.4f}")
            print(f"    Most common fix    : {top_fix}")

    print(f"\nAll outputs saved to: {OUTPUT_DIR}")
    return priority_df if all_priority_rows else None


if __name__ == "__main__":
    main()
