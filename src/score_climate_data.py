"""
EU Water Dataset Observatory — Climate Adaptation Data Scorer

Scores harvested climate datasets against:
  - 3 climate tasks: drought_early_warning, climate_infrastructure, nbs_monitoring
    (weights loaded from config/climate_weights.json)
  - 3 original water tasks: early_warning, compliance_reporting, cross_border
    (weights loaded from config/weights.json via sufficiency_scoring.load_weights)

Inputs:
  data/harvested/climate_harvest.csv
  config/climate_weights.json

Outputs:
  data/outputs_climate/climate_prepared_records.csv   — column-mapped prepared data
  data/outputs_climate/climate_sufficiency_scores.csv — dataset_uri, domain, 6 scores
  data/outputs_climate/climate_analysis_summary.json  — summary statistics
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import sys

sys.path.insert(0, str(Path(__file__).parent))

from sufficiency_scoring import (
    compute_sufficiency,
    classify_readiness,
    load_weights,
    score_temporal_recency,
    score_update_frequency,
    score_temporal_coverage,
    score_spatial_precision,
    score_format_readability,
    score_license_openness,
    score_description_quality,
    score_vocabulary_standard,
    score_multilingual,
)

# Re-use all column-prep helpers from score_real_data
from score_real_data import (
    prepare_harvested_data,
    _parse_date_to_str,
    _count_keywords,
    _count_languages,
    _is_open_license,
    _infer_update_frequency,
    _infer_spatial_precision,
    _infer_publisher_type,
    _calc_temporal_span,
    _extract_format,
)

ROOT       = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
OUTPUT_DIR = ROOT / "data" / "outputs_climate"

# Dimension scoring functions for climate tasks
METRIC_FUNCTIONS = {
    "temporal_recency":    score_temporal_recency,
    "update_frequency":    score_update_frequency,
    "temporal_coverage":   score_temporal_coverage,
    "spatial_precision":   score_spatial_precision,
    "format_readability":  score_format_readability,
    "license_openness":    score_license_openness,
    "description_quality": score_description_quality,
    "vocabulary_standard": score_vocabulary_standard,
    "multilingual":        score_multilingual,
}


def load_climate_weights() -> dict:
    with open(CONFIG_DIR / "climate_weights.json") as f:
        return json.load(f)


def compute_climate_task_score(row: pd.Series, task: str,
                                climate_weights: dict) -> tuple[float, dict]:
    """Compute sufficiency score for a climate task, plus per-dimension detail."""
    weights = climate_weights[task]
    total_w = 0.0
    weighted_sum = 0.0
    detail = {}
    for metric, weight in weights.items():
        fn = METRIC_FUNCTIONS.get(metric)
        if fn is None:
            continue
        try:
            s = fn(row)
        except Exception:
            s = 0.0
        detail[metric] = round(float(s), 4)
        weighted_sum += weight * s
        total_w += weight
    score = round(weighted_sum / total_w, 4) if total_w > 0 else 0.0
    return score, detail


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("CLIMATE ADAPTATION SUFFICIENCY SCORING")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    # ── 1. Load & prepare ────────────────────────────────────────────────────
    harvest_path = ROOT / "data" / "harvested" / "climate_harvest.csv"
    if not harvest_path.exists():
        print(f"ERROR: {harvest_path} not found. Run harvest_climate.py first.")
        return

    print("\n=== Loading & preparing climate harvest ===")
    raw_df = pd.read_csv(harvest_path, low_memory=False)
    print(f"  Loaded {len(raw_df)} rows")

    # Use the same preparation pipeline as score_real_data.py
    df = prepare_harvested_data(harvest_path)

    # Restore `domain` from climate harvest (prepare_harvested_data reads 'domain')
    df["domain"] = raw_df["domain"].fillna("unknown").astype(str).values

    df.to_csv(OUTPUT_DIR / "climate_prepared_records.csv", index=False)
    print(f"  Saved climate_prepared_records.csv ({len(df)} rows)")

    # ── 2. Load weight profiles ───────────────────────────────────────────────
    climate_weights  = load_climate_weights()
    original_weights = load_weights()      # from config/weights.json

    climate_tasks  = list(climate_weights.keys())
    original_tasks = ["early_warning", "compliance_reporting", "cross_border"]

    print(f"\n  Climate tasks  : {climate_tasks}")
    print(f"  Original tasks : {original_tasks}")

    # ── 3. Score all 6 tasks per dataset ─────────────────────────────────────
    print("\n=== Computing sufficiency scores (6 tasks) ===")

    score_records = []
    for idx, row in df.iterrows():
        rec = {
            "dataset_uri": row["dataset_id"],
            "domain":      row["domain"],
        }

        # Climate tasks
        for task in climate_tasks:
            score, detail = compute_climate_task_score(row, task, climate_weights)
            rec[f"{task}_score"]     = score
            rec[f"{task}_readiness"] = classify_readiness(score)
            for dim, s in detail.items():
                rec[f"{task}_{dim}"] = s

        # Original tasks (re-use existing compute_sufficiency)
        for task in original_tasks:
            score, detail = compute_sufficiency(row, task, original_weights)
            rec[f"{task}_score"]     = round(score, 4)
            rec[f"{task}_readiness"] = classify_readiness(score)
            for dim, s in detail.items():
                rec[f"{task}_{dim}"] = round(float(s), 4)

        score_records.append(rec)

    scores_df = pd.DataFrame(score_records)
    scores_df.to_csv(OUTPUT_DIR / "climate_sufficiency_scores.csv", index=False)
    print(f"  Saved climate_sufficiency_scores.csv ({len(scores_df)} rows, "
          f"{len(scores_df.columns)} columns)")

    # ── 4. Summary statistics ─────────────────────────────────────────────────
    print("\n=== Generating climate_analysis_summary.json ===")

    all_tasks = climate_tasks + original_tasks

    def _task_stats(task: str) -> dict:
        col = f"{task}_score"
        vals = scores_df[col].dropna()
        if len(vals) == 0:
            return {}
        return {
            "mean":             round(float(vals.mean()),   4),
            "median":           round(float(vals.median()), 4),
            "min":              round(float(vals.min()),    4),
            "max":              round(float(vals.max()),    4),
            "std":              round(float(vals.std()),    4),
            "ready_pct":        round(float((scores_df[f"{task}_readiness"] == "Ready").mean()    * 100), 2),
            "partial_pct":      round(float((scores_df[f"{task}_readiness"] == "Partial").mean()  * 100), 2),
            "insufficient_pct": round(float((scores_df[f"{task}_readiness"] == "Insufficient").mean() * 100), 2),
        }

    # Per-domain breakdown for climate tasks
    domain_breakdown: dict = {}
    for domain in scores_df["domain"].unique():
        sub = scores_df[scores_df["domain"] == domain]
        domain_breakdown[domain] = {
            "count": len(sub),
            "task_scores": {
                task: round(float(sub[f"{task}_score"].mean()), 4)
                for task in all_tasks
                if f"{task}_score" in sub.columns
            }
        }

    # Load original water harvest stats for comparison
    water_stats_path = ROOT / "data" / "outputs_real" / "analysis_summary_real.json"
    water_comparison: dict = {}
    if water_stats_path.exists():
        with open(water_stats_path) as f:
            water_summary = json.load(f)
        # water uses "early_warning", "compliance", "cross_border" keys in summary
        suf = water_summary.get("sufficiency_scores", {})
        for label, wdata in suf.items():
            water_comparison[label] = {
                "water_mean":   wdata.get("mean", None),
                "climate_mean": (
                    round(float(scores_df[f"{label}_score"].mean()), 4)
                    if f"{label}_score" in scores_df.columns else
                    (round(float(scores_df[f"{label.replace('compliance','compliance_reporting').replace('cross_border','cross_border')}_score"].mean()), 4)
                     if f"{label.replace('compliance','compliance_reporting')}_score" in scores_df.columns else None)
                ),
            }
        # Map directly for the three tasks
        for orig_lbl, col_key in [("early_warning", "early_warning"),
                                   ("compliance", "compliance_reporting"),
                                   ("cross_border", "cross_border")]:
            col = f"{col_key}_score"
            if col in scores_df.columns and orig_lbl in suf:
                water_comparison[orig_lbl] = {
                    "water_mean_all_datasets": suf[orig_lbl].get("mean"),
                    "climate_dataset_mean":    round(float(scores_df[col].mean()), 4),
                }

    # Metadata completeness
    meta_completeness = {
        "has_open_license_pct":  round(float(df["is_open_license"].mean()   * 100), 2),
        "has_coordinates_pct":   round(float(df["has_coordinates"].mean()   * 100), 2),
        "has_temporal_pct":      round(float(df["has_temporal_flag"].mean() * 100), 2),
        "is_machine_readable_pct": round(float(df["is_machine_readable"].mean() * 100), 2),
        "has_eurovoc_pct":       round(float(df["has_eurovoc"].mean()       * 100), 2),
        "is_multilingual_pct":   round(float(df["has_multilingual"].mean()  * 100), 2),
        "has_description_pct":   round(float((df["description_length"] > 0).mean() * 100), 2),
    }

    summary = {
        "analysis_timestamp":    datetime.now().isoformat(),
        "total_records":         len(scores_df),
        "unique_datasets":       int(scores_df["dataset_uri"].nunique()),
        "domain_distribution":   scores_df["domain"].value_counts().to_dict(),
        "task_scores":           {task: _task_stats(task) for task in all_tasks},
        "domain_breakdown":      domain_breakdown,
        "water_comparison":      water_comparison,
        "metadata_completeness": meta_completeness,
        "discovery_gap_note":    (
            "nature_based_solutions domain returned 0 datasets in both primary "
            "and fallback SPARQL harvests. drought_early_warning: 500 datasets; "
            "climate_infrastructure: 494 datasets."
        ),
    }

    with open(OUTPUT_DIR / "climate_analysis_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("  Saved climate_analysis_summary.json")

    # ── 5. Console summary ────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("CLIMATE SUFFICIENCY SCORES SUMMARY")
    print("=" * 60)
    print(f"\nTotal datasets : {len(scores_df)}")
    print(f"Domains        : {scores_df['domain'].value_counts().to_dict()}")
    print()
    print(f"{'Task':<30s} {'Mean':>6} {'Median':>7} {'Ready%':>8} {'Partial%':>9} {'Insuff%':>9}")
    print("-" * 72)
    for task in all_tasks:
        st = _task_stats(task)
        if st:
            label = "climate_" + task if task in climate_tasks else task
            print(f"  {task:<28s} {st['mean']:>6.3f} {st['median']:>7.3f} "
                  f"{st['ready_pct']:>7.1f}% {st['partial_pct']:>8.1f}% "
                  f"{st['insufficient_pct']:>8.1f}%")

    print("\nMETADATA COMPLETENESS:")
    for k, v in meta_completeness.items():
        print(f"  {k:<30s}: {v:.1f}%")

    # ── 6. Spot-check 3 individual datasets ──────────────────────────────────
    print("\n=== SPOT-CHECK: 3 random dataset scores ===")
    sample = scores_df.sample(min(3, len(scores_df)), random_state=42)
    for _, row in sample.iterrows():
        print(f"\n  URI   : {row['dataset_uri'][:70]}")
        print(f"  Domain: {row['domain']}")
        for task in climate_tasks:
            col = f"{task}_score"
            print(f"  {task:<30s}: {row[col]:.4f}  ({row[f'{task}_readiness']})")

    print(f"\nAll outputs saved to: {OUTPUT_DIR}")
    return summary


if __name__ == "__main__":
    main()
