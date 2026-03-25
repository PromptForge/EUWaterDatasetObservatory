"""
EU Water Dataset Observatory - Climate Adaptation Data Scorer

Scores harvested climate datasets against three climate-specific task profiles:
  - drought_early_warning    : drought monitoring & forecasting
  - climate_infrastructure   : climate-resilient infrastructure planning
  - nbs_monitoring           : nature-based solutions monitoring

Reuses the existing column-preparation helpers from score_real_data and calls
the same sufficiency_scoring / sensitivity_analysis / impact_proxy modules.

Inputs:  data/harvested/climate_harvest.csv
Outputs: data/outputs_climate/climate_sufficiency_scores.csv
         data/outputs_climate/climate_sensitivity_summary.csv
         data/outputs_climate/climate_sensitivity_detail.csv
         data/outputs_climate/climate_priority_scores.csv
         data/outputs_climate/climate_analysis_summary.json
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import sys

# Ensure sibling imports work when called from any cwd
sys.path.insert(0, str(Path(__file__).parent))

from sufficiency_scoring import compute_all_sufficiency_scores, classify_readiness
from sensitivity_analysis import run_sensitivity_analysis
from impact_proxy import compute_impact, compute_priority

# Re-use all column-prep helpers from score_real_data
from score_real_data import (
    _parse_date_to_str,
    _extract_format,
    _infer_publisher_type,
    _infer_update_frequency,
    _calc_temporal_span,
    _infer_spatial_precision,
    _count_keywords,
    _count_languages,
    _is_open_license,
)

# ─── Climate task weight profiles ────────────────────────────────────────────

CLIMATE_TASKS = {
    "drought_early_warning": {
        "temporal_recency":    0.20,
        "temporal_coverage":   0.25,   # Long time series critical
        "update_frequency":    0.15,
        "spatial_precision":   0.15,
        "format_readability":  0.10,
        "license_openness":    0.05,
        "description_quality": 0.05,
        "vocabulary_standard": 0.05,
    },
    "climate_infrastructure": {
        "temporal_coverage":   0.20,   # Need projections/long history
        "spatial_precision":   0.25,   # Location-specific planning
        "format_readability":  0.15,
        "description_quality": 0.15,   # Methodology documentation
        "license_openness":    0.10,
        "vocabulary_standard": 0.10,
        "temporal_recency":    0.05,
    },
    "nbs_monitoring": {
        "temporal_coverage":   0.25,   # Long-term monitoring essential
        "description_quality": 0.20,   # Need methodology docs
        "spatial_precision":   0.15,
        "format_readability":  0.15,
        "vocabulary_standard": 0.10,
        "license_openness":    0.10,
        "temporal_recency":    0.05,
    },
}

THRESHOLDS = {"ready": 0.70, "partial": 0.40}


# ─── Column mapping (mirrors prepare_harvested_data in score_real_data.py) ───

def prepare_climate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map climate_harvest.csv columns to the schema expected by
    sufficiency_scoring, sensitivity_analysis and impact_proxy.
    """
    prepared = pd.DataFrame()

    # ── Core identifiers & text ──────────────────────────────────────────────
    prepared["dataset_id"]  = df["dataset_uri"].fillna("").astype(str)
    prepared["title"]       = df["title"].fillna("").astype(str)
    prepared["description"] = df["description"].fillna("").astype(str)
    prepared["domain"]      = df["climate_domain"].fillna("climate_general").astype(str)
    prepared["country"]     = df.get("country", pd.Series("unknown", index=df.index)).fillna("unknown").astype(str)

    # ── Temporal ─────────────────────────────────────────────────────────────
    prepared["last_modified"] = df["modified"].apply(_parse_date_to_str)
    prepared["temporal_span_years"] = df.apply(
        lambda r: _calc_temporal_span(r.get("temporal_start"), r.get("temporal_end")),
        axis=1,
    )
    prepared["update_frequency"] = df.apply(
        lambda r: _infer_update_frequency(r.get("title", ""), r.get("description", "")),
        axis=1,
    )

    # ── Spatial ──────────────────────────────────────────────────────────────
    has_coords = df.get("has_coordinates", pd.Series(False, index=df.index)).fillna(False)
    if has_coords.dtype == object:
        has_coords = has_coords.map(lambda x: str(x).strip().lower() == "true")
    prepared["has_coordinates"]   = has_coords.astype(bool)
    prepared["spatial_precision"] = df.apply(
        lambda r: _infer_spatial_precision(
            r.get("spatial", ""),
            prepared.loc[r.name, "has_coordinates"],
        ),
        axis=1,
    )

    # ── License ──────────────────────────────────────────────────────────────
    prepared["is_open_license"] = df["license"].apply(_is_open_license)
    prepared["license"]         = df["license"].fillna("").astype(str)

    # ── Format ───────────────────────────────────────────────────────────────
    prepared["format"] = df["format"].apply(_extract_format)

    # ── Description quality ──────────────────────────────────────────────────
    prepared["description_length"] = prepared["description"].apply(
        lambda x: len(str(x).split()) if x else 0
    )

    # ── Keywords / vocabulary ─────────────────────────────────────────────────
    prepared["num_keywords"] = df["keyword"].apply(_count_keywords)
    prepared["has_eurovoc"]  = (
        (prepared["num_keywords"] >= 3) &
        prepared["country"].isin(["eu"])
    )

    # ── Languages ────────────────────────────────────────────────────────────
    prepared["num_languages"]    = df["language"].apply(_count_languages)
    prepared["has_multilingual"] = prepared["num_languages"] >= 2

    # ── Publisher type ────────────────────────────────────────────────────────
    prepared["publisher_type"] = df.apply(
        lambda r: _infer_publisher_type(
            r.get("publisher_name", ""), r.get("publisher_uri", "")
        ),
        axis=1,
    )
    prepared["publisher"] = df.get(
        "publisher_name", pd.Series("", index=df.index)
    ).fillna("").astype(str)

    # ── Pass-through fields ───────────────────────────────────────────────────
    prepared["dataset_uri"]       = df["dataset_uri"].fillna("").astype(str)
    prepared["climate_domain"]    = df["climate_domain"].fillna("climate_general").astype(str)
    prepared["modified_date"]     = df["modified"].fillna("").astype(str)
    prepared["temporal_start"]    = df.get("temporal_start", pd.Series("", index=df.index)).fillna("").astype(str)
    prepared["temporal_end"]      = df.get("temporal_end",   pd.Series("", index=df.index)).fillna("").astype(str)
    prepared["keyword_raw"]       = df["keyword"].fillna("").astype(str)
    prepared["language_raw"]      = df["language"].fillna("").astype(str)
    prepared["has_license_flag"]  = df.get("has_license", pd.Series(False, index=df.index)).map(
        lambda x: str(x).strip().lower() == "true"
    )
    prepared["has_temporal_flag"] = df.get("has_temporal", pd.Series(False, index=df.index)).map(
        lambda x: str(x).strip().lower() == "true"
    )
    prepared["is_machine_readable"] = df.get("is_machine_readable", pd.Series(False, index=df.index)).map(
        lambda x: str(x).strip().lower() == "true"
    )

    print(f"  Prepared {len(prepared)} records with {len(prepared.columns)} columns")
    return prepared


# ─── Main analysis pipeline ───────────────────────────────────────────────────

def main():
    ROOT        = Path(__file__).parent.parent
    INPUT_PATH  = ROOT / "data" / "harvested" / "climate_harvest.csv"
    OUTPUT_DIR  = ROOT / "data" / "outputs_climate"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("CLIMATE ADAPTATION DATA SCORING")
    print("=" * 60)

    if not INPUT_PATH.exists():
        print(f"ERROR: {INPUT_PATH} not found!")
        print("Run harvest_climate.py first.")
        return

    # ── 1. Load & prepare ────────────────────────────────────────────────────
    print("\n=== Loading & preparing climate harvest data ===")
    raw_df = pd.read_csv(INPUT_PATH, low_memory=False)
    print(f"  Loaded {len(raw_df)} rows, columns: {list(raw_df.columns)}")
    df = prepare_climate_data(raw_df)
    df.to_csv(OUTPUT_DIR / "climate_prepared_records.csv", index=False)
    print(f"  Saved climate_prepared_records.csv ({len(df)} rows)")

    # ── 2. Sufficiency scoring (uses existing compute_all_sufficiency_scores) ─
    #
    # compute_all_sufficiency_scores uses the task weights defined in weights.json
    # for early_warning / compliance_reporting / cross_border.
    # We additionally compute the three climate task scores here using the
    # CLIMATE_TASKS weight profiles.
    # ─────────────────────────────────────────────────────────────────────────
    print("\n=== Computing sufficiency scores (base + climate tasks) ===")
    base_scores_df = compute_all_sufficiency_scores(df)
    full_df = df.merge(base_scores_df, on="dataset_id", how="left")

    # Climate task scoring (manual weighted sum matching sufficiency_scoring logic)
    from sufficiency_scoring import (
        score_temporal_recency,
        score_temporal_coverage,
        score_update_frequency,
        score_spatial_precision,
        score_format_readability,
        score_license_openness,
        score_description_quality,
        score_vocabulary_standard,
    )

    def _compute_climate_score(row: pd.Series, weights: dict) -> float:
        """Weighted sum using the same individual metric scorers."""
        metric_funcs = {
            "temporal_recency":    score_temporal_recency,
            "temporal_coverage":   score_temporal_coverage,
            "update_frequency":    score_update_frequency,
            "spatial_precision":   score_spatial_precision,
            "format_readability":  score_format_readability,
            "license_openness":    score_license_openness,
            "description_quality": score_description_quality,
            "vocabulary_standard": score_vocabulary_standard,
        }
        total = 0.0
        for metric, weight in weights.items():
            fn = metric_funcs.get(metric)
            if fn:
                try:
                    total += weight * fn(row)
                except Exception:
                    total += 0.0
        return round(total, 4)

    for task_name, weights in CLIMATE_TASKS.items():
        col = f"{task_name}_score"
        print(f"  Scoring: {task_name} …")
        full_df[col] = full_df.apply(
            lambda r, w=weights: _compute_climate_score(r, w), axis=1
        )

    full_df.to_csv(OUTPUT_DIR / "climate_sufficiency_scores.csv", index=False)
    print(f"  Saved climate_sufficiency_scores.csv ({len(full_df)} rows)")

    # ── 3. Sensitivity analysis ───────────────────────────────────────────────
    print("\n=== Running sensitivity analysis (±25% weight perturbation) ===")
    # Pass only climate task score columns to the sensitivity analyser.
    # run_sensitivity_analysis looks for columns ending in '_score'.
    climate_score_cols = [f"{t}_score" for t in CLIMATE_TASKS]
    sens_df = full_df[["dataset_id"] + climate_score_cols +
                       list(df.columns.difference(["dataset_id"]))].copy()

    try:
        sens_results     = run_sensitivity_analysis(sens_df, perturbation=0.25)
        sens_summary_df  = pd.DataFrame(sens_results["summary"])
        sens_detail_df   = pd.DataFrame(sens_results["detail"])
        sens_summary_df.to_csv(OUTPUT_DIR / "climate_sensitivity_summary.csv", index=False)
        sens_detail_df.to_csv(OUTPUT_DIR  / "climate_sensitivity_detail.csv",  index=False)
        print("  Saved climate_sensitivity_summary.csv & climate_sensitivity_detail.csv")
        has_sensitivity = True
    except Exception as e:
        print(f"  Sensitivity analysis skipped: {e}")
        sens_summary_df = pd.DataFrame()
        has_sensitivity = False

    # ── 4. Impact & priority scores ───────────────────────────────────────────
    print("\n=== Computing impact & priority scores ===")
    impacts = []
    for _, row in full_df.iterrows():
        impact_val, impact_components = compute_impact(row)
        impacts.append({
            "dataset_id":  row["dataset_id"],
            "impact_score": impact_val,
            **{f"impact_{k}": v for k, v in impact_components.items()},
        })
    impact_df = pd.DataFrame(impacts)
    full_df = full_df.merge(impact_df, on="dataset_id", how="left")

    for task_name in CLIMATE_TASKS:
        score_col    = f"{task_name}_score"
        priority_col = f"{task_name}_priority"
        if score_col in full_df.columns:
            full_df[priority_col] = full_df.apply(
                lambda r, sc=score_col: compute_priority(r["impact_score"], r[sc]),
                axis=1,
            )

    # Top-20 priority fixes per climate task
    priority_rows = []
    for task_name in CLIMATE_TASKS:
        priority_col = f"{task_name}_priority"
        if priority_col in full_df.columns:
            top = full_df.nlargest(20, priority_col).copy()
            top["task"]          = task_name
            top["priority_rank"] = range(1, len(top) + 1)
            priority_rows.append(top)

    if priority_rows:
        priority_df = pd.concat(priority_rows, ignore_index=True)
        priority_df.to_csv(OUTPUT_DIR / "climate_priority_fixes.csv", index=False)
        print("  Saved climate_priority_fixes.csv")

    full_df.to_csv(OUTPUT_DIR / "climate_priority_scores.csv", index=False)
    print(f"  Saved climate_priority_scores.csv ({len(full_df)} rows)")

    # ── 5. Summary statistics ─────────────────────────────────────────────────
    print("\n=== Generating summary statistics ===")

    def _pct(col, label):
        return float(
            (full_df[col].apply(classify_readiness) == label).mean() * 100
        )

    task_stats = {}
    for task_name in CLIMATE_TASKS:
        col = f"{task_name}_score"
        if col not in full_df.columns:
            continue
        task_stats[task_name] = {
            "mean":             float(full_df[col].mean()),
            "std":              float(full_df[col].std()),
            "median":           float(full_df[col].median()),
            "ready_pct":        _pct(col, "Ready"),
            "partial_pct":      _pct(col, "Partial"),
            "insufficient_pct": _pct(col, "Insufficient"),
        }

    summary = {
        "analysis_timestamp": datetime.now().isoformat(),
        "data_source":        "data.europa.eu SPARQL harvest (climate keywords)",
        "total_records":      len(full_df),
        "unique_datasets":    int(full_df["dataset_id"].nunique()),

        "climate_domain_distribution": (
            full_df["climate_domain"].value_counts().to_dict()
            if "climate_domain" in full_df.columns else {}
        ),
        "country_distribution": full_df["country"].value_counts().head(15).to_dict(),

        "task_scores": task_stats,

        "metadata_completeness": {
            "has_open_license":    float(full_df["is_open_license"].mean() * 100),
            "has_coordinates":     float(full_df["has_coordinates"].mean() * 100),
            "has_temporal":        float(full_df["has_temporal_flag"].mean() * 100),
            "is_machine_readable": float(full_df["is_machine_readable"].mean() * 100),
            "has_eurovoc":         float(full_df["has_eurovoc"].mean() * 100),
            "is_multilingual":     float(full_df["has_multilingual"].mean() * 100),
        },

        "sensitivity": (
            {
                row["task"]: {
                    "primary_metric": row["primary_metric"],
                    "baseline_mean":  float(row["baseline_mean"]),
                    "range_pp":       float(row["range_pp"]),
                    "low_ready_pct":  float(row["low_ready_pct"]),
                    "high_ready_pct": float(row["high_ready_pct"]),
                }
                for _, row in sens_summary_df.iterrows()
            }
            if has_sensitivity and not sens_summary_df.empty else {}
        ),
    }

    with open(OUTPUT_DIR / "climate_analysis_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("  Saved climate_analysis_summary.json")

    # ── 6. Print key results ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("CLIMATE ADAPTATION READINESS RESULTS")
    print("=" * 60)
    print(f"\nTotal datasets analyzed  : {summary['total_records']}")
    print(f"Unique dataset URIs      : {summary['unique_datasets']}")

    print("\nCLIMATE DOMAIN DISTRIBUTION:")
    for dom, cnt in summary["climate_domain_distribution"].items():
        print(f"  {dom}: {cnt}")

    print("\nSUFFICIENCY SCORES — CLIMATE TASKS (Mean ± SD):")
    for task, s in task_stats.items():
        print(
            f"  {task:28s}: {s['mean']:.3f} ± {s['std']:.3f}  "
            f"Ready={s['ready_pct']:.1f}%  "
            f"Partial={s['partial_pct']:.1f}%  "
            f"Insufficient={s['insufficient_pct']:.1f}%"
        )

    print("\nMETADATA COMPLETENESS (% of datasets):")
    for field, pct in summary["metadata_completeness"].items():
        print(f"  {field:25s}: {pct:.1f}%")

    print(f"\nAll climate results saved to: {OUTPUT_DIR}")
    return summary


if __name__ == "__main__":
    main()
