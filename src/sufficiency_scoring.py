"""
Task-specific sufficiency scoring engine.

Computes sufficiency scores for three management tasks:
1. Early Warning Systems
2. Compliance Reporting
3. Cross-Border Coordination
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

CONFIG_DIR = Path(__file__).parent.parent / "config"

def load_weights():
    with open(CONFIG_DIR / "weights.json") as f:
        return json.load(f)

def score_temporal_recency(row, threshold_days=90):
    """Score based on how recently data was modified."""
    if not row["last_modified"]:
        return 0.0
    
    try:
        last_mod = datetime.strptime(row["last_modified"], "%Y-%m-%d")
        days_ago = (datetime.now() - last_mod).days
        
        if days_ago <= 3:  # Within 72 hours
            return 1.0
        elif days_ago <= 30:
            return 0.8
        elif days_ago <= threshold_days:
            return 0.5
        elif days_ago <= 365:
            return 0.3
        else:
            return 0.1
    except:
        return 0.0

def score_update_frequency(row):
    """Score based on update frequency metadata."""
    freq = row.get("update_frequency", "")
    if pd.isna(freq):
        freq = ""
    freq = str(freq).lower()
    
    scores = {
        "daily": 1.0,
        "weekly": 0.8,
        "monthly": 0.6,
        "annual": 0.3,
        "irregular": 0.2,
        "unknown": 0.0
    }
    
    return scores.get(freq, 0.0)

def score_spatial_precision(row):
    """Score based on spatial metadata quality."""
    if row.get("has_coordinates"):
        return 1.0
    
    precision = row.get("spatial_precision", "")
    if pd.isna(precision):
        precision = ""
    precision = str(precision).lower()
    
    if precision == "bbox":
        return 0.8
    elif precision == "region_name":
        return 0.5
    elif precision == "country_level":
        return 0.3
    else:
        return 0.0

def score_format_readability(row):
    """Score based on machine-readability of format."""
    fmt = row.get("format", "")
    if pd.isna(fmt):
        fmt = ""
    fmt = str(fmt).lower()
    
    scores = {
        "api": 1.0,
        "csv": 0.9,
        "json": 0.9,
        "netcdf": 0.85,
        "geotiff": 0.8,
        "shapefile": 0.75,
        "xml": 0.7,
        "excel": 0.6,
        "pdf": 0.2,
        "": 0.0
    }
    
    return scores.get(fmt, 0.3)

def score_license_openness(row):
    """Score based on license openness."""
    if row.get("is_open_license"):
        return 1.0
    else:
        license_val = row.get("license", "")
        if pd.isna(license_val):
            license_val = ""
        if license_val and str(license_val).strip():
            return 0.3  # Has license but restricted
        else:
            return 0.0  # No license information

def score_description_quality(row, min_words=100):
    """Score based on description completeness."""
    length = row.get("description_length", 0)
    
    if length >= min_words:
        return 1.0
    elif length >= 50:
        return 0.7
    elif length >= 20:
        return 0.4
    elif length > 0:
        return 0.2
    else:
        return 0.0

def score_vocabulary_standard(row):
    """Score based on controlled vocabulary usage."""
    if row.get("has_eurovoc"):
        return 1.0
    elif row.get("num_keywords", 0) >= 3:
        return 0.5
    elif row.get("num_keywords", 0) > 0:
        return 0.3
    else:
        return 0.0

def score_temporal_coverage(row, min_years=6):
    """Score based on temporal span (for compliance reporting)."""
    span = row.get("temporal_span_years", 0)
    
    if span >= min_years:
        return 1.0
    elif span >= 3:
        return 0.6
    elif span >= 1:
        return 0.3
    else:
        return 0.0

def score_spatial_aggregation(row):
    """Score based on spatial aggregation level."""
    precision = row.get("spatial_precision", "")
    if pd.isna(precision):
        precision = ""
    precision = str(precision).lower()
    
    # For compliance, river basin or water body level is preferred
    if precision in ["coordinates", "bbox"]:
        return 0.8  # Detailed but may need aggregation
    elif precision == "region_name":
        return 1.0  # Likely river basin level
    elif precision == "country_level":
        return 0.5
    else:
        return 0.0

def score_multilingual(row, min_languages=2):
    """Score based on multilingual metadata."""
    num_lang = row.get("num_languages", 1)
    
    if num_lang >= min_languages:
        return 1.0
    else:
        return 0.3

def score_spatial_coverage(row):
    """Score based on transboundary potential."""
    country = row.get("country", "")
    if pd.isna(country):
        country = ""
    
    if country == "EU":
        return 1.0
    elif row.get("has_multilingual"):
        return 0.7  # Suggests cross-border intent
    else:
        return 0.4  # Single country

def compute_sufficiency(row, task, weights):
    """Compute weighted sufficiency score for a task."""
    
    # Define metric functions for each task
    metric_functions = {
        "temporal_recency": score_temporal_recency,
        "update_frequency": score_update_frequency,
        "spatial_precision": score_spatial_precision,
        "format_readability": score_format_readability,
        "license_openness": score_license_openness,
        "description_quality": score_description_quality,
        "vocabulary_standard": score_vocabulary_standard,
        "temporal_coverage": score_temporal_coverage,
        "spatial_aggregation": score_spatial_aggregation,
        "multilingual": score_multilingual,
        "spatial_coverage": score_spatial_coverage
    }
    
    task_weights = weights[task]
    
    weighted_sum = 0.0
    weight_total = 0.0
    
    scores_detail = {}
    
    for metric, weight in task_weights.items():
        if metric in metric_functions:
            score = metric_functions[metric](row)
            scores_detail[metric] = score
            weighted_sum += weight * score
            weight_total += weight
    
    sufficiency = weighted_sum / weight_total if weight_total > 0 else 0.0
    
    return sufficiency, scores_detail

def classify_readiness(score):
    """Classify sufficiency score into readiness category."""
    if score >= 0.7:
        return "Ready"
    elif score >= 0.4:
        return "Partial"
    else:
        return "Insufficient"

def compute_all_sufficiency_scores(df):
    """Compute sufficiency scores for all records and all tasks."""
    weights = load_weights()
    
    tasks = ["early_warning", "compliance_reporting", "cross_border"]
    
    results = []
    
    for idx, row in df.iterrows():
        record_scores = {"dataset_id": row["dataset_id"]}
        
        for task in tasks:
            score, detail = compute_sufficiency(row, task, weights)
            record_scores[f"{task}_score"] = score
            record_scores[f"{task}_readiness"] = classify_readiness(score)
            
            # Add individual metric scores
            for metric, value in detail.items():
                record_scores[f"{task}_{metric}"] = value
        
        results.append(record_scores)
    
    return pd.DataFrame(results)

def main():
    # Load simulated data
    data_path = Path(__file__).parent.parent / "data" / "simulated" / "metadata_records.csv"
    df = pd.read_csv(data_path)
    
    print(f"Computing sufficiency scores for {len(df)} records...")
    
    scores_df = compute_all_sufficiency_scores(df)
    
    # Merge with original data
    result_df = df.merge(scores_df, on="dataset_id")
    
    # Save results
    output_path = Path(__file__).parent.parent / "data" / "outputs" / "sufficiency_scores.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(output_path, index=False)
    
    print(f"\nSaved scores to {output_path}")
    
    # Print summary
    print("\n=== SUFFICIENCY SCORE SUMMARY ===")
    
    for task in ["early_warning", "compliance_reporting", "cross_border"]:
        print(f"\n{task.upper().replace('_', ' ')}:")
        print(f"  Mean score: {result_df[f'{task}_score'].mean():.3f}")
        print(f"  Median score: {result_df[f'{task}_score'].median():.3f}")
        print(f"  Readiness distribution:")
        print(result_df[f"{task}_readiness"].value_counts())
    
    # By domain
    print("\n=== MEAN SCORES BY DOMAIN ===")
    domain_means = result_df.groupby("domain")[["early_warning_score", "compliance_reporting_score", "cross_border_score"]].mean()
    print(domain_means.round(3))
    
    return result_df

if __name__ == "__main__":
    main()
