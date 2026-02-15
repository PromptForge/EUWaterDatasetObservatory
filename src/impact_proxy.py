"""
Composite Impact Proxy calculation.

5-component proxy for operational importance when download counts unavailable.
"""

import pandas as pd
import numpy as np
from pathlib import Path

def score_publisher_type(publisher_type):
    """Score based on publisher credibility and operational role."""
    scores = {
        "national_hydro": 1.0,
        "eu_agency": 0.9,
        "ministry": 0.7,
        "regional": 0.5,
        "research": 0.6,
        "other": 0.3
    }
    return scores.get(publisher_type, 0.3)

def score_operational_keywords(title, description):
    """Score based on operational keywords in title/description."""
    text = f"{title} {description}".lower()
    
    operational_terms = [
        "real-time", "realtime", "operational", "forecast", "early warning",
        "monitoring", "alert", "live", "current", "daily update"
    ]
    
    if any(term in text for term in operational_terms):
        return 1.0
    else:
        return 0.2

def score_format_accessibility(format_type):
    """Score based on format accessibility for operations."""
    scores = {
        "API": 1.0,
        "CSV": 0.9,
        "JSON": 0.9,
        "NetCDF": 0.85,
        "GeoTIFF": 0.8,
        "Shapefile": 0.75,
        "XML": 0.7,
        "Excel": 0.6,
        "PDF": 0.2,
        "": 0.1
    }
    return scores.get(format_type, 0.3)

def score_temporal_extent(temporal_span_years):
    """Score based on length of time series."""
    if temporal_span_years >= 20:
        return 1.0
    elif temporal_span_years >= 10:
        return 0.8
    elif temporal_span_years >= 5:
        return 0.6
    else:
        return 0.4

def score_spatial_scope(country):
    """Score based on geographic coverage."""
    if country == "EU":
        return 1.0
    else:
        return 0.6  # National/regional

def compute_impact(row):
    """Compute composite impact score."""
    weights = {
        "publisher_type": 0.30,
        "operational_keywords": 0.25,
        "format_accessibility": 0.20,
        "temporal_extent": 0.15,
        "spatial_scope": 0.10
    }
    
    scores = {
        "publisher_type": score_publisher_type(row.get("publisher_type", "")),
        "operational_keywords": score_operational_keywords(
            row.get("title", ""), row.get("description", "")
        ),
        "format_accessibility": score_format_accessibility(row.get("format", "")),
        "temporal_extent": score_temporal_extent(row.get("temporal_span_years", 0)),
        "spatial_scope": score_spatial_scope(row.get("country", ""))
    }
    
    impact = sum(weights[k] * scores[k] for k in weights)
    
    return impact, scores

def compute_priority(impact, sufficiency):
    """Compute priority score: high impact + low sufficiency = high priority."""
    return impact * (1 - sufficiency)

def main():
    # Load data
    data_path = Path(__file__).parent.parent / "data" / "outputs" / "sufficiency_scores.csv"
    df = pd.read_csv(data_path)
    
    print("Computing impact and priority scores...")
    
    # Compute impact for each record
    impacts = []
    for idx, row in df.iterrows():
        impact, scores = compute_impact(row)
        impacts.append({
            "dataset_id": row["dataset_id"],
            "impact_score": impact,
            **{f"impact_{k}": v for k, v in scores.items()}
        })
    
    impact_df = pd.DataFrame(impacts)
    df = df.merge(impact_df, on="dataset_id")
    
    # Compute priority for each task
    for task in ["early_warning", "compliance_reporting", "cross_border"]:
        df[f"{task}_priority"] = df.apply(
            lambda row: compute_priority(row["impact_score"], row[f"{task}_score"]),
            axis=1
        )
    
    # Save results
    output_path = Path(__file__).parent.parent / "data" / "outputs" / "priority_scores.csv"
    df.to_csv(output_path, index=False)
    
    # Generate top priority fixes
    print("\n=== TOP 20 PRIORITY FIXES (Early Warning) ===")
    top_ew = df.nlargest(20, "early_warning_priority")[
        ["dataset_id", "title", "domain", "impact_score", "early_warning_score", "early_warning_priority"]
    ]
    print(top_ew.to_string(index=False))
    
    # Save priority fixes
    priority_path = Path(__file__).parent.parent / "data" / "outputs" / "priority_fixes.csv"
    
    all_priorities = []
    for task in ["early_warning", "compliance_reporting", "cross_border"]:
        top = df.nlargest(20, f"{task}_priority").copy()
        top["task"] = task
        top["priority_rank"] = range(1, 21)
        all_priorities.append(top)
    
    pd.concat(all_priorities).to_csv(priority_path, index=False)
    print(f"\nSaved priority fixes to {priority_path}")
    
    return df

if __name__ == "__main__":
    main()
