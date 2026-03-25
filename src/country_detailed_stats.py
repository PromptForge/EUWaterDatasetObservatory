#!/usr/bin/env python3
"""
Compute detailed country-level statistics for Idea 3 requirements:
- Median sufficiency per task (not just mean)
- % in each readiness category per country
- Mean dimension scores per country
"""

import pandas as pd
import numpy as np
from pathlib import Path


def compute_detailed_country_stats():
    ROOT = Path(__file__).parent.parent
    OUTPUT_DIR = ROOT / "data" / "outputs_real"

    # Load from datasets_with_country.csv which has country_extracted (ISO codes)
    # This file contains both scores AND country_extracted
    country_path = OUTPUT_DIR / "datasets_with_country.csv"
    df = pd.read_csv(country_path)

    print(f"Loaded {len(df)} total records from datasets_with_country.csv")

    # Use country_extracted for proper ISO country codes
    if 'country_extracted' in df.columns:
        df['country'] = df['country_extracted']
    elif 'country' in df.columns:
        df['country'] = df['country']
    else:
        print("ERROR: No country column found")
        return None

    # Filter to known EU-27 countries (exclude 'unknown' and 'EU' multi-country entries)
    eu27 = {'AT','BE','BG','HR','CY','CZ','DK','EE','FI','FR',
            'DE','GR','HU','IE','IT','LV','LT','LU','MT','NL',
            'PL','PT','RO','SK','SI','ES','SE'}

    df = df[df['country'].isin(eu27)].copy()
    print(f"Analyzing {len(df)} datasets from {df['country'].nunique()} EU-27 countries")

    # Correct task score column names
    task_map = {
        'early_warning':  'early_warning_score',
        'compliance':     'compliance_reporting_score',
        'cross_border':   'cross_border_score'
    }

    # Identify numeric dimension sub-score columns
    # These are columns starting with task prefixes but are NOT the main score/readiness columns
    exclude_cols = {'early_warning_score', 'early_warning_readiness',
                    'compliance_reporting_score', 'compliance_reporting_readiness',
                    'cross_border_score', 'cross_border_readiness'}

    task_prefixes = ('early_warning_', 'compliance_reporting_', 'cross_border_')
    numeric_dim_cols = [
        c for c in df.columns
        if c not in exclude_cols
        and any(c.startswith(p) for p in task_prefixes)
        and pd.api.types.is_numeric_dtype(df[c])
    ]

    print(f"Task score columns:      {list(task_map.values())}")
    print(f"Numeric dimension cols:  {len(numeric_dim_cols)} found")

    # Compute per-country statistics
    results = []

    for country, group in df.groupby('country'):
        row = {'country_code': country, 'n_datasets': len(group)}

        # Task scores: mean, median, std + readiness percentages
        for task_label, score_col in task_map.items():
            if score_col not in group.columns:
                continue
            vals = pd.to_numeric(group[score_col], errors='coerce').dropna()
            if len(vals) == 0:
                continue

            row[f'{task_label}_mean']   = round(vals.mean(), 4)
            row[f'{task_label}_median'] = round(vals.median(), 4)
            row[f'{task_label}_std']    = round(vals.std(), 4)

            # Readiness percentages
            n = len(vals)
            row[f'{task_label}_ready_pct']       = round(100 * (vals >= 0.70).sum() / n, 2)
            row[f'{task_label}_partial_pct']     = round(100 * ((vals >= 0.40) & (vals < 0.70)).sum() / n, 2)
            row[f'{task_label}_insufficient_pct']= round(100 * (vals < 0.40).sum() / n, 2)

        # Composite score across all three tasks
        avail_score_cols = [sc for sc in task_map.values() if sc in group.columns]
        if avail_score_cols:
            numeric_tasks = group[avail_score_cols].apply(pd.to_numeric, errors='coerce')
            composite = numeric_tasks.mean(axis=1)
            row['composite_mean']   = round(composite.mean(), 4)
            row['composite_median'] = round(composite.median(), 4)
            row['composite_std']    = round(composite.std(), 4)

        # Dimension sub-scores (mean per country)
        for dim in numeric_dim_cols:
            vals = pd.to_numeric(group[dim], errors='coerce').dropna()
            if len(vals) > 0:
                row[f'{dim}_mean'] = round(vals.mean(), 4)

        results.append(row)

    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values('n_datasets', ascending=False).reset_index(drop=True)

    # Save
    output_path = OUTPUT_DIR / "country_detailed_stats.csv"
    df_results.to_csv(output_path, index=False)
    print(f"\nSaved: {output_path}")
    print(f"Output shape: {df_results.shape[0]} countries × {df_results.shape[1]} columns")

    # ── Print summary ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("COUNTRY DETAILED STATISTICS")
    print("=" * 60)

    print(f"\nCountries analyzed: {len(df_results)}")
    print(f"Total datasets:     {df_results['n_datasets'].sum()}")
    print(f"Datasets per country: min={df_results['n_datasets'].min()}, "
          f"max={df_results['n_datasets'].max()}, "
          f"median={df_results['n_datasets'].median():.0f}")

    # Flag countries with <10 datasets
    small = df_results[df_results['n_datasets'] < 10]
    if len(small) > 0:
        print(f"\n⚠️  Countries with <10 datasets (unreliable for stats):")
        for _, r in small.iterrows():
            print(f"   {r['country_code']}: n={r['n_datasets']}")

    # Top / bottom 5 by composite mean
    if 'composite_mean' in df_results.columns:
        print(f"\n--- Top 5 Countries by Composite Mean ---")
        for _, r in df_results.nlargest(5, 'composite_mean').iterrows():
            print(f"   {r['country_code']}: {r['composite_mean']:.3f}  (n={r['n_datasets']})")

        print(f"\n--- Bottom 5 Countries by Composite Mean ---")
        for _, r in df_results.nsmallest(5, 'composite_mean').iterrows():
            print(f"   {r['country_code']}: {r['composite_mean']:.3f}  (n={r['n_datasets']})")

    # Readiness breakdown
    for task_label in task_map:
        rc = f'{task_label}_ready_pct'
        if rc in df_results.columns:
            print(f"\n--- {task_label.upper()} Readiness (mean across countries) ---")
            print(f"   Ready   (≥70%):      {df_results[rc].mean():.1f}%")
            print(f"   Partial (40-70%):    {df_results[f'{task_label}_partial_pct'].mean():.1f}%")
            print(f"   Insufficient (<40%): {df_results[f'{task_label}_insufficient_pct'].mean():.1f}%")

    return df_results


if __name__ == "__main__":
    compute_detailed_country_stats()
