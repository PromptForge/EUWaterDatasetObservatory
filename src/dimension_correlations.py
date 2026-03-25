#!/usr/bin/env python3
"""
Compute correlations between individual metadata dimensions and Eurostat indicators.
Shows which specific metadata gaps are most strongly associated with country characteristics.
"""

import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path
import json


def run_dimension_correlations():
    ROOT = Path(__file__).parent.parent
    OUTPUT_DIR = ROOT / "data" / "outputs_real"

    # Load country stats and Eurostat indicators
    stats_path = OUTPUT_DIR / "country_detailed_stats.csv"
    eurostat_path = OUTPUT_DIR / "eurostat_indicators.csv"

    if not stats_path.exists():
        print("ERROR: Run country_detailed_stats.py first!")
        return None

    df_stats = pd.read_csv(stats_path)
    df_eurostat = pd.read_csv(eurostat_path)

    # Merge
    df = df_stats.merge(df_eurostat, on='country_code', how='inner')
    print(f"Merged data: {len(df)} countries")

    # Filter countries with >=10 datasets for reliable correlations
    df_reliable = df[df['n_datasets'] >= 10].copy()
    print(f"Countries with >=10 datasets: {len(df_reliable)}")

    if len(df_reliable) < 5:
        print("WARNING: Too few countries for reliable correlations. Using all countries.")
        df_reliable = df.copy()

    # Define dimension columns (those ending in _mean that are metadata dimensions)
    dimension_cols = [c for c in df_reliable.columns if c.endswith('_mean') and
                      any(dim in c for dim in ['temporal', 'spatial', 'format', 'license',
                                                'description', 'vocabulary', 'multilingual'])]

    # Also include task scores
    score_cols = ['early_warning_mean', 'compliance_mean', 'cross_border_mean', 'composite_mean']
    all_outcome_cols = [c for c in score_cols + dimension_cols if c in df_reliable.columns]

    # Eurostat indicators
    indicator_cols = [
        'gdp_per_capita_pps', 'desi_score', 'water_exploitation_index',
        'open_data_maturity', 'egovernment_usage_pct', 'data_infrastructure_score',
        'env_expenditure_pct_gdp'
    ]
    indicator_cols = [c for c in indicator_cols if c in df_reliable.columns]

    print(f"\nOutcome variables: {len(all_outcome_cols)}")
    print(f"  {all_outcome_cols}")
    print(f"Predictor variables: {len(indicator_cols)}")
    print(f"  {indicator_cols}")

    # Run correlations
    results = []

    for outcome in all_outcome_cols:
        for indicator in indicator_cols:
            # Get valid pairs
            mask = df_reliable[outcome].notna() & df_reliable[indicator].notna()
            x = df_reliable.loc[mask, indicator]
            y = df_reliable.loc[mask, outcome]

            if len(x) < 5:
                continue

            # Spearman
            rho, p_spearman = stats.spearmanr(x, y)

            # Pearson
            r, p_pearson = stats.pearsonr(x, y)

            results.append({
                'outcome': outcome,
                'indicator': indicator,
                'n': len(x),
                'spearman_rho': round(rho, 4),
                'spearman_p': round(p_spearman, 4),
                'pearson_r': round(r, 4),
                'pearson_p': round(p_pearson, 4),
                'significant_spearman': bool(p_spearman < 0.05),
                'significant_pearson': bool(p_pearson < 0.05)
            })

    df_results = pd.DataFrame(results)

    # Save
    output_path = OUTPUT_DIR / "dimension_correlations.csv"
    df_results.to_csv(output_path, index=False)
    print(f"\nSaved: {output_path}")
    print(f"Total correlation pairs computed: {len(df_results)}")

    # Report significant correlations
    print("\n" + "=" * 60)
    print("SIGNIFICANT CORRELATIONS (Spearman p < 0.05)")
    print("=" * 60)

    sig = df_results[df_results['significant_spearman']].copy()
    sig = sig.sort_values('spearman_rho', key=abs, ascending=False)

    if len(sig) > 0:
        for _, row in sig.iterrows():
            direction = "+" if row['spearman_rho'] > 0 else ""
            print(f"  {row['outcome']:30s} ↔ {row['indicator']:30s}: "
                  f"ρ={direction}{row['spearman_rho']:.3f}, p={row['spearman_p']:.4f}  (n={row['n']})")
    else:
        print("  No significant Spearman correlations found.")

    # Summary by indicator
    print("\n" + "=" * 60)
    print("CORRELATION SUMMARY BY INDICATOR")
    print("=" * 60)
    for indicator in indicator_cols:
        sub = df_results[df_results['indicator'] == indicator]
        sig_count = sub['significant_spearman'].sum()
        avg_abs_rho = sub['spearman_rho'].abs().mean()
        print(f"  {indicator:35s}: {sig_count:2d} significant, avg |ρ|={avg_abs_rho:.3f}")

    # Summary by outcome type
    print("\n" + "=" * 60)
    print("CORRELATION SUMMARY BY OUTCOME")
    print("=" * 60)
    for outcome in all_outcome_cols:
        sub = df_results[df_results['outcome'] == outcome].dropna(subset=['spearman_rho'])
        sig_count = sub['significant_spearman'].sum() if len(sub) > 0 else 0
        if len(sub) > 0:
            best_idx = sub['spearman_rho'].abs().idxmax()
            if pd.notna(best_idx):
                best_ind = sub.loc[best_idx, 'indicator']
                best_rho = sub.loc[best_idx, 'spearman_rho']
                print(f"  {outcome:35s}: {sig_count:2d} sig | best predictor: "
                      f"{best_ind} (ρ={best_rho:+.3f})")
            else:
                print(f"  {outcome:35s}: {sig_count:2d} sig | (no valid correlations)")
        else:
            print(f"  {outcome:35s}: {sig_count:2d} sig | (no data)")

    return df_results


if __name__ == "__main__":
    run_dimension_correlations()
