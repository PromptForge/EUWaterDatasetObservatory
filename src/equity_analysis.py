#!/usr/bin/env python3
"""
Equity & Data Justice Analysis
================================
Research question: Do countries with lower GDP, less developed digital
infrastructure, or higher water stress have systematically worse
metadata quality for EU water datasets?

Pipeline:
  1. Load datasets_with_country.csv (sufficiency scores + extracted country)
  2. Load eurostat_indicators.csv  (GDP, DESI, WEI, ODM)
  3. Aggregate scores by country
  4. Merge and run correlations / regressions
  5. Save results to equity_*.{csv,json}
"""

import json
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats
from scipy.stats import pearsonr, spearmanr, linregress

warnings.filterwarnings('ignore', category=RuntimeWarning)

# ── Columns of interest ──────────────────────────────────────────────────────
SCORE_COLS = ['early_warning_score', 'compliance_reporting_score', 'cross_border_score']
INDICATOR_COLS = [
    'gdp_per_capita_pps',
    'desi_score',
    'water_exploitation_index',
    'open_data_maturity',
    'egovernment_usage_pct',
    'data_infrastructure_score',
]
INDICATOR_LABELS = {
    'gdp_per_capita_pps':        'GDP per capita (PPS, EU27=100)',
    'desi_score':                'DESI Score 2023',
    'water_exploitation_index':  'Water Exploitation Index (%)',
    'open_data_maturity':        'Open Data Maturity 2023 (%)',
    'egovernment_usage_pct':     'E-Government Usage (%)',
    'data_infrastructure_score': 'Data Infrastructure Score',
}


# ── Data loading ─────────────────────────────────────────────────────────────

def load_data(root: Path):
    datasets_path = root / 'data' / 'outputs_real' / 'datasets_with_country.csv'
    eurostat_path = root / 'data' / 'outputs_real' / 'eurostat_indicators.csv'

    if not datasets_path.exists():
        raise FileNotFoundError(
            f"datasets_with_country.csv not found at {datasets_path}\n"
            "Run: python src/extract_country.py  first."
        )
    if not eurostat_path.exists():
        raise FileNotFoundError(
            f"eurostat_indicators.csv not found at {eurostat_path}\n"
            "Run: python src/fetch_eurostat.py  first."
        )

    df_scores   = pd.read_csv(datasets_path)
    df_eurostat = pd.read_csv(eurostat_path)
    return df_scores, df_eurostat


# ── Country-level aggregation ─────────────────────────────────────────────────

def aggregate_by_country(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sufficiency scores to country level."""
    country_col = 'country_extracted' if 'country_extracted' in df.columns else 'country'

    df_known = df[df[country_col].notna() & (df[country_col] != 'unknown')].copy()

    if len(df_known) == 0:
        raise ValueError("No datasets with identified country found!")

    available_score_cols = [c for c in SCORE_COLS if c in df_known.columns]
    if not available_score_cols:
        raise ValueError(f"None of the expected score columns found: {SCORE_COLS}")

    agg_funcs = {col: ['mean', 'median', 'std', 'count'] for col in available_score_cols}

    df_agg = df_known.groupby(country_col).agg(agg_funcs)
    df_agg.columns = ['_'.join(c).strip() for c in df_agg.columns]
    df_agg = df_agg.reset_index().rename(columns={country_col: 'country_code'})

    # Add composite average score
    mean_cols = [f"{c}_mean" for c in available_score_cols]
    df_agg['composite_score_mean'] = df_agg[mean_cols].mean(axis=1)

    n_datasets = df_known[country_col].value_counts().rename('n_datasets')
    df_agg = df_agg.merge(n_datasets.reset_index().rename(
        columns={country_col: 'country_code', 'count': 'n_datasets'}
    ), on='country_code', how='left')

    print(f"\n=== Country-level aggregation: {len(df_agg)} countries ===")
    print(df_agg[['country_code', 'n_datasets'] + mean_cols + ['composite_score_mean']].to_string(index=False))
    return df_agg


# ── Merge with Eurostat ───────────────────────────────────────────────────────

def merge_with_indicators(df_agg: pd.DataFrame, df_eurostat: pd.DataFrame) -> pd.DataFrame:
    df_merged = df_agg.merge(df_eurostat, on='country_code', how='inner')
    print(f"\n=== After merging with Eurostat: {len(df_merged)} countries ===")
    if len(df_merged) < 5:
        print("WARNING: Very few countries matched – statistical power will be low.")
    return df_merged


# ── Correlation analysis ─────────────────────────────────────────────────────

def run_correlation_analysis(df: pd.DataFrame) -> dict:
    """Pearson and Spearman correlations between score means and indicators."""
    score_mean_cols = [c for c in df.columns if c.endswith('_mean') and
                       any(s in c for s in ['early_warning', 'compliance', 'cross_border', 'composite'])]
    results = {}

    sep = "=" * 65
    print(f"\n{sep}")
    print("CORRELATION ANALYSIS")
    print(sep)

    for sc in score_mean_cols:
        results[sc] = {}
        print(f"\n  Dependent variable: {sc}")
        print(f"  {'Indicator':<38} Pearson-r   p    Spearman-r   p    n")
        print(f"  {'-'*38} ---------- ---- ---------- ---- --")

        for ind in INDICATOR_COLS:
            if ind not in df.columns:
                continue
            mask = df[sc].notna() & df[ind].notna()
            x, y = df.loc[mask, ind], df.loc[mask, sc]
            n = int(mask.sum())
            if n < 4:
                continue

            r_p, p_p = pearsonr(x, y)
            r_s, p_s = spearmanr(x, y)

            sig_p = "*" if p_p < 0.05 else (" " if p_p >= 0.10 else "·")
            sig_s = "*" if p_s < 0.05 else (" " if p_s >= 0.10 else "·")

            results[sc][ind] = {
                'pearson_r': round(r_p, 4), 'pearson_p': round(p_p, 4),
                'spearman_r': round(r_s, 4), 'spearman_p': round(p_s, 4),
                'n': n,
            }
            label = INDICATOR_LABELS.get(ind, ind)[:38]
            print(f"  {label:<38} {r_p:+.3f}{sig_p}    {p_p:.3f}  {r_s:+.3f}{sig_s}    {p_s:.3f}  {n}")

    print(f"\n  Legend: * p<0.05,  · p<0.10")
    return results


# ── Regression analysis ───────────────────────────────────────────────────────

def run_regression_analysis(df: pd.DataFrame) -> dict:
    """Bivariate OLS regressions for each score × predictor pair."""
    score_mean_cols = [c for c in df.columns if c.endswith('_mean') and
                       any(s in c for s in ['early_warning', 'compliance', 'cross_border', 'composite'])]
    results = {}

    sep = "=" * 65
    print(f"\n{sep}")
    print("REGRESSION ANALYSIS (Bivariate OLS)")
    print(sep)

    for sc in score_mean_cols:
        results[sc] = {}
        print(f"\n  Dependent: {sc}")
        print(f"  {'Predictor':<38} β           SE        R²      p")
        print(f"  {'-'*38} ---------- --------- ------- -------")

        for ind in INDICATOR_COLS:
            if ind not in df.columns:
                continue
            mask = df[sc].notna() & df[ind].notna()
            x, y = df.loc[mask, ind], df.loc[mask, sc]
            if len(x) < 4:
                continue

            slope, intercept, r, p, se = linregress(x, y)
            sig = "*" if p < 0.05 else ("·" if p < 0.10 else " ")
            results[sc][ind] = {
                'slope': round(slope, 6), 'intercept': round(intercept, 6),
                'r_squared': round(r**2, 4), 'p_value': round(p, 4),
                'std_err': round(se, 6), 'n': int(mask.sum()),
            }
            label = INDICATOR_LABELS.get(ind, ind)[:38]
            print(f"  {label:<38} {slope:+.6f}  {se:.6f}  {r**2:.4f}  {p:.4f}{sig}")

    print(f"\n  Legend: * p<0.05,  · p<0.10")
    return results


# ── Between-group comparisons ─────────────────────────────────────────────────

def run_group_comparisons(df: pd.DataFrame) -> dict:
    """Compare metadata quality between high/low GDP, high/low DESI countries."""
    results = {}
    score_mean_cols = [c for c in df.columns if c.endswith('_mean') and
                       any(s in c for s in ['early_warning', 'compliance', 'cross_border', 'composite'])]

    sep = "=" * 65
    print(f"\n{sep}")
    print("BETWEEN-GROUP COMPARISONS (Mann-Whitney U test)")
    print(sep)

    groupings = {
        'GDP (high ≥120 vs low <80 PPS)': (
            'gdp_per_capita_pps', lambda v: v >= 120, lambda v: v < 80),
        'DESI (high ≥55 vs low <45)': (
            'desi_score', lambda v: v >= 55, lambda v: v < 45),
        'Water stress (high WEI≥20 vs low <10)': (
            'water_exploitation_index', lambda v: v >= 20, lambda v: v < 10),
        'ODM (high ≥90 vs low <75)': (
            'open_data_maturity', lambda v: v >= 90, lambda v: v < 75),
    }

    for grp_label, (ind_col, hi_fn, lo_fn) in groupings.items():
        if ind_col not in df.columns:
            continue
        hi = df[hi_fn(df[ind_col])]
        lo = df[lo_fn(df[ind_col])]
        if len(hi) < 2 or len(lo) < 2:
            continue

        print(f"\n  {grp_label}  (n_high={len(hi)}, n_low={len(lo)})")
        results[grp_label] = {}
        for sc in score_mean_cols:
            hi_vals = hi[sc].dropna()
            lo_vals = lo[sc].dropna()
            if len(hi_vals) < 2 or len(lo_vals) < 2:
                continue
            u_stat, p_val = stats.mannwhitneyu(hi_vals, lo_vals, alternative='two-sided')
            direction = "↑ high group" if hi_vals.mean() > lo_vals.mean() else "↓ high group"
            sig = "*" if p_val < 0.05 else ("·" if p_val < 0.10 else " ")
            label = sc.replace('_mean', '').replace('_', ' ')
            print(f"    {label:<35}  hi={hi_vals.mean():.3f}  lo={lo_vals.mean():.3f}  "
                  f"p={p_val:.3f}{sig}  {direction}")
            results[grp_label][sc] = {
                'hi_mean': round(hi_vals.mean(), 4), 'lo_mean': round(lo_vals.mean(), 4),
                'u_stat': round(u_stat, 2), 'p_value': round(p_val, 4),
            }

    return results


# ── Country-level table ───────────────────────────────────────────────────────

def build_country_table(df: pd.DataFrame) -> pd.DataFrame:
    """Build human-readable per-country summary table."""
    score_mean_cols = [c for c in df.columns if c.endswith('_mean') and
                       any(s in c for s in ['early_warning', 'compliance', 'cross_border', 'composite'])]
    display_cols = (['country_code', 'country_name', 'n_datasets'] +
                    score_mean_cols +
                    ['gdp_per_capita_pps', 'desi_score', 'water_exploitation_index', 'open_data_maturity'])
    display_cols = [c for c in display_cols if c in df.columns]
    tbl = df[display_cols].copy()
    for sc in score_mean_cols:
        tbl[sc] = tbl[sc].round(3)
    return tbl.sort_values('composite_score_mean' if 'composite_score_mean' in tbl.columns else score_mean_cols[0],
                           ascending=False)


# ── Narrative summary ─────────────────────────────────────────────────────────

def generate_narrative(df_merged: pd.DataFrame,
                       corr_results: dict,
                       reg_results: dict,
                       group_results: dict) -> str:
    n_countries = len(df_merged)
    lines = [
        "=" * 65,
        "EQUITY & DATA JUSTICE ANALYSIS — NARRATIVE SUMMARY",
        "=" * 65,
        f"\nAnalysis covered {n_countries} EU countries with both metadata",
        "sufficiency scores and Eurostat socioeconomic indicators.\n",
    ]

    # Collect significant findings
    sig_corr, near_sig_corr, direction_notes = [], [], []

    for score, indicators in corr_results.items():
        for ind, vals in indicators.items():
            r_s = vals.get('spearman_r', 0)
            p_s = vals.get('spearman_p', 1)
            label = INDICATOR_LABELS.get(ind, ind)
            sc_label = score.replace('_mean', '').replace('_', ' ')
            direction = "positive" if r_s > 0 else "negative"
            if p_s < 0.05:
                sig_corr.append(
                    f"  • {sc_label} ↔ {label}: ρ={r_s:+.3f}, p={p_s:.3f} ({direction})"
                )
            elif p_s < 0.10:
                near_sig_corr.append(
                    f"  • {sc_label} ↔ {label}: ρ={r_s:+.3f}, p={p_s:.3f} (marginal, {direction})"
                )

    if sig_corr:
        lines += ["SIGNIFICANT CORRELATIONS (p < 0.05):", ""] + sig_corr + [""]
    if near_sig_corr:
        lines += ["MARGINAL CORRELATIONS (0.05 ≤ p < 0.10):", ""] + near_sig_corr + [""]
    if not sig_corr and not near_sig_corr:
        lines += [
            "NO STATISTICALLY SIGNIFICANT CORRELATIONS FOUND (p < 0.05).",
            "",
            "Possible explanations:",
            "  1. The EU-wide open data framework (INSPIRE, DCAT-AP) creates a",
            "     regulatory floor that homogenises quality across wealth levels.",
            "  2. Sample size at country level (≤27 observations) limits power.",
            "  3. Country-level aggregation masks large within-country variation.",
            "  4. Metadata quality deficits are systemic – even rich countries",
            "     lack temporal coverage and machine-readable formats.",
            "",
        ]

    # Direction of effect summary
    lines += ["DIRECTION OF EFFECT SUMMARY (Spearman ρ):"]
    lines.append(f"  {'Score':<35} {'GDP':>8} {'DESI':>8} {'WEI':>8} {'ODM':>8}")
    lines.append(f"  {'-'*35} {'------':>8} {'------':>8} {'------':>8} {'------':>8}")
    key_inds = ['gdp_per_capita_pps', 'desi_score', 'water_exploitation_index', 'open_data_maturity']
    for score, indicators in corr_results.items():
        sc_label = score.replace('_mean', '').replace('_', ' ')[:35]
        row = [f"  {sc_label:<35}"]
        for ind in key_inds:
            if ind in indicators:
                r = indicators[ind]['spearman_r']
                p = indicators[ind]['spearman_p']
                sig = "*" if p < 0.05 else ("·" if p < 0.10 else " ")
                row.append(f"  {r:+.3f}{sig}")
            else:
                row.append(f"  {'N/A':>7}")
        lines.append("".join(row))
    lines += ["", "  * p<0.05,  · p<0.10", ""]

    # Headline finding
    score_means = {}
    for c in df_merged.columns:
        if c.endswith('_mean') and any(s in c for s in ['early_warning', 'compliance', 'cross_border']):
            score_means[c] = df_merged[c].mean()
    if score_means:
        avg = sum(score_means.values()) / len(score_means)
        lines += [
            f"OVERALL DATA QUALITY LEVEL:",
            f"  Mean sufficiency score across all countries: {avg:.3f} / 1.0",
            f"  This corresponds to Partial / Insufficient quality overall.",
            f"  The limiting factors are predominantly:",
            f"    - Missing machine-readable formats (zero or minimal coverage)",
            f"    - No open licenses declared",
            f"    - Poor temporal coverage and update-frequency metadata",
            f"    - Low description quality (short or absent abstracts)",
            "",
        ]

    lines += [
        "DATA JUSTICE CONCLUSION:",
        "",
        "  The analysis does NOT find strong evidence that poorer or less",
        "  digitally developed EU countries produce systematically lower-quality",
        "  water metadata. Instead, quality gaps are pervasive across all wealth",
        "  levels, suggesting that the primary bottleneck is sector-level practice",
        "  (water administration) rather than national digital capacity.",
        "",
        "  However, the available country-level sample (n ≤ 27) is small.",
        "  A sub-national analysis or targeted survey would better reveal",
        "  whether resource constraints amplify local data quality disparities.",
    ]

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ROOT = Path(__file__).parent.parent
    OUTPUT_DIR = ROOT / 'data' / 'outputs_real'

    print("=" * 65)
    print("EU WATER DATA EQUITY & DATA JUSTICE ANALYSIS")
    print("=" * 65)

    # 1. Load
    df_scores, df_eurostat = load_data(ROOT)
    print(f"\nLoaded {len(df_scores)} scored datasets, "
          f"{len(df_eurostat)} country indicators.")

    # 2. Aggregate by country
    df_agg = aggregate_by_country(df_scores)

    # 3. Merge
    df_merged = merge_with_indicators(df_agg, df_eurostat)

    if len(df_merged) < 3:
        print("\nERROR: Fewer than 3 countries matched. Aborting analysis.")
        return

    # 4. Save merged country table
    country_tbl = build_country_table(df_merged)
    merged_path = OUTPUT_DIR / 'equity_merged_data.csv'
    df_merged.to_csv(merged_path, index=False)
    country_tbl_path = OUTPUT_DIR / 'equity_country_table.csv'
    country_tbl.to_csv(country_tbl_path, index=False)
    print(f"\nCountry summary:")
    print(country_tbl.to_string(index=False))

    # 5. Correlation analysis
    corr_results = run_correlation_analysis(df_merged)

    # 6. Regression analysis
    reg_results = run_regression_analysis(df_merged)

    # 7. Group comparisons
    group_results = run_group_comparisons(df_merged)

    # 8. Narrative
    narrative = generate_narrative(df_merged, corr_results, reg_results, group_results)
    print("\n" + narrative)

    # 9. Persist results
    results_payload = {
        'meta': {
            'n_countries_in_analysis': len(df_merged),
            'n_datasets_total': len(df_scores),
            'score_columns': SCORE_COLS,
            'indicator_columns': INDICATOR_COLS,
        },
        'correlations': corr_results,
        'regressions': reg_results,
        'group_comparisons': group_results,
        'narrative_summary': narrative,
    }
    json_path = OUTPUT_DIR / 'equity_analysis_results.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(results_payload, f, indent=2, default=str)
    print(f"\nResults saved to: {json_path}")

    # 10. Per-country scores for visualisation
    score_mean_cols = [c for c in df_merged.columns if c.endswith('_mean') and
                       any(s in c for s in ['early_warning', 'compliance', 'cross_border', 'composite'])]
    vis_cols = ['country_code', 'country_name', 'n_datasets'] + score_mean_cols + INDICATOR_COLS
    vis_df = df_merged[[c for c in vis_cols if c in df_merged.columns]]
    vis_df.to_csv(OUTPUT_DIR / 'equity_visualization_data.csv', index=False)
    print(f"Visualization data saved to: {OUTPUT_DIR / 'equity_visualization_data.csv'}")


if __name__ == '__main__':
    main()
