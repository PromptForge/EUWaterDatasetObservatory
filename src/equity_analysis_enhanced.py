#!/usr/bin/env python3
"""
Equity Analysis: Correlate metadata quality with socioeconomic indicators.
Enhanced with VIF multicollinearity check, weighted regression, and LaTeX tables.
"""

import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path
import json

# Check for statsmodels
try:
    import statsmodels.api as sm
    from statsmodels.stats.outliers_influence import variance_inflation_factor
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False
    print("WARNING: statsmodels not installed. Run: pip install statsmodels")


# ── Predictors and outcomes ───────────────────────────────────────────────────

PREDICTORS = [
    "gdp_per_capita_pps",
    "desi_score",
    "open_data_maturity",
    "water_exploitation_index",
    "egovernment_usage_pct",
    "data_infrastructure_score",
]

PREDICTOR_LABELS = {
    "gdp_per_capita_pps":       "GDP per capita (PPS)",
    "desi_score":               "DESI Score",
    "open_data_maturity":       "Open Data Maturity",
    "water_exploitation_index": "Water Exploitation Index",
    "egovernment_usage_pct":    "E-Government Usage (%)",
    "data_infrastructure_score":"Data Infrastructure Score",
    "const":                    "Constant",
}

SCORE_COLS = [
    "composite_score_mean",
    "early_warning_score_mean",
    "compliance_reporting_score_mean",
    "cross_border_score_mean",
]

SCORE_LATEX_LABELS = {
    "composite_score_mean":           "Overall",
    "early_warning_score_mean":       "Early Warning",
    "compliance_reporting_score_mean":"Compliance",
    "cross_border_score_mean":        "Cross-Border",
}


# ── VIF ───────────────────────────────────────────────────────────────────────

def calculate_vif(df: pd.DataFrame, predictors: list) -> pd.DataFrame:
    """
    Calculate Variance Inflation Factor for multicollinearity detection.
      VIF < 5   → OK
      5 ≤ VIF < 10 → Warning
      VIF ≥ 10  → Severe
    """
    if not HAS_STATSMODELS:
        print("Cannot calculate VIF without statsmodels")
        return None

    available = [p for p in predictors if p in df.columns]
    X = df[available].dropna()

    if len(X) < len(available) + 1:
        print(f"Insufficient observations ({len(X)}) for VIF calculation")
        return None

    vif_data = pd.DataFrame()
    vif_data["Variable"] = available
    vif_data["VIF"] = [
        variance_inflation_factor(X.values, i) for i in range(len(available))
    ]
    vif_data["Status"] = vif_data["VIF"].apply(
        lambda x: "OK" if x < 5 else ("Warning" if x < 10 else "Severe")
    )
    return vif_data


# ── OLS ───────────────────────────────────────────────────────────────────────

def run_ols(df: pd.DataFrame, y_col: str, x_cols: list) -> dict:
    """Standard OLS regression."""
    if not HAS_STATSMODELS:
        print("Cannot run OLS without statsmodels")
        return None

    available_x = [c for c in x_cols if c in df.columns]
    cols_needed = [y_col] + available_x
    data = df[cols_needed].dropna()

    if len(data) < len(available_x) + 2:
        print(f"Insufficient observations ({len(data)}) for regression on {y_col}")
        return None

    y = data[y_col]
    X = sm.add_constant(data[available_x])

    results = sm.OLS(y, X).fit()

    return {
        "params":        results.params.to_dict(),
        "pvalues":       results.pvalues.to_dict(),
        "std_err":       results.bse.to_dict(),
        "r_squared":     results.rsquared,
        "adj_r_squared": results.rsquared_adj,
        "f_pvalue":      results.f_pvalue,
        "n_obs":         int(results.nobs),
        "summary":       results.summary().as_text(),
    }


# ── Weighted OLS ──────────────────────────────────────────────────────────────

def run_weighted_ols(df: pd.DataFrame, y_col: str, x_cols: list,
                     weight_col: str) -> dict:
    """
    Weighted OLS regression.
    Countries with more datasets receive higher weight.
    """
    if not HAS_STATSMODELS:
        print("Cannot run weighted OLS without statsmodels")
        return None

    available_x = [c for c in x_cols if c in df.columns]
    cols_needed = [y_col, weight_col] + available_x
    data = df[cols_needed].dropna()

    if len(data) < len(available_x) + 2:
        print(f"Insufficient observations ({len(data)}) for weighted regression on {y_col}")
        return None

    y       = data[y_col]
    X       = sm.add_constant(data[available_x])
    weights = data[weight_col]

    results = sm.WLS(y, X, weights=weights).fit()

    return {
        "params":        results.params.to_dict(),
        "pvalues":       results.pvalues.to_dict(),
        "std_err":       results.bse.to_dict(),
        "r_squared":     results.rsquared,
        "adj_r_squared": results.rsquared_adj,
        "f_pvalue":      results.f_pvalue,
        "n_obs":         int(results.nobs),
        "summary":       results.summary().as_text(),
    }


# ── LaTeX table ───────────────────────────────────────────────────────────────

def generate_latex_table(results_dict: dict,
                         score_cols: list,
                         predictor_labels: dict) -> str:
    """
    Generate a publication-ready LaTeX regression table.
    Columns = score outcomes.  Rows = predictors.
    """
    col_labels = [SCORE_LATEX_LABELS.get(sc, sc) for sc in score_cols]
    n_cols = len(score_cols)
    col_spec = "l" + "c" * n_cols

    latex = []
    latex.append(r"\begin{table}[htbp]")
    latex.append(r"\centering")
    latex.append(
        r"\caption{OLS Regression Results: Metadata Quality and "
        r"Socioeconomic Indicators}"
    )
    latex.append(r"\label{tab:regression}")
    latex.append(rf"\begin{{tabular}}{{{col_spec}}}")
    latex.append(r"\toprule")

    header = "& " + " & ".join([rf"\textbf{{{l}}}" for l in col_labels]) + r" \\"
    latex.append(header)
    latex.append(r"\midrule")

    # Collect all variables that appear in any regression
    all_vars = []
    for sc in score_cols:
        res = results_dict.get(sc)
        if res and "params" in res:
            for v in res["params"]:
                if v not in all_vars:
                    all_vars.append(v)

    # Print rows: intercept last
    ordered_vars = [v for v in all_vars if v != "const"] + (["const"] if "const" in all_vars else [])

    for pred in ordered_vars:
        label = predictor_labels.get(pred, pred).replace("_", r"\_")
        row_coef = [label]
        row_se   = [""]   # SE row (blank label)

        for sc in score_cols:
            res = results_dict.get(sc)
            if res and pred in res.get("params", {}):
                coef = res["params"][pred]
                pval = res["pvalues"][pred]
                se   = res["std_err"][pred]

                stars = ""
                if pval < 0.01:
                    stars = "^{***}"
                elif pval < 0.05:
                    stars = "^{**}"
                elif pval < 0.10:
                    stars = "^{*}"

                row_coef.append(f"${coef:.4f}{stars}$")
                row_se.append(f"$({se:.4f})$")
            else:
                row_coef.append("--")
                row_se.append("")

        latex.append(" & ".join(row_coef) + r" \\")
        latex.append(" & ".join(row_se) + r" \\")

    latex.append(r"\midrule")

    # R² row
    r2_row = [r"$R^2$"]
    for sc in score_cols:
        res = results_dict.get(sc)
        r2_row.append(f"{res['r_squared']:.3f}" if res else "--")
    latex.append(" & ".join(r2_row) + r" \\")

    # Adj R² row
    adj_r2_row = [r"Adj.\ $R^2$"]
    for sc in score_cols:
        res = results_dict.get(sc)
        adj_r2_row.append(f"{res['adj_r_squared']:.3f}" if res else "--")
    latex.append(" & ".join(adj_r2_row) + r" \\")

    # N row
    n_row = ["$N$"]
    for sc in score_cols:
        res = results_dict.get(sc)
        n_row.append(str(res["n_obs"]) if res else "--")
    latex.append(" & ".join(n_row) + r" \\")

    latex.append(r"\bottomrule")
    latex.append(
        rf"\multicolumn{{{n_cols + 1}}}{{l}}{{\footnotesize "
        r"Standard errors in parentheses. "
        r"$^{*}$\,p$<$0.10, $^{**}$\,p$<$0.05, $^{***}$\,p$<$0.01}} \\"
    )
    latex.append(r"\end{tabular}")
    latex.append(r"\end{table}")

    return "\n".join(latex)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ROOT       = Path(__file__).parent.parent
    OUTPUT_DIR = ROOT / "data" / "outputs_real"

    print("=" * 65)
    print("ENHANCED EQUITY ANALYSIS")
    print("VIF + Weighted Regression + LaTeX Tables")
    print("=" * 65)

    # ── Load merged data ──────────────────────────────────────────────────────
    merged_path = OUTPUT_DIR / "equity_merged_data.csv"
    if not merged_path.exists():
        print(f"ERROR: {merged_path} not found!")
        print("Run the basic equity analysis first (python src/equity_analysis.py).")
        return

    df = pd.read_csv(merged_path)
    print(f"Loaded {len(df)} countries")

    # Identify available predictors and score columns
    available_predictors = [p for p in PREDICTORS if p in df.columns]
    available_scores     = [s for s in SCORE_COLS if s in df.columns]

    if not available_scores:
        print("ERROR: No recognised score columns found.")
        return

    # Ensure weight column exists
    if "n_datasets" not in df.columns:
        print("WARNING: 'n_datasets' not found. Using equal weights.")
        df["n_datasets"] = 1
    weight_col = "n_datasets"

    print(f"\nPredictors : {available_predictors}")
    print(f"Outcomes   : {available_scores}")
    print(f"Weight col : {weight_col}")

    # ── VIF ───────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("VIF MULTICOLLINEARITY CHECK")
    print("=" * 65)

    vif_results = calculate_vif(df, available_predictors)
    if vif_results is not None:
        print(vif_results.to_string(index=False))

        problematic = vif_results[vif_results["VIF"] > 5]
        if len(problematic) > 0:
            print(f"\n⚠  WARNING: {len(problematic)} variable(s) with VIF > 5")
            print("   Consider removing or combining these predictors.")
        else:
            print("\n✓  No multicollinearity issues detected (all VIF < 5)")

        vif_results.to_csv(OUTPUT_DIR / "vif_results.csv", index=False)
        print(f"\nSaved: {OUTPUT_DIR / 'vif_results.csv'}")

        # Use only predictors with VIF < 10 for regressions
        safe_predictors = vif_results[vif_results["VIF"] < 10]["Variable"].tolist()
    else:
        safe_predictors = available_predictors

    # ── Standard OLS ─────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("STANDARD OLS REGRESSION")
    print("=" * 65)

    ols_results = {}

    for sc in available_scores:
        print(f"\n--- Dependent: {sc} ---")
        res = run_ols(df, sc, safe_predictors)
        if res:
            ols_results[sc] = res
            print(f"  R²      = {res['r_squared']:.4f}")
            print(f"  Adj R²  = {res['adj_r_squared']:.4f}")
            print(f"  F p-val = {res['f_pvalue']:.4f}")
            print(f"  N       = {res['n_obs']}")
            print("  Coefficients:")
            for var, coef in res["params"].items():
                pval = res["pvalues"][var]
                se   = res["std_err"][var]
                sig  = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.10 else ""))
                lbl  = PREDICTOR_LABELS.get(var, var)
                print(f"    {lbl:<35} {coef:+.6f}  SE={se:.6f}  p={pval:.4f} {sig}")

    # ── Weighted OLS ──────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("WEIGHTED OLS REGRESSION  (weighted by n_datasets)")
    print("=" * 65)

    wols_results = {}

    for sc in available_scores:
        print(f"\n--- Dependent: {sc} (WLS) ---")
        res = run_weighted_ols(df, sc, safe_predictors, weight_col)
        if res:
            wols_results[sc] = res
            print(f"  R²      = {res['r_squared']:.4f}")
            print(f"  Adj R²  = {res['adj_r_squared']:.4f}")
            print(f"  F p-val = {res['f_pvalue']:.4f}")
            print(f"  N       = {res['n_obs']}")
            print("  Coefficients:")
            for var, coef in res["params"].items():
                pval = res["pvalues"][var]
                se   = res["std_err"][var]
                sig  = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.10 else ""))
                lbl  = PREDICTOR_LABELS.get(var, var)
                print(f"    {lbl:<35} {coef:+.6f}  SE={se:.6f}  p={pval:.4f} {sig}")

    # ── OLS vs WLS comparison ─────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("OLS vs WEIGHTED OLS — R² COMPARISON")
    print("=" * 65)
    print(f"  {'Outcome':<40} {'OLS R²':>8} {'WLS R²':>8} {'Δ R²':>8}")
    print(f"  {'-'*40} {'------':>8} {'------':>8} {'------':>8}")
    for sc in available_scores:
        ols_r2  = ols_results.get(sc, {}).get("r_squared", float("nan"))
        wols_r2 = wols_results.get(sc, {}).get("r_squared", float("nan"))
        delta   = wols_r2 - ols_r2
        print(f"  {sc:<40} {ols_r2:>8.4f} {wols_r2:>8.4f} {delta:>+8.4f}")

    # ── LaTeX table ───────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("LATEX REGRESSION TABLE  (Standard OLS)")
    print("=" * 65)

    latex_table = generate_latex_table(ols_results, available_scores, PREDICTOR_LABELS)
    print(latex_table)

    latex_path = OUTPUT_DIR / "regression_table.tex"
    with open(latex_path, "w", encoding="utf-8") as f:
        f.write(latex_table)
    print(f"\nSaved: {latex_path}")

    # ── Also generate WLS LaTeX table ─────────────────────────────────────────
    latex_wls = generate_latex_table(wols_results, available_scores, PREDICTOR_LABELS)
    latex_wls_path = OUTPUT_DIR / "regression_table_wls.tex"
    # Add WLS caption
    latex_wls = latex_wls.replace(
        r"\caption{OLS Regression Results:",
        r"\caption{WLS Regression Results (weighted by $n$ datasets):"
    ).replace(r"\label{tab:regression}", r"\label{tab:regression_wls}")
    with open(latex_wls_path, "w", encoding="utf-8") as f:
        f.write(latex_wls)
    print(f"Saved: {latex_wls_path}")

    # ── Persist full results ──────────────────────────────────────────────────
    all_results = {
        "vif":          vif_results.to_dict("records") if vif_results is not None else None,
        "ols":          {k: {kk: vv for kk, vv in v.items() if kk != "summary"}
                         for k, v in ols_results.items()},
        "weighted_ols": {k: {kk: vv for kk, vv in v.items() if kk != "summary"}
                         for k, v in wols_results.items()},
    }

    json_path = OUTPUT_DIR / "enhanced_equity_results.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nAll results saved: {json_path}")

    print("\n" + "=" * 65)
    print("DONE")
    print("=" * 65)


if __name__ == "__main__":
    main()
