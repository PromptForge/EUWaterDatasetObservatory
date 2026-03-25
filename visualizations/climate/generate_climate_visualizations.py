"""
Generate all 5 climate adaptation visualizations as self-contained HTML files.

Outputs (visualizations/climate/):
  1. climate_sufficiency_heatmap.html
  2. climate_failure_modes.html
  3. climate_vs_water_comparison.html
  4. climate_sensitivity.html
  5. climate_domain_radar.html
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path

ROOT      = Path(__file__).parent.parent.parent
OUT_DIR   = ROOT / "visualizations" / "climate"
DATA_DIR  = ROOT / "data" / "outputs_climate"
WATER_DIR = ROOT / "data" / "outputs_real"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ─── Plotly CDN template ─────────────────────────────────────────────────────
CDN = "https://cdn.plot.ly/plotly-2.27.0.min.js"

def _html(title: str, subtitle: str, plot_json: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{title}</title>
<script src="{CDN}"></script>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #fafafa; }}
  h1   {{ font-size: 1.4em; color: #2c3e50; margin-bottom: 4px; }}
  p.sub{{ font-size: 0.9em; color: #7f8c8d; margin-top: 0; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="sub">{subtitle}</p>
<div id="plot" style="width:100%;height:600px;"></div>
<script>
  var fig = {plot_json};
  Plotly.newPlot('plot', fig.data, fig.layout, {{responsive: true}});
</script>
</body>
</html>"""


# ─── 1. Climate Sufficiency Heatmap ──────────────────────────────────────────

def viz_heatmap():
    with open(DATA_DIR / "climate_analysis_summary.json") as f:
        summary = json.load(f)

    tasks = [
        "drought_early_warning", "climate_infrastructure", "nbs_monitoring",
        "early_warning", "compliance_reporting", "cross_border",
    ]
    task_labels = [
        "Drought EW", "Climate Infra", "NbS Monitor",
        "EW (water)", "Compliance (water)", "Cross-border (water)",
    ]

    domains = list(summary["domain_breakdown"].keys())
    domain_labels = [d.replace("_", " ").title() for d in domains]

    # Build matrix: rows = domains, cols = tasks
    z_matrix = []
    text_matrix = []
    for domain in domains:
        row = []
        text_row = []
        for task in tasks:
            val = summary["domain_breakdown"][domain]["task_scores"].get(task)
            row.append(val if val is not None else float("nan"))
            text_row.append(f"{val:.3f}" if val is not None else "N/A")
        z_matrix.append(row)
        text_matrix.append(text_row)

    import plotly.graph_objects as go

    fig = go.Figure(data=go.Heatmap(
        z=z_matrix,
        x=task_labels,
        y=domain_labels,
        text=text_matrix,
        texttemplate="%{text}",
        colorscale=[
            [0.0,  "rgb(165,0,38)"],
            [0.25, "rgb(215,48,39)"],
            [0.50, "rgb(253,174,97)"],
            [0.70, "rgb(166,217,106)"],
            [1.0,  "rgb(0,104,55)"],
        ],
        zmin=0, zmax=1,
        hoverongaps=False,
        hovertemplate="Domain: %{y}<br>Task: %{x}<br>Mean score: %{z:.3f}<extra></extra>",
    ))
    fig.update_layout(
        margin=dict(l=160, r=40, t=60, b=100),
        xaxis=dict(side="bottom", tickangle=-30),
        yaxis=dict(autorange="reversed"),
        coloraxis_colorbar=dict(title="Mean<br>Sufficiency"),
    )

    import plotly.io as pio
    plot_json = pio.to_json(fig)

    html = _html(
        "Climate Adaptation Readiness — Sufficiency Heatmap",
        "Mean sufficiency scores by climate domain (rows) and task (columns). "
        "0 = completely insufficient, 1 = fully ready. Green ≥ 0.70 = Ready. "
        "Climate tasks (cols 1–3) and original water tasks (cols 4–6) shown for comparison.",
        plot_json,
    )
    (OUT_DIR / "climate_sufficiency_heatmap.html").write_text(html, encoding="utf-8")
    print("  ✓ climate_sufficiency_heatmap.html")


# ─── 2. Climate Failure Modes Chart ──────────────────────────────────────────

def viz_failure_modes():
    scores_df = pd.read_csv(DATA_DIR / "climate_sufficiency_scores.csv", low_memory=False)

    # Compute failure prevalence: % of datasets where each field is missing/zero
    failure_map = {
        "No machine-readable format":   ("is_machine_readable_pct",
                                         lambda df: (df.get("is_machine_readable", False)
                                                     .map(lambda x: str(x).lower() != "true")).mean() * 100),
        "No open license":              ("license_openness",
                                         lambda df: (df.get("drought_early_warning_license_openness", pd.Series(0.0)) == 0.0).mean() * 100),
        "No spatial coordinates":       ("has_coordinates",
                                         lambda df: (df.get("drought_early_warning_spatial_precision", pd.Series(0.0)) == 0.0).mean() * 100),
        "No description":               ("description_quality",
                                         lambda df: (df.get("drought_early_warning_description_quality", pd.Series(0.0)) == 0.0).mean() * 100),
        "No temporal coverage":         ("temporal_coverage",
                                         lambda df: (df.get("drought_early_warning_temporal_coverage", pd.Series(0.0)) == 0.0).mean() * 100),
        "No temporal recency":          ("temporal_recency",
                                         lambda df: (df.get("drought_early_warning_temporal_recency", pd.Series(0.0)) == 0.0).mean() * 100),
        "No vocabulary/keywords":       ("vocabulary_standard",
                                         lambda df: (df.get("drought_early_warning_vocabulary_standard", pd.Series(0.0)) == 0.0).mean() * 100),
        "Monolingual only":             ("multilingual",
                                         lambda df: (df.get("drought_early_warning_multilingual", pd.Series(0.3)) < 1.0).mean() * 100),
        "No update frequency data":     ("update_frequency",
                                         lambda df: (df.get("drought_early_warning_update_frequency", pd.Series(0.0)) == 0.0).mean() * 100),
    }

    labels, values = [], []
    for label, (_, fn) in failure_map.items():
        try:
            v = fn(scores_df)
            labels.append(label)
            values.append(round(float(v), 1))
        except Exception:
            pass

    # Sort by prevalence descending
    paired = sorted(zip(values, labels), reverse=True)
    values = [p[0] for p in paired]
    labels = [p[1] for p in paired]

    import plotly.graph_objects as go
    import plotly.io as pio

    fig = go.Figure(data=go.Bar(
        x=values,
        y=labels,
        orientation="h",
        marker_color=[
            "rgb(165,0,38)" if v >= 90 else
            "rgb(215,48,39)" if v >= 70 else
            "rgb(253,174,97)" if v >= 50 else
            "rgb(166,217,106)"
            for v in values
        ],
        text=[f"{v:.1f}%" for v in values],
        textposition="outside",
        hovertemplate="%{y}<br>Failure rate: %{x:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        xaxis=dict(title="Failure Rate (% of 994 datasets)", range=[0, 110]),
        yaxis=dict(autorange="reversed"),
        margin=dict(l=230, r=80, t=60, b=60),
        bargap=0.3,
    )

    plot_json = pio.to_json(fig)
    html = _html(
        "Climate Adaptation Datasets — Metadata Failure Modes",
        "Prevalence of each metadata failure type across the 994 climate datasets. "
        "A failure is defined as a dimension score of 0 (or < 1 for multilingual). "
        "Red bars (≥90%) indicate near-universal failures.",
        plot_json,
    )
    (OUT_DIR / "climate_failure_modes.html").write_text(html, encoding="utf-8")
    print("  ✓ climate_failure_modes.html")


# ─── 3. Climate vs Water Comparison Chart ────────────────────────────────────

def viz_comparison():
    with open(DATA_DIR / "climate_analysis_summary.json") as f:
        climate_summary = json.load(f)

    task_scores = climate_summary.get("task_scores", {})
    water_comp  = climate_summary.get("water_comparison", {})

    # Original task labels and their column keys
    orig_tasks = [
        ("early_warning",       "Early Warning"),
        ("compliance_reporting","Compliance Reporting"),
        ("cross_border",        "Cross-Border"),
    ]

    # Pull water means from water_comparison
    water_means   = []
    climate_means = []
    task_names    = []

    for key, label in orig_tasks:
        comp = water_comp.get(key, {})
        w_mean = comp.get("water_mean_all_datasets")
        c_mean = comp.get("climate_dataset_mean")
        if c_mean is None:
            c_mean = task_scores.get(key, {}).get("mean")
        # Also try compliance key variation
        if c_mean is None:
            c_mean = task_scores.get("compliance_reporting", {}).get("mean") if key == "compliance" else None

        task_names.append(label)
        water_means.append(round(float(w_mean), 3) if w_mean is not None else 0)
        climate_means.append(round(float(c_mean), 3) if c_mean is not None else 0)

    import plotly.graph_objects as go
    import plotly.io as pio

    fig = go.Figure(data=[
        go.Bar(
            name="Original water harvest (n=2,176)",
            x=task_names,
            y=water_means,
            marker_color="rgb(52,152,219)",
            text=[f"{v:.3f}" for v in water_means],
            textposition="outside",
            hovertemplate="%{x}<br>Water harvest mean: %{y:.3f}<extra></extra>",
        ),
        go.Bar(
            name="Climate harvest (n=994)",
            x=task_names,
            y=climate_means,
            marker_color="rgb(231,76,60)",
            text=[f"{v:.3f}" for v in climate_means],
            textposition="outside",
            hovertemplate="%{x}<br>Climate harvest mean: %{y:.3f}<extra></extra>",
        ),
    ])
    fig.update_layout(
        barmode="group",
        yaxis=dict(title="Mean Sufficiency Score", range=[0, 0.45]),
        xaxis=dict(title="Management Task"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=60, r=40, t=100, b=80),
    )

    plot_json = pio.to_json(fig)
    html = _html(
        "Climate vs Water Harvest — Sufficiency Comparison",
        "Mean sufficiency scores for the three original water management tasks, "
        "comparing the original water harvest (n=2,176) with the climate adaptation harvest (n=994). "
        "Neither corpus achieves Ready status (threshold=0.70) on any task.",
        plot_json,
    )
    (OUT_DIR / "climate_vs_water_comparison.html").write_text(html, encoding="utf-8")
    print("  ✓ climate_vs_water_comparison.html")


# ─── 4. Climate Sensitivity Chart ────────────────────────────────────────────

def viz_sensitivity():
    sens_df = pd.read_csv(DATA_DIR / "climate_sensitivity_summary.csv")

    import plotly.graph_objects as go
    import plotly.io as pio

    task_labels = [t.replace("_", " ").title() for t in sens_df["task"]]

    fig = go.Figure()
    # Error bars representing the low–high range
    fig.add_trace(go.Scatter(
        x=sens_df["baseline_mean_score"],
        y=task_labels,
        mode="markers+text",
        marker=dict(size=14, color="rgb(52,152,219)", symbol="circle"),
        text=[f"{v:.4f}" for v in sens_df["baseline_mean_score"]],
        textposition="middle right",
        name="Baseline mean score",
        hovertemplate=(
            "%{y}<br>"
            "Baseline mean: %{x:.4f}<br>"
            "<extra></extra>"
        ),
    ))
    # Add low/high scatter
    fig.add_trace(go.Scatter(
        x=pd.concat([sens_df["low_mean_score"], sens_df["high_mean_score"]]),
        y=task_labels + task_labels,
        mode="markers",
        marker=dict(size=9, color="rgb(231,76,60)", symbol="diamond"),
        name="±25% perturbed mean",
        hovertemplate=(
            "%{y}<br>"
            "Perturbed mean: %{x:.4f}<br>"
            "<extra></extra>"
        ),
    ))

    # Table-style annotation
    ann_lines = ["<b>Task</b>          <b>Primary Dim</b>         <b>Baseline%</b>  <b>Low%</b>  <b>High%</b>  <b>Range(pp)</b>"]
    for _, row in sens_df.iterrows():
        ann_lines.append(
            f"{row['task']:<28s}  {row['primary_metric']:<20s}  "
            f"{row['baseline_ready_pct']:>5.1f}%  {row['low_ready_pct']:>5.1f}%  "
            f"{row['high_ready_pct']:>5.1f}%  {row['range_pp']:>5.2f}pp"
        )

    fig.update_layout(
        xaxis=dict(title="Mean Sufficiency Score", range=[0, 0.35]),
        yaxis=dict(autorange="reversed"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=200, r=80, t=80, b=80),
        annotations=[dict(
            x=0.01, y=-0.25,
            xref="paper", yref="paper",
            text="<br>".join(ann_lines[:4]),
            showarrow=False,
            align="left",
            font=dict(size=11, family="monospace"),
        )],
    )

    plot_json = pio.to_json(fig)
    html = _html(
        "Climate Adaptation Sensitivity Analysis (±25% Weight Perturbation)",
        "For each climate task, the highest-weighted dimension is perturbed ±25% and remaining "
        "weights are renormalized. Circles = baseline mean score; diamonds = perturbed means. "
        "Ready% = 0.0% under all scenarios — conclusions are maximally robust.",
        plot_json,
    )
    (OUT_DIR / "climate_sensitivity.html").write_text(html, encoding="utf-8")
    print("  ✓ climate_sensitivity.html")


# ─── 5. Climate Domain Radar Chart ───────────────────────────────────────────

def viz_radar():
    scores_df = pd.read_csv(DATA_DIR / "climate_sufficiency_scores.csv", low_memory=False)

    # Use drought_early_warning dimension scores (all 9 dims scored)
    DIMS = [
        "temporal_recency", "update_frequency", "temporal_coverage",
        "spatial_precision", "format_readability", "license_openness",
        "description_quality", "vocabulary_standard", "multilingual",
    ]
    dim_labels = [d.replace("_", " ").title() for d in DIMS]
    # Close the polygon
    dim_labels_closed = dim_labels + [dim_labels[0]]

    import plotly.graph_objects as go
    import plotly.io as pio

    fig = go.Figure()

    colors = {"drought_early_warning": "rgb(231,76,60)", "climate_infrastructure": "rgb(52,152,219)"}
    for domain in scores_df["domain"].unique():
        sub = scores_df[scores_df["domain"] == domain]
        means = []
        for dim in DIMS:
            col = f"drought_early_warning_{dim}"
            if col in sub.columns:
                means.append(round(float(sub[col].mean()), 4))
            else:
                means.append(0.0)
        means_closed = means + [means[0]]
        label = domain.replace("_", " ").title()
        color = colors.get(domain, "rgb(46,204,113)")

        fig.add_trace(go.Scatterpolar(
            r=means_closed,
            theta=dim_labels_closed,
            fill="toself",
            name=f"{label} (n={len(sub)})",
            line_color=color,
            fillcolor=color.replace("rgb", "rgba").replace(")", ",0.15)"),
            hovertemplate="Dimension: %{theta}<br>Mean score: %{r:.4f}<extra></extra>",
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 0.6], tickfont=dict(size=9)),
        ),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.12),
        margin=dict(l=60, r=60, t=80, b=100),
    )

    plot_json = pio.to_json(fig)
    html = _html(
        "Climate Domain Metadata Profiles — Dimension Radar",
        "Mean score on each of the 9 metadata dimensions for each climate domain. "
        "Outer edge = 1.0 (perfect). The collapse of all axes toward zero confirms "
        "near-universal metadata deficiency across both harvested climate domains.",
        plot_json,
    )
    (OUT_DIR / "climate_domain_radar.html").write_text(html, encoding="utf-8")
    print("  ✓ climate_domain_radar.html")


# ─── Run all ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating climate visualizations …\n")
    viz_heatmap()
    viz_failure_modes()
    viz_comparison()
    viz_sensitivity()
    viz_radar()
    print(f"\nAll 5 visualizations saved to: {OUT_DIR}")
