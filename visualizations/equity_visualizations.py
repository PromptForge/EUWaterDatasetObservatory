#!/usr/bin/env python3
"""
Generate interactive equity visualizations for the EU Water Data Observatory.

Outputs (all in visualizations/):
  equity_scatter_gdp.html          – composite score vs GDP
  equity_scatter_desi.html         – composite score vs DESI
  equity_scatter_wei.html          – composite score vs Water Exploitation Index
  equity_scatter_odm.html          – composite score vs Open Data Maturity
  equity_country_heatmap.html      – per-country × per-score heatmap
  equity_dashboard.html            – full dashboard (all charts + table)
"""

import json
import math
import pandas as pd
from pathlib import Path

# ── helpers ──────────────────────────────────────────────────────────────────

def _fmt(v):
    """Format float for JS – handles NaN/None safely."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return 'null'
    return repr(round(float(v), 4))


def scatter_html(df: pd.DataFrame, x_col: str, y_col: str,
                 title: str, x_label: str, y_label: str,
                 color_col: str = None) -> str:
    """Return self-contained Plotly scatter HTML string."""
    rows = df[[x_col, y_col, 'country_code', 'country_name', 'n_datasets']
               + ([color_col] if color_col else [])].dropna(subset=[x_col, y_col])

    x_vals  = [_fmt(v) for v in rows[x_col]]
    y_vals  = [_fmt(v) for v in rows[y_col]]
    labels  = [str(v) for v in rows['country_code']]
    htext   = [
        f"{r['country_name']} ({r['country_code']})<br>"
        f"{x_label}: {r[x_col]:.1f}<br>"
        f"{y_label}: {r[y_col]:.3f}<br>"
        f"n datasets: {int(r['n_datasets'])}"
        for _, r in rows.iterrows()
    ]

    # Trend-line via Pearson
    if len(rows) >= 3:
        from scipy.stats import linregress, pearsonr
        slope, intercept, r_val, p_val, _ = linregress(rows[x_col], rows[y_col])
        x_range = [float(rows[x_col].min()), float(rows[x_col].max())]
        trend_x = f"[{x_range[0]}, {x_range[1]}]"
        trend_y = f"[{x_range[0]*slope+intercept:.4f}, {x_range[1]*slope+intercept:.4f}]"
        r_p, p_p = pearsonr(rows[x_col], rows[y_col])
        stat_note = f"Pearson r = {r_p:+.3f}, p = {p_p:.3f}"
    else:
        trend_x, trend_y, stat_note = "[]", "[]", "n < 3"

    color_scale = '#2E75B6'

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js" charset="utf-8"></script>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; background: #fafafa; }}
    h2   {{ color: #1a3a5c; }}
    .stat{{ font-size:13px; color:#555; margin-top:4px; }}
  </style>
</head>
<body>
  <h2>{title}</h2>
  <p class="stat">{stat_note}</p>
  <div id="plot" style="width:820px;height:560px;"></div>
  <script>
    var scatter = {{
      x: [{",".join(x_vals)}],
      y: [{",".join(y_vals)}],
      text: {json.dumps(labels)},
      customdata: {json.dumps(htext)},
      hovertemplate: "%{{customdata}}<extra></extra>",
      mode: "markers+text",
      textposition: "top center",
      type: "scatter",
      name: "Countries",
      marker: {{
        size: 12,
        color: "{color_scale}",
        line: {{ width: 1, color: "#fff" }}
      }}
    }};

    var trend = {{
      x: {trend_x},
      y: {trend_y},
      mode: "lines",
      type: "scatter",
      name: "Trend",
      line: {{ color: "#e05c2d", width: 2, dash: "dash" }}
    }};

    var layout = {{
      title: {{ text: "{title}", font: {{ size: 15 }} }},
      xaxis: {{ title: {{ text: "{x_label}" }}, zeroline: false }},
      yaxis: {{ title: {{ text: "{y_label}" }}, range: [0, 0.65], zeroline: false }},
      legend: {{ x: 0.01, y: 0.99 }},
      plot_bgcolor: "#fff",
      paper_bgcolor: "#fafafa",
      shapes: [{{
        type: "line", x0: 0, x1: 1, xref: "paper",
        y0: 0.5, y1: 0.5,
        line: {{ color: "#aaa", width: 1, dash: "dot" }}
      }}],
      annotations: [{{
        x: 1, y: 0.5, xref: "paper", yref: "y",
        text: "Threshold 0.5", showarrow: false,
        xanchor: "right", font: {{ size: 10, color: "#aaa" }}
      }}]
    }};

    Plotly.newPlot("plot", [scatter, trend], layout, {{responsive: true}});
  </script>
</body>
</html>"""


def heatmap_html(df: pd.DataFrame,
                 score_cols: list,
                 title: str = "Metadata Quality Heatmap by Country & Task") -> str:
    """Return Plotly heatmap HTML."""
    countries = df['country_code'].tolist()
    country_names = df['country_name'].tolist() if 'country_name' in df.columns else countries

    score_labels = {
        'early_warning_score_mean': 'Early Warning',
        'compliance_reporting_score_mean': 'Compliance',
        'cross_border_score_mean': 'Cross-Border',
        'composite_score_mean': 'Composite',
    }
    s_cols = [c for c in score_cols if c in df.columns]
    labels = [score_labels.get(c, c.replace('_mean', '').replace('_', ' ').title())
              for c in s_cols]

    z_rows = []
    for _, row in df.iterrows():
        z_rows.append([round(float(row[c]), 3) if pd.notna(row[c]) else None for c in s_cols])

    z_js = json.dumps(z_rows)
    x_js = json.dumps(labels)
    y_js = json.dumps(countries)
    text_js = json.dumps([[
        f"{cn} – {lbl}: {v:.3f}" if v is not None else f"{cn} – {lbl}: N/A"
        for lbl, v in zip(labels, row_vals)
    ] for cn, row_vals in zip(country_names, z_rows)])

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js" charset="utf-8"></script>
  <style>body{{ font-family:Arial,sans-serif; margin:20px; background:#fafafa; }}
         h2  {{ color:#1a3a5c; }}</style>
</head>
<body>
  <h2>{title}</h2>
  <div id="plot" style="width:900px;height:600px;"></div>
  <script>
    var data = [{{
      z: {z_js},
      x: {x_js},
      y: {y_js},
      text: {text_js},
      hovertemplate: "%{{text}}<extra></extra>",
      type: "heatmap",
      colorscale: [
        [0.0, "#d73027"], [0.25, "#fc8d59"],
        [0.5, "#fee090"], [0.75, "#91bfdb"],
        [1.0, "#4575b4"]
      ],
      zmin: 0, zmax: 0.6,
      colorbar: {{ title: "Score" }}
    }}];
    var layout = {{
      title: "{title}",
      xaxis: {{ title: "Task Category" }},
      yaxis: {{ title: "Country", autorange: "reversed" }},
      plot_bgcolor: "#fff",
      paper_bgcolor: "#fafafa",
      margin: {{ l: 60, r: 20, t: 60, b: 60 }}
    }};
    Plotly.newPlot("plot", data, layout, {{responsive: true}});
  </script>
</body>
</html>"""


def bar_country_html(df: pd.DataFrame,
                     score_col: str,
                     title: str,
                     y_label: str = "Score (0–1)") -> str:
    """Sorted bar chart of per-country score."""
    df_sorted = df[['country_code', 'country_name', score_col, 'n_datasets']].dropna(
        subset=[score_col]).sort_values(score_col, ascending=False)

    x_js = json.dumps(df_sorted['country_code'].tolist())
    y_js = json.dumps([round(float(v), 4) for v in df_sorted[score_col]])
    hover = [
        f"{r['country_name']} ({r['country_code']})<br>Score: {r[score_col]:.3f}<br>n datasets: {int(r['n_datasets'])}"
        for _, r in df_sorted.iterrows()
    ]
    hover_js = json.dumps(hover)

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js" charset="utf-8"></script>
  <style>body{{ font-family:Arial,sans-serif; margin:20px; background:#fafafa; }}
         h2  {{ color:#1a3a5c; }}</style>
</head>
<body>
  <h2>{title}</h2>
  <div id="plot" style="width:900px;height:480px;"></div>
  <script>
    Plotly.newPlot("plot", [{{
      x: {x_js},
      y: {y_js},
      customdata: {hover_js},
      hovertemplate: "%{{customdata}}<extra></extra>",
      type: "bar",
      marker: {{
        color: {y_js},
        colorscale: [
          [0, "#d73027"], [0.4, "#fc8d59"], [0.6, "#fee090"],
          [0.8, "#91bfdb"], [1, "#4575b4"]
        ],
        showscale: true,
        cmin: 0, cmax: 0.6,
        colorbar: {{ title: "Score" }}
      }}
    }}], {{
      title: "{title}",
      xaxis: {{ title: "Country" }},
      yaxis: {{ title: "{y_label}", range: [0, 0.65] }},
      plot_bgcolor: "#fff",
      paper_bgcolor: "#fafafa",
      shapes: [{{
        type: "line", x0: 0, x1: 1, xref: "paper",
        y0: 0.5, y1: 0.5,
        line: {{ color: "#e05c2d", width: 1.5, dash: "dot" }}
      }}]
    }}, {{responsive: true}});
  </script>
</body>
</html>"""


def dashboard_html(scatter_gdp: str, scatter_desi: str,
                   scatter_wei: str, scatter_odm: str,
                   heatmap: str, bar: str,
                   country_table_html: str,
                   corr_json: dict) -> str:
    """Combine all charts into a tabbed dashboard."""

    def _tab_btn(tab_id, label, active=""):
        return (f'<button class="tablink {active}" '
                f'onclick="openTab(event,\'{tab_id}\')">{label}</button>')

    def _tab_panel(tab_id, content, active=""):
        disp = "block" if active else "none"
        return (f'<div id="{tab_id}" class="tabcontent" style="display:{disp}">'
                f'{content}</div>')

    def _inline(html_str):
        """Extract body content from a full HTML page."""
        import re
        body = re.search(r'<body>(.*?)</body>', html_str, re.DOTALL)
        return body.group(1) if body else html_str

    # Build correlation table
    corr_rows = []
    key_inds  = ['gdp_per_capita_pps', 'desi_score',
                 'water_exploitation_index', 'open_data_maturity']
    ind_short = {'gdp_per_capita_pps': 'GDP (PPS)',
                 'desi_score': 'DESI',
                 'water_exploitation_index': 'WEI',
                 'open_data_maturity': 'ODM'}
    for sc, inds in corr_json.items():
        sc_label = sc.replace('_mean', '').replace('_', ' ').title()
        for ind in key_inds:
            if ind not in inds:
                continue
            vals = inds[ind]
            r_s, p_s = vals['spearman_r'], vals['spearman_p']
            n = vals['n']
            sig = "★" if p_s < 0.05 else ("·" if p_s < 0.10 else "")
            color = "#4575b4" if r_s > 0.2 else ("#d73027" if r_s < -0.2 else "#aaa")
            corr_rows.append(
                f"<tr><td>{sc_label}</td><td>{ind_short[ind]}</td>"
                f"<td style='color:{color};font-weight:bold'>{r_s:+.3f}{sig}</td>"
                f"<td>{p_s:.3f}</td><td>{n}</td></tr>"
            )
    corr_table = f"""
    <h3>Correlation Table (Spearman ρ)</h3>
    <p style='font-size:12px;color:#666'>★ p&lt;0.05 &nbsp; · p&lt;0.10</p>
    <table border='1' cellpadding='5' cellspacing='0' style='border-collapse:collapse;font-size:13px;'>
      <thead style='background:#1a3a5c;color:#fff'>
        <tr><th>Score</th><th>Indicator</th><th>Spearman ρ</th><th>p-value</th><th>n</th></tr>
      </thead>
      <tbody>{"".join(corr_rows)}</tbody>
    </table>"""

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>EU Water Data Equity Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js" charset="utf-8"></script>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f0f4f8; }}
    h1   {{ color: #1a3a5c; font-size: 22px; margin-bottom: 4px; }}
    .subtitle {{ color: #555; font-size: 13px; margin-bottom: 16px; }}
    .tab {{ overflow: hidden; border-bottom: 2px solid #1a3a5c; margin-bottom: 12px; }}
    .tablink {{
      background: #dde6f0; border: none; outline: none; cursor: pointer;
      padding: 10px 16px; font-size: 13px; color: #333; border-radius: 4px 4px 0 0;
      margin-right: 3px; transition: background 0.2s;
    }}
    .tablink:hover, .tablink.active {{ background: #1a3a5c; color: #fff; }}
    .tabcontent {{
      background: #fff; padding: 16px; border-radius: 0 4px 4px 4px;
      box-shadow: 0 1px 4px rgba(0,0,0,.12);
    }}
    table {{ border-collapse: collapse; }}
    th,td {{ padding: 6px 10px; border: 1px solid #ccc; font-size: 12px; }}
    th {{ background: #1a3a5c; color: #fff; }}
    tr:nth-child(even) {{ background: #f5f8fb; }}
  </style>
</head>
<body>
  <h1>EU Water Data Equity &amp; Data Justice Analysis</h1>
  <p class="subtitle">
    Metadata quality (sufficiency scores) vs socioeconomic indicators for EU-27 countries.
    Data sources: EU Open Data Portal harvested datasets; Eurostat 2022-2023.
  </p>

  <div class="tab">
    {_tab_btn("tab-gdp",    "Score vs GDP",           "active")}
    {_tab_btn("tab-desi",   "Score vs DESI")}
    {_tab_btn("tab-wei",    "Score vs Water Stress")}
    {_tab_btn("tab-odm",    "Score vs Open Data")}
    {_tab_btn("tab-heat",   "Country Heatmap")}
    {_tab_btn("tab-bar",    "Country Bar")}
    {_tab_btn("tab-corr",   "Correlation Table")}
    {_tab_btn("tab-table",  "Country Data")}
  </div>

  {_tab_panel("tab-gdp",   _inline(scatter_gdp), "block")}
  {_tab_panel("tab-desi",  _inline(scatter_desi))}
  {_tab_panel("tab-wei",   _inline(scatter_wei))}
  {_tab_panel("tab-odm",   _inline(scatter_odm))}
  {_tab_panel("tab-heat",  _inline(heatmap))}
  {_tab_panel("tab-bar",   _inline(bar))}
  {_tab_panel("tab-corr",  corr_table)}
  {_tab_panel("tab-table", country_table_html)}

  <script>
    function openTab(evt, tabName) {{
      document.querySelectorAll(".tabcontent").forEach(t => t.style.display = "none");
      document.querySelectorAll(".tablink").forEach(b => b.classList.remove("active"));
      document.getElementById(tabName).style.display = "block";
      evt.currentTarget.classList.add("active");
    }}
  </script>
</body>
</html>"""


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    ROOT    = Path(__file__).parent.parent
    VIZ_DIR = ROOT / 'visualizations'
    DATA    = ROOT / 'data' / 'outputs_real'

    vis_path = DATA / 'equity_visualization_data.csv'
    json_path = DATA / 'equity_analysis_results.json'

    if not vis_path.exists():
        raise FileNotFoundError(
            f"equity_visualization_data.csv not found at {vis_path}\n"
            "Run: python src/equity_analysis.py  first."
        )

    df = pd.read_csv(vis_path)
    print(f"Loaded visualization data: {len(df)} countries")

    # Load correlation results for dashboard table
    corr_json = {}
    if json_path.exists():
        with open(json_path) as f:
            analysis = json.load(f)
        corr_json = analysis.get('correlations', {})

    # Identify score columns
    score_mean_cols = [c for c in df.columns if c.endswith('_mean') and
                       any(s in c for s in ['early_warning', 'compliance', 'cross_border', 'composite'])]
    primary_score = 'composite_score_mean' if 'composite_score_mean' in score_mean_cols else score_mean_cols[0]

    # ── Scatter plots ────────────────────────────────────────────────────────
    print("Generating scatter plots...")
    scatter_gdp = scatter_html(df, 'gdp_per_capita_pps', primary_score,
        "Metadata Quality vs GDP per capita",
        "GDP per capita (PPS, EU27=100)", "Composite Metadata Score")

    scatter_desi = scatter_html(df, 'desi_score', primary_score,
        "Metadata Quality vs DESI Score",
        "DESI Score 2023 (0–100)", "Composite Metadata Score")

    scatter_wei = scatter_html(df, 'water_exploitation_index', primary_score,
        "Metadata Quality vs Water Exploitation Index",
        "Water Exploitation Index (% renewable water)", "Composite Metadata Score")

    scatter_odm = scatter_html(df, 'open_data_maturity', primary_score,
        "Metadata Quality vs Open Data Maturity",
        "Open Data Maturity 2023 (%)", "Composite Metadata Score")

    # Write individual files
    (VIZ_DIR / 'equity_scatter_gdp.html').write_text(scatter_gdp,  encoding='utf-8')
    (VIZ_DIR / 'equity_scatter_desi.html').write_text(scatter_desi, encoding='utf-8')
    (VIZ_DIR / 'equity_scatter_wei.html').write_text(scatter_wei,   encoding='utf-8')
    (VIZ_DIR / 'equity_scatter_odm.html').write_text(scatter_odm,   encoding='utf-8')

    # ── Heatmap ──────────────────────────────────────────────────────────────
    print("Generating heatmap...")
    heatmap = heatmap_html(df, score_mean_cols,
        "EU Water Metadata Quality by Country and Task")
    (VIZ_DIR / 'equity_country_heatmap.html').write_text(heatmap, encoding='utf-8')

    # ── Bar chart ────────────────────────────────────────────────────────────
    print("Generating bar chart...")
    bar = bar_country_html(df, primary_score,
        "Composite Metadata Quality Score by Country")
    (VIZ_DIR / 'equity_bar_countries.html').write_text(bar, encoding='utf-8')

    # ── Country data table (HTML) ────────────────────────────────────────────
    disp_cols = (['country_code', 'country_name', 'n_datasets'] +
                 score_mean_cols +
                 ['gdp_per_capita_pps', 'desi_score',
                  'water_exploitation_index', 'open_data_maturity'])
    disp_cols = [c for c in disp_cols if c in df.columns]
    tbl = df[disp_cols].sort_values(primary_score, ascending=False).round(3)

    header_row = "<tr>" + "".join(f"<th>{c.replace('_',' ')}</th>" for c in tbl.columns) + "</tr>"
    data_rows  = ""
    for _, row in tbl.iterrows():
        data_rows += "<tr>" + "".join(f"<td>{v}</td>" for v in row.values) + "</tr>"
    country_table_html = (
        "<h3>Per-Country Data Table</h3>"
        f"<table><thead>{header_row}</thead><tbody>{data_rows}</tbody></table>"
    )

    # ── Dashboard ────────────────────────────────────────────────────────────
    print("Generating dashboard...")
    dashboard = dashboard_html(scatter_gdp, scatter_desi, scatter_wei,
                                scatter_odm, heatmap, bar,
                                country_table_html, corr_json)
    dash_path = VIZ_DIR / 'equity_dashboard.html'
    dash_path.write_text(dashboard, encoding='utf-8')

    print("\n=== Equity Visualizations Created ===")
    for name in ['equity_scatter_gdp.html', 'equity_scatter_desi.html',
                 'equity_scatter_wei.html', 'equity_scatter_odm.html',
                 'equity_country_heatmap.html', 'equity_bar_countries.html',
                 'equity_dashboard.html']:
        print(f"  ✓ visualizations/{name}")


if __name__ == '__main__':
    main()
