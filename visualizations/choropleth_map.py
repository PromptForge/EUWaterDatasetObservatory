#!/usr/bin/env python3
"""
Create choropleth maps of Europe showing metadata quality by country.
Uses self-contained HTML with Plotly CDN — no extra Python packages needed.

Note: Plotly's choropleth with locationmode='ISO-3' expects 3-letter ISO codes.
      The data uses 2-letter ISO codes, so we map them to 3-letter codes here.
"""

import pandas as pd
import json
from pathlib import Path


# ── ISO-2 → ISO-3 mapping (EU27) ─────────────────────────────────────────────

ISO2_TO_ISO3 = {
    "AT": "AUT", "BE": "BEL", "BG": "BGR", "HR": "HRV",
    "CY": "CYP", "CZ": "CZE", "DK": "DNK", "EE": "EST",
    "FI": "FIN", "FR": "FRA", "DE": "DEU", "GR": "GRC",
    "HU": "HUN", "IE": "IRL", "IT": "ITA", "LV": "LVA",
    "LT": "LTU", "LU": "LUX", "MT": "MLT", "NL": "NLD",
    "PL": "POL", "PT": "PRT", "RO": "ROU", "SK": "SVK",
    "SI": "SVN", "ES": "ESP", "SE": "SWE",
}

COUNTRY_NAMES = {
    "AT": "Austria",     "BE": "Belgium",     "BG": "Bulgaria",
    "HR": "Croatia",     "CY": "Cyprus",      "CZ": "Czechia",
    "DK": "Denmark",     "EE": "Estonia",     "FI": "Finland",
    "FR": "France",      "DE": "Germany",     "GR": "Greece",
    "HU": "Hungary",     "IE": "Ireland",     "IT": "Italy",
    "LV": "Latvia",      "LT": "Lithuania",   "LU": "Luxembourg",
    "MT": "Malta",       "NL": "Netherlands", "PL": "Poland",
    "PT": "Portugal",    "RO": "Romania",     "SK": "Slovakia",
    "SI": "Slovenia",    "ES": "Spain",       "SE": "Sweden",
}

SCORE_LABELS = {
    "composite_score_mean":           "Overall Quality Score",
    "early_warning_score_mean":       "Early Warning Readiness",
    "compliance_reporting_score_mean":"Compliance Reporting Readiness",
    "cross_border_score_mean":        "Cross-Border Coordination Readiness",
}


# ── HTML template ─────────────────────────────────────────────────────────────

def _build_choropleth_html(title: str, subtitle: str,
                           iso3_codes: list, values: list,
                           hover_text: list, custom_data: list,
                           colorscale: str, cbar_title: str,
                           vmin: float, vmax: float,
                           has_custom: bool) -> str:
    """Return full HTML string for a choropleth map."""

    if has_custom:
        hover_tmpl = "<b>%{text}</b><br>Score: %{z:.3f}<br>Datasets: %{customdata}<extra></extra>"
    else:
        hover_tmpl = "<b>%{text}</b><br>Score: %{z:.3f}<extra></extra>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    body   {{ font-family: Arial, sans-serif; margin: 0; padding: 20px;
              background: #f8f9fa; }}
    h1     {{ color: #2E75B6; text-align: center; margin-bottom: 4px; }}
    p.sub  {{ text-align: center; color: #555; margin-top: 0; font-size: 0.9em; }}
    #map   {{ width: 100%; height: 680px; }}
    footer {{ text-align: center; color: #888; font-size: 0.8em; margin-top: 8px; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <p class="sub">{subtitle}</p>
  <div id="map"></div>
  <footer>
    Color scale: 🔴 Low quality &nbsp;→&nbsp; 🟡 Medium &nbsp;→&nbsp; 🟢 High quality
    &nbsp;|&nbsp; Based on EU water datasets from SPARQL harvesting
  </footer>

  <script>
    var data = [{{
      type: 'choropleth',
      locationmode: 'ISO-3',
      locations: {json.dumps(iso3_codes)},
      z: {json.dumps(values)},
      text: {json.dumps(hover_text)},
      customdata: {json.dumps(custom_data)},
      colorscale: {json.dumps(colorscale)},
      reversescale: false,
      zmin: {vmin:.4f},
      zmax: {vmax:.4f},
      marker: {{
        line: {{ color: 'rgb(150,150,150)', width: 0.8 }}
      }},
      colorbar: {{
        title: '{cbar_title}',
        thickness: 18,
        len: 0.6,
        tickformat: '.2f'
      }},
      hovertemplate: '{hover_tmpl}'
    }}];

    var layout = {{
      geo: {{
        scope: 'europe',
        resolution: 50,
        showframe: false,
        showcoastlines: true,
        coastlinecolor: 'rgb(150,150,150)',
        showland: true,
        landcolor: 'rgb(240,240,240)',
        showocean: true,
        oceancolor: 'rgb(220,240,255)',
        showlakes: true,
        lakecolor: 'rgb(220,240,255)',
        showcountries: true,
        countrycolor: 'rgb(180,180,180)',
        lonaxis: {{ range: [-14, 36] }},
        lataxis: {{ range: [33, 72] }}
      }},
      margin: {{ t: 10, b: 10, l: 10, r: 10 }}
    }};

    Plotly.newPlot('map', data, layout, {{responsive: true, displayModeBar: true}});
  </script>
</body>
</html>"""


def _build_count_html(title: str,
                      iso3_codes: list, values: list,
                      hover_text: list) -> str:
    """Return full HTML string for dataset count choropleth."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    body  {{ font-family: Arial, sans-serif; margin: 0; padding: 20px;
             background: #f8f9fa; }}
    h1    {{ color: #2E75B6; text-align: center; }}
    #map  {{ width: 100%; height: 680px; }}
    footer{{ text-align: center; color: #888; font-size: 0.8em; margin-top: 8px; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <div id="map"></div>
  <footer>Darker blue = more water datasets harvested from EU open data portals</footer>

  <script>
    var data = [{{
      type: 'choropleth',
      locationmode: 'ISO-3',
      locations: {json.dumps(iso3_codes)},
      z: {json.dumps(values)},
      text: {json.dumps(hover_text)},
      colorscale: 'Blues',
      reversescale: false,
      marker: {{
        line: {{ color: 'rgb(150,150,150)', width: 0.8 }}
      }},
      colorbar: {{
        title: 'Number of<br>Datasets',
        thickness: 18,
        len: 0.6
      }},
      hovertemplate: '<b>%{{text}}</b><br>Datasets: %{{z}}<extra></extra>'
    }}];

    var layout = {{
      geo: {{
        scope: 'europe',
        resolution: 50,
        showframe: false,
        showcoastlines: true,
        coastlinecolor: 'rgb(150,150,150)',
        showland: true,
        landcolor: 'rgb(240,240,240)',
        showocean: true,
        oceancolor: 'rgb(220,240,255)',
        lonaxis: {{ range: [-14, 36] }},
        lataxis: {{ range: [33, 72] }}
      }},
      margin: {{ t: 10, b: 10, l: 10, r: 10 }}
    }};

    Plotly.newPlot('map', data, layout, {{responsive: true, displayModeBar: true}});
  </script>
</body>
</html>"""


# ── Public helpers ────────────────────────────────────────────────────────────

# A diverging red→yellow→green colorscale (matches Plotly 'RdYlGn')
RDYLGN = [
    [0.0,  "rgb(165,0,38)"],
    [0.1,  "rgb(215,48,39)"],
    [0.2,  "rgb(244,109,67)"],
    [0.3,  "rgb(253,174,97)"],
    [0.4,  "rgb(254,224,139)"],
    [0.5,  "rgb(255,255,191)"],
    [0.6,  "rgb(217,239,139)"],
    [0.7,  "rgb(166,217,106)"],
    [0.8,  "rgb(102,189,99)"],
    [0.9,  "rgb(26,152,80)"],
    [1.0,  "rgb(0,104,55)"],
]


def create_choropleth_html(df: pd.DataFrame, value_col: str,
                           title: str, subtitle: str,
                           output_path: Path,
                           colorscale=None) -> None:
    """Write an interactive choropleth map to *output_path*."""
    if colorscale is None:
        colorscale = RDYLGN

    df_plot = df.copy()
    if "country_code" not in df_plot.columns:
        df_plot = df_plot.rename(columns={"country": "country_code"})

    df_plot["iso3"]  = df_plot["country_code"].map(ISO2_TO_ISO3)
    df_plot["cname"] = df_plot["country_code"].map(COUNTRY_NAMES).fillna(df_plot["country_code"])

    # Drop rows where iso3 or value is missing
    df_plot = df_plot.dropna(subset=["iso3", value_col])

    iso3_codes  = df_plot["iso3"].tolist()
    values      = [round(v, 4) for v in df_plot[value_col].tolist()]
    hover_text  = df_plot["cname"].tolist()

    has_custom = "n_datasets" in df_plot.columns
    custom_data = df_plot["n_datasets"].astype(int).tolist() if has_custom else [None] * len(iso3_codes)

    vmin = min(values)
    vmax = max(values)
    cbar_title = "Metadata\\nQuality\\nScore"

    html = _build_choropleth_html(
        title=title,
        subtitle=subtitle,
        iso3_codes=iso3_codes,
        values=values,
        hover_text=hover_text,
        custom_data=custom_data,
        colorscale=colorscale,
        cbar_title=cbar_title,
        vmin=vmin,
        vmax=vmax,
        has_custom=has_custom,
    )

    output_path.write_text(html, encoding="utf-8")
    print(f"  Created: {output_path.name}  ({len(iso3_codes)} countries)")


def create_dataset_count_map(df: pd.DataFrame, output_path: Path) -> None:
    """Write dataset-count choropleth to *output_path*."""
    df_plot = df.copy()
    if "country_code" not in df_plot.columns:
        df_plot = df_plot.rename(columns={"country": "country_code"})

    df_plot["iso3"]  = df_plot["country_code"].map(ISO2_TO_ISO3)
    df_plot["cname"] = df_plot["country_code"].map(COUNTRY_NAMES).fillna(df_plot["country_code"])
    df_plot = df_plot.dropna(subset=["iso3", "n_datasets"])

    html = _build_count_html(
        title="Number of Water Datasets by EU Country",
        iso3_codes=df_plot["iso3"].tolist(),
        values=df_plot["n_datasets"].astype(int).tolist(),
        hover_text=df_plot["cname"].tolist(),
    )

    output_path.write_text(html, encoding="utf-8")
    print(f"  Created: {output_path.name}  ({len(df_plot)} countries)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ROOT       = Path(__file__).parent.parent
    OUTPUT_DIR = ROOT / "data" / "outputs_real"
    VIZ_DIR    = ROOT / "visualizations"

    print("=" * 65)
    print("CREATING CHOROPLETH MAPS OF EUROPE")
    print("=" * 65)

    merged_path = OUTPUT_DIR / "equity_merged_data.csv"
    if not merged_path.exists():
        print(f"ERROR: {merged_path} not found!")
        print("Run:  python src/equity_analysis.py  first.")
        return

    df = pd.read_csv(merged_path)
    print(f"Loaded {len(df)} countries")
    print(f"Columns: {list(df.columns)}\n")

    # ── 1. Overall composite quality ──────────────────────────────────────────
    composite_col = None
    for candidate in ["composite_score_mean", "composite_mean", "overall_mean"]:
        if candidate in df.columns:
            composite_col = candidate
            break

    # If composite is not a column, compute it from the three task means
    if composite_col is None:
        task_means = [c for c in df.columns if c.endswith("_mean") and
                      any(t in c for t in ["early_warning", "compliance", "cross_border"])]
        if task_means:
            df["composite_score_mean"] = df[task_means].mean(axis=1)
            composite_col = "composite_score_mean"
            print(f"  Computed composite_score_mean from: {task_means}")

    if composite_col:
        create_choropleth_html(
            df, composite_col,
            title="EU Water Data: Metadata Quality by Country",
            subtitle="Composite score across early-warning, compliance and cross-border readiness",
            output_path=VIZ_DIR / "choropleth_quality.html",
        )

    # ── 2. Task-specific maps ─────────────────────────────────────────────────
    task_map = {
        "early_warning_score_mean":       ("Early Warning Readiness",
                                           "Readiness of national water data for early-warning systems"),
        "compliance_reporting_score_mean":("Compliance Reporting Readiness",
                                           "Readiness for EU compliance reporting (WFD, Floods Directive)"),
        "cross_border_score_mean":        ("Cross-Border Coordination Readiness",
                                           "Readiness of data for transboundary river-basin coordination"),
    }

    for col, (short_title, subtitle) in task_map.items():
        if col in df.columns:
            create_choropleth_html(
                df, col,
                title=f"EU Water Data: {short_title}",
                subtitle=subtitle,
                output_path=VIZ_DIR / f"choropleth_{col.replace('_score_mean','').replace('_reporting','')}.html",
            )
        else:
            print(f"  Skipping {col} — not found in data")

    # ── 3. Dataset count map ──────────────────────────────────────────────────
    if "n_datasets" in df.columns:
        create_dataset_count_map(df, VIZ_DIR / "choropleth_dataset_count.html")
    else:
        print("  Skipping dataset count map — 'n_datasets' column not found")

    print("\n" + "=" * 65)
    print("ALL CHOROPLETH MAPS CREATED")
    print("=" * 65)
    print(f"Output directory: {VIZ_DIR}")
    maps = list(VIZ_DIR.glob("choropleth_*.html"))
    for m in sorted(maps):
        print(f"  {m.name}")


if __name__ == "__main__":
    main()
