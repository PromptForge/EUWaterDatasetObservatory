#!/usr/bin/env python3
"""
Create grouped bar chart showing dimension scores for top-5 and bottom-5 countries.
Reveals whether low-scoring countries fail on the same dimensions as high-scoring ones.
"""

import pandas as pd
import json
from pathlib import Path


def create_grouped_bar_chart():
    ROOT = Path(__file__).parent.parent
    OUTPUT_DIR = ROOT / "data" / "outputs_real"
    VIZ_DIR = ROOT / "visualizations"

    # Load country stats
    stats_path = OUTPUT_DIR / "country_detailed_stats.csv"
    if not stats_path.exists():
        print("ERROR: Run country_detailed_stats.py first!")
        return

    df = pd.read_csv(stats_path)

    # Filter countries with >=10 datasets
    df_filtered = df[df['n_datasets'] >= 10].copy()
    print(f"Countries with >=10 datasets: {len(df_filtered)}")

    if len(df_filtered) < 4:
        print(f"WARNING: Only {len(df_filtered)} countries with >=10 datasets, using all countries")
        df_filtered = df.copy()

    n_each = min(5, len(df_filtered) // 2)
    if n_each < 1:
        n_each = 1

    # Ensure composite_mean column exists
    if 'composite_mean' not in df_filtered.columns:
        task_cols = ['early_warning_mean', 'compliance_mean', 'cross_border_mean']
        available = [c for c in task_cols if c in df_filtered.columns]
        if available:
            df_filtered['composite_mean'] = df_filtered[available].mean(axis=1)
        else:
            print("ERROR: No task score columns found!")
            return

    # Get top and bottom countries by composite mean
    top = df_filtered.nlargest(n_each, 'composite_mean')
    bottom = df_filtered.nsmallest(n_each, 'composite_mean')

    top_countries = top['country_code'].tolist()
    bottom_countries = bottom['country_code'].tolist()
    all_countries = top_countries + bottom_countries

    print(f"\nTop {n_each} countries:    {', '.join(top_countries)}")
    print(f"Bottom {n_each} countries: {', '.join(bottom_countries)}")

    # Dimension columns to plot
    dim_cols = ['early_warning_mean', 'compliance_mean', 'cross_border_mean']
    dim_labels = ['Early Warning', 'Compliance', 'Cross-Border']
    colors = ['#2E86AB', '#A23B72', '#F18F01']

    # Build traces
    traces_js = []
    for i, (col, label) in enumerate(zip(dim_cols, dim_labels)):
        values = []
        for country in all_countries:
            row = df_filtered[df_filtered['country_code'] == country]
            if len(row) > 0 and col in row.columns:
                values.append(round(float(row[col].values[0]), 4))
            else:
                values.append(0.0)

        traces_js.append(f'''{{
            name: '{label}',
            x: {json.dumps(all_countries)},
            y: {json.dumps(values)},
            type: 'bar',
            marker: {{
                color: '{colors[i]}',
                opacity: 0.85
            }},
            text: {json.dumps([f'{v:.3f}' for v in values])},
            textposition: 'outside',
            textfont: {{ size: 10 }}
        }}''')

    # Composite score trace (line overlay)
    composite_vals = []
    for country in all_countries:
        row = df_filtered[df_filtered['country_code'] == country]
        if len(row) > 0 and 'composite_mean' in row.columns:
            composite_vals.append(round(float(row['composite_mean'].values[0]), 4))
        else:
            composite_vals.append(0.0)

    traces_js.append(f'''{{
        name: 'Composite Mean',
        x: {json.dumps(all_countries)},
        y: {json.dumps(composite_vals)},
        type: 'scatter',
        mode: 'lines+markers',
        line: {{ color: '#333333', width: 2, dash: 'dot' }},
        marker: {{ size: 8, color: '#333333' }},
        yaxis: 'y'
    }}''')

    # Divider line position
    divider_x = n_each - 0.5

    # Compute y-axis max
    all_vals = [v for v in composite_vals if v > 0]
    y_max = max(all_vals) * 1.35 if all_vals else 0.5

    # Dataset counts for hover info
    n_datasets = {}
    for country in all_countries:
        row = df_filtered[df_filtered['country_code'] == country]
        if len(row) > 0:
            n_datasets[country] = int(row['n_datasets'].values[0])
        else:
            n_datasets[country] = 0

    n_labels = [f"n={n_datasets.get(c, '?')}" for c in all_countries]

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Metadata Quality by Dimension: Top vs Bottom Countries</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background: #f9f9f9;
        }}
        h1 {{
            color: #2E75B6;
            text-align: center;
            font-size: 1.3em;
            margin-bottom: 5px;
        }}
        .subtitle {{
            text-align: center;
            color: #555;
            font-size: 0.9em;
            margin-bottom: 15px;
        }}
        .note {{
            text-align: center;
            color: #666;
            font-size: 0.85em;
            margin-top: 10px;
            line-height: 1.6;
        }}
        .legend-note {{
            display: flex;
            justify-content: center;
            gap: 20px;
            margin-top: 5px;
            font-size: 0.85em;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}
        .dot-top {{ width: 12px; height: 12px; background: #28a745; border-radius: 50%; }}
        .dot-bottom {{ width: 12px; height: 12px; background: #dc3545; border-radius: 50%; }}
    </style>
</head>
<body>
    <h1>Metadata Quality by Dimension: Top {n_each} vs Bottom {n_each} Countries</h1>
    <div class="subtitle">
        Sufficiency scores per use-case task, comparing highest and lowest performing countries
    </div>

    <div id="chart" style="width:100%; height:580px;"></div>

    <div class="note">
        <div class="legend-note">
            <div class="legend-item"><div class="dot-top"></div> Top {n_each} countries (left of dashed line)</div>
            <div class="legend-item"><div class="dot-bottom"></div> Bottom {n_each} countries (right of dashed line)</div>
        </div>
        Countries with &lt;10 datasets excluded for statistical reliability.<br>
        Dataset counts: {' | '.join([f'{c}: {n_datasets.get(c,"?")} datasets' for c in all_countries])}
    </div>

    <script>
        var data = [{','.join(traces_js)}];

        var layout = {{
            barmode: 'group',
            xaxis: {{
                title: {{ text: 'Country Code', font: {{ size: 13 }} }},
                tickangle: -30,
                tickfont: {{ size: 13, color: '#333' }}
            }},
            yaxis: {{
                title: {{ text: 'Mean Sufficiency Score', font: {{ size: 13 }} }},
                range: [0, {round(y_max, 3)}],
                tickformat: '.3f'
            }},
            legend: {{
                orientation: 'h',
                x: 0.5,
                xanchor: 'center',
                y: 1.08
            }},
            shapes: [{{
                type: 'line',
                x0: {divider_x},
                x1: {divider_x},
                y0: 0,
                y1: {round(y_max, 3)},
                line: {{
                    color: '#888',
                    width: 2,
                    dash: 'dash'
                }}
            }}],
            annotations: [
                {{
                    x: {n_each / 2 - 0.5},
                    y: {round(y_max * 0.97, 3)},
                    text: '▲ TOP {n_each}',
                    showarrow: false,
                    font: {{ size: 13, color: '#28a745', weight: 'bold' }}
                }},
                {{
                    x: {n_each + n_each / 2 - 0.5},
                    y: {round(y_max * 0.97, 3)},
                    text: '▼ BOTTOM {n_each}',
                    showarrow: false,
                    font: {{ size: 13, color: '#dc3545', weight: 'bold' }}
                }}
            ],
            margin: {{ t: 60, b: 100, l: 70, r: 30 }},
            plot_bgcolor: 'white',
            paper_bgcolor: '#f9f9f9'
        }};

        Plotly.newPlot('chart', data, layout, {{responsive: true}});
    </script>
</body>
</html>'''

    output_path = VIZ_DIR / "grouped_bar_dimensions.html"
    output_path.write_text(html, encoding='utf-8')
    print(f"\nCreated: {output_path}")

    return {
        'top_countries': top_countries,
        'bottom_countries': bottom_countries,
        'n_each': n_each
    }


if __name__ == "__main__":
    create_grouped_bar_chart()
