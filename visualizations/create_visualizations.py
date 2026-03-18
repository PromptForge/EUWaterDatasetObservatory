"""
Generate HTML visualizations for the EU Water Dataset Observatory.
"""

import json
from pathlib import Path
import pandas as pd
import numpy as np

OUTPUT_DIR = Path(__file__).parent

def create_country_task_heatmap(df):
    """Create country × task heatmap as HTML."""
    
    # Aggregate by country and task
    countries = df["country"].unique()
    tasks = ["early_warning", "compliance_reporting", "cross_border"]
    
    heatmap_data = []
    for country in sorted(countries):
        country_df = df[df["country"] == country]
        row = {"country": country}
        for task in tasks:
            row[task] = country_df[f"{task}_score"].mean()
        heatmap_data.append(row)
    
    heatmap_df = pd.DataFrame(heatmap_data)
    
    # Generate HTML
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Country × Task Sufficiency Heatmap</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #2e75b6; }
        .description { margin-bottom: 20px; color: #666; }
    </style>
</head>
<body>
    <h1>EU Water Dataset Observatory: Country × Task Sufficiency</h1>
    <p class="description">Mean sufficiency scores by country and management task. 
    Higher scores (yellow/green) indicate better metadata readiness for operational use.</p>
    <div id="heatmap"></div>
    <script>
        var data = [{
            z: """ + json.dumps([
                [row["early_warning"], row["compliance_reporting"], row["cross_border"]] 
                for _, row in heatmap_df.iterrows()
            ]) + """,
            x: ['Early Warning', 'Compliance Reporting', 'Cross-Border'],
            y: """ + json.dumps(list(heatmap_df["country"])) + """,
            type: 'heatmap',
            colorscale: 'RdYlGn',
            zmin: 0,
            zmax: 1
        }];
        
        var layout = {
            title: 'Task-Specific Sufficiency by Country',
            xaxis: {title: 'Management Task'},
            yaxis: {title: 'Country', autorange: 'reversed'},
            height: 800
        };
        
        Plotly.newPlot('heatmap', data, layout);
    </script>
</body>
</html>"""
    
    output_path = OUTPUT_DIR / "country_task_heatmap.html"
    with open(output_path, "w") as f:
        f.write(html)
    print(f"Created {output_path}")

def create_failure_modes_chart(df):
    """Create failure modes bar chart."""
    
    # Calculate failure rates
    failures = {
        "Missing update frequency": (df["update_frequency"] == "unknown").mean() * 100,
        "No machine-readable format": (df["format"].isin(["PDF", ""])).mean() * 100,
        "No spatial coordinates": (~df["has_coordinates"]).mean() * 100,
        "Description < 50 words": (df["description_length"] < 50).mean() * 100,
        "No temporal coverage": (df["temporal_span_years"] == 0).mean() * 100,
        "No EuroVoc keywords": (~df["has_eurovoc"]).mean() * 100,
        "No open license": (~df["is_open_license"]).mean() * 100,
        "Single language only": (~df["has_multilingual"]).mean() * 100
    }
    
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Metadata Failure Modes</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #2e75b6; }
    </style>
</head>
<body>
    <h1>EU Water Dataset Observatory: Metadata Failure Modes</h1>
    <div id="chart"></div>
    <script>
        var data = [{
            x: """ + json.dumps(list(failures.values())) + """,
            y: """ + json.dumps(list(failures.keys())) + """,
            type: 'bar',
            orientation: 'h',
            marker: {color: '#c0392b'}
        }];
        
        var layout = {
            title: 'Percentage of Datasets with Each Metadata Gap',
            xaxis: {title: 'Percentage of Datasets (%)'},
            yaxis: {autorange: 'reversed'},
            margin: {l: 200}
        };
        
        Plotly.newPlot('chart', data, layout);
    </script>
</body>
</html>"""
    
    output_path = OUTPUT_DIR / "failure_modes_chart.html"
    with open(output_path, "w") as f:
        f.write(html)
    print(f"Created {output_path}")

def create_sensitivity_chart(sensitivity_df):
    """Create sensitivity analysis visualization."""
    
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Sensitivity Analysis</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #2e75b6; }
        .interpretation { background: #f0f0f0; padding: 15px; margin-top: 20px; }
    </style>
</head>
<body>
    <h1>Sensitivity Analysis: Weight Perturbation (±25%)</h1>
    <div id="chart"></div>
    <div class="interpretation">
        <h3>Interpretation</h3>
        <p>This chart shows how the percentage of "Ready" datasets changes when the primary 
        weight for each task is perturbed by ±25%. Small ranges indicate robust conclusions.</p>
    </div>
    <script>
        var tasks = """ + json.dumps(list(sensitivity_df["task"])) + """;
        var baseline = """ + json.dumps(list(sensitivity_df["baseline_ready_pct"])) + """;
        var low = """ + json.dumps(list(sensitivity_df["low_ready_pct"])) + """;
        var high = """ + json.dumps(list(sensitivity_df["high_ready_pct"])) + """;
        
        var trace1 = {
            x: tasks,
            y: baseline,
            name: 'Baseline',
            type: 'bar',
            marker: {color: '#3498db'}
        };
        
        var trace2 = {
            x: tasks,
            y: low,
            name: '-25% Primary Weight',
            type: 'bar',
            marker: {color: '#e74c3c'}
        };
        
        var trace3 = {
            x: tasks,
            y: high,
            name: '+25% Primary Weight',
            type: 'bar',
            marker: {color: '#2ecc71'}
        };
        
        var layout = {
            title: 'Ready Dataset Percentage Under Weight Perturbation',
            yaxis: {title: 'Percentage Ready (%)'},
            barmode: 'group'
        };
        
        Plotly.newPlot('chart', [trace1, trace2, trace3], layout);
    </script>
</body>
</html>"""
    
    output_path = OUTPUT_DIR / "sensitivity_analysis.html"
    with open(output_path, "w") as f:
        f.write(html)
    print(f"Created {output_path}")

def create_priority_fixes_table(df):
    """Create top-20 priority fixes HTML table."""
    
    top_fixes = df.nlargest(20, "early_warning_priority")[
        ["title", "domain", "country", "impact_score", "early_warning_score", "early_warning_priority", "format", "has_coordinates", "update_frequency"]
    ]
    
    # Determine suggested fix for each
    fixes = []
    for _, row in top_fixes.iterrows():
        if row["update_frequency"] == "unknown":
            fix = "Add update frequency metadata"
        elif not row["has_coordinates"]:
            fix = "Add spatial coordinates/bbox"
        elif row["format"] == "PDF":
            fix = "Provide machine-readable format"
        else:
            fix = "Improve description quality"
        fixes.append(fix)
    
    top_fixes["suggested_fix"] = fixes
    
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Priority Metadata Fixes</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #2e75b6; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #2e75b6; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .high-priority { background-color: #ffcccc; }
    </style>
</head>
<body>
    <h1>Top 20 Priority Metadata Fixes (Early Warning Task)</h1>
    <p>Datasets ranked by Priority = Impact × (1 - Sufficiency)</p>
    <table>
        <tr>
            <th>Rank</th>
            <th>Dataset</th>
            <th>Domain</th>
            <th>Country</th>
            <th>Impact</th>
            <th>Sufficiency</th>
            <th>Priority</th>
            <th>Suggested Fix</th>
        </tr>
"""
    
    for i, (_, row) in enumerate(top_fixes.iterrows(), 1):
        html += f"""        <tr>
            <td>{i}</td>
            <td>{row['title'][:50]}...</td>
            <td>{row['domain']}</td>
            <td>{row['country']}</td>
            <td>{row['impact_score']:.2f}</td>
            <td>{row['early_warning_score']:.2f}</td>
            <td>{row['early_warning_priority']:.3f}</td>
            <td>{row['suggested_fix']}</td>
        </tr>
"""
    
    html += """    </table>
</body>
</html>"""
    
    output_path = OUTPUT_DIR / "priority_fixes.html"
    with open(output_path, "w") as f:
        f.write(html)
    print(f"Created {output_path}")

def create_domain_summary_chart(df):
    """Create domain summary bar chart."""
    
    domain_means = df.groupby("domain")[
        ["early_warning_score", "compliance_reporting_score", "cross_border_score"]
    ].mean()
    
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Domain Summary</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #2e75b6; }
    </style>
</head>
<body>
    <h1>Mean Sufficiency Scores by Water Domain</h1>
    <div id="chart"></div>
    <script>
        var domains = """ + json.dumps(list(domain_means.index)) + """;
        
        var trace1 = {
            x: domains,
            y: """ + json.dumps(list(domain_means["early_warning_score"])) + """,
            name: 'Early Warning',
            type: 'bar',
            marker: {color: '#e74c3c'}
        };
        
        var trace2 = {
            x: domains,
            y: """ + json.dumps(list(domain_means["compliance_reporting_score"])) + """,
            name: 'Compliance Reporting',
            type: 'bar',
            marker: {color: '#2ecc71'}
        };
        
        var trace3 = {
            x: domains,
            y: """ + json.dumps(list(domain_means["cross_border_score"])) + """,
            name: 'Cross-Border',
            type: 'bar',
            marker: {color: '#3498db'}
        };
        
        var layout = {
            title: 'Task-Specific Sufficiency by Domain',
            yaxis: {title: 'Mean Sufficiency Score', range: [0, 1]},
            barmode: 'group'
        };
        
        Plotly.newPlot('chart', [trace1, trace2, trace3], layout);
    </script>
</body>
</html>"""
    
    output_path = OUTPUT_DIR / "domain_summary.html"
    with open(output_path, "w") as f:
        f.write(html)
    print(f"Created {output_path}")

def create_analysis_summary_json(df, sensitivity_df):
    """Create JSON summary of all analysis results."""
    
    summary = {
        "metadata": {
            "generated": pd.Timestamp.now().isoformat(),
            "total_records": len(df),
            "domains": list(df["domain"].unique()),
            "countries": list(df["country"].unique())
        },
        "overall_statistics": {
            "early_warning": {
                "mean": float(df["early_warning_score"].mean()),
                "median": float(df["early_warning_score"].median()),
                "std": float(df["early_warning_score"].std()),
                "ready_pct": float((df["early_warning_readiness"] == "Ready").mean() * 100),
                "partial_pct": float((df["early_warning_readiness"] == "Partial").mean() * 100),
                "insufficient_pct": float((df["early_warning_readiness"] == "Insufficient").mean() * 100)
            },
            "compliance_reporting": {
                "mean": float(df["compliance_reporting_score"].mean()),
                "median": float(df["compliance_reporting_score"].median()),
                "std": float(df["compliance_reporting_score"].std()),
                "ready_pct": float((df["compliance_reporting_readiness"] == "Ready").mean() * 100),
                "partial_pct": float((df["compliance_reporting_readiness"] == "Partial").mean() * 100),
                "insufficient_pct": float((df["compliance_reporting_readiness"] == "Insufficient").mean() * 100)
            },
            "cross_border": {
                "mean": float(df["cross_border_score"].mean()),
                "median": float(df["cross_border_score"].median()),
                "std": float(df["cross_border_score"].std()),
                "ready_pct": float((df["cross_border_readiness"] == "Ready").mean() * 100),
                "partial_pct": float((df["cross_border_readiness"] == "Partial").mean() * 100),
                "insufficient_pct": float((df["cross_border_readiness"] == "Insufficient").mean() * 100)
            }
        },
        "domain_statistics": df.groupby("domain")[
            ["early_warning_score", "compliance_reporting_score", "cross_border_score"]
        ].mean().to_dict(),
        "sensitivity_analysis": sensitivity_df.to_dict(orient="records"),
        "completeness_rates": {
            "has_description": float((df["description_length"] > 0).mean()),
            "has_temporal": float((df["temporal_span_years"] > 0).mean()),
            "has_coordinates": float(df["has_coordinates"].mean()),
            "has_open_license": float(df["is_open_license"].mean()),
            "has_eurovoc": float(df["has_eurovoc"].mean()),
            "has_multilingual": float(df["has_multilingual"].mean())
        }
    }
    
    output_path = Path(__file__).parent.parent / "data" / "outputs_real" / "analysis_summary.json"
    with open(output_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Created {output_path}")
    
    return summary

def main():
    # Load data
    data_dir = Path(__file__).parent.parent / "data" / "outputs_real"
    
    df = pd.read_csv(data_dir / "priority_scores.csv")
    sensitivity_df = pd.read_csv(data_dir / "sensitivity_summary.csv")
    
    print("Generating visualizations...")
    
    create_country_task_heatmap(df)
    create_failure_modes_chart(df)
    create_sensitivity_chart(sensitivity_df)
    create_priority_fixes_table(df)
    create_domain_summary_chart(df)
    
    summary = create_analysis_summary_json(df, sensitivity_df)
    
    print("\n=== ALL VISUALIZATIONS CREATED ===")
    print(f"Total records analyzed: {len(df)}")
    print(f"Output directory: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
