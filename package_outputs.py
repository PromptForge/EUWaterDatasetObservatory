#!/usr/bin/env python3
"""
Package all Idea 3 (Equity Analysis) outputs into a single ZIP file.
"""

import zipfile
from pathlib import Path
from datetime import datetime


def create_package():
    ROOT = Path(__file__).parent
    OUTPUT_DIR = ROOT / "data" / "outputs_real"
    VIZ_DIR = ROOT / "visualizations"

    # Define files to include
    files_to_include = [
        # Data files
        OUTPUT_DIR / "datasets_with_country.csv",
        OUTPUT_DIR / "eurostat_indicators.csv",
        OUTPUT_DIR / "equity_merged_data.csv",
        OUTPUT_DIR / "country_detailed_stats.csv",
        OUTPUT_DIR / "equity_analysis_results.json",
        OUTPUT_DIR / "enhanced_equity_results.json",
        OUTPUT_DIR / "dimension_correlations.csv",
        OUTPUT_DIR / "vif_results.csv",
        OUTPUT_DIR / "regression_table.tex",
        OUTPUT_DIR / "regression_table_wls.tex",

        # Visualizations
        VIZ_DIR / "equity_dashboard.html",
        VIZ_DIR / "equity_scatter_gdp.html",
        VIZ_DIR / "equity_scatter_desi.html",
        VIZ_DIR / "equity_scatter_wei.html",
        VIZ_DIR / "equity_scatter_odm.html",
        VIZ_DIR / "equity_country_heatmap.html",
        VIZ_DIR / "equity_bar_countries.html",
        VIZ_DIR / "grouped_bar_dimensions.html",
        VIZ_DIR / "choropleth_quality.html",
        VIZ_DIR / "choropleth_early_warning.html",
        VIZ_DIR / "choropleth_compliance.html",
        VIZ_DIR / "choropleth_cross_border.html",
        VIZ_DIR / "choropleth_dataset_count.html",
    ]

    # Create ZIP
    timestamp = datetime.now().strftime("%Y%m%d")
    zip_path = ROOT / f"equity_analysis_package_{timestamp}.zip"

    added = []
    missing = []

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filepath in files_to_include:
            if filepath.exists():
                arcname = filepath.relative_to(ROOT)
                zf.write(filepath, arcname)
                added.append(str(arcname))
                print(f"  ✓ Added: {arcname}")
            else:
                missing.append(str(filepath.relative_to(ROOT)))
                print(f"  ✗ MISSING: {filepath.relative_to(ROOT)}")

    size_kb = zip_path.stat().st_size / 1024
    size_mb = size_kb / 1024

    print(f"\n{'='*60}")
    print(f"✓ Package created: {zip_path.name}")
    print(f"  Size: {size_kb:.1f} KB ({size_mb:.2f} MB)")
    print(f"  Files included: {len(added)}")
    print(f"  Files missing:  {len(missing)}")

    if missing:
        print(f"\nMissing files:")
        for f in missing:
            print(f"    - {f}")

    return zip_path


if __name__ == "__main__":
    create_package()
