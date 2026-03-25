#!/usr/bin/env python3
"""
EU Water Dataset Observatory - Full Analysis Pipeline
Runs SPARQL harvest and scoring on real EU water datasets.

Optional flags:
  --climate   Also run climate adaptation analysis
              (harvests climate datasets & scores against drought_early_warning,
               climate_infrastructure, and nbs_monitoring task profiles)
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
SRC = ROOT / "src"

def run_step(name: str, script: str, *args):
    """Run a pipeline step and exit on failure."""
    print(f"\n{'='*60}")
    print(f"STEP: {name}")
    print(f"{'='*60}")
    
    cmd = [sys.executable, str(SRC / script)] + list(args)
    result = subprocess.run(cmd, cwd=str(ROOT))
    
    if result.returncode != 0:
        print(f"ERROR: {name} failed with code {result.returncode}")
        sys.exit(result.returncode)
    
    print(f"✓ {name} completed successfully")

def main():
    print("EU Water Dataset Observatory - Full Analysis Pipeline")
    print("="*60)
    
    # Step 1: Harvest data from data.europa.eu (skip if already done)
    harvest_output = ROOT / "data" / "harvested" / "raw_harvest.csv"
    if harvest_output.exists():
        print(f"\n✓ Harvest data already exists: {harvest_output}")
        print("  (Delete this file to re-run harvest)")
    else:
        run_step("SPARQL Harvest", "harvest_sparql.py")
    
    # Step 2: Score real data
    run_step("Score Real Data", "score_real_data.py")
    
    # Step 3: Generate visualizations (optional)
    viz_script = ROOT / "visualizations" / "create_visualizations.py"
    if viz_script.exists():
        print(f"\n{'='*60}")
        print(f"STEP: Create Visualizations")
        print(f"{'='*60}")
        result = subprocess.run([sys.executable, str(viz_script)], cwd=str(ROOT))
        if result.returncode != 0:
            print(f"ERROR: Create Visualizations failed with code {result.returncode}")
            sys.exit(result.returncode)
        print(f"✓ Create Visualizations completed successfully")
    
    # ── Optional: Climate Adaptation Analysis ────────────────────────────────
    run_climate = "--climate" in sys.argv
    climate_harvest = ROOT / "data" / "harvested" / "climate_harvest.csv"

    if run_climate:
        print("\n" + "="*60)
        print("CLIMATE ADAPTATION ANALYSIS")
        print("="*60)

        if climate_harvest.exists():
            print(f"\n✓ Climate harvest data already exists: {climate_harvest}")
            print("  (Delete this file to re-run climate harvest)")
        else:
            run_step("Climate SPARQL Harvest", "harvest_climate.py")

        run_step("Score Climate Data", "score_climate_data.py")

        print(f"\n✓ Climate outputs available in: {ROOT / 'data' / 'outputs_climate'}")
    else:
        print("\n  (Run with --climate flag to include climate adaptation analysis)")

    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"\nOutputs available in: {ROOT / 'data' / 'outputs_real'}")
    if run_climate:
        print(f"Climate outputs in  : {ROOT / 'data' / 'outputs_climate'}")

if __name__ == "__main__":
    main()
