#!/usr/bin/env python3
"""
EU Water Dataset Observatory - Full Analysis Pipeline

Run this script to execute the complete analysis:
1. Simulate 3,500 metadata records
2. Compute task-specific sufficiency scores
3. Run sensitivity analysis
4. Compute impact and priority scores
5. Generate all visualizations
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def main():
    print("=" * 60)
    print("EU WATER DATASET OBSERVATORY - FULL ANALYSIS")
    print("=" * 60)
    
    # Step 1: Simulate records
    print("\n[1/5] Simulating metadata records...")
    from simulate_records import main as simulate
    simulate()
    
    # Step 2: Compute sufficiency scores
    print("\n[2/5] Computing sufficiency scores...")
    from sufficiency_scoring import main as score
    score()
    
    # Step 3: Sensitivity analysis
    print("\n[3/5] Running sensitivity analysis...")
    from sensitivity_analysis import main as sensitivity
    sensitivity()
    
    # Step 4: Impact and priority scores
    print("\n[4/5] Computing impact and priority scores...")
    from impact_proxy import main as impact
    impact()
    
    # Step 5: Generate visualizations
    print("\n[5/5] Generating visualizations...")
    sys.path.insert(0, str(Path(__file__).parent / "visualizations"))
    from create_visualizations import main as visualize
    visualize()
    
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    print("\nOutputs available in:")
    print("  - data/simulated/metadata_records.csv (3,500 records)")
    print("  - data/outputs/sufficiency_scores.csv")
    print("  - data/outputs/sensitivity_summary.csv")
    print("  - data/outputs/priority_scores.csv")
    print("  - data/outputs/analysis_summary.json")
    print("  - visualizations/*.html")

if __name__ == "__main__":
    main()
