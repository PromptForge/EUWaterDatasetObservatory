# EU Water Dataset Observatory

A reproducible framework for assessing metadata quality across EU water data portals for policy-relevant management tasks.

## Overview

This repository contains tools for:
1. **SPARQL Harvesting** - Automated collection of water dataset metadata from data.europa.eu
2. **Task-Specific Scoring** - Assessment against Early Warning, Compliance Reporting, and Cross-Border coordination requirements
3. **Priority Ranking** - Identification of high-impact metadata improvements

## Quick Start

```bash
# Install dependencies
pip install pandas requests SPARQLWrapper

# Run full pipeline
python run_full_analysis.py
```

## Repository Structure

```
├── src/
│   ├── harvest_sparql.py      # SPARQL harvester for data.europa.eu
│   ├── score_real_data.py     # Score harvested datasets
│   ├── sufficiency_scoring.py # Task-specific sufficiency calculations
│   ├── sensitivity_analysis.py # Weight perturbation analysis
│   ├── impact_proxy.py        # Dataset importance proxy
│   └── validation_analysis.py # Manual validation analysis
├── config/
│   ├── weights.json           # Task-specific dimension weights
│   └── keywords.json          # Domain-specific search keywords
├── data/
│   ├── harvested/             # Raw SPARQL harvest results
│   │   ├── raw_harvest.csv    # 2,176 real EU water datasets
│   │   └── harvest_summary.json
│   └── outputs_real/          # Scoring outputs
│       ├── sufficiency_scores.csv
│       ├── sensitivity_summary.csv
│       ├── priority_fixes.csv
│       └── analysis_summary_real.json
├── visualizations/
│   └── create_visualizations.py
├── run_full_analysis.py       # Main pipeline orchestrator
└── Validation_Checklist.xlsx  # Manual validation of 25 datasets
```

## Data

The analysis is based on **2,176 real EU water datasets** harvested via SPARQL from data.europa.eu, covering:
- Floods (1,477 datasets, 67.9%)
- Groundwater (699 datasets, 32.1%)

## Key Findings

- **0% operational readiness** across all three management tasks
- **100%** of datasets lack explicit license metadata
- **100%** lack machine-readable format declarations
- **48%** of curated dataset URLs are broken

## Citation

[Paper citation to be added after publication]

## License

[License to be specified]
