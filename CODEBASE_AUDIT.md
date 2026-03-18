# CODEBASE AUDIT REPORT
**EU Water Dataset Observatory — Old vs New Methodology**
**Generated:** 2026-03-18
**Status:** Exploratory Only — No Files Modified

---

## EXECUTIVE SUMMARY

The repository contains **two parallel, largely disconnected pipelines**:

| Pipeline | Status | Entry Point | Data Source | Output Dir |
|---|---|---|---|---|
| **OLD — Simulation-based** | Legacy | `run_full_analysis.py` | `data/simulated/` | `data/outputs/` |
| **NEW — SPARQL Harvest-based** | Target | `src/score_real_data.py` (partial) | `data/harvested/` | `data/outputs_real/` |

The new pipeline is **incomplete**: `harvest_sparql.py` and `score_real_data.py` exist and produce outputs, but there is **no orchestration script** for the new pipeline — `run_full_analysis.py` still calls the old simulation pipeline exclusively.

---

## SECTION 1: COMPLETE FILE INVENTORY WITH CLASSIFICATION

### Python Source Files (`src/`)

| File | Classification | Reason |
|---|---|---|
| `src/simulate_records.py` | 🔴 **OLD** | Generates 3,500 synthetic records using Monte Carlo sampling calibrated to simulation_params.json. Core of old methodology. Not called by anything in the new pipeline. |
| `src/harvest_sparql.py` | 🟢 **NEW** | Real SPARQL harvesting from `data.europa.eu/sparql`. Queries real EU open data portal. Writes to `data/harvested/`. No equivalent in old pipeline. |
| `src/score_real_data.py` | 🟢 **NEW** | Orchestrates scoring of harvested records. Calls `sufficiency_scoring`, `sensitivity_analysis`, `impact_proxy`. Reads from `data/harvested/`, writes to `data/outputs_real/`. |
| `src/sufficiency_scoring.py` | 🟡 **SHARED** | Core scoring engine. Called by both `run_full_analysis.py` (step 2, on simulated data) and `score_real_data.py` (on harvested data). Contains the 3-dimension scoring logic (EW, CR, CB). |
| `src/sensitivity_analysis.py` | 🟡 **SHARED** | Weight perturbation sensitivity analysis. Called by both pipelines. Reads a scored DataFrame, produces sensitivity outputs. |
| `src/impact_proxy.py` | 🟡 **SHARED** | Priority/impact scoring (download counts, freshness, recency). Called by both pipelines. |
| `src/validation_analysis.py` | 🟡 **SHARED** | Reads `validation_results.json` and produces validation statistics. Currently outputs to `data/outputs/validation_results.json` (old dir). Could serve new pipeline if path updated. |
| `src/__init__.py` | 🟡 **SHARED** | Empty package init. Needed regardless. |

### Root-Level Orchestration Scripts

| File | Classification | Reason |
|---|---|---|
| `run_full_analysis.py` | 🔴 **OLD (needs replacement)** | Hardwired to run old 5-step simulation pipeline: `simulate_records → sufficiency_scoring (on simulated data) → sensitivity_analysis → impact_proxy → create_visualizations`. All paths point to `data/simulated/` and `data/outputs/`. No awareness of harvested data. |

### Configuration Files (`config/`)

| File | Classification | Reason |
|---|---|---|
| `config/weights.json` | 🟡 **SHARED** | Scoring dimension weights (EW, CR, CB) and field weights. Used by `sufficiency_scoring.py` in both pipelines. Must be kept. |
| `config/keywords.json` | 🟡 **SHARED** | Water domain keyword lists. Used by `simulate_records.py` (keyword assignment) AND `harvest_sparql.py` (SPARQL query construction). Must be kept. |
| `config/simulation_params.json` | 🔴 **OLD** | Country distributions, quality profiles, temporal patterns for synthetic data generation. Only imported by `simulate_records.py`. Not used anywhere in new pipeline. |

### Data Directories

| Directory | Classification | Contents | Reason |
|---|---|---|---|
| `data/simulated/` | 🔴 **OLD** | `metadata_records.csv` (3,500 synthetic rows) | Output of `simulate_records.py`. Not used by any new pipeline component. |
| `data/harvested/` | 🟢 **NEW** | `raw_harvest.csv`, `harvest_summary.json` | Real records from SPARQL harvest. Input to `score_real_data.py`. Must be kept. |
| `data/outputs/` | 🔴 **OLD** | 7 CSV/JSON files from simulation scoring | Outputs of old `run_full_analysis.py` pipeline. Based on 3,500 synthetic records. |
| `data/outputs_real/` | 🟢 **NEW** | 6 CSV/JSON files from real data scoring | Outputs of `score_real_data.py`. Based on real SPARQL-harvested data. Must be kept. |

**`data/outputs/` file list (OLD — from simulation):**
- `analysis_summary.json` — 3,500 records, generated 2026-02-15
- `priority_fixes.csv`
- `priority_scores.csv`
- `sensitivity_detail.csv`
- `sensitivity_summary.csv`
- `sufficiency_scores.csv`
- `validation_results.json`

**`data/outputs_real/` file list (NEW — from SPARQL harvest):**
- `analysis_summary_real.json`
- `prepared_records.csv`
- `priority_fixes.csv`
- `priority_scores.csv`
- `sensitivity_detail.csv`
- `sensitivity_summary.csv`
- `sufficiency_scores.csv`

### Visualization Files (`visualizations/`)

| File | Classification | Reason |
|---|---|---|
| `visualizations/create_visualizations.py` | 🔴 **OLD (needs update)** | Reads from `data/outputs/` (old simulation outputs). Generates all HTML charts from simulated data. Needs to be pointed at `data/outputs_real/`. |
| `visualizations/country_task_heatmap.html` | 🔴 **OLD** | Generated from simulation data. 3,500-record simulation has all 28 EU countries. |
| `visualizations/domain_summary.html` | 🔴 **OLD** | Generated from simulation domain distribution. |
| `visualizations/failure_modes_chart.html` | 🔴 **OLD** | Based on simulated failure mode distribution. |
| `visualizations/priority_fixes.html` | 🔴 **OLD** | Based on simulated priority_fixes.csv. |
| `visualizations/sensitivity_analysis.html` | 🔴 **OLD** | Based on simulated sensitivity outputs. |
| `visualizations/validation_results.html` | 🔴 **OLD** | Based on simulated validation_results.json. |

### Documentation Files

| File | Classification | Reason |
|---|---|---|
| `README.md` | 🔴 **OLD (needs rewrite)** | Describes simulation methodology as primary. References `run_full_analysis.py` as the entry point for the old 5-step pipeline. Statistics cited (3,500 records, 48% URL breakage, 61.5% agreement) come from the simulation, not real harvest. |
| `METHODOLOGY_v3.md` | 🔴 **OLD / PROBLEMATIC** | Reviewer-response document for journal submission. AUDIT_REPORT.md identifies fabricated statistics within it (EuroVoc metrics, "89% agreement rate", "450-sample validation"). References simulation as real data. **High risk if published.** |
| `AUDIT_REPORT.md` | 🟢 **KEEP** | Documents the discrepancies and fabrications found in METHODOLOGY_v3.md. Critical institutional memory. |
| `VERIFICATION_REPORT.md` | 🟢 **KEEP** | Independently verified statistics from actual code execution. Trustworthy reference for real scores. |
| `validation_instructions.md` | 🟡 **SHARED** | Human expert validation protocol. Methodology-agnostic — describes how to fill in Validation_Checklist.xlsx. Keep. |
| `Validation_Checklist.xlsx` | 🟡 **SHARED** | Human expert validation workbook. Could be used with either pipeline's outputs. Keep. |

---

## SECTION 2: DEPENDENCY MAP

```
OLD PIPELINE (run_full_analysis.py)
=====================================
run_full_analysis.py
  ├── Step 1: src/simulate_records.py
  │     ├── reads:  config/simulation_params.json  [OLD]
  │     ├── reads:  config/keywords.json           [SHARED]
  │     └── writes: data/simulated/metadata_records.csv
  │
  ├── Step 2: src/sufficiency_scoring.py           [SHARED module]
  │     ├── reads:  data/simulated/metadata_records.csv
  │     ├── reads:  config/weights.json            [SHARED]
  │     └── writes: data/outputs/sufficiency_scores.csv
  │
  ├── Step 3: src/sensitivity_analysis.py          [SHARED module]
  │     ├── reads:  data/outputs/sufficiency_scores.csv
  │     ├── writes: data/outputs/sensitivity_detail.csv
  │     └── writes: data/outputs/sensitivity_summary.csv
  │
  ├── Step 4: src/impact_proxy.py                 [SHARED module]
  │     ├── reads:  data/outputs/sufficiency_scores.csv
  │     ├── writes: data/outputs/priority_scores.csv
  │     ├── writes: data/outputs/priority_fixes.csv
  │     └── writes: data/outputs/analysis_summary.json
  │
  └── Step 5: visualizations/create_visualizations.py  [OLD]
        ├── reads:  data/outputs/*.csv / *.json
        └── writes: visualizations/*.html


NEW PIPELINE (no orchestrator yet)
=====================================
src/harvest_sparql.py
  ├── reads:  config/keywords.json                [SHARED]
  ├── queries: data.europa.eu/sparql (live)
  ├── writes: data/harvested/raw_harvest.csv
  └── writes: data/harvested/harvest_summary.json

src/score_real_data.py
  ├── reads:  data/harvested/raw_harvest.csv
  ├── calls:  src/sufficiency_scoring.py          [SHARED module]
  │     └── reads: config/weights.json            [SHARED]
  ├── calls:  src/sensitivity_analysis.py         [SHARED module]
  ├── calls:  src/impact_proxy.py                 [SHARED module]
  ├── writes: data/outputs_real/prepared_records.csv
  ├── writes: data/outputs_real/sufficiency_scores.csv
  ├── writes: data/outputs_real/sensitivity_detail.csv
  ├── writes: data/outputs_real/sensitivity_summary.csv
  ├── writes: data/outputs_real/priority_scores.csv
  ├── writes: data/outputs_real/priority_fixes.csv
  └── writes: data/outputs_real/analysis_summary_real.json

src/validation_analysis.py
  ├── reads:  data/outputs/validation_results.json  [⚠️ currently points to OLD dir]
  └── writes: data/outputs/validation_results.json  [⚠️ currently points to OLD dir]

visualizations/create_visualizations.py
  ├── reads:  data/outputs/*.csv / *.json            [⚠️ reads OLD simulation outputs]
  └── writes: visualizations/*.html


NOTHING imports simulate_records.py except run_full_analysis.py (Step 1).
NOTHING in the new pipeline touches: simulate_records.py, simulation_params.json,
  data/simulated/, or data/outputs/.
```

---

## SECTION 3: RECOMMENDATION TABLE

### ✅ Files to KEEP (No Changes)

| File | Reason |
|---|---|
| `src/harvest_sparql.py` | Core of new methodology — real SPARQL harvesting |
| `src/score_real_data.py` | Core of new methodology — scores real records |
| `src/sufficiency_scoring.py` | Shared scoring engine used by new pipeline |
| `src/sensitivity_analysis.py` | Shared analysis module used by new pipeline |
| `src/impact_proxy.py` | Shared priority module used by new pipeline |
| `src/__init__.py` | Package init |
| `config/weights.json` | Scoring weights — used by new pipeline |
| `config/keywords.json` | Query keywords — used by harvest_sparql.py |
| `data/harvested/raw_harvest.csv` | Real SPARQL-harvested records |
| `data/harvested/harvest_summary.json` | Harvest metadata |
| `data/outputs_real/` (all 6 files) | Outputs from real data pipeline |
| `AUDIT_REPORT.md` | Critical record of what went wrong |
| `VERIFICATION_REPORT.md` | Verified ground-truth statistics |
| `validation_instructions.md` | Human validation protocol |
| `Validation_Checklist.xlsx` | Human validation workbook |
| `requirements.txt` | Python dependencies |

### 🔴 Files to REMOVE (Old Simulation Only)

| File | Risk Level | Reason |
|---|---|---|
| `src/simulate_records.py` | LOW | Only called by `run_full_analysis.py` (old). Zero imports from new pipeline. Safe to remove after run_full_analysis.py is replaced. |
| `config/simulation_params.json` | LOW | Only imported by `simulate_records.py`. If simulate_records.py is removed, this has no purpose. |
| `data/simulated/metadata_records.csv` | LOW | 3,500 synthetic rows. Not referenced by new pipeline. |
| `data/outputs/analysis_summary.json` | LOW | Generated from 3,500 simulated records. Superseded by `data/outputs_real/analysis_summary_real.json`. |
| `data/outputs/sufficiency_scores.csv` | LOW | Simulated. Superseded by `data/outputs_real/sufficiency_scores.csv`. |
| `data/outputs/sensitivity_detail.csv` | LOW | Simulated. Superseded by real equivalent. |
| `data/outputs/sensitivity_summary.csv` | LOW | Simulated. Superseded by real equivalent. |
| `data/outputs/priority_scores.csv` | LOW | Simulated. Superseded by real equivalent. |
| `data/outputs/priority_fixes.csv` | LOW | Simulated. Superseded by real equivalent. |
| `data/outputs/validation_results.json` | MEDIUM | ⚠️ `validation_analysis.py` still writes here. Remove only AFTER validation_analysis.py paths are updated. |
| `visualizations/country_task_heatmap.html` | LOW | Generated from simulation. Will be regenerated from real data once visualizations are updated. |
| `visualizations/domain_summary.html` | LOW | Generated from simulation. |
| `visualizations/failure_modes_chart.html` | LOW | Generated from simulation. |
| `visualizations/priority_fixes.html` | LOW | Generated from simulation. |
| `visualizations/sensitivity_analysis.html` | LOW | Generated from simulation. |
| `visualizations/validation_results.html` | LOW | Generated from simulation. |

### 🔧 Files to UPDATE (Require Modification)

| File | What Needs Changing | Priority |
|---|---|---|
| `run_full_analysis.py` | **Entire pipeline needs replacement.** Currently: simulate → score simulated → visualize simulated. Needs to become: harvest_sparql → score_real_data → visualize real. | 🔴 HIGH |
| `visualizations/create_visualizations.py` | Change input paths from `data/outputs/` to `data/outputs_real/`. Also check column name compatibility between simulated and real score CSVs. | 🔴 HIGH |
| `src/validation_analysis.py` | Change hardcoded paths from `data/outputs/validation_results.json` to `data/outputs_real/` equivalent. Confirm validation schema matches real data. | 🟡 MEDIUM |
| `README.md` | Full rewrite. Remove simulation methodology as primary. Document SPARQL-first pipeline. Update statistics to cite real harvest results, not simulation results. Remove references to `run_full_analysis.py` old steps. | 🔴 HIGH |
| `METHODOLOGY_v3.md` | **Do NOT use for publication in current form.** Contains fabricated statistics. Either rewrite from scratch using only verified real-data findings from VERIFICATION_REPORT.md, or retire entirely. | 🔴 CRITICAL |

### 📁 Directories to REMOVE (when contents are removed)

| Directory | When Safe to Remove |
|---|---|
| `data/simulated/` | After `metadata_records.csv` is removed |
| `data/outputs/` | After `validation_analysis.py` is updated to write to `data/outputs_real/` |

### 📁 Directories to KEEP

| Directory | Reason |
|---|---|
| `data/harvested/` | Real SPARQL harvest data — core of new methodology |
| `data/outputs_real/` | Real pipeline outputs |
| `config/` | Keep `weights.json` and `keywords.json`; remove `simulation_params.json` |
| `src/` | Keep all except `simulate_records.py` |
| `visualizations/` | Keep after updating `create_visualizations.py` and regenerating HTML from real data |

---

## SECTION 4: RISK ASSESSMENT

### 🔴 HIGH RISK

| Risk | Description | Mitigation |
|---|---|---|
| `METHODOLOGY_v3.md` published | Contains statistics that AUDIT_REPORT.md identifies as fabricated or unverifiable (EuroVoc metrics, "89% agreement", "450-sample validation"). If submitted/published, this creates a scientific integrity problem. | Retire or fully rewrite using only VERIFICATION_REPORT.md verified figures. |
| `run_full_analysis.py` confusion | Still the only orchestration script. Running it produces simulation outputs. Someone unfamiliar with the codebase could mistake simulated outputs for real results. | Replace with new orchestrator ASAP. |
| `README.md` misleading | Documents old simulation methodology as if it is the real methodology. | Rewrite before any public sharing. |

### 🟡 MEDIUM RISK

| Risk | Description | Mitigation |
|---|---|---|
| `validation_analysis.py` path mismatch | Reads/writes `data/outputs/` (old dir). If `data/outputs/` is cleaned before this is updated, it will break. | Update paths to `data/outputs_real/` before removing `data/outputs/`. |
| `create_visualizations.py` column mismatch | Simulated data and harvested data may have different column names or value ranges. Simply redirecting paths may not be enough; code may need adaptation. | Test carefully after path changes. |
| No harvest orchestrator | `score_real_data.py` is a script, not fully integrated into a pipeline. The step sequence (harvest → score → visualize) is not documented or automated. | Create a new `run_real_pipeline.py` orchestrator. |

### 🟢 LOW RISK (Safe to Remove)

| File/Dir | Why Safe |
|---|---|
| `data/simulated/metadata_records.csv` | Purely synthetic. Zero scientific value after migration. Not referenced by new pipeline. |
| `config/simulation_params.json` | Only consumed by `simulate_records.py`. Removing both together is safe. |
| All 6 HTML files in `visualizations/` | Will be regenerated from real data once `create_visualizations.py` is updated. |
| `data/outputs/` CSV files | All superseded by `data/outputs_real/` equivalents. |

---

## SECTION 5: KEY STATISTICS COMPARISON

| Metric | OLD (Simulated, 3,500 records) | NEW (Real SPARQL harvest) |
|---|---|---|
| Record count | 3,500 (synthetic) | Actual harvested count |
| EW mean score | 0.440 | See `data/outputs_real/analysis_summary_real.json` |
| CR mean score | 0.451 | See `data/outputs_real/analysis_summary_real.json` |
| CB mean score | 0.466 | See `data/outputs_real/analysis_summary_real.json` |
| EW ready % | 3.8% | See `data/outputs_real/analysis_summary_real.json` |
| Data source | `config/simulation_params.json` (fabricated distributions) | `data.europa.eu/sparql` (live EU portal) |
| Generated by | `simulate_records.py` | `harvest_sparql.py` |
| Outputs in | `data/outputs/` | `data/outputs_real/` |

---

## SECTION 6: RECOMMENDED CLEANUP SEQUENCE (When Ready)

When ready to execute cleanup (after this audit is reviewed), the recommended **safe order** is:

```
Step 1: Update src/validation_analysis.py paths (data/outputs/ → data/outputs_real/)
Step 2: Update visualizations/create_visualizations.py paths (data/outputs/ → data/outputs_real/)
Step 3: Create new run_real_pipeline.py orchestration script
Step 4: Test new pipeline end-to-end
Step 5: Regenerate all HTML visualizations from real data
Step 6: Remove data/simulated/ directory
Step 7: Remove data/outputs/ directory (now fully superseded)
Step 8: Remove src/simulate_records.py
Step 9: Remove config/simulation_params.json
Step 10: Rewrite README.md
Step 11: Retire or rewrite METHODOLOGY_v3.md
```

**Do NOT skip steps or reorder — specifically, do NOT remove data/outputs/ before Step 1-2 are done.**

---

*This audit was performed without modifying any files. All findings are based on static analysis of the codebase as of 2026-03-18.*
