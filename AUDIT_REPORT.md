# EU Water Dataset Observatory — Comprehensive Audit Report

**Audit Date:** 2026-03-09  
**Auditor:** Independent Code & Data Review  
**Scope:** Full implementation audit — code, data, outputs, and paper claims

---

## EXECUTIVE SUMMARY

This repository implements a **methodology paper framework** for EU water dataset metadata assessment. The core pipeline works correctly and runs end-to-end without errors. However, there are **serious discrepancies** between what the `METHODOLOGY_v3.md` claims and what the code actually produces, including:

- **Sufficiency score values in the methodology doc are ~0.10 lower** than what the code computes
- **"89% agreement rate" does not exist anywhere** in the codebase or data
- **"450-sample human validation" does not exist** — only 25 datasets were validated
- **`harvest_sparql.py` does not exist** despite being referenced as "ready"
- **EuroVoc precision/recall metrics are fabricated** — no supporting code or data exists
- **METHODOLOGY_v3.md validation table still shows dashes** — never updated with real results

The "brutally honest" bottom line: **the code works as a simulation framework, but the methodology document makes claims that significantly overstate or misrepresent what has actually been implemented and measured.**

---

## TASK 1: PROJECT STRUCTURE INVENTORY

### Root Level Files

| File | Size | Modified | Purpose |
|------|------|----------|---------|
| `README.md` | 5.3 KB | Feb 15 21:39 | Project documentation |
| `METHODOLOGY_v3.md` | 14.1 KB | Feb 15 20:09 | Paper methodology (reviewer response) |
| `VERIFICATION_REPORT.md` | 11.2 KB | Feb 15 21:52 | Self-assessment report |
| `run_full_analysis.py` | 1.9 KB | Feb 15 20:45 | Pipeline orchestrator |
| `requirements.txt` | 28 B | Feb 15 20:45 | Only lists: `pandas`, `plotly` |
| `Validation_Checklist.xlsx` | 15.2 KB | Feb 15 20:09 | 25-dataset manual validation |
| `validation_instructions.md` | 1.5 KB | Feb 15 20:09 | Validation protocol |

### `/config/` Files

| File | Size | Modified | Purpose |
|------|------|----------|---------|
| `weights.json` | 739 B | Feb 15 20:13 | Task-specific scoring weights |
| `simulation_params.json` | 2.6 KB | Feb 15 20:13 | Data generation parameters |
| `keywords.json` | 2.7 KB | Feb 15 20:13 | Domain keywords (5 languages) |

### `/src/` Files

| File | Size | Modified | Purpose |
|------|------|----------|---------|
| `simulate_records.py` | 10.1 KB | Feb 15 20:27 | Generate synthetic records |
| `sufficiency_scoring.py` | 8.7 KB | Feb 15 20:46 | Task-specific scoring engine |
| `sensitivity_analysis.py` | 5.0 KB | Feb 15 20:33 | Weight perturbation analysis |
| `impact_proxy.py` | 4.7 KB | Feb 15 20:33 | Impact/priority calculation |
| `validation_analysis.py` | 7.4 KB | Feb 15 21:36 | Excel-based validation |
| `__init__.py` | 84 B | Feb 15 20:27 | Package init |

### `/data/` Files

| File | Size | Modified | Notes |
|------|------|----------|-------|
| `simulated/metadata_records.csv` | 991 KB | Feb 15 20:47 | 3,500 synthetic records |
| `outputs/sufficiency_scores.csv` | 1.5 MB | Feb 15 20:47 | 3,500 records × 50 columns |
| `outputs/priority_scores.csv` | 1.7 MB | Feb 15 20:47 | 3,500 records + impact scores |
| `outputs/sensitivity_detail.csv` | 906 KB | Feb 15 20:47 | 10,500 scenario rows |
| `outputs/sensitivity_summary.csv` | 596 B | Feb 15 20:47 | 3-row summary table |
| `outputs/priority_fixes.csv` | 31.3 KB | Feb 15 20:47 | Top 60 priority items |
| `outputs/analysis_summary.json` | 6.2 KB | Feb 15 21:39 | Full statistical summary |
| `outputs/validation_results.json` | 2.2 KB | Feb 15 21:46 | Real validation results |

### `/visualizations/` Files

| File | Size | Modified | Notes |
|------|------|----------|-------|
| `create_visualizations.py` | 14.5 KB | Feb 15 20:45 | Generates all HTML charts |
| `country_task_heatmap.html` | 3.1 KB | Feb 15 20:47 | Country × Task matrix |
| `domain_summary.html` | 1.8 KB | Feb 15 20:47 | Domain statistics |
| `failure_modes_chart.html` | 1.2 KB | Feb 15 20:47 | Metadata gaps |
| `priority_fixes.html` | 6.8 KB | Feb 15 20:47 | Top 20 fixes |
| `sensitivity_analysis.html` | 1.9 KB | Feb 15 20:47 | Sensitivity results |
| `validation_results.html` | 11.5 KB | Feb 15 21:38 | Validation dashboard |

### Missing Files Referenced in METHODOLOGY_v3.md

| File Referenced | Exists? | Impact |
|-----------------|---------|--------|
| `harvest_sparql.py` | ❌ NO | Entire harvesting claim is unfounded |
| `full_analysis_results.csv` | ❌ NO | Referenced as "raw analysis results" |
| `full_analysis_framework.py` | ❌ NO | Referenced in reproduction instructions |
| `src/eurovoc_mapping.py` | ❌ NO | EuroVoc metrics have no code support |
| `visualizations/danube_case_study.html` | ❌ NO | Listed in deliverables |
| `visualizations/staleness_analysis.html` | ❌ NO | Listed in deliverables |

---

## TASK 2: CODE FUNCTIONALITY AUDIT

### `src/simulate_records.py`
**What it actually does:** Generates 3,500 **completely synthetic** metadata records using Python's `random` module.

- **Data source:** 100% internally generated — no external APIs, no SPARQL, no real portals
- **Dataset IDs:** UUID-based (`eu-water-6178ae403437` format) — not real EU portal identifiers
- **Titles/descriptions:** Template strings with random keyword injection
- **Metadata fields:** Randomly assigned based on probability tables in `simulation_params.json`
- **"Calibrated to EU characteristics":** Parameters are asserted as calibrated but there is no code that fetches or validates against actual data.europa.eu statistics
- **Input:** `config/simulation_params.json`, `config/keywords.json`
- **Output:** `data/simulated/metadata_records.csv` (3,500 rows × 23 columns)

> **Verdict: Generates FAKE data. Correctly described as simulated in README but not in METHODOLOGY_v3.md's reviewer-response framing.**

---

### `src/sufficiency_scoring.py`
**What it actually does:** Computes deterministic weighted scores for the 3,500 simulated records.

- Implements 11 scoring functions for metadata dimensions (temporal recency, format, license, etc.)
- Applies task-specific weight vectors from `config/weights.json`
- Classifies records as Ready/Partial/Insufficient using thresholds: `≥0.7 = Ready`, `≥0.4 = Partial`, `<0.4 = Insufficient`
- **Real math, real computation — but applied to simulated inputs**
- **Input:** `data/simulated/metadata_records.csv`, `config/weights.json`
- **Output:** `data/outputs/sufficiency_scores.csv` (3,500 rows × 50 columns)

> **Verdict: Framework is REAL and correctly implemented. Results reflect the simulation parameters, not real EU data.**

---

### `src/sensitivity_analysis.py`
**What it actually does:** Perturbs the highest-weighted metric by ±25% and recomputes readiness percentages.

- Only perturbs the PRIMARY (highest) weight per task — not all weights
- Renormalizes the perturbed weight set so they still sum to 1.0
- Runs three scenarios (baseline, low, high) per task across all 3,500 records
- **Input:** `data/outputs/sufficiency_scores.csv`
- **Output:** `data/outputs/sensitivity_summary.csv`, `data/outputs/sensitivity_detail.csv`

> **Verdict: Methodologically sound but applied to simulated data. The note in METHODOLOGY_v3.md claims ±20% perturbation but the code uses ±25%.**

---

### `src/impact_proxy.py`
**What it actually does:** Computes a 5-component weighted impact score and a priority score.

- Components: publisher_type (0.30), operational_keywords (0.25), format_accessibility (0.20), temporal_extent (0.15), spatial_scope (0.10)
- Priority = Impact × (1 - Sufficiency) — high impact + low sufficiency = high priority
- Operational keywords check looks for "real-time", "forecast", "early warning" in the simulated title/description text — but these terms appear rarely in template-generated text
- **Input:** `data/outputs/sufficiency_scores.csv`
- **Output:** `data/outputs/priority_scores.csv`, `data/outputs/priority_fixes.csv`

> **Verdict: Formula is REAL. Input data is SIMULATED. Results are mathematically valid but not empirically grounded.**

---

### `src/validation_analysis.py`
**What it actually does:** Reads the REAL `Validation_Checklist.xlsx` and computes agreement statistics.

- **This is the ONLY script that reads real-world data**
- Correctly computes agreement rates using `(Yes + 0.5×Partial) / Total`
- Produces `data/outputs/validation_results.json` with correct numbers
- **Note:** This script is NOT called by `run_full_analysis.py` — it must be run separately
- **Input:** `Validation_Checklist.xlsx` (25 real datasets)
- **Output:** `data/outputs/validation_results.json`

> **Verdict: REAL data, REAL analysis, CORRECT results. The key evidence-based component of the project.**

---

### `run_full_analysis.py`
**What it actually does:** Runs steps 1-5: simulate → score → sensitivity → impact → visualize.

- **Does NOT call `validation_analysis.py`** — the validation step is excluded from the automated pipeline
- **Does NOT reference any SPARQL harvesting** — the pipeline is simulation-only
- Successfully executes all 5 called modules without errors

---

## TASK 3: DATA REALITY CHECK

### `Validation_Checklist.xlsx` — Full Row-by-Row Count

**Sheet: "Validation Checklist" | 25 data rows + 1 header | 2 sheets total**

| # | Domain | Dataset | Publisher | URL Works | Notes |
|---|--------|---------|-----------|-----------|-------|
| 1 | Floods | River Flood Hazard Maps | JRC | ❌ No | page not found |
| 2 | Floods | EFAS Historical Simulations | Copernicus/JRC | ❌ No | page not found |
| 3 | Floods | Copernicus EMS Rapid Mapping | Copernicus EMS | ✅ Yes | free resource |
| 4 | Floods | European Past Floods Database | EEA | ❌ No | **page retired** |
| 5 | Floods | Floods Directive Reported Data | EEA | ❌ No | **page retired** |
| 6 | Water Quality | Waterbase - Water Quality ICM | EEA | ✅ Yes | free resource |
| 7 | Water Quality | Waterbase - Biology | EEA | ❌ No | page not found |
| 8 | Water Quality | Bathing Water Quality | EEA | ✅ Yes | free resource |
| 9 | Water Quality | Waterbase - Transitional/Coastal | EEA | ❌ No | page not found |
| 10 | Water Quality | Nutrients in Freshwater | EEA | ✅ Yes | free resource |
| 11 | WFD Metrics | WISE WFD Database | EEA | ✅ Yes | free resource |
| 12 | WFD Metrics | WISE Spatial Data (Water Bodies) | EEA | ❌ No | **page retired** |
| 13 | WFD Metrics | UWWTD Reported Data | EEA | ✅ Yes | free resource |
| 14 | WFD Metrics | Ecological Status Dashboard | EEA | ✅ Yes | free resource |
| 15 | WFD Metrics | Chemical Status of Water Bodies | EEA | ❌ No | page not found |
| 16 | Groundwater | Groundwater Bodies (WISE) | EEA | ✅ Yes | free resource |
| 17 | Groundwater | Nitrates in Groundwater | EEA | ❌ No | page not found |
| 18 | Groundwater | Groundwater Quantitative Status | EEA | ✅ Yes | looks blank, outdated data |
| 19 | Groundwater | Hydrogeological Map of Europe | JRC/ESDAC | ❌ No | page not found |
| 20 | Groundwater | Groundwater Dependent Ecosystems | EEA | ✅ Yes | **same URL as row 16** |
| 21 | Utilities | Eurostat Water Statistics | Eurostat | ❌ No | page not found |
| 22 | Utilities | SDG 6 Indicators | Eurostat | ❌ No | page not found |
| 23 | Utilities | Water Exploitation Index | EEA | ✅ Yes | free resource |
| 24 | Utilities | Population Connected to WWTP | Eurostat | ✅ Yes | missing values for small countries |
| 25 | Utilities | Water Abstraction by Source | Eurostat | ✅ Yes | missing values for small countries |

**URL Summary:**
- ✅ Working: **13** (52%)
- ❌ Broken/Retired: **12** (48%)
- By domain: Floods 1/5 (20%), Water Quality 3/5 (60%), WFD 3/5 (60%), Groundwater 3/5 (60%), Utilities 3/5 (60%)

**Special Notes from the Data:**
- Row 20 (Groundwater Dependent Ecosystems) uses the **same URL** as Row 16 — this is not truly an independent dataset
- Rows 4, 5, 12 were **explicitly retired** pages, not just broken
- All "working" URL entries are **free resources** from EEA, Copernicus, or Eurostat
- **License stated: 0 out of 25** — every single dataset, including all working ones, had no explicit license statement on their portal page

### Metadata Accuracy Counts (from Excel — verified by independent computation):

**All 25 Datasets:**

| Category | Yes | No | Partial | Agreement Rate |
|----------|-----|----|---------|----------------|
| Description Accurate | 12 | 13 | 0 | 48.0% |
| Temporal Accurate | 10 | 13 | 2 | 44.0% |
| Format Accurate | 5 | 12 | 8 | 36.0% |
| License Stated | 0 | 25 | 0 | 0.0% |
| **OVERALL** | | | | **32.0%** |

**Working URLs Only (13 Datasets):**

| Category | Yes | No | Partial | Agreement Rate |
|----------|-----|----|---------|----------------|
| Description Accurate | 12 | 1 | 0 | 92.3% |
| Temporal Accurate | 10 | 1 | 2 | 84.6% |
| Format Accurate | 5 | 0 | 8 | 69.2% |
| License Stated | 0 | 13 | 0 | 0.0% |
| **OVERALL** | | | | **61.5%** |

### Is the CSV Data Real or Simulated?

**`data/simulated/metadata_records.csv`:** 100% SYNTHETIC. Dataset IDs are `eu-water-{uuid}` — not real portal identifiers. All 3,500 records were generated by `simulate_records.py` using random number generation.

**`data/outputs/*.csv`:** All computed from the simulated CSV. No real portal data anywhere in the outputs folder except indirectly via `validation_results.json`.

---

## TASK 4: CONFIGURATION AUDIT

### `config/weights.json` — Are These the Paper Weights?

The weights ARE used by the scoring code and ARE internally consistent. Whether they match "the paper" depends on which paper version you consult:

| Task | Primary Weight | Value | Consistent with code? |
|------|---------------|-------|----------------------|
| early_warning → temporal_recency | highest | 0.25 | ✅ Yes |
| compliance_reporting → temporal_coverage | highest | 0.25 | ✅ Yes |
| cross_border → vocabulary_standard | highest | 0.25 | ✅ Yes |

All weights sum to 1.0 per task. ✅

**However:** METHODOLOGY_v3.md lists `cross_border → multilingual` as weight 0.25 (same as vocabulary_standard), but `weights.json` sets multilingual=0.20 and vocabulary_standard=0.25. Minor inconsistency between methodology text and config file.

### `config/keywords.json` — Are These Used for SPARQL?

**NO.** The README and METHODOLOGY_v3.md strongly imply keywords.json drives SPARQL queries:
> *"The `harvest_sparql.py` script is ready for execution... [uses] domain-specific keywords"*

In reality, `keywords.json` is **only used inside `generate_description()`** in `simulate_records.py` to pick random words for fake description text. It is not used in any SPARQL query (no SPARQL code exists at all).

### `config/simulation_params.json` — What Drives the Simulation?

This is the single most important configuration file. It defines:
- Country weights (e.g., DE=12%, FR=11%, IT=10%)
- Publisher type distribution (national_hydro=25%, eu_agency=20%, ministry=20%, regional=20%)
- Completeness profiles (e.g., eu_agency has 95% description probability, regional has 70%)
- Format distribution (CSV=30%, PDF=20%, Shapefile=15%)
- Update frequency (annual=35%, irregular=20%, monthly=20%)

These numbers are **asserted** as calibrated to EU data characteristics. There is no validation code or reference data to confirm this calibration.

---

## TASK 5: OUTPUT VERIFICATION

### Visualizations

All 6 HTML files exist and were generated by `create_visualizations.py`. They are:
- **Self-contained Plotly charts** embedded in HTML
- **Sourced from simulated data** (except `validation_results.html` which uses real validation data)
- Small file sizes (1.2–11.5 KB) indicate they contain embedded data summaries, not raw CSVs

### `analysis_summary.json` — Verified Values

The following values from `analysis_summary.json` were **independently verified** against raw CSV computation:

| Statistic | In JSON | Verified | Match? |
|-----------|---------|----------|--------|
| Total records | 3500 | 3500 | ✅ |
| EW mean score | 0.4401 | 0.4401 | ✅ |
| CR mean score | 0.4513 | 0.4513 | ✅ |
| CB mean score | 0.4657 | 0.4657 | ✅ |
| EW Ready % | 3.8% | 3.8% (133/3500) | ✅ |
| CR Ready % | 5.46% | 5.46% (191/3500) | ✅ |
| CB Ready % | 9.89% | 9.89% (346/3500) | ✅ |
| Working URLs | 13 | 13 | ✅ |
| Broken URLs | 12 | 12 | ✅ |
| Overall agreement (working) | 0.6154 | 0.615 | ✅ |

**The analysis_summary.json values accurately reflect what the code computes.** The problem is with what METHODOLOGY_v3.md claims, not with the JSON.

---

## TASK 6: DISCREPANCY REPORT

### Full Discrepancy Table

| Metric/Claim | Where Claimed | Actual Computed Value | Source | Status |
|---|---|---|---|---|
| **48% URL breakage rate** | README.md | 12/25 = **48.0%** | Validation_Checklist.xlsx | ✅ VERIFIED |
| **52% URL accessibility** | README, analysis_summary | 13/25 = **52.0%** | Excel, code | ✅ VERIFIED |
| **61.5% agreement (working URLs)** | README.md | **(Yes+0.5×Partial)/Total = 61.5%** | Excel computation | ✅ VERIFIED |
| **Description accuracy 92.3%** | README.md | 12/13 = **92.3%** | Excel | ✅ VERIFIED |
| **Temporal accuracy 84.6%** | README.md | (10+1)/13 = **84.6%** | Excel | ✅ VERIFIED |
| **Format accuracy 69.2%** | README.md | (5+4)/13 = **69.2%** | Excel | ✅ VERIFIED |
| **License metadata 0%** | README.md | 0/25 = **0%** | Excel | ✅ VERIFIED |
| **3,500 records** | All docs | CSV = **3,500 rows** | metadata_records.csv | ✅ VERIFIED |
| **7 domains, 28 countries** | README | ✅ **Confirmed** | analysis_summary.json | ✅ VERIFIED |
| **"89% agreement rate"** | Task description | **NOT FOUND ANYWHERE** in repo | grep of all files | ❌ DOES NOT EXIST |
| **EW sufficiency 0.342** | METHODOLOGY_v3.md §2.4 | **0.440** | sufficiency_scores.csv | ❌ WRONG by +0.10 |
| **CR sufficiency 0.418** | METHODOLOGY_v3.md §2.4 | **0.451** | sufficiency_scores.csv | ❌ WRONG by +0.03 |
| **CB sufficiency 0.387** | METHODOLOGY_v3.md §2.4 | **0.466** | sufficiency_scores.csv | ❌ WRONG by +0.08 |
| **Scores range 0.34-0.42** | Task description | **0.44–0.47** | analysis_summary.json | ❌ WRONG (task uses methodology doc values) |
| **EW Ready 1.7%** | METHODOLOGY_v3.md §2.3 | **3.8%** | sensitivity_summary.csv | ❌ WRONG (2.2× higher) |
| **CR Ready 4.9%** | METHODOLOGY_v3.md §2.3 | **5.5%** | sensitivity_summary.csv | ⚠️ CLOSE (0.6pp difference) |
| **CB Ready 4.0%** | METHODOLOGY_v3.md §2.3 | **9.9%** | sensitivity_summary.csv | ❌ WRONG (2.5× higher) |
| **EW sensitivity range 0.3pp** | METHODOLOGY_v3.md §2.3 | **-0.49pp** | sensitivity_summary.csv | ❌ Direction differs |
| **CR sensitivity range 1.7pp** | METHODOLOGY_v3.md §2.3 | **2.63pp** | sensitivity_summary.csv | ❌ WRONG |
| **450-sample human validation** | METHODOLOGY_v3.md §2.4, §7 | **25 datasets only** | Validation_Checklist.xlsx | ❌ FALSE CLAIM |
| **EuroVoc Precision 0.88** | METHODOLOGY_v3.md §5.3 | **No code/data exists** | No eurovoc_mapping.py | ❌ FABRICATED |
| **EuroVoc Recall 0.72** | METHODOLOGY_v3.md §5.3 | **No code/data exists** | No eurovoc_mapping.py | ❌ FABRICATED |
| **EuroVoc F1 0.79** | METHODOLOGY_v3.md §5.3 | **No code/data exists** | No eurovoc_mapping.py | ❌ FABRICATED |
| **harvest_sparql.py "ready"** | METHODOLOGY_v3.md §1.2 | **File does not exist** | Directory listing | ❌ FALSE CLAIM |
| **15,000-25,000 harvest** | METHODOLOGY_v3.md §1.2 | **No harvest code** | Directory listing | ❌ NOT IMPLEMENTED |
| **Table 6 validation (from Excel)** | Table in methodology | **Validation table shows dashes** | METHODOLOGY_v3.md §4.5 | ❌ NEVER FILLED IN |
| **Partial threshold ≥0.5** | VERIFICATION_REPORT.md | **Code uses ≥0.4** | classify_readiness() | ❌ DOC ERROR |
| **Sensitivity uses ±20%** | METHODOLOGY_v3.md | **Code uses ±25%** | sensitivity_analysis.py | ❌ DOC/CODE MISMATCH |
| **Validation in pipeline** | Implied by methodology | **Not in run_full_analysis.py** | run_full_analysis.py | ⚠️ OMISSION |

---

## TASK 7: BRUTALLY HONEST ASSESSMENT

### ✅ WHAT IS REAL AND VERIFIED

1. **48% URL breakage rate** — Independently verified from 25 real EU dataset URLs. This is the most significant real finding in the project.

2. **61.5% overall metadata agreement (working URLs only)** — Computed from real Excel data, mathematically correct.

3. **0% license metadata** — Every single one of the 25 checked datasets had no explicit license statement. Verified from Excel.

4. **The scoring framework** — `sufficiency_scoring.py`, `sensitivity_analysis.py`, and `impact_proxy.py` implement real, mathematically sound methodologies. They work correctly on whatever input data is provided.

5. **The pipeline runs end-to-end** — `run_full_analysis.py` executes all 5 steps without errors.

6. **analysis_summary.json is internally consistent** — All values in this file match what the code actually computes.

7. **3,500 synthetic records exist** and have the described properties (7 domains, 28 countries, correct column structure).

8. **Sensitivity analysis confirms robustness** — All tasks show <3pp range under ±25% weight perturbation. This conclusion is valid.

---

### ⚠️ WHAT IS SIMULATED BUT CORRECTLY (OR MOSTLY) DESCRIBED AS SUCH

1. **3,500 metadata records** — The README, METHODOLOGY_v3.md §1.1, and analysis_summary.json all acknowledge these are simulated. Correctly framed in most places.

2. **Country/domain distributions** — Labeled as "calibrated to known EU characteristics." Stated with appropriate caveats. Cannot be independently verified but is transparent about being a simulation.

3. **Task-specific scores (EW 0.440, CR 0.451, CB 0.466)** — Correctly presented in `analysis_summary.json` and `VERIFICATION_REPORT.md` as outputs of the simulation. The problem is METHODOLOGY_v3.md shows different (lower) numbers.

4. **Publisher-type completeness profiles** — Asserted from literature but used correctly within the simulation framework.

---

### ❌ WHAT IS CLAIMED BUT NOT SUPPORTED BY CODE OR DATA

#### Critical Fabrications:

**1. "89% Agreement Rate"**
This number appears **nowhere** in the repository. Not in any Python file, JSON output, HTML visualization, or Markdown document. `grep -rn "89%"` returns no results. The actual overall agreement rate for working URLs is 61.5%. If this number appeared in a paper draft, it has no computational basis in this project.

**2. EuroVoc Precision 0.88 / Recall 0.72 / F1 0.79**
METHODOLOGY_v3.md §5.3 presents these metrics from a "200-sample manual validation." There is:
- No `src/eurovoc_mapping.py`
- No 200-sample validation dataset
- No EuroVoc mapping code of any kind
- The `has_eurovoc` field in simulation is just a random boolean
These numbers are completely fabricated. No code or data supports them.

**3. 450-Sample Human Validation**
METHODOLOGY_v3.md §2.4 states "validated against 450-sample human ground truth" and §7 references "450-sample human validation protocol." The only human validation that exists is the 25-dataset Excel file. That is 1/18th of the claimed sample size.

**4. `harvest_sparql.py` "Ready for Execution"**
METHODOLOGY_v3.md §1.2 states: *"The `harvest_sparql.py` script is ready for execution on infrastructure with data.europa.eu access."* This file does not exist in the repository. The SPARQL query shown in the methodology is illustrative pseudocode, not a working script.

**5. Sufficiency Score Values in METHODOLOGY_v3.md**
The methodology document presents these as paper-ready results:
```
Early Warning Mean Sufficiency: 0.342 ± 0.012 (95% CI: 0.330-0.354)
Compliance Mean Sufficiency:    0.418 ± 0.011 (95% CI: 0.407-0.429)
Cross-Border Mean Sufficiency:  0.387 ± 0.010 (95% CI: 0.377-0.397)
```
The code produces: 0.440, 0.451, 0.466. **The methodology numbers are systematically ~0.10 lower.** This suggests the methodology was written with a different version of the simulation parameters or different scoring functions. There is no version of the code that produces the methodology's values.

**6. Sensitivity Table Values in METHODOLOGY_v3.md**
The claimed "Early Warning: 1.7% ready" vs. actual 3.8%; "Cross-Border: 4.0% ready" vs. actual 9.9%. These cannot come from the current code under any parameter setting.

**7. METHODOLOGY_v3.md Validation Table (§4.5)**
The table that should report validation results still shows dashes with the note "[To be completed after manual validation - expected ~2 hours]". The validation HAS been completed — the Excel file exists with real data — but the methodology document was never updated. A reviewer receiving METHODOLOGY_v3.md would see placeholder dashes where actual results exist.

---

### ❌ DISCREPANCIES BETWEEN IMPLEMENTATION AND PAPER

| Category | METHODOLOGY_v3.md Claims | Implementation Reality |
|----------|--------------------------|------------------------|
| Data source | "SPARQL harvest ready; 15K-25K datasets expected" | 3,500 synthetic records; no SPARQL code |
| Sufficiency range | "0.34–0.42 across tasks" | 0.44–0.47 across tasks |
| Ready % | EW=1.7%, CR=4.9%, CB=4.0% | EW=3.8%, CR=5.5%, CB=9.9% |
| Validation scale | "450-sample human ground truth" | 25-dataset Excel file |
| Agreement rate | Table shows "—%" (unfilled) | 61.5% (computed, correct) |
| EuroVoc quality | P=0.88, R=0.72, F1=0.79 | No EuroVoc code exists |
| Sensitivity perturbation | ±20% stated | ±25% in code |
| Partial threshold | "0.5–0.7" (VERIFICATION_REPORT) | ≥0.4 (code) |
| keywords.json use | SPARQL filtering | Only description text generation |
| Confidence intervals | Reported to 3 decimal places | Not computed by any script |

---

## SUMMARY FINDINGS

### The Project Is: A simulation framework dressed up as an empirical study

**What actually works:**
- A clean Python pipeline that generates synthetic metadata, scores it, tests sensitivity, and visualizes results
- A real 25-dataset manual validation that correctly finds 48% URL breakage and 0% license disclosure
- Sound mathematical implementations of the scoring functions

**What is misleading:**
- METHODOLOGY_v3.md presents itself as a response to peer reviewer concerns about an empirical study, but the core data is synthetic
- Multiple specific metrics in the methodology document do not match what the code computes
- Claims of "ready-to-run SPARQL harvester," "450-sample validation," and "EuroVoc precision/recall" have no implementation basis whatsoever
- The confidence intervals in the methodology document (e.g., 0.330-0.354) cannot be computed by any script in the repo

**Recommended actions before any publication:**
1. Remove or clearly label the non-existent `harvest_sparql.py` claim
2. Correct all sufficiency score values in METHODOLOGY_v3.md to match code output
3. Remove EuroVoc precision/recall claims entirely or implement the code
4. Change "450-sample" to "25-sample" throughout
5. Fill in the §4.5 validation table with actual results
6. Reconcile "89% agreement rate" — either identify its source or remove it
7. Correct "±20% perturbation" to "±25% perturbation" throughout

---

*Report generated from: full source code review, manual Excel audit, independent numerical verification, and grep-based claim checking across all repository files.*
