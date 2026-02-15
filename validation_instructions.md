
# EU Water Dataset Observatory - Manual Validation Instructions

## Purpose
Verify a stratified sample of 25 datasets to establish ground-truth accuracy 
for automated metadata extraction.

## Time Required
Approximately 2 hours (5 minutes per dataset)

## Procedure

For each dataset in the checklist:

1. **Open the portal URL** in your browser

2. **Verify Description Accuracy**
   - Does the portal description match what we would extract via SPARQL?
   - Mark: Yes / No / Partial
   
3. **Verify Temporal Coverage**
   - Is the temporal coverage clearly stated?
   - Does it match expected values (e.g., "2000-2023")?
   - Mark: Yes / No / Partial
   
4. **Verify Format Availability**
   - Is the expected format (CSV, NetCDF, etc.) actually available?
   - Mark: Yes / No / Partial
   
5. **Verify License**
   - Is a clear license statement visible?
   - Mark: Yes / No
   
6. **Verify URL Accessibility**
   - Does the URL work and lead to the dataset?
   - Mark: Yes / No
   
7. **Add Notes** for any discrepancies found

## Scoring

After completing all 25 datasets:

- Count "Yes" responses for each category
- Calculate agreement rate: (Yes + 0.5*Partial) / Total
- Target: >80% agreement validates our automated approach

## Expected Statement for Paper

"We manually verified a stratified random sample of N=25 datasets 
against their source portals (EEA, JRC, Eurostat, Copernicus), 
finding [X]% agreement between catalogued metadata and actual 
portal content, consistent with published INSPIRE conformance benchmarks."
