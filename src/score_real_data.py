"""
Score the harvested real data using the existing framework.

Maps harvested SPARQL fields to the column schema expected by
sufficiency_scoring.py, sensitivity_analysis.py, and impact_proxy.py.
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import sys

# Add src to path for sibling imports
sys.path.insert(0, str(Path(__file__).parent))

from sufficiency_scoring import compute_all_sufficiency_scores, classify_readiness
from sensitivity_analysis import run_sensitivity_analysis
from impact_proxy import compute_impact, compute_priority


# ─── Column-mapping helpers ───────────────────────────────────────────────────

def _parse_date_to_str(date_val):
    """
    Try to parse a date value and return 'YYYY-MM-DD' string, or '' on failure.
    score_temporal_recency() does strptime(row['last_modified'], '%Y-%m-%d').
    """
    if not date_val or (isinstance(date_val, float) and np.isnan(date_val)):
        return ""
    s = str(date_val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return ""
    # Try ISO date-time / date formats
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d", "%Y/%m/%d",
                "%d-%m-%Y", "%d/%m/%Y", "%Y"):
        try:
            dt = datetime.strptime(s[:len(fmt.replace("%Y","0000").replace("%m","00")
                                       .replace("%d","00").replace("%H","00")
                                       .replace("%M","00").replace("%S","00")
                                       .replace("%f","000000").replace("T","T"))],
                                   fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    # Fallback: take the first 10 chars if they look like YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # Year-only
    if len(s) == 4 and s.isdigit():
        return f"{s}-01-01"
    return ""


def _extract_format(format_val):
    """
    The SPARQL format field may be a full URI or a label.
    Map it to a short string used by score_format_readability().
    """
    if not format_val or (isinstance(format_val, float) and np.isnan(format_val)):
        return ""
    s = str(format_val).strip().lower()
    # Strip common prefixes
    for prefix in ("http://publications.europa.eu/resource/authority/file-type/",
                   "http://www.iana.org/assignments/media-types/",
                   "https://www.iana.org/assignments/media-types/"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    # Normalise to labels used in score_format_readability
    mapping = {
        "csv": "CSV", "text/csv": "CSV",
        "json": "JSON", "application/json": "JSON",
        "geojson": "JSON", "application/geo+json": "JSON",
        "xml": "XML", "text/xml": "XML", "application/xml": "XML",
        "netcdf": "NetCDF", "nc": "NetCDF", "application/x-netcdf": "NetCDF",
        "geotiff": "GeoTIFF", "tiff": "GeoTIFF", "image/tiff": "GeoTIFF",
        "shapefile": "Shapefile", "shp": "Shapefile",
        "pdf": "PDF", "application/pdf": "PDF",
        "excel": "Excel", "xlsx": "Excel", "xls": "Excel",
        "application/vnd.ms-excel": "Excel",
        "api": "API", "wfs": "API", "wms": "API", "ogcapi": "API",
        "sparql": "API", "rest": "API",
    }
    for key, val in mapping.items():
        if key in s:
            return val
    return s.upper()[:20] if s else ""


def _infer_publisher_type(publisher_name, publisher_uri):
    """
    Infer publisher type from name/URI for impact scoring.
    Must match keys in impact_proxy.score_publisher_type().
    """
    combined = (str(publisher_name) + " " + str(publisher_uri)).lower()
    if any(k in combined for k in ["joint research", "jrc", "eea", "european environment",
                                    "eurostat", "copernicus", "emsa", "efas", "ecmwf",
                                    "european commission", "ec.europa"]):
        return "eu_agency"
    if any(k in combined for k in ["hydrologi", "hydrograph", "bundesanstalt",
                                    "rijkswaterstaat", "umweltbundes", "seai",
                                    "smet", "smhi", "syke", "brgm", "grdc"]):
        return "national_hydro"
    if any(k in combined for k in ["ministère", "ministerio", "ministry", "ministère",
                                    "minister", "bundesminister"]):
        return "ministry"
    if any(k in combined for k in ["university", "universit", "research", "institute",
                                    "hochschule", "ifremer"]):
        return "research"
    if any(k in combined for k in ["region", "province", "county", "commune",
                                    "gemeente", "departement", "district"]):
        return "regional"
    return "other"


def _infer_update_frequency(title, description):
    """Infer update frequency from title/description text."""
    text = (str(title) + " " + str(description)).lower()
    if any(k in text for k in ["real-time", "realtime", "hourly", "near real",
                                "live", "streaming"]):
        return "daily"
    if any(k in text for k in ["daily", "jour", "täglich"]):
        return "daily"
    if any(k in text for k in ["weekly", "hebdomadaire", "wöchentlich"]):
        return "weekly"
    if any(k in text for k in ["monthly", "mensuel", "monatlich"]):
        return "monthly"
    if any(k in text for k in ["annual", "yearly", "annuel", "jährlich",
                                "six-year", "6-year"]):
        return "annual"
    if any(k in text for k in ["irregular", "irrégulier", "unregelmäßig"]):
        return "irregular"
    return "unknown"


def _calc_temporal_span(temporal_start, temporal_end):
    """Calculate temporal span in years."""
    s_str = _parse_date_to_str(temporal_start)
    e_str = _parse_date_to_str(temporal_end)
    if not s_str:
        return 0
    try:
        s_year = int(s_str[:4])
        e_year = int(e_str[:4]) if e_str else datetime.now().year
        return max(0, e_year - s_year)
    except Exception:
        return 0


def _infer_spatial_precision(spatial_val, has_coordinates):
    """
    Map spatial URI/string to a precision level compatible with
    score_spatial_precision() and score_spatial_aggregation().
    """
    if has_coordinates:
        return "coordinates"
    if not spatial_val or (isinstance(spatial_val, float) and np.isnan(spatial_val)):
        return ""
    s = str(spatial_val).lower()
    if any(k in s for k in ["-180", "-90", "bbox", "envelope",
                              "polygon", "point", "linestring"]):
        return "bbox"
    if any(k in s for k in ["nuts3", "nuts2", "province", "region",
                              "river basin", "watershed", "catchment"]):
        return "region_name"
    if any(k in s for k in ["nuts0", "country", "nation", "member state"]):
        return "country_level"
    return "country_level"   # conservative default


def _count_keywords(keyword_val):
    """Count keywords from a pipe-separated or comma-separated keyword string."""
    if not keyword_val or (isinstance(keyword_val, float) and np.isnan(keyword_val)):
        return 0
    s = str(keyword_val).strip()
    if not s:
        return 0
    for sep in ("|", ";", ","):
        if sep in s:
            return len([k for k in s.split(sep) if k.strip()])
    return 1


def _count_languages(language_val):
    """Count distinct languages from language field."""
    if not language_val or (isinstance(language_val, float) and np.isnan(language_val)):
        return 1
    s = str(language_val).strip()
    if not s or s.lower() in ("nan", "none"):
        return 1
    for sep in (",", ";", " "):
        parts = [p.strip() for p in s.split(sep) if p.strip()]
        if len(parts) > 1:
            return len(parts)
    return 1


def _is_open_license(license_val):
    """Return True if the license URI/string indicates an open license."""
    if not license_val or (isinstance(license_val, float) and np.isnan(license_val)):
        return False
    s = str(license_val).lower()
    open_indicators = [
        "cc-by", "cc0", "cc zero", "odbl", "odc-by", "pddl",
        "open", "eupl", "lgpl", "gpl", "mit", "apache",
        "data.europa.eu/euodp", "creativecommons", "publicdomain",
        "etalab", "iodl", "nlod", "dl-de", "govdata",
    ]
    restricted_indicators = ["restricted", "proprietary", "confidential",
                              "not open", "commercial only"]
    for r in restricted_indicators:
        if r in s:
            return False
    for o in open_indicators:
        if o in s:
            return True
    # Some endpoints publish license URIs; if it contains a recognizable authority
    if "creativecommons.org" in s or "opendefinition.org" in s:
        return True
    return False


# ─── Main preparation function ────────────────────────────────────────────────

def prepare_harvested_data(harvest_path):
    """
    Load raw_harvest.csv and produce a DataFrame with all columns
    expected by the sufficiency / sensitivity / impact modules.
    """
    df = pd.read_csv(harvest_path, low_memory=False)
    print(f"  Loaded {len(df)} rows, columns: {list(df.columns)}")

    prepared = pd.DataFrame()

    # ── Core identifiers & text ──────────────────────────────────────────────
    prepared["dataset_id"]    = df["dataset_uri"].fillna("").astype(str)
    prepared["title"]         = df["title"].fillna("").astype(str)
    prepared["description"]   = df["description"].fillna("").astype(str)
    prepared["domain"]        = df["domain"].fillna("unknown").astype(str)
    prepared["country"]       = df.get("country", pd.Series("unknown", index=df.index)).fillna("unknown").astype(str)

    # ── Temporal ─────────────────────────────────────────────────────────────
    prepared["last_modified"] = df["modified"].apply(_parse_date_to_str)
    prepared["temporal_span_years"] = df.apply(
        lambda r: _calc_temporal_span(r.get("temporal_start"), r.get("temporal_end")),
        axis=1
    )
    prepared["update_frequency"] = df.apply(
        lambda r: _infer_update_frequency(r.get("title", ""), r.get("description", "")),
        axis=1
    )

    # ── Spatial ──────────────────────────────────────────────────────────────
    has_coords = df.get("has_coordinates", pd.Series(False, index=df.index)).fillna(False)
    # Convert string "True"/"False" if loaded from CSV
    if has_coords.dtype == object:
        has_coords = has_coords.map(lambda x: str(x).strip().lower() == "true")
    prepared["has_coordinates"]   = has_coords.astype(bool)
    prepared["spatial_precision"] = df.apply(
        lambda r: _infer_spatial_precision(r.get("spatial", ""), prepared.loc[r.name, "has_coordinates"]),
        axis=1
    )

    # ── License ──────────────────────────────────────────────────────────────
    prepared["is_open_license"] = df["license"].apply(_is_open_license)
    prepared["license"]         = df["license"].fillna("").astype(str)

    # ── Format ───────────────────────────────────────────────────────────────
    prepared["format"] = df["format"].apply(_extract_format)

    # ── Description quality ──────────────────────────────────────────────────
    prepared["description_length"] = prepared["description"].apply(
        lambda x: len(str(x).split()) if x else 0
    )

    # ── Keywords / vocabulary ─────────────────────────────────────────────────
    prepared["num_keywords"] = df["keyword"].apply(_count_keywords)
    # EuroVoc proxy: keyword field exists and publisher is EU-level
    prepared["has_eurovoc"] = (
        (prepared["num_keywords"] >= 3) &
        (prepared["country"].isin(["eu"]) | prepared["domain"].isin(["wfd_metrics"]))
    )

    # ── Languages ────────────────────────────────────────────────────────────
    prepared["num_languages"]  = df["language"].apply(_count_languages)
    prepared["has_multilingual"] = prepared["num_languages"] >= 2

    # ── Publisher type (needed by impact_proxy) ───────────────────────────────
    prepared["publisher_type"] = df.apply(
        lambda r: _infer_publisher_type(
            r.get("publisher_name", ""), r.get("publisher_uri", "")
        ),
        axis=1
    )
    prepared["publisher"] = df.get("publisher_name", pd.Series("", index=df.index)).fillna("").astype(str)

    # ── Extra pass-through fields ─────────────────────────────────────────────
    prepared["dataset_uri"]      = df["dataset_uri"].fillna("").astype(str)
    prepared["modified_date"]    = df["modified"].fillna("").astype(str)
    prepared["temporal_start"]   = df.get("temporal_start", pd.Series("", index=df.index)).fillna("").astype(str)
    prepared["temporal_end"]     = df.get("temporal_end",   pd.Series("", index=df.index)).fillna("").astype(str)
    prepared["keyword_raw"]      = df["keyword"].fillna("").astype(str)
    prepared["language_raw"]     = df["language"].fillna("").astype(str)
    prepared["has_license_flag"] = df.get("has_license", pd.Series(False, index=df.index)).map(
        lambda x: str(x).strip().lower() == "true"
    )
    prepared["has_temporal_flag"] = df.get("has_temporal", pd.Series(False, index=df.index)).map(
        lambda x: str(x).strip().lower() == "true"
    )
    prepared["is_machine_readable"] = df.get("is_machine_readable", pd.Series(False, index=df.index)).map(
        lambda x: str(x).strip().lower() == "true"
    )

    print(f"  Prepared {len(prepared)} records with {len(prepared.columns)} columns")
    return prepared


# ─── Analysis runner ──────────────────────────────────────────────────────────

def main():
    """Run full scoring and analysis pipeline on harvested data."""

    harvest_path = Path("/root/my_projects/EUWaterDatasetObservatory/data/harvested/raw_harvest.csv")
    output_dir   = Path("/root/my_projects/EUWaterDatasetObservatory/data/outputs_real")
    output_dir.mkdir(parents=True, exist_ok=True)

    if not harvest_path.exists():
        print(f"ERROR: Harvest file not found at {harvest_path}")
        print("Run harvest_sparql.py first.")
        return

    # ── 1. Load & prepare ────────────────────────────────────────────────────
    print("\n=== Loading & preparing harvested data ===")
    df = prepare_harvested_data(harvest_path)
    df.to_csv(output_dir / "prepared_records.csv", index=False)
    print(f"  Saved prepared_records.csv ({len(df)} rows)")

    # ── 2. Sufficiency scoring ────────────────────────────────────────────────
    print("\n=== Computing sufficiency scores ===")
    scores_df = compute_all_sufficiency_scores(df)
    # Merge with prepared data to carry through all columns
    full_df = df.merge(scores_df, on="dataset_id", how="left")
    full_df.to_csv(output_dir / "sufficiency_scores.csv", index=False)
    print(f"  Saved sufficiency_scores.csv ({len(full_df)} rows)")

    # ── 3. Sensitivity analysis ───────────────────────────────────────────────
    print("\n=== Running sensitivity analysis (±25% weight perturbation) ===")
    sens_results = run_sensitivity_analysis(full_df, perturbation=0.25)
    sens_summary_df = pd.DataFrame(sens_results["summary"])
    sens_detail_df  = pd.DataFrame(sens_results["detail"])
    sens_summary_df.to_csv(output_dir / "sensitivity_summary.csv", index=False)
    sens_detail_df.to_csv(output_dir  / "sensitivity_detail.csv",  index=False)
    print(f"  Saved sensitivity_summary.csv & sensitivity_detail.csv")

    # ── 4. Impact & priority scores ───────────────────────────────────────────
    print("\n=== Computing impact & priority scores ===")
    impacts = []
    for _, row in full_df.iterrows():
        impact_val, impact_components = compute_impact(row)
        impacts.append({"dataset_id": row["dataset_id"],
                        "impact_score": impact_val,
                        **{f"impact_{k}": v for k, v in impact_components.items()}})
    impact_df = pd.DataFrame(impacts)
    full_df = full_df.merge(impact_df, on="dataset_id", how="left")

    for task in ["early_warning", "compliance_reporting", "cross_border"]:
        score_col = f"{task}_score"
        if score_col in full_df.columns:
            full_df[f"{task}_priority"] = full_df.apply(
                lambda r: compute_priority(r["impact_score"], r[score_col]),
                axis=1
            )

    # Save priority fixes (top 20 per task)
    priority_rows = []
    for task in ["early_warning", "compliance_reporting", "cross_border"]:
        priority_col = f"{task}_priority"
        if priority_col in full_df.columns:
            top = full_df.nlargest(20, priority_col).copy()
            top["task"] = task
            top["priority_rank"] = range(1, len(top) + 1)
            priority_rows.append(top)

    if priority_rows:
        priority_df = pd.concat(priority_rows, ignore_index=True)
        priority_df.to_csv(output_dir / "priority_fixes.csv", index=False)
        print(f"  Saved priority_fixes.csv")

    full_df.to_csv(output_dir / "priority_scores.csv", index=False)
    print(f"  Saved priority_scores.csv ({len(full_df)} rows)")

    # ── 5. Summary statistics ─────────────────────────────────────────────────
    print("\n=== Generating summary statistics ===")

    def _pct_ready(col):
        return float((full_df[col].apply(classify_readiness) == "Ready").mean() * 100)
    def _pct_partial(col):
        return float((full_df[col].apply(classify_readiness) == "Partial").mean() * 100)
    def _pct_insufficient(col):
        return float((full_df[col].apply(classify_readiness) == "Insufficient").mean() * 100)

    task_map = {
        "early_warning":    "early_warning_score",
        "compliance":       "compliance_reporting_score",
        "cross_border":     "cross_border_score",
    }

    suf_stats = {}
    for label, col in task_map.items():
        if col not in full_df.columns:
            continue
        suf_stats[label] = {
            "mean":            float(full_df[col].mean()),
            "std":             float(full_df[col].std()),
            "median":          float(full_df[col].median()),
            "ready_pct":       _pct_ready(col),
            "partial_pct":     _pct_partial(col),
            "insufficient_pct": _pct_insufficient(col),
        }

    summary = {
        "analysis_timestamp": datetime.now().isoformat(),
        "data_source": "data.europa.eu SPARQL harvest",
        "total_records":    len(full_df),
        "unique_datasets":  int(full_df["dataset_id"].nunique()),

        "sufficiency_scores": suf_stats,

        "metadata_completeness": {
            "has_license":       float(full_df["is_open_license"].mean() * 100),
            "has_coordinates":   float(full_df["has_coordinates"].mean() * 100),
            "has_temporal":      float(full_df["has_temporal_flag"].mean() * 100),
            "is_machine_readable": float(full_df["is_machine_readable"].mean() * 100),
            "has_eurovoc":       float(full_df["has_eurovoc"].mean() * 100),
            "is_multilingual":   float(full_df["has_multilingual"].mean() * 100),
        },

        "domain_distribution": full_df["domain"].value_counts().to_dict(),
        "country_distribution": full_df["country"].value_counts().head(15).to_dict(),

        "sensitivity": {
            row["task"]: {
                "primary_metric":   row["primary_metric"],
                "baseline_mean":    float(row["baseline_mean"]),
                "range_pp":         float(row["range_pp"]),
                "low_ready_pct":    float(row["low_ready_pct"]),
                "high_ready_pct":   float(row["high_ready_pct"]),
            }
            for _, row in sens_summary_df.iterrows()
        },
    }

    summary_path = output_dir / "analysis_summary_real.json"
    with open(summary_path, "w") as f:
        json.dump(summary, indent=2, fp=f)
    print(f"  Saved analysis_summary_real.json")

    # ── 6. Print key results ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("REAL DATA ANALYSIS RESULTS")
    print("=" * 60)
    print(f"\nTotal datasets analyzed : {summary['total_records']}")
    print(f"Unique dataset URIs     : {summary['unique_datasets']}")

    print("\nSUFFICIENCY SCORES (Mean ± SD):")
    for task, s in suf_stats.items():
        print(f"  {task:20s}: {s['mean']:.3f} ± {s['std']:.3f}  "
              f"  Ready={s['ready_pct']:.1f}%  Partial={s['partial_pct']:.1f}%  "
              f"Insufficient={s['insufficient_pct']:.1f}%")

    print("\nMETADATA COMPLETENESS (% of datasets):")
    for field, pct in summary["metadata_completeness"].items():
        print(f"  {field:25s}: {pct:.1f}%")

    print("\nDOMAIN DISTRIBUTION:")
    for domain, cnt in summary["domain_distribution"].items():
        print(f"  {domain}: {cnt}")

    print("\nTOP COUNTRIES (up to 10):")
    for country, cnt in list(summary["country_distribution"].items())[:10]:
        print(f"  {country}: {cnt}")

    print(f"\nAll results saved to: {output_dir}")
    return summary


if __name__ == "__main__":
    main()
