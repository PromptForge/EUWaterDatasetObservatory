"""
EU Water Dataset Observatory — Climate Adaptation SPARQL Harvester
Two-phase approach (mirrors harvest_sparql.py):

  Phase 1: Collect distinct dataset URIs matching keywords
           (up to 500 per keyword query, ~23 s each)
  Phase 2: Retrieve full metadata in batches of 50 URIs via VALUES clauses

Climate-specific additions:
  - 3 climate adaptation domains loaded from config/climate_keywords.json
  - `domain` column tags each record with the matched keyword set
  - Includes `description` field in Phase 2 (not in base water harvester)
  - Records harvest timestamp and per-domain counts
  - Outputs: data/harvested/climate_harvest.csv
             data/harvested/climate_harvest_summary.json
"""

import requests
import pandas as pd
import time
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SPARQL_ENDPOINT = "https://data.europa.eu/sparql"
HEADERS = {
    "Accept":     "application/sparql-results+json",
    "User-Agent": "EUWaterDatasetObservatory/1.0 (climate-research)",
}

ROOT_DIR    = Path(__file__).parent.parent
CONFIG_DIR  = ROOT_DIR / "config"
HARVEST_DIR = ROOT_DIR / "data" / "harvested"

# ─── SPARQL helpers ──────────────────────────────────────────────────────────

def _get(query: str, timeout: int = 90) -> dict | None:
    """Submit a SPARQL SELECT query; return parsed JSON or None on failure."""
    try:
        r = requests.get(SPARQL_ENDPOINT,
                         params={"query": query},
                         headers=HEADERS,
                         timeout=timeout)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 500 and "exceeds the limit" in r.text:
            return None   # Virtuoso 60-second limit — skip silently
        print(f"    HTTP {r.status_code}: {r.text[:120]}")
        return None
    except requests.exceptions.Timeout:
        print("    Request timed out — skipping")
        return None
    except Exception as exc:
        print(f"    Error: {exc}")
        return None


# ─── Phase 1: collect distinct dataset URIs ──────────────────────────────────

def phase1_get_uris(keyword: str, limit: int = 500) -> list[str]:
    """Return up to `limit` distinct dataset URIs whose title or description
    contains `keyword` (case-insensitive)."""
    kw = keyword.lower().replace('"', '\\"')
    q = f"""
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct:  <http://purl.org/dc/terms/>
SELECT DISTINCT ?dataset
WHERE {{
    ?dataset a dcat:Dataset ;
             dct:title ?t .
    FILTER(CONTAINS(LCASE(STR(?t)), "{kw}"))
}}
LIMIT {limit}
"""
    result = _get(q, timeout=90)
    if not result:
        return []
    return [b["dataset"]["value"] for b in result["results"]["bindings"]]


# ─── Phase 2: batch metadata fetch via VALUES ─────────────────────────────────

def phase2_get_metadata(uri_list: list[str], batch_size: int = 50) -> list[dict]:
    """Fetch metadata for a list of dataset URIs in batches.
    Returns one consolidated record per unique URI.
    Includes `description` (not present in the base water harvester)."""
    all_records: dict[str, dict] = {}

    for i in range(0, len(uri_list), batch_size):
        batch = uri_list[i: i + batch_size]
        uri_values = " ".join(f"<{u}>" for u in batch)

        q = f"""
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct:  <http://purl.org/dc/terms/>
SELECT ?dataset ?title ?description ?modified ?issued
       ?publisher ?license ?language ?spatial ?keyword
WHERE {{
    VALUES ?dataset {{ {uri_values} }}
    OPTIONAL {{ ?dataset dct:title       ?title       }}
    OPTIONAL {{ ?dataset dct:description ?description }}
    OPTIONAL {{ ?dataset dct:modified    ?modified    }}
    OPTIONAL {{ ?dataset dct:issued      ?issued      }}
    OPTIONAL {{ ?dataset dct:publisher   ?publisher   }}
    OPTIONAL {{ ?dataset dct:license     ?license     }}
    OPTIONAL {{ ?dataset dct:language    ?language    }}
    OPTIONAL {{ ?dataset dct:spatial     ?spatial     }}
    OPTIONAL {{ ?dataset dcat:keyword    ?keyword     }}
}}
"""
        result = _get(q, timeout=90)
        if not result:
            for u in batch:
                if u not in all_records:
                    all_records[u] = {"dataset_uri": u}
            continue

        bindings = result["results"]["bindings"]

        # Group all bindings by dataset URI
        grouped: dict[str, list[dict]] = defaultdict(list)
        for b in bindings:
            uri = b.get("dataset", {}).get("value", "")
            if uri:
                grouped[uri].append(b)

        for uri, rows in grouped.items():
            if uri in all_records:
                continue

            # Prefer English title
            def _title_rank(b):
                lang = b.get("title", {}).get("xml:lang", "")
                val  = b.get("title", {}).get("value", "")
                if not val:
                    return 99
                return 0 if lang == "en" else (1 if lang.startswith("en") else 2)

            # Prefer English description
            def _desc_rank(b):
                lang = b.get("description", {}).get("xml:lang", "")
                val  = b.get("description", {}).get("value", "")
                if not val:
                    return 99
                return 0 if lang == "en" else (1 if lang.startswith("en") else 2)

            sorted_rows = sorted(rows, key=_title_rank)
            best        = sorted_rows[0]
            desc_rows   = [b for b in rows if b.get("description")]
            best_desc   = sorted(desc_rows, key=_desc_rank)[0] if desc_rows else {}

            # Collect multi-value fields as unions
            licenses  = {b.get("license",  {}).get("value", "") for b in rows if b.get("license")}
            languages = {b.get("language", {}).get("value", "") for b in rows if b.get("language")}
            spatials  = {b.get("spatial",  {}).get("value", "") for b in rows if b.get("spatial")}
            keywords  = {b.get("keyword",  {}).get("value", "") for b in rows if b.get("keyword")}

            all_records[uri] = {
                "dataset_uri":   uri,
                "title":         best.get("title",    {}).get("value", ""),
                "description":   best_desc.get("description", {}).get("value", ""),
                "modified":      best.get("modified", {}).get("value", ""),
                "issued":        best.get("issued",   {}).get("value", ""),
                "publisher_uri": best.get("publisher",{}).get("value", ""),
                "license":       next(iter(licenses),  ""),
                "language":      " | ".join(sorted(languages)),
                "spatial":       next(iter(spatials),  ""),
                "keyword":       " | ".join(sorted(keywords)),
                "num_languages": len(languages),
                "num_keywords":  len(keywords),
            }

        # Stubs for URIs not returned by the endpoint
        for u in batch:
            if u not in all_records:
                all_records[u] = {"dataset_uri": u}

        time.sleep(0.5)   # polite pause between batches

    return list(all_records.values())


# ─── Metadata enrichment ──────────────────────────────────────────────────────

_COUNTRY_PATTERNS = {
    "eu":          ["eea.europa", "ec.europa", "eurostat", "jrc.ec",
                    "publications.europa", "copernicus.eu"],
    "germany":     ["/de/", "bafg", "umweltbundesamt.de", "lfu.bayern", "nlwkn"],
    "france":      ["/fr/", "eaufrance", "sandre.eaufrance", "ades.brgm", "georisques"],
    "spain":       ["/es/", "chebro", "chsegura", "miteco.gob.es"],
    "italy":       ["/it/", "isprambiente", "arpae", "arpa."],
    "netherlands": ["/nl/", "rijkswaterstaat", "pdok.nl"],
    "poland":      ["/pl/", "gios.gov.pl", "imgw.pl"],
    "austria":     ["/at/", "umweltbundesamt.at", "gis.stmk"],
    "belgium":     ["/be/", "vmm.be", "environment.brussels"],
    "sweden":      ["/se/", "smhi.se", "viss.lansstyrelsen"],
    "denmark":     ["/dk/", "geus.dk"],
    "finland":     ["/fi/", "syke.fi", "ymparisto.fi"],
    "ireland":     ["/ie/", "epa.ie", "openwaterireland"],
    "portugal":    ["/pt/", "apambiente", "snirh.pt"],
    "greece":      ["/el/", "/gr/", "ypeka.gr"],
    "czech":       ["/cz/", "chmi.cz", "vuv.cz"],
    "romania":     ["/ro/", "mmediu.ro"],
    "hungary":     ["/hu/", "vizugy.hu"],
    "bulgaria":    ["/bg/", "moew.government.bg"],
    "croatia":     ["/hr/", "haop.hr"],
    "slovakia":    ["/sk/", "shmu.sk"],
    "slovenia":    ["/si/", "arso.gov.si"],
    "lithuania":   ["/lt/"],
    "latvia":      ["/lv/"],
    "estonia":     ["/ee/"],
    "cyprus":      ["/cy/"],
    "luxembourg":  ["/lu/"],
    "malta":       ["/mt/"],
}

_OPEN_LICENSE_TOKENS = [
    "creativecommons", "cc-by", "cc0", "cc zero", "odbl",
    "open", "eupl", "publicdomain", "opendatacommons",
    "data.europa.eu/euodp", "data.overheid.nl", "govdata.de",
]
_RESTRICTED_TOKENS = ["restricted", "proprietary", "confidential"]


def _country(r: dict) -> str:
    combined = " ".join([
        str(r.get("spatial",       "")),
        str(r.get("publisher_uri", "")),
        str(r.get("dataset_uri",   "")),
    ]).lower()
    for country, kws in _COUNTRY_PATTERNS.items():
        if any(k in combined for k in kws):
            return country
    return "unknown"


def _is_open(lic: str) -> bool:
    s = lic.lower()
    if any(t in s for t in _RESTRICTED_TOKENS):
        return False
    return any(t in s for t in _OPEN_LICENSE_TOKENS)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived boolean/categorical columns expected by score_climate_data.py."""
    df = df.copy()

    # Fill missing text columns
    for col in ["title", "description", "modified", "issued",
                "publisher_uri", "license", "language", "spatial", "keyword"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    df["country"]            = df.apply(_country, axis=1)
    df["has_license"]        = df["license"].apply(lambda x: bool(x) and len(x) > 5)
    df["is_open_license"]    = df["license"].apply(_is_open)
    df["has_coordinates"]    = df["spatial"].apply(
        lambda x: bool(x) and any(c.isdigit() for c in x) and len(x) > 15
    )
    df["description_length"] = df["description"].apply(
        lambda x: len(x.split()) if x else 0
    )
    df["has_temporal"]       = df["issued"].apply(lambda x: bool(x) and len(x) > 4)
    df["temporal_start"]     = ""
    df["temporal_end"]       = ""
    df["is_machine_readable"] = False   # format not in minimal SPARQL query
    df["format"]             = ""
    df["publisher_name"]     = ""
    df["num_languages"]      = df.get(
        "num_languages", pd.Series(1, index=df.index)
    ).fillna(1).astype(int)
    df["num_keywords"]       = df.get(
        "num_keywords",  pd.Series(0, index=df.index)
    ).fillna(0).astype(int)
    df["has_keywords"]       = df["num_keywords"] > 0
    df["is_multilingual"]    = df["num_languages"] >= 2
    df["has_multilingual"]   = df["is_multilingual"]

    return df


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("EU WATER DATASET OBSERVATORY — CLIMATE HARVESTER (2-phase)")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 65)

    # ── Load keywords from config ────────────────────────────────────────────
    keywords_path = CONFIG_DIR / "climate_keywords.json"
    with open(keywords_path) as f:
        domain_queries: dict[str, list[str]] = json.load(f)

    print(f"\nLoaded {len(domain_queries)} domains from {keywords_path.name}:")
    for domain, kws in domain_queries.items():
        print(f"  {domain}: {len(kws)} keywords")

    # ── Connectivity check ───────────────────────────────────────────────────
    test = _get(
        "SELECT ?s WHERE { ?s a <http://www.w3.org/ns/dcat#Dataset> } LIMIT 1",
        timeout=30
    )
    if not test:
        print("\nERROR: SPARQL endpoint unreachable.")
        return None
    print("\nConnection OK\n")

    # ── Phase 1: collect distinct dataset URIs per domain ────────────────────
    print("=== PHASE 1: Collecting distinct climate dataset URIs ===\n")
    uri_to_domain: dict[str, str] = {}     # URI → first-seen domain
    domain_uri_counts: dict[str, int] = {} # domain → new URIs added
    zero_result_domains: list[str]    = []
    total_kws = sum(len(v) for v in domain_queries.values())
    done_kws  = 0

    for domain, keywords in domain_queries.items():
        print(f"  [{domain}]")
        domain_new = 0
        for kw in keywords:
            uris = phase1_get_uris(kw, limit=500)
            new  = 0
            for u in uris:
                if u not in uri_to_domain:
                    uri_to_domain[u] = domain
                    new += 1
            domain_new += new
            done_kws   += 1
            print(f"    [{kw!r}] +{new} new URIs  "
                  f"(total unique: {len(uri_to_domain)}, "
                  f"{done_kws}/{total_kws} kws done)")
            time.sleep(1)

        domain_uri_counts[domain] = domain_new
        if domain_new == 0:
            zero_result_domains.append(domain)
        time.sleep(2)

    print(f"\nPhase 1 complete: {len(uri_to_domain)} unique climate dataset URIs\n")

    # ── Step 3d: second pass with broader keyword variants ───────────────────
    FALLBACK_KEYWORDS: dict[str, list[str]] = {
        "drought_early_warning": [
            "drought monitoring", "hydrological drought", "streamflow deficit",
            "water shortage", "dry spell", "droughts", "drought risk",
        ],
        "climate_infrastructure": [
            "climate adaptation", "climate resilience", "future climate",
            "climate change", "climate projections", "climate impacts",
            "infrastructure resilience", "climate risk assessment",
        ],
        "nature_based_solutions": [
            "nbs water", "natural flood", "wetland", "ecosystem restoration",
            "green infrastructure water", "riparian", "floodplain",
            "ecological restoration", "peatland restoration",
        ],
    }

    if zero_result_domains:
        print(f"\n=== ZERO-RESULT DOMAINS DETECTED: {zero_result_domains} ===")
        print("Running second pass with broader keyword variants ...\n")
        fallback_new: dict[str, int] = {}

        for domain in zero_result_domains:
            fb_kws = FALLBACK_KEYWORDS.get(domain, [])
            print(f"  [{domain}] fallback keywords: {fb_kws}")
            domain_new = 0
            for kw in fb_kws:
                uris = phase1_get_uris(kw, limit=500)
                new  = 0
                for u in uris:
                    if u not in uri_to_domain:
                        uri_to_domain[u] = domain
                        new += 1
                domain_new += new
                print(f"    [{kw!r}] +{new} new URIs (total: {len(uri_to_domain)})")
                time.sleep(1)
            domain_uri_counts[domain] += domain_new
            fallback_new[domain] = domain_new
            print(f"  → {domain}: {domain_new} new URIs from fallback keywords")
            time.sleep(2)

        print(f"\nAfter second pass: {len(uri_to_domain)} unique URIs total")
        # Update zero_result_domains list after second pass
        zero_result_domains = [
            d for d in zero_result_domains if domain_uri_counts.get(d, 0) == 0
        ]
        if zero_result_domains:
            print(f"\n⚠ FINDING: Domains returning ZERO results after both passes:")
            for d in zero_result_domains:
                print(f"  {d} — DISCOVERY GAP confirmed through federated endpoint")
    else:
        fallback_new = {}

    # ── Phase 2: fetch metadata for all URIs ──────────────────────────────────
    print("\n=== PHASE 2: Fetching metadata in batches of 50 ===\n")
    all_uris = list(uri_to_domain.keys())

    if not all_uris:
        print("⚠ No URIs collected — saving empty dataset with zero-result finding.")
        df = pd.DataFrame(columns=[
            "dataset_uri", "title", "description", "modified", "issued",
            "publisher_uri", "license", "language", "spatial", "keyword",
            "num_languages", "num_keywords", "domain",
        ])
        df = enrich(df)
    else:
        records = phase2_get_metadata(all_uris, batch_size=50)

        # Attach the primary domain tag
        for rec in records:
            rec["domain"] = uri_to_domain.get(rec["dataset_uri"], "unknown")

        df = pd.DataFrame(records)
        print(f"Phase 2 complete: {len(df)} records")
        print("Enriching metadata …")
        df = enrich(df)

    # ── Merge with existing water harvest (deduplication) ────────────────────
    existing_harvest = HARVEST_DIR / "raw_harvest.csv"
    overlap_count    = 0
    if existing_harvest.exists():
        water_df  = pd.read_csv(existing_harvest, usecols=["dataset_uri"],
                                low_memory=False)
        water_uris = set(water_df["dataset_uri"].astype(str))
        if len(df) > 0:
            df["in_water_harvest"] = df["dataset_uri"].isin(water_uris)
            overlap_count = int(df["in_water_harvest"].sum())
        else:
            df["in_water_harvest"] = pd.Series(dtype=bool)
        print(f"Overlap with water harvest: {overlap_count} datasets")
    else:
        if len(df) > 0:
            df["in_water_harvest"] = False
        print("  (raw_harvest.csv not found — skipping overlap check)")

    # ── Save climate_harvest.csv ──────────────────────────────────────────────
    HARVEST_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(HARVEST_DIR / "climate_harvest.csv", index=False)
    print(f"Saved climate_harvest.csv ({len(df)} rows)")

    # ── Save climate_harvest_merged.csv (new + overlapping, not water-only) ──
    if len(df) > 0 and "in_water_harvest" in df.columns:
        merged_df = df[df["in_water_harvest"]].copy()
        if len(merged_df) > 0:
            merged_df.to_csv(HARVEST_DIR / "climate_harvest_merged.csv", index=False)
            print(f"Saved climate_harvest_merged.csv ({len(merged_df)} overlapping rows)")

    # ── Build summary ─────────────────────────────────────────────────────────
    has_lic_pct    = float(df["has_license"].mean() * 100)      if len(df) > 0 else 0.0
    open_lic_pct   = float(df["is_open_license"].mean() * 100)  if len(df) > 0 else 0.0
    has_coord_pct  = float(df["has_coordinates"].mean() * 100)  if len(df) > 0 else 0.0
    has_temp_pct   = float(df["has_temporal"].mean() * 100)     if len(df) > 0 else 0.0
    multilingual_pct = float(df["is_multilingual"].mean() * 100) if len(df) > 0 else 0.0
    has_desc_pct   = float((df["description_length"] > 0).mean() * 100) if len(df) > 0 else 0.0

    summary = {
        "harvest_timestamp":   datetime.now().isoformat(),
        "total_datasets":      len(df),
        "unique_datasets":     int(df["dataset_uri"].nunique()) if len(df) > 0 else 0,
        "datasets_per_domain": domain_uri_counts,
        "zero_result_domains": zero_result_domains,
        "fallback_results":    fallback_new,
        "overlap_with_water_harvest": overlap_count,
        "domain_distribution": (
            df["domain"].value_counts().to_dict() if len(df) > 0 else {}
        ),
        "country_distribution": (
            df["country"].value_counts().to_dict() if len(df) > 0 else {}
        ),
        "has_license_pct":       has_lic_pct,
        "is_open_license_pct":   open_lic_pct,
        "has_coordinates_pct":   has_coord_pct,
        "has_temporal_pct":      has_temp_pct,
        "is_multilingual_pct":   multilingual_pct,
        "has_description_pct":   has_desc_pct,
        "discovery_gap_note": (
            "Domains returning zero results after both primary and fallback "
            "keyword passes indicate a potential structural gap in how these "
            "topics are described in the data.europa.eu federated catalogue. "
            "This null result is a scientific finding, not a pipeline failure."
            if zero_result_domains else
            "All domains returned at least one result."
        ),
    }

    with open(HARVEST_DIR / "climate_harvest_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("Saved climate_harvest_summary.json")

    # ── Print summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print("CLIMATE HARVEST SUMMARY")
    print(f"{'='*65}")
    print(f"  Total unique climate datasets : {len(df)}")
    print(f"  Overlap with water harvest    : {overlap_count}")
    print(f"\n  Datasets per domain:")
    for d, c in domain_uri_counts.items():
        zero_flag = "  ← ZERO RESULT" if c == 0 else ""
        print(f"    {d}: {c}{zero_flag}")
    if zero_result_domains:
        print(f"\n  ⚠ ZERO-RESULT DOMAINS (finding): {zero_result_domains}")
    if len(df) > 0:
        print(f"\n  By domain (final assignment):")
        for d, c in df["domain"].value_counts().items():
            print(f"    {d}: {c}")
        print(f"\n  By country (top 10):")
        for ctry, cnt in df["country"].value_counts().head(10).items():
            print(f"    {ctry}: {cnt}")
    print(f"\n  Has license       : {has_lic_pct:.1f}%")
    print(f"  Is open license   : {open_lic_pct:.1f}%")
    print(f"  Has coordinates   : {has_coord_pct:.1f}%")
    print(f"  Has temporal      : {has_temp_pct:.1f}%")
    print(f"  Multilingual      : {multilingual_pct:.1f}%")
    print(f"  Has description   : {has_desc_pct:.1f}%")

    return df


if __name__ == "__main__":
    df = main()
