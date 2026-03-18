"""
EU Water Dataset Observatory - SPARQL Harvester
Two-phase approach proven by endpoint testing:

  Phase 1: SELECT DISTINCT ?dataset WHERE { ... FILTER(CONTAINS(LCASE(STR(?t)), "keyword")) }
           → up to 500 unique dataset URIs per keyword in ~23s
  Phase 2: VALUES-batch metadata fetch (50 URIs per query, ~1-2s each)
           → all properties, deduplicated to 1 record/dataset in Python

Result: ~1,500–5,000 real unique EU water datasets
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
    "User-Agent": "EUWaterDatasetObservatory/1.0 (research)",
}

# Domain → keyword list
DOMAIN_QUERIES = {
    "floods": [
        "flood", "hochwasser", "inondation", "inundación",
        "flood hazard", "flood risk", "flood forecast", "flooding areas",
    ],
    "water_quality": [
        "water quality", "bathing water", "surface water quality",
        "drinking water quality", "water monitoring", "waterbase",
        "biological quality", "chemical status",
    ],
    "groundwater": [
        "groundwater", "grundwasser", "aquifer",
        "groundwater level", "groundwater body",
    ],
    "wfd_metrics": [
        "water framework directive", "ecological status",
        "river basin management", "water body status",
        "river basin district",
    ],
    "utilities": [
        "water supply", "wastewater treatment", "drinking water",
        "water services", "sewage treatment", "urban wastewater",
    ],
    "agricultural_runoff": [
        "nitrate water", "pesticide water", "agricultural water",
        "diffuse pollution", "nutrient loading water",
    ],
    "cross_cutting": [
        "hydrological data", "water resources", "copernicus water",
        "satellite water", "remote sensing water", "climate water",
    ],
}


# ─── SPARQL helpers ───────────────────────────────────────────────────────────

def _get(query: str, timeout: int = 90) -> dict | None:
    try:
        r = requests.get(SPARQL_ENDPOINT, params={"query": query},
                         headers=HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 500 and "exceeds the limit" in r.text:
            return None   # Virtuoso time limit → skip silently
        print(f"    HTTP {r.status_code}: {r.text[:80]}")
        return None
    except requests.exceptions.Timeout:
        print("    Request timed out – skipping")
        return None
    except Exception as exc:
        print(f"    Error: {exc}")
        return None


# ─── Phase 1: get distinct dataset URIs ──────────────────────────────────────

def phase1_get_uris(keyword: str, limit: int = 500) -> list[str]:
    """Return up to `limit` distinct dataset URIs matching keyword in title."""
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
    """
    Fetch metadata for a list of URIs in batches.
    Returns one record per unique URI (best values picked in Python).
    """
    all_records: dict[str, dict] = {}

    for i in range(0, len(uri_list), batch_size):
        batch = uri_list[i: i + batch_size]
        uri_values = " ".join(f"<{u}>" for u in batch)

        q = f"""
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct:  <http://purl.org/dc/terms/>
SELECT ?dataset ?title ?modified ?issued ?publisher ?license ?language ?spatial ?keyword
WHERE {{
    VALUES ?dataset {{ {uri_values} }}
    OPTIONAL {{ ?dataset dct:title    ?title    }}
    OPTIONAL {{ ?dataset dct:modified ?modified }}
    OPTIONAL {{ ?dataset dct:issued   ?issued   }}
    OPTIONAL {{ ?dataset dct:publisher ?publisher }}
    OPTIONAL {{ ?dataset dct:license  ?license  }}
    OPTIONAL {{ ?dataset dct:language ?language }}
    OPTIONAL {{ ?dataset dct:spatial  ?spatial  }}
    OPTIONAL {{ ?dataset dcat:keyword ?keyword  }}
}}
"""
        result = _get(q, timeout=60)
        if not result:
            # fallback: add minimal stub records
            for u in batch:
                if u not in all_records:
                    all_records[u] = {"dataset_uri": u}
            continue

        bindings = result["results"]["bindings"]
        # Group all rows by dataset URI
        grouped: dict[str, list[dict]] = defaultdict(list)
        for b in bindings:
            uri = b.get("dataset", {}).get("value", "")
            if uri:
                grouped[uri].append(b)

        for uri, rows in grouped.items():
            if uri in all_records:
                continue  # already processed

            # Pick best English title (prefer genuine 'en', then 'en-*', then any)
            def title_score(b):
                lang = b.get("title", {}).get("xml:lang", "")
                v = b.get("title", {}).get("value", "")
                if not v:
                    return 99
                if lang == "en":
                    return 0
                if lang.startswith("en"):
                    return 1
                return 2

            sorted_rows = sorted(rows, key=title_score)
            best = sorted_rows[0]

            # Collect multi-value fields
            licenses  = {b.get("license", {}).get("value", "") for b in rows if b.get("license")}
            languages = {b.get("language", {}).get("value", "") for b in rows if b.get("language")}
            spatials  = {b.get("spatial",  {}).get("value", "") for b in rows if b.get("spatial")}
            keywords  = {b.get("keyword",  {}).get("value", "") for b in rows if b.get("keyword")}

            all_records[uri] = {
                "dataset_uri":    uri,
                "title":          best.get("title",    {}).get("value", ""),
                "modified":       best.get("modified", {}).get("value", ""),
                "issued":         best.get("issued",   {}).get("value", ""),
                "publisher_uri":  best.get("publisher",{}).get("value", ""),
                "license":        next(iter(licenses),  ""),
                "language":       " | ".join(sorted(languages)),
                "spatial":        next(iter(spatials),  ""),
                "keyword":        " | ".join(sorted(keywords)),
                "num_languages":  len(languages),
                "num_keywords":   len(keywords),
            }

        # Add stubs for URIs not in grouped (no metadata returned)
        for u in batch:
            if u not in all_records:
                all_records[u] = {"dataset_uri": u}

        time.sleep(0.5)  # polite pause

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

# Open license URI fragments
_OPEN_LICENSE_TOKENS = [
    "creativecommons", "cc-by", "cc0", "cc zero", "odbl",
    "open", "eupl", "publicdomain", "opendatacommons",
    "data.europa.eu/euodp", "data.overheid.nl", "govdata.de",
]
_RESTRICTED_TOKENS = ["restricted", "proprietary", "confidential"]

_MR_FORMATS = {"csv", "json", "xml", "netcdf", "geojson", "gml",
               "api", "wfs", "wms", "sparql", "rdf"}


def _country(r: dict) -> str:
    combined = " ".join([
        str(r.get("spatial", "")),
        str(r.get("publisher_uri", "")),
        str(r.get("dataset_uri", "")),
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
    df["country"]            = df.apply(_country, axis=1)
    df["has_license"]        = df["license"].apply(lambda x: bool(x) and len(str(x)) > 5)
    df["is_open_license"]    = df["license"].apply(_is_open)
    df["has_coordinates"]    = df["spatial"].apply(
        lambda x: bool(x) and any(c.isdigit() for c in str(x)) and len(str(x)) > 15
    )
    df["description_length"] = 0   # not harvested in this minimal query
    df["description"]        = ""  # not harvested
    df["has_temporal"]       = df["issued"].apply(lambda x: bool(x) and len(str(x)) > 4)
    df["temporal_start"]     = ""
    df["temporal_end"]       = ""
    df["is_machine_readable"] = False   # format not harvested
    df["format"]             = ""
    df["publisher_name"]     = ""
    df["num_languages"]      = df.get("num_languages", pd.Series(1, index=df.index)).fillna(1).astype(int)
    df["num_keywords"]       = df.get("num_keywords",  pd.Series(0, index=df.index)).fillna(0).astype(int)
    df["has_keywords"]       = df["num_keywords"] > 0
    df["is_multilingual"]    = df["num_languages"] >= 2
    df["has_multilingual"]   = df["is_multilingual"]
    return df


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("EU WATER DATASET OBSERVATORY — SPARQL HARVESTER (2-phase)")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    test = _get("SELECT ?s WHERE { ?s a <http://www.w3.org/ns/dcat#Dataset> } LIMIT 1", timeout=30)
    if not test:
        print("ERROR: SPARQL endpoint unreachable.")
        return None
    print("Connection OK\n")

    # ── Phase 1: collect all unique URIs by keyword / domain ─────────────────
    print("=== PHASE 1: Collecting distinct dataset URIs ===\n")
    uri_to_domain: dict[str, str] = {}   # URI → first-seen domain

    total_kws = sum(len(v) for v in DOMAIN_QUERIES.values())
    done_kws  = 0

    for domain, keywords in DOMAIN_QUERIES.items():
        print(f"  [{domain}]")
        for kw in keywords:
            uris = phase1_get_uris(kw, limit=500)
            new  = 0
            for u in uris:
                if u not in uri_to_domain:
                    uri_to_domain[u] = domain
                    new += 1
            done_kws += 1
            print(f"    [{kw}] +{new} new URIs  "
                  f"(total unique: {len(uri_to_domain)}, "
                  f"{done_kws}/{total_kws} kws done)")
            time.sleep(1)
        time.sleep(2)

    print(f"\nPhase 1 complete: {len(uri_to_domain)} unique dataset URIs\n")

    # ── Phase 2: fetch metadata for all URIs ──────────────────────────────────
    print("=== PHASE 2: Fetching metadata in batches of 50 ===\n")
    all_uris = list(uri_to_domain.keys())
    records  = phase2_get_metadata(all_uris, batch_size=50)

    # Attach domain
    for rec in records:
        rec["domain"] = uri_to_domain.get(rec["dataset_uri"], "unknown")

    df = pd.DataFrame(records)
    print(f"Phase 2 complete: {len(df)} records")

    # ── Enrich ────────────────────────────────────────────────────────────────
    print("Enriching metadata …")
    df = enrich(df)

    # ── Save ──────────────────────────────────────────────────────────────────
    out_dir = Path("/root/my_projects/EUWaterDatasetObservatory/data/harvested")
    out_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_dir / "raw_harvest.csv", index=False)
    print(f"Saved raw_harvest.csv ({len(df)} rows)")

    summary = {
        "harvest_timestamp":    datetime.now().isoformat(),
        "total_records":        len(df),
        "unique_datasets":      int(df["dataset_uri"].nunique()),
        "domains":              df["domain"].value_counts().to_dict(),
        "countries":            df["country"].value_counts().to_dict(),
        "has_license_pct":      float(df["has_license"].mean() * 100),
        "is_open_license_pct":  float(df["is_open_license"].mean() * 100),
        "has_coordinates_pct":  float(df["has_coordinates"].mean() * 100),
        "has_temporal_pct":     float(df["has_temporal"].mean() * 100),
        "is_multilingual_pct":  float(df["is_multilingual"].mean() * 100),
    }
    with open(out_dir / "harvest_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Saved harvest_summary.json")

    print(f"\n{'='*60}")
    print("HARVEST SUMMARY")
    print(f"{'='*60}")
    print(f"  Total unique datasets: {len(df)}")
    print("\n  By domain:")
    for d, c in df["domain"].value_counts().items():
        print(f"    {d}: {c}")
    print("\n  By country (top 10):")
    for ctry, cnt in df["country"].value_counts().head(10).items():
        print(f"    {ctry}: {cnt}")
    print(f"\n  Has license      : {summary['has_license_pct']:.1f}%")
    print(f"  Is open license  : {summary['is_open_license_pct']:.1f}%")
    print(f"  Has coordinates  : {summary['has_coordinates_pct']:.1f}%")
    print(f"  Has temporal     : {summary['has_temporal_pct']:.1f}%")
    print(f"  Multilingual     : {summary['is_multilingual_pct']:.1f}%")

    return df


if __name__ == "__main__":
    df = main()
