"""
EU Water Dataset Observatory - Climate Adaptation SPARQL Harvester

Two-phase approach (mirrors harvest_sparql.py):

  Phase 1: SELECT DISTINCT ?dataset WHERE { ... FILTER(CONTAINS(LCASE(?t), "keyword")) }
           → up to 500 unique dataset URIs per keyword
  Phase 2: VALUES-batch metadata fetch (50 URIs per query)
           → full metadata, deduplicated in Python

Climate-specific additions:
  - 5 climate domains: drought, climate_projections, precipitation,
    nature_based_solutions, temperature
  - Multilingual keywords (EN/DE/FR/ES/IT)
  - Assigns `climate_domain` column for downstream scoring
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

# Climate domain → keyword list (multilingual, prioritised by retrieval value)
CLIMATE_DOMAIN_QUERIES = {
    "drought": [
        # EN
        "drought", "water scarcity", "water stress", "low flow",
        "drought monitoring", "drought index", "SPI", "SPEI",
        "drought early warning",
        # DE
        "dürre", "wasserknappheit", "trockenheit",
        # FR
        "sécheresse", "stress hydrique",
        # ES
        "sequía", "escasez de agua",
        # IT
        "siccità",
    ],
    "climate_projections": [
        # EN
        "climate projection", "climate scenario", "RCP", "SSP",
        "climate change impact", "future climate", "climate model",
        "downscaling", "CMIP",
        # DE
        "klimaprojektion", "klimaszenario", "klimawandel",
        # FR
        "projection climatique", "changement climatique",
        # ES
        "proyección climática", "cambio climático",
        # IT
        "proiezione climatica", "cambiamento climatico",
    ],
    "precipitation": [
        # EN
        "precipitation", "rainfall", "rain gauge",
        "extreme precipitation", "rainfall intensity",
        # DE
        "niederschlag", "regenmesser",
        # FR
        "précipitation", "pluviométrie",
        # ES
        "precipitación", "pluviometría",
        # IT
        "precipitazione", "pluviometria",
    ],
    "nature_based_solutions": [
        # EN
        "nature-based solutions", "green infrastructure",
        "ecosystem services", "wetland restoration",
        "natural water retention", "floodplain restoration",
        # DE
        "naturbasierte lösungen", "grüne infrastruktur",
        # FR
        "solutions fondées sur la nature", "infrastructure verte",
        # ES
        "soluciones basadas en la naturaleza",
        # IT
        "soluzioni basate sulla natura",
    ],
    "temperature": [
        # EN
        "water temperature", "air temperature", "heat wave",
        "thermal regime", "temperature trend",
        # DE
        "wassertemperatur", "lufttemperatur", "hitzewelle",
        # FR
        "température de l'eau", "vague de chaleur",
        # ES
        "temperatura del agua", "ola de calor",
        # IT
        "temperatura dell'acqua", "ondata di calore",
    ],
}

# Ordered list used for domain assignment (first match wins)
_DOMAIN_KEYWORDS_FOR_ASSIGNMENT = {
    domain: [kw.lower() for kw in kws]
    for domain, kws in CLIMATE_DOMAIN_QUERIES.items()
}


# ─── SPARQL helpers (identical pattern to harvest_sparql.py) ─────────────────

def _get(query: str, timeout: int = 90) -> dict | None:
    try:
        r = requests.get(SPARQL_ENDPOINT, params={"query": query},
                         headers=HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 500 and "exceeds the limit" in r.text:
            return None   # Virtuoso time limit → skip silently
        print(f"    HTTP {r.status_code}: {r.text[:120]}")
        return None
    except requests.exceptions.Timeout:
        print("    Request timed out – skipping")
        return None
    except Exception as exc:
        print(f"    Error: {exc}")
        return None


# ─── Phase 1: collect distinct dataset URIs ──────────────────────────────────

def phase1_get_uris(keyword: str, limit: int = 500) -> list[str]:
    """Return up to `limit` distinct dataset URIs whose title contains `keyword`."""
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
    Fetch metadata for a list of URIs in batches of `batch_size`.
    Returns one consolidated record per unique URI.
    Includes `description` field (not present in the base water harvester).
    """
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
                if lang == "en":
                    return 0
                if lang.startswith("en"):
                    return 1
                return 2

            sorted_rows = sorted(rows, key=_title_rank)
            best = sorted_rows[0]

            # Prefer English description
            def _desc_rank(b):
                lang = b.get("description", {}).get("xml:lang", "")
                val  = b.get("description", {}).get("value", "")
                if not val:
                    return 99
                if lang == "en":
                    return 0
                if lang.startswith("en"):
                    return 1
                return 2

            desc_rows = [b for b in rows if b.get("description")]
            best_desc = sorted(desc_rows, key=_desc_rank)[0] if desc_rows else {}

            # Multi-value fields: union all values
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

_OPEN_LICENSE_TOKENS  = [
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


def _assign_climate_domain(row: dict) -> str:
    """
    Assign the best-matching climate domain using title + description text.
    Returns the first domain whose keyword list contains a match.
    Falls back to 'climate_general'.
    """
    text = (
        str(row.get("title", "")) + " " + str(row.get("description", ""))
    ).lower()

    for domain, keywords in _DOMAIN_KEYWORDS_FOR_ASSIGNMENT.items():
        for kw in keywords:
            if kw in text:
                return domain

    return "climate_general"


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived boolean/categorical columns expected by score_climate_data.py."""
    df["country"]             = df.apply(_country, axis=1)
    df["has_license"]         = df["license"].apply(lambda x: bool(x) and len(str(x)) > 5)
    df["is_open_license"]     = df["license"].apply(_is_open)
    df["has_coordinates"]     = df["spatial"].apply(
        lambda x: bool(x) and any(c.isdigit() for c in str(x)) and len(str(x)) > 15
    )
    df["description_length"]  = df["description"].apply(
        lambda x: len(str(x).split()) if x else 0
    )
    df["has_temporal"]        = df["issued"].apply(lambda x: bool(x) and len(str(x)) > 4)
    df["temporal_start"]      = ""
    df["temporal_end"]        = ""
    df["is_machine_readable"] = False   # format not in minimal query
    df["format"]              = ""
    df["publisher_name"]      = ""
    df["num_languages"]       = df.get("num_languages", pd.Series(1, index=df.index)).fillna(1).astype(int)
    df["num_keywords"]        = df.get("num_keywords",  pd.Series(0, index=df.index)).fillna(0).astype(int)
    df["has_keywords"]        = df["num_keywords"] > 0
    df["is_multilingual"]     = df["num_languages"] >= 2
    df["has_multilingual"]    = df["is_multilingual"]

    # Climate-specific domain assignment
    df["climate_domain"] = df.apply(_assign_climate_domain, axis=1)

    return df


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("EU WATER DATASET OBSERVATORY — CLIMATE HARVESTER (2-phase)")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    # Connectivity check
    test = _get(
        "SELECT ?s WHERE { ?s a <http://www.w3.org/ns/dcat#Dataset> } LIMIT 1",
        timeout=30
    )
    if not test:
        print("ERROR: SPARQL endpoint unreachable.")
        return None
    print("Connection OK\n")

    # ── Phase 1: collect all unique URIs by keyword / domain ─────────────────
    print("=== PHASE 1: Collecting distinct climate dataset URIs ===\n")
    uri_to_domain: dict[str, str] = {}   # URI → first-seen climate domain
    domain_uri_counts: dict[str, int] = {}

    total_kws = sum(len(v) for v in CLIMATE_DOMAIN_QUERIES.values())
    done_kws  = 0

    for domain, keywords in CLIMATE_DOMAIN_QUERIES.items():
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
            done_kws += 1
            print(f"    [{kw}] +{new} new URIs  "
                  f"(total unique: {len(uri_to_domain)}, "
                  f"{done_kws}/{total_kws} kws done)")
            time.sleep(1)
        domain_uri_counts[domain] = domain_new
        time.sleep(2)

    print(f"\nPhase 1 complete: {len(uri_to_domain)} unique climate dataset URIs\n")

    # ── Phase 2: fetch metadata for all URIs ──────────────────────────────────
    print("=== PHASE 2: Fetching metadata in batches of 50 ===\n")
    all_uris = list(uri_to_domain.keys())
    records  = phase2_get_metadata(all_uris, batch_size=50)

    # Attach the primary harvesting domain
    for rec in records:
        rec["domain"] = uri_to_domain.get(rec["dataset_uri"], "unknown")

    df = pd.DataFrame(records)
    print(f"Phase 2 complete: {len(df)} records")

    # ── Enrich ────────────────────────────────────────────────────────────────
    print("Enriching metadata …")
    df = enrich(df)

    # ── Save ──────────────────────────────────────────────────────────────────
    out_dir = Path(__file__).parent.parent / "data" / "harvested"
    out_dir.mkdir(parents=True, exist_ok=True)

    output_path = out_dir / "climate_harvest.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved climate_harvest.csv ({len(df)} rows)")

    summary = {
        "harvest_timestamp":       datetime.now().isoformat(),
        "total_records":           len(df),
        "unique_datasets":         int(df["dataset_uri"].nunique()),
        "domain_uri_counts":       domain_uri_counts,
        "climate_domain_distribution": df["climate_domain"].value_counts().to_dict(),
        "country_distribution":    df["country"].value_counts().to_dict(),
        "has_license_pct":         float(df["has_license"].mean() * 100),
        "is_open_license_pct":     float(df["is_open_license"].mean() * 100),
        "has_coordinates_pct":     float(df["has_coordinates"].mean() * 100),
        "has_temporal_pct":        float(df["has_temporal"].mean() * 100),
        "is_multilingual_pct":     float(df["is_multilingual"].mean() * 100),
        "has_description_pct":     float((df["description_length"] > 0).mean() * 100),
    }
    with open(out_dir / "climate_harvest_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("Saved climate_harvest_summary.json")

    print(f"\n{'='*60}")
    print("CLIMATE HARVEST SUMMARY")
    print(f"{'='*60}")
    print(f"  Total unique climate datasets: {len(df)}")
    print("\n  By harvest domain (first-seen):")
    for d, c in df["domain"].value_counts().items():
        print(f"    {d}: {c}")
    print("\n  By climate_domain (title/desc assignment):")
    for d, c in df["climate_domain"].value_counts().items():
        print(f"    {d}: {c}")
    print("\n  By country (top 10):")
    for ctry, cnt in df["country"].value_counts().head(10).items():
        print(f"    {ctry}: {cnt}")
    print(f"\n  Has license       : {summary['has_license_pct']:.1f}%")
    print(f"  Is open license   : {summary['is_open_license_pct']:.1f}%")
    print(f"  Has coordinates   : {summary['has_coordinates_pct']:.1f}%")
    print(f"  Has temporal      : {summary['has_temporal_pct']:.1f}%")
    print(f"  Multilingual      : {summary['is_multilingual_pct']:.1f}%")
    print(f"  Has description   : {summary['has_description_pct']:.1f}%")

    return df


if __name__ == "__main__":
    df = main()
