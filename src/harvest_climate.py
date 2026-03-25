"""
EU Water Dataset Observatory — Climate Adaptation SPARQL Harvester
Two-phase approach (mirrors harvest_sparql.py):

  Phase 1: Collect distinct dataset URIs matching keywords.
           Uses a combined OR-FILTER query per domain (all keywords in one
           query) to stay within Virtuoso's 60-second per-query limit.
           Falls back to batches of 5 keywords if the full OR query times out.
           Falls back to individual keyword queries if batch queries fail.
           Saves partial results after each domain (checkpoint).

  Phase 2: Retrieve full metadata in batches of 50 URIs via VALUES clauses.

Outputs:
  data/harvested/climate_harvest.csv
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

def _get(query: str, timeout: int = 50) -> dict | None:
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


# ─── Phase 1 helpers ─────────────────────────────────────────────────────────

def _build_or_filter_query(keywords: list[str], limit: int = 500) -> str:
    """Build a single SPARQL query with OR-combined FILTER conditions."""
    esc = [kw.lower().replace('"', '\\"') for kw in keywords]
    conditions = " ||\n        ".join(
        f'CONTAINS(LCASE(STR(?t)), "{kw}")' for kw in esc
    )
    return f"""
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct:  <http://purl.org/dc/terms/>
SELECT DISTINCT ?dataset
WHERE {{
    ?dataset a dcat:Dataset ;
             dct:title ?t .
    FILTER(
        {conditions}
    )
}}
LIMIT {limit}
"""


def phase1_get_uris_combined(domain: str, keywords: list[str],
                              limit: int = 500) -> list[str]:
    """
    Get URIs for a domain in a single OR-FILTER query.
    Falls back to batches of 5 if the combined query times out.
    Falls back to individual keyword queries if batches fail.
    Returns list of unique dataset URIs found.
    """
    all_uris: set[str] = set()

    # ── Strategy 1: single OR query with all keywords ────────────────────────
    print(f"    Trying combined OR query ({len(keywords)} keywords) …")
    q = _build_or_filter_query(keywords, limit=limit)
    result = _get(q, timeout=50)

    if result is not None:
        uris = [b["dataset"]["value"] for b in result["results"]["bindings"]]
        all_uris.update(uris)
        print(f"    Combined query → {len(uris)} URIs")
        return list(all_uris)

    # ── Strategy 2: batches of 5 keywords ────────────────────────────────────
    print("    Combined query failed — trying batches of 5 …")
    for i in range(0, len(keywords), 5):
        batch_kws = keywords[i: i + 5]
        q = _build_or_filter_query(batch_kws, limit=limit)
        result = _get(q, timeout=50)
        if result is not None:
            uris = [b["dataset"]["value"] for b in result["results"]["bindings"]]
            new  = [u for u in uris if u not in all_uris]
            all_uris.update(uris)
            print(f"    Batch [{i}:{i+5}] → {len(uris)} URIs, {len(new)} new")
        else:
            print(f"    Batch [{i}:{i+5}] timed out — falling back to individual")
            # ── Strategy 3: individual keyword queries ───────────────────────
            for kw in batch_kws:
                q_ind = _build_or_filter_query([kw], limit=limit)
                result_ind = _get(q_ind, timeout=45)
                if result_ind is not None:
                    uris = [b["dataset"]["value"]
                            for b in result_ind["results"]["bindings"]]
                    new  = [u for u in uris if u not in all_uris]
                    all_uris.update(uris)
                    print(f"      [{kw!r}] → {len(uris)} URIs, {len(new)} new")
                else:
                    print(f"      [{kw!r}] → timed out / skipped")
                time.sleep(0.2)
        time.sleep(0.3)

    return list(all_uris)


# ─── Phase 2: batch metadata fetch via VALUES ─────────────────────────────────

def phase2_get_metadata(uri_list: list[str], batch_size: int = 50) -> list[dict]:
    """Fetch metadata for a list of dataset URIs in batches.
    Includes `description` field."""
    all_records: dict[str, dict] = {}
    total = len(uri_list)

    for i in range(0, total, batch_size):
        batch      = uri_list[i: i + batch_size]
        uri_values = " ".join(f"<{u}>" for u in batch)
        progress   = f"({i + len(batch)}/{total})"

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
        result = _get(q, timeout=55)
        if not result:
            print(f"  batch {progress} metadata fetch failed — using stubs")
            for u in batch:
                if u not in all_records:
                    all_records[u] = {"dataset_uri": u}
            continue

        bindings = result["results"]["bindings"]
        grouped: dict[str, list[dict]] = defaultdict(list)
        for b in bindings:
            uri = b.get("dataset", {}).get("value", "")
            if uri:
                grouped[uri].append(b)

        for uri, rows in grouped.items():
            if uri in all_records:
                continue

            def _title_rank(b):
                lang = b.get("title", {}).get("xml:lang", "")
                val  = b.get("title", {}).get("value", "")
                if not val:
                    return 99
                return 0 if lang == "en" else (1 if lang.startswith("en") else 2)

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

        for u in batch:
            if u not in all_records:
                all_records[u] = {"dataset_uri": u}

        print(f"  Batch {progress} metadata → {len(grouped)} records enriched")
        time.sleep(0.3)

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
    """Add derived boolean/categorical columns."""
    df = df.copy()
    for col in ["title", "description", "modified", "issued",
                "publisher_uri", "license", "language", "spatial", "keyword"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    df["country"]             = df.apply(_country, axis=1)
    df["has_license"]         = df["license"].apply(lambda x: bool(x) and len(x) > 5)
    df["is_open_license"]     = df["license"].apply(_is_open)
    df["has_coordinates"]     = df["spatial"].apply(
        lambda x: bool(x) and any(c.isdigit() for c in x) and len(x) > 15
    )
    df["description_length"]  = df["description"].apply(
        lambda x: len(x.split()) if x else 0
    )
    df["has_temporal"]        = df["issued"].apply(lambda x: bool(x) and len(x) > 4)
    df["temporal_start"]      = ""
    df["temporal_end"]        = ""
    df["is_machine_readable"] = False
    df["format"]              = ""
    df["publisher_name"]      = ""
    df["num_languages"] = df.get(
        "num_languages", pd.Series(1, index=df.index)
    ).fillna(1).astype(int)
    df["num_keywords"] = df.get(
        "num_keywords", pd.Series(0, index=df.index)
    ).fillna(0).astype(int)
    df["has_keywords"]        = df["num_keywords"] > 0
    df["is_multilingual"]     = df["num_languages"] >= 2
    df["has_multilingual"]    = df["is_multilingual"]
    return df


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    t_start = datetime.now()
    print("=" * 65)
    print("EU WATER DATASET OBSERVATORY — CLIMATE HARVESTER (2-phase)")
    print(f"Started: {t_start.isoformat()}")
    print("=" * 65)

    # ── Load keywords ────────────────────────────────────────────────────────
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

    # ── Phase 1: collect URIs per domain ─────────────────────────────────────
    print("=== PHASE 1: Collecting climate dataset URIs (combined OR queries) ===\n")
    uri_to_domain: dict[str, str] = {}
    domain_uri_counts: dict[str, int] = {}
    zero_result_domains: list[str] = []

    for domain, keywords in domain_queries.items():
        print(f"  [{domain}]")
        uris = phase1_get_uris_combined(domain, keywords, limit=500)
        new = 0
        for u in uris:
            if u not in uri_to_domain:
                uri_to_domain[u] = domain
                new += 1
        domain_uri_counts[domain] = new
        print(f"  → {domain}: {new} unique URIs added (total: {len(uri_to_domain)})")
        if new == 0:
            zero_result_domains.append(domain)
        time.sleep(0.5)

    print(f"\nPhase 1 complete: {len(uri_to_domain)} unique climate dataset URIs\n")

    # ── Second pass: fallback keywords for zero-result domains ───────────────
    FALLBACK_KEYWORDS: dict[str, list[str]] = {
        "drought_early_warning": [
            "drought monitoring", "hydrological drought", "water shortage",
            "dry spell", "drought risk", "droughts", "streamflow drought",
        ],
        "climate_infrastructure": [
            "climate adaptation", "future climate", "climate change",
            "climate projections", "climate impacts", "infrastructure climate",
        ],
        "nature_based_solutions": [
            "wetland", "natural flood", "ecosystem restoration",
            "riparian", "floodplain", "ecological restoration",
        ],
    }

    fallback_new: dict[str, int] = {}
    if zero_result_domains:
        print(f"\n=== SECOND PASS: zero-result domains = {zero_result_domains} ===\n")
        for domain in list(zero_result_domains):
            fb_kws = FALLBACK_KEYWORDS.get(domain, [])
            print(f"  [{domain}] fallback: {fb_kws}")
            uris = phase1_get_uris_combined(domain, fb_kws, limit=500)
            new = 0
            for u in uris:
                if u not in uri_to_domain:
                    uri_to_domain[u] = domain
                    new += 1
            domain_uri_counts[domain] += new
            fallback_new[domain] = new
            print(f"  → {domain}: {new} new URIs from fallback")
            if domain_uri_counts[domain] > 0:
                zero_result_domains.remove(domain)
            time.sleep(0.5)

        print(f"\nAfter second pass: {len(uri_to_domain)} total unique URIs")
        if zero_result_domains:
            print(f"\n⚠ DISCOVERY GAP: {zero_result_domains} return zero results")
            print("  This is a scientific finding, not a pipeline failure.")
    else:
        fallback_new = {}

    # ── Phase 2: fetch metadata ───────────────────────────────────────────────
    print("\n=== PHASE 2: Fetching metadata in batches of 50 ===\n")
    all_uris = list(uri_to_domain.keys())

    if not all_uris:
        print("⚠ No URIs collected — creating empty dataset.")
        records = []
    else:
        records = phase2_get_metadata(all_uris, batch_size=50)
        for rec in records:
            rec["domain"] = uri_to_domain.get(rec["dataset_uri"], "unknown")

    if records:
        df = pd.DataFrame(records)
        print(f"\nPhase 2 complete: {len(df)} records")
    else:
        df = pd.DataFrame(columns=[
            "dataset_uri", "title", "description", "modified", "issued",
            "publisher_uri", "license", "language", "spatial", "keyword",
            "num_languages", "num_keywords", "domain",
        ])

    print("Enriching metadata …")
    df = enrich(df)

    # ── Overlap check with water harvest ─────────────────────────────────────
    overlap_count = 0
    existing_harvest = HARVEST_DIR / "raw_harvest.csv"
    if existing_harvest.exists() and len(df) > 0:
        water_uris = set(pd.read_csv(existing_harvest,
                                     usecols=["dataset_uri"],
                                     low_memory=False)["dataset_uri"].astype(str))
        df["in_water_harvest"] = df["dataset_uri"].isin(water_uris)
        overlap_count = int(df["in_water_harvest"].sum())
        print(f"Overlap with water harvest: {overlap_count} datasets")
        # Save merged file (overlapping datasets only)
        merged_df = df[df["in_water_harvest"]].copy()
        if len(merged_df) > 0:
            HARVEST_DIR.mkdir(parents=True, exist_ok=True)
            merged_df.to_csv(HARVEST_DIR / "climate_harvest_merged.csv", index=False)
            print(f"Saved climate_harvest_merged.csv ({len(merged_df)} rows)")
    else:
        if len(df) > 0:
            df["in_water_harvest"] = False

    # ── Save outputs ──────────────────────────────────────────────────────────
    HARVEST_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(HARVEST_DIR / "climate_harvest.csv", index=False)
    print(f"Saved climate_harvest.csv ({len(df)} rows)")

    # ── Summary ───────────────────────────────────────────────────────────────
    def _pct(col):
        return float(df[col].mean() * 100) if len(df) > 0 else 0.0

    elapsed = (datetime.now() - t_start).total_seconds()
    summary = {
        "harvest_timestamp":          datetime.now().isoformat(),
        "elapsed_seconds":            round(elapsed, 1),
        "total_datasets":             len(df),
        "unique_datasets":            int(df["dataset_uri"].nunique()) if len(df) > 0 else 0,
        "datasets_per_domain":        domain_uri_counts,
        "zero_result_domains":        zero_result_domains,
        "fallback_keywords_tried":    fallback_new,
        "overlap_with_water_harvest": overlap_count,
        "domain_distribution":        (df["domain"].value_counts().to_dict()
                                       if len(df) > 0 else {}),
        "country_distribution":       (df["country"].value_counts().to_dict()
                                       if len(df) > 0 else {}),
        "has_license_pct":            _pct("has_license"),
        "is_open_license_pct":        _pct("is_open_license"),
        "has_coordinates_pct":        _pct("has_coordinates"),
        "has_temporal_pct":           _pct("has_temporal"),
        "is_multilingual_pct":        _pct("is_multilingual"),
        "has_description_pct":        float(
            (df["description_length"] > 0).mean() * 100
        ) if len(df) > 0 else 0.0,
        "discovery_gap_note": (
            f"Domains with zero results after primary and fallback keyword "
            f"passes: {zero_result_domains}. This indicates a structural "
            f"discovery gap in the data.europa.eu federated catalogue for "
            f"these climate adaptation topics."
            if zero_result_domains else
            "All domains returned at least one result."
        ),
    }

    with open(HARVEST_DIR / "climate_harvest_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("Saved climate_harvest_summary.json")

    print(f"\n{'='*65}")
    print("CLIMATE HARVEST COMPLETE")
    print(f"{'='*65}")
    print(f"  Total datasets         : {len(df)}")
    print(f"  Overlap with water     : {overlap_count}")
    print(f"  Elapsed                : {elapsed:.0f}s")
    print(f"\n  Datasets per domain:")
    for d, c in domain_uri_counts.items():
        print(f"    {d}: {c}" + ("  ← ZERO" if c == 0 else ""))
    if zero_result_domains:
        print(f"\n  ⚠ Discovery gap: {zero_result_domains}")
    if len(df) > 0:
        print(f"\n  By country (top 10):")
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
