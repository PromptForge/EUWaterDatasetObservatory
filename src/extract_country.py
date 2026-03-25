#!/usr/bin/env python3
"""
Extract country from EU water dataset metadata.
Uses multiple strategies: publisher URI domain, spatial field patterns,
language code inference, dataset URI patterns, and keyword/title hints.
"""

import pandas as pd
import re
from pathlib import Path

# EU country codes (ISO 3166-1 alpha-2)
EU_COUNTRIES = {
    'AT': 'Austria', 'BE': 'Belgium', 'BG': 'Bulgaria', 'HR': 'Croatia',
    'CY': 'Cyprus', 'CZ': 'Czechia', 'DK': 'Denmark', 'EE': 'Estonia',
    'FI': 'Finland', 'FR': 'France', 'DE': 'Germany', 'GR': 'Greece',
    'HU': 'Hungary', 'IE': 'Ireland', 'IT': 'Italy', 'LV': 'Latvia',
    'LT': 'Lithuania', 'LU': 'Luxembourg', 'MT': 'Malta', 'NL': 'Netherlands',
    'PL': 'Poland', 'PT': 'Portugal', 'RO': 'Romania', 'SK': 'Slovakia',
    'SI': 'Slovenia', 'ES': 'Spain', 'SE': 'Sweden'
}

# Publisher URI domain → country mapping (highest confidence)
PUBLISHER_DOMAIN_MAP = {
    # Ireland
    'data.gov.ie': 'IE',
    # Belgium
    'belgif.be': 'BE',
    # France
    'data.gouv.fr': 'FR',
    # Germany (various states/agencies)
    'opendata.schleswig-holstein.de': 'DE',
    'opendata.sachsen.de': 'DE',
    'opendata.sachsen-anhalt.de': 'DE',
    'opendata-hro.de': 'DE',          # Rostock
    'opendata.thueringen.de': 'DE',
    'opendata.nrw.de': 'DE',
    'geodaten.sachsen.de': 'DE',
    'dcat-ap.de': 'DE',
    'gdi-de.org': 'DE',
    'bkg.bund.de': 'DE',
    # Czech Republic
    'egon.gov.cz': 'CZ',
    'cuzk.cz': 'CZ',
    'linked.cuzk.cz': 'CZ',
    'rpp-opendata.egon.gov.cz': 'CZ',
    # Netherlands
    'overheid.nl': 'NL',
    'standaarden.overheid.nl': 'NL',
    'pdok.nl': 'NL',
    # Spain
    'datos.gob.es': 'ES',
    'idee.es': 'ES',
    'ide.cat': 'ES',          # Catalonia
    # Croatia
    'hrvatska-vode.hr': 'HR',
    'inspire.hr': 'HR',
    # Austria
    'data.gv.at': 'AT',
    'kagis.at': 'AT',
    'umweltbundesamt.at': 'AT',
    # Finland
    'syke.fi': 'FI',
    'ymparisto.fi': 'FI',
    # Italy
    'isprambiente.gov.it': 'IT',
    'pcn.minambiente.it': 'IT',
    # Sweden
    'lansstyrelsen.se': 'SE',
    'smhi.se': 'SE',
    # Portugal
    'snirh.apambiente.pt': 'PT',
    'apambiente.pt': 'PT',
    # Poland
    'kzgw.gov.pl': 'PL',
    'isok.gov.pl': 'PL',
    # Romania
    'rowater.ro': 'RO',
    # Denmark
    'miljoeportal.dk': 'DK',
    # Slovakia
    'shmu.sk': 'SK',
    # Slovenia
    'arso.gov.si': 'SI',
    # Hungary
    'vizugy.hu': 'HU',
    # Bulgaria
    'eea.government.bg': 'BG',
    # Estonia
    'keskkonnaagentuur.ee': 'EE',
    # Latvia
    'lvgmc.lv': 'LV',
    # Lithuania
    'aplinka.lt': 'LT',
    # Luxembourg
    'geoportal.lu': 'LU',
    # EU/multi (exclude from country-specific)
    'publications.europa.eu': 'EU',
    'europa.eu': 'EU',
}

# Language URI suffix → country (lower confidence, used as fallback)
LANGUAGE_COUNTRY_MAP = {
    'DEU': 'DE',    # German → most likely Germany (also AT, LU, CH)
    'NLD': 'NL',    # Dutch → most likely Netherlands (also BE)
    'FRE': 'FR',    # French (old code) → most likely France (also BE, LU)
    'FRA': 'FR',    # French → most likely France
    'ITA': 'IT',    # Italian
    'SPA': 'ES',    # Spanish
    'HRV': 'HR',    # Croatian
    'FIN': 'FI',    # Finnish
    'SWE': 'SE',    # Swedish
    'POL': 'PL',    # Polish
    'POR': 'PT',    # Portuguese
    'PRT': 'PT',    # Portuguese
    'HUN': 'HU',    # Hungarian
    'RUM': 'RO',    # Romanian
    'RON': 'RO',    # Romanian
    'BUL': 'BG',    # Bulgarian
    'SLV': 'SI',    # Slovenian
    'SLO': 'SI',    # Slovenian (alt code)
    'SLK': 'SK',    # Slovak
    'CES': 'CZ',    # Czech
    'CZE': 'CZ',    # Czech (B code)
    'EST': 'EE',    # Estonian
    'LAV': 'LV',    # Latvian
    'LIT': 'LT',    # Lithuanian
    'GLE': 'IE',    # Irish
    'MLT': 'MT',    # Maltese
    'DAN': 'DK',    # Danish
    'ELL': 'GR',    # Greek
    'BAQ': 'ES',    # Basque
    'EUS': 'ES',    # Basque
    'CAT': 'ES',    # Catalan
    'GLC': 'ES',    # Galician
    'GLG': 'ES',    # Galician
}

# GeoNames region → country mapping (for spatial field)
GEONAMES_REGION_COUNTRY = {
    # Belgium
    '2803139': 'BE',  # Ghent
    '3337388': 'BE',  # Walloon Region
    '2800867': 'BE',  # Brussels
    '2802361': 'BE',  # Belgium
    '3337387': 'BE',  # Flemish Region
    # Netherlands
    '2750405': 'NL',  # Netherlands
    '2745912': 'NL',  # Utrecht
    '2745022': 'NL',  # Zeeland
    '2749879': 'NL',  # South Holland
    '2756113': 'NL',  # Drenthe
    # Germany
    '2921044': 'DE',  # Germany
    '2838632': 'DE',  # Schleswig-Holstein
    '2842565': 'DE',  # Saxony
    # Czech Republic
    '3077311': 'CZ',  # Pilsen
    '3067696': 'CZ',  # Plzeňský kraj
    # Spain
    '2510769': 'ES',  # Spain
    # France
    '3017382': 'FR',  # France
    # Italy
    '3175395': 'IT',  # Italy
    # Austria
    '2782113': 'AT',  # Austria
    # Croatia
    '3202326': 'HR',  # Croatia
}

# Country name/term → country code (for keyword/title matching)
COUNTRY_NAME_TERMS = {
    'ireland': 'IE', 'irish': 'IE',
    'belgium': 'BE', 'belgian': 'BE', 'belgique': 'BE', 'belgie': 'BE',
    'france': 'FR', 'french': 'FR', 'française': 'FR',
    'germany': 'DE', 'german': 'DE', 'deutschland': 'DE', 'deutsche': 'DE',
    'netherlands': 'NL', 'dutch': 'NL', 'nederland': 'NL',
    'austria': 'AT', 'österreich': 'AT', 'austrian': 'AT',
    'spain': 'ES', 'spanish': 'ES', 'españa': 'ES', 'espana': 'ES',
    'italy': 'IT', 'italian': 'IT', 'italia': 'IT', 'italiano': 'IT',
    'croatia': 'HR', 'hrvatska': 'HR', 'croatian': 'HR',
    'czechia': 'CZ', 'czech': 'CZ', 'plzeň': 'CZ', 'plzen': 'CZ',
    'finland': 'FI', 'finnish': 'FI', 'suomi': 'FI',
    'sweden': 'SE', 'swedish': 'SE', 'sverige': 'SE',
    'poland': 'PL', 'polish': 'PL', 'polska': 'PL',
    'portugal': 'PT', 'portuguese': 'PT',
    'hungary': 'HU', 'hungarian': 'HU', 'magyarország': 'HU',
    'romania': 'RO', 'romanian': 'RO', 'românia': 'RO',
    'bulgaria': 'BG', 'bulgarian': 'BG',
    'slovenia': 'SI', 'slovenian': 'SI', 'slovenija': 'SI',
    'slovakia': 'SK', 'slovak': 'SK', 'slovensko': 'SK',
    'estonia': 'EE', 'estonian': 'EE', 'eesti': 'EE',
    'latvia': 'LV', 'latvian': 'LV', 'latvija': 'LV',
    'lithuania': 'LT', 'lithuanian': 'LT', 'lietuva': 'LT',
    'luxembourg': 'LU', 'luxemburg': 'LU',
    'malta': 'MT', 'maltese': 'MT',
    'denmark': 'DK', 'danish': 'DK', 'danmark': 'DK',
    'greece': 'GR', 'greek': 'GR', 'hellas': 'GR',
    'cyprus': 'CY', 'cypriot': 'CY',
    # German state hints → DE
    'sachsen': 'DE', 'saarland': 'DE', 'thüringen': 'DE', 'thuringen': 'DE',
    'hessen': 'DE', 'hessian': 'DE', 'bayerisch': 'DE', 'bavarian': 'DE',
    'hamburg': 'DE', 'berlin': 'DE', 'chemnitz': 'DE', 'köln': 'DE',
    'köl': 'DE', 'trier': 'DE', 'nrw': 'DE', 'rheinland': 'DE',
    'rhein': 'DE', 'niedersachsen': 'DE', 'braunschweig': 'DE',
    'freiburg': 'DE', 'magdeburg': 'DE', 'rostock': 'DE',
    # Austrian state hints → AT
    'kärnten': 'AT', 'carinthia': 'AT', 'steiermark': 'AT',
    'styria': 'AT', 'tirol': 'AT', 'tyrol': 'AT', 'salzburg': 'AT',
    'oberösterreich': 'AT', 'vorarlberg': 'AT', 'niederösterreich': 'AT',
    # French region hints → FR
    'normandie': 'FR', 'normandy': 'FR', 'bretagne': 'FR', 'brittany': 'FR',
    'languedoc': 'FR', 'aquitaine': 'FR', 'occitanie': 'FR',
    'alsace': 'FR', 'guyane': 'FR', 'guadeloupe': 'FR', 'martinique': 'FR',
    # Italian region hints → IT
    'lombardia': 'IT', 'piemonte': 'IT', 'veneto': 'IT', 'toscana': 'IT',
    'calabria': 'IT', 'sardegna': 'IT', 'sicilia': 'IT',
    # Spanish region hints → ES
    'cataluña': 'ES', 'catalonia': 'ES', 'andalucia': 'ES', 'andalucía': 'ES',
    'navarra': 'ES', 'navarre': 'ES', 'valenciana': 'ES', 'galicia': 'ES',
    'pamplona': 'ES', 'aragón': 'ES',
}


def extract_from_publisher_uri(publisher_uri: str) -> str:
    """Extract country from publisher URI domain."""
    if not publisher_uri or pd.isna(publisher_uri):
        return None
    uri = publisher_uri.lower().strip()
    
    # Skip blank nodes and urns (no domain to extract)
    if uri.startswith('nodeid://') or uri.startswith('urn:'):
        return None
    
    # Check each domain pattern
    for domain, country in PUBLISHER_DOMAIN_MAP.items():
        if domain in uri:
            if country == 'EU':
                return None  # EU-wide publisher, not country-specific
            return country
    
    # Try to extract TLD for EU country TLDs
    tld_match = re.search(r'\.([a-z]{2})(?:/|$)', uri)
    if tld_match:
        tld = tld_match.group(1).upper()
        # Map TLDs to EU country codes
        tld_to_country = {
            'IE': 'IE', 'BE': 'BE', 'FR': 'FR', 'DE': 'DE',
            'NL': 'NL', 'AT': 'AT', 'ES': 'ES', 'IT': 'IT',
            'HR': 'HR', 'CZ': 'CZ', 'FI': 'FI', 'SE': 'SE',
            'PL': 'PL', 'PT': 'PT', 'HU': 'HU', 'RO': 'RO',
            'BG': 'BG', 'SI': 'SI', 'SK': 'SK', 'EE': 'EE',
            'LV': 'LV', 'LT': 'LT', 'LU': 'LU', 'MT': 'MT',
            'DK': 'DK', 'GR': 'GR', 'CY': 'CY',
        }
        if tld in tld_to_country:
            return tld_to_country[tld]
    
    return None


def extract_from_spatial(spatial: str) -> str:
    """Extract country from spatial/geographic field."""
    if not spatial or pd.isna(spatial):
        return None
    
    # GeoNames URL patterns
    gn_match = re.search(r'geonames\.org/(\d+)', spatial)
    if gn_match:
        gn_id = gn_match.group(1)
        if gn_id in GEONAMES_REGION_COUNTRY:
            return GEONAMES_REGION_COUNTRY[gn_id]
    
    # INSPIRE/EU country authority URIs
    country_match = re.search(r'country/([A-Z]{2,3})\b', spatial, re.IGNORECASE)
    if country_match:
        code = country_match.group(1).upper()
        iso3_to_iso2 = {
            'AUT': 'AT', 'BEL': 'BE', 'BGR': 'BG', 'HRV': 'HR', 'CYP': 'CY',
            'CZE': 'CZ', 'DNK': 'DK', 'EST': 'EE', 'FIN': 'FI', 'FRA': 'FR',
            'DEU': 'DE', 'GRC': 'GR', 'HUN': 'HU', 'IRL': 'IE', 'ITA': 'IT',
            'LVA': 'LV', 'LTU': 'LT', 'LUX': 'LU', 'MLT': 'MT', 'NLD': 'NL',
            'POL': 'PL', 'PRT': 'PT', 'ROU': 'RO', 'SVK': 'SK', 'SVN': 'SI',
            'ESP': 'ES', 'SWE': 'SE'
        }
        if code in iso3_to_iso2:
            return iso3_to_iso2[code]
        if code in EU_COUNTRIES:
            return code
    
    # German dcat-ap.de political geocoding
    if 'dcat-ap.de/def/politicalGeocoding' in spatial:
        return 'DE'
    
    # Czech RUIAN linked data
    if 'linked.cuzk.cz' in spatial:
        return 'CZ'
    
    # Austrian state names
    austrian_states = ['kärnten', 'steiermark', 'tirol', 'salzburg', 'vorarlberg',
                       'oberösterreich', 'niederösterreich', 'burgenland', 'wien']
    spatial_lower = spatial.lower()
    for state in austrian_states:
        if state in spatial_lower:
            return 'AT'
    
    return None


def extract_from_language(language_raw: str) -> str:
    """Extract likely country from language code (lower confidence)."""
    if not language_raw or pd.isna(language_raw):
        return None
    
    # Extract language code from URI like http://publications.europa.eu/resource/authority/language/DEU
    lang_match = re.search(r'/language/([A-Z]{2,3})(?:\s|$|\|)', language_raw)
    if lang_match:
        lang_code = lang_match.group(1).upper()
        return LANGUAGE_COUNTRY_MAP.get(lang_code)
    
    # Handle pipe-separated multiple languages
    codes = re.findall(r'/language/([A-Z]{2,3})', language_raw)
    if codes:
        # Use first non-English code as more distinctive
        for code in codes:
            if code != 'ENG' and code in LANGUAGE_COUNTRY_MAP:
                return LANGUAGE_COUNTRY_MAP[code]
        if 'ENG' in codes:
            return None  # English alone is ambiguous
    
    return None


def extract_from_text_fields(text: str) -> str:
    """Extract country from text (title, description, keywords)."""
    if not text or pd.isna(text):
        return None
    
    text_lower = text.lower()
    
    # Check country name terms (longer strings first to avoid false matches)
    for term, country in sorted(COUNTRY_NAME_TERMS.items(), key=lambda x: -len(x[0])):
        if term in text_lower:
            return country
    
    return None


def extract_country(row: pd.Series, has_language_col: bool = False) -> tuple:
    """
    Extract country from a dataset row, trying multiple fields in priority order.
    Returns (country_code, method_used)
    """
    
    # Strategy 1: publisher_uri (highest confidence)
    publisher_col = 'publisher_uri' if 'publisher_uri' in row.index else 'publisher'
    country = extract_from_publisher_uri(row.get(publisher_col, ''))
    if country:
        return country, 'publisher_uri'
    
    # Strategy 2: spatial field
    country = extract_from_spatial(row.get('spatial', ''))
    if country:
        return country, 'spatial'
    
    # Strategy 3: dataset_uri (can contain country TLD)
    dataset_uri = str(row.get('dataset_id', row.get('dataset_uri', '')))
    country = extract_from_publisher_uri(dataset_uri)
    if country:
        return country, 'dataset_uri'
    
    # Strategy 4: keyword/title hints
    keyword_col = 'keyword_raw' if 'keyword_raw' in row.index else 'keyword'
    country = extract_from_text_fields(str(row.get(keyword_col, '')))
    if country:
        return country, 'keywords'
    
    country = extract_from_text_fields(str(row.get('title', '')))
    if country:
        return country, 'title'
    
    # Strategy 5: language code (lowest confidence, only use if unique/non-English)
    lang_col = 'language_raw' if 'language_raw' in row.index else 'language'
    country = extract_from_language(str(row.get(lang_col, '')))
    if country:
        return country, 'language'
    
    return 'unknown', 'none'


def main():
    ROOT = Path(__file__).parent.parent
    OUTPUT_DIR = ROOT / 'data' / 'outputs_real'
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load sufficiency scores (primary source - already has scores)
    scores_path = OUTPUT_DIR / 'sufficiency_scores.csv'
    df = pd.read_csv(scores_path)
    print(f"Loaded {len(df)} records from sufficiency_scores.csv")
    
    # Also load raw harvest to get publisher_uri and spatial
    harvest_path = ROOT / 'data' / 'harvested' / 'raw_harvest.csv'
    df_harvest = pd.read_csv(harvest_path)
    print(f"Loaded {len(df_harvest)} records from raw_harvest.csv")
    
    # Merge to get publisher_uri and spatial from harvest
    df_harvest_slim = df_harvest[['dataset_uri', 'publisher_uri', 'spatial', 'keyword']].copy()
    df_harvest_slim = df_harvest_slim.rename(columns={'dataset_uri': 'dataset_id'})
    
    df_merged = df.merge(df_harvest_slim, on='dataset_id', how='left', suffixes=('', '_harvest'))
    print(f"Merged dataset: {len(df_merged)} records")
    
    # Extract countries
    results = df_merged.apply(extract_country, axis=1)
    df_merged['country_extracted'] = [r[0] for r in results]
    df_merged['extraction_method'] = [r[1] for r in results]
    
    # Report results
    total = len(df_merged)
    identified = len(df_merged[df_merged['country_extracted'] != 'unknown'])
    
    print(f"\n=== Country Extraction Results ===")
    print(f"Total records: {total}")
    print(f"Identified: {identified} ({100*identified/total:.1f}%)")
    print(f"Unknown: {total - identified}")
    
    print(f"\n--- Country Distribution ---")
    country_counts = df_merged[df_merged['country_extracted'] != 'unknown']['country_extracted'].value_counts()
    print(country_counts.to_string())
    
    print(f"\n--- Extraction Methods ---")
    print(df_merged['extraction_method'].value_counts().to_string())
    
    # Validate against existing country column where not 'unknown'
    known_mask = df_merged['country'].notna() & (df_merged['country'] != 'unknown')
    if known_mask.sum() > 0:
        agree = (df_merged.loc[known_mask, 'country'] == df_merged.loc[known_mask, 'country_extracted']).sum()
        print(f"\n--- Validation vs existing country field ---")
        print(f"Pre-labelled records: {known_mask.sum()}, Agreement: {agree}/{known_mask.sum()}")
    
    # Save
    output_path = OUTPUT_DIR / 'datasets_with_country.csv'
    df_merged.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")
    
    return df_merged


if __name__ == '__main__':
    main()
