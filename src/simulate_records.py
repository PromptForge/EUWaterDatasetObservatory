"""
Simulate 3,500 EU water dataset metadata records.

Calibrated to known EU data characteristics from:
- data.europa.eu category distributions
- Open Data Maturity scores by country
- INSPIRE metadata conformance studies
- EEA Datahub vs national portal comparisons
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

# Load configuration
CONFIG_DIR = Path(__file__).parent.parent / "config"

def load_config():
    with open(CONFIG_DIR / "simulation_params.json") as f:
        params = json.load(f)
    with open(CONFIG_DIR / "keywords.json") as f:
        keywords = json.load(f)
    return params, keywords

def generate_dataset_id():
    return f"eu-water-{uuid.uuid4().hex[:12]}"

def weighted_choice(options_dict):
    """Select from dictionary with weights as values."""
    options = list(options_dict.keys())
    weights = list(options_dict.values())
    return random.choices(options, weights=weights, k=1)[0]

def generate_temporal_coverage():
    """Generate realistic temporal coverage."""
    end_year = random.randint(2020, 2025)
    span = random.choice([1, 2, 3, 5, 6, 10, 15, 20])
    start_year = end_year - span
    return f"{start_year}-01-01", f"{end_year}-12-31", span

def generate_description(domain, has_description, keywords):
    """Generate realistic description text."""
    if not has_description or random.random() > 0.85:
        return ""
    
    domain_kw = keywords.get(domain.lower().replace(" ", "_"), {}).get("en", [])
    if not domain_kw:
        domain_kw = ["water", "data", "monitoring"]
    
    templates = [
        f"This dataset contains {random.choice(domain_kw)} data collected from monitoring stations across the region.",
        f"Comprehensive {random.choice(domain_kw)} information compiled under EU reporting requirements.",
        f"Time series data for {random.choice(domain_kw)} parameters measured at multiple locations.",
        f"Spatial dataset representing {random.choice(domain_kw)} conditions based on field surveys and modeling.",
        f"Aggregated statistics on {random.choice(domain_kw)} derived from national reporting systems.",
    ]
    
    base = random.choice(templates)
    
    # Add more detail for longer descriptions
    if random.random() > 0.5:
        extras = [
            " Data quality has been validated according to standard procedures.",
            " Updated regularly as part of ongoing monitoring programs.",
            " Covers all relevant water bodies within the jurisdiction.",
            " Includes both raw measurements and derived indicators.",
            " Harmonized with EU-level reporting standards.",
        ]
        base += random.choice(extras)
    
    return base

def generate_single_record(params, keywords, record_num):
    """Generate a single metadata record."""
    
    # Core attributes
    domain = weighted_choice(params["domain_distribution"])
    country = weighted_choice(params["country_weights"])
    publisher_type = weighted_choice(params["publisher_types"])
    
    # Get completeness profile
    profile = params["completeness_profiles"].get(publisher_type, params["completeness_profiles"]["other"])
    
    # Generate based on probabilities
    has_description = random.random() < profile["has_description"]
    has_temporal = random.random() < profile["has_temporal"]
    has_spatial = random.random() < profile["has_spatial"]
    has_format = random.random() < profile["has_format"]
    has_license = random.random() < profile["has_license"]
    has_keywords = random.random() < profile["has_keywords"]
    has_eurovoc = random.random() < profile["has_eurovoc"]
    
    # Temporal coverage
    if has_temporal:
        start_date, end_date, temporal_span = generate_temporal_coverage()
    else:
        start_date, end_date, temporal_span = "", "", 0
    
    # Format
    format_type = weighted_choice(params["format_distribution"]) if has_format else ""
    
    # Update frequency
    update_freq = weighted_choice(params["temporal_characteristics"]["update_frequency_distribution"])
    
    # Last modified (recent vs stale)
    if random.random() < params["temporal_characteristics"]["recent_probability"]:
        days_ago = random.randint(1, 90)
    else:
        days_ago = random.randint(91, 1500)
    last_modified = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    
    # Spatial precision
    if has_spatial:
        spatial_precision = random.choice(["coordinates", "bbox", "region_name", "country_level"])
        has_coordinates = spatial_precision in ["coordinates", "bbox"]
    else:
        spatial_precision = ""
        has_coordinates = False
    
    # License
    if has_license:
        license_type = random.choice(["CC-BY-4.0", "CC-BY-SA-4.0", "CC0", "EU-ODP", "custom_open", "restricted"])
        is_open_license = license_type in ["CC-BY-4.0", "CC-BY-SA-4.0", "CC0", "EU-ODP", "custom_open"]
    else:
        license_type = ""
        is_open_license = False
    
    # Description
    description = generate_description(domain, has_description, keywords)
    description_length = len(description.split())
    
    # Keywords and EuroVoc
    domain_keywords = keywords.get(domain.lower().replace(" ", "_"), {}).get("en", [])
    num_keywords = random.randint(0, 5) if has_keywords else 0
    selected_keywords = random.sample(domain_keywords, min(num_keywords, len(domain_keywords))) if domain_keywords else []
    
    # Multilingual (EU agencies more likely)
    has_multilingual = random.random() < (0.6 if publisher_type == "eu_agency" else 0.2)
    num_languages = random.randint(2, 5) if has_multilingual else 1
    
    # Publisher name
    publisher_names = {
        "eu_agency": ["European Environment Agency", "Joint Research Centre", "Eurostat", "Copernicus"],
        "national_hydro": [f"{country} Hydrological Service", f"{country} Water Agency", f"{country} Environmental Agency"],
        "ministry": [f"{country} Ministry of Environment", f"{country} Ministry of Agriculture"],
        "regional": [f"{country} Regional Water Authority", f"{country} River Basin District"],
        "research": [f"{country} Research Institute", f"University of {country}"],
        "other": [f"{country} Data Provider", f"{country} Open Data Portal"]
    }
    publisher = random.choice(publisher_names.get(publisher_type, publisher_names["other"]))
    
    # Dataset title
    domain_titles = {
        "water_quality": ["Water Quality Monitoring Data", "Surface Water Chemistry", "Biological Quality Elements"],
        "groundwater": ["Groundwater Level Measurements", "Aquifer Characteristics", "Groundwater Body Status"],
        "floods": ["Flood Hazard Maps", "Flood Risk Assessment", "Historical Flood Events"],
        "utilities": ["Water Supply Statistics", "Wastewater Treatment Data", "Drinking Water Quality"],
        "wfd_metrics": ["WFD Status Assessment", "Programme of Measures", "Pressures and Impacts"],
        "agricultural_runoff": ["Nitrate Concentrations", "Pesticide Monitoring", "Agricultural Pollution"],
        "cross_cutting": ["Remote Sensing Water Data", "Climate Water Projections", "Integrated Water Resources"]
    }
    title_base = random.choice(domain_titles.get(domain, ["Water Dataset"]))
    title = f"{title_base} - {country}" if country != "EU" else f"European {title_base}"
    
    return {
        "dataset_id": generate_dataset_id(),
        "title": title,
        "description": description,
        "description_length": description_length,
        "domain": domain,
        "country": country,
        "publisher": publisher,
        "publisher_type": publisher_type,
        "format": format_type,
        "temporal_start": start_date,
        "temporal_end": end_date,
        "temporal_span_years": temporal_span,
        "last_modified": last_modified,
        "update_frequency": update_freq,
        "spatial_precision": spatial_precision,
        "has_coordinates": has_coordinates,
        "license": license_type,
        "is_open_license": is_open_license,
        "num_keywords": num_keywords,
        "keywords": "|".join(selected_keywords),
        "has_eurovoc": has_eurovoc,
        "num_languages": num_languages,
        "has_multilingual": has_multilingual
    }

def simulate_all_records(n_records=3500):
    """Generate all simulated records."""
    params, keywords = load_config()
    
    records = []
    for i in range(n_records):
        record = generate_single_record(params, keywords, i)
        records.append(record)
        
        if (i + 1) % 500 == 0:
            print(f"Generated {i + 1}/{n_records} records...")
    
    df = pd.DataFrame(records)
    return df

def main():
    print("Simulating 3,500 EU water dataset metadata records...")
    df = simulate_all_records(3500)
    
    # Save to CSV
    output_path = Path(__file__).parent.parent / "data" / "simulated" / "metadata_records.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    
    print(f"\nSaved {len(df)} records to {output_path}")
    
    # Print summary statistics
    print("\n=== SIMULATION SUMMARY ===")
    print(f"\nRecords by domain:")
    print(df["domain"].value_counts())
    print(f"\nRecords by country (top 10):")
    print(df["country"].value_counts().head(10))
    print(f"\nRecords by publisher type:")
    print(df["publisher_type"].value_counts())
    print(f"\nFormat distribution:")
    print(df["format"].value_counts())
    print(f"\nCompleteness rates:")
    print(f"  Has description: {(df['description_length'] > 0).mean():.1%}")
    print(f"  Has temporal: {(df['temporal_span_years'] > 0).mean():.1%}")
    print(f"  Has coordinates: {df['has_coordinates'].mean():.1%}")
    print(f"  Has open license: {df['is_open_license'].mean():.1%}")
    print(f"  Has EuroVoc: {df['has_eurovoc'].mean():.1%}")
    print(f"  Has multilingual: {df['has_multilingual'].mean():.1%}")
    
    return df

if __name__ == "__main__":
    main()
