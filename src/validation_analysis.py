"""
EU Water Dataset Observatory - Validation Analysis
Analyzes manual validation checklist to calculate metadata accuracy rates
"""

import pandas as pd
import json
from pathlib import Path


def calculate_agreement_rate(series):
    """
    Calculate agreement rate for a validation category.
    Agreement rate = (Yes_count + 0.5 * Partial_count) / Total
    """
    counts = series.value_counts()
    yes_count = counts.get('Yes', 0)
    no_count = counts.get('No', 0)
    partial_count = counts.get('Partial', 0)
    
    total = yes_count + no_count + partial_count
    if total == 0:
        return 0.0
    
    agreement = (yes_count + 0.5 * partial_count) / total
    
    return {
        'yes': int(yes_count),
        'no': int(no_count),
        'partial': int(partial_count),
        'total': int(total),
        'agreement_rate': round(agreement, 4)
    }


def analyze_validation_data():
    """
    Main analysis function for validation checklist.
    """
    # Load the Excel file
    df = pd.read_excel('Validation_Checklist.xlsx', sheet_name='Validation Checklist')
    
    # Column names have newlines - normalize them
    df.columns = [col.replace('\n', ' ') for col in df.columns]
    
    print("=" * 70)
    print("EU WATER DATASET OBSERVATORY - VALIDATION ANALYSIS")
    print("=" * 70)
    print(f"\nTotal datasets validated: {len(df)}")
    
    # Calculate URL accessibility
    url_works_counts = df['URL Works?'].value_counts()
    working_urls = int(url_works_counts.get('Yes', 0))
    broken_urls = len(df) - working_urls
    url_accessibility_rate = working_urls / len(df)
    
    print(f"\nURL Accessibility:")
    print(f"  Working URLs: {working_urls}/{len(df)} ({url_accessibility_rate:.1%})")
    print(f"  Broken/Retired URLs: {broken_urls}/{len(df)} ({(1-url_accessibility_rate):.1%})")
    
    # Categories to analyze
    categories = {
        'description': 'Description Accurate?',
        'temporal': 'Temporal Accurate?',
        'format': 'Format Accurate?',
        'license': 'License Stated?'
    }
    
    # Calculate agreement rates for all datasets
    print("\n" + "-" * 70)
    print("METADATA ACCURACY - ALL DATASETS")
    print("-" * 70)
    
    by_category_all = {}
    for key, col in categories.items():
        result = calculate_agreement_rate(df[col])
        by_category_all[key] = result
        print(f"\n{col}")
        print(f"  Yes: {result['yes']}, No: {result['no']}, Partial: {result['partial']}")
        print(f"  Agreement Rate: {result['agreement_rate']:.1%}")
    
    # Calculate overall agreement (all datasets)
    all_responses = []
    for col in categories.values():
        all_responses.extend(df[col].tolist())
    
    overall_all = calculate_agreement_rate(pd.Series(all_responses))
    print(f"\nOVERALL AGREEMENT (All Datasets): {overall_all['agreement_rate']:.1%}")
    
    # Calculate agreement rates for working URLs only
    print("\n" + "-" * 70)
    print("METADATA ACCURACY - WORKING URLS ONLY")
    print("-" * 70)
    
    df_working = df[df['URL Works?'] == 'Yes']
    print(f"Number of working URLs: {len(df_working)}")
    
    by_category_working = {}
    for key, col in categories.items():
        result = calculate_agreement_rate(df_working[col])
        by_category_working[key] = result
        print(f"\n{col}")
        print(f"  Yes: {result['yes']}, No: {result['no']}, Partial: {result['partial']}")
        print(f"  Agreement Rate: {result['agreement_rate']:.1%}")
    
    # Calculate overall agreement (working URLs only)
    working_responses = []
    for col in categories.values():
        working_responses.extend(df_working[col].tolist())
    
    overall_working = calculate_agreement_rate(pd.Series(working_responses))
    print(f"\nOVERALL AGREEMENT (Working URLs Only): {overall_working['agreement_rate']:.1%}")
    
    # Analyze by domain
    print("\n" + "-" * 70)
    print("ANALYSIS BY DOMAIN")
    print("-" * 70)
    
    by_domain = {}
    for domain in df['Domain'].unique():
        df_domain = df[df['Domain'] == domain]
        df_domain_working = df_domain[df_domain['URL Works?'] == 'Yes']
        
        # Calculate URL accessibility for this domain
        domain_working = len(df_domain_working)
        domain_total = len(df_domain)
        
        # Calculate overall agreement for working URLs in this domain
        if len(df_domain_working) > 0:
            domain_responses = []
            for col in categories.values():
                domain_responses.extend(df_domain_working[col].tolist())
            domain_agreement = calculate_agreement_rate(pd.Series(domain_responses))
            domain_agreement_rate = domain_agreement['agreement_rate']
        else:
            domain_agreement_rate = 0.0
        
        by_domain[domain] = {
            'total_datasets': int(domain_total),
            'working_urls': int(domain_working),
            'broken_urls': int(domain_total - domain_working),
            'url_accessibility_rate': round(domain_working / domain_total if domain_total > 0 else 0, 4),
            'overall_agreement_working': round(domain_agreement_rate, 4)
        }
        
        print(f"\n{domain}:")
        print(f"  Total: {domain_total}, Working: {domain_working}, Broken: {domain_total - domain_working}")
        print(f"  URL Accessibility: {by_domain[domain]['url_accessibility_rate']:.1%}")
        print(f"  Agreement (working): {domain_agreement_rate:.1%}")
    
    # Prepare output structure
    output = {
        'total_datasets': int(len(df)),
        'working_urls': int(working_urls),
        'broken_urls': int(broken_urls),
        'url_accessibility_rate': round(url_accessibility_rate, 4),
        'overall_agreement_all': round(overall_all['agreement_rate'], 4),
        'overall_agreement_working_only': round(overall_working['agreement_rate'], 4),
        'by_category_all': by_category_all,
        'by_category_working': by_category_working,
        'by_domain': by_domain
    }
    
    # Save to JSON
    output_path = Path('data/outputs_real/validation_results.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    
    print("\n" + "=" * 70)
    print(f"Results saved to: {output_path}")
    print("=" * 70)
    
    return output


if __name__ == '__main__':
    results = analyze_validation_data()
    
    print("\n" + "=" * 70)
    print("KEY FINDINGS")
    print("=" * 70)
    print(f"\n1. URL Infrastructure Instability:")
    print(f"   {results['broken_urls']}/{results['total_datasets']} ({(1-results['url_accessibility_rate']):.1%}) of curated dataset URLs are broken or retired")
    print(f"\n2. Metadata Quality:")
    print(f"   Overall agreement (all datasets): {results['overall_agreement_all']:.1%}")
    print(f"   Overall agreement (working URLs): {results['overall_agreement_working_only']:.1%}")
    print(f"\n3. License Metadata:")
    print(f"   Explicit licenses stated: {results['by_category_all']['license']['yes']}/{results['total_datasets']} ({results['by_category_all']['license']['agreement_rate']:.1%})")
    print(f"\n4. Methodology Validation:")
    print(f"   For accessible datasets, ~{results['overall_agreement_working_only']:.0%} metadata agreement")
    print(f"   validates the automated extraction approach")
    print("=" * 70)
