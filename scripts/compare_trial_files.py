"""
Compare sample intermediate files between original and trial data folders.

Validates that the trial copy process preserved data integrity.
"""

import pandas as pd
from pathlib import Path
import numpy as np

# Configuration
BASE_DIR = Path(r"c:\Users\frank\Documents\FRM project")
ORIGINAL_DIR = BASE_DIR / "Data"
TRIAL_DIR = BASE_DIR / "Data" / "trial data folder"
OUTPUT_DIR = BASE_DIR / "output" / "trial data folder"

# Sample files to compare (representative across regions and maturities)
SAMPLE_FILES = [
    "BOND_France_10Y.csv",
    "BOND_France_30Y.csv",
    "BOND_Italy_10Y.csv",
    "BOND_Italy_30Y.csv",
    "BOND_United_10Y.csv",
    "BOND_United_30Y.csv",
    "BOND_Germany_10Y.csv",
    "BOND_Spain_10Y.csv",
    "BOND_France_2Y.csv",
    "BOND_France_5Y.csv",
]

def compare_files(file_name):
    """Compare a single file between original and trial directories."""
    original_path = ORIGINAL_DIR / file_name
    trial_path = TRIAL_DIR / file_name
    
    if not original_path.exists():
        return {
            "file": file_name,
            "status": "MISSING_ORIGINAL",
            "match": False,
            "rows_original": 0,
            "rows_trial": 0,
            "max_diff": None
        }
    
    if not trial_path.exists():
        return {
            "file": file_name,
            "status": "MISSING_TRIAL",
            "match": False,
            "rows_original": 0,
            "rows_trial": 0,
            "max_diff": None
        }
    
    # Load both files
    df_original = pd.read_csv(original_path, parse_dates=['Date'])
    df_trial = pd.read_csv(trial_path, parse_dates=['Date'])
    
    # Compare shapes
    if df_original.shape != df_trial.shape:
        return {
            "file": file_name,
            "status": "SHAPE_MISMATCH",
            "match": False,
            "rows_original": len(df_original),
            "rows_trial": len(df_trial),
            "max_diff": None
        }
    
    # Sort both by date for comparison
    df_original = df_original.sort_values('Date').reset_index(drop=True)
    df_trial = df_trial.sort_values('Date').reset_index(drop=True)
    
    # Compare dates
    date_match = (df_original['Date'] == df_trial['Date']).all()
    
    if not date_match:
        return {
            "file": file_name,
            "status": "DATE_MISMATCH",
            "match": False,
            "rows_original": len(df_original),
            "rows_trial": len(df_trial),
            "max_diff": None
        }
    
    # Compare values
    value_diff = (df_original['Value'] - df_trial['Value']).abs()
    max_diff = value_diff.max()
    
    # Consider exact match if max difference < 1e-10
    exact_match = max_diff < 1e-10
    
    return {
        "file": file_name,
        "status": "EXACT_MATCH" if exact_match else "VALUE_DIFF",
        "match": exact_match,
        "rows_original": len(df_original),
        "rows_trial": len(df_trial),
        "max_diff": float(max_diff)
    }

def main():
    print("=" * 70)
    print("COMPARING ORIGINAL vs TRIAL INTERMEDIATE FILES")
    print("=" * 70)
    print(f"\nOriginal directory: {ORIGINAL_DIR}")
    print(f"Trial directory:    {TRIAL_DIR}")
    print(f"Sample files:       {len(SAMPLE_FILES)}")
    
    results = []
    
    for file_name in SAMPLE_FILES:
        print(f"\n  Checking {file_name}...", end=" ")
        result = compare_files(file_name)
        results.append(result)
        
        if result["match"]:
            print(f"✅ {result['status']} ({result['rows_original']} rows)")
        else:
            print(f"❌ {result['status']}")
    
    # Create summary DataFrame
    summary_df = pd.DataFrame(results)
    
    # Save summary
    output_file = OUTPUT_DIR / "file_comparison_summary.csv"
    summary_df.to_csv(output_file, index=False)
    
    # Print summary statistics
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total files checked: {len(results)}")
    print(f"Exact matches:       {sum(summary_df['match'])}")
    print(f"Mismatches:          {len(results) - sum(summary_df['match'])}")
    
    if summary_df['match'].all():
        print("\n✅ ALL FILES MATCH EXACTLY — Trial copy is perfect!")
    else:
        print("\n⚠️  Some files have differences:")
        mismatches = summary_df[~summary_df['match']]
        for _, row in mismatches.iterrows():
            print(f"   {row['file']}: {row['status']}")
    
    print(f"\n📊 Detailed comparison saved to: {output_file}")

if __name__ == "__main__":
    main()
