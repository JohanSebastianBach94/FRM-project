"""
Append local series (USA_HPI_REAL and BIS_LBS_Household_Loans_*) to industry_data_raw.csv
so the merge pipeline picks them up.

Usage:
    python scripts/append_local_series_to_industry_raw.py
"""
from pathlib import Path
import pandas as pd

ROOT = Path('.').resolve()
INDUSTRY_RAW = ROOT / 'industry_data_raw.csv'

# Series to append with their local paths
SERIES_TO_ADD = {
    'USA_HPI_REAL': ROOT / 'data_repository' / 'raw' / 'fred' / 'CSUSHPISA.csv',
    'BIS_LBS_Household_Loans_USA': ROOT / 'data_repository' / 'raw' / 'providers' / 'bis_lbs' / 'monthly' / 'BIS_LBS_Household_Loans_USA.csv',
    'BIS_LBS_Household_Loans_DEU': ROOT / 'data_repository' / 'raw' / 'providers' / 'bis_lbs' / 'monthly' / 'BIS_LBS_Household_Loans_DEU.csv',
    'BIS_LBS_Household_Loans_FRA': ROOT / 'data_repository' / 'raw' / 'providers' / 'bis_lbs' / 'monthly' / 'BIS_LBS_Household_Loans_FRA.csv',
    'BIS_LBS_Household_Loans_ITA': ROOT / 'data_repository' / 'raw' / 'providers' / 'bis_lbs' / 'monthly' / 'BIS_LBS_Household_Loans_ITA.csv',
    'BIS_LBS_Household_Loans_ESP': ROOT / 'data_repository' / 'raw' / 'providers' / 'bis_lbs' / 'monthly' / 'BIS_LBS_Household_Loans_ESP.csv',
}


def load_local_series(path: Path, series_name: str) -> pd.Series:
    """Load a single series from a local CSV file."""
    if not path.exists():
        print(f"  [SKIP] {series_name}: file not found at {path}")
        return None
    
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    # If multiple columns, take the first numeric one
    if df.shape[1] > 1:
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                return df[col].rename(series_name)
        print(f"  [SKIP] {series_name}: no numeric column found")
        return None
    else:
        return df.iloc[:, 0].rename(series_name)


def main():
    print("Loading industry_data_raw.csv...")
    if not INDUSTRY_RAW.exists():
        print(f"[ERROR] {INDUSTRY_RAW} not found. Run collect_industry_data.py first.")
        return
    
    industry_df = pd.read_csv(INDUSTRY_RAW, index_col=0, parse_dates=True)
    print(f"  Loaded: {industry_df.shape[0]} rows x {industry_df.shape[1]} columns")
    
    series_to_merge = []
    for name, path in SERIES_TO_ADD.items():
        if name in industry_df.columns:
            print(f"  [SKIP] {name}: already in industry_data_raw.csv")
            continue
        ser = load_local_series(path, name)
        if ser is not None:
            series_to_merge.append(ser)
            print(f"  [LOADED] {name}: {len(ser.dropna())} observations")
    
    if not series_to_merge:
        print("\n[INFO] No new series to add.")
        return
    
    # Merge new series into the industry dataframe
    new_df = pd.DataFrame({s.name: s for s in series_to_merge})
    combined = pd.concat([industry_df, new_df], axis=1)
    
    # Backup old file
    backup = INDUSTRY_RAW.with_suffix('.bak.csv')
    INDUSTRY_RAW.rename(backup)
    print(f"\n[BACKUP] Saved old industry_data_raw.csv to {backup.name}")
    
    # Write updated file
    combined.to_csv(INDUSTRY_RAW)
    print(f"[SAVED] Updated industry_data_raw.csv: {combined.shape[0]} rows x {combined.shape[1]} columns")


if __name__ == '__main__':
    main()
