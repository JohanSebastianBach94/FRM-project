"""
Remove high-autocorrelation series (GDPC1, BAMLC0A4CBBB and their lags) from USA_factors.csv
to improve residual quality for ADCC estimation.
"""
import pandas as pd
from pathlib import Path

def filter_bad_actors(input_path, output_path=None):
    """Remove GDPC1* and BAMLC0A4CBBB* columns from factors CSV."""
    df = pd.read_csv(input_path)
    
    # Identify columns to drop
    bad_actors = ['GDPC1', 'BAMLC0A4CBBB']
    cols_to_drop = []
    
    for col in df.columns:
        for bad in bad_actors:
            if col.startswith(bad):
                cols_to_drop.append(col)
                
    print(f"Original shape: {df.shape}")
    print(f"Dropping {len(cols_to_drop)} columns: {cols_to_drop}")
    
    # Drop columns
    df_filtered = df.drop(columns=cols_to_drop)
    
    print(f"Filtered shape: {df_filtered.shape}")
    
    # Save
    if output_path is None:
        output_path = input_path.replace('.csv', '_filtered.csv')
    
    df_filtered.to_csv(output_path, index=False)
    print(f"Saved filtered data to: {output_path}")
    
    return df_filtered

if __name__ == '__main__':
    base_path = Path(__file__).parent.parent
    input_csv = base_path / 'analysis_outputs' / 'factor_preparation' / 'USA_factors.csv'
    output_csv = base_path / 'analysis_outputs' / 'factor_preparation' / 'USA_factors_filtered.csv'
    
    filter_bad_actors(str(input_csv), str(output_csv))
