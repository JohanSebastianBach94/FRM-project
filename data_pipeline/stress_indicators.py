"""
Stress Indicators Data Loader Module
Loads FRED and Yahoo Finance stress testing indicators into the data pipeline
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import yaml

# Suppress warnings
warnings.filterwarnings('ignore')


@dataclass
class StressIndicatorsConfig:
    """Configuration for stress indicators data loading"""
    
    base_dir: Path
    output_dir: Path
    date_start: pd.Timestamp
    date_end: pd.Timestamp
    
    # Data file paths
    fred_data_file: str = "fred_stress_indicators.csv"
    yahoo_data_file: str = "yahoo_market_data.csv"
    spreads_data_file: str = "sovereign_spreads.csv"
    fred_metadata_file: str = "fred_metadata.csv"
    yahoo_metadata_file: str = "yahoo_metadata.csv"
    series_metadata_file: Path = Path("config") / "series_metadata.yaml"
    
    @classmethod
    def from_project_config(cls, project_config) -> "StressIndicatorsConfig":
        """Create stress indicators config from main project config"""
        stress_data_dir = project_config.output_dir / "trial data folder" / "stress_indicators"
        
        return cls(
            base_dir=project_config.base_dir,
            output_dir=stress_data_dir,
            date_start=project_config.date_start,
            date_end=project_config.date_end,
            series_metadata_file=project_config.base_dir / "config" / "series_metadata.yaml",
        )


class StressIndicatorsLoader:
    """
    Loader for stress testing indicators from FRED and Yahoo Finance
    
    Provides unified interface to:
    - Credit risk indicators (corporate spreads, high yield)
    - Macro indicators (GDP, inflation, unemployment)
    - Market indicators (VIX, equity indices, FX rates)
    - Liquidity indicators (money supply, central bank balance sheets)
    - Real estate prices
    - Commodity prices
    """
    
    def __init__(self, config: StressIndicatorsConfig):
        self.config = config
        self._fred_data: Optional[pd.DataFrame] = None
        self._yahoo_data: Optional[pd.DataFrame] = None
        self._spreads_data: Optional[pd.DataFrame] = None
        self._fred_metadata: Optional[pd.DataFrame] = None
        self._yahoo_metadata: Optional[pd.DataFrame] = None
        self._series_metadata: Optional[Dict[str, Dict[str, str]]] = None
        self._rt_overlay: Optional[pd.DataFrame] = None
        self._rt_driver_contrib: Optional[pd.DataFrame] = None
        self._nss_rt_dir = self.config.base_dir / "Output" / "nss_parameters"
        self._manual_data: Optional[pd.DataFrame] = None
    
    def load_all(self) -> Dict[str, pd.DataFrame]:
        """
        Load all stress indicators datasets
        
        Returns:
            Dictionary with keys: 'fred', 'yahoo', 'spreads', 'combined'
        """
        fred = self.load_fred_indicators()
        yahoo = self.load_yahoo_indicators()
        spreads = self.load_sovereign_spreads()
        
        # Create combined dataset
        manual = self.load_manual_commodities()
        combined = self._combine_datasets(
            fred,
            yahoo,
            spreads,
            extras=[manual] if not manual.empty else None,
        )
        
        return {
            'fred': fred,
            'yahoo': yahoo,
            'spreads': spreads,
            'combined': combined,
            'manual': manual,
            'rt_overlay': self.load_rt_overlay(),
            'rt_driver_contrib': self.load_rt_driver_contrib()
        }
    
    def load_fred_indicators(self) -> pd.DataFrame:
        """Load FRED stress indicators (44 series)"""
        if self._fred_data is not None:
            return self._fred_data
        
        file_path = self.config.output_dir / self.config.fred_data_file
        if not file_path.exists():
            raise FileNotFoundError(
                f"FRED data file not found: {file_path}\n"
                f"Please run the data collection script first."
            )
        
        df = pd.read_csv(file_path, index_col='date', parse_dates=True)
        
        # Filter to configured date range
        df = df.loc[
            (df.index >= self.config.date_start) & 
            (df.index <= self.config.date_end)
        ]
        
        self._fred_data = df
        return df
    
    def load_yahoo_indicators(self) -> pd.DataFrame:
        """Load Yahoo Finance indicators (9 series: commodities, equities, FX)"""
        if self._yahoo_data is not None:
            return self._yahoo_data
        
        file_path = self.config.output_dir / self.config.yahoo_data_file
        if not file_path.exists():
            raise FileNotFoundError(
                f"Yahoo data file not found: {file_path}\n"
                f"Please run the data collection script first."
            )
        
        # Read with first column as index and parse dates
        # Read with skiprows to skip Ticker and date header rows
        df = pd.read_csv(file_path, skiprows=[1, 2], index_col=0, parse_dates=True)
        df.index.name = 'date'
        
        # Ensure index is DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # Filter to configured date range
        df = df.loc[
            (df.index >= self.config.date_start) & 
            (df.index <= self.config.date_end)
        ]
        
        self._yahoo_data = df
        return df
    
    def load_sovereign_spreads(self) -> pd.DataFrame:
        """Load computed sovereign spreads (3 series)"""
        if self._spreads_data is not None:
            return self._spreads_data
        
        file_path = self.config.output_dir / self.config.spreads_data_file
        if not file_path.exists():
            raise FileNotFoundError(
                f"Spreads data file not found: {file_path}\n"
                f"Please run the data collection script first."
            )
        
        df = pd.read_csv(file_path, index_col='date', parse_dates=True)
        
        # Filter to configured date range
        df = df.loc[
            (df.index >= self.config.date_start) & 
            (df.index <= self.config.date_end)
        ]
        
        self._spreads_data = df
        return df
    
    def get_metadata(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load metadata for all indicators
        
        Returns:
            Tuple of (fred_metadata, yahoo_metadata)
        """
        if self._fred_metadata is None:
            fred_meta_path = self.config.output_dir / self.config.fred_metadata_file
            if fred_meta_path.exists():
                self._fred_metadata = pd.read_csv(fred_meta_path)
        
        if self._yahoo_metadata is None:
            yahoo_meta_path = self.config.output_dir / self.config.yahoo_metadata_file
            if yahoo_meta_path.exists():
                self._yahoo_metadata = pd.read_csv(yahoo_meta_path)
        
        return self._fred_metadata, self._yahoo_metadata
    
    def get_indicators_by_category(self, category: str) -> pd.DataFrame:
        """
        Get all indicators for a specific risk category
        
        Args:
            category: One of 'credit', 'inflation', 'macro', 'monetary', 
                     'banking_stress', 'market_volatility', 'real_estate',
                     'interest_rate_derivative', 'commodity'
        
        Returns:
            DataFrame with indicators from that category
        """
        fred_meta, _ = self.get_metadata()
        
        if fred_meta is None:
            raise ValueError("Metadata not available")
        
        # Get series codes for this category
        category_series = fred_meta[fred_meta['category'] == category]['series_code'].tolist()
        
        if not category_series:
            return pd.DataFrame()
        
        fred_data = self.load_fred_indicators()
        return fred_data[category_series]
    
    def get_indicators_by_country(self, country: str) -> pd.DataFrame:
        """
        Get all indicators for a specific country
        
        Args:
            country: Country code (e.g., 'USA', 'ITA', 'FRA', 'DEU', 'ESP')
        
        Returns:
            DataFrame with indicators for that country
        """
        fred_meta, _ = self.get_metadata()
        
        if fred_meta is None:
            raise ValueError("Metadata not available")
        
        # Build filter condition - check both 'country' and 'region' columns if they exist
        mask = pd.Series([False] * len(fred_meta))
        
        if 'country' in fred_meta.columns:
            mask |= (fred_meta['country'] == country)
        
        if 'region' in fred_meta.columns:
            mask |= (fred_meta['region'] == country)
        
        country_series = fred_meta[mask]['series_code'].tolist()
        
        if not country_series:
            return pd.DataFrame()
        
        fred_data = self.load_fred_indicators()
        available_series = [s for s in country_series if s in fred_data.columns]
        
        if not available_series:
            return pd.DataFrame()
        
        return fred_data[available_series]
    
    def _combine_datasets(
        self,
        fred: pd.DataFrame,
        yahoo: pd.DataFrame,
        spreads: pd.DataFrame,
        extras: Optional[List[pd.DataFrame]] = None
    ) -> pd.DataFrame:
        """
        Combine all datasets into a single DataFrame
        
        Uses outer join to preserve all dates from all sources
        """
        # Start with FRED data (most comprehensive)
        combined = fred.copy()
        
        # Add Yahoo data
        for col in yahoo.columns:
            if col not in combined.columns:
                combined = combined.join(yahoo[[col]], how='outer')
        
        # Add spreads
        for col in spreads.columns:
            if col not in combined.columns:
                combined = combined.join(spreads[[col]], how='outer')
        
        # Sort by date
        # Append any extra datasets (manual CSVs, proxies, etc.)
        for extra in extras or []:
            if extra is None or extra.empty:
                continue
            for col in extra.columns:
                if col in combined.columns:
                    continue
                combined = combined.join(extra[[col]], how='outer')

        # Sort by date
        combined = combined.sort_index()
        
        return combined

    def load_manual_commodities(self) -> pd.DataFrame:
        """Load manually maintained commodity CSVs such as FAO or Gold"""
        if self._manual_data is not None:
            return self._manual_data

        manual_dir = self.config.base_dir / "data_repository" / "raw" / "commodities"
        if not manual_dir.exists():
            self._manual_data = pd.DataFrame()
            return self._manual_data

        frames = []
        for csv_path in sorted(manual_dir.glob("*.csv")):
            series_name = csv_path.stem
            try:
                df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
            except ValueError:
                df = pd.read_csv(csv_path)
                df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
                df = df.set_index(df.columns[0])

            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            df = df[~df.index.duplicated(keep="first")]
            numeric_cols = df.select_dtypes(include="number").columns
            if numeric_cols.empty:
                continue

            value_col = numeric_cols[0]
            series_df = df[[value_col]].rename(columns={value_col: series_name})
            series_df = series_df.dropna(how="all")
            if not series_df.empty:
                frames.append(series_df)

        manual_df = pd.concat(frames, axis=1, join="outer") if frames else pd.DataFrame()
        manual_df = manual_df.sort_index()
        self._manual_data = manual_df
        return self._manual_data

    def load_series_metadata(self) -> Dict[str, Dict[str, str]]:
        if self._series_metadata is not None:
            return self._series_metadata

        metadata_path = self.config.series_metadata_file
        if metadata_path.exists():
            try:
                with metadata_path.open("r", encoding="utf-8") as fh:
                    raw = yaml.safe_load(fh) or {}
                self._series_metadata = raw.get("series_metadata", {})
            except Exception:
                self._series_metadata = {}
        else:
            self._series_metadata = {}

        return self._series_metadata

    def write_resampling_log(
        self,
        columns: List[str],
        fill_limit_days: int,
        upsampled: bool,
    ) -> Path:
        metadata = self.load_series_metadata()
        rows = []
        for column in columns:
            entry = metadata.get(column, {})
            original_freq = entry.get("frequency", "daily")
            measurement = entry.get("measurement_type", "level")
            if original_freq in {"monthly", "quarterly"}:
                if upsampled:
                    action = f"forward_fill_{fill_limit_days}d"
                else:
                    action = f"maintain_{original_freq}"
            else:
                action = "daily_native"
            rows.append({
                "series": column,
                "original_frequency": original_freq,
                "measurement_type": measurement,
                "resampling_action": action,
            })

        diag_dir = self.config.base_dir / "analysis_outputs" / "diagnostics"
        diag_dir.mkdir(parents=True, exist_ok=True)
        log_path = diag_dir / "resampling_log.csv"
        pd.DataFrame(rows).to_csv(log_path, index=False)
        return log_path
    
    def get_data_quality_summary(self) -> pd.DataFrame:
        """
        Generate data quality summary for all indicators
        
        Returns:
            DataFrame with completeness metrics per indicator
        """
        combined = self.load_all()['combined']
        
        summary_rows = []
        for col in combined.columns:
            series = combined[col]
            total_dates = len(combined)
            non_null = series.notna().sum()
            completeness = (non_null / total_dates * 100) if total_dates > 0 else 0
            
            first_valid = series.first_valid_index()
            last_valid = series.last_valid_index()
            
            summary_rows.append({
                'Indicator': col,
                'Total Observations': non_null,
                'Total Dates': total_dates,
                'Completeness %': f"{completeness:.1f}",
                'First Date': first_valid,
                'Last Date': last_valid,
                'Date Range Days': (last_valid - first_valid).days if first_valid and last_valid else 0
            })
        
        return pd.DataFrame(summary_rows)
    
    def resample_to_monthly(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Resample daily/weekly data to monthly frequency (end of month)
        
        Args:
            df: DataFrame with DatetimeIndex
        
        Returns:
            Monthly resampled DataFrame
        """
        return df.resample('ME').last()
    
    def forward_fill_missing(
        self, 
        df: pd.DataFrame, 
        max_fill_days: int = 30
    ) -> pd.DataFrame:
        """
        Forward fill missing values with a maximum gap limit
        
        Args:
            df: DataFrame with DatetimeIndex
            max_fill_days: Maximum number of days to forward fill
        
        Returns:
            DataFrame with forward-filled values (limited)
        """
        return df.ffill(limit=max_fill_days)
    
    def upsample_to_daily_with_ffill(
        self,
        df: pd.DataFrame,
        fill_limit_days: int = 92
    ) -> pd.DataFrame:
        """
        Upsample monthly/quarterly data to daily frequency using forward-fill
        
        Critical for including GDP, CPI, unemployment and other macro indicators
        in daily stress testing models.
        
        Logic:
        - Monthly data: Assume value persists until next release (realistic)
        - Quarterly data: Assume value persists for full quarter (standard practice)
        - Forward fill with limit to avoid excessive extrapolation
        
        Args:
            df: DataFrame with DatetimeIndex (any frequency)
            fill_limit_days: Maximum days to forward fill (default 92 = ~1 quarter)
                            Prevents filling far into future if series ends
        
        Returns:
            DataFrame resampled to daily frequency with forward-filled values
            
        Example:
            # GDP is quarterly - resample to daily
            gdp_daily = loader.upsample_to_daily_with_ffill(gdp_quarterly)
            
            # Now GDP aligns with daily credit spreads
            combined = pd.concat([credit_spreads_daily, gdp_daily], axis=1)
        """
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have DatetimeIndex")
        
        # Use a business-day calendar to avoid weekend dilution (phantom observations)
        # while still preserving releases that land on weekends (e.g., month-end).
        start = df.index.min()
        end = df.index.max()
        business_index = pd.bdate_range(start=start, end=end)

        # Ensure weekend-dated observations are not dropped before forward fill.
        union_index = business_index.union(df.index).sort_values()

        df_daily = df.reindex(union_index)
        df_daily = df_daily.ffill(limit=fill_limit_days)

        # Final output is strictly business days.
        return df_daily.reindex(business_index)
    
    def load_rt_overlay(self) -> pd.DataFrame:
        """Load the regime-aware Rt overlay pickled series"""
        if self._rt_overlay is not None:
            return self._rt_overlay

        overlay_path = self._nss_rt_dir / 'Rt_strategy_overlay.pkl'
        if not overlay_path.exists():
            raise FileNotFoundError(
                f"Rt overlay pickle not found: {overlay_path}\n"
                "Please rerun Rt diagnostics to regenerate Rt_strategy_overlay.pkl."
            )

        self._rt_overlay = pd.read_pickle(overlay_path)
        return self._rt_overlay

    def load_rt_driver_contrib(self) -> pd.DataFrame:
        """Load Rt driver contribution breakdown"""
        if self._rt_driver_contrib is not None:
            return self._rt_driver_contrib

        contrib_path = self._nss_rt_dir / 'Rt_driver_contrib.pkl'
        if not contrib_path.exists():
            raise FileNotFoundError(
                f"Rt driver contribution pickle not found: {contrib_path}\n"
                "Run the Rt driver attribution diagnostics script first."
            )

        self._rt_driver_contrib = pd.read_pickle(contrib_path)
        return self._rt_driver_contrib


def load_stress_indicators(
    config=None,
    categories: Optional[List[str]] = None,
    countries: Optional[List[str]] = None,
    resample_freq: Optional[str] = None,
    upsample_to_daily: bool = True,
    fill_limit_days: int = 92
) -> Dict[str, pd.DataFrame]:
    """
    Convenience function to load stress indicators
    
    Args:
        config: StressIndicatorsConfig or ProjectConfig instance
        categories: Optional list of categories to filter
        countries: Optional list of countries to filter
        resample_freq: Optional resampling frequency ('M', 'Q', 'Y')
        upsample_to_daily: If True, resample all data to daily frequency with forward-fill
                          This allows monthly/quarterly data (GDP, CPI) to be used
                          alongside daily data (credit spreads, VIX)
        fill_limit_days: Maximum days to forward-fill when upsampling (default 92 = 1 quarter)
    
    Returns:
        Dictionary of DataFrames with stress indicators
        
    Usage:
        # Load all indicators, upsampled to daily
        data = load_stress_indicators(upsample_to_daily=True)
        combined = data['combined']  # All 52 variables at daily frequency
        
        # GDP, CPI, unemployment will be forward-filled from monthly values
        # Credit spreads, VIX, rates already daily
    """
    from data_pipeline.loaders import load_project_config
    
    # Handle config
    if config is None:
        project_config = load_project_config()
        stress_config = StressIndicatorsConfig.from_project_config(project_config)
    elif isinstance(config, StressIndicatorsConfig):
        stress_config = config
    else:
        # Assume it's a ProjectConfig
        stress_config = StressIndicatorsConfig.from_project_config(config)
    
    loader = StressIndicatorsLoader(stress_config)
    
    # Load base datasets
    datasets = loader.load_all()
    
    # ✅ NEW: Upsample to daily frequency if requested
    if upsample_to_daily:
        print(f"[INFO] Upsampling all data to daily frequency (forward-fill limit={fill_limit_days} days)")
        print(f"       This allows monthly/quarterly indicators (GDP, CPI, unemployment) to be included")
        
        for key in ['fred', 'yahoo', 'spreads', 'combined']:
            if key in datasets and not datasets[key].empty:
                original_shape = datasets[key].shape
                datasets[key] = loader.upsample_to_daily_with_ffill(
                    datasets[key], 
                    fill_limit_days=fill_limit_days
                )
                new_shape = datasets[key].shape
                print(f"       {key}: {original_shape} -> {new_shape}")
    
    # Log resampling decisions even if upsampling is disabled so diagnostics know the source cadence
    if 'combined' in datasets and not datasets['combined'].empty:
        loader.write_resampling_log(
            list(datasets['combined'].columns),
            fill_limit_days=fill_limit_days,
            upsampled=upsample_to_daily,
        )

    # Apply filters if requested
    if categories:
        filtered_frames = []
        for cat in categories:
            cat_data = loader.get_indicators_by_category(cat)
            if not cat_data.empty:
                filtered_frames.append(cat_data)
        if filtered_frames:
            datasets['filtered'] = pd.concat(filtered_frames, axis=1)
    
    if countries:
        country_frames = []
        for country in countries:
            country_data = loader.get_indicators_by_country(country)
            if not country_data.empty:
                country_frames.append(country_data)
        if country_frames:
            datasets['country_filtered'] = pd.concat(country_frames, axis=1)
    
    # Resample if requested
    if resample_freq:
        for key in ['fred', 'yahoo', 'spreads', 'combined']:
            if key in datasets:
                datasets[f'{key}_{resample_freq}'] = datasets[key].resample(
                    resample_freq
                ).last()
    
    return datasets


# Export public API
__all__ = [
    'StressIndicatorsConfig',
    'StressIndicatorsLoader',
    'load_stress_indicators',
]
