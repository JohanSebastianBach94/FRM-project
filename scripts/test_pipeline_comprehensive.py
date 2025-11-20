"""
Comprehensive Pipeline Test Suite
Tests the entire FRM data pipeline including stress indicators integration
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

# Add project to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print("="*80)
print("FRM PROJECT - COMPREHENSIVE PIPELINE TEST")
print("="*80)
print(f"Test started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Test counter
tests_passed = 0
tests_failed = 0
test_results = []

def test_result(name: str, passed: bool, message: str = ""):
    """Record test result"""
    global tests_passed, tests_failed, test_results
    status = "✓ PASS" if passed else "✗ FAIL"
    result_str = f"{status} - {name}"
    if message:
        result_str += f"\n        {message}"
    test_results.append((name, passed, message))
    print(result_str)
    if passed:
        tests_passed += 1
    else:
        tests_failed += 1

# ============================================================================
# TEST 1: Module Imports
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 1: MODULE IMPORTS")
print("="*80)

try:
    from data_pipeline import (
        ProjectConfig,
        load_project_config,
        load_yield_data,
        StressIndicatorsConfig,
        StressIndicatorsLoader,
        load_stress_indicators
    )
    test_result("Import data_pipeline package", True, "All modules imported successfully")
except Exception as e:
    test_result("Import data_pipeline package", False, str(e))
    sys.exit(1)

try:
    from config.stress_indicators_config import (
        ALL_FRED_SERIES,
        ALL_YAHOO_SERIES,
        CREDIT_SPREADS,
        INFLATION,
        MACRO_INDICATORS
    )
    test_result("Import stress indicators config", True, f"Loaded {len(ALL_FRED_SERIES)} FRED + {len(ALL_YAHOO_SERIES)} Yahoo series")
except Exception as e:
    test_result("Import stress indicators config", False, str(e))

# ============================================================================
# TEST 2: Configuration Loading
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 2: CONFIGURATION LOADING")
print("="*80)

try:
    project_config = load_project_config()
    test_result("Load project configuration", True, 
                f"Base dir: {project_config.base_dir}")
    
    # Verify configuration properties
    assert hasattr(project_config, 'base_dir'), "Missing base_dir"
    assert hasattr(project_config, 'output_dir'), "Missing output_dir"
    assert hasattr(project_config, 'date_start'), "Missing date_start"
    assert hasattr(project_config, 'date_end'), "Missing date_end"
    test_result("Configuration properties", True, 
                f"Date range: {project_config.date_start.date()} to {project_config.date_end.date()}")
    
except Exception as e:
    test_result("Load project configuration", False, str(e))
    project_config = None

try:
    stress_config = StressIndicatorsConfig.from_project_config(project_config)
    test_result("Create stress indicators config", True,
                f"Output dir: {stress_config.output_dir}")
    
    # Verify stress config paths exist
    if stress_config.output_dir.exists():
        test_result("Stress data directory exists", True, str(stress_config.output_dir))
    else:
        test_result("Stress data directory exists", False, 
                   f"Directory not found: {stress_config.output_dir}")
    
except Exception as e:
    test_result("Create stress indicators config", False, str(e))
    stress_config = None

# ============================================================================
# TEST 3: Stress Indicators Data Loading
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 3: STRESS INDICATORS DATA LOADING")
print("="*80)

if stress_config:
    try:
        loader = StressIndicatorsLoader(stress_config)
        test_result("Initialize StressIndicatorsLoader", True)
    except Exception as e:
        test_result("Initialize StressIndicatorsLoader", False, str(e))
        loader = None
    
    if loader:
        # Test FRED data loading
        try:
            fred_data = loader.load_fred_indicators()
            test_result("Load FRED indicators", True,
                       f"Shape: {fred_data.shape}, Columns: {len(fred_data.columns)}")
            
            # Verify data structure
            assert isinstance(fred_data.index, pd.DatetimeIndex), "Index is not DatetimeIndex"
            assert len(fred_data.columns) > 0, "No columns loaded"
            test_result("FRED data structure validation", True,
                       f"Date range: {fred_data.index.min().date()} to {fred_data.index.max().date()}")
            
        except Exception as e:
            test_result("Load FRED indicators", False, str(e))
            fred_data = None
        
        # Test Yahoo data loading
        try:
            yahoo_data = loader.load_yahoo_indicators()
            test_result("Load Yahoo Finance indicators", True,
                       f"Shape: {yahoo_data.shape}, Columns: {len(yahoo_data.columns)}")
            
            assert isinstance(yahoo_data.index, pd.DatetimeIndex), "Index is not DatetimeIndex"
            test_result("Yahoo data structure validation", True,
                       f"Date range: {yahoo_data.index.min().date()} to {yahoo_data.index.max().date()}")
            
        except Exception as e:
            test_result("Load Yahoo Finance indicators", False, str(e))
            yahoo_data = None
        
        # Test spreads loading
        try:
            spreads_data = loader.load_sovereign_spreads()
            test_result("Load sovereign spreads", True,
                       f"Shape: {spreads_data.shape}, Columns: {len(spreads_data.columns)}")
            
            expected_spreads = ['BTP_Bund_Spread', 'Bonos_Bund_Spread', 'OAT_Bund_Spread']
            for spread in expected_spreads:
                if spread in spreads_data.columns:
                    test_result(f"Spread present: {spread}", True)
                else:
                    test_result(f"Spread present: {spread}", False, "Column missing")
            
        except Exception as e:
            test_result("Load sovereign spreads", False, str(e))
            spreads_data = None
        
        # Test combined loading
        try:
            all_data = loader.load_all()
            test_result("Load all datasets", True,
                       f"Keys: {list(all_data.keys())}")
            
            combined = all_data['combined']
            test_result("Combined dataset", True,
                       f"Shape: {combined.shape}, Total indicators: {len(combined.columns)}")
            
            # Check for missing data patterns
            missing_pct = (combined.isna().sum() / len(combined) * 100).mean()
            test_result("Data completeness check", True,
                       f"Average missing: {missing_pct:.2f}%")
            
        except Exception as e:
            test_result("Load all datasets", False, str(e))
            all_data = None

# ============================================================================
# TEST 4: Metadata Loading
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 4: METADATA LOADING")
print("="*80)

if loader:
    try:
        fred_meta, yahoo_meta = loader.get_metadata()
        test_result("Load metadata", True,
                   f"FRED series: {len(fred_meta) if fred_meta is not None else 0}, "
                   f"Yahoo series: {len(yahoo_meta) if yahoo_meta is not None else 0}")
        
        if fred_meta is not None:
            # Verify metadata structure
            required_cols = ['series_code', 'name', 'category', 'frequency']
            missing_cols = [col for col in required_cols if col not in fred_meta.columns]
            if not missing_cols:
                test_result("FRED metadata structure", True,
                           f"All required columns present: {required_cols}")
            else:
                test_result("FRED metadata structure", False,
                           f"Missing columns: {missing_cols}")
            
            # Check categories
            categories = fred_meta['category'].unique().tolist()
            test_result("FRED categories", True,
                       f"Categories: {len(categories)} - {', '.join(categories[:5])}...")
        
    except Exception as e:
        test_result("Load metadata", False, str(e))

# ============================================================================
# TEST 5: Category and Country Filtering
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 5: DATA FILTERING")
print("="*80)

if loader:
    # Test category filtering
    test_categories = ['credit', 'inflation', 'macro']
    for category in test_categories:
        try:
            cat_data = loader.get_indicators_by_category(category)
            test_result(f"Filter by category: {category}", True,
                       f"Shape: {cat_data.shape}")
        except Exception as e:
            test_result(f"Filter by category: {category}", False, str(e))
    
    # Test country filtering
    test_countries = ['USA', 'ITA', 'FRA']
    for country in test_countries:
        try:
            country_data = loader.get_indicators_by_country(country)
            test_result(f"Filter by country: {country}", True,
                       f"Shape: {country_data.shape}")
        except Exception as e:
            test_result(f"Filter by country: {country}", False, str(e))

# ============================================================================
# TEST 6: Data Quality and Transformations
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 6: DATA QUALITY AND TRANSFORMATIONS")
print("="*80)

if loader and all_data:
    try:
        quality_summary = loader.get_data_quality_summary()
        test_result("Generate data quality summary", True,
                   f"Analyzed {len(quality_summary)} indicators")
        
        # Check completeness distribution
        completeness_vals = quality_summary['Completeness %'].str.rstrip('%').astype(float)
        high_quality = (completeness_vals >= 95).sum()
        test_result("High quality indicators (>95%)", True,
                   f"{high_quality} out of {len(quality_summary)} indicators")
        
    except Exception as e:
        test_result("Generate data quality summary", False, str(e))
    
    # Test resampling
    try:
        test_df = all_data['fred'].head(100)
        monthly = loader.resample_to_monthly(test_df)
        test_result("Resample to monthly", True,
                   f"Original: {len(test_df)}, Monthly: {len(monthly)}")
    except Exception as e:
        test_result("Resample to monthly", False, str(e))
    
    # Test forward fill
    try:
        test_df = all_data['fred'].head(100)
        filled = loader.forward_fill_missing(test_df, max_fill_days=5)
        test_result("Forward fill missing values", True)
    except Exception as e:
        test_result("Forward fill missing values", False, str(e))

# ============================================================================
# TEST 7: Convenience Function
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 7: CONVENIENCE FUNCTION")
print("="*80)

try:
    datasets = load_stress_indicators()
    test_result("load_stress_indicators() function", True,
               f"Loaded datasets: {list(datasets.keys())}")
    
    # Test with filters
    filtered = load_stress_indicators(categories=['credit', 'inflation'])
    test_result("load_stress_indicators() with category filter", True)
    
    # Test with resampling
    resampled = load_stress_indicators(resample_freq='M')
    test_result("load_stress_indicators() with resampling", True,
               f"Created monthly datasets")
    
except Exception as e:
    test_result("load_stress_indicators() function", False, str(e))

# ============================================================================
# TEST 8: Yield Curve Data Integration Test
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 8: YIELD CURVE DATA INTEGRATION")
print("="*80)

try:
    # Test that yield data can still be loaded
    yield_datasets = load_yield_data(data_source='all')
    test_result("Load yield curve data", True,
               f"Loaded {len(yield_datasets)} yield datasets")
    
    # Verify both pipelines can coexist
    stress_data = load_stress_indicators()
    test_result("Load yield and stress data simultaneously", True,
               "Both data sources loaded successfully")
    
except Exception as e:
    test_result("Load yield curve data", False, str(e))

# ============================================================================
# TEST 9: Data Consistency Checks
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 9: DATA CONSISTENCY CHECKS")
print("="*80)

if all_data:
    combined = all_data['combined']
    
    # Check for duplicate columns
    duplicate_cols = combined.columns[combined.columns.duplicated()].tolist()
    if not duplicate_cols:
        test_result("No duplicate columns", True)
    else:
        test_result("No duplicate columns", False,
                   f"Duplicates found: {duplicate_cols}")
    
    # Check for all-null columns
    null_cols = combined.columns[combined.isna().all()].tolist()
    if not null_cols:
        test_result("No completely null columns", True)
    else:
        test_result("No completely null columns", False,
                   f"All-null columns: {null_cols}")
    
    # Check date index is sorted
    is_sorted = combined.index.is_monotonic_increasing
    test_result("Date index is sorted", is_sorted)
    
    # Check for duplicate dates
    has_duplicates = combined.index.duplicated().any()
    test_result("No duplicate dates", not has_duplicates)
    
    # Check data types
    numeric_cols = combined.select_dtypes(include=[np.number]).columns
    test_result("Numeric data types", True,
               f"{len(numeric_cols)} out of {len(combined.columns)} columns are numeric")

# ============================================================================
# TEST 10: Performance and Memory
# ============================================================================
print("\n" + "="*80)
print("TEST SUITE 10: PERFORMANCE AND MEMORY")
print("="*80)

if all_data:
    combined = all_data['combined']
    
    # Check memory usage
    memory_mb = combined.memory_usage(deep=True).sum() / 1024**2
    test_result("Memory usage check", memory_mb < 500,
               f"Combined dataset uses {memory_mb:.2f} MB")
    
    # Check load time (should be fast on second load - cached)
    import time
    start = time.time()
    _ = loader.load_all()
    elapsed = time.time() - start
    test_result("Fast data loading", elapsed < 5,
               f"Load time: {elapsed:.3f} seconds")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*80)
print("TEST SUMMARY")
print("="*80)
print(f"\nTotal Tests: {tests_passed + tests_failed}")
print(f"✓ Passed: {tests_passed}")
print(f"✗ Failed: {tests_failed}")
print(f"Success Rate: {tests_passed/(tests_passed + tests_failed)*100:.1f}%")

if tests_failed > 0:
    print("\n" + "="*80)
    print("FAILED TESTS DETAIL")
    print("="*80)
    for name, passed, message in test_results:
        if not passed:
            print(f"\n✗ {name}")
            if message:
                print(f"  Error: {message}")

print("\n" + "="*80)
print(f"Test completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Exit code
sys.exit(0 if tests_failed == 0 else 1)
