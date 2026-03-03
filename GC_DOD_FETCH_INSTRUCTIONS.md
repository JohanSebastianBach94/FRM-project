# GC.DOD Comprehensive Fetch Instructions

## Problem
The current `GC.DOD.TOTL.GD.ZS` (Government Central Debt as % of GDP) series in the repo has very limited coverage:
- USA: 1990-2023 (good)
- ESP: 1990-2023 (good)  
- DEU: Only 1990 (single year!)
- ITA: Only 1991-1992 (2 years!)
- FRA: No data at all

## Solution - Manual Fetch Required

Network fetches are timing out in the current environment. You need to run the fetcher script manually on a machine with Internet access.

### Step 1: Run the Comprehensive Fetcher

Open PowerShell and run:

```powershell
cd "C:\Users\frank\Documents\FRM project"
python scripts/fetch_gc_dod_comprehensive.py
```

This script will:
1. Try World Bank WDI API (primary source)
2. Try IMF IFS SDMX (fallback)
3. Try Eurostat (for EU countries)
4. Try OECD (additional fallback)
5. Merge all sources and write:
   - `data_repository/raw/macro/wb_GC.DOD.TOTL.GD.ZS_{ISO}.json` (World Bank raw)
   - `data_repository/raw/macro/general_government_gross_debt_pct_gdp_{ISO}.csv` (merged CSV with source column)

### Step 2: Extend Series to 1990

After fetching, run the backfill script to extend any gaps:

```powershell
python scripts/extend_gc_dod_backfill.py
```

This will create `general_government_gross_debt_pct_gdp_{ISO}_extended.csv` files with:
- Full coverage from 1990 to present
- Linear interpolation for internal gaps
- Forward/backward fill for leading/trailing gaps
- `imputed` flag column marking filled values

### Step 3: Update Config

Once you have good coverage, update `config/country_blocks_extended.yaml` to point to the extended CSVs:

```yaml
local_series_files:
  GC.DOD.TOTL.GD.ZS_USA: data_repository/raw/macro/general_government_gross_debt_pct_gdp_USA_extended.csv
  GC.DOD.TOTL.GD.ZS_DEU: data_repository/raw/macro/general_government_gross_debt_pct_gdp_DEU_extended.csv
  GC.DOD.TOTL.GD.ZS_FRA: data_repository/raw/macro/general_government_gross_debt_pct_gdp_FRA_extended.csv
  GC.DOD.TOTL.GD.ZS_ITA: data_repository/raw/macro/general_government_gross_debt_pct_gdp_ITA_extended.csv
  GC.DOD.TOTL.GD.ZS_ESP: data_repository/raw/macro/general_government_gross_debt_pct_gdp_ESP_extended.csv
```

## Alternative: Use Existing World Bank PowerShell Script

If the Python fetcher doesn't work, you can use the existing PowerShell script:

```powershell
cd "C:\Users\frank\Documents\FRM project"
.\scripts\prepare_attach_downloads.ps1 -AttachDir "C:\tmp\frm_attach"
```

Then ingest the downloaded files:

```powershell
python .\scripts\extend_fetch_structural_data.py --attach "C:\tmp\frm_attach"
```

## Direct World Bank API Calls (Manual Fallback)

If scripts fail, fetch manually via browser or curl:

- USA: https://api.worldbank.org/v2/country/USA/indicator/GC.DOD.TOTL.GD.ZS?format=json&per_page=2000&date=1960:2025
- DEU: https://api.worldbank.org/v2/country/DEU/indicator/GC.DOD.TOTL.GD.ZS?format=json&per_page=2000&date=1960:2025
- FRA: https://api.worldbank.org/v2/country/FRA/indicator/GC.DOD.TOTL.GD.ZS?format=json&per_page=2000&date=1960:2025
- ITA: https://api.worldbank.org/v2/country/ITA/indicator/GC.DOD.TOTL.GD.ZS?format=json&per_page=2000&date=1960:2025
- ESP: https://api.worldbank.org/v2/country/ESP/indicator/GC.DOD.TOTL.GD.ZS?format=json&per_page=2000&date=1960:2025

Save each as `wb_GC.DOD.TOTL.GD.ZS_{ISO}.json` in `data_repository/raw/macro/`.

## Expected Coverage After Fetch

Based on World Bank WDI typical coverage for government debt:
- USA: Should get ~1990-2023 (current repo has this)
- DEU: Should get ~1995-2023 (repo only has 1990!)
- FRA: Should get ~1995-2023 (repo has nothing!)
- ITA: Should get ~1988-2023 (repo only has 1991-1992!)
- ESP: Should get ~1980-2023 (current repo has 1990-2023)

Note: Some European countries may have sparse WB data for GC.DOD (which is *central* government debt). For *general* government debt, Eurostat's `gov_10_gdp` series has better coverage but the comprehensive fetcher doesn't fully implement Eurostat parsing yet.

## If Fetching Fails

Contact me with the error message from running `fetch_gc_dod_comprehensive.py` and I can:
1. Debug the specific API endpoint issues
2. Provide alternative Eurostat/IMF/OECD endpoints
3. Help build a custom parser for XML SDMX responses
4. Suggest manual data download from provider portals

## Status: Action Required

✅ Created comprehensive fetcher script  
✅ Created backfill/extension script  
❌ Network fetches timing out in current environment  
❌ Need manual execution on Internet-connected machine  

**Next step: Run `python scripts/fetch_gc_dod_comprehensive.py` on a machine with Internet access.**
