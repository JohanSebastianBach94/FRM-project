"""Utility to map proxy series codes to their data paths and provider hints."""
from collections import defaultdict
from pathlib import Path
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "country_blocks_extended.yaml"
PROVIDERS_DIR = REPO_ROOT / "data_repository" / "raw" / "providers"

TARGET_SERIES = {
    "Rent_to_income_ratio_{ISO}": [
        "Rent_to_income_ratio_USA",
        "Rent_to_income_ratio_DEU",
        "Rent_to_income_ratio_FRA",
        "Rent_to_income_ratio_ITA",
        "Rent_to_income_ratio_ESP",
    ],
    "Price_to_income_{ISO}": [
        "Price_to_income_USA",
        "Price_to_income_DEU",
        "Price_to_income_FRA",
        "Price_to_income_ITA",
        "Price_to_income_ESP",
    ],
    "Bank_equity_index_{ISO}": [
        "Bank_equity_index_USA",
        "Bank_equity_index_DEU",
        "Bank_equity_index_FRA",
        "Bank_equity_index_ITA",
        "Bank_equity_index_ESP",
    ],
    "MOVE": ["MOVE"],
}

MOVE_CANDIDATES = [
    {
        "provider": "Bank of America (BofA)",
        "series": "MOVE",
        "method": "Subscription or portal download; look for MOVE index snapshots or CSV exports (BofA website or via Bloomberg).",
    },
    {
        "provider": "Bloomberg/Macroeconomic terminal",
        "series": "MOVE Index (BofA MOVE) via <FMBT> or similar ticker",
        "method": "Use Bloomberg API to pull historical MOVE series (requires license).",
    },
]


def load_config(path):
    with path.open() as fh:
        return yaml.safe_load(fh)


def gather_series_metadata(config):
    lookup = defaultdict(list)
    for block in config.get("country_blocks", []):
        country = block.get("country")
        iso = block.get("iso_code")
        for entry in block.get("blocks", []):
            block_key = entry.get("key")
            local_files = entry.get("local_series_files", {})
            data_prov = entry.get("data_provenance", {})
            missing = entry.get("missing_series", {})
            for code in entry.get("series_codes", []):
                if code in _target_ids():
                    lookup[code].append(
                        {
                            "country": country,
                            "iso": iso,
                            "block": block_key,
                            "local_file": local_files.get(code),
                            "provenance": data_prov.get(code),
                            "status": missing.get(code, "present"),
                        }
                    )
    return lookup


def _target_ids():
    return {code for codes in TARGET_SERIES.values() for code in codes}


def describe_proxy_sources():
    config = load_config(CONFIG_PATH)
    lookup = gather_series_metadata(config)
    print("Proxy series mapping and availability")
    for template, codes in TARGET_SERIES.items():
        print(f"\n{template}:")
        for code in codes:
            entries = lookup.get(code)
            if not entries:
                print(f"  - {code}: not mapped in config (needs derivation)")
                continue
            for entry in entries:
                path = entry["local_file"] or "[not specified]"
                prov = entry["provenance"] or "[none]"
                print(
                    f"  - {code} ({entry['country']} / {entry['block']}): status={entry['status']}, file={path}, provider={prov}"
                )


def inspect_provider_files():
    if not PROVIDERS_DIR.exists():
        return []
    files = sorted(str(p.relative_to(REPO_ROOT)) for p in PROVIDERS_DIR.glob("**/*.csv"))
    print("\nProvider data files under data_repository/raw/providers:")
    for f in files:
        print(f"  - {f}")
    return files


def describe_move_options():
    print("\nMOVE index provider candidates:")
    for entry in MOVE_CANDIDATES:
        print(f"  - {entry['provider']}: {entry['method']}")


def main():
    describe_proxy_sources()
    inspect_provider_files()
    describe_move_options()


if __name__ == "__main__":
    main()
