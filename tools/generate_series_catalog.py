"""Utility to export the in-repo series catalog for documentation."""

from collections import OrderedDict
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT_DIR)

import config.stress_indicators_config as sic
import scripts.extend_fetch_structural_data as efs


def gather_entries():
    entries = OrderedDict()

    def add(code, *, common_name='', description='', frequency='', unit='', category='', country='', region='', coverage='', source='', notes=''):
        if not code:
            return
        existing = entries.get(code, {})
        existing.update({
            'common_name': common_name or existing.get('common_name', ''),
            'description': description or existing.get('description', ''),
            'frequency': frequency or existing.get('frequency', ''),
            'unit': unit or existing.get('unit', ''),
            'category': category or existing.get('category', ''),
            'country': country or existing.get('country', ''),
            'region': region or existing.get('region', ''),
            'coverage': coverage or existing.get('coverage', ''),
            'source': source or existing.get('source', ''),
            'notes': notes or existing.get('notes', ''),
        })
        entries[code] = existing

    # stress indicators configuration dictionaries
    for attr, value in sic.__dict__.items():
        if attr.isupper() and isinstance(value, dict):
            for code, meta in value.items():
                if not isinstance(meta, dict):
                    continue
                add(
                    code,
                    common_name=meta.get('name', ''),
                    description=meta.get('description', ''),
                    frequency=meta.get('frequency', ''),
                    unit=meta.get('unit', ''),
                    category=meta.get('category', attr),
                    country=meta.get('country', ''),
                    region=meta.get('region', ''),
                    coverage=meta.get('coverage', ''),
                    source=f'config.stress_indicators_config.{attr}',
                    notes=meta.get('notes', ''),
                )

    # World Bank core indicators
    add(
        'GC.DOD.TOTL.GD.ZS',
        common_name='General government gross debt (% GDP)',
        description='World Bank WDI: general government gross debt as a share of GDP',
        frequency='annual',
        unit='percent of GDP',
        category='macro',
        country='multi',
        coverage='1990-present (country-dependent)',
        source='World Bank WDI via scripts/extend_fetch_structural_data.py',
        notes='Countries: FRA, DEU, ITA, ESP, USA, GBR, CHE',
    )
    add(
        'NY.GDP.MKTP.CD',
        common_name='Nominal GDP (current US$)',
        description='World Bank WDI: GDP measured in current US dollars',
        frequency='annual',
        unit='current USD',
        category='macro',
        country='multi',
        coverage='1990-present (country-dependent)',
        source='World Bank WDI via scripts/extend_fetch_structural_data.py',
        notes='Used for canonical nominal level references',
    )

    # Provider-specific structural series
    for series in efs.ECB_SERIES:
        add(
            series['series_id'],
            common_name=series.get('notes', ''),
            description=series.get('notes', ''),
            frequency=series.get('frequency', ''),
            category='structural',
            region='Euro area',
            coverage='1990-present',
            source='ECB Data Portal API (scripts/extend_fetch_structural_data.py)',
            notes=series.get('notes', ''),
        )
    for series in efs.IMF_SERIES:
        add(
            series['series_id'],
            common_name=series.get('notes', ''),
            description=series.get('notes', ''),
            frequency=series.get('frequency', ''),
            category='macro',
            country=series['key'].split('.')[0],
            coverage=f"{series.get('startPeriod', '1990')}-present",
            source='IMF REST SDMX (scripts/extend_fetch_structural_data.py)',
            notes=series.get('notes', ''),
        )
    for series in efs.BIS_DOWNLOADS:
        add(
            series['series_id'],
            common_name=series.get('notes', ''),
            description=series.get('notes', ''),
            frequency=series.get('frequency', ''),
            category='structural',
            country='global',
            coverage='provider default',
            source='BIS Statistics API (scripts/extend_fetch_structural_data.py)',
            notes=series.get('notes', ''),
        )

    return entries


def main():
    entries = gather_entries()
    header = [
        'series_code',
        'common_name',
        'description',
        'frequency',
        'unit',
        'category',
        'country',
        'region',
        'coverage',
        'source',
        'notes',
    ]
    print(','.join(header))
    for code, meta in entries.items():
        row = [
            code,
            meta.get('common_name', ''),
            meta.get('description', ''),
            meta.get('frequency', ''),
            meta.get('unit', ''),
            meta.get('category', ''),
            meta.get('country', ''),
            meta.get('region', ''),
            meta.get('coverage', ''),
            meta.get('source', ''),
            meta.get('notes', ''),
        ]
        escaped = []
        for value in row:
            if not value:
                escaped.append('')
            elif isinstance(value, str):
                escaped.append('"' + value.replace('"', '""') + '"')
            else:
                escaped.append(str(value))
        print(','.join(escaped))


if __name__ == '__main__':
    main()