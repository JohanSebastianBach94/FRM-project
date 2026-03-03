import os
from urllib.request import urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError
import json

key = os.environ.get('FRED_API_KEY')
if not key:
    raise SystemExit('FRED_API_KEY missing')

for sid in ['BKX', 'SX7E']:
    url = 'https://api.stlouisfed.org/fred/series?' + urlencode({'series_id': sid, 'api_key': key, 'file_type': 'json'})
    try:
        with urlopen(url, timeout=30) as r:
            data = json.load(r)
    except HTTPError as exc:
        print(sid, '-> HTTP', exc.code)
        continue
    print(sid, '->', 'error' if 'error_code' in data else data.get('seriess', [{}])[0].get('title'))
