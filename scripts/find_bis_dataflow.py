from urllib.request import urlopen
import json

url = 'https://stats.bis.org/api/v2/dataflow'
with urlopen(url, timeout=30) as resp:
    data = json.load(resp)
flows = data.get('structure', {}).get('dataflows', [])
term = 'non'
for flow in flows:
    name = flow.get('name', '').lower()
    if 'non' in name:
        print(flow.get('id'), flow.get('name'))
