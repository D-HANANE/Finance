"""Generate the committed Grafana dashboard without a running Grafana."""
import json
from pathlib import Path

panels = []
for index, symbol in enumerate(('AAPL', 'MSFT', 'NVDA')):
    panels.append({
        'id': index+1, 'type': 'candlestick', 'title': f'{symbol} · cours simulés',
        'datasource': {'type': 'postgres', 'uid': 'finance-pg'},
        'gridPos': {'x': index*8, 'y': 0, 'w': 8, 'h': 13},
        'targets': [{'refId': 'A', 'format': 'table', 'rawQuery': True,
                     'rawSql': f"SELECT minute_start AS time, open::float8, high::float8, low::float8, close::float8, volume::float8 FROM candles WHERE symbol='{symbol}' AND $__timeFilter(minute_start) ORDER BY 1"}],
        'options': {'mode': 'both', 'candleStyle': 'candles', 'colorStrategy': 'open-close',
                    'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'},
        'fieldConfig': {'defaults': {'unit': 'currencyUSD'}, 'overrides': []},
    })
dashboard = {'uid': 'finance-live', 'title': 'Marché financier simulé', 'schemaVersion': 40,
             'version': 1, 'timezone': 'utc', 'refresh': '5s',
             'time': {'from': 'now-30m', 'to': 'now'}, 'panels': panels}
Path('grafana/dashboards/finance.json').write_text(json.dumps(dashboard, indent=2), encoding='utf-8')
