"""Contrat métier et oracle indépendant utilisé par les tests."""
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal


def candles_reference(events):
    groups, seen = defaultdict(list), set()
    for event in events:
        if event['trade_id'] in seen:
            continue
        seen.add(event['trade_id'])
        ts = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
        groups[event['symbol'], ts.replace(second=0, microsecond=0)].append(
            (ts, event['trade_id'], Decimal(str(event['price'])), event['quantity']))
    result = []
    for (symbol, minute), rows in sorted(groups.items()):
        rows.sort()
        result.append(dict(symbol=symbol, minute_start=minute,
                           open=rows[0][2], close=rows[-1][2],
                           high=max(r[2] for r in rows), low=min(r[2] for r in rows),
                           volume=sum(r[3] for r in rows),
                           notional=sum(r[2] * r[3] for r in rows)))
    return result


def alert_change(price, reference):
    if reference is None or reference <= 0:
        return None
    change = Decimal(str(price)) / Decimal(str(reference)) - 1
    return change if abs(change) > Decimal('0.05') else None
