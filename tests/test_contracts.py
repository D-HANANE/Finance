import sys
import unittest
from pathlib import Path
from decimal import Decimal
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
from contracts import candles_reference, alert_change


class CalculationTests(unittest.TestCase):
    def test_event_time_ties_duplicates_quantity(self):
        def event(identifier, timestamp, price, quantity):
            return dict(trade_id=identifier,symbol='AAPL',event_time=f'2026-10-26T12:00:{timestamp}Z',price=price,quantity=quantity)
        rows = [event('z','40',103,2),event('b','00',101,3),
                event('a','00',100,4),event('c','20',98,1)]
        result = candles_reference(rows+[rows[0]])[0]
        self.assertEqual([result[k] for k in ('open','high','low','close','volume','notional')],
                         [100,103,98,103,10,Decimal('1007')])

    def test_strict_alert_and_missing_reference(self):
        self.assertIsNone(alert_change(105,100))
        self.assertIsNone(alert_change(95,100))
        self.assertIsNone(alert_change(110,None))
        self.assertEqual(alert_change(108,100),Decimal('0.08'))
        self.assertEqual(alert_change(92,100),Decimal('-0.08'))
