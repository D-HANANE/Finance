import sys
import unittest
from pathlib import Path
from datetime import datetime,timedelta,timezone
from decimal import Decimal
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
from alerting import episodes

class AlertLifecycleTests(unittest.TestCase):
    def observations(self,values):
        start=datetime(2026,11,4,12,tzinfo=timezone.utc)
        return [dict(observed_at=start+timedelta(minutes=i),price=100,
          reference_price=100,reference_time=start+timedelta(minutes=i-5),
          change_fraction=v) for i,v in enumerate(values)]
    def test_strict_threshold(self):
        self.assertEqual(episodes('AAPL',self.observations(['0.05','-0.05',None])),[])
    def test_one_episode_is_maintained_then_resolved(self):
        result=episodes('AAPL',self.observations(['0.08','0.09',None,'0.02']))
        self.assertEqual(len(result),1)
        self.assertEqual(result[0]['status'],'resolved')
        self.assertEqual(result[0]['peak_value'],Decimal('0.09'))
        self.assertEqual(result[0]['trigger_value'],Decimal('0.08'))
        self.assertEqual(result[0]['resolved_at'].minute,3)
    def test_replay_stable_and_negative_breach(self):
        values=self.observations(['-0.08','-0.09','0.01','0.10'])
        first=episodes('AAPL',values)
        self.assertEqual(first,episodes('AAPL',values))
        self.assertEqual(len(first),2)
        self.assertNotEqual(first[0]['alert_id'],first[1]['alert_id'])
        self.assertEqual(first[1]['status'],'active')
    def test_missing_reference_does_not_resolve(self):
        result=episodes('AAPL',self.observations(['0.08',None,None]))
        self.assertEqual(result[0]['status'],'active')
