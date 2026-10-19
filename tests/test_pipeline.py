"""Vérifications locales du générateur et du contrat d'archivage sans broker."""
import json
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from market import Market, SYMBOLS


class MarketTests(unittest.TestCase):
    def test_twenty_symbols_identifiers_and_valid_events(self):
        events = [Market().next_trade()]  # Vérifier aussi une première transaction.
        market = Market()
        events += [market.next_trade() for _ in range(2000)]
        self.assertEqual(set(e["symbol"] for e in events), set(SYMBOLS))
        self.assertEqual(len({e["trade_id"] for e in events}), len(events))
        for event in events:
            self.assertGreater(event["price"], 0)
            self.assertGreater(event["volume"], 0)
            self.assertIsNotNone(datetime.fromisoformat(event["event_time"]).tzinfo)
            self.assertEqual(json.loads(json.dumps(event)), event)

    def test_controlled_shock(self):
        market = Market()
        old = market.prices["FIN01"]
        market.shock()
        self.assertAlmostEqual(market.prices["FIN01"] / old, 1.08)


class StorageTests(unittest.TestCase):
    def test_raw_payload_offsets_and_parquet(self):
        try:
            import storage
            import pyarrow.parquet as pq
        except ImportError:
            self.skipTest("Installer requirements.txt pour tester Parquet et boto3")
        import io

        class Message:
            def __init__(self, p, offset, event):
                self.p, self.o = p, offset
                self.payload = json.dumps(event).encode()
            def topic(self): return "trades"
            def partition(self): return self.p
            def offset(self): return self.o
            def value(self): return self.payload

        market = Market()
        messages = [Message(0, 10, market.next_trade()),
                    Message(1, 5, market.next_trade()),
                    Message(0, 11, market.next_trade())]
        batch_id, day, raw, parquet, offsets = storage.build_batch(messages)
        self.assertEqual(raw.splitlines(), [m.value() for m in messages])
        self.assertEqual(offsets, {("trades", 0): 12, ("trades", 1): 6})
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("_offset").to_pylist(), [10, 5, 11])
        self.assertEqual(table.schema.field("event_time").type.unit, "us")
        self.assertEqual(storage.build_batch(messages)[0], batch_id)

        class FakeS3:
            def __init__(self): self.keys = []
            def put_object(self, **kw): self.keys.append(kw["Key"])

        fake = FakeS3()
        storage.archive(fake, "bronze", messages)
        self.assertTrue(fake.keys[-1].startswith("manifests/"))


if __name__ == "__main__":
    unittest.main()
