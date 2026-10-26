"""Écriture d'un lot Bronze et publication du manifeste en dernier."""
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config


SCHEMA = pa.schema([
    ("trade_id", pa.string()),
    ("symbol", pa.string()),
    ("price", pa.decimal128(18, 4)),
    ("quantity", pa.int64()),
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("_topic", pa.string()),
    ("_partition", pa.int32()),
    ("_offset", pa.int64()),
    ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ("_batch_id", pa.string()),
    ("_parse_error", pa.string()),
])


def connect_s3():
    return boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        config=Config(connect_timeout=5, read_timeout=30,
                      retries={"max_attempts": 3}, s3={"addressing_style": "path"}),
    )


def build_batch(messages):
    """Conserver les payloads JSON et leurs coordonnées Kafka."""
    coords = sorted((m.topic(), m.partition(), m.offset()) for m in messages)
    batch_id = hashlib.sha256(json.dumps(coords).encode()).hexdigest()[:24]
    now = datetime.now(timezone.utc)
    rows, raw, offsets = [], [], {}
    for message in messages:
        payload = message.value()
        row = {
            "_topic": message.topic(), "_partition": message.partition(),
            "_offset": message.offset(), "_ingested_at": now, "_batch_id": batch_id,
        }
        try:
            event = json.loads(payload)
            timestamp = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                raise ValueError("event_time doit inclure un fuseau horaire")
            price = Decimal(str(event["price"])).quantize(Decimal("0.0001"))
            quantity = int(event.get("quantity", event.get("volume")))
            if not price.is_finite() or abs(price) >= Decimal('100000000000000'):
                raise ValueError('Prix hors decimal(18,4)')
            if not -(2**63) <= quantity < 2**63:
                raise ValueError('Quantité hors int64')
            row.update(trade_id=str(event['trade_id']), symbol=str(event['symbol']),
                       price=price, quantity=quantity,
                       event_time=timestamp.astimezone(timezone.utc), _parse_error=None)
        except (ValueError, KeyError, TypeError, ArithmeticError) as error:
            # Bronze must archive poison records too, and must not block forever.
            row['_parse_error'] = f'{type(error).__name__}: {error}'
        rows.append(row)
        raw.append(payload + b"\n")
        key = (message.topic(), message.partition())
        offsets[key] = max(offsets.get(key, -1), message.offset() + 1)
    # Date stable pour le chemin en cas de rejeu du même lot.
    # Le véritable instant d'ingestion reste _ingested_at.
    timestamps = [row['event_time'] for row in rows if row.get('event_time') is not None]
    day = min(timestamps).date().isoformat() if timestamps else 'invalid'
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy", version="2.6")
    return batch_id, day, b"".join(raw), buffer.getvalue(), offsets


def archive(s3, bucket, messages):
    batch_id, day, raw, parquet, offsets = build_batch(messages)
    raw_key = f"json/trades/batch_date={day}/{batch_id}.jsonl"
    parquet_key = f"parquet/trades/batch_date={day}/{batch_id}.parquet"
    manifest_key = f"manifests/trades/batch_date={day}/{batch_id}.json"
    s3.put_object(Bucket=bucket, Key=raw_key, Body=raw, ContentType="application/x-ndjson")
    s3.put_object(Bucket=bucket, Key=parquet_key, Body=parquet)
    manifest = {
        "batch_id": batch_id, "row_count": len(messages),
        "json_key": raw_key, "parquet_key": parquet_key,
        "next_offsets": [{"topic": t, "partition": p, "offset": o}
                         for (t, p), o in sorted(offsets.items())],
    }
    s3.put_object(Bucket=bucket, Key=manifest_key,
                  Body=json.dumps(manifest).encode(), ContentType="application/json")
    return manifest, offsets
