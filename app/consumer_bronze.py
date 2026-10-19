"""Un consommateur Bronze continu, distinct du futur lecteur Spark."""
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition
from storage import archive, connect_s3


def main():
    running = True
    closing = False

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "bronze-trades-writer"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": 300000,
    })
    s3 = connect_s3()
    bucket = os.getenv("BRONZE_BUCKET", "bronze")
    size = int(os.getenv("BATCH_SIZE", "10000"))
    seconds = float(os.getenv("BATCH_SECONDS", "30"))
    if size <= 0 or seconds <= 0:
        raise ValueError("Taille et délai du lot doivent être positifs")
    messages = []
    last_flush = time.monotonic()

    def flush():
        nonlocal last_flush
        if not messages:
            return
        manifest, offsets = archive(s3, bucket, messages)
        # Aucun offset n'est validé avant JSONL + Parquet + manifeste.
        consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()],
                        asynchronous=False)
        print(f"BRONZE lignes={len(messages)} batch={manifest['batch_id']} "
              f"partitions={len(offsets)} parquet={manifest['parquet_key']}", flush=True)
        messages.clear()
        last_flush = time.monotonic()

    def revoked(client, partitions):
        # Vider le lot tant que les partitions peuvent encore être validées.
        if not closing:
            flush()

    def lost(client, partitions):
        raise RuntimeError("Partitions perdues : arrêt sans validation du lot en mémoire")

    topic = os.getenv("KAFKA_TOPIC", "trades")
    consumer.subscribe([topic], on_revoke=revoked, on_lost=lost)
    print(f"BRONZE_DEMARRE topic={topic} lot={size} delai={seconds}s", flush=True)
    try:
        while running:
            message = consumer.poll(0.5)
            if message is not None:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        raise RuntimeError(str(message.error()))
                else:
                    messages.append(message)
            if messages and (len(messages) >= size or
                             time.monotonic() - last_flush >= seconds):
                flush()
        # Arrêt normal : écrire aussi le dernier lot incomplet.
        flush()
    finally:
        # Après une exception d'écriture, empêcher un flush implicite au close.
        closing = True
        consumer.close()


if __name__ == "__main__":
    main()
