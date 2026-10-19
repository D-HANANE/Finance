
"""Publier des transactions et mesurer les confirmations Kafka."""

import json
import os
import signal
import time

from confluent_kafka import Producer
from market import Market, SYMBOLS


def main():
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    rate = int(os.getenv("TRADES_PER_SECOND", "220"))
    if rate < 200:
        raise ValueError(
            "FIN-01 demande au moins 200 transactions/s au total."
        )

    producer = Producer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
        "message.timeout.ms": 30000,
    })

    topic = os.getenv("KAFKA_TOPIC", "trades")
    market = Market()

    sent = delivered = failed = previous_delivered = 0
    delivered_symbols = set()

    started = last_report = time.monotonic()

    shock_at = float(os.getenv("SHOCK_AFTER_SECONDS", "360"))
    shock_every = float(os.getenv("SHOCK_EVERY_SECONDS", "300"))

    if shock_at < 0 or shock_every <= 0:
        raise ValueError("Délais de choc invalides.")

    def delivery(error, message):
        nonlocal delivered, failed

        if error:
            failed += 1
            print(f"LIVRAISON_ECHEC {error}", flush=True)
        else:
            delivered += 1
            key = message.key()
            if key is not None:
                delivered_symbols.add(key.decode("utf-8"))

    print(
        f"DEMARRE cible={rate}/s "
        f"symboles={len(SYMBOLS)} topic={topic}",
        flush=True,
    )

    try:
        while running:
            now = time.monotonic()
            elapsed = now - started

            if elapsed >= shock_at:
                market.shock()
                print(
                    "CHOC symbol=FIN01 variation_prix=+8%",
                    flush=True,
                )
                shock_at += shock_every

            # Maintain the target rate using a monotonic clock.
            due = min(
                max(0, int(elapsed * rate) - sent),
                rate,
            )

            for _ in range(due):
                event = market.next_trade()
                payload = json.dumps(
                    event, separators=(",", ":")
                ).encode("utf-8")

                while running:
                    try:
                        producer.produce(
                            topic,
                            key=event["symbol"],
                            value=payload,
                            on_delivery=delivery,
                        )
                        sent += 1
                        break
                    except BufferError:
                        producer.poll(0.1)

            producer.poll(0)

            now = time.monotonic()

            if now - last_report >= 5:
                measured = (
                    (delivered - previous_delivered)
                    / (now - last_report)
                )
                status = "OK" if measured >= 200 else "SOUS_200"

                print(
                    f"DEBIT confirme={measured:.1f}/s "
                    f"fenetre=5s statut={status} "
                    f"total_confirme={delivered} "
                    f"symboles_confirmes={len(delivered_symbols)} "
                    f"erreurs={failed}",
                    flush=True,
                )

                previous_delivered = delivered
                last_report = now

            time.sleep(0.002)

    finally:
        remaining = producer.flush(20)

        print(
            f"ARRET soumis={sent} confirmes={delivered} "
            f"erreurs={failed} non_livres={remaining}",
            flush=True,
        )

        if remaining or failed:
            raise RuntimeError(
                "Toutes les transactions n'ont pas été livrées."
            )


if __name__ == "__main__":
    main()
