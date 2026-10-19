"""Créer les buckets et vérifier que trades possède exactement 6 partitions."""
import os
import time

from botocore.exceptions import ClientError
from confluent_kafka.admin import AdminClient, NewTopic
from storage import connect_s3


def main():
    s3 = connect_s3()
    for attempt in range(60):
        try:
            s3.list_buckets()
            break
        except Exception:
            if attempt == 59:
                raise
            time.sleep(2)
    for bucket in ("bronze", "silver", "gold"):
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError as error:
            if str(error.response["Error"]["Code"]) not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            s3.create_bucket(Bucket=bucket)
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    topic = os.getenv("KAFKA_TOPIC", "trades")
    metadata = admin.list_topics(timeout=20)
    if topic not in metadata.topics:
        admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=1)],
                            request_timeout=30)[topic].result()
    metadata = admin.list_topics(topic=topic, timeout=20)
    info = metadata.topics[topic]
    if info.error or len(info.partitions) != 6:
        raise RuntimeError(f"Le topic {topic} doit avoir exactement 6 partitions")
    print(f"INIT_OK buckets=bronze,silver,gold topic={topic} partitions=6", flush=True)


if __name__ == "__main__":
    main()
