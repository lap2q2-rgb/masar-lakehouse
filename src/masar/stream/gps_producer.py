import json
import sys
import time
from collections import Counter
from kafka import KafkaProducer

TOPIC = "masar.gps.pings"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    futures = []
    with open("data/raw/gps/sample.ndjson") as f:
        for i, line in enumerate(f):
            if i >= limit:
                break
            ping = json.loads(line)
            # key = trip_id => same partition => ordered pings per trip
            futures.append(producer.send(TOPIC, key=ping["trip_id"], value=ping))
            time.sleep(0.005)
    producer.flush()
    per_partition = Counter(fut.get().partition for fut in futures)
    print(f"sent {len(futures)} pings; per partition: {dict(sorted(per_partition.items()))}")
