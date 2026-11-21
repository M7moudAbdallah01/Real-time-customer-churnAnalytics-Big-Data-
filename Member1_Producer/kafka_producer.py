import os
import csv
import json
import time
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "churn_raw"
CSV_FILE = "customer_churn_data.csv"
DELAY_SECONDS = 0.0
LIMIT = None
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

sent = 0
with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)          # each row becomes a dict: {column: value}
    for row in reader:

        record = {col: (val if val != "" else None) for col, val in row.items()}

        record["event_time"] = datetime.now().isoformat(timespec="seconds")

        producer.send(TOPIC, key=record["customerID"], value=record)

        sent += 1
        print(f"Sent {sent}: {record['customerID']} (Churn={record['Churn']})")

        if LIMIT and sent >= LIMIT:
            break
        time.sleep(DELAY_SECONDS)

producer.flush()
producer.close()
print(f"Done. {sent} messages published to '{TOPIC}'.")