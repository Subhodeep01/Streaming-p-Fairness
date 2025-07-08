import os
import time
import pandas as pd
import json
from datetime import datetime
from confluent_kafka import Producer
from user_inputs import cleaned_df, date_col, attr, CSV
import socket


TOPIC = "hospital-raw-age-v2"
SLEEP = 0.05  # seconds between rows

# Configure the Kafka Producer
producer_conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(producer_conf)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def main() -> None:
    for _, row in cleaned_df.iterrows():
        # Prepare message
        key = "stream".encode('utf-8')
        value = row.to_json()

        # Produce message
        producer.produce(
            topic=TOPIC,
            key=key,
            value=value,
            callback=delivery_report
        )

        # Poll to handle delivery reports
        producer.poll(0)
        time.sleep(SLEEP)

    # Wait for all messages to be delivered
    producer.flush()
    print(f"✅ Published {len(cleaned_df)} rows to topic '{TOPIC}'. Done.")


if __name__ == "__main__":
    main()
