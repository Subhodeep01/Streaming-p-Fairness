"""Standalone test measuring pure stream-buffering latency -- how long it
takes to receive items 1 through N from Kafka -- with NO reordering,
sketching, or fairness-check logic involved. Isolates raw message-arrival
cadence from every other cost the main consumer pipeline measures.

Usage:
    python measure_buffering_latency.py --topic_name <topic> --num_items 100 --out_path metrics/buffering_latency_test.csv
"""

import argparse
import time

import pandas as pd
from confluent_kafka import Consumer


def main():
    parser = argparse.ArgumentParser(description="Measure pure stream-buffering latency (no reordering)")
    parser.add_argument("--topic_name", type=str, required=True)
    parser.add_argument("--num_items", type=int, default=100)
    parser.add_argument("--out_path", type=str, default="metrics/buffering_latency_test.csv")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "buffering-latency-test",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([args.topic_name])

    rows = []
    t0 = time.perf_counter()
    prev_ms = 0.0
    count = 0
    print(f"Listening to '{args.topic_name}' -- measuring buffering time for {args.num_items} items...")
    while count < args.num_items:
        msg = consumer.poll(100.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        count += 1
        elapsed_ms = (time.perf_counter() - t0) * 1000
        rows.append({
            "item_number": count,
            "buffering_time_ms": elapsed_ms,          # cumulative time to receive items 1..item_number
            "interarrival_ms": elapsed_ms - prev_ms,  # time since the previous item
        })
        prev_ms = elapsed_ms

    consumer.close()
    df = pd.DataFrame(rows)
    df.to_csv(args.out_path, index=False)
    print(f"Saved buffering-time data ({len(df)} items) -> {args.out_path}")
    print(df.describe())


if __name__ == "__main__":
    main()
