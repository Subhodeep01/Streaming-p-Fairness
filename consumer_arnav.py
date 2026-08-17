import argparse
import json
import math
from collections import defaultdict

import pandas as pd
from confluent_kafka import Consumer

from utils import sketcher, verify_sketch
from bfair import bfair_reorder


parser = argparse.ArgumentParser(description="Arnav's p-fairness sliding-window consumer")
parser.add_argument("--topic",        type=str, required=True)
parser.add_argument("--window_size",  type=int, required=True)
parser.add_argument("--block_size",   type=int, required=True)
parser.add_argument("--attribute",    type=str, default="GENDER")
parser.add_argument("--max_windows",  type=int, default=50)
args = parser.parse_args()

assert args.window_size % args.block_size == 0, "block_size must divide window_size evenly"

TOPIC       = args.topic
WINDOW_SIZE = args.window_size
BLOCK_SIZE  = args.block_size
ATTRIBUTE   = args.attribute
MAX_WINDOWS = args.max_windows


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id":          f"arnav-consumer-{ATTRIBUTE}",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])
print(f"Subscribed to '{TOPIC}' | window={WINDOW_SIZE} block={BLOCK_SIZE} attr={ATTRIBUTE}")


window_buffer = []
sketch        = []
fairness      = {}
floor         = {}
ceiling       = {}
position      = {}

window_count   = 0
fair_original  = 0
fair_reordered = 0
total_blocks   = 0


def build_fairness(values):
    unique = sorted(set(values))
    n      = len(unique)
    p      = 1 / n
    pos    = {v: i for i, v in enumerate(unique)}
    flr    = {v: math.floor(p * BLOCK_SIZE) for v in unique}
    cel    = {v: math.ceil(p * BLOCK_SIZE) for v in unique}
    base   = BLOCK_SIZE // n
    fair   = {v: base for v in unique}
    for i, v in enumerate(unique):
        if i < BLOCK_SIZE % n:
            fair[v] += 1
    return pos, fair, flr, cel


def reorder_window(rows, fairness_counts):
    total = sum(fairness_counts.values())
    if not total:
        return rows
    proportions = {k: v / total for k, v in fairness_counts.items()}
    try:
        return bfair_reorder(rows, proportions, BLOCK_SIZE, attr_fn=lambda r: str(r.get(ATTRIBUTE, "")))
    except Exception as e:
        print(f"  [reorder] failed: {e}")
        return rows


def count_fair_blocks(rows, pos, flr, cel):
    blocks_per_window = WINDOW_SIZE // BLOCK_SIZE
    fair_count = 0
    for b in range(blocks_per_window):
        block_rows = rows[b * BLOCK_SIZE : (b + 1) * BLOCK_SIZE]
        counts = defaultdict(int)
        for r in block_rows:
            counts[str(r.get(ATTRIBUTE, ""))] += 1
        if all(flr.get(v, 0) <= counts.get(v, 0) <= cel.get(v, BLOCK_SIZE) for v in flr):
            fair_count += 1
    return fair_count


def process_window():
    global fair_original, fair_reordered, total_blocks, fairness, floor, ceiling, position

    rows = list(window_buffer)

    if not fairness:
        vals = [str(r.get(ATTRIBUTE, "")) for r in rows]
        position, fairness, floor, ceiling = build_fairness(vals)
        print(f"Fairness constraints: {fairness} | floor: {floor} | ceiling: {ceiling}")

    attr_series = pd.Series([str(r.get(ATTRIBUTE, "")) for r in rows])

    if len(sketch) == 0:
        popped = sketcher(attr_series, sketch, position)
    else:
        popped = sketcher(attr_series.iloc[-1:], sketch, position)

    query_result, fair_block = verify_sketch(sketch, position, BLOCK_SIZE, floor, ceiling, popped)

    blocks_this_window = WINDOW_SIZE // BLOCK_SIZE
    total_blocks  += blocks_this_window
    fair_original += fair_block

    is_fair = bool(query_result and "✅" in query_result[0])

    reordered_rows  = reorder_window(rows, fairness)
    reordered_fairs = count_fair_blocks(reordered_rows, position, floor, ceiling)
    fair_reordered += reordered_fairs

    print(
        f"Window {window_count:>3} | "
        f"{'✅ FAIR' if is_fair else '❌ NOT FAIR':12} | "
        f"fair blocks: {fair_block}/{blocks_this_window} original, "
        f"{reordered_fairs}/{blocks_this_window} reordered"
    )


try:
    while window_count < MAX_WINDOWS:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        row = json.loads(msg.value().decode("utf-8"))
        window_buffer.append(row)

        if len(window_buffer) > WINDOW_SIZE:
            window_buffer.pop(0)

        if len(window_buffer) == WINDOW_SIZE:
            window_count += 1
            process_window()

except KeyboardInterrupt:
    print("\nStopped.")

finally:
    consumer.close()

    print("\n" + "="*60)
    print("FINAL METRICS")
    print("="*60)
    print(f"Windows processed          : {window_count}")
    print(f"Total blocks               : {total_blocks}")
    print(f"Fair blocks (original)     : {fair_original}  ({fair_original*100/max(total_blocks,1):.1f}%)")
    print(f"Fair blocks (reordered)    : {fair_reordered}  ({fair_reordered*100/max(total_blocks,1):.1f}%)")
    print("="*60)
