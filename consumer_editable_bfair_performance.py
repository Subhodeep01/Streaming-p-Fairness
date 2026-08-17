"""Editable/reorderable sliding-window consumer with performance metrics.

Refactor of consumer_editable_performance.py that benchmarks five block
reordering strategies side by side on the same landmarked window:

  - naive_bfair_reorder : the original prefix-swap heuristic
                           (multi_proc_editable.max_rep_blocks/build_max_rep)
  - bfairreorder         : the optimal sliding-block reorderer (bfair.bfair_reorder)
  - internal_swap, greedy_swap, weighted_greedy_swap :
                           suboptimal baselines ported from bettergreedyswapper.py
                           (see baselines.py)

Metrics shared by all strategies (pre-swap sketching/querying, cardinality,
brute-force baseline) are stored unprefixed. Strategy-specific metrics
(swap time/memory, post-swap sketching/querying, fairness gains, latency
tail, throughput) are stored with a `<strategy_name>_` prefix. The
bfairreorder output is used to advance the live stream; every other
strategy is only used to compute its comparison metrics.
"""

import argparse
import json
import os
import time
import tracemalloc
from collections import deque

import numpy as np
import pandas as pd
from confluent_kafka import Consumer

from baselines import BASELINE_STRATEGIES
from bfair import bfair_reorder
from brute_forcer import brute_force
from multi_proc_editable import build_max_rep, max_rep_blocks
from utils import position_finder, sketcher, verify_sketch

MB = 1024 * 1024


def attach_extra_columns(seq, extra_rows, edited):
    """extra_rows[i] is the dict of extra-column values riding alongside
    seq[i]. Items sharing a category label are interchangeable to every
    reorder algorithm here (they only ever see/permute labels), so extra
    columns are reattached by stable per-category FIFO consumption: the
    k-th occurrence of a label in `seq` fills the k-th occurrence of that
    label in `edited`. Returns a list of extra-column dicts aligned to
    `edited`."""
    queues = {}
    for label, extra in zip(seq, extra_rows):
        queues.setdefault(label, deque()).append(extra)
    return [queues[label].popleft() for label in edited]


def naive_reorder(seq, fairness, window_size, block_size):
    m, rem_counts, unique = max_rep_blocks(seq, block_size, window_size, fairness)
    return build_max_rep(m, rem_counts, unique, block_size, fairness)


def bfair_reorder_variant(seq, floor, ceiling, block_size):
    """Builds proportions directly from the independent floor/ceiling bounds
    (not a derived integer reorder target), so bfair_reorder's own internal
    F=floor(p*s)/C=ceil(p*s) computation exactly reproduces floor/ceiling --
    guaranteeing its optimum aligns with what verify_sketch checks.

    For each category, floor(p*block_size)==floor[k] and ceil(p*block_size)
    ==ceiling[k] iff p*block_size lies in [floor[k], floor[k]+1), landing
    strictly inside (floor[k], floor[k]+1) whenever ceiling[k]>floor[k]. A
    single shared fractional offset frac=remainder/len(fractional_categories)
    (remainder = block_size - sum(floor)) applied to every such category
    satisfies that for all of them simultaneously while keeping the
    proportions summing to exactly 1 (required by bfair_reorder)."""
    total_floor = sum(floor.values())
    remainder = block_size - total_floor
    fractional_categories = [k for k in floor if ceiling[k] > floor[k]]
    frac = remainder / len(fractional_categories) if fractional_categories else 0.0
    proportions = {
        k: (floor[k] + (frac if ceiling[k] > floor[k] else 0)) / block_size
        for k in floor
    }
    return bfair_reorder(seq, proportions, block_size)


def identity_reorder(seq, fairness, window_size, block_size):
    """No-op 'strategy' used to measure before-reorder fairness across the
    same (landmark+1) shifted sliding windows every real strategy is scored
    on -- so fair_blocks_before is directly comparable to *_fair_blocks_after,
    and matches internal_swap_fair_blocks_after whenever internal_swap makes
    no changes (it very often can't: see internal_swap_reorder's donor-block
    requirement)."""
    return list(seq)


ALL_STRATEGIES = {
    "naive_bfair_reorder": naive_reorder,
    "bfairreorder": bfair_reorder_variant,
    "internal_swap": BASELINE_STRATEGIES["internal_swap"],
    "greedy_swap": BASELINE_STRATEGIES["greedy_swap"],
    "weighted_greedy_swap": BASELINE_STRATEGIES["weighted_greedy_swap"],
}
# greedy_swap / weighted_greedy_swap are slow at large MAX_WINDOWS scale
# (~10-25s per edit episode at large window/landmark sizes) -- callers pick
# a smaller subset via --strategies for large-MAX_WINDOWS runs instead of
# always paying for all five.
DEFAULT_STRATEGY_NAMES = ["naive_bfair_reorder", "bfairreorder", "internal_swap"]


class Accumulator:
    """Running sums/latency samples for one reordering strategy's post-swap work."""

    def __init__(self):
        self.swap_sum_ms = 0.0
        self.swap_mem_mb = 0.0
        self.swap_cnt = 0
        self.swap_latency = []

        self.sketch_sum_ms = 0.0
        self.sketch_mem_mb = 0.0
        self.query_sum_ms = 0.0
        self.query_mem_mb = 0.0
        self.query_latency = []

        self.fair_blocks_after = 0
        self.total_swaps = 0
        self.fair_windows_after = 0
        self.windows_checked = 0

    def avg(self, total, its):
        return total / its if its else 0.0

    def p90(self, samples):
        return float(np.percentile(samples, 90)) if samples else 0.0

    def summarize(self, prefix, its, total_blocks, landmark):
        swap_ms = self.avg(self.swap_sum_ms, self.swap_cnt)
        return {
            f"{prefix}_swap_ms": swap_ms,
            # f"{prefix}_swap_mem_mb": self.avg(self.swap_mem_mb, self.swap_cnt),
            f"{prefix}_swap_p90_ms": self.p90(self.swap_latency),
            # f"{prefix}_preproc_ms": self.avg(self.sketch_sum_ms, its),
            # f"{prefix}_preproc_mem_mb": self.avg(self.sketch_mem_mb, its),
            # f"{prefix}_query_ms": self.avg(self.query_sum_ms, its),
            # f"{prefix}_query_mem_mb": self.avg(self.query_mem_mb, its),
            # f"{prefix}_query_p90_ms": self.p90(self.query_latency),
            f"{prefix}_fair_blocks_after": self.avg(self.fair_blocks_after, its),
            f"{prefix}_pct_fair_after": 100 * self.fair_blocks_after / total_blocks if total_blocks else 0.0,
            # Fairness-checked windows/sec: one reorder call produces one
            # (landmark+1)-window-scored chunk, so throughput is (call rate) *
            # (windows produced per call) -- NOT 1000/(sketch_ms+query_ms),
            # which was blind to the reorder cost entirely (see conversation).
            f"{prefix}_throughput_wps": (landmark + 1) * (1000 / swap_ms) if swap_ms else 0.0,
            f"{prefix}_total_swaps": self.total_swaps,
            # A window (all window_size/block_size blocks in it) is fair, not just individual blocks.
            f"{prefix}_fair_windows_after": self.fair_windows_after,
            f"{prefix}_pct_fair_windows_after": 100 * self.fair_windows_after / self.windows_checked if self.windows_checked else 0.0,
        }


def sketch_step(effective_window, sketch, position, is_first):
    """Builds/updates `sketch` in place, returns (popped_ele, ms, mem_mb)."""
    tracemalloc.reset_peak()
    t0 = time.perf_counter()
    popped = sketcher(effective_window, sketch, position)
    t1 = time.perf_counter()
    _, peak = tracemalloc.get_traced_memory()
    return popped, (t1 - t0) * 1000, round(peak / MB, 4)


def query_step(sketch, position, block_size, floor, ceiling, popped):
    tracemalloc.reset_peak()
    t0 = time.perf_counter()
    query_result, fair_block = verify_sketch(sketch, position, block_size, floor, ceiling, popped)
    t1 = time.perf_counter()
    _, peak = tracemalloc.get_traced_memory()
    return query_result, fair_block, (t1 - t0) * 1000, round(peak / MB, 4)


def run_variant(name, reorder_fn, seq, attr, fairness, floor, ceiling, position, window_size,
                 block_size, landmark, acc: Accumulator):
    """Reorders `seq` with `reorder_fn` (driven by `fairness`, the reorder
    target -- non-zero, sums to block_size), re-sketches/queries every
    resulting window against the independent `floor`/`ceiling` bounds, and
    accumulates the strategy's post-swap metrics in `acc`. Returns the
    edited stream as a list of {attr: value} records.
    """
    tracemalloc.start()
    tracemalloc.reset_peak()
    t0 = time.perf_counter()
    edited = reorder_fn(seq, fairness, window_size, block_size)
    t1 = time.perf_counter()
    _, swap_peak = tracemalloc.get_traced_memory()
    swap_ms = (t1 - t0) * 1000
    acc.swap_sum_ms += swap_ms
    acc.swap_mem_mb += round(swap_peak / MB, 4)
    acc.swap_cnt += 1
    acc.swap_latency.append(swap_ms)
    acc.total_swaps += sum(1 for before, after in zip(seq, edited) if before != after)

    edited_df = pd.DataFrame(edited, columns=[attr])
    local_sketch = []
    blocks_per_window = window_size // block_size

    for i in range(landmark + 1):
        effective_window = (
            edited_df[attr][i:window_size + i] if not local_sketch
            else edited_df[attr][i + window_size - 1:i + window_size]
        )
        popped, sketch_ms, sketch_mem = sketch_step(effective_window, local_sketch, position, not local_sketch)
        query_result, fair_block, query_ms, query_mem = query_step(local_sketch, position, block_size, floor, ceiling, popped)

        acc.sketch_sum_ms += sketch_ms
        acc.sketch_mem_mb += sketch_mem
        acc.query_sum_ms += query_ms
        acc.query_mem_mb += query_mem
        acc.query_latency.append(query_ms + swap_ms)
        acc.fair_blocks_after += fair_block
        acc.windows_checked += 1
        if fair_block == blocks_per_window:
            acc.fair_windows_after += 1

    tracemalloc.stop()
    return edited_df.to_dict(orient="records"), local_sketch


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Editable consumer with per-strategy reordering metrics")
    parser.add_argument("--window_size", type=int, required=True)
    parser.add_argument("--block_size", type=int, required=True)
    parser.add_argument("--topic_name", type=str, required=True)
    parser.add_argument("--max_windows", type=int, required=True)
    parser.add_argument("--landmark", type=int, required=True)
    parser.add_argument("--brt_force", type=str, required=True)
    parser.add_argument("--strategies", type=str, default=",".join(DEFAULT_STRATEGY_NAMES),
                         help="Comma-separated subset of " + ",".join(ALL_STRATEGIES) +
                              f" to run (default: {','.join(DEFAULT_STRATEGY_NAMES)}).")
    parser.add_argument("--out_dir", type=str, default="metrics",
                         help="Directory the per-run metric_bfair_*.csv is written to.")
    parser.add_argument("--save_reordered_path", type=str, default="",
                         help="If set, writes bfairreorder's window+landmark reordered chunk "
                              "(one row per item, tagged by edit_event/position_in_chunk) to this CSV path.")
    parser.add_argument("--save_original_path", type=str, default="",
                         help="If set, writes the pre-reorder window+landmark chunk "
                              "(one row per item, tagged by edit_event/position_in_chunk) to this CSV path.")
    parser.add_argument("--extra_cols", type=str, default="",
                         help="Comma-separated extra source-row columns (e.g. movieId,avg_rating,vote_count) "
                              "to attach to each original_records/reordered_records row, reattached post-reorder "
                              "via stable per-category consumption. Only meaningful with --save_original_path/"
                              "--save_reordered_path set.")
    args = parser.parse_args()

    IN_TOPIC = args.topic_name
    WINDOW_SIZE = args.window_size
    BLOCK_SIZE = args.block_size
    MAX_WINDOWS = args.max_windows
    LANDMARK = args.landmark
    brt_force = args.brt_force == "True"
    GROUP_ID = "hospital-sliding-consumer"
    save_reordered_path = args.save_reordered_path
    save_original_path = args.save_original_path
    extra_cols = [c.strip() for c in args.extra_cols.split(",") if c.strip()]

    strategy_names = [s.strip() for s in args.strategies.split(",") if s.strip()]
    unknown = [s for s in strategy_names if s not in ALL_STRATEGIES]
    if unknown:
        raise ValueError(f"Unknown strategy name(s) {unknown}; choose from {list(ALL_STRATEGIES)}")
    if "bfairreorder" not in strategy_names:
        raise ValueError("bfairreorder must be included -- it drives the live stream forward.")
    STRATEGIES = {name: ALL_STRATEGIES[name] for name in strategy_names}

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([IN_TOPIC])

    df_temp = pd.read_csv("cleaned_input_files/cleaned_df.csv")
    try:
        print(pd.read_csv(f"cleaned_input_files/summary_{df_temp.columns[0]}.csv"))
    except FileNotFoundError:
        print("Discrete value, no binning info")

    fairness = {}
    floor = {}
    ceiling = {}
    position, fairness, floor, ceiling = position_finder(df_temp, fairness, floor, ceiling, BLOCK_SIZE)

    if "bfairreorder" in STRATEGIES:
        def _bfair_reorder_bound(seq, _fairness, window_size, block_size, _floor=floor, _ceiling=ceiling):
            return bfair_reorder_variant(seq, _floor, _ceiling, block_size)
        STRATEGIES["bfairreorder"] = _bfair_reorder_bound

    metrics = {
        "win_size": WINDOW_SIZE,
        "block_size": BLOCK_SIZE,
        "landmark": LANDMARK,
        "cardinality": len(position),
    }

    brt_swap_sum_ms = 0.0
    brt_query_sum_ms = 0.0
    brt_swap_latency = []
    brt_fair_blocks = 0

    fair_blocks_before = 0
    total_blocks = 0
    fair_windows_before = 0
    windows_before = 0
    edit_events = 0
    window_counter = 0
    count = 0
    its = 0

    accumulators = {name: Accumulator() for name in STRATEGIES}

    message_buffer = []
    sketch = []
    reordered_records = []
    original_records = []

    def process_window(batch):
        """Sketches/queries one WINDOW_SIZE-sized window (pre-swap)."""
        global sketch, count, fair_blocks_before, total_blocks
        tracemalloc.start()
        count += 1
        read_window = pd.DataFrame(batch)
        attr = read_window.columns[0]
        effective_window = read_window[attr] if not sketch else read_window[attr][-1:]

        popped, sketch_ms, _ = sketch_step(effective_window, sketch, position, not sketch)
        query_result, fair_block, query_ms, _ = query_step(sketch, position, BLOCK_SIZE, floor, ceiling, popped)
        tracemalloc.stop()

        total_blocks += WINDOW_SIZE // BLOCK_SIZE

        return query_result, fair_block, attr, read_window

    def edit_window(batch_landmarked, attr):
        """Runs every reordering strategy on the landmarked window."""
        global sketch, count, total_blocks, edit_events, fair_blocks_before
        global fair_windows_before, windows_before
        global brt_swap_sum_ms, brt_query_sum_ms, brt_fair_blocks
        global reordered_records, original_records

        count += LANDMARK
        read_window = pd.DataFrame(batch_landmarked)
        seq = read_window[attr].to_list()
        extra_rows = read_window[extra_cols].to_dict(orient="records") if extra_cols else None

        if save_original_path:
            for position_in_chunk, value in enumerate(seq):
                record = {
                    "edit_event": edit_events,
                    "position_in_chunk": position_in_chunk,
                    attr: value,
                }
                if extra_rows:
                    record.update(extra_rows[position_in_chunk])
                original_records.append(record)

        # Before-reorder fairness, scored across the same (landmark+1) shifted
        # sliding windows every strategy below is scored on (not just the single
        # pre-landmark window), so it's directly comparable to *_fair_blocks_after.
        before_acc = Accumulator()
        run_variant("before_reorder", identity_reorder, seq, attr, fairness, floor, ceiling, position,
                    WINDOW_SIZE, BLOCK_SIZE, LANDMARK, before_acc)
        fair_blocks_before += before_acc.fair_blocks_after
        fair_windows_before += before_acc.fair_windows_after
        windows_before += before_acc.windows_checked

        if brt_force:
            t0 = time.perf_counter()
            tracemalloc.start()
            _, _, ideal_stream = brute_force(seq, fairness, WINDOW_SIZE, BLOCK_SIZE, LANDMARK)
            _, brt_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            t1 = time.perf_counter()
            brt_ms = (t1 - t0) * 1000
            brt_swap_sum_ms += brt_ms
            brt_swap_latency.append(brt_ms)

            brt_sketch = []
            ideal_df = pd.DataFrame(ideal_stream, columns=[attr])
            for i in range(LANDMARK + 1):
                window = ideal_df[attr][i:WINDOW_SIZE + i]
                popped, _, _ = sketch_step(window, brt_sketch, position, not brt_sketch)
                _, fair_block, q_ms, _ = query_step(brt_sketch, position, BLOCK_SIZE, floor, ceiling, popped)
                brt_query_sum_ms += q_ms
                brt_fair_blocks += fair_block

            metrics["brt_swap_ms"] = brt_swap_sum_ms / edit_events if edit_events else brt_ms
            metrics["brt_query_ms"] = brt_query_sum_ms / count
            metrics["brt_swap_p90_ms"] = float(np.percentile(brt_swap_latency, 90))
            metrics["brt_fair_blocks"] = brt_fair_blocks

        edit_events += 1
        total_blocks += (WINDOW_SIZE // BLOCK_SIZE) * (LANDMARK + 1)

        results = {}
        for name, reorder_fn in STRATEGIES.items():
            buffer, local_sketch = run_variant(
                name, reorder_fn, seq, attr, fairness, floor, ceiling, position,
                WINDOW_SIZE, BLOCK_SIZE, LANDMARK, accumulators[name],
            )
            results[name] = (buffer, local_sketch)

        # BFairReOrder drives the live stream forward.
        buffer, sketch = results["bfairreorder"]
        primary_fair_blocks = accumulators["bfairreorder"].fair_blocks_after

        if save_reordered_path:
            buffer_extras = (
                attach_extra_columns(seq, extra_rows, [r[attr] for r in buffer])
                if extra_rows else None
            )
            for position_in_chunk, record in enumerate(buffer):
                row = {
                    "edit_event": edit_events - 1,
                    "position_in_chunk": position_in_chunk,
                    **record,
                }
                if buffer_extras:
                    row.update(buffer_extras[position_in_chunk])
                reordered_records.append(row)

        return buffer, primary_fair_blocks

    print(f"Listening to '{IN_TOPIC}' from broker 'localhost:9092' …")
    try:
        while True:
            msg = consumer.poll(100.0)
            if msg is None:
                print("No msg received")
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            message_buffer.append(json.loads(msg.value().decode("utf-8")))
            if len(message_buffer) > WINDOW_SIZE:
                message_buffer.pop(0)

            if len(message_buffer) == WINDOW_SIZE:
                window_counter += 1
                query_result, fair_block, attr, _ = process_window(message_buffer.copy())
                print(query_result, "\n")

                if fair_block < WINDOW_SIZE // BLOCK_SIZE:
                    for _ in range(LANDMARK):
                        window_counter += 1
                        msg = consumer.poll(100.0)
                        if msg is None:
                            print("No msg received")
                            continue
                        if msg.error():
                            print("Consumer error:", msg.error())
                            continue
                        message_buffer.append(json.loads(msg.value().decode("utf-8")))

                    message_buffer, fair_after = edit_window(message_buffer.copy(), attr)
                    print(f"Fair blocks after reorder (BFairReOrder) for {LANDMARK + 1} windows: {fair_after}\n")
                    for _ in range(LANDMARK):
                        message_buffer.pop(0)
                else:
                    fair_blocks_before += fair_block
                    windows_before += 1
                    if fair_block == WINDOW_SIZE // BLOCK_SIZE:
                        fair_windows_before += 1

            if window_counter >= MAX_WINDOWS:
                its += 1
                metrics["windows_covered"] = count // its
                metrics["total_blocks"] = total_blocks // its
                metrics["fair_blocks_before"] = fair_blocks_before // its
                metrics["pct_fair_before"] = 100 * fair_blocks_before / total_blocks if total_blocks else 0.0
                metrics["windows_before"] = windows_before // its
                metrics["fair_windows_before"] = fair_windows_before // its
                metrics["pct_fair_windows_before"] = 100 * fair_windows_before / windows_before if windows_before else 0.0

                for name in STRATEGIES:
                    prefix = name
                    metrics.update(accumulators[name].summarize(prefix, its, total_blocks, LANDMARK))

                window_counter = 0

            if its == 1:
                break
    except KeyboardInterrupt:
        print("Stopping consumer…")
    finally:
        metric_df = pd.DataFrame(metrics, index=[0])
        print("Metrics: ", metric_df)

        out_dir = args.out_dir
        if brt_force:
            out_dir = f"{args.out_dir}/baselines/brute_force"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/metric_bfair_{df_temp.columns[0]}WIN{WINDOW_SIZE}BLC{BLOCK_SIZE}LAN{LANDMARK}.csv"
        metric_df.to_csv(out_path, index=False)

        if save_reordered_path and reordered_records:
            os.makedirs(os.path.dirname(save_reordered_path), exist_ok=True)
            pd.DataFrame(reordered_records).to_csv(save_reordered_path, index=False)

        if save_original_path and original_records:
            os.makedirs(os.path.dirname(save_original_path), exist_ok=True)
            pd.DataFrame(original_records).to_csv(save_original_path, index=False)

        consumer.close()
