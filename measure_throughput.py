"""bfairreorder-only throughput measurement: for one attribute, runs every
W/s/X sweep point (same SWEEPS dict simulate_stream.py's sweeps use) with
strategies=["bfairreorder"] only, max_windows=5000, and records
windows_per_sec = (landmark+1) * (1000/swap_ms) per run -- the
fairness-checked-windows/sec throughput (see conversation): one bfairreorder
call reorders a (window_size+landmark)-item chunk, and that single reordered
chunk gets sketch/query-checked across landmark+1 sliding windows, so
windows/sec = (call rate) * (windows produced per call).

Writes ONLY to metric_throughput/<dataset_label>/<attr>/sweep_{W,s,X}_throughput.csv
-- never touches metrics/, runs.csv, STATS.txt, or any _sweep_cache (calls
run_phase directly with save_artifacts=False, bypassing get_or_run_phase's
caching entirely).

Usage:
    python measure_throughput.py --attr sentiment --dataset datasets/tweets.csv --date-col stream_date
"""

import argparse
import os

import pandas as pd

from simulate_stream import (
    SWEEPS,
    attr_categories_and_percentages,
    derive_fairness_bounds,
    derive_fairness_counts,
    run_phase,
    METRICS_DIR,
)

THROUGHPUT_DIR = "metric_throughput"
BFAIR_ONLY_PHASE = {"label": "bfair_only_throughput", "max_windows": 5000, "strategies": ["bfairreorder"]}


def main():
    parser = argparse.ArgumentParser(description="Measure bfairreorder-only windows/sec throughput across W/s/X sweeps")
    parser.add_argument("--attr", type=str, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--date-col", type=str, required=True)
    parser.add_argument("--is-discrete", type=int, choices=[0, 1], default=1)
    parser.add_argument("--out-label", type=str, default=None)
    parser.add_argument("--dataset-label", type=str, default=None)
    parser.add_argument("--max-bins", type=int, default=5)
    parser.add_argument("--dayfirst", action="store_true")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--sweeps", choices=["W", "s", "X"], action="append",
                         help="Which sweep(s) to run; omit to run all three.")
    args = parser.parse_args()
    sweep_labels = args.sweeps if args.sweeps else list(SWEEPS.keys())

    out_label = args.out_label or args.attr
    dataset_label = args.dataset_label or os.path.splitext(os.path.basename(args.dataset))[0]
    out_dir = f"{THROUGHPUT_DIR}/{dataset_label}/{out_label}"
    os.makedirs(out_dir, exist_ok=True)

    categories, percentages = attr_categories_and_percentages(args.dataset, args.attr, args.is_discrete,
                                                                max_bins=args.max_bins)
    cardinality = len(categories)
    print(f"=== throughput: {out_dir} (cardinality={cardinality}) ===")

    fairness_by_block_size = {}

    for sweep_label in sweep_labels:
        spec = SWEEPS[sweep_label]
        param = spec["param"]
        # W=100 excluded here only (not in simulate_stream.SWEEPS itself, which
        # the main fairness-rerun pipeline also depends on) -- per-instance
        # throughput-measurement choice, not a change to the shared sweep grid.
        values = [v for v in spec["values"] if not (sweep_label == "W" and v == 100)]
        rows = []
        for value in values:
            config = dict(spec["fixed"])
            config[param] = value
            window_size = config["window_size"]
            block_size = config["block_size"]
            landmark = config["landmark"]

            if block_size < cardinality:
                print(f"  -- {sweep_label}={value} (W={window_size}, s={block_size}, X={landmark}) -- "
                      f"SKIPPED: block_size {block_size} < cardinality {cardinality} --")
                continue

            if block_size not in fairness_by_block_size:
                fairness_by_block_size[block_size] = (
                    derive_fairness_counts(percentages, block_size),
                    derive_fairness_bounds(percentages, block_size),
                )
            fairness_target, fairness_bounds = fairness_by_block_size[block_size]

            print(f"  -- {sweep_label}={value} (W={window_size}, s={block_size}, X={landmark}) --")
            all_runs = run_phase(out_dir, args.dataset, args.date_col, args.attr, args.is_discrete,
                                  window_size, block_size, landmark, fairness_target, fairness_bounds,
                                  BFAIR_ONLY_PHASE, args.runs, topic_prefix=f"{out_label.lower()}_throughput",
                                  save_artifacts=False, max_bins=args.max_bins, dayfirst=args.dayfirst)

            # bfairreorder_throughput_wps IS windows_per_sec now (Accumulator.summarize
            # computes (landmark+1)*(1000/swap_ms) directly) -- no need to rederive it here.
            rows.append(pd.DataFrame({
                "sweep_label": sweep_label,
                "sweep_value": value,
                "win_size": all_runs["win_size"],
                "block_size": all_runs["block_size"],
                "landmark": all_runs["landmark"],
                "cardinality": cardinality,
                "bfairreorder_swap_ms": all_runs["bfairreorder_swap_ms"],
                "windows_per_sec": all_runs["bfairreorder_throughput_wps"],
            }))

        if rows:
            combined = pd.concat(rows, ignore_index=True)
            out_path = f"{out_dir}/sweep_{sweep_label}_throughput.csv"
            combined.to_csv(out_path, index=False)
            summary = combined.groupby("sweep_value")["windows_per_sec"].agg(["mean", "std"])
            print(f"  saved -> {out_path}")
            print(summary.round(1).to_string())

    print(f"=== done: {out_dir} ===")


if __name__ == "__main__":
    main()
