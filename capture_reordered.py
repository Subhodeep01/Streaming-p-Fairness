"""One-off capture-only pass: reruns ONLY the bfairreorder strategy at
5000 windows for every W/s/X sweep point already computed for --attr, to
backfill the original/reordered stream CSVs with extra per-row columns
(e.g. movieId/avg_rating/vote_count, or likes/stream_date) that the earlier
--save-reordered run didn't capture.

Writes ONLY <attr_dir>/original/*.csv and <attr_dir>/reordered/*.csv
(overwriting the column-poor versions from the earlier run) -- never reads
or writes _sweep_cache/, runs.csv, STATS.txt, or sweep_config_averages.csv.
Calls simulate_stream.run_phase directly (bypassing get_or_run_phase), so
the cache is never touched, and the returned per-run metrics DataFrame is
simply discarded.

Usage:
    python capture_reordered.py --attr sentiment --dataset datasets/tweets.csv \\
        --date-col stream_date --extra-cols likes,stream_date
"""

import argparse
import os

from simulate_stream import (
    METRICS_DIR,
    SWEEPS,
    attr_categories_and_percentages,
    derive_fairness_bounds,
    derive_fairness_counts,
    run_phase,
)

CAPTURE_PHASE = {
    "label": "3strat_5000win",
    "kind": "fast",
    "max_windows": 5000,
    "strategies": ["bfairreorder"],
}


def main():
    parser = argparse.ArgumentParser(description="Capture-only backfill of original/reordered stream CSVs")
    parser.add_argument("--attr", type=str, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--date-col", type=str, required=True)
    parser.add_argument("--is-discrete", type=int, choices=[0, 1], default=1)
    parser.add_argument("--out-label", type=str, default=None)
    parser.add_argument("--dataset-label", type=str, default=None)
    parser.add_argument("--extra-cols", type=str, required=True,
                         help="Comma-separated extra source-row columns to attach, e.g. movieId,avg_rating,vote_count")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--consumer-script", type=str, default="consumer_editable_bfair_performance.py",
                         help="Consumer script to invoke (default: consumer_editable_bfair_performance.py). "
                              "Use consumer_editable_bfair_performance_carryfix.py to also fix the extra_cols "
                              "carryover gap (missing extra_cols on the window_size prefix carried over from "
                              "the previous edit_event's reordered buffer).")
    args = parser.parse_args()
    extra_cols = [c.strip() for c in args.extra_cols.split(",") if c.strip()]

    out_label = args.out_label or args.attr
    dataset_label = args.dataset_label or os.path.splitext(os.path.basename(args.dataset))[0]
    attr_dir = f"{METRICS_DIR}/{dataset_label}/{out_label}"
    categories, percentages = attr_categories_and_percentages(args.dataset, args.attr, args.is_discrete)
    cardinality = len(categories)

    print(f"=== capture-only backfill: {attr_dir} (extra_cols={extra_cols}) ===")

    fairness_by_block_size = {}
    for sweep_label, spec in SWEEPS.items():
        param = spec["param"]
        for value in spec["values"]:
            config = dict(spec["fixed"])
            config[param] = value
            window_size = config["window_size"]
            block_size = config["block_size"]
            landmark = config["landmark"]

            if block_size < cardinality:
                print(f"\n-- {sweep_label}={value} (W={window_size}, s={block_size}, X={landmark}) -- "
                      f"SKIPPED: block_size {block_size} < cardinality {cardinality} --")
                continue

            if block_size not in fairness_by_block_size:
                fairness_by_block_size[block_size] = (
                    derive_fairness_counts(percentages, block_size),
                    derive_fairness_bounds(percentages, block_size),
                )
            fairness_target, fairness_bounds = fairness_by_block_size[block_size]

            print(f"\n-- capture {sweep_label}={value} (W={window_size}, s={block_size}, X={landmark}) --")
            run_phase(attr_dir, args.dataset, args.date_col, args.attr, args.is_discrete,
                      window_size, block_size, landmark, fairness_target, fairness_bounds,
                      CAPTURE_PHASE, args.runs, topic_prefix=f"{out_label.lower()}_capture",
                      save_artifacts=True, extra_cols=extra_cols, consumer_script=args.consumer_script)

    print(f"\n=== done: {attr_dir} original/reordered CSVs backfilled with {extra_cols} ===")


if __name__ == "__main__":
    main()
