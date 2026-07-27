"""bfair-only X (landmark) ablation, fixed window_size=500 / block_size=25.

Sweeps only the landmark parameter (X) across an integer range (default 1-100)
while running ONLY the "bfairreorder" strategy -- no naive/baseline comparison,
no brute force, no significance testing (nothing to compare bfair against here).
Dataset/attribute are fully generic via CLI flags, matching simulate_stream.py's
convention.

Reuses simulate_stream.py's shuffle/produce/consume/collect machinery
(get_or_run_phase, build_report, derive_fairness_*) unchanged -- this script only
supplies a different (fixed W/s, fine-grained X) sweep grid and a bfair-only phase,
mirroring the pattern simulate_intersectional.py / capture_reordered.py already use.

Landmark values are NOT required to be multiples of block_size here, unlike
simulate_stream.validate_config's stricter check (see validate_bfair_window_landmark
below) -- bfair_reorder handles non-tileable window+landmark lengths natively via
its leftover-region regime, and this script never runs the strategies that check
required that stricter divisibility.

Usage:
    python simulate_bfair_x_ablation.py --attr genre-1 --dataset datasets/movie_vote_summary.csv --date-col stream_date
    python simulate_bfair_x_ablation.py --attr genre-1 --dataset datasets/movie_vote_summary.csv --date-col stream_date --runs 2 --x-min 1 --x-max 3
"""

import argparse
import os
import time

import pandas as pd

from simulate_stream import (
    METRICS_DIR,
    PRIMARY_STRATEGY,
    attr_categories_and_percentages,
    build_report,
    derive_fairness_bounds,
    derive_fairness_counts,
    get_or_run_phase,
)

WINDOW_SIZE = 500
BLOCK_SIZE = 25
DEFAULT_RUNS = 10
PHASE = {"label": "bfair_only", "max_windows": 5000, "strategies": [PRIMARY_STRATEGY]}


def validate_bfair_window_landmark(window_size, block_size, landmark):
    assert window_size % block_size == 0, f"block_size {block_size} must divide window_size {window_size}"
    assert landmark <= window_size, f"landmark {landmark} must not exceed window_size {window_size}"


def run_x_ablation(args):
    categories, percentages = attr_categories_and_percentages(args.dataset, args.attr, args.is_discrete)
    cardinality = len(categories)
    assert BLOCK_SIZE >= cardinality, (
        f"block_size {BLOCK_SIZE} < cardinality {cardinality} for attr={args.attr} "
        f"dataset={args.dataset} -- can't give every category a slot in one block"
    )

    fairness_target = derive_fairness_counts(percentages, BLOCK_SIZE)
    fairness_bounds = derive_fairness_bounds(percentages, BLOCK_SIZE)
    x_values = list(range(args.x_min, args.x_max + 1, args.x_step))

    print(f"\n=== bfair-only X-ablation: attr={args.out_label} dataset={args.dataset} "
          f"window_size={WINDOW_SIZE} block_size={BLOCK_SIZE} X in [{args.x_min}, {args.x_max}] step {args.x_step} "
          f"({len(x_values)} points, {args.runs} runs each) ===")
    print(f"  categories(sorted)={categories} percentages={[f'{p:.4f}' for p in percentages]}")
    print(f"  reorder_target={fairness_target} floors={fairness_bounds[0]} ceilings={fairness_bounds[1]}")

    os.makedirs(args.attr_dir, exist_ok=True)

    report_lines = [
        f"bfair-only X-Ablation -- attr={args.out_label} -- window_size={WINDOW_SIZE} block_size={BLOCK_SIZE}",
        "=" * 100,
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Dataset: {args.dataset} (attribute: {args.attr}), row order reshuffled per run (seed=run_id)",
        f"X (landmark) swept over {x_values}",
        "",
    ]
    run_frames = []
    summary_rows = []

    for x in x_values:
        validate_bfair_window_landmark(WINDOW_SIZE, BLOCK_SIZE, x)
        print(f"\n-- X={x} (W={WINDOW_SIZE}, s={BLOCK_SIZE}) --")
        all_runs = get_or_run_phase(
            args.attr_dir, args.dataset, args.date_col, args.attr, args.is_discrete,
            WINDOW_SIZE, BLOCK_SIZE, x, fairness_target, fairness_bounds, PHASE, args.runs,
            topic_prefix=f"{args.out_label.lower()}_bfairablation", use_cache=True, save_artifacts=False,
        )
        run_frames.append(all_runs)

        report_lines.append(build_report(args.out_label, args.dataset, PHASE, PHASE["strategies"], args.runs, all_runs))
        report_lines.append("")

        avg = all_runs.drop(columns=["run_id"]).mean(numeric_only=True).to_dict()
        avg["landmark"] = x
        summary_rows.append(avg)

    combined = pd.concat(run_frames, ignore_index=True)
    runs_path = f"{args.attr_dir}/bfair_x_ablation_runs.csv"
    combined.to_csv(runs_path, index=False)
    print(f"\n  saved all runs -> {runs_path}")

    report_path = f"{args.attr_dir}/bfair_x_ablation_STATS.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"  saved report -> {report_path}")

    summary = pd.DataFrame(summary_rows)
    cols = ["landmark"] + [c for c in summary.columns if c != "landmark"]
    summary = summary[cols]
    summary_path = f"{args.attr_dir}/bfair_x_ablation_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"  saved per-X summary -> {summary_path}")


def main():
    parser = argparse.ArgumentParser(description="bfair-only landmark (X) ablation, fixed window_size=500/block_size=25")
    parser.add_argument("--attr", type=str, required=True, help="Attribute/column name to stream and monitor.")
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--date-col", type=str, required=True)
    parser.add_argument("--is-discrete", type=int, choices=[0, 1], default=1,
                         help="1 (default): attr is already categorical/pre-binned. "
                              "0: attr is raw/continuous and needs auto-binning first.")
    parser.add_argument("--out-label", type=str, default=None, help="Label used in output filenames; defaults to --attr.")
    parser.add_argument("--dataset-label", type=str, default=None,
                         help="Top-level metrics/ subdirectory name; defaults to --dataset's basename.")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--x-min", type=int, default=1)
    parser.add_argument("--x-max", type=int, default=100)
    parser.add_argument("--x-step", type=int, default=1)
    args = parser.parse_args()

    args.out_label = args.out_label or args.attr
    args.dataset_label = args.dataset_label or os.path.splitext(os.path.basename(args.dataset))[0]
    args.attr_dir = f"{METRICS_DIR}/{args.dataset_label}/{args.out_label}"

    run_x_ablation(args)


if __name__ == "__main__":
    main()
