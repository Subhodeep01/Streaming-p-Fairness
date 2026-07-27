"""Attribute/dataset-generic stream-fairness simulation harness.

CLI-driven tool:
  --mode single   one fixed (window_size, block_size, landmark) config, 10 runs
  --mode sweep    W/s/X parameter sweeps (values ported from the AGE_bin sweep),
                  each point run the same way as --mode single

Every config -- single or swept -- is measured through the same two-phase
methodology:
  Phase A: all five strategies (naive_bfair_reorder, bfairreorder,
           internal_swap, greedy_swap, weighted_greedy_swap), capped at
           max_windows=500 (the two greedy baselines are too slow to run at
           the full 5000-window scale).
  Phase B: the three fast strategies (naive_bfair_reorder, bfairreorder,
           internal_swap) only, at the full max_windows=5000.

Fairness proportions are derived from the attribute's real distribution in
the source dataset:
  --is-discrete 1 (default): attribute is already categorical/pre-binned
      (e.g. AGE_bin, GENDER, OUTCOME) -- proportions come straight from
      value_counts.
  --is-discrete 0: attribute is a raw continuous column that needs binning
      first, via the same utils.bin_with_min_pct(max_bins=5, min_pct=0.15)
      logic user_inputs.py itself uses -- order-independent (quantile-based),
      so it's safe to precompute once from the unshuffled source dataset.

Output is organized per dataset/attribute: metrics/<dataset_label>/<out_label>/
(sweep runs.csv/STATS.txt, _sweep_cache/, and optionally reordered/). Nothing
disposable is left in metrics/ -- the per-run metric_bfair_*.csv the consumer
writes is staged in metrics/_tmp/ and deleted right after being read.

--save-reordered (only meaningful with bfairreorder in play, which is every
run) persists bfairreorder's window+landmark reordered chunks -- one
consolidated CSV per (config, phase) under <attr_dir>/reordered/, covering
all `runs` runs (tagged by run_id/edit_event), not one file per run.

Usage:
    python simulate_stream.py --attr AGE_bin --mode single
    python simulate_stream.py --attr GENDER --mode sweep
    python simulate_stream.py --attr topic --dataset datasets/tweets.csv --date-col stream_date --mode sweep --save-reordered
"""

import argparse
import math
import os
import subprocess
import sys
import time

import pandas as pd
from scipy import stats

from utils import bin_with_min_pct

# Our own stdout/stderr may be redirected to a file under a non-UTF-8 locale
# (cp1252 on Windows); child processes' captured output can contain emoji
# (e.g. utils.verify_sketch's fairness messages), so printing it back out
# via print() must not crash. Reconfigure eagerly rather than special-casing
# every print call.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

SUBPROCESS_ENV = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}

METRICS_DIR = "metrics"
TMP_DIR = f"{METRICS_DIR}/_tmp"

RUN_CONFIG_COLUMNS = ["win_size", "block_size", "landmark", "cardinality"]
SHARED_COLUMNS = [
    "windows_covered", "total_blocks", "fair_blocks_before", "pct_fair_before",
    "windows_before", "fair_windows_before", "pct_fair_windows_before",
]
STRATEGY_METRIC_SUFFIXES = [
    "swap_ms", "swap_p90_ms", "fair_blocks_after", "pct_fair_after", "throughput_wps", "total_swaps",
    "fair_windows_after", "pct_fair_windows_after",
]
PRIMARY_STRATEGY = "bfairreorder"

ALL_PHASES = [
    {
        "label": "5strat_500win",
        "kind": "slow",
        "max_windows": 500,
        "strategies": ["naive_bfair_reorder", "bfairreorder", "internal_swap", "greedy_swap", "weighted_greedy_swap"],
    },
    {
        "label": "3strat_5000win",
        "kind": "fast",
        "max_windows": 5000,
        "strategies": ["naive_bfair_reorder", "bfairreorder", "internal_swap"],
    },
]
# Overridable via --phase; defaults to both. Every function below iterates
# PHASES, never ALL_PHASES directly, so main() can narrow this once at
# startup to run just the fast (5000-window) or slow (500-window) phase.
PHASES = list(ALL_PHASES)

# Sweep values/fixed-parameters ported verbatim from the AGE_bin sweep.
SWEEPS = {
    "W": {"param": "window_size", "values": [100, 200, 500, 1000, 2000], "fixed": {"block_size": 50, "landmark": 100}},
    "s": {"param": "block_size", "values": [25, 50, 100, 250], "fixed": {"window_size": 1000, "landmark": 500}},
    "X": {"param": "landmark", "values": [50, 100, 250, 500], "fixed": {"window_size": 500, "block_size": 50}},
}


def run(cmd, stdin_text):
    result = subprocess.run(cmd, input=stdin_text, text=True, capture_output=True, env=SUBPROCESS_ENV,
                             encoding="utf-8", errors="replace")
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}")
    return result


def shuffle_dataset(dataset_path, run_id, shuffled_name):
    """Writes a row-shuffled copy of the source dataset into datasets/ and
    returns its bare name (as user_inputs.py's dataset prompt expects)."""
    df = pd.read_csv(dataset_path)
    shuffled = df.sample(frac=1, random_state=run_id).reset_index(drop=True)
    shuffled.to_csv(f"datasets/{shuffled_name}.csv", index=False)
    return shuffled_name


def attr_categories_and_percentages(dataset_path, attr, is_discrete, max_bins=5):
    """Sorted categories (matching utils.position_finder's `unique.sort()`
    order, which fixes the CONSUMER_STDIN input order) and their real
    proportions in the source dataset. max_bins only applies to the
    is_discrete=0 path, and must match the max_bins passed to user_inputs.py
    (via producer_stdin) for a given run -- otherwise the fairness
    percentages computed here would be derived from a different binning
    than what the live stream actually assigns categories with."""
    df = pd.read_csv(dataset_path)
    if is_discrete:
        counts = df[attr].value_counts(normalize=True)
        categories = sorted(counts.index.tolist())
        percentages = [float(counts[c]) for c in categories]
    else:
        codes, _ = bin_with_min_pct(df[attr], max_bins=max_bins, min_pct=0.15)
        counts = pd.Series(codes).value_counts(normalize=True)
        categories = sorted(counts.index.tolist())
        percentages = [float(counts[c]) for c in categories]
    return categories, percentages


def derive_fairness_bounds(percentages, block_size):
    """Per-category (floor, ceiling) pair derived independently from each
    category's real proportion: floor = floor(p*block_size), ceiling =
    ceil(p*block_size). Unlike derive_fairness_counts below, floors don't
    need to sum to block_size and can legitimately be 0 for very rare
    categories -- used only for the fairness *check* (a block is fair for a
    category only if its count lands within [floor, ceiling], not just
    >= floor)."""
    floors = [int(math.floor(p * block_size)) for p in percentages]
    ceilings = [int(math.ceil(p * block_size)) for p in percentages]
    return floors, ceilings


def derive_fairness_counts(percentages, block_size):
    """Largest-remainder apportionment of `percentages` into integer
    per-block counts summing exactly to block_size. This is the *reorder
    target* fed to naive_reorder/baselines.py's algorithms (bfairreorder
    builds its own proportions straight from derive_fairness_bounds instead,
    see bfair_reorder_variant) -- it must stay within [floor, ceiling] per
    category (guaranteed by construction: pure largest-remainder never moves
    a category more than one unit above its floor) and sum to exactly
    block_size, since multi_proc_editable.build_max_rep's repeated "base
    pattern" is sized off that sum. A category can legitimately land on 0
    here now (multi_proc_editable.py treats 0 as "no minimum, place freely"
    instead of dividing by it)."""
    raw = [p * block_size for p in percentages]
    floors = [int(math.floor(r)) for r in raw]
    remainder = block_size - sum(floors)
    order = sorted(range(len(raw)), key=lambda i: raw[i] - floors[i], reverse=True)
    counts = list(floors)
    for i in order[:remainder]:
        counts[i] += 1
    return counts


def validate_config(window_size, block_size, landmark):
    assert window_size % block_size == 0, f"block_size {block_size} must divide window_size {window_size}"
    assert landmark <= window_size, f"landmark {landmark} must not exceed window_size {window_size}"
    assert (window_size + landmark) % block_size == 0, (
        f"block_size {block_size} must divide window_size+landmark {window_size + landmark}"
    )


def describe(series):
    """mean, median, mode, sample std dev for one metric's run values."""
    mean = series.mean()
    median = series.median()
    modes = series.mode()
    mode = modes.iloc[0] if not modes.empty else float("nan")
    multimodal_note = "" if modes.nunique() <= 1 or series.nunique() == len(series) else " (multiple modes, first shown)"
    std = series.std(ddof=1) if len(series) > 1 else 0.0
    return mean, median, mode, std, multimodal_note


def build_report(out_label, dataset_path, phase, strategies, runs, all_runs: pd.DataFrame) -> str:
    baselines = [s for s in strategies if s != PRIMARY_STRATEGY]
    window_size = int(all_runs["win_size"].iloc[0])
    block_size = int(all_runs["block_size"].iloc[0])
    landmark = int(all_runs["landmark"].iloc[0])

    lines = []
    lines.append(f"Stream Fairness Simulation -- {out_label} ({phase['label']}) -- {runs}-Run Statistical Report")
    lines.append("=" * 70)
    lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Dataset: {dataset_path}, row order reshuffled per run (seed=run_id)")
    lines.append(f"Runs: {runs}   window_size={window_size}  block_size={block_size}  landmark={landmark}  "
                 f"max_windows={phase['max_windows']}  strategies={strategies}")
    lines.append("")

    lines.append("Run configuration (constant across runs)")
    lines.append("-" * 41)
    for col in RUN_CONFIG_COLUMNS:
        lines.append(f"  {col}: {all_runs[col].iloc[0]}")
    lines.append("")

    lines.append(f"Shared metrics -- mean, median, mode, std dev (n={runs})")
    lines.append("-" * 54)
    for col in SHARED_COLUMNS:
        mean, median, mode, std, note = describe(all_runs[col])
        lines.append(f"  {col}: mean={mean:.4f}  median={median:.4f}  mode={mode:.4f}{note}  std={std:.4f}")
    lines.append("")

    lines.append(f"Per-strategy metrics -- mean, median, mode, std dev (n={runs})")
    lines.append("-" * 60)
    for suffix in STRATEGY_METRIC_SUFFIXES:
        for strategy in strategies:
            col = f"{strategy}_{suffix}"
            mean, median, mode, std, note = describe(all_runs[col])
            lines.append(f"  {col}: mean={mean:.4f}  median={median:.4f}  mode={mode:.4f}{note}  std={std:.4f}")
        lines.append("")

    lines.append(f"Statistical significance: each baseline vs {PRIMARY_STRATEGY} (paired, n={runs})")
    lines.append("-" * 92)
    lines.append(
        f"{'strategy':<22}{'metric':<20}{'base mean':>12}{'bfair mean':>12}"
        f"{'t':>9}{'t p-value':>12}{'W p-value':>12}  significant@.05"
    )
    for baseline in baselines:
        for suffix in STRATEGY_METRIC_SUFFIXES:
            base_vals = all_runs[f"{baseline}_{suffix}"]
            bfair_vals = all_runs[f"{PRIMARY_STRATEGY}_{suffix}"]
            t_stat, t_p = stats.ttest_rel(bfair_vals, base_vals)
            diffs = bfair_vals - base_vals
            if (diffs != 0).any():
                w_stat, w_p = stats.wilcoxon(bfair_vals, base_vals)
            else:
                w_p = 1.0
            significant = "yes" if t_p < 0.05 else "no"
            lines.append(
                f"{baseline:<22}{suffix:<20}{base_vals.mean():>12.4f}{bfair_vals.mean():>12.4f}"
                f"{t_stat:>9.3f}{t_p:>12.4g}{w_p:>12.4g}  {significant}"
            )
        lines.append("")
    lines.append("t = paired t-test (scipy.stats.ttest_rel); W p-value = Wilcoxon signed-rank test (non-parametric).")
    lines.append(f"significant@.05 flags the paired t-test result; both tests compare {PRIMARY_STRATEGY} against the listed baseline.")

    return "\n".join(lines)


def run_phase(attr_dir, dataset_path, date_col, attr, is_discrete, window_size, block_size, landmark,
              fairness_target, fairness_bounds, phase, runs, topic_prefix, save_artifacts, extra_cols=None,
              consumer_script="consumer_editable_bfair_performance.py", max_bins=5, dayfirst=False):
    """Runs one phase (strategy subset + max_windows) `runs` times against
    real Kafka and returns the concatenated per-run metrics DataFrame. If
    save_artifacts, also writes one consolidated original-chunk CSV and one
    consolidated reordered-chunk CSV for this (config, phase) under
    <attr_dir>/original/ and <attr_dir>/reordered/.

    fairness_target (from derive_fairness_counts) drives the reordering
    algorithms; fairness_bounds=(floors, ceilings) (from derive_fairness_bounds)
    is the independent [floor, ceiling] range the fairness *check* uses.
    utils.position_finder prompts for these in three separate blocks (all
    targets, then all floors, then all ceilings), so CONSUMER_STDIN must
    match that block order, not interleaved per category."""
    strategies = phase["strategies"]
    max_windows = phase["max_windows"]
    floors, ceilings = fairness_bounds
    consumer_stdin = (
        "".join(f"{t}\n" for t in fairness_target)
        + "".join(f"{f}\n" for f in floors)
        + "".join(f"{c}\n" for c in ceilings)
    )

    os.makedirs(TMP_DIR, exist_ok=True)
    session_stamp = int(time.time())
    run_frames = []
    reordered_frames = []
    original_frames = []

    for run_id in range(1, runs + 1):
        topic = f"{topic_prefix}_{phase['label']}_{session_stamp}_{run_id}"
        print(f"    run {run_id}/{runs} (topic={topic})")

        shuffled_name = f"{topic_prefix}_shuffled_run{run_id}"
        shuffle_dataset(dataset_path, run_id, shuffled_name)
        shuffled_path = f"datasets/{shuffled_name}.csv"
        reordered_tmp_path = f"{TMP_DIR}/{topic}_reordered.csv"
        original_tmp_path = f"{TMP_DIR}/{topic}_original.csv"
        try:
            extra_cols_line = ",".join(extra_cols) if extra_cols else ""
            producer_stdin = (f"{shuffled_name}\n{date_col}\n{attr}\n{is_discrete}\n{extra_cols_line}\n"
                               f"{max_bins}\n{int(dayfirst)}\n")
            run([sys.executable, "producer.py", "--topic_name", topic], producer_stdin)

            consumer_cmd = [
                sys.executable, consumer_script,
                "--window_size", str(window_size),
                "--block_size", str(block_size),
                "--topic_name", topic,
                "--max_windows", str(max_windows),
                "--landmark", str(landmark),
                "--brt_force", "False",
                "--strategies", ",".join(strategies),
                "--out_dir", TMP_DIR,
            ]
            if save_artifacts:
                consumer_cmd += ["--save_reordered_path", reordered_tmp_path,
                                  "--save_original_path", original_tmp_path]
                if extra_cols:
                    consumer_cmd += ["--extra_cols", ",".join(extra_cols)]
            run(consumer_cmd, consumer_stdin)
        finally:
            if os.path.exists(shuffled_path):
                os.remove(shuffled_path)

        output_attr = attr if is_discrete else f"{attr}_bin"
        metric_name = f"metric_bfair_{output_attr}WIN{window_size}BLC{block_size}LAN{landmark}.csv"
        metric_path = f"{TMP_DIR}/{metric_name}"
        df = pd.read_csv(metric_path)
        df.insert(0, "run_id", run_id)
        run_frames.append(df)
        os.remove(metric_path)

        if save_artifacts and os.path.exists(reordered_tmp_path):
            rdf = pd.read_csv(reordered_tmp_path)
            rdf.insert(0, "run_id", run_id)
            reordered_frames.append(rdf)
            os.remove(reordered_tmp_path)

        if save_artifacts and os.path.exists(original_tmp_path):
            odf = pd.read_csv(original_tmp_path)
            odf.insert(0, "run_id", run_id)
            original_frames.append(odf)
            os.remove(original_tmp_path)

    if save_artifacts and reordered_frames:
        os.makedirs(f"{attr_dir}/reordered", exist_ok=True)
        reordered_path = f"{attr_dir}/reordered/W{window_size}_s{block_size}_X{landmark}_{phase['label']}.csv"
        pd.concat(reordered_frames, ignore_index=True).to_csv(reordered_path, index=False)
        print(f"    saved reordered output -> {reordered_path}")

    if save_artifacts and original_frames:
        os.makedirs(f"{attr_dir}/original", exist_ok=True)
        original_path = f"{attr_dir}/original/W{window_size}_s{block_size}_X{landmark}_{phase['label']}.csv"
        pd.concat(original_frames, ignore_index=True).to_csv(original_path, index=False)
        print(f"    saved original output -> {original_path}")

    return pd.concat(run_frames, ignore_index=True)


def config_cache_path(attr_dir, window_size, block_size, landmark, phase_label):
    return f"{attr_dir}/_sweep_cache/W{window_size}_s{block_size}_X{landmark}_{phase_label}.csv"


def get_or_run_phase(attr_dir, dataset_path, date_col, attr, is_discrete, window_size, block_size, landmark,
                      fairness_target, fairness_bounds, phase, runs, topic_prefix, use_cache, save_artifacts,
                      extra_cols=None, max_bins=5, dayfirst=False):
    os.makedirs(f"{attr_dir}/_sweep_cache", exist_ok=True)
    path = config_cache_path(attr_dir, window_size, block_size, landmark, phase["label"])
    if use_cache and os.path.exists(path):
        print(f"  (reusing cached results for {attr_dir} W={window_size} s={block_size} X={landmark} {phase['label']})")
        return pd.read_csv(path)
    print(f"  running {attr_dir} W={window_size} s={block_size} X={landmark} {phase['label']} ({runs} runs)...")
    all_runs = run_phase(attr_dir, dataset_path, date_col, attr, is_discrete, window_size, block_size, landmark,
                          fairness_target, fairness_bounds, phase, runs, topic_prefix, save_artifacts,
                          extra_cols=extra_cols, max_bins=max_bins, dayfirst=dayfirst)
    if use_cache:
        all_runs.to_csv(path, index=False)
    return all_runs


def run_two_phase_config(attr_dir, out_label, dataset_path, date_col, attr, is_discrete, window_size, block_size,
                          landmark, fairness_target, fairness_bounds, runs, topic_prefix, use_cache=False,
                          save_reordered=False, extra_cols=None, max_bins=5, dayfirst=False):
    """Runs both phases for one config and writes {runs.csv, STATS.txt} per
    phase. Returns {phase_label: all_runs_df}. save_reordered (despite the
    name, covers both the original- and reordered-stream artifacts) only
    applies to the 3strat_5000win phase -- the 500-window phase's artifacts
    aren't kept."""
    validate_config(window_size, block_size, landmark)
    floors, _ = fairness_bounds
    assert block_size >= len(floors), (
        f"block_size {block_size} < cardinality {len(floors)} -- "
        "can't give every category a slot in one block"
    )
    results = {}
    for phase in PHASES:
        save_artifacts = save_reordered and phase["label"] == "3strat_5000win"
        all_runs = get_or_run_phase(attr_dir, dataset_path, date_col, attr, is_discrete, window_size, block_size,
                                     landmark, fairness_target, fairness_bounds, phase, runs, topic_prefix,
                                     use_cache, save_artifacts, extra_cols=extra_cols, max_bins=max_bins,
                                     dayfirst=dayfirst)
        results[phase["label"]] = all_runs

        if not use_cache:
            runs_path = f"{attr_dir}/metric_{phase['label']}_runs_WIN{window_size}BLC{block_size}LAN{landmark}.csv"
            all_runs.to_csv(runs_path, index=False)
            print(f"  saved {runs} runs -> {runs_path}")

            report = build_report(out_label, dataset_path, phase, phase["strategies"], runs, all_runs)
            report_path = f"{attr_dir}/metric_{phase['label']}_STATS{runs}_WIN{window_size}BLC{block_size}LAN{landmark}.txt"
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(report)
            print(f"  saved report -> {report_path}")

    return results


def run_single(args):
    fairness_target = derive_fairness_counts(args.percentages, args.block_size)
    fairness_bounds = derive_fairness_bounds(args.percentages, args.block_size)
    print(f"\n=== {args.out_label}: categories(sorted)={args.categories} "
          f"percentages={[f'{p:.4f}' for p in args.percentages]} "
          f"reorder_target={fairness_target} floors={fairness_bounds[0]} ceilings={fairness_bounds[1]} ===")

    for phase in PHASES:
        print(f"\n--- {args.out_label} / {phase['label']} (max_windows={phase['max_windows']}, "
              f"strategies={phase['strategies']}) ---")

    os.makedirs(args.attr_dir, exist_ok=True)
    run_two_phase_config(args.attr_dir, args.out_label, args.dataset, args.date_col, args.attr, args.is_discrete,
                          args.window_size, args.block_size, args.landmark, fairness_target, fairness_bounds,
                          args.runs, topic_prefix=args.out_label.lower(), use_cache=False,
                          save_reordered=args.save_reordered, extra_cols=args.extra_cols, max_bins=args.max_bins,
                          dayfirst=args.dayfirst)


def run_sweep_label(sweep_label, args, fairness_by_block_size):
    spec = SWEEPS[sweep_label]
    param = spec["param"]
    values = spec["values"]
    fixed = spec["fixed"]

    print(f"\n=== Sweep {sweep_label}: {param} in {values}, fixed {fixed} (attr={args.out_label}) ===")

    frames_by_phase = {phase["label"]: [] for phase in PHASES}
    reports_by_phase = {phase["label"]: [
        f"Parameter Sweep: {sweep_label} ({param} in {values}, fixed {fixed}) -- attr={args.out_label} -- {phase['label']}",
        "=" * 100,
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Dataset: {args.dataset} (attribute: {args.attr}), row order reshuffled per run (seed=run_id)",
        "",
    ] for phase in PHASES}

    for value in values:
        config = dict(fixed)
        config[param] = value
        window_size = config["window_size"]
        block_size = config["block_size"]
        landmark = config["landmark"]

        cardinality = len(args.categories)
        if block_size < cardinality:
            print(f"\n-- {sweep_label}={value} (W={window_size}, s={block_size}, X={landmark}) -- "
                  f"SKIPPED: block_size {block_size} < cardinality {cardinality}, "
                  f"can't give every category a slot in one block --")
            continue

        if block_size not in fairness_by_block_size:
            fairness_by_block_size[block_size] = (
                derive_fairness_counts(args.percentages, block_size),
                derive_fairness_bounds(args.percentages, block_size),
            )
        fairness_target, fairness_bounds = fairness_by_block_size[block_size]

        print(f"\n-- {sweep_label}={value} (W={window_size}, s={block_size}, X={landmark}) --")
        results = run_two_phase_config(args.attr_dir, args.out_label, args.dataset, args.date_col, args.attr,
                                        args.is_discrete, window_size, block_size, landmark, fairness_target,
                                        fairness_bounds, args.runs, topic_prefix=f"{args.out_label.lower()}_sweep",
                                        use_cache=True, save_reordered=args.save_reordered,
                                        extra_cols=args.extra_cols, max_bins=args.max_bins, dayfirst=args.dayfirst)
        for phase in PHASES:
            all_runs = results[phase["label"]]
            frames_by_phase[phase["label"]].append(all_runs)
            reports_by_phase[phase["label"]].append(build_report(args.out_label, args.dataset, phase,
                                                                   phase["strategies"], args.runs, all_runs))
            reports_by_phase[phase["label"]].append("")

    for phase in PHASES:
        label = phase["label"]
        combined = pd.concat(frames_by_phase[label], ignore_index=True)
        runs_path = f"{args.attr_dir}/sweep_{sweep_label}_{label}_runs.csv"
        combined.to_csv(runs_path, index=False)
        print(f"\n  saved sweep runs -> {runs_path}")

        report_path = f"{args.attr_dir}/sweep_{sweep_label}_{label}_STATS.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(reports_by_phase[label]))
        print(f"  saved sweep report -> {report_path}")


def build_config_averages_csv(args):
    """One row per unique (window_size, block_size, landmark, phase) config
    found in this attribute's disk cache, holding that config's runs-mean
    for every numeric metric, plus which sweep(s) it belongs to."""
    sweeps_by_key = {}
    for label, spec in SWEEPS.items():
        for value in spec["values"]:
            config = dict(spec["fixed"])
            config[spec["param"]] = value
            key = (config["window_size"], config["block_size"], config["landmark"])
            sweeps_by_key.setdefault(key, []).append(label)

    cache_dir = f"{args.attr_dir}/_sweep_cache"
    if not os.path.isdir(cache_dir):
        print("\n  no cached configs found; skipping config_averages.csv")
        return

    rows = []
    for fname in sorted(os.listdir(cache_dir)):
        if not fname.endswith(".csv"):
            continue
        phase_label = next((p["label"] for p in PHASES if fname.endswith(f"_{p['label']}.csv")), None)
        if phase_label is None:
            continue
        all_runs = pd.read_csv(f"{cache_dir}/{fname}")
        key = (int(all_runs["win_size"].iloc[0]), int(all_runs["block_size"].iloc[0]), int(all_runs["landmark"].iloc[0]))
        avg = all_runs.mean(numeric_only=True).to_dict()
        avg["phase"] = phase_label
        avg["sweeps"] = ",".join(sweeps_by_key.get(key, []))
        rows.append(avg)

    if not rows:
        print(f"\n  no cached configs found for {args.out_label}; skipping config_averages.csv")
        return

    averages = pd.DataFrame(rows)
    cols = ["sweeps", "phase"] + [c for c in averages.columns if c not in ("sweeps", "phase")]
    averages = averages[cols]
    path = f"{args.attr_dir}/sweep_config_averages.csv"
    averages.to_csv(path, index=False)
    print(f"\n=== Saved consolidated config averages ({len(averages)} rows) -> {path} ===")


def run_sweep(args):
    sweep_labels = args.sweep if args.sweep else list(SWEEPS.keys())
    os.makedirs(args.attr_dir, exist_ok=True)
    fairness_by_block_size = {}
    for label in sweep_labels:
        run_sweep_label(label, args, fairness_by_block_size)
    build_config_averages_csv(args)


def main():
    parser = argparse.ArgumentParser(description="Attribute/dataset-generic stream-fairness simulation harness")
    parser.add_argument("--attr", type=str, required=True, help="Attribute/column name to stream and monitor.")
    parser.add_argument("--dataset", type=str, default="datasets/HDHI_Admission_data_modified.csv")
    parser.add_argument("--date-col", type=str, default="D.O.A")
    parser.add_argument("--is-discrete", type=int, choices=[0, 1], default=1,
                         help="1 (default): attr is already categorical/pre-binned. "
                              "0: attr is raw/continuous and needs auto-binning first.")
    parser.add_argument("--out-label", type=str, default=None,
                         help="Label used in output filenames; defaults to --attr.")
    parser.add_argument("--dataset-label", type=str, default=None,
                         help="Top-level metrics/ subdirectory name; defaults to --dataset's basename.")
    parser.add_argument("--mode", choices=["single", "sweep"], default="single")
    parser.add_argument("--window-size", type=int, default=40)
    parser.add_argument("--block-size", type=int, default=20)
    parser.add_argument("--landmark", type=int, default=20)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--sweep", choices=["W", "s", "X"], action="append",
                         help="Sweep mode only: which sweep(s) to run; omit to run all three.")
    parser.add_argument("--save-reordered", action="store_true",
                         help="Persist bfairreorder's reordered window+landmark chunks per (config, phase).")
    parser.add_argument("--phase", choices=["fast", "slow"], action="append",
                         help="Which phase(s) to run: fast=3strat_5000win, slow=5strat_500win. Omit for both.")
    parser.add_argument("--extra-cols", type=str, default=None,
                         help="Comma-separated extra source-row columns (e.g. movieId,avg_rating,vote_count) "
                              "to attach to each row of the saved original/reordered CSVs. Only meaningful "
                              "with --save-reordered.")
    parser.add_argument("--max-bins", type=int, default=5,
                         help="Max bins for --is-discrete 0 continuous binning (bin_with_min_pct). "
                              "Ignored when --is-discrete 1.")
    parser.add_argument("--dayfirst", action="store_true",
                         help="Parse --date-col as day-first (e.g. DD-MM-YYYY) instead of the pandas default "
                              "month-first. Only needed for genuinely day-first date columns -- ISO/unambiguous "
                              "columns must NOT set this (pandas dayfirst=True also reinterprets those).")
    args = parser.parse_args()
    args.extra_cols = [c.strip() for c in args.extra_cols.split(",") if c.strip()] if args.extra_cols else None

    global PHASES
    if args.phase:
        PHASES = [p for p in ALL_PHASES if p["kind"] in args.phase]

    args.out_label = args.out_label or args.attr
    args.dataset_label = args.dataset_label or os.path.splitext(os.path.basename(args.dataset))[0]
    args.attr_dir = f"{METRICS_DIR}/{args.dataset_label}/{args.out_label}"
    args.categories, args.percentages = attr_categories_and_percentages(args.dataset, args.attr, args.is_discrete,
                                                                          max_bins=args.max_bins)

    if args.mode == "single":
        run_single(args)
    else:
        run_sweep(args)


if __name__ == "__main__":
    main()
