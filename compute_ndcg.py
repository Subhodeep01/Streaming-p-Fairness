"""Per-window NDCG (original vs. reordered stream) for every captured
attribute/config under metrics/movie_vote_summary/ and metrics/tweets/.

Relevance is graded linearly (gain = the relevance value itself -- avg_rating
/likes are continuous/large-range, not small ordinal grades, so the usual
2^rel-1 IR gain would blow up on a likes count in the hundreds). Rank comes
straight from position_in_chunk (0-indexed -> rank = position_in_chunk + 1,
the item's actual delivery order within its window). IDCG is the classic
self-normalized kind: the same window's items re-sorted by relevance
descending, not scored against any external ground-truth ranking.

A window is one (run_id, edit_event) group. "original" is the pre-reorder
landmarked window; "reordered" is bfairreorder's output for that same
window -- so orig vs. reordered isolates exactly what the fairness
reordering did to relevance-ranking quality, with everything else (which
items are in the window) held fixed.

The "per_block" cutoff computes NDCG separately within each block
(position_in_chunk // block_size) instead of across the whole window, then
averages over blocks -- that's the granularity bfairreorder's fairness
target actually applies at (every block, not just the window as a whole).

Usage:
    python compute_ndcg.py
    python compute_ndcg.py --attr-dir metrics/movie_vote_summary/genre-1 --relevance-col avg_rating
"""

import argparse
import glob
import os
import re

import numpy as np
import pandas as pd
from scipy import stats

ATTR_RELEVANCE = {
    "metrics/movie_vote_summary/genre-1": "avg_rating",
    "metrics/movie_vote_summary/audience_reception": "avg_rating",
    "metrics/movie_vote_summary/popularity_tier": "avg_rating",
    "metrics/movie_vote_summary/release_era": "avg_rating",
    "metrics/tweets/sentiment": "likes",
    "metrics/tweets/engagement": "likes",
    "metrics/tweets/topic": "likes",
    "metrics/tweets/tweet_length_tier": "likes",
}

# "block_size" truncates the *whole window* at that config's own block_size
# (bfairreorder's atomic reorder unit, parsed from the filename) -- i.e. it
# only scores the first block. "per_block" instead computes NDCG separately
# within *every* block of the window (rank/discount reset each block) and
# reports the mean over all blocks -- see ndcg_per_block. That's the metric
# that matches what bfairreorder actually operates on: every block gets its
# own fairness target, not just the first one.
CUTOFFS = ["block_size", 50, 100, "per_block"]

CONFIG_NAME_RE = re.compile(r"_s(\d+)_X")


def parse_block_size(config_name):
    m = CONFIG_NAME_RE.search(config_name)
    if not m:
        raise ValueError(f"Couldn't parse block_size (s) out of config name {config_name!r}")
    return int(m.group(1))


def _discount(rank):
    return 1.0 / np.log2(rank + 1)


def ndcg_per_window(df, relevance_col, cutoff):
    """df has run_id/edit_event/position_in_chunk/relevance_col columns (one
    row per streamed item). Returns a Series of NDCG indexed by
    (run_id, edit_event); NaN where IDCG is 0 (every item in that window has
    zero relevance -- can happen with `likes`, never with avg_rating)."""
    delivered = df[["run_id", "edit_event", "position_in_chunk", relevance_col]].copy()
    delivered["rank"] = delivered["position_in_chunk"] + 1
    if cutoff is not None:
        delivered = delivered[delivered["rank"] <= cutoff]
    delivered["gain"] = delivered[relevance_col] * _discount(delivered["rank"])
    dcg = delivered.groupby(["run_id", "edit_event"])["gain"].sum()

    ideal = df[["run_id", "edit_event", relevance_col]].copy()
    ideal = ideal.sort_values(["run_id", "edit_event", relevance_col], ascending=[True, True, False])
    ideal["ideal_rank"] = ideal.groupby(["run_id", "edit_event"]).cumcount() + 1
    if cutoff is not None:
        ideal = ideal[ideal["ideal_rank"] <= cutoff]
    ideal["ideal_gain"] = ideal[relevance_col] * _discount(ideal["ideal_rank"])
    idcg = ideal.groupby(["run_id", "edit_event"])["ideal_gain"].sum()

    ndcg = (dcg / idcg).replace([np.inf, -np.inf], np.nan)
    return ndcg


def ndcg_per_block(df, relevance_col, block_size):
    """Like ndcg_per_window, but computed independently *within* each block
    (position_in_chunk // block_size) instead of across the whole window --
    bfairreorder's fairness target applies per block, not per window, so this
    is the granularity its swaps actually operate at. Rank/discount reset at
    the start of every block. Returns a Series of per-block NDCG indexed by
    (run_id, edit_event, block_index); the window-level number is the mean of
    these, not a single DCG/IDCG over the concatenated window."""
    d = df[["run_id", "edit_event", "position_in_chunk", relevance_col]].copy()
    d["block_index"] = d["position_in_chunk"] // block_size
    d["rank_in_block"] = d["position_in_chunk"] % block_size + 1
    group = ["run_id", "edit_event", "block_index"]

    d["gain"] = d[relevance_col] * _discount(d["rank_in_block"])
    dcg = d.groupby(group)["gain"].sum()

    ideal = d[group + [relevance_col]].sort_values(group + [relevance_col],
                                                     ascending=[True, True, True, False])
    ideal["ideal_rank"] = ideal.groupby(group).cumcount() + 1
    ideal["ideal_gain"] = ideal[relevance_col] * _discount(ideal["ideal_rank"])
    idcg = ideal.groupby(group)["ideal_gain"].sum()

    ndcg = (dcg / idcg).replace([np.inf, -np.inf], np.nan)
    return ndcg


def summarize_config(attr_dir, config_name, relevance_col):
    orig_path = f"{attr_dir}/original/{config_name}"
    reord_path = f"{attr_dir}/reordered/{config_name}"
    orig = pd.read_csv(orig_path)
    reord = pd.read_csv(reord_path)
    block_size = parse_block_size(config_name)

    rows = []
    for cutoff_spec in CUTOFFS:
        if cutoff_spec == "block_size":
            cutoff, label = block_size, "block_size"
        elif cutoff_spec == "per_block":
            cutoff, label = block_size, "per_block"
        else:
            cutoff, label = cutoff_spec, cutoff_spec

        if cutoff_spec == "per_block":
            # NDCG computed and averaged *within* each block, not across the
            # whole window -- see ndcg_per_block. "n" here counts blocks
            # (windows * blocks_per_window), not windows.
            orig_ndcg = ndcg_per_block(orig, relevance_col, block_size)
            reord_ndcg = ndcg_per_block(reord, relevance_col, block_size)
        else:
            orig_ndcg = ndcg_per_window(orig, relevance_col, cutoff)
            reord_ndcg = ndcg_per_window(reord, relevance_col, cutoff)
        paired = pd.concat([orig_ndcg, reord_ndcg], axis=1, keys=["orig", "reord"]).dropna()

        row = {
            "config": config_name.replace("_3strat_5000win.csv", ""),
            "cutoff": label,
            "k": cutoff,
            "n": len(paired),
            "orig_mean": paired["orig"].mean(),
            "orig_median": paired["orig"].median(),
            "orig_std": paired["orig"].std(ddof=1) if len(paired) > 1 else 0.0,
            "reord_mean": paired["reord"].mean(),
            "reord_median": paired["reord"].median(),
            "reord_std": paired["reord"].std(ddof=1) if len(paired) > 1 else 0.0,
            "mean_diff": paired["reord"].mean() - paired["orig"].mean(),
        }
        row["pct_change"] = (
            100 * row["mean_diff"] / row["orig_mean"] if row["orig_mean"] else float("nan")
        )
        if len(paired) > 1 and (paired["reord"] != paired["orig"]).any():
            t_stat, t_p = stats.ttest_rel(paired["reord"], paired["orig"])
            w_stat, w_p = stats.wilcoxon(paired["reord"], paired["orig"])
        else:
            t_stat, t_p, w_p = float("nan"), 1.0, 1.0
        row["t_stat"] = t_stat
        row["t_p"] = t_p
        row["wilcoxon_p"] = w_p
        rows.append(row)
    return rows


def run_attr_dir(attr_dir, relevance_col):
    reordered_dir = f"{attr_dir}/reordered"
    if not os.path.isdir(reordered_dir):
        print(f"  skip {attr_dir}: no reordered/ dir")
        return None
    configs = sorted(os.path.basename(p) for p in glob.glob(f"{reordered_dir}/*.csv"))
    all_rows = []
    for config_name in configs:
        if not os.path.exists(f"{attr_dir}/original/{config_name}"):
            print(f"  skip {config_name}: no matching original/ file")
            continue
        print(f"  {attr_dir} :: {config_name}")
        all_rows.extend(summarize_config(attr_dir, config_name, relevance_col))
    if not all_rows:
        return None
    summary = pd.DataFrame(all_rows)
    out_path = f"{attr_dir}/ndcg_summary.csv"
    summary.to_csv(out_path, index=False)
    print(f"  saved -> {out_path}")
    return summary.assign(attr_dir=attr_dir, relevance_col=relevance_col)


def main():
    parser = argparse.ArgumentParser(description="Per-window NDCG, original vs. reordered stream")
    parser.add_argument("--attr-dir", type=str, default=None,
                         help="Single attr dir to run (default: every dir in ATTR_RELEVANCE).")
    parser.add_argument("--relevance-col", type=str, default=None,
                         help="Relevance column to use with --attr-dir (required if --attr-dir is set).")
    args = parser.parse_args()

    if args.attr_dir:
        if not args.relevance_col:
            parser.error("--relevance-col is required with --attr-dir")
        targets = {args.attr_dir: args.relevance_col}
    else:
        targets = ATTR_RELEVANCE

    combined = []
    for attr_dir, relevance_col in targets.items():
        print(f"=== {attr_dir} (relevance={relevance_col}) ===")
        result = run_attr_dir(attr_dir, relevance_col)
        if result is not None:
            combined.append(result)

    if combined:
        all_df = pd.concat(combined, ignore_index=True)
        cols = ["attr_dir", "relevance_col"] + [c for c in all_df.columns if c not in ("attr_dir", "relevance_col")]
        all_df = all_df[cols]
        out_path = "metrics/ndcg_all_summary.csv"
        all_df.to_csv(out_path, index=False)
        print(f"\n=== saved combined summary ({len(all_df)} rows) -> {out_path} ===")


if __name__ == "__main__":
    main()
