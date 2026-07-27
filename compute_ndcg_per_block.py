"""Per-BLOCK NDCG (original vs. reordered stream), averaged across all blocks.

Unlike compute_ndcg.py (which scores each whole window -- or the top-k of a
window -- as a single ranked list), this treats every non-overlapping
block of `block_size` consecutive delivered items as its own ranked list:
rank resets to 1 at the start of each block, and IDCG is computed from that
block's own items re-sorted by relevance descending (not the whole window's).
NDCG is then computed per (run_id, edit_event, block_id) and averaged over
every block in the config -- i.e. "average the NDCG per block" rather than
scoring full windows.

Usage:
    python compute_ndcg_per_block.py
    python compute_ndcg_per_block.py --attr-dir metrics/movie_vote_summary/genre-1 --relevance-col avg_rating
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
    "metrics/tweets/sentiment": "likes",
}

CONFIG_NAME_RE = re.compile(r"_s(\d+)_X")


def parse_block_size(config_name):
    m = CONFIG_NAME_RE.search(config_name)
    if not m:
        raise ValueError(f"Couldn't parse block_size (s) out of config name {config_name!r}")
    return int(m.group(1))


def _discount(rank):
    return 1.0 / np.log2(rank + 1)


def ndcg_per_block(df, relevance_col, block_size):
    """df has run_id/edit_event/position_in_chunk/relevance_col columns (one
    row per streamed item). Returns a Series of NDCG indexed by
    (run_id, edit_event, block_id) -- one value per block, rank/IDCG both
    local to that block."""
    delivered = df[["run_id", "edit_event", "position_in_chunk", relevance_col]].copy()
    delivered["block_id"] = delivered["position_in_chunk"] // block_size
    delivered["rank"] = delivered["position_in_chunk"] % block_size + 1
    delivered["gain"] = delivered[relevance_col] * _discount(delivered["rank"])
    dcg = delivered.groupby(["run_id", "edit_event", "block_id"])["gain"].sum()

    ideal = df[["run_id", "edit_event", "position_in_chunk", relevance_col]].copy()
    ideal["block_id"] = ideal["position_in_chunk"] // block_size
    ideal = ideal.sort_values(["run_id", "edit_event", "block_id", relevance_col],
                               ascending=[True, True, True, False])
    ideal["ideal_rank"] = ideal.groupby(["run_id", "edit_event", "block_id"]).cumcount() + 1
    ideal["ideal_gain"] = ideal[relevance_col] * _discount(ideal["ideal_rank"])
    idcg = ideal.groupby(["run_id", "edit_event", "block_id"])["ideal_gain"].sum()

    ndcg = (dcg / idcg).replace([np.inf, -np.inf], np.nan)
    return ndcg


def summarize_config(attr_dir, config_name, relevance_col):
    orig_path = f"{attr_dir}/original/{config_name}"
    reord_path = f"{attr_dir}/reordered/{config_name}"
    orig = pd.read_csv(orig_path)
    reord = pd.read_csv(reord_path)
    block_size = parse_block_size(config_name)

    orig_ndcg = ndcg_per_block(orig, relevance_col, block_size)
    reord_ndcg = ndcg_per_block(reord, relevance_col, block_size)
    paired = pd.concat([orig_ndcg, reord_ndcg], axis=1, keys=["orig", "reord"]).dropna()

    row = {
        "config": config_name.replace("_3strat_5000win.csv", ""),
        "block_size": block_size,
        "n_blocks": len(paired),
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
    return row


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
        all_rows.append(summarize_config(attr_dir, config_name, relevance_col))
    if not all_rows:
        return None
    summary = pd.DataFrame(all_rows)
    out_path = f"{attr_dir}/ndcg_per_block_summary.csv"
    summary.to_csv(out_path, index=False)
    print(f"  saved -> {out_path}")
    return summary.assign(attr_dir=attr_dir, relevance_col=relevance_col)


def main():
    parser = argparse.ArgumentParser(description="Per-block NDCG (averaged), original vs. reordered stream")
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
        out_path = "metrics/ndcg_per_block_all_summary.csv"
        all_df.to_csv(out_path, index=False)
        print(f"\n=== saved combined summary ({len(all_df)} rows) -> {out_path} ===")


if __name__ == "__main__":
    main()
