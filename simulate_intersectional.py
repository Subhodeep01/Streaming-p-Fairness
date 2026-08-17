"""Intersectional-attribute experiments: all 5 strategies, one fixed config.

For each of five attribute groups, builds a new "intersectional" categorical
column as the Cartesian product of 2-3 existing attributes' value sets, with
fairness floors derived from that column's *actual empirical joint
frequency* (plain value_counts, no independence assumption).

Fixed config for every group: window_size=500, block_size=50, landmark=50,
max_windows=500, strategies=all five (naive_bfair_reorder, bfairreorder,
internal_swap, greedy_swap, weighted_greedy_swap), 10 runs -- reuses
simulate_stream.py's own "5strat_500win" phase definition so this can never
drift from the sweep harness's version of that phase.

Two groups (movie genre-1 x genre-2, adult education x race x sex) would
exceed cardinality 50 (the max block_size can support here, since block_size
must divide gcd(500,50)=50) at full resolution, so the oversized attribute(s)
are collapsed to a top-K-plus-"Other" bucketing first -- see build_* functions
below for the exact collapse per group.

Reuses simulate_stream.py's run_phase (already generic over an arbitrary
phase dict) for the actual shuffle->produce->consume->collect loop -- no new
driver logic needed, just the intersectional-column/fairness-percentage
construction.
"""

import os

import pandas as pd

from simulate_stream import (
    build_report, describe, derive_fairness_bounds, derive_fairness_counts, run_phase, validate_config,
    ALL_PHASES, METRICS_DIR,
)

WINDOW_SIZE = 500
BLOCK_SIZE = 50
LANDMARK = 50
RUNS = 10
PHASE = next(p for p in ALL_PHASES if p["label"] == "5strat_500win")

DATASETS_DIR = "datasets"


def marginal_percentages(series: pd.Series) -> dict:
    counts = series.value_counts(normalize=True)
    return {str(k): float(v) for k, v in counts.items()}


def build_intersectional_column(df: pd.DataFrame, attrs: list, joined_col: str, percentage_source: pd.DataFrame = None):
    """Adds `joined_col` (Cartesian-product string of `attrs`) to df in
    place, and returns (categories, percentages) -- sorted alphabetically
    to match utils.position_finder's ordering. Percentages are the actual
    empirical joint frequency of the intersectional groups (value_counts on
    the joined column itself, not a product-of-marginals independence
    assumption), so only combos that actually occur ever appear.

    Computed from `percentage_source` if given, else from `df` itself --
    pass a pre-filtered view when the real pipeline drops rows before
    producing (e.g. utils.load_clean dropping rows with an unparseable
    date), so the computed categories/percentages match what
    utils.position_finder will actually see downstream, not the raw file."""
    source = percentage_source if percentage_source is not None else df

    df[joined_col] = df[attrs[0]].astype(str)
    for a in attrs[1:]:
        df[joined_col] = df[joined_col] + " | " + df[a].astype(str)

    if percentage_source is not None:
        source_joined = source[attrs[0]].astype(str)
        for a in attrs[1:]:
            source_joined = source_joined + " | " + source[a].astype(str)
    else:
        source_joined = df[joined_col]

    observed = marginal_percentages(source_joined)
    categories = sorted(observed.keys())
    percentages = [observed[c] for c in categories]
    return categories, percentages


# ---------------------------------------------------------------------------
# Group builders
# ---------------------------------------------------------------------------

def build_tweets_group():
    df = pd.read_csv(f"{DATASETS_DIR}/tweets.csv")
    joined_col = "engagement_tweet_length_tier_sentiment"
    categories, percentages = build_intersectional_column(df, ["engagement", "tweet_length_tier", "sentiment"], joined_col)
    out_path = f"{DATASETS_DIR}/tweets_intersect_engagement_tweet_length_tier_sentiment.csv"
    df.to_csv(out_path, index=False)
    return {
        "dataset_label": "tweets", "group_name": "engagement_tweet_length_tier_sentiment",
        "dataset_path": out_path, "date_col": "stream_date", "attr": joined_col,
        "categories": categories, "percentages": percentages,
    }


def build_hdhi_group():
    df = pd.read_csv(f"{DATASETS_DIR}/HDHI_Admission_data_modified.csv")
    # D.O.A has genuinely ambiguous date formats -- pandas' format
    # auto-detection is shuffle-order-dependent (confirmed: 10,102 rows kept
    # unshuffled vs 11,990 kept on a shuffled copy of the same data), which
    # silently changes which rows survive utils.load_clean's dropna from run
    # to run. That's noise for AGE_bin/GENDER/OUTCOME (every category has
    # plenty of margin) but this cross has combos backed by 1-7 rows, so it
    # actually flips len(unique) between runs. Fix: give this derived
    # dataset its own synthetic, unambiguous date column (same trick used
    # for tweets/movie/adult) instead of touching the shared D.O.A parsing.
    dates = pd.date_range("2020-01-01", periods=len(df), freq="min")
    df["stream_date"] = dates.strftime("%Y-%m-%d %H:%M:%S")

    joined_col = "AGE_bin_OUTCOME_SMOKING"
    categories, percentages = build_intersectional_column(df, ["AGE_bin", "OUTCOME", "SMOKING"], joined_col)
    out_path = f"{DATASETS_DIR}/HDHI_Admission_data_modified_intersect_AGE_bin_OUTCOME_SMOKING.csv"
    df.to_csv(out_path, index=False)
    return {
        "dataset_label": "HDHI_Admission_data_modified", "group_name": "AGE_bin_OUTCOME_SMOKING",
        "dataset_path": out_path, "date_col": "stream_date", "attr": joined_col,
        "categories": categories, "percentages": percentages,
    }


def build_movie_group1():
    df = pd.read_csv(f"{DATASETS_DIR}/movie_vote_summary.csv")
    joined_col = "audience_reception_popularity_tier_release_era"
    categories, percentages = build_intersectional_column(
        df, ["audience_reception", "popularity_tier", "release_era"], joined_col)
    out_path = f"{DATASETS_DIR}/movie_vote_summary_intersect_audience_reception_popularity_tier_release_era.csv"
    df.to_csv(out_path, index=False)
    return {
        "dataset_label": "movie_vote_summary", "group_name": "audience_reception_popularity_tier_release_era",
        "dataset_path": out_path, "date_col": "stream_date", "attr": joined_col,
        "categories": categories, "percentages": percentages,
    }


def build_movie_genre_group():
    df = pd.read_csv(f"{DATASETS_DIR}/movie_vote_summary.csv")
    df = df[df["genre-1"] != "No Genre Listed"].reset_index(drop=True)

    g1_top = df["genre-1"].value_counts().nlargest(5).index.tolist()
    g2_top = df["genre-2"].value_counts().nlargest(4).index.tolist()
    df["genre1_collapsed"] = df["genre-1"].where(df["genre-1"].isin(g1_top), "Other")
    df["genre2_collapsed"] = df["genre-2"].where(df["genre-2"].isin(g2_top), "Other")
    print(f"  genre-1 collapsed to top-5+Other: {g1_top + ['Other']}")
    print(f"  genre-2 collapsed to top-4+Other: {g2_top + ['Other']}")

    joined_col = "genre1_genre2"
    categories, percentages = build_intersectional_column(df, ["genre1_collapsed", "genre2_collapsed"], joined_col)
    out_path = f"{DATASETS_DIR}/movie_vote_summary_intersect_genre1_genre2.csv"
    df.to_csv(out_path, index=False)
    return {
        "dataset_label": "movie_vote_summary", "group_name": "genre1_genre2",
        "dataset_path": out_path, "date_col": "stream_date", "attr": joined_col,
        "categories": categories, "percentages": percentages,
    }


def build_adult_group():
    df = pd.read_csv(f"{DATASETS_DIR}/adult_census_income.csv")
    no_grad = {"Preschool", "1st-4th", "5th-6th", "7th-8th", "9th", "10th", "11th", "12th"}
    hs_college = {"HS-grad", "Some-college", "Assoc-voc", "Assoc-acdm"}
    bach_plus = {"Bachelors", "Masters", "Prof-school", "Doctorate"}

    def bucket(e):
        if e in no_grad:
            return "No Grad"
        if e in hs_college:
            return "HS Grad / Some College"
        if e in bach_plus:
            return "Bachelors+"
        raise ValueError(f"unrecognized education value: {e}")

    df["education_collapsed"] = df["education"].apply(bucket)
    print(f"  education collapsed to 3 ordinal buckets: {sorted(df['education_collapsed'].unique())}")

    joined_col = "education_race_sex"
    categories, percentages = build_intersectional_column(df, ["education_collapsed", "race", "sex"], joined_col)
    out_path = f"{DATASETS_DIR}/adult_census_income_intersect_education_race_sex.csv"
    df.to_csv(out_path, index=False)
    return {
        "dataset_label": "adult_census_income", "group_name": "education_race_sex",
        "dataset_path": out_path, "date_col": "stream_date", "attr": joined_col,
        "categories": categories, "percentages": percentages,
    }


GROUP_BUILDERS = {
    "tweets": build_tweets_group,
    "hdhi": build_hdhi_group,
    "movie1": build_movie_group1,
    "movie_genre": build_movie_genre_group,
    "adult": build_adult_group,
}


def run_group(group_key):
    print(f"\n=== Building group: {group_key} ===")
    spec = GROUP_BUILDERS[group_key]()
    categories = spec["categories"]
    percentages = spec["percentages"]
    cardinality = len(categories)
    assert BLOCK_SIZE >= cardinality, f"block_size {BLOCK_SIZE} < cardinality {cardinality}"
    validate_config(WINDOW_SIZE, BLOCK_SIZE, LANDMARK)

    fairness_target = derive_fairness_counts(percentages, BLOCK_SIZE)
    fairness_bounds = derive_fairness_bounds(percentages, BLOCK_SIZE)
    floors, ceilings = fairness_bounds
    print(f"  cardinality={cardinality}  categories(sorted)={categories}")
    print(f"  percentages={[f'{p:.4f}' for p in percentages]}")
    print(f"  reorder_target={fairness_target}  floors={floors}  ceilings={ceilings}")

    attr_dir = f"{METRICS_DIR}/{spec['dataset_label']}/intersect_{spec['group_name']}"
    os.makedirs(attr_dir, exist_ok=True)

    derivation = pd.DataFrame({
        "category": categories,
        "percentage": percentages,
        "reorder_target": fairness_target,
        "floor": floors,
        "ceiling": ceilings,
    })
    derivation.to_csv(f"{attr_dir}/fairness_derivation.csv", index=False)

    all_runs = run_phase(
        attr_dir=attr_dir, dataset_path=spec["dataset_path"], date_col=spec["date_col"],
        attr=spec["attr"], is_discrete=1, window_size=WINDOW_SIZE, block_size=BLOCK_SIZE,
        landmark=LANDMARK, fairness_target=fairness_target, fairness_bounds=fairness_bounds, phase=PHASE, runs=RUNS,
        topic_prefix=f"intersect_{group_key}", save_artifacts=False,
    )

    runs_path = f"{attr_dir}/runs.csv"
    all_runs.to_csv(runs_path, index=False)
    print(f"  saved {RUNS} runs -> {runs_path}")

    report = build_report(spec["group_name"], spec["dataset_path"], PHASE, PHASE["strategies"], RUNS, all_runs)
    report_path = f"{attr_dir}/STATS.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"  saved report -> {report_path}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Intersectional-attribute bfair-only experiments")
    parser.add_argument("--group", choices=list(GROUP_BUILDERS), action="append",
                         help="Which group(s) to run; omit to run all five.")
    args = parser.parse_args()
    groups = args.group if args.group else list(GROUP_BUILDERS)
    for g in groups:
        run_group(g)


if __name__ == "__main__":
    main()
