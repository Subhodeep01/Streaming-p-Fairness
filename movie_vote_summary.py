import pandas as pd


def summarize_movie_votes(path, movie_col="movieId", rating_col="rating", title_col="title"):
    """
    For each movie, compute the number of votes, average rating, and (optionally) title.
    Returns a DataFrame with one row per movie.
    """
    df = pd.read_csv(path)

    agg = df.groupby(movie_col)[rating_col].agg(
        avg_rating="mean",
        vote_count="count",
    ).reset_index()

    if title_col in df.columns:
        titles = df[[movie_col, title_col]].drop_duplicates(subset=movie_col)
        agg = agg.merge(titles, on=movie_col, how="left")

    agg = agg.sort_values("vote_count", ascending=False).reset_index(drop=True)
    return agg


if __name__ == "__main__":
    dataset_path = "datasets/movie_rating_3moods.csv"
    summary = summarize_movie_votes(dataset_path)
    print(summary.head(20))
    summary.to_csv("datasets/movie_vote_summary.csv", index=False)
    print(f"\nSaved {len(summary)} movies to movie_vote_summary.csv")
