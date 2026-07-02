import pandas as pd


def summarize_tweet_frequency(path, user_col="user", tweet_id_col="ids"):
    """
    For each user id, count how many tweets they made.
    Returns a DataFrame with one row per user and their tweet frequency.
    """
    df = pd.read_csv(path)

    summary = (
        df.groupby(user_col)[tweet_id_col]
        .count()
        .reset_index(name="tweet_count")
        .sort_values("tweet_count", ascending=False)
        .reset_index(drop=True)
    )
    return summary


if __name__ == "__main__":
    dataset_path = "datasets/output_tweets.csv"
    summary = summarize_tweet_frequency(dataset_path)
    print(summary.head(20))
    summary.to_csv("datasets/tweet_frequency_summary.csv", index=False)
    print(f"\nSaved {len(summary)} users to tweet_frequency_summary.csv")
