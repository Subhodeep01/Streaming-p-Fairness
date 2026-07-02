import pandas as pd
import numpy as np


def inspect_dataset(path, n_preview=5):
    """Print a quick feature-level summary of any CSV dataset."""
    df = pd.read_csv(path)

    print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns\n")
    print("--- Preview ---")
    print(df.head(n_preview), "\n")

    print("--- Column overview ---")
    overview = pd.DataFrame({
        "dtype": df.dtypes,
        "n_missing": df.isna().sum(),
        "pct_missing": (df.isna().mean() * 100).round(2),
        "n_unique": df.nunique(),
    })
    print(overview, "\n")

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols):
        print("--- Numeric feature stats ---")
        print(df[numeric_cols].describe().T, "\n")

    cat_cols = df.select_dtypes(exclude=[np.number]).columns
    if len(cat_cols):
        print("--- Categorical feature value counts (top 5) ---")
        for col in cat_cols:
            print(f"\n{col}:")
            print(df[col].value_counts().head(5))

    return df


if __name__ == "__main__":
    dataset_path = "datasets/tweet_frequency_summary.csv"  # change to your dataset path
    df = inspect_dataset(dataset_path)
