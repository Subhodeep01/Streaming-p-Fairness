import pandas as pd
from utils import position_finder, load_clean, make_bins

csv_ip = input("Enter the name of the dataset to produce from (type exact without any extension like .csv): ")
CSV    = "datasets/"+csv_ip+".csv"

df = pd.read_csv(CSV)
print(f"Here are the attributes for your given CSV {CSV}: \n", df.columns)
date_col = input("Please write the appropiate date column name for the given dataset (e.g. DATE, D.O.A., etc): ")
attr = input("Please write the name of the attribute you want to stream and monitor: ")
print("Values for the selected attribute: ", df[attr])
is_discrete = int(input("Is the selected attribute discrete? (0 == No | 1 == Yes)"))
clean = load_clean(CSV, attr, date_col)
print(clean)
if is_discrete == 0:
    cleaned_df, summary = make_bins(clean, date_col, attr, max_bins=5, min_pct= 0.15)
    print("After binning we have: \n", summary.to_string(index=False))
    summary.to_csv(f"cleaned_input_files/summary_{cleaned_df.columns[0]}.csv", index=False)
else:
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    cleaned=df.dropna(subset=date_col).reset_index(drop=True)
    cleaned_df = clean[[attr]]
    print(cleaned_df)

cleaned_df.to_csv("cleaned_input_files/cleaned_df.csv", index=False)

