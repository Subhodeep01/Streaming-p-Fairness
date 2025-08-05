import pandas as pd
import math
import numpy as np
from collections import deque

def position_finder(df: pd.Series, fairness, block_size):
    unique = df.iloc[:, 0].unique().tolist()
    unique.sort()
    position = {unique[i]: i for i in range(len(unique))}
    if len(fairness) == 0:
        for i in range(len(unique)):
            fairness[unique[i]] = int(input(f"Input the fairness constraint for {df.columns[0]} {unique[i]} (how many of this item you want in the stream) cannot exceed {block_size}: "))
            if sum(fairness.values()) >= block_size:
                print("Total fairness exceeding 100%. Updating the rest...")
                diff = sum(fairness.values()) - block_size
                fairness[unique[i]] -= diff
                rest = {unique[j]: 0 for j in range(i+1, len(unique))}
                fairness.update(rest)
                break
        print("Fairness constraints: ", fairness)
    return position, fairness

def sketcher(df: pd.Series, sketch, position) -> dict:
    """
    Builds or updates sketch as required
    """
    df_num = df
    # print("DF Num",df_num)
    rec = [0]*len(position)
    # print("Rec and position",rec,position)
    if len(sketch) == 0:
        for i in df_num:
            rec[position[i]] += 1
            sketch.append(tuple(rec))
        return None
    # elif len(sketch) == window_size
    else:
        popped_ele = sketch.pop(0)
        # print("popped_ele",popped_ele)
        i = df_num.iloc[-1]
        # print(i)
        last = sketch[-1]
        rec[position[i]] += 1
        rec = [x + y for x, y in zip(last, rec)]

        sketch.append(tuple(rec))

    return popped_ele

def add_element(sketch_bwd, ele):
    sketch_bwd = deque(sketch_bwd)  # using deque and appending makes the time complexity O(1)
    sketch_bwd.appendleft(ele)
    sketch_bwd = list(sketch_bwd)


def bwd_sketcher(df: pd.Series, position) -> dict:
    """
    Builds backward sketch
    """
    sketch_bwd = deque([])
    df_num = df
    # print(sketch_bwd)
    rec = [0]*len(position)
    for i in df_num[::-1]:
        rec[position[i]] += 1
        sketch_bwd.appendleft(tuple(rec))
    sketch_bwd = list(sketch_bwd)
    return sketch_bwd

    
def verify_bwd_sketch(sketch, position, block_size, fairness):
    rec = [0]*len(position)
    # stream_data = {}
    stream_data = []
    fair_block = 0
    for i in range(0, len(sketch), block_size):
        block_num = i//block_size + 1
        fairs = 0
        block_start = sketch[i]
        if i + block_size < len(sketch):
            block_end = sketch[i+block_size]
        else:
            block_end = rec
        
        for key, pos in position.items():
            diff = block_end[pos] - block_start[pos]
            if diff >= fairness[key]:
                fairs += 1
                
        if fairs == len(position.keys()):
            fair_block += 1
            stream_data.append(f"Block {block_num} is p-fair ✅")
        else:
            stream_data.append(f"Block {block_num} is not p-fair ❌")
    
    return stream_data, fair_block

def verify_sketch(sketch, position, block_size, fairness, popped_ele):
    rec = [0]*len(position)
    # stream_data = {}
    stream_data = []
    fair_block = 0
    for i in range(0, len(sketch), block_size):
        block_num = i//block_size + 1
        fairs = 0
        if block_num == 1:
            if popped_ele: block_start = popped_ele
            else: block_start = rec
        else:
            block_start = sketch[i-1]
        block_end = sketch[i+block_size - 1]

        
        for key, pos in position.items():
            diff = block_end[pos] - block_start[pos]
            if diff >= fairness[key]:
                fairs += 1
                
        if fairs == len(position.keys()):
            fair_block += 1
        #     print(f"Block {block_num} is p-fair ✅")
        # else:
        #     print(f"Block {block_num} is not p-fair ❌")
    # print(fair_block, len(sketch)//block_size)
    if fair_block == len(sketch)//block_size:
        stream_data.append(f"Window is p-fair ✅")
    else:
        stream_data.append(f"Window is not p-fair ❌")
    
    return stream_data, fair_block



# PREPROCESSING TOOLS ---------------------------------------------------------------------------------------------

def load_clean(path, col, date_col) -> pd.DataFrame:
    df = pd.read_csv(path, na_values=["", " ", "NA", "N/A", "--"])
    # print(df, col)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    cleaned = df.dropna(subset=date_col).reset_index(drop=True)
    print(f"Loaded {len(df)} rows – kept {len(cleaned)} after cleaning.")
    return cleaned

def bin_with_min_pct(series: pd.Series,
                     max_bins: int = 5,
                     min_pct: float = 0.15):
    n = len(series)
    # work downward from max_bins to 1
    for q in range(max_bins, 0, -1):
        try:
            # q labels are 0 … q-1
            bin_codes, bin_edges = pd.qcut(
                series, q=q, labels=False, retbins=True,
                duplicates="drop", precision=6
            )
        except ValueError:
            # too many duplicate edges – try fewer bins
            continue
        counts = pd.Series(bin_codes).value_counts(normalize=True)
        if counts.min() >= min_pct:
            return bin_codes.astype(int), bin_edges
    # fallback: a single bin covering the whole range
    return pd.Series(0, index=series.index), np.array([series.min(), series.max()])

def make_bins(df, date, col="AGE",
              max_bins=5, min_pct=0.15):
    summary = []                    # rows for the info table


    codes, edges = bin_with_min_pct(df[col], max_bins, min_pct)
    df[f"{col}_bin"] = codes

    # record the edge ranges & counts
    for i in range(len(edges) - 1):
            left, right = edges[i], edges[i + 1]
            closed_right = i == len(edges) - 2  # last bin is right-closed
            rng_str = (f"[{left:.2f}, {right:.2f}]"
                       if closed_right else
                       f"[{left:.2f}, {right:.2f})")
            count = (codes == i).sum()
            summary.append(
                dict(Column=col,
                     Bin=i,
                     Range=rng_str,
                     Count=count,
                     Percent=f"{100*count/len(df):.1f}%")
            )

    return df[[f"{col}_bin"]], pd.DataFrame(summary)

# clean_df = load_clean("HDHI_Admission_data.csv", "AGE")
# binned_df, ranges_df = make_bins(clean_df)

# print("\n=== Bin ranges & sizes ===")
# print(ranges_df.to_string(index=False))
# clean_df.to_csv("HDHI_Admission_data_binned.csv", index=False)

# data_dict = {'age': ['a', 'b', 'c', 'c', 'a', 'b', 'a', 'a', 'b', 'c']}

# df = pd.DataFrame(data_dict)
# print(df)
# position, fairness = position_finder(df)
# print(position)
# l = [0] * len(position)
# sketch = []
# pop = sketcher(df, sketch, position)
# print(sketch)
# verify_sketch(sketch, position, 5, fairness, pop)