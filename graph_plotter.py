import pandas as pd
import matplotlib.pyplot as plt
import argparse

# Function to plot metrics for varying one parameter while fixing the others
def plot_metrics(df, vary_col, fixed_cols, fixed_vals, title_suffix, file_path):
    subset = df.copy()
    for col, val in zip(fixed_cols, fixed_vals):
        subset = subset[subset[col] == val]

    subset = subset.sort_values(by=vary_col)
    x = subset[vary_col]

    plt.figure(figsize=(10, 6))
    for metric in ['Avg preprocessing', 'Avg query processing', 'Preprocessing: Sketch Building time']:
        plt.plot(x, subset[metric], marker='o', label=metric)

    plt.xlabel(vary_col)
    plt.ylabel('Time (seconds)')
    plt.title(f'Metrics vs {vary_col} ({title_suffix})')
    plt.legend()
    plt.grid(True)
    plt.savefig(f"figures/{file_path[:-4]}_varying_{vary_col}")
    plt.show()
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consumer args")
    parser.add_argument("--window_size", type=int, required=True)
    parser.add_argument("--block_size", type=int, required=True)
    parser.add_argument("--cardinality", type=int, required=True)
    parser.add_argument("--metric_file", type=str, required=True)
    args = parser.parse_args()
    
    fixed_win = args.window_size
    fixed_block = args.block_size
    fixed_cardinality = args.cardinality
    file_path = args.metric_file
    
    df = pd.read_csv("metrics/"+file_path)
    # 1. Vary Window size (fix Block size=10, Cardinality=5)
    plot_metrics(df, 'Window size', ['Block size', 'Cardinality'], [fixed_block, fixed_cardinality], f'Block size={fixed_block}, Cardinality={fixed_cardinality}', file_path)

    # 2. Vary Block size (fix Window size=50, Cardinality=5)
    plot_metrics(df, 'Block size', ['Window size', 'Cardinality'], [fixed_win, fixed_cardinality], f'Window size={fixed_win}, Cardinality={fixed_cardinality}', file_path)

    # 3. Vary Cardinality (fix Window size=50, Block size=10)
    plot_metrics(df, 'Cardinality', ['Window size', 'Block size'], [fixed_win, fixed_block], f'Window size={fixed_win}, Block size={fixed_block}', file_path)