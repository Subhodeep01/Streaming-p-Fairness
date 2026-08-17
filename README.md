# Streaming Fairness Framework

## 🛠 How to Get Started

### 1. Clone the Repository

```bash
git clone <repo_url>
```

> 💡 It is highly recommended to open the directory in **VSCode**. The code was built on Windows 11 OS. Should be compatible with MacOS but Windows OS is recommended.

### 2. Set Up Python Environment

Create a Python virtual environment (tested on 3.14) and install dependencies:

```bash
pip install confluent-kafka pandas numpy scipy more-itertools
```

That covers everything needed to run the producer/consumer scripts and every `simulate_*.py`/`compute_ndcg*.py` experiment script. Only needed if you're also running the web API (`api/main.py`):

```bash
pip install fastapi uvicorn pydantic
```

Versions this repo is currently tested against: `confluent-kafka==2.15.0`, `pandas==3.0.3`, `numpy==2.5.0`, `scipy==1.18.0`, `more-itertools==11.1.0`, `fastapi==0.139.0`, `uvicorn==0.49.0`, `pydantic==2.13.4`.

### 3. Start Kafka via Docker

Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop) is installed and running, then run:

```bash
docker compose -f ./zk-single-kafka-single.yml up -d
```

### 4. Prepare Directory Structure

Before producing and consuming data:

- Delete and rereate the `datasets` directory and place all datasets there.  
  📥 Download from: [Datasets](https://osf.io/q4fu2/overview?view_only=04e3328f2c514ee3b8f4a4822f1c9a23)
- Create a `metrics` directory to store performance metrics.

---

## ▶️ Run Producer

```bash
python run_producer.py
```

> 🛠 If errors occur (e.g. missing modules), use:
```bash
python producer.py --topic_name <your_topic>
```

### ⚠️ Input Notes

- You will be prompted for a topic name—change it when switching datasets or attributes.
- When prompted for `is_discrete`, input `1` (Yes) or `0` (No).
- ❗ If you make a mistake during input, terminate and rerun with a new topic.

---

## 📖 Run Read-Only Consumer

```bash
python run_consumer_readable.py
```

> 🛠 If errors occur, use:
```bash
python consumer.py --window_size <size> --block_size <size> --topic_name <topic> --max_windows <count>
```

### ⚠️ Input Notes

- ✅ Use the same topic name as the producer.
- ✅ Ensure `block size` divides `window size`.
- ✅ For fairness counts, input **absolute values** (e.g., 10 for M, 10 for F in a block of size 20).
- ❗ The counts **must sum to block size** and **none should be zero**, or the program will crash.
- 📈 Results will be printed and performance metrics for the session will be stored in `metrics/`.

---

## ✨ Run Editable/Reorderable Consumer

```bash
python run_consumer_editable.py
```

> 🛠 If errors occur, use:
```bash
python consumer_editable_performance.py --window_size <size> --block_size <size> --topic_name <topic> --max_windows <count> --landmark <value> --brt_force False --backward False
```

### ⚠️ Input Notes

- ✅ Use the same topic name as the producer.
- ✅ `block size` must divide `window size + landmark`.
- ✅ `landmark` must not exceed `window size`.
- ✅ Use **counts** for fairness (e.g., 10 for F and 10 for M).
- ❗ Counts **must sum to block size** and **none should be zero**.
- 📈 Results will be printed and performance metrics for the session will be stored in `metrics/`.

---

## 🔌 Using `bfair.py` as a Plug-and-Play Module

`bfair.py` is a single, dependency-free file — it only uses the Python standard library, so you can copy it into any project and import it directly with no install step.

```python
from bfair import bfair_reorder, is_block_fair, sliding_fair_count

# items are their own group label by default
out = bfair_reorder(list("AABBBCCCC"), {"A": 0.3, "B": 0.3, "C": 0.4}, block_size=3)

# or pass attr_fn to pull the group off richer items (dicts, objects, rows, ...)
rows = [{"gender": "m"}, {"gender": "f"}, {"gender": "f"}, {"gender": "m"}]
out = bfair_reorder(rows, {"m": 0.5, "f": 0.5}, block_size=4, attr_fn=lambda r: r["gender"])
```

**API**

- `bfair_reorder(stream, fairness_constraint, block_size, attr_fn=lambda x: x)` — returns a permutation of `stream` that maximizes the number of fair size-`block_size` sliding blocks. `fairness_constraint` is `{group: target_proportion}` with proportions summing to 1; `attr_fn` maps an item to its group (identity by default).
- `is_block_fair(block, fairness_constraint, attr_fn=...)` — checks whether one block meets the `[floor, ceil]` bound for every group.
- `sliding_fair_count(stream, fairness_constraint, block_size, attr_fn=...)` — counts how many of the `N - block_size + 1` sliding windows are fair; useful for verifying/benchmarking a reordering.

**Requirements to plug it in**

- Every item's group (via `attr_fn`) must be a key in `fairness_constraint`, or it raises `KeyError`.
- The target proportions must sum to 1 (within `1e-6`), or it raises `ValueError`.
- `block_size` must be a positive integer.

To wire it into a streaming pipeline: buffer items into a `deque`/list per window/landmark, call `bfair_reorder` on that buffer with your fairness targets, and emit the returned order downstream — that's exactly how `consumer_editable_bfair_performance.py` uses it in this repo, so that file (and `simulate_stream.py` for an offline example) are good references for integrating it into a live consumer loop.

---

## 🧪 Running Experiments (`simulate_*.py`)

These scripts drive Kafka end-to-end themselves (shuffle → produce → consume → collect) — no manual producer/consumer pairing needed. All require Kafka running (step 3 above) and `datasets/` populated (step 4).

### `simulate_stream.py` — attribute/dataset-generic sweep harness

The main harness. Every config is measured through two phases: **fast** (`3strat_5000win` — naive_bfair_reorder/bfairreorder/internal_swap at 5000 windows) and **slow** (`5strat_500win` — adds greedy_swap/weighted_greedy_swap at 500 windows, since those two are too slow to run at full scale).

```bash
# One fixed config, both phases
python simulate_stream.py --attr GENDER --dataset datasets/HDHI_Admission_data_modified.csv --date-col D.O.A --mode single

# Full W/s/X parameter sweep
python simulate_stream.py --attr sentiment --dataset datasets/tweets.csv --date-col stream_date --mode sweep

# Only the fast phase, only the W sweep
python simulate_stream.py --attr sentiment --dataset datasets/tweets.csv --date-col stream_date --mode sweep --phase fast --sweep W
```

Key flags:
- `--is-discrete 0` — attribute is continuous and needs binning first (e.g. stock `Volume`); pair with `--max-bins N` (default 5) to control bin count.
- `--dayfirst` — parse `--date-col` as day-first (e.g. `DD-MM-YYYY`). Only set this for genuinely day-first date columns — it also affects unambiguous `YYYY-MM-DD` columns, so leave it off unless you've checked the format.
- `--save-reordered` — persist the pre/post-reorder window+landmark chunks to `<attr_dir>/original/` and `<attr_dir>/reordered/` (fast phase only).
- `--extra-cols col1,col2` — attach extra source-row columns (e.g. `movieId,avg_rating,vote_count`) to those saved chunks, reattached post-reorder via stable per-category consumption. Only meaningful with `--save-reordered`.

Output lands in `metrics/<dataset_label>/<attr>/` (`runs.csv`, `STATS.txt`, `_sweep_cache/`, and optionally `original/`+`reordered/`).

### `simulate_intersectional.py` — Cartesian-product attribute experiments

Builds a new categorical column as the Cartesian product of 2-3 existing attributes (e.g. `AGE_bin × OUTCOME × SMOKING`), with fairness floors derived from that column's real empirical joint frequency (not an independence assumption), and runs all 5 strategies at a fixed 500-window config.

```bash
# All 5 predefined groups (tweets, hdhi, movie1, movie_genre, adult)
python simulate_intersectional.py

# Just one
python simulate_intersectional.py --group hdhi
```

Output: `metrics/<dataset_label>/intersect_<group_name>/` (`runs.csv`, `STATS.txt`, `fairness_derivation.csv`).

### `simulate_bfair_x_ablation.py` — fine-grained landmark (X) ablation

bfairreorder-only (no baselines, nothing to compare against), fixed `window_size=500`/`block_size=25`, sweeping landmark across a fine-grained integer range instead of the coarse `[50,100,250,500]` used elsewhere.

```bash
python simulate_bfair_x_ablation.py --attr genre-1 --dataset datasets/movie_vote_summary.csv --date-col stream_date
python simulate_bfair_x_ablation.py --attr genre-1 --dataset datasets/movie_vote_summary.csv --date-col stream_date --runs 2 --x-min 1 --x-max 3 --x-step 1
```

### Computing NDCG

Requires `--save-reordered --extra-cols <relevance_col>` to have already been run for the attribute (so `original/`+`reordered/` chunks with a relevance column like `avg_rating` or `likes` exist).

```bash
# Per-window NDCG (whole window is one ranked list)
python compute_ndcg.py --attr-dir metrics/movie_vote_summary/genre-1 --relevance-col avg_rating

# Per-block NDCG (each block_size-sized block is its own ranked list, then averaged)
python compute_ndcg_per_block.py --attr-dir metrics/movie_vote_summary/genre-1 --relevance-col avg_rating

# Omit --attr-dir/--relevance-col to run every dir in the script's built-in ATTR_RELEVANCE map
python compute_ndcg_per_block.py
```

Both compare relevance-ranking quality (linear gain, self-normalized IDCG) between the pre-reorder and bfairreorder-reordered stream for the same window — `compute_ndcg.py` scores the whole window as one ranked list; `compute_ndcg_per_block.py` scores each block separately (the granularity bfairreorder's fairness target actually applies at) and averages. Output: `<attr_dir>/ndcg_summary.csv` / `ndcg_per_block_summary.csv`, plus a combined `metrics/ndcg_all_summary.csv` / `metrics/ndcg_per_block_all_summary.csv` when run without `--attr-dir`.

---