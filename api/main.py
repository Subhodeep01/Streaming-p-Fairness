"""
FastAPI server bridging the Kafka p-Fairness consumer to the React UI via WebSockets.

Run from the Streaming-p-Fairness root:
    uvicorn api.main:app --reload --port 8000
"""

import asyncio
import json
import math
import os
import queue
import re
import sys
import threading
import time
import tracemalloc
import socket
from collections import defaultdict
from typing import Dict, List

import numpy as np
import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
from utils import sketcher, verify_sketch
from bfair import bfair_reorder as _bfair_reorder

app = FastAPI(title="Streaming p-Fairness API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Dataset configs ───────────────────────────────────────────────────────────

BUNDLED_DATASETS = {
    "Hospital Admissions Data": {
        "csv": "datasets/HDHI_Admission_data.csv",
        "csv_alts": ["datasets/HDHI_Admission_data_modified.csv"],
        "topic_base": "hospital",
        "date_column": "D.O.A",
        "attributes": [
            {"label": "Gender", "column": "GENDER"},
            {"label": "Hospitalization Outcome", "column": "OUTCOME"},
            {"label": "Age", "column": "AGE_BIN"},
        ],
    },
    "Stocks (AAPL)": {
        "csv": "datasets/AAPL_pct_change_binned.csv",
        "topic_base": "stock",
        "date_column": "Date",
        "attributes": [
            {"label": "Price Change", "column": "PRICE_CHANGE_BIN"},
            {"label": "Volume", "column": "VOLUME_BIN"},
        ],
    },
    "Tweets": {
        "csv": "datasets/tweets.csv",
        "topic_base": "tweets",
        "attributes": [
            {"label": "Engagement", "column": "engagement"},
            {"label": "Tweet Length", "column": "tweet_length_tier"},
            {"label": "Sentiment", "column": "sentiment"},
            {"label": "Topic", "column": "topic"},
        ],
    },
    "Movies": {
        "csv": "datasets/movie_vote_summary.csv",
        "topic_base": "movies",
        "attributes": [
            {"label": "Audience Reception", "column": "audience_reception"},
            {"label": "Popularity Tier", "column": "popularity_tier"},
            {"label": "Release Era", "column": "release_era"},
            {"label": "Genre", "column": "genre-1"},
        ],
    },
    "Census": {
        "csv": "datasets/adult_census_income_education_collapsed.csv",
        "topic_base": "census",
        "attributes": [
            {"label": "Sex", "column": "sex"},
            {"label": "Education", "column": "education_collapsed"},
            {"label": "Race", "column": "race"},
            {"label": "Marital Status", "column": "marital_status"},
            {"label": "Occupation", "column": "occupation"},
        ],
    },
}


def all_datasets() -> dict:
    merged = dict(BUNDLED_DATASETS)
    merged.update(discover_custom_datasets())
    return merged


def _preprocess_hospital(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: str(c).strip())

    def text(col, default=""):
        return df[col].astype(str) if col in df.columns else pd.Series(default, index=df.index)

    def flag(col, yes, no):
        if col not in df.columns:
            return pd.Series("Unknown", index=df.index)
        s = df[col]
        if pd.api.types.is_numeric_dtype(s):
            return s.map({1: yes, 0: no}).fillna("Unknown")
        vals = s.astype(str).str.strip().str.lower()
        return pd.Series(
            [
                "Unknown" if v in ("", "nan", "none")
                else no if v.startswith("not") or v in ("0", "no", "false")
                else yes
                for v in vals
            ],
            index=df.index,
        )

    out = pd.DataFrame()
    out["GENDER"] = df["GENDER"].astype(str).str.strip()
    outcome_map = {"DISCHARGE": "discharged", "EXPIRY": "expired", "DAMA": "dama"}
    out["OUTCOME"] = df["OUTCOME"].astype(str).str.strip().map(outcome_map).fillna("discharged")
    out["AGE_BIN"] = pd.cut(
        df["AGE"],
        bins=[0, 51, 60, 65, 72, 200],
        labels=["4-51", "51-60", "60-65", "65-72", "72+"],
        right=False,
    ).astype(str)
    out["MRD_NO"] = text("MRD No.")
    out["AGE"] = df["AGE"].astype(str)
    out["RURAL"] = text("RURAL").str.strip()
    out["D_O_A"] = text("D.O.A")
    out["DURATION_OF_STAY"] = text("DURATION OF STAY")
    out["ICU_STAY"] = text("duration of intensive unit stay")
    out["ADMISSION_TYPE"] = text("TYPE OF ADMISSION-EMERGENCY/OPD").str.strip()
    out["SMOKING"] = flag("SMOKING", "Smoker", "Non-Smoker")
    out["ALCOHOL"] = flag("ALCOHOL", "Yes", "No")
    out["DIABETES"] = flag("DM", "Yes", "No")
    out["HYPERTENSION"] = flag("HTN", "Yes", "No")
    out["_display_title"] = "MRD " + text("MRD No.")
    return out


def _preprocess_stocks(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    if "bins" in df.columns:
        price_labels = {
            0: "-11.78% to -1.51%",
            1: "-1.51% to -0.39%",
            2: "-0.39% to 0.34%",
            3: "0.34% to 1.52%",
            4: "1.52% to 19.27%",
        }
        out["PRICE_CHANGE_BIN"] = df["bins"].map(price_labels).fillna("Unknown")
    else:
        from utils import bin_with_min_pct
        codes, edges = bin_with_min_pct(df["% Change"], max_bins=5, min_pct=0.15)
        price_labels = {
            i: f"{edges[i]:.2f}% to {edges[i + 1]:.2f}%" for i in range(len(edges) - 1)
        }
        out["PRICE_CHANGE_BIN"] = pd.Series(codes).map(price_labels).fillna("Unknown")
    out["VOLUME_BIN"] = pd.cut(
        df["Volume"],
        bins=[0, 57664900, float("inf")],
        labels=["Low Volume", "High Volume"],
    ).astype(str)
    out["DATE"] = df["Date"].astype(str)
    out["PCT_CHANGE"] = df["% Change"].astype(str)
    out["VOLUME"] = df["Volume"].astype(str)
    out["_display_title"] = df["Date"].astype(str)
    return out


def _preprocess_tweets(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["engagement"] = df["engagement"].astype(str).str.strip()
    out["tweet_length_tier"] = df["tweet_length_tier"].astype(str).str.strip()
    out["sentiment"] = df["sentiment"].astype(str).str.strip()
    out["topic"] = df["topic"].astype(str).str.strip()
    out["likes"] = df["likes"].astype(str)
    out["tweet"] = df["tweet"].astype(str).str[:120]
    out["_display_title"] = df["topic"].astype(str).str.strip()
    return out


def _preprocess_movies(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["audience_reception"] = df["audience_reception"].astype(str).str.strip()
    out["popularity_tier"] = df["popularity_tier"].astype(str).str.strip()
    out["release_era"] = df["release_era"].astype(str).str.strip()
    out["genre-1"] = df["genre-1"].astype(str).str.strip()
    out["title"] = df["title"].astype(str)
    out["avg_rating"] = df["avg_rating"].round(2).astype(str)
    out["vote_count"] = df["vote_count"].astype(str)
    out["genres"] = df["genres"].astype(str)
    out["_display_title"] = df["title"].astype(str).str.extract(r'^(.+?)\s*\(\d{4}\)')[0].fillna(df["title"].astype(str))
    return out


def _preprocess_census(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["sex"] = df["sex"].astype(str).str.strip()
    out["education_collapsed"] = df["education_collapsed"].astype(str).str.strip()
    out["race"] = df["race"].astype(str).str.strip()
    out["marital_status"] = df["marital_status"].astype(str).str.strip()
    out["occupation"] = df["occupation"].astype(str).str.strip()
    out["age"] = df["age"].astype(str)
    out["workclass"] = df["workclass"].astype(str).str.strip()
    out["income"] = df["income"].astype(str).str.strip()
    out["hours_per_week"] = df["hours_per_week"].astype(str)
    out["native_country"] = df["native_country"].astype(str).str.strip()
    out["_display_title"] = df["age"].astype(str) + " y/o · " + df["occupation"].astype(str).str.strip()
    return out


def broker() -> str:
    """Kafka bootstrap server; override with COBLOC_BROKER when the broker is
    not on this machine."""
    return os.environ.get("COBLOC_BROKER", "localhost:9092")


def datasets_dir() -> str:
    """Where CSVs live. Set COBLOC_DATASETS to point at your own folder;
    otherwise ./datasets beside wherever the server was started, falling back
    to the copy inside the repo for a dev checkout."""
    env = os.environ.get("COBLOC_DATASETS")
    if env:
        return os.path.abspath(env)
    cwd = os.path.join(os.getcwd(), "datasets")
    if os.path.isdir(cwd):
        return cwd
    repo = os.path.join(_ROOT, "datasets")
    if os.path.isdir(repo):
        return repo
    # Installed with nothing to point at yet: name the path the user is most
    # likely to create, not one buried in site-packages.
    return cwd


def resolve_csv(cfg: dict) -> str:
    names = [cfg["csv"]] + cfg.get("csv_alts", [])
    for base in (datasets_dir(), os.path.join(_ROOT, "datasets")):
        for rel in names:
            path = os.path.join(base, os.path.basename(rel))
            if os.path.exists(path):
                return path
    return os.path.join(datasets_dir(), os.path.basename(cfg["csv"]))


def discover_custom_datasets() -> dict:
    """Any CSV dropped into the datasets folder that is not one of the bundled
    five shows up as its own dataset, with low-cardinality columns offered as
    protected attributes."""
    known = set()
    for cfg in BUNDLED_DATASETS.values():
        for rel in [cfg["csv"]] + cfg.get("csv_alts", []):
            known.add(os.path.basename(rel).lower())

    found = {}
    d = datasets_dir()
    if not os.path.isdir(d):
        return found
    for fname in sorted(os.listdir(d)):
        if not fname.lower().endswith(".csv") or fname.lower() in known:
            continue
        try:
            head = pd.read_csv(os.path.join(d, fname), nrows=2000)
        except Exception:
            continue
        attrs = []
        for col in head.columns:
            vals = head[col].dropna().astype(str).str.strip()
            n = vals.nunique()
            if 2 <= n <= 20 and vals.str.len().max() <= 60:
                attrs.append({"label": str(col), "column": str(col)})
        if not attrs:
            continue
        name = os.path.splitext(fname)[0]
        found[name] = {
            "csv": f"datasets/{fname}",
            "topic_base": "custom_" + "".join(c if c.isalnum() else "_" for c in name).lower()[:40],
            "attributes": attrs[:8],
            "custom": True,
        }
    return found


def _preprocess_custom(df: pd.DataFrame) -> pd.DataFrame:
    """User-supplied CSV: keep every column as text and title rows by the
    first column, so the UI has something to show without a bespoke mapper."""
    out = df.rename(columns=lambda c: str(c).strip()).copy()
    for c in out.columns:
        out[c] = out[c].astype(str).str.strip()
    if len(out.columns):
        # Whole words only. A substring test drops "sentiment", which
        # contains "time" and is a protected attribute, not a date.
        date_like = re.compile(r"(^|[^a-z])(date|time|timestamp|doa)([^a-z]|$)", re.I)
        for c in [c for c in out.columns if date_like.search(str(c))]:
            out = out.drop(columns=[c])
        out["_display_title"] = out[out.columns[0]]
    return out


def preprocess_for(name: str, cfg: dict, df: pd.DataFrame) -> pd.DataFrame:
    fn = DATASET_PREPROCESSORS.get(name)
    if fn:
        return fn(df)
    if cfg.get("custom"):
        return _preprocess_custom(df)
    return df


DATASET_PREPROCESSORS = {
    "Hospital Admissions Data": _preprocess_hospital,
    "Stocks (AAPL)": _preprocess_stocks,
    "Tweets": _preprocess_tweets,
    "Movies": _preprocess_movies,
    "Census": _preprocess_census,
}

# ── Shared state ──────────────────────────────────────────────────────────────
_active_ws: List[WebSocket] = []
_metrics_queue: queue.Queue = queue.Queue()
_stop_event = threading.Event()
_is_running = False
_is_producing = False
_current_metrics: dict = {}
_topic_counters: Dict[str, int] = {}
_producer_generation = 0
# Bumped on every start/stop. A consumer loop exits as soon as its own
# generation is stale, which _stop_event alone could not guarantee: stop
# cleared _is_running eagerly and the next start cleared the event again,
# reviving the old thread so window numbers carried over between sessions.
_consumer_generation = 0

# How far the consumer may run ahead of the window the user is looking at.
# Windows are stepped through by hand, so a consumer that streams flat out
# produces tens of thousands of windows nobody will ever open.
LEAD_WINDOWS = 200
_client_position = 0


# Upper bounds mirroring the UI. Every window is held in memory, reordered and
# pushed over the socket, so an unbounded window size makes the server and the
# browser unusable rather than merely slow. Enforced here too so a direct call
# to /api/start cannot get past the inputs.
MAX_WINDOW_SIZE = 1000
# The landmark bound is set by the summary sweep, not by the reorder. Sweeping
# 1..X costs roughly X^2: landmark 100 over a 100 window session takes ~20s,
# while the old 5000 ceiling worked out at ~14 hours and simply hung the
# summary screen. 100 is also the range the ablation script itself sweeps.
MAX_LANDMARK_SIZE = 100
MAX_WINDOWS = 100_000


class ConsumerConfig(BaseModel):
    topic_name: str
    window_size: int = Field(ge=1, le=MAX_WINDOW_SIZE)
    block_size: int = Field(ge=1, le=MAX_WINDOW_SIZE)
    fairness: Dict[str, int]
    proportions: Dict[str, float] = {}
    landmark_size: int = Field(default=5, ge=0, le=MAX_LANDMARK_SIZE)
    attribute_column: str = "GENDER"
    max_windows: int = Field(default=50, ge=1, le=MAX_WINDOWS)
    delay_ms: int = Field(default=0, ge=0, le=60_000)


class ProduceConfig(BaseModel):
    dataset_name: str


# ── Fairness constraint helpers ───────────────────────────────────────────────

def with_observed_groups(props: Dict[str, float], rows: list, col: str) -> Dict[str, float]:
    """bfair raises on any item whose group is not a key in the constraint, so
    one stray or blank value aborts the whole call. Values the caller did not
    name get a zero target, which is the honest reading: no share was asked
    for, so a block containing one cannot be fair."""
    out = dict(props)
    for r in rows:
        g = str(r.get(col, ""))
        if g not in out:
            out[g] = 0.0
    return out


def normalized_proportions(proportions: Dict[str, float], fairness: Dict[str, int]) -> Dict[str, float]:
    raw = {str(k): float(v) for k, v in (proportions or fairness).items()}
    total = sum(raw.values())
    if total <= 0:
        raise ValueError("fairness proportions must include at least one positive target")
    return {k: v / total for k, v in raw.items()}


def bounds_from_proportions(props: Dict[str, float], block_size: int) -> tuple[dict, dict]:
    floor = {k: math.floor(p * block_size) for k, p in props.items()}
    ceiling = {k: math.ceil(p * block_size) for k, p in props.items()}
    return floor, ceiling


def count_fair_blocks(rows: list, col: str, floor: dict, ceiling: dict, block_size: int) -> int:
    fair = 0
    for start in range(0, len(rows) - block_size + 1, block_size):
        counts: dict = defaultdict(int)
        for row in rows[start:start + block_size]:
            counts[str(row.get(col, ""))] += 1
        if all(floor[k] <= counts.get(k, 0) <= ceiling[k] for k in floor):
            fair += 1
    return fair


# ── Broadcast helpers ─────────────────────────────────────────────────────────
async def _broadcast(msg: dict):
    # Bound each send. Awaiting a client that has stopped reading blocks this
    # loop, and with it every HTTP request the server owes anyone else, which
    # made the whole API look hung whenever a viewer fell behind the stream.
    dead = []
    for ws in _active_ws:
        try:
            await asyncio.wait_for(ws.send_json(msg), timeout=2.0)
        except asyncio.TimeoutError:
            dead.append(ws)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in _active_ws:
            _active_ws.remove(ws)


async def _drain_forever():
    global _is_running, _current_metrics
    while True:
        try:
            msg = _metrics_queue.get_nowait()
        except queue.Empty:
            await asyncio.sleep(0.02)
            continue

        if msg.get("type") in ("window_update", "current_metrics"):
            _current_metrics = msg.get("metrics", _current_metrics)

        await _broadcast(msg)

        if msg.get("type") in ("done", "error"):
            _is_running = False


@app.on_event("startup")
async def _startup():
    asyncio.create_task(_drain_forever())


# ── Consumer thread ───────────────────────────────────────────────────────────
def _run_consumer(config: ConsumerConfig, generation: int):
    try:
        from confluent_kafka import Consumer as KafkaConsumer

        col = config.attribute_column
        props = normalized_proportions(config.proportions, config.fairness)
        floor, ceiling = bounds_from_proportions(props, config.block_size)
        unique_vals = sorted(props)
        position = {v: i for i, v in enumerate(unique_vals)}

        kafka_conf = {
            "bootstrap.servers": broker(),
            "group.id": f"ui-fairness-{col}-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
        consumer = KafkaConsumer(kafka_conf)
        from confluent_kafka import TopicPartition, OFFSET_BEGINNING
        tp = TopicPartition(config.topic_name, 0, OFFSET_BEGINNING)
        consumer.assign([tp])

        message_buffer: list = []
        sketch: list = []

        sketching_sum = 0.0
        processing_sum = 0.0
        count = 0
        window_counter = 0
        fair_blocks_ini = 0
        fair_blocks_reordered = 0
        total_blocks = 0
        process_latency: list = []
        sketch_bld_latency: list = []
        sketch_upd_latency: list = []

        while generation == _consumer_generation and not _stop_event.is_set():
            # Wait for the viewer to catch up rather than racing ahead of them.
            while (window_counter - _client_position >= LEAD_WINDOWS
                   and generation == _consumer_generation
                   and not _stop_event.is_set()):
                time.sleep(0.05)
            if generation != _consumer_generation or _stop_event.is_set():
                break

            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                _metrics_queue.put({"type": "error", "message": str(msg.error())})
                break

            row = json.loads(msg.value().decode())
            message_buffer.append(row)

            if len(message_buffer) > config.window_size:
                message_buffer.pop(0)
            if len(message_buffer) < config.window_size:
                continue

            window_counter += 1
            count += 1
            read_window = pd.DataFrame(message_buffer)
            read_window[col] = read_window[col].astype(str)

            tracemalloc.start()
            t1 = time.perf_counter()
            tracemalloc.reset_peak()

            if len(sketch) == 0:
                popped = sketcher(read_window[col], sketch, position)
                t2 = time.perf_counter()
                sketch_bld_latency.append((t2 - t1) * 1000)
            else:
                popped = sketcher(read_window[col].iloc[-1:], sketch, position)
                t2 = time.perf_counter()
                sketch_upd_latency.append((t2 - t1) * 1000)

            sketching_ms = (t2 - t1) * 1000

            t3 = time.perf_counter()
            tracemalloc.reset_peak()
            query_result, fair_block = verify_sketch(
                sketch, position, config.block_size, floor, ceiling, popped
            )
            t4 = time.perf_counter()
            tracemalloc.stop()

            processing_ms = (t4 - t3) * 1000
            process_latency.append(processing_ms)

            sum_blocks = config.window_size // config.block_size
            total_blocks += sum_blocks
            fair_blocks_ini += fair_block
            sketching_sum += sketching_ms
            processing_sum += processing_ms

            is_fair = bool(query_result and "✅" in query_result[0])

            window_items = [
                {"value": str(row.get(col, "")), **{k: str(v) if v is not None else "" for k, v in row.items()}}
                for row in message_buffer
            ]

            # bfair reorder — landmark look-ahead when window is unfair
            reordered_items = window_items
            reordered_rows = list(message_buffer)
            # Reorder within this window only. Look-ahead belongs to the
            # explicit /api/reorder call: polling landmark messages here would
            # consume rows that are owed to later windows (the window would hop
            # instead of slide) and would let items from the future appear in
            # this window's reordered view.
            try:
                reordered_rows = _bfair_reorder(
                    list(message_buffer),
                    props,
                    config.block_size,
                    attr_fn=lambda r: str(r.get(col, "")),
                )
                reordered_items = [
                    {"value": str(r.get(col, "")), **{k: str(v) if v is not None else "" for k, v in r.items()}}
                    for r in reordered_rows
                ]
            except Exception as e:
                print(f"[bfair] {e}", flush=True)

            fair_block_reordered = count_fair_blocks(
                reordered_rows, col, floor, ceiling, config.block_size
            )
            fair_blocks_reordered += fair_block_reordered

            metrics = {
                "Window size": config.window_size,
                "Block size": config.block_size,
                "Avg preprocessing (ms)": round(sketching_sum / count, 4),
                "Avg query processing (ms)": round(processing_sum / count, 4),
                "Windows covered": window_counter,
                "Fair blocks": fair_blocks_ini,
                "Fair blocks (reordered)": fair_blocks_reordered,
                "Total blocks": total_blocks,
                "Fair block %": round(fair_blocks_ini * 100 / total_blocks, 2) if total_blocks else 0,
                "Fair block % (reordered)": round(fair_blocks_reordered * 100 / total_blocks, 2) if total_blocks else 0,
            }

            _metrics_queue.put({
                "type": "window_update",
                # so a client can tell this run's updates from a previous run's
                # stragglers that were already in flight when it restarted
                "run_id": generation,
                "window_number": window_counter,
                "is_fair": is_fair,
                "fair_text": query_result[0] if query_result else "",
                "preprocessing_ms": round(sketching_ms, 4),
                "query_ms": round(processing_ms, 4),
                "metrics": metrics,
                "window_items": window_items,
                "reordered_items": reordered_items,
                "block_size": config.block_size,
                "attribute": col,
                "fair_blocks_before": fair_block,
                "fair_blocks_after": fair_block_reordered,
                "blocks_per_window": sum_blocks,
                "reorder_feasible": fair_block_reordered >= sum_blocks,
            })

            # Always yield, even with no configured delay. This thread does
            # sketching and reordering per window with no natural pause, so at
            # delay_ms=0 it holds the GIL continuously and the event loop
            # cannot answer requests, which made the API look hung while a
            # stream was live. 10ms caps it near 100 windows/sec, which still reads
            # as live but leaves the loop room to serve requests.
            time.sleep(max(config.delay_ms / 1000.0, 0.01))

            if window_counter >= config.max_windows:
                break

        summary: dict = {}
        if process_latency:
            summary["Processing tail latency (p90 ms)"] = round(float(np.percentile(process_latency, 90)), 4)
        if sketch_bld_latency:
            summary["Sketch build latency (p90 ms)"] = round(float(np.percentile(sketch_bld_latency, 90)), 4)
        if sketch_upd_latency:
            summary["Sketch update latency (p90 ms)"] = round(float(np.percentile(sketch_upd_latency, 90)), 4)

        _metrics_queue.put({"type": "done", "summary": summary})
        consumer.close()

    except Exception as exc:
        _metrics_queue.put({"type": "error", "message": str(exc)})


# ── REST endpoints ────────────────────────────────────────────────────────────

_datasets_cache: dict = {}


def _build_datasets() -> list:
    result = []
    for name, cfg in all_datasets().items():
        csv_path = resolve_csv(cfg)
        try:
            df = pd.read_csv(csv_path)
            df = preprocess_for(name, cfg, df)

            attrs = []
            for attr in cfg["attributes"]:
                col = attr["column"]
                if col in df.columns:
                    unique_vals = sorted(df[col].dropna().unique().tolist())
                    shares = df[col].dropna().astype(str).value_counts(normalize=True)
                    attrs.append({
                        "label": attr["label"],
                        "column": col,
                        "unique_values": [str(v) for v in unique_vals],
                        "suggested_constraints": {
                            str(v): round(float(shares.get(str(v), 0.0)) * 100, 1)
                            for v in unique_vals
                        },
                    })

            result.append({"name": name, "topic_base": cfg["topic_base"], "attributes": attrs})
        except Exception as e:
            result.append({"name": name, "topic_base": cfg["topic_base"], "attributes": [], "error": str(e)})

    return result


@app.get("/api/datasets")
async def get_datasets():
    """Reads and preprocesses every CSV, which is tens of MB of pandas work.
    Doing that inline on each call blocked the event loop for seconds at a
    time, stalling every other request behind it. Built once off the loop and
    reused; the mtimes of the files key the cache so a swapped CSV is picked up.
    """
    key = tuple(
        (name, os.path.getmtime(p) if os.path.exists(p) else 0)
        for name, cfg in sorted(all_datasets().items())
        for p in [resolve_csv(cfg)]
    )
    if _datasets_cache.get("key") != key:
        _datasets_cache["value"] = await asyncio.to_thread(_build_datasets)
        _datasets_cache["key"] = key
    return _datasets_cache["value"]


class AblationRequest(BaseModel):
    stream_items: List[dict]
    window_size: int = Field(ge=1, le=MAX_WINDOW_SIZE)
    block_size: int = Field(ge=1, le=MAX_WINDOW_SIZE)
    proportions: Dict[str, float]
    attribute_column: str = "GENDER"
    x_max: int = Field(default=50, ge=1, le=MAX_LANDMARK_SIZE)
    # One pass. Averaging repeats matters for the paper's experiments, not for
    # a live session the user is waiting on.
    runs: int = Field(default=1, ge=1, le=50)


def _run_ablation(req: "AblationRequest") -> dict:
    """Sweep landmark 1..x_max over the pre-reorder stream and report, for each,
    the share of blocks that come out fair and how long the reorder took.

    simulate_bfair_x_ablation.py measures the same two quantities but drives a
    real Kafka round trip at window_size=500 over 5000 windows per value, which
    is ~2 minutes per landmark. Sweeping 50 that way is over an hour, far too
    slow to run while someone waits. The reorder itself is the part being
    measured, so it is timed directly here instead.

    Runs off the event loop: this is seconds of straight CPU, and doing it in
    the request coroutine froze every other endpoint and the websocket with it.
    """
    col = req.attribute_column
    try:
        rows = req.stream_items
        props = normalized_proportions(
            with_observed_groups(req.proportions, rows, col), {}
        )
        floor, ceiling = bounds_from_proportions(props, req.block_size)
        attr = lambda r: str(r.get(col, ""))

        # Every window of the stream and every landmark in the range, the way
        # simulate_bfair_x_ablation.py sweeps. Sampling windows was faster but
        # meant the fairness and latency figures described a subset of the
        # stream rather than the stream.
        starts = list(range(0, max(1, len(rows) - req.window_size + 1)))
        x_values = list(range(1, req.x_max + 1))

        points = []
        for x in x_values:
            fair = blocks = 0
            elapsed = 0.0
            timed = 0
            for s in starts:
                window = rows[s:s + req.window_size]
                if len(window) < req.window_size:
                    continue
                combined = rows[s:s + req.window_size + x]
                # Average repeated passes like the script's --runs. A single
                # pass at these sub-millisecond durations is mostly timer
                # noise, and which landmarks reached the pareto front changed
                # from one sweep to the next because of it.
                out = None
                for _ in range(req.runs):
                    t0 = time.perf_counter()
                    out = _bfair_reorder(combined, props, req.block_size, attr_fn=attr)
                    elapsed += (time.perf_counter() - t0) * 1000
                    timed += 1
                shown = out[:req.window_size]
                fair += count_fair_blocks(shown, col, floor, ceiling, req.block_size)
                blocks += req.window_size // req.block_size
            if not blocks:
                continue
            points.append({
                "landmark": x,
                "pct_fair": round(fair * 100 / blocks, 2),
                "latency_ms": round(elapsed / max(1, timed), 3),
            })

        # a point is on the front when nothing else is both fairer and faster
        pareto = [
            p for p in points
            if not any(q["pct_fair"] >= p["pct_fair"] and q["latency_ms"] < p["latency_ms"]
                       or q["pct_fair"] > p["pct_fair"] and q["latency_ms"] <= p["latency_ms"]
                       for q in points)
        ]
        pareto.sort(key=lambda p: p["latency_ms"])

        # Landmarks that score identically are not distinct choices, so keep the
        # smallest of each tie. Without this a short stream, where every
        # landmark performs the same, produced a slider of interchangeable
        # points that looked like a tradeoff and was not one.
        deduped, seen = [], set()
        for p in sorted(pareto, key=lambda p: p["landmark"]):
            k = (p["pct_fair"], p["latency_ms"])
            if k not in seen:
                seen.add(k)
                deduped.append(p)
        deduped.sort(key=lambda p: p["latency_ms"])

        return {"status": "ok", "points": points, "pareto": deduped,
                "windows_evaluated": len(starts)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.post("/api/ablation")
async def landmark_ablation(req: AblationRequest):
    return await asyncio.to_thread(_run_ablation, req)


class ReorderRequest(BaseModel):
    window_items: List[dict]
    window_size: int
    block_size: int
    proportions: Dict[str, float]
    attribute_column: str = "GENDER"


@app.post("/api/reorder")
async def reorder_window(req: ReorderRequest):
    col = req.attribute_column
    try:
        props = normalized_proportions(
            with_observed_groups(req.proportions, req.window_items, col), {}
        )
        floor, ceiling = bounds_from_proportions(props, req.block_size)
        blocks_per_window = req.window_size // req.block_size

        # Count both sides over the window alone. window_items carries the
        # landmark look-ahead too, so counting all of it made "before" span
        # window+landmark blocks while "after" spanned the window's, which with
        # a landmark of 50 reported 5 fair blocks before against 2 after and
        # read as a reorder that made things worse.
        before = count_fair_blocks(
            req.window_items[:req.window_size], col, floor, ceiling, req.block_size
        )
        reordered = _bfair_reorder(
            req.window_items,
            props,
            req.block_size,
            attr_fn=lambda r: str(r.get(col, "")),
        )
        after = count_fair_blocks(reordered[:req.window_size], col, floor, ceiling, req.block_size)

        return {
            "status": "ok",
            "reordered_items": reordered,
            "window_size": req.window_size,
            "fair_blocks_before": before,
            "fair_blocks_after": after,
            "blocks_per_window": blocks_per_window,
            "reorder_feasible": after >= blocks_per_window,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.post("/api/start")
async def start_consumer(config: ConsumerConfig):
    global _is_running
    # A stream no longer ends after a fixed window count, so an earlier one is
    # usually still running. Refusing here meant the caller silently kept
    # watching the previous session, which is why a fresh run could open on
    # window 509. Retire the old consumer and start clean instead.
    global _consumer_generation, _client_position
    _consumer_generation += 1
    generation = _consumer_generation
    _client_position = 0
    _stop_event.set()
    await asyncio.sleep(0.6)          # let any previous consumer notice and exit
    _stop_event.clear()
    while not _metrics_queue.empty():
        try:
            _metrics_queue.get_nowait()
        except queue.Empty:
            break

    _is_running = True
    t = threading.Thread(target=_run_consumer, args=(config, generation), daemon=True)
    t.start()
    return {"status": "started", "run_id": generation}


@app.post("/api/position")
async def set_position(payload: dict):
    """Tell the consumer which window the viewer is on so it can pace itself."""
    global _client_position
    try:
        _client_position = max(0, int(payload.get("window_number", 0)))
    except (TypeError, ValueError):
        pass
    return {"status": "ok", "position": _client_position}


@app.post("/api/stop")
async def stop_consumer():
    global _is_running, _consumer_generation
    _consumer_generation += 1        # stale generation stops the loop for good
    _stop_event.set()
    _is_running = False
    return {"status": "stopped"}


@app.get("/api/status")
async def get_status():
    return {"running": _is_running, "metrics": _current_metrics}


# ── Producer thread + endpoint ────────────────────────────────────────────────
def _run_producer(dataset_name: str, topic_name: str, generation: int, loop: asyncio.AbstractEventLoop):
    global _is_producing
    try:
        from confluent_kafka import Producer

        cfg = all_datasets().get(dataset_name)
        if not cfg:
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "produce_error", "message": f"Unknown dataset: {dataset_name}"}),
                loop,
            ).result()
            return

        csv_path = resolve_csv(cfg)
        df = pd.read_csv(csv_path)

        df = preprocess_for(dataset_name, cfg, df)

        # Hospital and Stocks carry real timestamps, so they stream in the
        # order the events happened. The others only have a date invented when
        # the archive was assembled, so their file order says nothing about
        # arrival; shuffling makes records arrive i.i.d. from the global
        # distribution, which is the distribution the constraints come from.
        date_col = cfg.get("date_column") if isinstance(cfg, dict) else None
        if date_col and date_col in df.columns:
            order = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
            df = df.assign(_order=order).sort_values(
                "_order", kind="mergesort", na_position="last"
            ).drop(columns=["_order"]).reset_index(drop=True)
        else:
            df = df.sample(frac=1, random_state=0).reset_index(drop=True)

        total = len(df)

        conf = {
            "bootstrap.servers": broker(),
            "client.id": socket.gethostname(),
            # the defaults fill long before a 100k+ row dataset is through
            "queue.buffering.max.messages": 1_000_000,
            "queue.buffering.max.kbytes": 1_048_576,
            "linger.ms": 20,
        }
        producer = Producer(conf)

        announced = False
        # Keep cycling through the dataset so the consumer never runs out of
        # data; a newer /api/produce call bumps _producer_generation, which
        # ends this loop.
        while generation == _producer_generation:
            for _, row in df.iterrows():
                if generation != _producer_generation:
                    break
                payload = row.to_json().encode()
                # produce() raises BufferError once the local queue is full,
                # which aborted the whole run ("Local: Queue full"). Give
                # delivery a chance to drain and try the same row again.
                while generation == _producer_generation:
                    try:
                        producer.produce(topic=topic_name, key=b"stream", value=payload)
                        break
                    except BufferError:
                        producer.poll(0.5)
                producer.poll(0)
            producer.flush()
            if not announced:
                asyncio.run_coroutine_threadsafe(
                    _broadcast({"type": "produce_done", "published": total, "topic": topic_name}),
                    loop,
                ).result()
                announced = True
                if generation == _producer_generation:
                    _is_producing = False
    except Exception as exc:
        asyncio.run_coroutine_threadsafe(
            _broadcast({"type": "produce_error", "message": str(exc)}),
            loop,
        ).result()
        if generation == _producer_generation:
            _is_producing = False


def _delete_kafka_topic(topic_name: str):
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": broker()})
        fs = admin.delete_topics([topic_name], operation_timeout=5)
        for t, f in fs.items():
            try:
                f.result()
            except Exception:
                pass
        time.sleep(1.5)
    except Exception:
        pass


@app.post("/api/produce")
async def produce_data(config: ProduceConfig):
    global _is_producing, _producer_generation
    if _is_producing:
        return {"status": "already_producing"}

    cfg = all_datasets().get(config.dataset_name)
    if not cfg:
        return {"status": "error", "message": f"Unknown dataset: {config.dataset_name}"}

    _topic_counters[config.dataset_name] = _topic_counters.get(config.dataset_name, 0) + 1
    topic_name = f"{cfg['topic_base']}{_topic_counters[config.dataset_name]}"
    _delete_kafka_topic(topic_name)

    _producer_generation += 1
    generation = _producer_generation
    _is_producing = True
    loop = asyncio.get_event_loop()
    t = threading.Thread(
        target=_run_producer, args=(config.dataset_name, topic_name, generation, loop), daemon=True
    )
    t.start()
    return {"status": "started", "topic": topic_name}


@app.get("/api/produce/status")
async def produce_status():
    return {"producing": _is_producing}


# ── WebSocket endpoint ────────────────────────────────────────────────────────
@app.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket):
    await websocket.accept()
    _active_ws.append(websocket)
    if _current_metrics:
        await websocket.send_json({"type": "current_metrics", "metrics": _current_metrics})
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=20.0)
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in _active_ws:
            _active_ws.remove(websocket)
