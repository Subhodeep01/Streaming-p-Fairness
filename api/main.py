"""
FastAPI server bridging the Kafka p-Fairness consumer to the React UI via WebSockets.

Run from the Streaming-p-Fairness root:
    uvicorn api.main:app --reload --port 8000
"""

import asyncio
import json
import os
import queue
import sys
import threading
import time
import tracemalloc
import socket
from typing import Dict, List

import numpy as np
import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
from utils import sketcher, verify_sketch

app = FastAPI(title="Streaming p-Fairness API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Dataset configs ───────────────────────────────────────────────────────────

DATASET_CONFIGS = {
    "Hospital Admissions Data": {
        "csv": "datasets/HDHI_Admission_data.csv",
        "topic": "hospital-stream",
        "attributes": [
            {"label": "Gender", "column": "GENDER"},
            {"label": "Hospitalization Outcome", "column": "OUTCOME"},
            {"label": "Primary Diagnosis", "column": "PRIMARY_DIAGNOSIS"},
        ],
    },
    "Stocks": {
        "csv": "datasets/AAPL_pct_change_binned.csv",
        "topic": "stock-stream",
        "attributes": [
            {"label": "Price Change", "column": "PRICE_CHANGE_BIN"},
            {"label": "Volume", "column": "VOLUME_BIN"},
        ],
    },
}


def _preprocess_hospital(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["GENDER"] = df["GENDER"].astype(str).str.strip()
    outcome_map = {"DISCHARGE": "discharged", "EXPIRY": "expired", "DAMA": "dama"}
    out["OUTCOME"] = df["OUTCOME"].astype(str).str.strip().map(outcome_map).fillna("discharged")

    def get_diagnosis(row):
        if row.get("ACS", 0) == 1:
            return "acs"
        if row.get("HEART FAILURE", 0) == 1:
            return "heart-failure"
        if row.get("ANAEMIA", 0) == 1 or row.get("SEVERE ANAEMIA", 0) == 1:
            return "anaemia"
        return "acs"

    out["PRIMARY_DIAGNOSIS"] = df.apply(get_diagnosis, axis=1)
    return out


def _preprocess_stocks(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["PRICE_CHANGE_BIN"] = df["bins"].astype(str)
    volume_bins = pd.qcut(df["Volume"], q=3, labels=["low", "medium", "high"], duplicates="drop")
    out["VOLUME_BIN"] = volume_bins.astype(str)
    return out


DATASET_PREPROCESSORS = {
    "Hospital Admissions Data": _preprocess_hospital,
    "Stocks": _preprocess_stocks,
}

# ── Shared state ──────────────────────────────────────────────────────────────
_active_ws: List[WebSocket] = []
_metrics_queue: queue.Queue = queue.Queue()
_stop_event = threading.Event()
_is_running = False
_is_producing = False
_current_metrics: dict = {}


class ConsumerConfig(BaseModel):
    topic_name: str
    window_size: int
    block_size: int
    fairness: Dict[str, int]
    attribute_column: str = "GENDER"
    max_windows: int = 50
    delay_ms: int = 0


class ProduceConfig(BaseModel):
    dataset_name: str


# ── Broadcast helpers ─────────────────────────────────────────────────────────
async def _broadcast(msg: dict):
    dead = []
    for ws in _active_ws:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
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
def _run_consumer(config: ConsumerConfig):
    try:
        from confluent_kafka import Consumer as KafkaConsumer

        col = config.attribute_column
        unique_vals = sorted(config.fairness.keys())
        position = {v: i for i, v in enumerate(unique_vals)}

        typed_fairness: dict = {}
        sample = unique_vals[0] if unique_vals else "F"
        for k, v in config.fairness.items():
            try:
                if isinstance(sample, (int, np.integer)):
                    typed_fairness[int(k)] = int(v)
                elif isinstance(sample, (float, np.floating)):
                    typed_fairness[float(k)] = int(v)
                else:
                    typed_fairness[k] = int(v)
            except (ValueError, TypeError):
                typed_fairness[k] = int(v)

        kafka_conf = {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"ui-fairness-{col}",
            "auto.offset.reset": "earliest",
        }
        consumer = KafkaConsumer(kafka_conf)
        consumer.subscribe([config.topic_name])

        message_buffer: list = []
        sketch: list = []

        sketching_sum = 0.0
        processing_sum = 0.0
        count = 0
        window_counter = 0
        fair_blocks_ini = 0
        total_blocks = 0
        process_latency: list = []
        sketch_bld_latency: list = []
        sketch_upd_latency: list = []

        while not _stop_event.is_set():
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                _metrics_queue.put({"type": "error", "message": str(msg.error())})
                break

            row = json.loads(msg.value().decode())
            attr_value = str(row.get(col, ""))
            message_buffer.append({col: attr_value})

            if len(message_buffer) > config.window_size:
                message_buffer.pop(0)
            if len(message_buffer) < config.window_size:
                continue

            window_counter += 1
            count += 1
            read_window = pd.DataFrame(message_buffer)

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
                sketch, position, config.block_size, typed_fairness, popped
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

            metrics = {
                "Window size": config.window_size,
                "Block size": config.block_size,
                "Avg preprocessing (ms)": round(sketching_sum / count, 4),
                "Avg query processing (ms)": round(processing_sum / count, 4),
                "Windows covered": window_counter,
                "Fair blocks": fair_blocks_ini,
                "Total blocks": total_blocks,
                "Fair block %": round(fair_blocks_ini * 100 / total_blocks, 2) if total_blocks else 0,
            }

            is_fair = bool(query_result and "✅" in query_result[0])

            window_items = [row[col] for row in message_buffer]

            _metrics_queue.put({
                "type": "window_update",
                "window_number": window_counter,
                "is_fair": is_fair,
                "fair_text": query_result[0] if query_result else "",
                "preprocessing_ms": round(sketching_ms, 4),
                "query_ms": round(processing_ms, 4),
                "metrics": metrics,
                "window_items": window_items,
                "block_size": config.block_size,
                "attribute": col,
            })

            if config.delay_ms > 0:
                time.sleep(config.delay_ms / 1000.0)

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
@app.get("/api/datasets")
async def get_datasets():
    result = []
    for name, cfg in DATASET_CONFIGS.items():
        csv_path = os.path.join(_ROOT, cfg["csv"])
        try:
            df = pd.read_csv(csv_path)
            preprocessor = DATASET_PREPROCESSORS.get(name)
            if preprocessor:
                df = preprocessor(df)

            attrs = []
            for attr in cfg["attributes"]:
                col = attr["column"]
                if col in df.columns:
                    unique_vals = sorted(df[col].dropna().unique().tolist())
                    attrs.append({
                        "label": attr["label"],
                        "column": col,
                        "unique_values": [str(v) for v in unique_vals],
                    })

            result.append({"name": name, "topic": cfg["topic"], "attributes": attrs})
        except Exception as e:
            result.append({"name": name, "topic": cfg["topic"], "attributes": [], "error": str(e)})

    return result


@app.post("/api/start")
async def start_consumer(config: ConsumerConfig):
    global _is_running
    if _is_running:
        return {"status": "already_running"}

    _stop_event.clear()
    while not _metrics_queue.empty():
        try:
            _metrics_queue.get_nowait()
        except queue.Empty:
            break

    _is_running = True
    t = threading.Thread(target=_run_consumer, args=(config,), daemon=True)
    t.start()
    return {"status": "started"}


@app.post("/api/stop")
async def stop_consumer():
    global _is_running
    _stop_event.set()
    _is_running = False
    return {"status": "stopped"}


@app.get("/api/status")
async def get_status():
    return {"running": _is_running, "metrics": _current_metrics}


# ── Producer thread + endpoint ────────────────────────────────────────────────
def _run_producer(dataset_name: str, loop: asyncio.AbstractEventLoop):
    global _is_producing
    try:
        from confluent_kafka import Producer

        cfg = DATASET_CONFIGS.get(dataset_name)
        if not cfg:
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "produce_error", "message": f"Unknown dataset: {dataset_name}"}),
                loop,
            ).result()
            return

        csv_path = os.path.join(_ROOT, cfg["csv"])
        df = pd.read_csv(csv_path)

        preprocessor = DATASET_PREPROCESSORS.get(dataset_name)
        if preprocessor:
            df = preprocessor(df)

        topic_name = cfg["topic"]
        total = len(df)

        conf = {"bootstrap.servers": "localhost:9092", "client.id": socket.gethostname()}
        producer = Producer(conf)

        for _, row in df.iterrows():
            producer.produce(
                topic=topic_name,
                key=b"stream",
                value=row.to_json().encode(),
            )
            producer.poll(0)

        producer.flush()
        asyncio.run_coroutine_threadsafe(
            _broadcast({"type": "produce_done", "published": total, "topic": topic_name}),
            loop,
        ).result()
    except Exception as exc:
        asyncio.run_coroutine_threadsafe(
            _broadcast({"type": "produce_error", "message": str(exc)}),
            loop,
        ).result()
    finally:
        _is_producing = False


@app.post("/api/produce")
async def produce_data(config: ProduceConfig):
    global _is_producing
    if _is_producing:
        return {"status": "already_producing"}
    _is_producing = True
    loop = asyncio.get_event_loop()
    t = threading.Thread(target=_run_producer, args=(config.dataset_name, loop), daemon=True)
    t.start()
    return {"status": "started"}


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
