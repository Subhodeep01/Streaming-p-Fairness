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
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Resolve project root so utils can be imported regardless of cwd
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

# ── Shared state ──────────────────────────────────────────────────────────────
_active_ws: List[WebSocket] = []
_metrics_queue: queue.Queue = queue.Queue()
_stop_event = threading.Event()
_is_running = False
_current_metrics: dict = {}


class ConsumerConfig(BaseModel):
    topic_name: str
    window_size: int
    block_size: int
    fairness: Dict[str, int]   # JSON keys are strings: {"0": 3, "1": 2, ...}
    max_windows: int = 200


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


async def _drain_queue():
    """Async task: pull updates from the consumer thread and push to all WebSocket clients."""
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
            break


# ── Consumer thread (blocking) ────────────────────────────────────────────────
def _run_consumer(config: ConsumerConfig):
    """Runs entirely in a daemon thread; communicates back via _metrics_queue."""
    try:
        from confluent_kafka import Consumer as KafkaConsumer

        csv_path = os.path.join(_ROOT, "cleaned_input_files", "cleaned_df.csv")
        df_temp = pd.read_csv(csv_path)
        col = df_temp.columns[0]
        unique_vals = sorted(df_temp[col].unique().tolist())
        position = {v: i for i, v in enumerate(unique_vals)}

        # Cast fairness keys to match actual data types (JSON keys are always strings)
        sample = unique_vals[0]
        typed_fairness: dict = {}
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
            "group.id": "ui-fairness-consumer",
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

            value = json.loads(msg.value().decode())
            message_buffer.append(value)
            if len(message_buffer) > config.window_size:
                message_buffer.pop(0)
            if len(message_buffer) < config.window_size:
                continue

            window_counter += 1
            count += 1
            read_window = pd.DataFrame(message_buffer)
            attr = read_window.columns[0]

            # ── Sketch update ──────────────────────────────────────────────
            tracemalloc.start()
            t1 = time.perf_counter()
            tracemalloc.reset_peak()

            if len(sketch) == 0:
                popped = sketcher(read_window[attr], sketch, position)
                t2 = time.perf_counter()
                sketch_bld_latency.append((t2 - t1) * 1000)
            else:
                popped = sketcher(read_window[attr][-1:], sketch, position)
                t2 = time.perf_counter()
                sketch_upd_latency.append((t2 - t1) * 1000)

            sketching_ms = (t2 - t1) * 1000

            # ── p-Fairness query ───────────────────────────────────────────
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

            _metrics_queue.put({
                "type": "window_update",
                "window_number": window_counter,
                "is_fair": is_fair,
                "fair_text": query_result[0] if query_result else "",
                "preprocessing_ms": round(sketching_ms, 4),
                "query_ms": round(processing_ms, 4),
                "metrics": metrics,
            })

            if window_counter >= config.max_windows:
                break

        summary: dict = {}
        if process_latency:
            summary["Processing tail latency (p90 ms)"] = round(
                float(np.percentile(process_latency, 90)), 4
            )
        if sketch_bld_latency:
            summary["Sketch build latency (p90 ms)"] = round(
                float(np.percentile(sketch_bld_latency, 90)), 4
            )
        if sketch_upd_latency:
            summary["Sketch update latency (p90 ms)"] = round(
                float(np.percentile(sketch_upd_latency, 90)), 4
            )

        _metrics_queue.put({"type": "done", "summary": summary})
        consumer.close()

    except Exception as exc:
        _metrics_queue.put({"type": "error", "message": str(exc)})


# ── REST endpoints ────────────────────────────────────────────────────────────
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
    asyncio.create_task(_drain_queue())
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


@app.get("/api/attributes")
async def get_attributes():
    try:
        df = pd.read_csv(os.path.join(_ROOT, "cleaned_input_files", "cleaned_df.csv"))
        col = df.columns[0]
        unique_vals = sorted(df[col].unique().tolist())
        try:
            summary = pd.read_csv(
                os.path.join(_ROOT, "cleaned_input_files", f"summary_{col}.csv")
            ).to_dict(orient="records")
        except FileNotFoundError:
            summary = []
        return {
            "column": col,
            "unique_values": [str(v) for v in unique_vals],
            "summary": summary,
        }
    except FileNotFoundError:
        return {"error": "Run user_inputs.py first.", "column": "", "unique_values": [], "summary": []}


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
