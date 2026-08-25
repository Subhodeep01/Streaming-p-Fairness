"""
Quick end-to-end WebSocket test.
1. Connects to ws://localhost:8000/ws/metrics
2. POSTs /api/produce (writes rows to Kafka)
3. Waits 2s, then POSTs /api/start with delay_ms=200
4. Receives up to 5 window_update messages and prints them
5. POSTs /api/stop
"""
import asyncio
import json
import time
import urllib.request

BASE = "http://localhost:8000"

DATASET = "Movies"
ATTRIBUTE = "release_era"
# Equal targets across all five eras -- deliberately not what the data looks
# like (it is ~83% New Hollywood Era), so reorder_feasible should report False.
ERAS = ["Golden Age Classic", "Modern Blockbuster Age", "New Hollywood Era",
        "Recent Release", "Turn-of-Millennium"]
PROPORTIONS = {e: 0.2 for e in ERAS}
FAIRNESS = {e: 1 for e in ERAS}


def post(path, body=None):
    data = json.dumps(body).encode() if body else b""
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


async def main():
    import websockets  # pip install websockets

    print("Connecting WebSocket…")
    async with websockets.connect("ws://localhost:8000/ws/metrics") as ws:
        print("Connected.")

        print("Producing data…")
        r = post("/api/produce", {"dataset_name": DATASET})
        print("Produce response:", r)
        topic = r["topic"]

        print("Waiting 3 s for producer to finish…")
        await asyncio.sleep(3)

        print(f"Starting consumer on '{topic}' with delay_ms=200…")
        r = post("/api/start", {
            "topic_name": topic,
            "window_size": 20,
            "block_size": 5,
            "max_windows": 10,
            "fairness": FAIRNESS,
            "proportions": PROPORTIONS,
            "attribute_column": ATTRIBUTE,
            "delay_ms": 200,
        })
        print("Start response:", r)

        received = 0
        async for raw in ws:
            msg = json.loads(raw)
            t = msg.get("type")
            if t == "window_update":
                received += 1
                print(f"[W{msg['window_number']}] fair={msg['is_fair']}  "
                      f"blocks={msg['fair_blocks_before']}->{msg['fair_blocks_after']}"
                      f"/{msg['blocks_per_window']}  "
                      f"feasible={msg['reorder_feasible']}  "
                      f"pre={msg['preprocessing_ms']}ms  "
                      f"q={msg['query_ms']}ms")
                if received >= 5:
                    print("Got 5 windows, stopping.")
                    post("/api/stop", {})
                    break
            elif t in ("done", "error"):
                print(f"[{t}]", msg)
                break

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
