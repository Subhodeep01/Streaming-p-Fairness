import os
import json
import time
import pandas as pd
from confluent_kafka import Consumer
from utils import sketcher, verify_sketch, position_finder
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consumer args")
    parser.add_argument("--window_size", type=int, required=True)
    parser.add_argument("--block_size", type=int, required=True)
    parser.add_argument("--topic_name", type=str, required=True)
    parser.add_argument("--max_windows", type=int, required=True)
    args = parser.parse_args()
    

    # ─── Config ─────────────────────────────────────────────────────
    IN_TOPIC = args.topic_name
    WINDOW_SIZE = args.window_size
    GROUP_ID = "hospital-sliding-consumer"
    BLOCK_SIZE = args.block_size
    MAX_WINDOWS = args.max_windows

    # ─── Kafka Consumer Setup ────────────────────────────────────────
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([IN_TOPIC])

    # ─── Global State ────────────────────────────────────────────────
    message_buffer = []
    sketch = []
    fairness = {}
    read_window = None
    metrics = {"Window size":WINDOW_SIZE, "Block size":BLOCK_SIZE}
    df_temp = pd.read_csv("cleaned_input_files/cleaned_df.csv")
    try:
        print(pd.read_csv(f"cleaned_input_files/summary_{df_temp.columns[0]}.csv"))
    except:
        print("Discrete value, no binning info")
    position, fairness = position_finder(df_temp, fairness, BLOCK_SIZE)

    window_counter = 0
    sketching_sum = 0
    processing_sum = 0
    total_sum = 0
    count = 0
    throughput = 0
    its = 0
    fair_block_sum = 0


    def process_window(batch):
        global sketch, fairness, read_window, window_counter, metrics, position, fairness
        global sketching_sum, processing_sum, total_sum, count, fair_block_sum
        
        count += 1

        read_window = pd.DataFrame(batch)
        # sketch = []
        
        attr = read_window.columns[0]
        # print(read_window[attr].to_list())
        effective_window = read_window[attr][-1:]
        t1 = time.perf_counter()
        if len(sketch) == 0:
            effective_window = read_window[attr]
            popped = sketcher(effective_window, sketch, position)
        else:
            popped = sketcher(effective_window, sketch, position)
        t2 = time.perf_counter()
        query_result, fair_block = verify_sketch(sketch, position, BLOCK_SIZE, fairness, popped)
        t3 = time.perf_counter()

        # print(sketch)
        # window_update_ms = 0 if window_counter == 1 else (t1 - t0) * 1000
        sketching_ms = (t2 - t1) * 1000
        processing_ms = (t3 - t2) * 1000
        total_querying_ms = (t3 - t1) * 1000

        # window_sum += window_update_ms
        sketching_sum += sketching_ms
        processing_sum += processing_ms
        total_sum += total_querying_ms
        fair_block_sum += fair_block
        avg_sketching = sketching_sum / count
        avg_processing = processing_sum / count
        
        # print(f"✔ Processed sliding window {count}")
        # print(f" Avg preprocessing: {avg_sketching:.3f} ms")
        print(f" Total querying time: {total_querying_ms:.3f} ms")
        # print(f" Output: {query_result}")

        read_window.drop(index=read_window.index[0], axis=0, inplace=True)
        
        metrics["Cardinality"]=len(position.keys())
        metrics["Avg preprocessing"]=avg_sketching
        metrics["Avg query processing"]=avg_processing
        metrics["Total Fair Blocks"] = fair_block_sum
        if count == 1:
            metrics["Preprocessing: Sketch Building time"] = sketching_ms
            
        return query_result, metrics


    # ─── Main Loop ───────────────────────────────────────────────────
    print(f"Listening to '{IN_TOPIC}' from broker '{'localhost:9092'}' …")

    try:
        while True:        
            msg = consumer.poll(1.0)  # timeout = 1 sec

            if msg is None:
                print("No msg received")
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue
            # Deserialize JSON
            value = json.loads(msg.value().decode('utf-8'))
            message_buffer.append(value)
            
            # Keep only latest WINDOW_SIZE messages (sliding behavior)
            if len(message_buffer) > WINDOW_SIZE:
                message_buffer.pop(0)

            if len(message_buffer) == WINDOW_SIZE:
                window_counter += 1
                
                # Process the current sliding window (every new message triggers this)
                query_result, metric = process_window(message_buffer.copy())
                # print("Query Result: ", query_result)
            
            print(total_sum)
            if total_sum >= 1000:
                throughput += window_counter
                its += 1
                window_counter = 1
                total_sum = 0
                print("Throughput = ", throughput//its)
                metric["Throughput"] = throughput//its
                
            if its == 1:
                break
            # if window_counter == MAX_WINDOWS:
            #     # print(total_sum)
            #     break
        
        

    except KeyboardInterrupt:
        print("Stopping consumer…")
    finally:
        metric = pd.DataFrame(metric, index = [0])
        print("Metrics: ", metric)
        metric.to_csv(f"metrics/metric_{df_temp.columns[0]}{WINDOW_SIZE}{BLOCK_SIZE}.csv", index= False)
        consumer.close()


