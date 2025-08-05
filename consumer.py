import numpy as np
import json
import time
import pandas as pd
from confluent_kafka import Consumer
from utils import sketcher, verify_sketch, position_finder
import argparse
import tracemalloc


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

    # Times
    sketching_sum = 0
    sketch_bld_time = 0
    sketch_upd_time = 0
    processing_sum = 0
    total_sum = 0
    sketch_bld_latency = []
    sketch_upd_latency = []
    process_latency = []

    # Memory
    sketching_memory = 0
    sketch_bld_sum = 0
    sketch_upd_sum = 0
    query_memory = 0

    # Counts
    window_counter = 0
    count = 0
    sk_bld_cnts = 0
    throughput = 0
    its = 0
    total_blocks = 0
    fair_blocks_ini = 0

    def process_window(batch):
        global sketch, fairness, read_window, window_counter, metrics, position, fairness
        global sketching_sum, sketching_memory, sketch_bld_sum, sketch_upd_sum, sketch_upd_time
        global sketch_upd_latency, sketch_bld_latency, process_latency
        global sketch_bld_time, processing_sum, query_memory, total_sum 
        global count, sk_bld_cnts, fair_blocks_ini, total_blocks

        tracemalloc.start()
        count += 1
        read_window = pd.DataFrame(batch)
        
        
        attr = read_window.columns[0]
        effective_window = read_window[attr][-1:]
        
        # Forward Sketch and query processing time and space consumption
        t1 = time.perf_counter()
        tracemalloc.reset_peak()
        if len(sketch) == 0:
            effective_window = read_window[attr]
            popped = sketcher(effective_window, sketch, position)
            t2 = time.perf_counter()
            _, sketch_peak_bld = tracemalloc.get_traced_memory()
            sketch_peak = round(sketch_peak_bld / (1024 * 1024), 4)
            sketch_bld_sum += sketch_peak 
            sketch_bld_time += (t2 - t1) * 1000 
            sk_bld_cnts += 1
            sketch_bld_latency.append((t2 - t1) * 1000)
        else:
            popped = sketcher(effective_window, sketch, position)
            t2 = time.perf_counter()
            _, sketch_peak_upd = tracemalloc.get_traced_memory()
            sketch_peak = round(sketch_peak_upd / (1024 * 1024), 4)
            sketch_upd_sum += sketch_peak
            sketch_upd_latency.append((t2 - t1) * 1000)
            sketch_upd_time += (t2 - t1) * 1000
            # print((t2 - t1) * 1000)
        sketching_ms = (t2 - t1) * 1000 
        t3 = time.perf_counter()
        tracemalloc.reset_peak()
        query_result, fair_block = verify_sketch(sketch, position, BLOCK_SIZE, fairness, popped)
        _, query_peak = tracemalloc.get_traced_memory()
        t4 = time.perf_counter()         
        
        
        processing_ms = (t4 - t3) * 1000
        process_latency.append(processing_ms)
        query_peak = round(query_peak / (1024 * 1024), 4)
        total_querying_ms = sketching_ms + processing_ms

        sum_blocks = WINDOW_SIZE // BLOCK_SIZE

        # if fair_block > sum_blocks: 
        #     print(sum_blocks, fair_block, read_window)
        #     input()
        total_blocks += sum_blocks
        fair_blocks_ini += fair_block
        sketching_sum += sketching_ms
        sketching_memory += sketch_peak

        processing_sum += processing_ms
        query_memory += query_peak

        total_sum += total_querying_ms
        avg_sketching = sketching_sum / count
        # avg_sketching_mem = sketching_memory/count

        avg_processing = processing_sum / count        
        # avg_query_mem = query_memory/count

        
        metrics["Avg preprocessing"]=avg_sketching
        metrics["Avg query processing"]=avg_processing
        
            
        return query_result, fair_block, metrics


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
                print(message_buffer)
                query_result,_, metric = process_window(message_buffer.copy())
                print("Query Result: ", query_result)
            
            # print(total_sum)
            if "Throughput" not in metrics.keys():
                    if processing_sum >= 1000:
                        metrics["Throughput"] = count


            if window_counter >= MAX_WINDOWS:
                its += 1
                        
                avg_fair_blc_ini = fair_blocks_ini//its
                avg_total_blocks = total_blocks//its
                avg_sketch_bld_time = sketch_bld_time/1
                avg_total_sum = total_sum/its
                avg_sk_sum = sketching_sum/its
                avg_proc_sum = processing_sum/its
                avg_window_counter = count//its
                avg_sk_bld_sum = sketch_bld_sum/1
                avg_sk_bld_cnts = 1
                avg_sketch_upd_sum = sketch_upd_sum/its
                avg_query_mem = query_memory/its
                avg_sketching_mem = sketching_memory/its

                    
                
                
                metrics["Windows covered"] = avg_window_counter
                metrics["Total fair blocks without swapping"] = avg_fair_blc_ini
                metrics["Percentage of fair blocks without swap"] = fair_blocks_ini*100/total_blocks
                metrics["Sketch build time as pct of total preprocessing"] = avg_sketch_bld_time *100/avg_sk_sum
                metrics["Total time"] = avg_total_sum
                metrics["Total preprocessing time"] = avg_sk_sum
                metrics["Total query processing time"] = avg_proc_sum
                metrics["Avg peak preprocessing mem consume (MB)"] = avg_sketching_mem/avg_window_counter
                metrics["Avg peak query processing mem consume (MB)"] = avg_query_mem/avg_window_counter
                metrics["Preprocessing: Avg Peak sketch building memory consumption (MB)"] = sketch_bld_sum / avg_sk_bld_cnts
                metrics["Preprocessing: Avg Sketch Building time"] = sketch_bld_time / avg_sk_bld_cnts
                metrics["Preprocessing: Avg Peak sketch updating memory consumption (MB)"] = sketch_upd_sum / count
                metrics["Preprocessing: Avg Sketch Updating time"] = sketch_upd_time / count
                # metrics["Percentage of total time spent in Preprocessing"] = avg_sk_sum/avg_total_sum * 100
                metrics["Percentage of total time spent in Query Processing"] = avg_proc_sum/avg_total_sum * 100
                
                # print("Iteration: ", its)
                # print(metrics)
                window_counter = 0

                

            if its == 1:
                break      
        
        
        

    except KeyboardInterrupt:
        print("Stopping consumer…")
    finally:
        metrics["Processing_tail latency"] = np.percentile(process_latency, 90)
        metrics["Sketch build latency"] = np.percentile(sketch_bld_latency, 90)
        metrics["Sketch update latency"] = np.percentile(sketch_upd_latency, 90)
        sketch_bld_latency.extend(sketch_upd_latency)
        metrics["Preprocessing latency"] = np.percentile(sketch_bld_latency, 90)
        metric = pd.DataFrame(metric, index = [0])
        print("Metrics: ", metric)
        metric.to_csv(f"metrics/metric_{df_temp.columns[0]}{WINDOW_SIZE}{BLOCK_SIZE}.csv", index= False)
        consumer.close()


