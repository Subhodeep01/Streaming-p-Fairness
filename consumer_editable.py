import os
import json
import time
import pandas as pd
from confluent_kafka import Consumer
from utils import sketcher, verify_sketch, position_finder
import argparse
from multi_proc_editable import max_rep_blocks, build_max_rep


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consumer args")
    parser.add_argument("--window_size", type=int, required=True)
    parser.add_argument("--block_size", type=int, required=True)
    parser.add_argument("--topic_name", type=str, required=True)
    parser.add_argument("--max_windows", type=int, required=True)
    parser.add_argument("--landmark", type=int, required=True)
    args = parser.parse_args()
    

    # ─── Config ─────────────────────────────────────────────────────
    IN_TOPIC = args.topic_name
    WINDOW_SIZE = args.window_size
    GROUP_ID = "hospital-sliding-consumer"
    BLOCK_SIZE = args.block_size
    MAX_WINDOWS = args.max_windows
    LANDMARK = args.landmark
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

    metrics["Cardinality"]=len(position.keys())
    window_counter = 0
    sketching_sum = 0
    processing_sum = 0
    total_sum = 0
    swapping_sum = 0
    count = 0
    throughput = 0
    its = 0
    windows_swapped = 0
    fair_blocks_ini = 0
    fair_blocks_swapped = 0


    def process_window(batch):
        global sketch, fairness, read_window, window_counter, metrics, position, fairness
        global sketching_sum, processing_sum, total_sum, count, swapping_sum, windows_swapped, fair_blocks_ini, fair_blocks_swapped
        
        count += 1
        read_window = pd.DataFrame(batch)
        
        attr = read_window.columns[0]
        t1 = time.perf_counter()
        popped = sketcher(read_window[attr], sketch, position)
        t2 = time.perf_counter()
        query_result, fair_block = verify_sketch(sketch, position, BLOCK_SIZE, fairness, popped)
        t3 = time.perf_counter()         
        
        sketching_ms = (t2 - t1) * 1000
        processing_ms = (t3 - t2) * 1000
        total_querying_ms = (t3 - t1) * 1000

        fair_blocks_ini += fair_block
        sketching_sum += sketching_ms
        processing_sum += processing_ms
        total_sum += total_querying_ms
        avg_sketching = sketching_sum / count
        avg_processing = processing_sum / count        
        
        metrics["Avg preprocessing"]=avg_sketching
        metrics["Avg query processing"]=avg_processing
        
        if count == 1:
            metrics["Preprocessing: Sketch Building time"] = sketching_ms
            
        return query_result, fair_block, metrics

    def edit_window(batch_landmarked, ):
        global sketch, fairness, read_window, window_counter, metrics, position, fairness
        global sketching_sum, processing_sum, total_sum, count, swapping_sum, windows_swapped, fair_blocks_ini, fair_blocks_swapped
        
        count += LANDMARK 
        read_window = pd.DataFrame(batch_landmarked)
        attr = read_window.columns[0]
        sketching_ms =  0
        processing_ms = 0
        total_querying_ms = 0
        fair_block_sum = 0
        fair_block_new_sum = 0
        query_results = []

        # Compute the query result and total fair blocks without applying the edit
        for i in range(LANDMARK):
            effective_window = read_window[attr][i+1:WINDOW_SIZE + i+1]
            popped = sketcher(read_window[attr], sketch, position)
            query_result, fair_block = verify_sketch(sketch, position, BLOCK_SIZE, fairness, popped)
            fair_block_sum += fair_block

        fair_blocks_ini += fair_block_sum
        t4 = time.perf_counter()
        sketch = []
        query_results = []
        # print("Reached here safely", batch_landmarked)
        m, rem_counts, unique = max_rep_blocks(read_window[attr].to_list(), BLOCK_SIZE, WINDOW_SIZE, fairness)
        # print("Did we reach here though?",m, rem_counts, unique)
        edited_stream = build_max_rep(m, rem_counts, unique, BLOCK_SIZE, fairness)
        # print("Can we reach here?", edited_stream)
        # input("Lets check!")
        read_window = pd.DataFrame(edited_stream, columns=[attr])
        buffer = read_window.to_dict(orient='records')
        t5 = time.perf_counter()
        # print("Did we reach here though?", buffer)
        swapping_time = (t5 - t4) * 1000
        processing_ms += (t5 - t4) * 1000  
        
        # Compute the fair blocks without applying the edit
        for i in range(LANDMARK+1):
            effective_window = read_window[attr][i:WINDOW_SIZE + i]
            t6 = time.perf_counter()
            popped = sketcher(effective_window, sketch, position)
            t7 = time.perf_counter()
            query_result, fair_block_new = verify_sketch(sketch, position, BLOCK_SIZE, fairness, popped)
            t8 = time.perf_counter()

            query_results.append(query_result)
            fair_block_new_sum += fair_block_new

            sketching_ms +=  (t7 - t6) * 1000
            processing_ms += (t8 - t7) * 1000 
            total_querying_ms += (t8 - t6) * 1000 

        fair_blocks_swapped += fair_block_new_sum   
            
        swapping_sum += swapping_time
        sketching_sum += sketching_ms
        processing_sum += processing_ms
        total_sum += total_querying_ms

        avg_swapping = (swapping_sum / count)
        avg_sketching = sketching_sum / count
        avg_processing = processing_sum / count
            
            
        metrics["Avg swapping time"] = avg_swapping
        metrics["Avg preprocessing"]=avg_sketching
        metrics["Avg query processing"]=avg_processing

        return query_results, buffer

    # ─── Main Loop ───────────────────────────────────────────────────
    print(f"Listening to '{IN_TOPIC}' from broker '{'localhost:9092'}' …")

    try:
        while True:        
            msg = consumer.poll(100.0)  # timeout = 1 sec

            if msg is None:
                print("No msg received")
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue
            # Deserialize JSON
            value = json.loads(msg.value().decode('utf-8'))
            message_buffer.append(value)
            # print(message_buffer)
            if len(message_buffer) >= WINDOW_SIZE+2:
                # print("here2")
                break
            # Keep only latest WINDOW_SIZE messages (sliding behavior)
            if len(message_buffer) > WINDOW_SIZE:
                message_buffer.pop(0)

            if len(message_buffer) == WINDOW_SIZE:
                window_counter += 1
                
                # Process the current sliding window (every new message triggers this)
                query_result, fair_block, metrics = process_window(message_buffer.copy())

                if fair_block < WINDOW_SIZE // BLOCK_SIZE:
                    # print("here")
                    # Pull in landmark records and process the editable stream
                    for _ in range(LANDMARK):
                        window_counter += 1
                        msg = consumer.poll(100.0)
                        
                        if msg is None:
                            print("No msg received")
                            continue
                        if msg.error():
                            print("Consumer error:", msg.error())
                            continue
                        
                        value = json.loads(msg.value().decode('utf-8'))
                        message_buffer.append(value)
                    # print("reached till here ", len(message_buffer))
                    query_results, message_buffer = edit_window(message_buffer.copy())
                    # print("now till here ", message_buffer)
                    for _ in range(LANDMARK):
                        message_buffer.pop(0)
                else:
                    fair_blocks_swapped += fair_block
            
            metrics["Total fair blocks without swapping"] = fair_blocks_ini
            metrics["Total fair blocks after swapping"] = fair_blocks_swapped
            metrics["Total windows covered"] = window_counter
            # print(message_buffer)
            print(total_sum)
            if total_sum >= 1000:
                throughput += window_counter
                its += 1
                window_counter = 1
                total_sum = 0
                print("Throughput = ", throughput//its)
                metrics["Throughput"] = throughput//its
                
            if its == 1:
                break
            # if window_counter == MAX_WINDOWS:
            #     # print(total_sum)
            #     break
        
        

    except KeyboardInterrupt:
        print("Stopping consumer…")
    finally:
        metric = pd.DataFrame(metrics, index = [0])
        print("Metrics: ", metric)
        metric.to_csv(f"metrics/metric_editable_{df_temp.columns[0]}{WINDOW_SIZE}{BLOCK_SIZE}.csv", index= False)
        consumer.close()