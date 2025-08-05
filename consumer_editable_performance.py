import os
import json
import time
import pandas as pd
import numpy as np
from confluent_kafka import Consumer
from utils import sketcher, verify_sketch, bwd_sketcher, verify_bwd_sketch, position_finder
import argparse
from multi_proc_editable import max_rep_blocks, build_max_rep
import tracemalloc
from brute_forcer import brute_force


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consumer args")
    parser.add_argument("--window_size", type=int, required=True)
    parser.add_argument("--block_size", type=int, required=True)
    parser.add_argument("--topic_name", type=str, required=True)
    parser.add_argument("--max_windows", type=int, required=True)
    parser.add_argument("--landmark", type=int, required=True)
    parser.add_argument("--brt_force", type=str, required=True)
    parser.add_argument("--backward", type=str, required=True)
    args = parser.parse_args()
    

    # ─── Config ─────────────────────────────────────────────────────
    IN_TOPIC = args.topic_name
    WINDOW_SIZE = args.window_size
    GROUP_ID = "hospital-sliding-consumer"
    BLOCK_SIZE = args.block_size
    MAX_WINDOWS = args.max_windows
    LANDMARK = args.landmark
    brt_force = True if args.brt_force == "True" else False
    backward = True if args.backward == "True" else False
    # print(brt_force, backward)
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
    metrics = {"Window size":WINDOW_SIZE, "Block size":BLOCK_SIZE, "Landmark": LANDMARK}
    df_temp = pd.read_csv("cleaned_input_files/cleaned_df.csv")
    try:
        print(pd.read_csv(f"cleaned_input_files/summary_{df_temp.columns[0]}.csv"))
    except:
        print("Discrete value, no binning info")
    position, fairness = position_finder(df_temp, fairness, BLOCK_SIZE)

    metrics["Cardinality"]=len(position.keys())

    # Times
    sketching_sum = 0
    sketch_bld_time = 0
    bwd_sketching_sum = 0
    processing_sum = 0
    bwd_processing_sum = 0
    total_sum = 0
    swapping_sum = 0
    brt_swapping_sum = 0
    sketch_bld_latency = []
    sketch_upd_latency = []
    process_latency = []
    bwd_process_latency = []
    swap_latency = []
    brt_swap_latency = []

    # Memory
    sketching_memory = 0
    sketch_bld_sum = 0
    sketch_upd_sum = 0
    bwd_sketching_memory = 0
    query_memory = 0
    bwd_query_memory = 0
    swapping_memory = 0
    brt_swapping_memory = 0

    # Counts
    window_counter = 0
    count = 0
    sk_bld_cnts = 0
    swap_cnts = 0
    throughput = 0
    its = 0
    windows_swapped = 0
    total_blocks = 0
    fair_blocks_ini = 0
    fair_blocks_swapped = 0
    brt_blocks = 0


    def process_window(batch):
        global sketch, fairness, read_window, window_counter, metrics, position, fairness
        global sketching_sum, sketching_memory, bwd_sketching_sum, bwd_sketching_memory, sketch_bld_sum, sketch_upd_sum
        global sketch_upd_latency, sketch_bld_latency
        global sketch_bld_time, processing_sum, query_memory, bwd_processing_sum, bwd_query_memory, total_sum 
        global count, sk_bld_cnts, swap_cnts, swapping_sum, windows_swapped, fair_blocks_ini, fair_blocks_swapped, total_blocks

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
        sketching_ms = (t2 - t1) * 1000 
        t2 = time.perf_counter()
        tracemalloc.reset_peak()
        query_result, fair_block = verify_sketch(sketch, position, BLOCK_SIZE, fairness, popped)
        _, query_peak = tracemalloc.get_traced_memory()
        t3 = time.perf_counter()        

        if brt_force==True:
            brt_blocks += fair_block 
        
        if backward == True:
        # Backward sketch and query processing time and space consumption
            t1_bwd = time.perf_counter()
            tracemalloc.reset_peak()
            bwd_effective_window = read_window[attr]
            bwd_sketch = bwd_sketcher(bwd_effective_window, position)
            _, bwd_sketch_peak = tracemalloc.get_traced_memory()
            t2_bwd = time.perf_counter()
            tracemalloc.reset_peak()
            _, _ = verify_bwd_sketch(bwd_sketch, position, BLOCK_SIZE, fairness)
            _, bwd_query_peak = tracemalloc.get_traced_memory()
            t3_bwd = time.perf_counter() 
            tracemalloc.stop()
            # Backward sketch metrics
            bwd_sketching_ms = (t2_bwd - t1_bwd) * 1000 
            bwd_processing_ms = (t3_bwd - t2_bwd) * 1000
            bwd_query_peak = round(query_peak / (1024 * 1024), 4)

            bwd_sketching_sum += bwd_sketching_ms
            bwd_sketching_memory += round(bwd_sketch_peak / (1024 * 1024), 4)

            bwd_processing_sum += bwd_processing_ms
            bwd_query_memory += bwd_query_peak

            # bwd_total_sum += (t3_bwd - t1_bwd) * 1000
            
            avg_bwd_sketching = bwd_sketching_sum / count
            avg_bwd_sketching_mem = bwd_sketching_memory/count

            avg_bwd_processing = bwd_processing_sum / count        
            avg_bwd_query_mem = bwd_query_memory/count

            metrics["Avg backward preprocessing"]=avg_bwd_sketching
            metrics["Avg backward query processing"]=avg_bwd_processing
            metrics["Avg backward peak preprocessing mem consume (MB)"] = avg_bwd_sketching_mem
            metrics["Avg backward peak query processing mem consume (MB)"] = avg_bwd_query_mem

        # Forward sketch metrics
        
        processing_ms = (t3 - t2) * 1000
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
        # metrics["Avg peak preprocessing mem consume (MB)"] = avg_sketching_mem
        # metrics["Avg peak query processing mem consume (MB)"] = avg_query_mem
        # print("Sketch length: ", sketch)
        # print("Original sketching and processing: ", sketching_ms, processing_ms)
            
        return query_result, fair_block, metrics

    def edit_window(batch_landmarked, ):
        global sketch, fairness, read_window, window_counter, metrics, position, fairness
        global swap_latency, process_latency, bwd_process_latency, sketch_upd_latency, sketch_bld_latency
        global sketching_sum, sketch_bld_sum, sketch_upd_sum, sketching_memory, bwd_sketching_sum, bwd_sketching_memory
        global query_memory, swapping_memory, processing_sum, bwd_processing_sum, bwd_query_memory, sketch_bld_sum, sketch_bld_time
        global total_sum, count, swapping_sum, windows_swapped, fair_blocks_ini, fair_blocks_swapped, sk_bld_cnts
        global brt_swapping_sum, brt_swapping_memory, brt_swap_latency, swap_cnts, total_blocks
        tracemalloc.start()
        count += LANDMARK 
        read_window = pd.DataFrame(batch_landmarked)
        attr = read_window.columns[0]
        sketching_ms =  0
        processing_ms = 0
        total_querying_ms = 0
        fair_block_sum = 0
        fair_block_new_sum = 0
        
        for i in range(LANDMARK):
            effective_window = read_window[attr][i+WINDOW_SIZE:i+WINDOW_SIZE+1]
            popped = sketcher(read_window[attr], sketch, position)

            query_result, fair_block = verify_sketch(sketch, position, BLOCK_SIZE, fairness, popped)

            fair_block_sum += fair_block
            

        fair_blocks_ini += fair_block_sum
        sketch = []
        sketching_ms =  0
        processing_ms = 0
        bwd_sketching_ms =  0
        bwd_processing_ms = 0
        sketch_peak = 0
        query_peak_mem = 0
        bwd_sketch_peak_mem = 0
        bwd_query_peak_mem = 0
        total_querying_ms = 0
        sum_blocks = 0
        query_results = []
        
        # Swapping with prefix-method
        t4 = time.perf_counter()
        tracemalloc.reset_peak()
        # print("HERE?, ", read_window[attr].to_list())
        m, rem_counts, unique = max_rep_blocks(read_window[attr].to_list(), BLOCK_SIZE, WINDOW_SIZE, fairness)
        edited_stream = build_max_rep(m, rem_counts, unique, BLOCK_SIZE, fairness)
        # print("Now here, ", edited_stream)
        _, swapping_mem = tracemalloc.get_traced_memory()
        read_window = pd.DataFrame(edited_stream, columns=[attr])
        buffer = read_window.to_dict(orient='records')
        t5 = time.perf_counter()
        swap_cnts += 1
        swapping_time = (t5 - t4) * 1000
        processing_ms += (t5 - t4) * 1000  
        bwd_processing_ms += (t5 - t4) * 1000
        swapping_mem = round(swapping_mem / (1024 * 1024),4)
        swap_latency.append(swapping_time)
        # input()
        #Swapping with brute-force method
        if brt_force == True:
            t4_brt = time.perf_counter()
            tracemalloc.reset_peak()
            _, _, ideal_stream = brute_force(read_window[attr].to_list(), fairness, WINDOW_SIZE, BLOCK_SIZE)
            _, brt_swapping_mem = tracemalloc.get_traced_memory()
            temp_read_window = pd.DataFrame(ideal_stream, columns=[attr])
            _ = temp_read_window.to_dict(orient='records')
            t5_brt = time.perf_counter()

            brt_swapping_time = (t5_brt - t4_brt) * 1000
            brt_swapping_mem = round(brt_swapping_mem / (1024 * 1024),4)
            brt_swap_latency.append(brt_swapping_time)
            brt_swapping_sum += brt_swapping_time
            brt_swapping_memory += brt_swapping_mem

            avg_brt_swapping = (brt_swapping_sum / count)
            avg_brt_swapping_mem = (brt_swapping_memory/count)

            metrics["Avg brute force swapping time"] = avg_brt_swapping
            metrics["Avg brute force peak swapping mem consume (MB)"] = avg_brt_swapping_mem
            # print(edited_stream, ideal_stream)
            
            

        for i in range(LANDMARK+1):
            sum_blocks += WINDOW_SIZE//BLOCK_SIZE
            # Forward sketch and query processing time and space consumption
            effective_window = read_window[attr][i+WINDOW_SIZE-1:i+WINDOW_SIZE]
            t6 = time.perf_counter()
            tracemalloc.reset_peak()
            if len(sketch) == 0:
                effective_window = read_window[attr][i:WINDOW_SIZE + i]
                popped = sketcher(effective_window, sketch, position)
                t7 = time.perf_counter()
                sketch_bld_time += (t7 - t6) * 1000
                _, sketch_peak_bld = tracemalloc.get_traced_memory()
                sketch_bld_sum += round(sketch_peak_bld / (1024 * 1024), 4)
                sketch_peak += round(sketch_peak_bld / (1024 * 1024), 4)
                sk_bld_cnts += 1
                sketch_bld_latency.append((t7 - t6) * 1000)
                # print((t7 - t6) * 1000)
            else:
                popped = sketcher(effective_window, sketch, position)
                t7 = time.perf_counter()
                _, sketch_peak_upd = tracemalloc.get_traced_memory()
                sketch_peak += round(sketch_peak_upd / (1024 * 1024), 4)
                sketch_upd_sum += round(sketch_peak_upd / (1024 * 1024), 4)
                sketch_upd_latency.append((t7 - t6) * 1000)
                # print( round(sketch_peak_upd / (1024 * 1024), 4))
            t8 = time.perf_counter()
            tracemalloc.reset_peak()
            query_result, fair_block_new = verify_sketch(sketch, position, BLOCK_SIZE, fairness, popped)
            _, query_peak = tracemalloc.get_traced_memory()
            t9 = time.perf_counter()
            # print(query_result, fair_block_new)

            if brt_force == True:
                brt_sketch = []
                temp_read_window = pd.DataFrame(ideal_stream, columns=[attr])
                id_eff_win = temp_read_window[attr][i:WINDOW_SIZE + i]
                _ = sketcher(id_eff_win, brt_sketch, position)
                qr, brt_fr_new = verify_sketch(brt_sketch, position, BLOCK_SIZE, fairness, None)

                # print(qr, brt_fr_new)
                brt_blocks += brt_fr_new


            # Foward sketch and query processing metrics
            
            query_results.append(query_result)
            fair_block_new_sum += fair_block_new

            sketching_ms +=  (t7 - t6) * 1000
            processing_ms += (t9 - t8) * 1000 
            query_peak_mem += round(query_peak / (1024 * 1024), 4)
            # total_querying_ms += (t9 - t6) * 1000 
            process_latency.append((t9 - t8) * 1000  + swapping_time)

            if backward == True:
            # Backward sketch and query processing time and space consumption
                t1_bwd = time.perf_counter()
                tracemalloc.reset_peak()
                bwd_effective_window = read_window[attr][i:WINDOW_SIZE + i]
                bwd_sketch = bwd_sketcher(bwd_effective_window, position) 
                _, bwd_sketch_peak = tracemalloc.get_traced_memory()
                t2_bwd = time.perf_counter()
                tracemalloc.reset_peak()
                _, _ = verify_bwd_sketch(bwd_sketch, position, BLOCK_SIZE, fairness)
                _, bwd_query_peak = tracemalloc.get_traced_memory()
                t3_bwd = time.perf_counter() 
                
                # Backward sketch and query processing
                bwd_sketching_ms +=  (t2_bwd - t1_bwd) * 1000
                bwd_processing_ms += (t3_bwd - t2_bwd) * 1000 
                bwd_sketch_peak_mem += round(bwd_sketch_peak / (1024 * 1024), 4)
                bwd_query_peak_mem += round(bwd_query_peak / (1024 * 1024), 4)
                bwd_process_latency.append((t3_bwd - t2_bwd) * 1000 + swapping_time)
            
        tracemalloc.stop()
        # if fair_block_new_sum != max_fairs:
        #     print(ideal_stream, '\n', edited_stream, '\n', max_fairs, fair_block_new_sum)
        #     input()
        if backward == True:
        # Backward sketch and query processing metrics
            bwd_sketching_sum += bwd_sketching_ms
            bwd_sketching_memory += bwd_sketch_peak_mem

            bwd_processing_sum += bwd_processing_ms
            bwd_query_memory += bwd_query_peak_mem

            avg_bwd_sketching = bwd_sketching_sum / count
            avg_bwd_sketching_mem = bwd_sketching_memory/count

            avg_bwd_processing = bwd_processing_sum / count        
            avg_bwd_query_mem = bwd_query_memory/count

            metrics["Avg backward preprocessing"]=avg_bwd_sketching
            metrics["Avg backward query processing"]=avg_bwd_processing
            metrics["Avg backward peak preprocessing mem consume (MB)"] = avg_bwd_sketching_mem
            metrics["Avg backward peak query processing mem consume (MB)"] = avg_bwd_query_mem
            metrics["Throughput of backward sketch query processing"] = 1000//(avg_bwd_processing+avg_bwd_sketching)

        # Foward sketch and query processing metrics
        # if sum_blocks < fair_block_new_sum:
        #     print(sum_blocks, fair_block_new_sum, read_window)
        #     input()
        
        total_blocks += sum_blocks
        fair_blocks_swapped += fair_block_new_sum   

        swapping_sum += swapping_time
        swapping_memory += swapping_mem

        sketching_sum += sketching_ms
        sketching_memory += sketch_peak

        processing_sum += processing_ms
        query_memory += query_peak_mem

        total_sum += sketching_ms + processing_ms

        avg_sketching = sketching_sum / count
        # avg_sketching_mem = sketching_memory / count

        avg_processing = processing_sum / count
        # avg_query_mem = query_memory / count        
            
        # print(f" Total querying time: {total_querying_ms:.3f} ms")
        # print("Average swapping, preprocessing, querying time: ", avg_swapping, avg_sketching, avg_processing)
        
        metrics["Avg preprocessing"]=avg_sketching
        metrics["Avg query processing"]=avg_processing
        
        
        
        # metrics["Sketch build memory as pct of total preprocessing"] = sketch_bld_sum *100/sketching_memory
        if brt_force == True:
            # brt_fair_blocks += brt_fr_blcs_sum
            metrics["Avg brute force query processing"]= (brt_swapping_sum + (processing_sum-swapping_sum))/count
            metrics["Brute Force Throughput"] = 1000//((sketching_sum + brt_swapping_sum + (processing_sum-swapping_sum))/count)

        return query_results, buffer, fair_block_new_sum

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
            # if len(message_buffer) >= WINDOW_SIZE+2:
            #     # print("here2")
            #     break
            # Keep only latest WINDOW_SIZE messages (sliding behavior)
            if len(message_buffer) > WINDOW_SIZE:
                message_buffer.pop(0)

            if len(message_buffer) == WINDOW_SIZE:
                window_counter += 1
                
                # Process the current sliding window (every new message triggers this)
                print(message_buffer)
                query_result, fair_block, metrics = process_window(message_buffer.copy())
                print(query_result, '\n')

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
                    print('\n', message_buffer)
                    query_results, message_buffer, fr_blc_new_sum = edit_window(message_buffer.copy())
                    print(f'Result after applying reorder for {LANDMARK+1} windows: ',query_results)
                    print(f"Number of blocks made fair: ", fr_blc_new_sum, '\n')
                    # print("now till here ", message_buffer)
                    for _ in range(LANDMARK):
                        message_buffer.pop(0)
                        # print("Here?")
                else:
                    fair_blocks_swapped += fair_block
            
            
            if brt_force == True:
                # print(fair_blocks_swapped, brt_blocks)
                # input()
                metrics["Total fair blocks brute force swapping"] = brt_blocks
            # print(message_buffer)
            # print("Verify:", window_counter == count, window_counter, count)
            # print(window_counter, total_sum)
            
            # if total_sum >= 1000:
            #     metrics["Percentage of total time spent in Preprocessing"] = sketching_sum/total_sum * 100
            #     metrics["Percentage of total time spent in Query Processing"] = processing_sum/total_sum * 100
                
            #     throughput += window_counter
            #     its += 1
            #     window_counter = 1
            #     total_sum = 0
            #     print("Throughput = ", throughput//its)
            #     metrics["Throughput"] = throughput//its
            
                

            # if its == 1:
            #     break
            # print(window_counter, MAX_WINDOWS)

            if "Throughput" not in metrics.keys():
                    if processing_sum >= 1000:
                        metrics["Throughput"] = count

            if window_counter >= MAX_WINDOWS:
                its += 1
                        
                avg_fair_blc_ini = fair_blocks_ini//its
                avg_fair_blc_swapped = fair_blocks_swapped//its
                avg_total_blocks = total_blocks//its
                avg_sketch_bld_time = sketch_bld_time/its
                avg_total_sum = total_sum/its
                avg_sk_sum = sketching_sum/its
                avg_proc_sum = processing_sum/its
                avg_swapping_sum = swapping_sum/its
                avg_swap_cnts = swap_cnts//its
                avg_window_counter = count//its
                avg_swapping_memory = swapping_memory/its
                avg_sk_bld_sum = sketch_bld_sum/its
                avg_sk_bld_cnts = sk_bld_cnts//its
                avg_sketch_upd_sum = sketch_upd_sum/its
                avg_query_mem = query_memory/its
                avg_sketching_mem = sketching_memory/its

                    
                
                
                metrics["Windows covered"] = avg_window_counter
                metrics["Total fair blocks without swapping"] = avg_fair_blc_ini
                metrics["Total fair blocks after swapping"] = avg_fair_blc_swapped
                metrics["Percentage of fair blocks before swap"] = fair_blocks_ini*100/total_blocks
                metrics["Percentage of fair blocks after swap"] = fair_blocks_swapped*100/total_blocks
                metrics["Sketch build time as pct of total preprocessing"] = avg_sketch_bld_time *100/avg_sk_sum
                metrics["Total edits"] = avg_swap_cnts
                metrics["Total time"] = avg_total_sum
                metrics["Total preprocessing time"] = avg_sk_sum
                metrics["Total query processing time"] = avg_proc_sum
                metrics["Avg peak preprocessing mem consume (MB)"] = avg_sketching_mem/avg_window_counter
                metrics["Avg peak query processing mem consume (MB)"] = avg_query_mem/avg_window_counter
                metrics["Avg swapping time"] = avg_swapping_sum/avg_swap_cnts
                metrics["Percentage of query processing time spent in Swapping"] = avg_swapping_sum/avg_proc_sum * 100
                metrics["Avg query answering time"] = (avg_proc_sum - avg_swapping_sum)/avg_window_counter
                metrics["Avg peak swapping mem consume (MB)"] = avg_swapping_memory/avg_swap_cnts
                metrics["Preprocessing: Avg Peak sketch building memory consumption (MB)"] = avg_sk_bld_sum / avg_sk_bld_cnts
                metrics["Preprocessing: Avg Sketch Building time"] = avg_sketch_bld_time / avg_sk_bld_cnts
                metrics["Preprocessing: Avg Peak sketch updating memory consumption (MB)"] = avg_sketch_upd_sum / (avg_window_counter - avg_sk_bld_cnts)
                metrics["Preprocessing: Avg Sketch Updating time"] = (avg_sk_sum - avg_sketch_bld_time) / (avg_window_counter - avg_sk_bld_cnts) 
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
        metrics["Swapping tail latency"] = np.percentile(swap_latency, 90)
        metrics["Processing_tail latency"] = np.percentile(process_latency, 90)
        metrics["Sketch build latency"] = np.percentile(sketch_bld_latency, 90)
        metrics["Sketch update latency"] = np.percentile(sketch_upd_latency, 90)
        sketch_bld_latency.extend(sketch_upd_latency)
        metrics["Preprocessing latency"] = np.percentile(sketch_bld_latency, 90)
        if backward == True:
            metrics["Backward Processing tail latency"] = np.percentile(bwd_process_latency, 90)
        if brt_force == True:
            metrics["Brute force swapping tail latency"] = np.percentile(brt_swap_latency, 90)
        metric = pd.DataFrame(metrics, index = [0])
        print("Metrics: ", metric)
        if backward == True:
            metric.to_csv(f"metrics/baselines/backward_sketch/metric_editable_{df_temp.columns[0]}WIN{WINDOW_SIZE}BLC{BLOCK_SIZE}LAN{LANDMARK}.csv", index= False)
        elif brt_force == True:
            metric.to_csv(f"metrics/baselines/brute_force/metric_editable_{df_temp.columns[0]}WIN{WINDOW_SIZE}BLC{BLOCK_SIZE}LAN{LANDMARK}.csv", index= False)
        else:
            metric.to_csv(f"metrics/metric_editable_{df_temp.columns[0]}WIN{WINDOW_SIZE}BLC{BLOCK_SIZE}LAN{LANDMARK}.csv", index= False)
        consumer.close()