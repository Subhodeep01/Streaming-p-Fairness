from multi_proc_editable import max_rep_blocks, build_max_rep
from brute_forcer import brute_force
import time
import pandas as pd
# import user_inputs

df = pd.read_csv("cleaned_input_files\cleaned_df.csv")

BLOCK_SIZE = 5
WINDOW_SIZE = 10
LANDMARK = 5
# fairness = {0: 2, 1: 2, 2:1}
fairness = {"DAMA":2, "F":2}
total_fair_blocks_sum = 0
brute_block_sum = 0
brute_blocks = {"window size": WINDOW_SIZE, "block size": BLOCK_SIZE, "landmark":LANDMARK, "Cardinality":len(fairness.keys())}

i = 0
def validate(output:list):
        global fairness, BLOCK_SIZE, WINDOW_SIZE
        block_size = BLOCK_SIZE
        window_size = WINDOW_SIZE
        unique = list(set(fairness.keys()))
        unique.sort()
        
        fair_window = 0
        current = 0
        total_fair_blocks = 0
        already_visited = []
        while current+window_size <= len(output):
            
            fair_block = 0
            # print(current, current+window_size, len(output))
            for block in range(current, current+window_size, block_size):
                if (block,block+block_size) in already_visited:
                    fair_block += 1
                    continue
                already_visited.append((block,block+block_size))
                count_dict = {}
                for i in unique:
                    count_dict[i] = output[block:block+block_size].count(i)
                flag = 0
                for i in unique:
                    if count_dict[i] >= fairness[i]:
                        flag += 1
                if flag == len(unique):
                        fair_block += 1
                        total_fair_blocks += 1
            if fair_block == int(window_size//block_size): fair_window += 1
            current += 1
        print(f"Total fair windows = {fair_window}, Total fair blocks = {total_fair_blocks}")
        # print(f"Unfair windows: {unfair_dict}")
        return total_fair_blocks

while(i <= 10):
    input_col = df["GENDER"][i:i+WINDOW_SIZE+LANDMARK].to_list()
    # input_col = ['M', 'M', 'M', 'F', 'F', 'M', 'F', 'M', 'F', 'M', 'M', 'M']
    # input_col = [0,1,1,0,0,2,2,2,2,0,0,0]
    # print(input_col)
    # unique = 
    m, rem_counts, unique = max_rep_blocks(input_col, BLOCK_SIZE, WINDOW_SIZE, fairness)
    # print(m, rem_counts, unique, fairness)
    output = build_max_rep(m, rem_counts, unique, BLOCK_SIZE, fairness)

    total_fair_blocks = validate(output)

    def compare_brute_force(input_col):
        global fairness, WINDOW_SIZE, BLOCK_SIZE
        brute_win, brute_block, brute_stream = brute_force(input_col, fairness, WINDOW_SIZE, BLOCK_SIZE)
        print(f"Using brute force max fair windows = {brute_win}, max fair blocks = {brute_block} and ideal stream = {brute_stream}")
        return brute_block

    # print(output)
    
    brute_block = compare_brute_force(input_col)
    i+=1

    if total_fair_blocks != brute_block:
         print("MISMATCH!", output)
         input()
    else:
         brute_block_sum += brute_block
         total_fair_blocks_sum += total_fair_blocks
         brute_blocks["BRUTE"] = brute_block_sum
         brute_blocks["pfair"] = total_fair_blocks_sum
    print(i)
    # input()
brute_blocks = pd.DataFrame(brute_blocks, index = [0])
brute_blocks.to_csv(f"metrics/Bruteforceblocks{WINDOW_SIZE}_{BLOCK_SIZE}_{LANDMARK}_{len(fairness.keys())}.csv", index=False)
# print(f"Time taken by PREFIX METHOD = {t2-t1}\nTime taken by BRUTE FORCE = {t4-t3}")