from multi_proc_editable import input_col, output, block_size, window_size, fairness_criteria
from brute_forcer import brute_force
import time

t1 = time.time()
def validate(output:list):
    global fairness_criteria, block_size, window_size
    unique = list(set(output))
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
                if count_dict[i] >= fairness_criteria[i]:
                    flag += 1
            if flag == len(unique):
                    fair_block += 1
                    total_fair_blocks += 1
        if fair_block == int(window_size//block_size): fair_window += 1
        current += 1
    print(f"Total fair windows = {fair_window}, Total fair blocks = {total_fair_blocks}")
    # print(f"Unfair windows: {unfair_dict}")
t2 = time.time()

validate(output)

def compare_brute_force(input_col):
    global fairness_criteria, window_size, block_size
    brute_win, brute_block, brute_stream = brute_force(input_col, fairness_criteria, window_size, block_size)
    print(f"Using brute force max fair windows = {brute_win}, max fair blocks = {brute_block} and ideal stream = {brute_stream}")

# print(input_col)
t3 = time.time()
compare_brute_force(input_col)
t4 = time.time()

print(f"Time taken by PREFIX METHOD = {t2-t1}\nTime taken by BRUTE FORCE = {t4-t3}")