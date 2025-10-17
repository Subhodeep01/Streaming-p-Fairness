from multi_proc_editable_slider import max_rep_blocks, build_max_rep
from brute_forcer import brute_force
from utils import position_finder
import time
import pandas as pd


# input = ['M' for i in range(5)] + ['F' for i in range(15)]
input = ['A' for i in range(5)] + ['B' for i in range(6)] + ['C' for i in range(9)]
win_len = 15
slide = 5
block_len = 5

# fairness = {'M': 2, 'F':3}
fairness = {'A': 2, 'B':1, 'C': 2}


i = 0

def validate(output:list, slide:int):
        global fairness, block_len, win_len
        block_len = block_len
        win_len = win_len
        unique = list(set(fairness.keys()))
        unique.sort()
        
        fair_window = 0
        current = 0
        total_fair_blocks = 0
        already_visited = {}
        while current+win_len <= len(output):
            
            fair_block = 0
            # print(current, current+win_len, len(output))
            print(output[current:current+win_len])
            for block in range(current, current+win_len, block_len):
                if (block,block+block_len) in already_visited:
                    fair_block += already_visited[(block,block+block_len)]
                    continue
                # already_visited.append((block,block+block_len))
                count_dict = {}
                for i in unique:
                    count_dict[i] = output[block:block+block_len].count(i)
                flag = 0
                for i in unique:
                    if count_dict[i] >= fairness[i]:
                        flag += 1
                        
                print(count_dict)
                if flag == len(unique):
                        fair_block += 1
                        total_fair_blocks += 1
                        already_visited[(block,block+block_len)] = 1
                else: already_visited[(block,block+block_len)] = 0
                        
            if fair_block == int(win_len//block_len): fair_window += 1
            current += slide
        print(f"Total fair windows = {fair_window}, Total fair blocks = {total_fair_blocks}")
        # print(f"Unfair windows: {unfair_dict}")
        print(already_visited)
        return total_fair_blocks

m, rem_counts, unique = max_rep_blocks(input, block_len, win_len, fairness)
print(m, rem_counts, unique, fairness)
output = build_max_rep(m, rem_counts, unique, block_len, fairness)

total_fair_blocks = validate(output, slide)

def compare_brute_force(input):
        global fairness, win_len, block_len
        brute_win, brute_block, brute_stream = brute_force(input, fairness, win_len, block_len, slide)
        print(f"Using brute force max fair windows = {brute_win}, max fair blocks = {brute_block} and ideal stream = {brute_stream}")
        return brute_block

    # print(output)
    
brute_block = compare_brute_force(input)