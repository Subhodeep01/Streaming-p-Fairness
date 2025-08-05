import math
from collections import deque, Counter

def max_rep_blocks(input_col:list, block_size, window_size, fairness:dict):
    unique = list(fairness.keys())
    unique.sort()
    t_xs = {i:input_col.count(i) for i in unique}
    
    m = window_size * block_size
    for i, Tx in t_xs.items():
        m = int(min(m, math.floor(Tx/fairness[i])))
    rem_counts = {}
    for i, Tx in t_xs.items():
        rem_counts[i] = int(Tx - fairness[i]*m)
    return m, rem_counts, unique
    
def build_max_rep(m, rem_counts:dict, unique:list, block_size:int, fairness_criteria:dict):
    temp_flag = False
    
    for r in rem_counts.values():
        if r != 0:
            temp_flag = False
            break
        else:
            temp_flag = True
    if temp_flag == True:
        output = []
        for i, fair in fairness_criteria.items():
            output.extend([i] * fair)
        return output * m
        
    
    # Building the unfair blocks with the remaining bits
    unfairblocks = []
    while (temp_flag == False):
        # rem_sorted = dict(sorted(rem_counts.items(), key=lambda item: (item[1], item[0]), reverse=True)) # sort by value and then by key
        # Compute difference for sorting
        diff = {k: rem_counts.get(k, 0) - fairness_criteria.get(k, 0) for k in set(rem_counts) | set(fairness_criteria)}

        # Sort D by difference in descending order (so higher difference first)
        rem_sorted = dict(sorted(rem_counts.items(), key=lambda x: diff[x[0]], reverse=True))
        # print(rem_sorted)
        unfair = []
        for r, rcounts in rem_sorted.items():
            if rcounts >= fairness_criteria[r]:
                unfair.extend([r] * fairness_criteria[r])  # if multiple values have rem counts >= fairness counts, do I always take the one with the max rem counts?
                rem_counts[r] -= fairness_criteria[r]
                if rem_counts[r] == 0: del rem_counts[r]
            else:
                unfair.extend([r] * rcounts)
                del rem_counts[r]
                
        while len(unfair) != block_size:
            diff = {k: rem_counts.get(k, 0) - fairness_criteria.get(k, 0) for k in set(rem_counts) | set(fairness_criteria)}
            
            # Sort D by difference in descending order (so higher difference first)
            rem_sorted = dict(sorted(rem_counts.items(), key=lambda x: diff[x[0]], reverse=True))
            # print(rem_sorted)
            # input()
            for r, rcounts in rem_sorted.items():
                l = block_size - len(unfair)
                unfair.extend([r] * min(fairness_criteria[r], l, rem_counts[r]))
                # print(unfair)
                # print(rem_counts, rem_sorted)
                rem_counts[r] -= min(fairness_criteria[r], l, rem_counts[r])
                if rem_counts[r] == 0: del rem_counts[r]
        # print(rem_counts)        
        if rem_counts: temp_flag = False
        else: temp_flag = True
        unfairblocks.extend(unfair)    
        
    fairblocks = []
    for bits in unfairblocks[0:block_size]:
        if len(fairblocks) == block_size: break
        if bits in fairblocks: continue
        prev_bit = bits
        fairblocks.extend([prev_bit] * fairness_criteria[prev_bit])
        
    for bits in unique:
        if bits in fairblocks: continue
        fairblocks.extend([bits] * fairness_criteria[bits])
    
    fairblocks = fairblocks * m
    fairblocks.extend(unfairblocks)
    return fairblocks
        

