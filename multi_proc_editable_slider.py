import math
from collections import deque, Counter
import copy

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
        # diff = {k: rem_counts.get(k, 0) - fairness_criteria.get(k, 0) for k in set(rem_counts) | set(fairness_criteria)}
        # print(diff)
        EP_dict = {k: min(fairness_criteria[k], rem_counts.get(k, 0)) for k in set(rem_counts) | set(fairness_criteria)}
        EP_dict =  dict(sorted(EP_dict.items(), key=lambda x: EP_dict[x[0]], reverse=True))
        print(EP_dict)
        form_EP = [key for key, count in EP_dict.items() for _ in range(count)]
        print(form_EP)
        IBC_counts = {k: fairness_criteria.get(k, 0) - EP_dict.get(k, 0) for k in set(EP_dict) | set(fairness_criteria)}
        print(IBC_counts)
        form_IBC = copy.deepcopy(form_EP)
        form_IBC.extend([key for key, count in IBC_counts.items() for _ in range(count)])
        print(form_IBC)
        rem_counts = {k: rem_counts.get(k, 0) - EP_dict.get(k, 0) for k in set(rem_counts) | set(EP_dict)}
        print(rem_counts)
        EStream = form_IBC * m + form_EP + [key for key, count in rem_counts.items() for _ in range(count)]
        print(EStream)
        print(EStream[::-1])
        return EStream[::-1]
        # Sort D by difference in descending order (so higher difference first)
    #     rem_sorted = dict(sorted(rem_counts.items(), key=lambda x: diff[x[0]], reverse=True))
    #     # print(rem_sorted)
    #     unfair = []
    #     for r, rcounts in rem_sorted.items():
    #         if rcounts >= fairness_criteria[r]:
    #             unfair.extend([r] * fairness_criteria[r])  # if multiple values have rem counts >= fairness counts, do I always take the one with the max rem counts?
    #             rem_counts[r] -= fairness_criteria[r]
    #             if rem_counts[r] == 0: del rem_counts[r]
    #         else:
    #             unfair.extend([r] * rcounts)
    #             del rem_counts[r]
                
    #     while len(unfair) != block_size:
    #         diff = {k: rem_counts.get(k, 0) - fairness_criteria.get(k, 0) for k in set(rem_counts) | set(fairness_criteria)}
            
    #         # Sort D by difference in descending order (so higher difference first)
    #         rem_sorted = dict(sorted(rem_counts.items(), key=lambda x: diff[x[0]], reverse=True))
    #         # print(rem_sorted)
    #         # input()
    #         for r, rcounts in rem_sorted.items():
    #             l = block_size - len(unfair)
    #             unfair.extend([r] * min(fairness_criteria[r], l, rem_counts[r]))
    #             # print(unfair)
    #             # print(rem_counts, rem_sorted)
    #             rem_counts[r] -= min(fairness_criteria[r], l, rem_counts[r])
    #             if rem_counts[r] == 0: del rem_counts[r]
    #     # print(rem_counts)        
    #     if rem_counts: temp_flag = False
    #     else: temp_flag = True
    #     unfairblocks.extend(unfair)    
        
    # fairblocks = []
    # for bits in unfairblocks[0:block_size]:
    #     if len(fairblocks) == block_size: break
    #     if bits in fairblocks: continue
    #     prev_bit = bits
    #     fairblocks.extend([prev_bit] * fairness_criteria[prev_bit])
        
    # for bits in unique:
    #     if bits in fairblocks: continue
    #     fairblocks.extend([bits] * fairness_criteria[bits])
    
    # fairblocks = fairblocks * m
    # fairblocks.extend(unfairblocks)
    # return fairblocks
        
# input = ['M' for i in range(5)] + ['F' for i in range(15)]
# win_len = 15
# slide = 5
# block_len = 5

# fairness = {'M': 2, 'F':3}
# m, rem_counts, unique = max_rep_blocks(input, block_len, win_len, fairness)
# print(m, rem_counts, unique, fairness)
# output = build_max_rep(m, rem_counts, unique, block_len, fairness)
