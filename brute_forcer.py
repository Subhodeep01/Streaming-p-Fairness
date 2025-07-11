import math
from collections import Counter
from more_itertools import distinct_permutations
# input = [1,0,2,1, 0,0,2,2, 0,1,0,1]

# fairness = [0.25, 0.5, 0.25]
# block_size = 4
# window_size = 8

def unique_permutations(lst):
    counter = Counter(lst)
    length = len(lst)

    def backtrack(path):
        if len(path) == length:
            yield tuple(path)
            return
        for num in counter:
            if counter[num] > 0:
                counter[num] -= 1
                path.append(num)
                yield from backtrack(path)
                path.pop()
                counter[num] += 1

    yield from backtrack([])

def brute_force(input_col, fairness_criteria, window_size, block_size):
    # global fairness
    unique = list(set(input_col))
    unique.sort()
    # position = {unique[i]: i for i in range(len(unique))}
    # fairness_criteria = {position[i]: int(fairness[i]*block_size) for i in unique}
    # print(fairness_criteria)
    max_fair_window = 0
    max_fair_blocks = 0
    ideal_stream = None
    gen = distinct_permutations(input_col)

    # Print first 5 lazy permutations
    for i, perm in enumerate(gen):
        # print(perm)
        # rec = [0] * len(position)
        fair_window = 0
        total_fair_blocks = 0
        current = 0
        already_visited = []
        while current+window_size <= len(perm):
            fair_block = 0
            # print(current, current+window_size, len(perm))
            for block in range(current, current+window_size, block_size):    
                # print(perm[block:block+block_size])
                if (block,block+block_size) in already_visited:
                    fair_block += 1
                    continue
                already_visited.append((block,block+block_size))
                count_dict = {}
                for i in unique:
                    count_dict[i] = perm[block:block+block_size].count(i)
                # count_zer = perm[block:block+block_size].count(0)
                # count_one = perm[block:block+block_size].count(1)
                # print(count_one, count_zer, x, y)
                    
                # if count_zer >= x and count_one >= y:
                
                flag = 0
                for i in unique:
                    if count_dict[i] >= fairness_criteria[i]:
                        flag += 1
                if flag == len(unique):
                    fair_block += 1
                    total_fair_blocks += 1
            # print(total_fair_blocks)
            if fair_block == int(window_size//block_size): fair_window += 1
            
            current += 1
        
        if max_fair_window <= fair_window:
            max_fair_window = fair_window
            if max_fair_blocks < total_fair_blocks:
                max_fair_blocks = total_fair_blocks
                ideal_stream = perm
                # print(max_fair_window, max_fair_blocks, ideal_stream)
                
        
        
        if max_fair_window == len(input_col) - window_size + 1: break
    # print(ideal_stream)
    return max_fair_window, max_fair_blocks, ideal_stream



# max_fair_blocks, max_fair_window, ideal_stream = brute_force(input, None, window_size, block_size)
# print(max_fair_blocks, max_fair_window, ideal_stream)