import math 
from itertools import combinations, permutations, product
from copy import deepcopy
from collections import Counter

# input = [0,0,0,0,0,1,0,0,0,0,0,1,0,0,0,0,0,0,1,0]
input = [0,0,1,1,0,1,0,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1,
         0,1,1,1,0,0,0,0,0,1,1,1,0,0,1,1,0,0,1,0,1,0,
         1,1,0,0,1,0,0,0,0,0,0,0,1,1,1,1,0,0,1,0,1,0,
         0,1,0,0,0,1,1,1,0,1,0,0,1,1,0,0,1,0,1,1,0,1,
         0,0,1,1,0,0,1,0,0,0,1,0,0,0,1,1,0]
# input = [0,1,0,0,1,1,0,1,0,0,0,1,0,0,0,0,1,1,1,0]
input = [1,1,1,0,0,1,1,1,0,0,0,0,0,1,0,0,0,1,0,0]
# b_size = 20
b_size = 5
# win_size = 80
win_size = 15
timeout = 5
eps = 0.8
fairness = 2
fairness1 = math.floor(fairness * eps)



def makefair(window):
    # print(len(window))
    sketch = compute_sketch(window)
    # print(sketch)
    i = 0
    count = window.count(1)
    while i < win_size:
        if i == 0: block_start = 0
        else: block_start = sketch[i-1]
        diff = sketch[i+b_size-1] - block_start
        if diff >= fairness1:
            i += b_size
        else:
            if i+b_size == win_size or count < fairness1:
                return window, i
            # print(f"Unfair window before at block {math.floor((i+1)/b_size),} index {i}: {window}")
            internal_swap(window, sketch, i, diff)
            # print(f"Fair window after internal swap at block {math.floor((i+1)/b_size)}: {window}")
            # print(f"Old sketch: {sketch}")
            sketch = compute_sketch(window)
            # print(f"New sketch: {sketch}")
            count = window[i+b_size:].count(1)
    # i = i if i < win_size else i - b_size
    return window, i
        
# def compute_sketch(window, size= win_size):
#     sketch = []
#     # print(window, win_size)
#     for i in range(0,size):
#         if len(sketch) == 0: sketch.append(window[i])
#         else: sketch.append(sketch[-1]+ window[i])
#     return sketch

def internal_swap(window, sketch, i, diff):
    rem_win = window[i+b_size:]
    rem_sketch = sketch[i+b_size:]
    # print(rem_win, rem_sketch)
    indices_z = [k+i for k, value in enumerate(window[i:i+b_size]) if value == 0] # figure out where the indices are zero in the given block [i,i+bsize]
    fair = False
    j = 0
    while fair != True: # keep on iterating if fair block is not found
        if j == 0: block_start = 0
        else: block_start = rem_sketch[j-1]
        if rem_sketch[j+b_size-1] - block_start >= fairness1:
            fair = True
            indices_o = [k+j+i+b_size for k, value in enumerate(rem_win[j:j+b_size]) if value == 1] # figure out for the next fair block [j, j+bsize] where the indices are one 
            break
        else:
            j += b_size 
    # print(indices_o, indices_z, diff)
    while diff < fairness1 and indices_o:
        index_z = indices_z.pop(0)
        index_o = indices_o.pop()
        temp = window[index_z]
        window[index_z] = window[index_o]
        window[index_o] = temp
        diff += 1

def count_deficit(input, bit_effect, ):
    bit_deficit = {}
    min_deficit = fairness1
    for bit, affected in bit_effect.items():   # Tn = O(number of bits in unfair part)
        deficit = 0
        window_num = 0
        for window in affected:     # Tn = O(number of timeout bits or windows)
            which_block, pos = window
            rem = pos % b_size
            # block_end = pos + (b_size - rem)
            # block_start = pos - rem
            block_end = pos + b_size 
            block_start = pos
            # print(block_start, block_end)
            # if block_end > win_size:
            #         block_end = win_size
            get_counts = dict(Counter(input[block_start:block_end]))
            get_counts2 = dict(Counter(input[block_start:block_end]))
            if 1 in get_counts:
                if get_counts[1] < fairness1:
                    deficit += (fairness1 - get_counts[1])
                    # print(f"Window {window_num} block {which_block}: needs {fairness1 - get_counts[1]} number of 1s")
            else:
                deficit += fairness1
                # print(f"Window {window_num} block {which_block}: needs {fairness1} number of 1s")

            # Check for minimum deficit across all the blocks of all the windows, excluding the timeout bits
            if 1 in get_counts2:
                if min_deficit > fairness1 - get_counts2[1]>0 :
                        min_deficit = fairness1 - get_counts2[1]
            window_num += 1
        bit_deficit[bit] = deficit
    
    return bit_deficit, min_deficit

def deficitsum(input, first_unfair, timeout):
    bit_effect = {}
    # i = first_unfair
    # start_off = int(first_unfair/b_size)
    for bit in input[first_unfair:-timeout]:  # Tn = O(number of bits in the unfair part of the window)
        if bit == 0:  # We dont look at bits that are already set 1
            bit_effect[first_unfair] = []
            for window in range(timeout+1):   # Tn = O(number of timeout bits or windows)
                pos = window
                which_block = 0
                while(pos+b_size <= first_unfair):  # Tn = O(log base(b_size) (total bits in prior fair blocks))
                    pos += b_size
                    which_block += 1
                bit_effect[first_unfair].append((which_block, pos))
        first_unfair += 1
    # print(bit_effect)
    bit_deficit, min_deficit = count_deficit(input, bit_effect)
    
    # print(bit_deficit, min_deficit)
    return bit_effect, bit_deficit, min_deficit
            
def wtd_deficitsum(input, first_unfair, timeout):
    bit_effect = {}
    # i = first_unfair
    # start_off = int(first_unfair/b_size)
    for bit in input[first_unfair:-timeout]:
        if bit == 0 or bit == 1:
            bit_effect[first_unfair] = []
            for window in range(timeout+1):
                pos = window
                which_block = 0
                while(pos+b_size <= first_unfair):
                    pos += b_size
                    which_block += 1
                bit_effect[first_unfair].append((which_block, pos))
        first_unfair += 1
    print(bit_effect)
    
    bit_deficit = {}
    for bit, affected in bit_effect.items():
        deficit = 0
        window_num = 0
        for window in affected:
            which_block, pos = window
            get_counts = dict(Counter(input[pos:pos+b_size]))
            if 1 in get_counts:
                if get_counts[1] < fairness1:
                    deficit += (fairness1 - get_counts[1])*(timeout - window_num)
            else:
                deficit += fairness1*(timeout - window_num)
            window_num += 1
        bit_deficit[bit] = deficit
    
    print(bit_deficit)
    return bit_effect, bit_deficit

# def compute_wins(temp_input):
#     sketch = compute_sketch(temp_input, len(temp_input))
#     # for i in range(0,win_size+timeout):
#     #     if len(sketch) == 0: sketch.append(temp_input[i])
#     #     else: sketch.append(sketch[-1]+ temp_input[i])
#     unfair_blocks = 0
#     totalwait = 0
#     for j in range(0, timeout+1):
#         # print(f"window {j} being checked: ")
#         for i in range(0+j, win_size+j, b_size):
#             if i == 0: block_start = 0
#             else: block_start = sketch[i-1]
#             # if 3 >= sketch[i+b_size-1] - block_start >= 2:
#             if  sketch[i+b_size-1] - block_start >= fairness1:
#                 pass
#                 # print(f"Block {math.floor((i+1-j)/b_size)} fair")
#             else:
#                 # print(f"Block {math.floor((i+1-j)/b_size)} not fair")
#                 unfair_blocks += 1
#                 totalwait += timeout - j
#     # print(f"For input {temp_input}, total unfair blocks = {unfair_blocks}, total wait time = {totalwait}")
#     return temp_input, unfair_blocks, totalwait

def swap(ip, bit, pos):
    temp = ip[bit]
    ip[bit] = ip[pos]
    ip[pos] = temp
    # print(ip)

def greedy_swapper_func(temp_input, bitdeficit, biteffect, min_deficit, onepos):
        get_count = dict(Counter(temp_input[-timeout:]))

        if 1 not in get_count:
            print("No 1s even in the timeout")
            return
        elif get_count[1] < min_deficit:
            print("Not enough 1s in timeout")
            return

        # Finding total number of blocks that can be made fair 
        
        bit_satisfy = {}
        # print("Bit effect: ", biteffect)
        for bit, aff in biteffect.items():
            fair_blocks = 0
            window_num = 0
            # print("Bit: ", bit)
            for windows in aff:
                which_block, pos = windows
                rem = pos % b_size
                # block_end = pos + (b_size - rem)
                # block_start = pos - rem
                block_end = pos + b_size 
                block_start = pos
                # if block_end > win_size:
                #     block_end = win_size
                # if block_end > win_size:
                # for one_index in onepos:
                #         if one_index > block_end: # only those ones in timeout are stored that are beyond the current block
                #             if bit not in swappables.keys():
                #                 swappables[bit] = [one_index] 
                #             else: swappables[bit].append(one_index)
                # print("Swappable ones for bit ", bit, " in window ", window_num, " are ", swappables[bit])        
                get_counts = dict(Counter(temp_input[block_start:block_end]))  # We exclude the timeout bits in this satisfaction calculation
                swappables = [ones for ones in onepos if ones > block_end]
                # print(block_end, swappables)
                # print(windows, pos, block_end, temp_input[block_start:block_end], get_counts)
                if 1 in get_counts:
                    if fairness1 - get_counts[1] < min_deficit:
                        fair_blocks += 1
                    elif fairness1 - get_counts[1] == min_deficit:
                        if len(swappables) >= min_deficit:
                            fair_blocks += 1
                    
                window_num += 1
            bit_satisfy[bit] = fair_blocks

        print("Which bit satisfies how many blocks based on minimum deficit ", bit_satisfy) 
                        

        # This part deals with finding the bit changing which to 1 will maximally reduce the unfair blocks
        temp = max(bit_satisfy.values())
        max_list = [key for key in bit_satisfy if bit_satisfy[key] == temp]
        # print(temp, max_list)
        # max_bit = max(bit_satisfy, key = bit_satisfy.get)
        max_bit = 0
        max_bit_def = 0
        for keys in max_list:
            
            if bitdeficit[keys] >  max_bit_def:
                max_bit_def = bitdeficit[keys]
                max_bit = keys
        print(bitdeficit)
        print("Selected bit: ", max_bit)
        # Greedily set that bit to one depeding on the available ones in the timeout bit
        # print(onepos)
        swap(temp_input, max_bit, onepos.pop())
        return temp_input
        
def brute_force(temp_input, first_unfair, onepos, min_ones_req, timeout):
    l = len(onepos)
    # print(min_ones_req)
    if l == 0:
        error = "No 1s even in the timeout"
        return error
    elif l < min_ones_req:
        error = "Not enough 1s in timeout"
        print("Minimum 1s required to make window fair: ", min_ones_req)
        print("Number of 1s in timeout: ", l)
        return error
    # print(first_unfair)
    while(first_unfair!=win_size):
        # print(onepos)
        zeropos_og = [(indices+first_unfair) for indices, values in enumerate(temp_input[first_unfair:first_unfair+b_size]) if values == 0]
        print(zeropos_og)
        tot_zeropos = len(zeropos_og)
        get_counts = dict(Counter(temp_input[first_unfair:first_unfair+b_size]))
        deficit = fairness1 - get_counts[1] if 1 in get_counts else fairness1
        deficit = int(deficit)
        combos = list(combinations(zeropos_og, deficit))
        pos_list = []
        considered = []
        print(combos)
        considered, unfair, min_totalwait = compute_wins(temp_input)
        # print(f"For input {considered}, total unfair blocks = {unfair}, total wait time = {min_totalwait}")
        for combo in combos:
            temp_onepos = deepcopy(onepos)
            temp_input2 = deepcopy(temp_input)
            # print(combo)
            for i in range(len(combo)):
                
                swap(temp_input2, combo[i], temp_onepos.pop())
                # print(combo[i], onepos[i], temp_input2)
                
            ip, unfair2, totalwait = compute_wins(temp_input2)
            print(combo, unfair2, totalwait)
            if unfair2 < unfair:
                considered = ip
                unfair = unfair2
                min_totalwait = totalwait
                print(f"Swapped pos: {combo}")
            elif unfair2 == unfair:
                if totalwait < min_totalwait:
                    considered = ip
                    unfair = unfair2
                    min_totalwait = totalwait
                    print(f"Swapped pos: {combo}")
        
        mod_input, first_unfair = makefair(considered[:win_size])
        temp_input = mod_input + considered[win_size:]
        onepos = [(indices+win_size) for indices, values in enumerate(temp_input[-timeout:]) if values == 1]
        # print(temp_input, first_unfair, onepos)
    print(f"Using BruteForce: Final Considered Window {considered}, total unfair blocks = {unfair}, total wait time = {min_totalwait}")
    return None

    # while(first_unfair != win_size):
    #     zeropos = [(indices+first_unfair) for indices, values in enumerate(temp_input[first_unfair:first_unfair+b_size]) if values == 0]
    #     swap(temp_input, zeropos.pop(0), onepos.pop(0))
        
    # compute_wins(temp_input)

def min_ones_func(input, win_size, first_unfair):
    deficit = 0
    while(first_unfair != win_size):
        get_counts = dict(Counter(input[first_unfair:first_unfair+b_size]))
        print(get_counts, fairness1)
        deficit += fairness1 - get_counts[1] if 1 in get_counts else fairness1
        first_unfair += b_size
        deficit = int(deficit)
    print("Here", deficit)
    return deficit


print("Initial input stream: ", input)
streamlen = len(input)
for win_start in range(0, streamlen - (win_size + timeout)):
    # print(win_start)
    if win_start == 5:
        break
    print("Window before any necessary internal swaps: ", input[win_start:win_start+win_size])
    mod_input, first_unfair = makefair(input[win_start:win_start+win_size])
    considered = mod_input
    print("Window after any necessary internal swaps: ", mod_input)
    mod_input.extend(input[win_start+win_size:win_start+win_size+timeout])
    # print(len(mod_input), mod_input)
    # print(first_unfair)
    
    if first_unfair == win_size:
        print(f"Window {win_start} already fair")
        # print(mod_input[:win_size])
    else:
        print(f"Window {win_start} unfair")
        min_ones_req = min_ones_func(mod_input, win_size, first_unfair)
        onepos = [(indices+win_size) for indices, values in enumerate(mod_input[-timeout:]) if values == 1]
        print("pos of ones in timeout", onepos, mod_input[-timeout:])
        temp_input = deepcopy(mod_input)
        error = brute_force(mod_input, first_unfair, onepos, min_ones_req, timeout)
        if error != None:
            print(error)
        else:
            while(len(onepos) > 0 and first_unfair != win_size):
                print("First unfair index: ", first_unfair)
                biteffect, bitdeficit, min_deficit = deficitsum(temp_input, first_unfair, timeout)
                # print("Bits affected, bit deficit for each bit and minimum deficit a block has respectively: ", biteffect, bitdeficit, min_deficit)
                total_deficit = sum(bitdeficit.values())
                # print("Total deficit: ", total_deficit)
                print(onepos)
                temp_input2 = greedy_swapper_func(temp_input, bitdeficit, biteffect, min_deficit, onepos)
                print(onepos)
                considered, unfair, totalwait = compute_wins(temp_input2)
                # print(f"Considered input {considered}, total unfair blocks = {unfair}, total wait time = {totalwait}")
                mod_input, first_unfair = makefair(considered[:win_size])
                temp_input = mod_input + temp_input2[win_size:]
                # print(first_unfair)
            print(f"Using GreedySwapper: Final Considered stream {considered}, total unfair blocks = {unfair}, total wait time = {totalwait}")
    # print(len(considered))
    input = input[:win_start] + considered + input[win_start+win_size+timeout:] # Update the input stream with the considered window and the timeout bits
    print("Updated input stream: ", input, len(input))
