import subprocess
import time

window_size = int(input("Window size: "))
block_size = int(input("Block size: "))
topic_name = input("Please enter SAME topic name as producer!!!!!: ")
landmark = int(input("Landmark number: "))
max_windows = int(input("Max number of windows to monitor: "))


backward = False
brt_force = False


if window_size <= block_size or window_size%block_size!=0 or landmark > window_size or (landmark+window_size)%block_size!=0:
    print("Parameters are not compatible. Block size should be a factor of the sum of window size and landmark items!!!")
else:
    command = ['python', 'consumer_editable_performance.py', '--window_size',f'{window_size}', '--block_size', f'{block_size}', '--topic_name',f'{topic_name}', '--max_windows',f'{max_windows}', '--landmark',f'{landmark}', '--brt_force',f'{brt_force}', '--backward',f'{backward}']
    consumption = subprocess.run(command, shell=True)

# for window_size in window_sizes:
#     for block_size in block_sizes:
#         for landmark in landmarks:
#             if window_size <= block_size or window_size%block_size!=0 or landmark >= window_size or (landmark+window_size)%block_size!=0:
#                 continue
#             else:
                

#                 command = ['python', 'consumer_editable_performance.py', '--window_size',f'{window_size}', '--block_size', f'{block_size}', '--topic_name',f'{topic_name}', '--max_windows',f'{max_windows}', '--landmark',f'{landmark}', '--brt_force',f'{brt_force}', '--backward',f'{backward}']

#                 consumption = subprocess.run(command, shell=True)
