import subprocess
import time

window_size = int(input("Window size: "))
block_size = int(input("Block size: "))
topic_name = input("Please enter SAME topic name as producer!!!!!: ")
max_windows = int(input("Max number of windows to monitor: "))

if window_size <= block_size or window_size%block_size!=0:
    print("Parameters are not compatible. Block size should be a factor of window size!!!")
else:
    command = ['python', 'consumer.py', '--window_size',f'{window_size}', '--block_size', f'{block_size}', '--topic_name',f'{topic_name}', '--max_windows',f'{max_windows}']
    consumption = subprocess.run(command, shell=True)

# for window_size in window_sizes:
#     for block_size in block_sizes:
        
#             continue
#         else:

            
