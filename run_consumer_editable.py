import subprocess
import time

window_sizes = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000]
# block_sizes = [5, 10, 25, 50, 100, 250, 500, 1000, 2500]
block_sizes = [25, 250]
topic_name = "hospital-raw-gender-v4"
max_windows = 500

for window_size in window_sizes:
    for block_size in block_sizes:
        if window_size <= block_size or window_size%block_size!=0:
            continue
        else:
            landmark = block_size

            command = ['python', 'consumer_editable.py', '--window_size',f'{window_size}', '--block_size', f'{block_size}', '--topic_name',f'{topic_name}', '--max_windows',f'{max_windows}', '--landmark',f'{landmark}']

            consumption = subprocess.run(command, shell=True)
