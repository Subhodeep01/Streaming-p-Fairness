import subprocess
import time

window_size = 1000
block_size = 5
topic_name = "hospital-raw-age-v3"
max_windows = 500

command = [f'python consumer.py --window_size={window_size} --block_size={block_size} --topic_name={topic_name} --max_windows={max_windows}']

consumption = subprocess.Popen(['cmd', '/c', command[0]], shell=True)

consumption.wait()