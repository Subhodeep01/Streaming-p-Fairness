import subprocess
import time

window_size = 100
block_size = 10
cardinality = 5
metric_file = "metric_HDHI.csv"

command = [f'python graph_plotter.py --window_size={window_size} --block_size={block_size} --cardinality={cardinality} --metric_file={metric_file}']

plotter = subprocess.Popen(['cmd', '/c', command[0]])
