import subprocess
import time

window_size = 2000
block_size = 50
cardinality = 5
metric_file = "metric_combined.csv"

command = [f'python graph_plotter.py --window_size={window_size} --block_size={block_size} --cardinality={cardinality} --metric_file={metric_file}']

plotter = subprocess.Popen(['cmd', '/c', command[0]])
