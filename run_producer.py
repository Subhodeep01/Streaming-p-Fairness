import subprocess
import time


topic_name = "hospital-raw-age-v3"


command = [f'python producer.py --topic_name={topic_name}']

production = subprocess.Popen(['cmd', '/c', command[0]], shell=True)

production.wait()