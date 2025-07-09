import subprocess
import time


topic_name = "hospital-raw-gender-v3"


command = [f'python producer.py --topic_name={topic_name}']

production = subprocess.Popen(['cmd', '/c', command[0]], shell=True)

production.wait()

production2 = subprocess.Popen(['cmd', '/c', command[0]], shell=True)

production2.wait()

production3 = subprocess.Popen(['cmd', '/c', command[0]], shell=True)

production3.wait()