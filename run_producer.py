import subprocess
import time


topic_name = input("Please enter topic name: ")


command = ['python', 'producer.py', '--topic_name',f'{topic_name}']

production = subprocess.Popen(command, shell=True)

production.wait()

