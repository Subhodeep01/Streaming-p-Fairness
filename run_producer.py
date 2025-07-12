import subprocess
import time


topic_name = "hospital-raw-gender-f1"


command = ['python', 'producer.py', '--topic_name',f'{topic_name}']

production = subprocess.Popen(command, shell=True)

production.wait()

production2 = subprocess.Popen(command, shell=True)

production2.wait()

production3 = subprocess.Popen(command, shell=True)

production3.wait()

production4 = subprocess.Popen(command, shell=True)

production4.wait()

production5 = subprocess.Popen(command, shell=True)

production5.wait()

production = subprocess.Popen(command, shell=True)

production.wait()

production2 = subprocess.Popen(command, shell=True)

production2.wait()

production3 = subprocess.Popen(command, shell=True)

production3.wait()

production4 = subprocess.Popen(command, shell=True)

production4.wait()

production5 = subprocess.Popen(command, shell=True)

production5.wait()