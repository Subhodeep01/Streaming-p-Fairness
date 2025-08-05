How to get started:

1. Git clone the repository first. It is highly suggested to open the directory in VScode.
Inside the current directory, create a python 3.9 (or higher) virtual environment. Then run the following line in the terminal(do not copy '$').

$ pip install confluent-kafka pandas numpy matplotlib more_itertools

2. Install docker desktop in your system and keep it running in the background. Start the kafka setup by running the following line in terminal(do not copy '$'): 
$ docker compose -f .\zk-single-kafka-single.yml up -d

3. Once the kafka container is installed and running inside a docker container, we can now start producing and consuming data streams.

4. Before moving to production and consumption it is essential to do the following things.
    a. Create a directory "datasets" under the project directory and place all the datasets inside the directory. Datasets can be downloaded from {link}.
    b. Create a directory "metrics". All the performance metrics of the session will be stored there for a particular set of parameters and a particular dataset.
    
4. Run the following command:
$ python run_producer.py 
[!! If an error is thrown like missing module or something else when using the above command (usually the case when not using VScode), run this directly as a failsafe:
$ python producer.py --topic_name #some_topic_name
Replace #some_topic_name with the topic name we would like.]

We will be prompted for the topic name (the place where the producer publishes the data items). We need to make sure to change it everytime we decide to monitor another dataset or a new attribute from the same dataset.
Then proceed with the following inputs as prompted.
(Make sure you provide the correct value 0/1 in is_discrete. 1 means YES, 0 means NO.)
NOTE:
!! If any mistake is comitted while inputting the data after topic name has been given, it is best to terminate as soon as we realize it.
!! If the production has already started, it is best to run the command again and this time produce correctly in a new topic.


6. Next, run the following command to run the read-only version of our framework:
$ python run_consumer_readable.py 
[!! If an error is thrown like missing module or something else when using the above command (usually the case when not using VScode), run this directly as a failsafe:
$ python consumer.py --window_size #window_size --block_size #block_size --topic_name #some_topic_name --max_windows #max_windows
Replace #some_topic_name with the topic name we would like and input the window size, block size and max windows to monitor in place of #window_size, #block_size, #max_windows, respectively]

Proceed with the following inputs as prompted.
!!! MAKE SURE TO KEEP THE TOPIC NAME SAME AS IN THE PRODUCER 
!! Make sure block size is a factor of window size.
!! When prompted for fairness, provide the counts of the given values and not percentage or fraction. For example when prompted to enter the fairness of F in a block of size 20, provide the count 10. And for M provide 10 as well.
!!! Make sure the counts provided add up to block size. Also make sure NONE of the values have a 0 count otherwise the code will crash.
The results will be printed in terminal and performance metrics for the session will be stored in the metrics directory.


7. Now, run the following command to run the reorder version of our framework:
$ python run_consumer_editable.py 
[!! If an error is thrown like missing module or something else when using the above command (usually the case when not using VScode), run this directly as a failsafe:
$ python consumer_editable_performance.py --window_size #window_size --block_size #block_size --topic_name #some_topic_name --max_windows #max_windows --landmark #landmark --brt_force False --backward False
Replace #some_topic_name with the topic name we would like and input the window size, block size, max windows and landmark number to monitor in place of #window_size, #block_size, #max_windows, #landmark respectively]

Proceed with the following inputs as prompted.
!!! MAKE SURE TO KEEP THE TOPIC NAME SAME AS IN THE PRODUCER 
!! Make sure block size is a factor of window size+landmark and landmark does not exceed window size.
!! When prompted for fairness, provide the counts of the given values and not percentage or fraction. For example when prompted to enter the fairness of F in a block of size 20, provide the count 10. And for M provide 10 as well.
!!! Make sure the counts provided add up to block size. Also make sure NONE of the values have a 0 count otherwise the code will crash.
The results will be printed in terminal and performance metrics for the session will be stored in the metrics directory.


