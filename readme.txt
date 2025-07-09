1. Inside the current directory, create a python 3.9 (or higher) virtual environment. Then run the following line in the terminal.

$ pip install confluent-kafka pandas numpy matplotlib

2. Start the kafka setup (do not copy '$'): 
$ docker compose -f .\zk-single-kafka-single.yml up -d

3. Make the required changes in the configuration files (starting with "run") for producer and consumer
!!! CHECK THE TOPIC EVERYTIME YOU CHANGE THE ATTRIBUTE (FOR "AGE" MAKE THE TOPIC NAME HAVE AGE, FOR "GENDER" MAKE IT HAVE "GENDER")
    FOR BOTH THE CONSUMER AND PRODUCER IN THEIR CONFIG files
!! change the window size, block size (and max_windows if needed in the config file for the consumer)

4. Once you have enough metrics collected for a particular dataset, copy the metrics from the different files generated into
    one metric file (look at "metric_HDHI.csv" for instance) and then run the graph_plotter config file "run_plotter.py"
    You can mention the fixed window size, block size and cardinality (like Gender's cardinality is2, Age's cardinality is 5)
    and the name of the metric file where you brought all the data together from other metric files ("metric_HDHI.csv" for my
    instance)

5. Graphs will be generated varying window_size, block_size and cardinality in the figures folder.

6. Repeat this for all the datasets. 
For HDHI_Admission_data run on AGE, GENDER and OUTCOME
For Twitter dataset run on sentiment
For aapl dataset run on % Change

