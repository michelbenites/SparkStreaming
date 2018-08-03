#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 04/05/2018
# Descr. : Counting url streaming 

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os

# Create spark context.
conf = SparkConf().setAppName("HW9.P02.01").setMaster("local[*]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)

# Function to parse the line.
def parse_log_line_w5(line):
    (uuid, timestamp, url, user) = line.strip().split(" ")
    hour = timestamp[0:13]
    return (url, 1)

# Defining the checkpoint directory
ssc.checkpoint("file:///home/michelbenites/hw9/check_p")

# Getting info from directory input
lines = ssc.textFileStream("file:///home/michelbenites/hw9/data_input")

# Set the log level to show only error.
sc.setLogLevel('ERROR')

# Map and reduce.
lines.window(5, 1)\
    .map(parse_log_line_w5)\
    .reduceByKey(lambda x, y: x + y)\
    .pprint()

# Start the streaming context.
ssc.start()
ssc.awaitTermination()
ssc.stop()
