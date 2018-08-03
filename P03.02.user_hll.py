#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 04/05/2018
# Descr. : Counting url streaming 

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
import datetime

# Verify if the numbers of arguments are Ok.
if len(sys.argv) < 2:
   print('Invalid number of arguments!!!!')
   exit()

# Create spark context.
conf = SparkConf().setAppName("HW9.P03.02").setMaster("local[*]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)

# Function to parse the line.
def parse_log_line_w5(line):
    (uuid, timestamp, url, user) = line.strip().split(" ")
    hour = timestamp[0:13]
    return (user)

# Function to print the information.
def fprint(xrdd):
    # Getting the arguments value.
    sd = float(sys.argv[1])
    
    dt = datetime.datetime.now()
    print (dt.strftime("%Y-%m-%d %H:%M:%S"), xrdd.countApproxDistinct(sd))

# Getting info from directory input
#lines = ssc.textFileStream("file:///home/michelbenites/hw9/data_input")
lines = ssc.textFileStream("file:///home/hadoop/hw9/data_input")

# Set the log level to show only error.
sc.setLogLevel('ERROR')

# Window frame of 30 seconds, Map and reduce and print distinct count.
user = lines.window(30,1) \
       .map(parse_log_line_w5) \
       .foreachRDD(fprint)

# Start the streaming context.
ssc.start()
ssc.awaitTermination()
ssc.stop()
