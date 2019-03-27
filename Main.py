# -*- coding: utf-8 -*-

import os
import sys


os.chdir("C:/Users/arora/Documents/spark-python")  
os.environ['SPARK_HOME'] = 'C:/Users/arora/spark-2.3.3-bin-hadoop2.7'

# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

#Add the following paths to the system path. 
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.10.7-src.zip"))

from pyspark.sql import Row
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("Nidhi") \
    .getOrCreate()
    
#Get the Spark Context from Spark Session    
sc = SpSession.sparkContext

#taking long time ,so unzipped manually
#import gzip
#path = r'C:/Users/arora/Documents/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz'
#with gzip.open(path) as f:
#    datalines = sc.textFile(f)
    
    
pathToTextFile = r'''C:\Users\arora\Documents\sparkcontent\2015_07_22_mktplace_shop_web_log_sample.log'''
datalines = sc.textFile(pathToTextFile)
datalines.count()

#created a dataframe
dataparts = datalines.map(lambda l: l.split(" "))
dataMap = dataparts.map(lambda p: Row(timestamp=p[0],elb_name=p[1],client_ip=p[2],back_end_ip=p[3],request_processing_time=p[4],\
         backend_processing_time=p[5], response_processing_time=p[6],elb_status_code=p[7],backend_status_code=p[8],\
        received_bytes=p[9],sent_bytes=p[10], request=p[11],user_agent=p[12],ssl_cipher=p[13],ssl_protocol=p[14]))

logDataFrame = SpSession.createDataFrame(dataMap)
logDataFrame.take(2)






