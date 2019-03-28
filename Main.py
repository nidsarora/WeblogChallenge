# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime

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
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import split
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.functions import lit
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import TimestampType,IntegerType
from pyspark.sql.functions import lag
from datetime import datetime


def time_delta(y,x): 
    from datetime import datetime
    end = datetime.strptime(y, '%Y-%m-%dT%H:%M:%S.%fZ')  
    start = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ')
    delta = (end-start).total_seconds()
    return delta

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("Nidhi") \
    .getOrCreate()
    
#Get the Spark Context from Spark Session    
sc = SpSession.sparkContext

pathToTextFile = r'''C:\Users\arora\WeblogChallenge\data\2015_07_22_mktplace_shop_web_log_sample.log.gz'''  
datalines = sc.textFile(pathToTextFile)
datalines.count()

#created a dataframe
dataparts = datalines.map(lambda l: l.split(" "))
dataMap = dataparts.map(lambda p: Row(timestamp_str=p[0],elb_name=p[1],client_ip=p[2],back_end_ip=p[3],request_processing_time=p[4],\
         backend_processing_time=p[5], response_processing_time=p[6],elb_status_code=p[7],backend_status_code=p[8],\
        received_bytes=p[9],sent_bytes=p[10], request=p[11],user_agent=p[12],ssl_cipher=p[13],ssl_protocol=p[14]))

logDataFrame = SpSession.createDataFrame(dataMap)
logDataFrame.take(2)

func =  udf (lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'), TimestampType())

split_col = split(logDataFrame['client_ip'], ':')
logDataFrame = logDataFrame.withColumn('IP', split_col.getItem(0))
logDataFrameWithIP = logDataFrame.withColumn('time_stamp', func(col('timestamp_str'))).cache()


logDataFrameWithIP.select("*").take(1)


logDataFrameIPTimeStamp = logDataFrameWithIP.withColumn('time_stamp_previous_str',
                        lag(logDataFrame.timestamp_str,1)
                                 .over(Window.partitionBy("IP").orderBy("time_stamp"))).cache()

logDataFrameIPTimeStamp.select('IP').show(2)

