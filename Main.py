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

logDataFrameIPTimeStamp = logDataFrameWithIP.withColumn('time_stamp_previous',
                        lag(logDataFrameWithIP.time_stamp,1)
                                 .over(Window.partitionBy("IP").orderBy("time_stamp"))).cache()

logDataFrameIPTimeStamp.select('IP').show(2)


#logDataFrameIPTimeStamp = logDataFrameIPTimeStamp.withColumn(
#    "time_diff_in_secs", 
#    (F.col("time_stamp").cast("long") - F.col("time_stamp_previous").cast("long"))
#)

logDataFrameIPTimeStamp = logDataFrameIPTimeStamp.withColumn(
    "time_diff_in_secs", 
    unix_timestamp("time_stamp") - unix_timestamp("time_stamp_previous")
)

logDataFrameIPTimeStamp.select('*').show(100)
logDataFrameIPTimeStamp.createOrReplaceTempView("log_session")
SpSession.sql("select IP,time_diff_in_secs from log_session where time_diff_in_secs>900 order by IP, time_stamp ").show()




#logDataFrameIPTimeStamp.select('user_agent').distinct().show(10)
#
logDataFrameSession = logDataFrameIPTimeStamp.select(F.when(logDataFrameIPTimeStamp.time_diff_in_secs > 900, lit(1)).otherwise(lit(0)).alias("new_session"))
logDataFrameSession.select('*').show(3)


logDataFrameSession = logDataFrameIPTimeStamp.withColumn('new_session',F.when(((logDataFrameIPTimeStamp.time_diff_in_secs > 900) | (logDataFrameIPTimeStamp.time_diff_in_secs.isNull())), lit(1)).otherwise(lit(0)))
logDataFrameSession.createOrReplaceTempView("log_new_session")
SpSession.sql("select IP,time_diff_in_secs,new_session from log_new_session").show()

  
logDataFrameSessionId = SpSession.sql("select *,(SUM(new_session) OVER (PARTITION BY IP ORDER BY IP,time_stamp)) as session_id from log_new_session")
logDataFrameSessionId.createOrReplaceTempView("log_new_session_id")
SpSession.sql("select IP,new_session,session_id,time_stamp,time_diff_in_secs,time_stamp_previous from log_new_session_id order by IP,time_stamp").show(5)

#Q1 i.e the sessions are represented by logDataFrameSessionId, Every IP has its own session ids

SpSession.sql("select * from log_new_session_id where IP is null").count()
#So, null IPs are there

###################################Q2###############################################

logDataFrameSessionTime = SpSession.sql("select IP,session_id,sum(time_diff_in_secs) as session_time from log_new_session_id group by IP,session_id order by IP,session_id")
logDataFrameSessionTime.createOrReplaceTempView("log_session_time")
SpSession.sql("select Avg(session_time) as avg_session_time from log_session_time").show()
#Average session time is 2625.0743308697874 seconds

logUniqueVisits = SpSession.sql("select IP,session_id,count(*) as unique_url_count from ( select distinct IP,session_id, user_agent from log_new_session_id )group by IP,session_id order by IP,session_id")
logUniqueVisits.show()






             


