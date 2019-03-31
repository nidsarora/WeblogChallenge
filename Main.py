# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace import sarimax
from statsmodels.tsa.stattools import acf, pacf
from statsmodels.graphics.tsaplots import plot_acf,plot_pacf
from pandas import Series
from statsmodels.tsa.arima_model import ARIMA 
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

#import re
#def parse_log_line(logline):
#    match = re.search(ACCESS_LOG_PATTERN, logline)
##     if match is None:
##         raise Error("Invalid logline: %s" % logline)
#    return Row(
#        timestamp_str    = match.group(1),
#        elb_name = match.group(2),
#        client_ip       = match.group(3),
#        back_end_ip = match.group(4),
#        request_processing_time = match.group(5),
#        backend_processing_time  =match.group(6),
#        response_processing_time = match.group(7),
#        elb_status_code      = match.group(8),
#        backend_status_code = match.group(9),
#        received_bytes  = match.group(10),
#        sent_bytes = match.group(11),
#        request = match.group(12),
#        url = match.group(13),
#        http_v = match.group(14),
#        user_agent = match.group(15),
#        ssl_cipher = match.group(16),
#        ssl_protocol = match.group(17)         
#    )
#    
#from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)
#access_logs = (sc.textFile(logFile)
#               .map(parse_log_line)
#               .cache())
#access_logs.collect().count()

#df = pd.read_csv(pathToTextFile, sep=" ", names=["timestamp_str","elb_name","client_ip","back_end_ip","request_processing_time","backend_processing_time",\
#           "response_processing_time","elb_status_code","backend_status_code","received_bytes","sent_bytes",\
#           "request","user_agent","ssl_cipher","ssl_protocol"])
#df['user_agent'] = df['user_agent'].astype(str)
#df['url'] = df['request'].astype(str).str.split().str[1]
#df['IP']=df['client_ip'].astype(str).str.split().str[0]
#DataFramePrediction = SpSession.createDataFrame(df)



# logDataFrame = SpSession.createDataFrame(access_logs)
pathToTextFile = r'''C:\Users\arora\WeblogChallenge\data\2015_07_22_mktplace_shop_web_log_sample.log.gz'''  
datalines = sc.textFile(pathToTextFile)
datalines.count()



#created a dataframe
dataparts = datalines.map(lambda l: l.split(" "))
dataMap = dataparts.map(lambda p: Row(timestamp_str=p[0],elb_name=p[1],client_ip=p[2],back_end_ip=p[3],request_processing_time=p[4],\
         backend_processing_time=p[5], response_processing_time=p[6],elb_status_code=p[7],backend_status_code=p[8],\
        received_bytes=p[9],sent_bytes=p[10], request=p[11],url =p[12],http_v= p[13],user_agent=p[14],ssl_cipher=p[15],ssl_protocol=p[16]))

dataMap.count()


logDataFrame = SpSession.createDataFrame(dataMap)
logDataFrame.show(2)

func =  udf (lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'), TimestampType())

func2 =  udf (lambda x: x.replace(microsecond=0), TimestampType())


split_col = split(logDataFrame['client_ip'], ':')
logDataFrame = logDataFrame.withColumn('IP', split_col.getItem(0))
logDataFrameWithIP = logDataFrame.withColumn('time_stamp', func(col('timestamp_str'))).cache()

logDataFrameWithIPNoMicroSecs = logDataFrameWithIP.withColumn('time_stamp_without_microsecs', func2(col('time_stamp'))).cache()
logDataFrameWithIPNoMicroSecs.take(1)
logDataFrameWithIPNoMicroSecs.createOrReplaceTempView("df_time_stamp_without_microsecs")

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
logDataFrameSessionTime.show()
logDataFrameSessionTime.createOrReplaceTempView("log_session_time")
SpSession.sql("select Avg(session_time) as avg_session_time from log_session_time").show()
#Average session time is 2625.0743308697874 seconds

###################################Q3###############################################

SpSession.sql("select distinct IP,session_id, request from log_new_session_id").take(1)
logUniqueVisits = SpSession.sql("select IP,session_id,count(*) as unique_url_count from ( select distinct IP,session_id, url from log_new_session_id )group by IP,session_id order by IP,session_id")
logUniqueVisits.show()

###################################Q4###############################################

SpSession.sql("select IP,session_time from log_session_time  where session_time is not null order by session_time desc").show()


#ML Data prep

SpSession.sql("select time_stamp_without_microsecs from df_time_stamp_without_microsecs order by time_stamp_without_microsecs asc").take(10)
requestsPrepDf = SpSession.sql("select time_stamp_without_microsecs, count(*) as requests_per_sec from df_time_stamp_without_microsecs group by time_stamp_without_microsecs order by time_stamp_without_microsecs")
SpSession.sql("select min(time_stamp_without_microsecs),max(time_stamp_without_microsecs) from df_time_stamp_without_microsecs").show(5)
requestsPrepDf.createOrReplaceTempView("df_requests_prep")
SpSession.sql("select time_stamp_without_microsecs,requests_per_sec from df_requests_prep").count()
requestsPredDf = SpSession.sql("select requests_per_sec from df_requests_prep")
df_requests_pd = requestsPredDf.toPandas()

acf = plot_acf(df_requests_pd[180:240])
acf.savefig(r'''C:\Users\arora\WeblogChallenge\images\acf.png''')
acf = plot_pacf(df_requests_pd[180:240])
acf.savefig(r'''C:\Users\arora\WeblogChallenge\images\pacf.png''')

#to check stationarity
def difference(dataset, interval=1):
	diff = list()
	for i in range(interval, len(dataset)):
		value = dataset[i] - dataset[i - interval]
		diff.append(value)
	return Series(diff)

diff = df_requests_pd.values.mean()
diff_d1 = difference(df_requests_pd.values,1).mean()#value of d=1 gives stationarity and minimum mean
diff_d2 = difference(df_requests_pd.values,2).mean()

df_requests_pd =df_requests_pd['requests_per_sec']
train = df_requests_pd[2289:4209]
test = df_requests_pd[:-60]

history=[x for x in train]

model = ARIMA(history,order=(5, 1, 4))
model_fit = model.fit(disp=0)
yhat = model_fit.forecast(60)
output = yhat[0]


predictions_df = pd.DataFrame(list(output))
original_df = pd.DataFrame(list(test))
timeseries_forecasts_Df = pd.concat([original_df,predictions_df],axis=1,keys=['Original','Predicted'])

#for t in range(len(test)):
#    model = ARIMA(history,order=(5, 1, 4))
#    model_fit = model.fit(disp=0)
#    output = model_fit.forecast(60)
    


model = sarimax.SARIMAX(history,order=(1, 1, 1), seasonal_order=(1, 1, 1,60),enforce_stationarity=False, enforce_invertibility=False)
model_fit = model.fit(disp=0)
current_aic = model_fit.aic
output = model_fit.forecast(60)

#final_aic = float('Inf')
#for p in range(0,7):
#    for q in range(0,7): 
#               print("p,q",p,q)
#               model = ARIMA(history,order=(p, 1, q))
#               model_fit = model.fit(disp=0)
#               current_aic = model_fit.aic
#               print(current_aic)
#               if(current_aic<final_aic):
#                   final_aic = current_aic
#                   final_p = p
#                   final_q = q
#                   print("final p q are",final_p,final_q)
#                   
#for p in range(0,5):
#    for q in range(0,5):
#       for  P in range(0,5):
#           for Q in range(0,5): 
#               print("p,q,P,Q",p,q,P,Q)
#               model = sarimax.SARIMAX(history,order=(p, 1, q), seasonal_order=(P, 1, Q,60),enforce_stationarity=False, enforce_invertibility=False)
#               model_fit = model.fit(disp=0)
#               current_aic = model_fit.aic
#               print(current_aic)
#               if(current_aic<final_aic):
#                   final_aic = current_aic
#                   final_p = p
#                   final_q = q
#                   final_P = P
#                   final_Q = Q
#                   print("final p q P Q are",final_p,final_q,final_P, final_Q)
                   
    
                   
#model1 = sarimax.SARIMAX(endog,  order=(1, 0, 0), seasonal_order=(0, 0, 0, 0), trend=None, measurement_error=False, time_varying_regression=False, mle_regression=True, simple_differencing=False, enforce_stationarity=True, enforce_invertibility=True, hamilton_representation=False, concentrate_scale=False, **kwargs)
SpSession.sql("select distinct  url from log_new_session_id").take(20)

logDataFrameSessionId.take(1)
split_col = split(logDataFrameSessionId['url'], '/')
df_for_clustering = logDataFrameSessionId.withColumn('category', split_col.getItem(3))
df_for_clustering = df_for_clustering.withColumn('sub_category', split_col.getItem(4))
split_col2 = split(df_for_clustering['sub_category'], '\?')
df_for_clustering = df_for_clustering.withColumn('sub_category1', split_col2.getItem(0))
df_for_clustering.take(2)

df_for_clustering.createOrReplaceTempView("dataforClustering")
sqlContext.sql("SELECT distinct category from dataforClustering").count()
sqlContext.sql("SELECT distinct category from dataforClustering").show(20)
#df_for_clustering = df_for_clustering.withColumn('third', split_col.getItem(5))
#df_for_clustering.take(20)
#split_col = split(df_for_clustering['url'], '\?')
#df_for_clustering = df_for_clustering.withColumn('third', split_col.getItem(1))
#df_for_clustering.take(20)
#df_for_clustering.createOrReplaceTempView("dataforClustering")
#SpSession.sql("select distinct ist from dataforClustering").count()


#import re
#import sys
#
#URL_LOG_PATTERN = '^(\S+)//(\S+)/(\S+)/(\S+)'  
#def parse_url(url):
#    match = re.search(URL_LOG_PATTERN, url)
#    if match is None:
#         raise Error("Invalid logline: %s" % logline)
#    return Row(
#        category    = match.group(3),
#        sub_category = match.group(4)
#    )
#from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)
#rdd_url = logDataFrameSessionId.select('url').rdd
#access_url = (rdd_url.map(lambda x: parse_url(x)).cache())
#access_url.collect().count()

