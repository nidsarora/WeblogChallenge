# WeblogChallenge


### Tool Used

Pyspark


## Assumptions


#To sessionize the web logs<br />

The sessions were created using:<br />
*IP <br />
*user_agent - As the session from different user agent will be different even if the IP remains the same<br />
*Session Window time is taken as 15 minutes<br />
Session length for sessions that just start are taken as sum of request_processing_time,response_processing_time <br />
and backend_processing_time, assuming the user waits for the response<br />
 

#For prediction of requests/sec for the next minute:<br />

*ARIMA and seasonal ARIMA were analyzed. <br />
*ACF and PACF plots were plotted ,AIC of different models were compared to find the best hyperparameters for the model.<br />
*The results of ACF ,PACF can be found in images folder<br />
*The results of ARIMA and seasonal ARIMA can be found in results folder<br />

Due to lack of memory, some of best parameters could  not be run for seasonal ARIMA<br />
Future work - Try the optimum parameters found by the analysis<br />


#For prediction of session Length and unique URL count:<br />

*Clusters were created using K-Means to find out the similar users on the basis of their activity<br />
The most important reason of K-Means was to find similar users based on the products they use, so that even if a new client appears,<br />
we can cluster them and predict the expected number of unique urls and their session time even using one or two of the history we have for them.<br />
*The url is the most important variable which gives for which product, the user is browsing, unique_url counts are taken<br />
*Other factors taken for K-Means are: avg_req_processing_time,avg_received_bytes, avg_session_time<br />
*The value of k was found to be 7 using the elbow method<br />

The prediction is given to be in between <the average session time of IP through history> and <average session time of the cluster to which it belongs><br />

In future, considering url_counts for products obtained from url for eg if user consists of wallet/sub_category, the clustering should considering
them to create powerful clusters<br />

The code has comments for all the operations applied to solve each problem.<br />

