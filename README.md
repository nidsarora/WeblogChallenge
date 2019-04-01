# WeblogChallenge

Tools Used: Pyspark 

#Notes

To sessionize the web logs

The sessions were created using:
-IP 
-user_agent - As the session from different user agent will be different even if the IP remains the same
-Session Window time is taken as 15 minutes 

For prediction of requests/sec for the next minute:

ARIMA andd seasonal ARIMA were analyzed. 
-ACF and PACF plots were plotted ,AIC of different models were compared to find the best hyperparameters for the model.

For prediction of session Length and unique URL count:
-
