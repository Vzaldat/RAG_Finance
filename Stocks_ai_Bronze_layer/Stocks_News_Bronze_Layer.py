# Databricks notebook source


# %sql
# DROP TABLE IF EXISTS stocks_ai.stocks_news_data.stock_news_bronze_layer;
# DROP SCHEMA IF EXISTS stocks_ai.stocks_news_data;



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col
from datetime import datetime, timedelta
import requests
import pandas as pd
import time

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Set your API key
api_key = "d10tqj1r01qse6le110gd10tqj1r01qse6le1110" 

if not spark.catalog.tableExists("stocks_ai.stocks_news_data.stock_news_bronze_layer"):
    is_incremental_flag = 0
else:
    is_incremental_flag = 1

def get_finnhub_news(tickers, api_key, start_date):
    """
    This function fetches news from the Finnhub API for a list of tickers.
    Only the date (not time) is returned.
    """
    all_records = []
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = datetime.now().strftime("%Y-%m-%d")

    for ticker in tickers:
        url = f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={start_date_str}&to={end_date_str}&token={api_key}"
        response = requests.get(url)
        if response.status_code != 200:
            continue
        data = response.json()

        for article in data:
            if 'summary' in article and 'datetime' in article:
                record = {
                    'stock': ticker.upper(),
                    'summary': article['summary'],
                    'date': datetime.fromtimestamp(article['datetime']).date()
                }
                all_records.append(record)
        time.sleep(1)  

    return pd.DataFrame(all_records)

# Get tickers
stock_names_df = spark.sql("SELECT * FROM stocks_ai.stocks_name_ticker.stock_names")
stock_list = [row['ticker'] for row in stock_names_df.collect()]

if is_incremental_flag == 0:
    # Full load (last 5 days)
    start_date = datetime.now() - timedelta(days=5)
    news_df = get_finnhub_news(stock_list, api_key, start_date=start_date)

    spark.sql("CREATE SCHEMA IF NOT EXISTS stocks_ai.stocks_news_data")
    df_spark = spark.createDataFrame(news_df)
    df_spark.write.format("delta").mode("overwrite").saveAsTable("stocks_ai.stocks_news_data.stock_news_bronze_layer")

else:
    # Incremental load from latest date in table
    latest_date_str = spark.read.table("stocks_ai.stocks_news_data.stock_news_bronze_layer") \
                                .agg(max("date").alias("latest_date")) \
                                .collect()[0]["latest_date"]

    start_date = latest_date_str + timedelta(days=1)
    news_df = get_finnhub_news(stock_list, api_key, start_date=start_date)
    df_spark = spark.createDataFrame(news_df)
    df_spark.write.format("delta").mode("append").saveAsTable("stocks_ai.stocks_news_data.stock_news_bronze_layer")


# COMMAND ----------

#things that can be improved- unit testing
#logging and monitoring


# COMMAND ----------

