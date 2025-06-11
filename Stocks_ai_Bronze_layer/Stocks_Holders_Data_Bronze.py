# Databricks notebook source
# %sql
# -- Drop the table if it exists
# DROP TABLE IF EXISTS stocks_ai.stocks_holders_data.stocks_institutional_holders;

# -- Drop the schema if it exists
# DROP SCHEMA IF EXISTS stocks_ai.stocks_holders_data;
!pip install yfinance



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, TimestampType
from datetime import datetime
import yfinance as yf
import pandas as pd

spark = SparkSession.builder.getOrCreate()


bronze_table = "stocks_ai.stocks_holders_data.stock_institutional_holders"

# Create schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS stocks_ai.stocks_holders_data")

# Define schema
schema = StructType([
    StructField("Date_Reported", TimestampType(), True),
    StructField("Holder", StringType(), True),
    StructField("pctHeld", FloatType(), True),
    StructField("Shares", LongType(), True),
    StructField("Value", LongType(), True),
    StructField("pctChange", FloatType(), True),
    StructField("ticker", StringType(), True)  # add ticker for multi-ticker support
])

stock_names_df = spark.sql("SELECT * FROM stocks_ai.stocks_name_ticker.stock_names")
stock_list = [row['ticker'] for row in stock_names_df.collect()]


all_data = []

for ticker in stock_list:
    t = yf.Ticker(ticker)
    df = t.institutional_holders

    if df is not None and not df.empty:
        df["ticker"] = ticker.upper()
        all_data.append(df)


if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)

    # Convert to Spark DF
    spark_df = spark.createDataFrame(combined_df, schema=schema)

    # Overwrite the table
    spark_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    print("Data written successfully.")
else:
    print("No institutional holder data to write.")


# COMMAND ----------



# COMMAND ----------

