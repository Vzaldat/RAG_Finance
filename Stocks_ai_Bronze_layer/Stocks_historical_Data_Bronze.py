# Databricks notebook source
# %sql
# DROP TABLE IF EXISTS stocks_ai.stocks_history_data.stock_history_bronze_layer;
# DROP TABLE IF EXISTS stocks_ai.stocks_history_data.stock_history_silver_layer;
# DROP SCHEMA IF EXISTS stocks_ai.stocks_history_data;
!pip install yfinance

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

spark = SparkSession.builder.getOrCreate()


bronze_table = "stocks_ai.stocks_history_data.stock_history_bronze_layer"

# Flag: check if table exists for incremental logic
if not spark.catalog.tableExists(bronze_table):
    is_incremental_flag = 0
else:
    is_incremental_flag = 1

def get_historical_data(tickers, start_date, end_date):
    all_records = []

    for ticker in tickers:
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)

        if not df.empty:
            df.reset_index(inplace=True)

            # Flatten multiindex: convert ('Open', 'AAPL') to 'open'
            df.columns = [col[0].lower() if isinstance(col, tuple) else col.lower() for col in df.columns]

            
            df['ticker'] = ticker.upper()

            all_records.append(df)

    if all_records:
        combined_df = pd.concat(all_records, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()

# Get ticker list from master table
stock_names_df = spark.sql("SELECT * FROM stocks_ai.stocks_name_ticker.stock_names")
stock_list = [row['ticker'] for row in stock_names_df.collect()]

# Determine start date
if is_incremental_flag == 0:
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
else:
    latest_date = spark.read.table(bronze_table) \
                            .agg(max("date").alias("latest_date")) \
                            .collect()[0]["latest_date"]
    start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')

end_date = datetime.now().strftime('%Y-%m-%d')




historical_df = get_historical_data(stock_list, start_date, end_date)

if not historical_df.empty:
    
    

    
    df_spark = spark.createDataFrame(historical_df)

    
    spark.sql("CREATE SCHEMA IF NOT EXISTS stocks_ai.stocks_history_data")

    
    write_mode = "overwrite" if is_incremental_flag == 0 else "append"
    df_spark.write.format("delta").mode(write_mode).saveAsTable(bronze_table)
else:
    print("No data to write.")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

