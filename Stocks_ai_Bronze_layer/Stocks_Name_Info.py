# Databricks notebook source
# MAGIC
# MAGIC %pip install yfinance
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd
import yfinance as yf


spark = SparkSession.builder.getOrCreate()

#using_top_100_stocks_only --- this is can be extended depending on the use case
top_100_tickers = [
"AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "UNH", "V",
"JNJ", "XOM", "PG", "MA", "LLY", "HD", "CVX", "MRK", "PEP", "ABBV",
"AVGO", "KO", "BAC", "PFE", "COST", "TMO", "CSCO", "ABT", "MCD", "CRM",
"ADBE", "WMT", "DHR", "ACN", "LIN", "TXN", "NKE", "CMCSA", "PM", "NEE",
"VZ", "RTX", "BMY", "UPS", "HON", "QCOM", "T", "MS", "LOW", "UNP",
"INTC", "ORCL", "SPGI", "IBM", "AMD", "GS", "CAT", "INTU", "DE", "BLK",
"AMAT", "ISRG", "SBUX", "GILD", "MMM", "ADI", "CVS", "C", "MDLZ", "AMGN",
"SYK", "BKNG", "PLD", "DIS", "TJX", "PYPL", "MO", "CI", "SCHW", "AXP",
"GE", "BDX", "CME", "EOG", "COP", "USB", "TGT", "TMUS", "NOC", "PNC",
"ITW", "CSX", "ICE", "MMC", "REGN", "SO", "DUK", "APD", "AON", "ETN"
]

company_info_list = []


for ticker in top_100_tickers:
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        company_info_list.append({
            'ticker': ticker,
            'company_name': info.get('longName', 'N/A')
        })
    except:
        pass


stocks_top_100_df = pd.DataFrame(company_info_list)


stocks_top_100_spark_df = spark.createDataFrame(stocks_top_100_df)

spark.sql("CREATE SCHEMA IF NOT EXISTS stocks_ai.stocks_name_ticker")

# Save table
stocks_top_100_spark_df.write.mode("overwrite").saveAsTable("stocks_ai.stocks_name_ticker.stock_names")



# COMMAND ----------

#df_test = spark.sql("select * from stocks_ai.stocks_name_ticker.stock_names")

# COMMAND ----------



# COMMAND ----------

display(df_top_100)

# COMMAND ----------

