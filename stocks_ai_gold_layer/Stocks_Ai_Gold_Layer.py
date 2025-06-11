# Databricks notebook source
historical_df = spark.sql("Select * from stocks_ai.stocks_history_data.stock_history_silver_layer")
news_df = spark.sql("Select * from stocks_ai.stocks_news_data.stock_news_silver_layer")
stocks_name_df = spark.sql("Select * from stocks_ai.stocks_name_ticker.stock_names")
holder_df = spark.sql("Select * from stocks_ai.stocks_holders_data.stock_institutional_holders")


# COMMAND ----------

from pyspark.sql.functions import collect_list
from pyspark.sql.functions import col, abs, datediff, row_number, to_date
from pyspark.sql.window import Window

grouped_news_df = news_df.groupBy(['date', 'stock']).agg(collect_list('summary').alias('news_list'))
grouped_news_df = grouped_news_df.withColumnRenamed("stock", "ticker")

# COMMAND ----------

from pyspark.sql.functions import to_date, col

news_df = grouped_news_df.withColumn("date", to_date(col("date"), "MM-dd-yyyy"))
hist_df = historical_df.withColumn("date", to_date(col("date"), "MM-dd-yyyy"))


news_cols = [c for c in news_df.columns if c not in ['date', 'ticker']]
news_df = news_df.select("date", "ticker", *news_cols)


joined_df = hist_df.alias("hist").join(
    news_df.alias("news"),
    on=["date", "ticker"],
    how="left"  
)


# COMMAND ----------

# Perform a left join to add company_name to joined_df
stocks_data_news_historical_df = joined_df.join(
    stocks_name_df, 
    on="ticker", 
    how="left"
)
# Create schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS stocks_ai.stocks_gold_layer")
stocks_data_news_historical_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("stocks_ai.stocks_GOLD_LAYER.Stocks_news_historical_RAG")


# COMMAND ----------

display(stocks_data_news_historical_df)

# COMMAND ----------

# Perform a left join to add company_name to joined_df
holders_final_df= holder_df.join(
    stocks_name_df, 
    on="ticker", 
    how="left"
)
holders_final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("stocks_ai.stocks_GOLD_LAYER.Stocks_holders_RAG")



# COMMAND ----------

display(holders_final_df)