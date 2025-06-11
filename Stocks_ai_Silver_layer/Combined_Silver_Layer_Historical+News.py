# Databricks notebook source
# MAGIC %md Silver Layer for Historical data
# MAGIC

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import date_format, round as pyspark_round, col, trim


historical_df = spark.sql("""
    SELECT * 
    FROM stocks_ai.stocks_history_data.stock_history_bronze_layer
""")

transformed_df = (
    historical_df
    .withColumn("date", date_format("date", "MM-dd-yyyy"))
    .withColumn("close", pyspark_round(col("close"), 3))
    .withColumn("high", pyspark_round(col("high"), 3))
    .withColumn("low", pyspark_round(col("low"), 3))
    .withColumn("open", pyspark_round(col("open"), 3))
    .withColumn("volume", col("volume"))
    .withColumn("ticker", col("ticker"))  
)



transformed_df.write.format("delta").mode("overwrite") \
    .saveAsTable("stocks_ai.stocks_history_data.stock_history_silver_layer")


# COMMAND ----------

# MAGIC %md Silver Layer for News_data

# COMMAND ----------

from pyspark.sql.functions import date_format, col, trim

# Load bronze ne
news_df = spark.sql("""
    SELECT * 
    FROM stocks_ai.stocks_news_data.stock_news_bronze_layer
""")


news_silver_df = (
    news_df
    .filter(col("summary").isNotNull())                   # Drop null
    .filter(trim(col("summary")) != "")                   # Drop empty/whitespace
    .dropna(how="any")                                     # Drop any other empty rows
    .withColumn("date", date_format("date", "MM-dd-yyyy")) # Format date
)

news_silver_df.write.format("delta").mode("overwrite") \
    .saveAsTable("stocks_ai.stocks_news_data.stock_news_silver_layer")


# COMMAND ----------

