# Databricks notebook source
events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    header=True,
    inferSchema=True
)

# COMMAND ----------

events.select("event_type", "product_id", "price").show(10)

# COMMAND ----------

events.filter("price > 100").count()

# COMMAND ----------

events.groupBy("event_type").count().show()

# COMMAND ----------

top_brands = events.groupBy("brand").count().orderBy("count", ascending=False).limit(5)