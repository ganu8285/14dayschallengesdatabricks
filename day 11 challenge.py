# Databricks notebook source
from pyspark.sql.functions import *

df = spark.sql("SELECT * FROM samples.bakehouse.media_gold_reviews_chunked")


# COMMAND ----------

df.display()

# COMMAND ----------

df.select(
    count("*").alias("total_events"),
    countDistinct("brand").alias("distinct_brands"),
    countDistinct("product_id").alias("distinct_products"),
    avg("price").alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    expr("percentile(price, 0.5)").alias("median_price")
).display()


# COMMAND ----------

df.groupBy("event_type") \
  .agg(
      count("*").alias("event_count"),
      avg("price").alias("avg_price")
  ) \
  .orderBy("event_count", ascending=False) \
  .display()

# COMMAND ----------

df_time = df.withColumn(
    "day_of_week", dayofweek("event_time")
).withColumn(
    "is_weekend",
    when(dayofweek("event_time").isin(1,7), 1).otherwise(0)
)

df_time.display()

# COMMAND ----------

weekday_avg = df_time.filter("is_weekend = 0").agg(avg("price")).collect()[0][0]
weekend_avg = df_time.filter("is_weekend = 1").agg(avg("price")).collect()[0][0]

percent_diff = ((weekend_avg - weekday_avg) / weekday_avg) * 100
display(percent_diff)

# COMMAND ----------

product_stats = df.groupBy("product_id") \
    .agg(
        count("*").alias("event_count"),
        avg("price").alias("avg_price")
    )

# COMMAND ----------

product_stats.stat.corr("event_count", "avg_price")
display(product_stats)

# COMMAND ----------

df_features = df_time.withColumn("hour_of_day", hour("event_time"))
df_features.display()

# COMMAND ----------

user_features = df_features.groupBy("brand") \
    .agg(
        count("*").alias("brand_event_count")
    )

df_features = df_features.join(user_features, "brand")
df_features.display()

# COMMAND ----------

product_features = df_features.groupBy("product_id") \
    .agg(
        count("*").alias("product_event_count"),
        avg("price").alias("product_avg_price")
    )

df_features = df_features.join(product_features, "product_id")
df_features.display()

# COMMAND ----------

df_features = df_features.withColumn(
    "user_activity_bucket",
    when(df_features.brand_event_count < 5, "low")
    .when(df_features.brand_event_count < 20, "medium")
    .otherwise("high")
)
df_features.display()

# COMMAND ----------

ml_df = df_features.select(
    "product_id",
    "event_type",
    "price",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "brand_event_count",
    "product_event_count",
    "product_avg_price",
    "user_activity_bucket"
)

# COMMAND ----------

ml_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.gold.ml_features_events")
     

# COMMAND ----------

