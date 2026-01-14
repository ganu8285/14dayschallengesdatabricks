# Databricks notebook source
bronze_path = "/Volumes/workspace/ecommerce/ecommerce_data/delta/bronze"
silver_path = "/Volumes/workspace/ecommerce/ecommerce_data/delta/silver"
gold_path   = "/Volumes/workspace/ecommerce/ecommerce_data/delta/gold"

print("Architecture design: Paths defined for Bronze, Silver, and Gold layers.")


# COMMAND ----------

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv")
)

# Save as Bronze layer (overwrite ensures a fresh start)
raw_df.write.format("delta").mode("overwrite").save(bronze_path)

print("Bronze Layer: Raw data ingested successfully.")


# COMMAND ----------


from pyspark.sql.functions import col

# Read from Bronze layer
bronze_df = spark.read.format("delta").load(bronze_path)

# Transformations: filter valid records
silver_df = (
    bronze_df
    .filter(col("user_id").isNotNull())
    .filter(col("price") > 0)
)

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").save(silver_path)

print("Silver Layer: Cleaned data created successfully.")

display(
    spark.read.format("delta").load(silver_path).limit(3)
)

# COMMAND ----------

from pyspark.sql.functions import sum as _sum

# Create business aggregates (revenue by brand)
gold_df = (
    spark.read.format("delta").load(silver_path)
    .groupBy("brand")
    .agg(_sum("price").alias("total_revenue"))
    .orderBy(col("total_revenue").desc())
)


# COMMAND ----------

gold_df.write.format("delta").mode("overwrite").save(gold_path)

print("Gold Layer: Business aggregates created (Revenue by Brand).")

display(
    spark.read.format("delta").load(gold_path).limit(5)
)
