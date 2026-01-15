# Databricks notebook source

from pyspark.sql import functions as F

def process_bronze():
    print("Running Bronze Layer...")
    raw_df = spark.read.csv(source_file, header=True, inferSchema=True)
    raw_df.withColumn(
        "ingestion_ts", F.current_timestamp()
    ).write.format("delta").mode("overwrite").saveAsTable("bronze_events")
    return "Bronze Success"


def process_silver():
    print("Running Silver Layer...")
    bronze_df = spark.read.table("bronze_events")

    # Using the cleaning logic from Day 6
    silver_df = (
        bronze_df
        .filter(F.col("price") > 0)
        .dropDuplicates(["user_session", "event_time"])
        .withColumn(
            "product_name",
            F.coalesce(
                F.element_at(F.split(F.col("category_code"), r"\."), -1),
                F.lit("Other")
            )
        )
    )

    silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_events")
    return "Silver Success"


def process_gold():
    print("Running Gold Layer...")
    silver_df = spark.read.table("silver_events")

    gold_df = (
        silver_df
        .groupBy("product_id", "product_name")
        .agg(F.sum("price").alias("total_revenue"))
    )

    gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_product_revenue")
    return "Gold Success"


# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    "Source File Path"
)

# COMMAND ----------

dbutils.widgets.dropdown(
    "layer",
    "bronze",
    ["bronze", "silver", "gold"],
    "Select Processing Layer"
)

# COMMAND ----------

source_file = dbutils.widgets.get("source_path")
active_layer = dbutils.widgets.get("layer")

print(f"Job started for Layer: {active_layer}")
print(f"Processing Source: {source_file}")


# COMMAND ----------

