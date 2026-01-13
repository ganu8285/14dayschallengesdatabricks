# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F
deltaTable = DeltaTable.forName(spark, "nov_events_table")

# COMMAND ----------

new_data = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv", header=True, inferSchema=True).limit(100)


# COMMAND ----------

updates = new_data.withColumn(
    "product_name",
    F.coalesce(F.element_at(F.split(F.col("category_code"), r"\."), -1), F.lit("Other"))
)

# COMMAND ----------

deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.user_session = source.user_session AND target.event_time = source.event_time"
).whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()

print("Incremental MERGE completed successfully.")

# COMMAND ----------

history_df = deltaTable.history().select("version", "timestamp", "operation", "operationMetrics")
display(history_df.limit(5))

# COMMAND ----------

display(deltaTable.history())

# Query a specific version (e.g., the state before the merge)
v0 = spark.read.format("delta").option("versionAsOf", 0).table("nov_events_table")

# COMMAND ----------

try:
    historical_state = spark.read.format("delta") \
        .option("timestampAsOf", "2026-01-12 12:00:00") \
        .table("nov_events_table")
    display(historical_state.limit(5))
except Exception as e:
    print(f"Timestamp query failed: {e}")

# COMMAND ----------

print(f"Current Row Count: {spark.table('nov_events_table').count():,}")

# Show Version 0 count (before the merge)
v0_df = spark.read.format("delta").option("versionAsOf", 0).table("nov_events_table")
print(f"Version 0 Row Count: {v0_df.count():,}")

# Display the actual data from the past
display(v0_df.limit(5))

# COMMAND ----------

