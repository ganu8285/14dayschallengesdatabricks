# Databricks notebook source
# Step 0: Combine October + November Silver tables (if not done already)
spark.sql("""
CREATE OR REPLACE TABLE default.silver_events_all AS
SELECT * FROM default.silver_events_oct
UNION ALL
SELECT * FROM default.silver_df_nov_realworld
""")

# Step 1: Explain Query Plan
spark.sql("SELECT * FROM default.silver_events_all WHERE event_type='purchase'").explain(True)

# Step 2: Partition Large Table
spark.sql("""
CREATE OR REPLACE TABLE default.silver_events_all_part
USING DELTA
PARTITIONED BY (event_date, event_type)
AS SELECT * FROM default.silver_events_all
""")

# Step 3: Apply ZORDER for Optimized Lookups
spark.sql("OPTIMIZE default.silver_events_all_part ZORDER BY (user_id, product_id)")

# Step 4: Benchmark Performance (Before vs After)

import time

# Before Optimization
start = time.time()
spark.sql("""
SELECT *
FROM default.silver_events_all
WHERE event_type = 'purchase'
AND event_date = '2019-11-10'
""").count()
print(f"Unoptimized time: {time.time() - start:.2f} seconds")

# After Optimization
start = time.time()
spark.sql("""
SELECT *
FROM default.silver_events_all_part
WHERE event_type = 'purchase'
AND event_date = '2019-11-10'
""").count()
print(f"Optimized time: {time.time() - start:.2f} seconds")

# Step 5: Note - Caching is NOT supported on Serverless compute
# cached_df = spark.table("default.silver_events_all_part").cache()
# cached_df.count()  # This will fail on Serverless

# COMMAND ----------

