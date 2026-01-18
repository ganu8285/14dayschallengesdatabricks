# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.events;
# MAGIC
# MAGIC CREATE TABLE bronze.events AS
# MAGIC SELECT * FROM workspace.default.bronze_df_nov;
# MAGIC
# MAGIC SELECT COUNT(*) FROM bronze.events;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.events_clean;
# MAGIC
# MAGIC CREATE TABLE silver.events_clean AS
# MAGIC SELECT * FROM workspace.default.silver_df_nov_realworld;
# MAGIC
# MAGIC SELECT COUNT(*) FROM silver.events_clean;

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold.product_metrics;
# MAGIC DROP TABLE IF EXISTS gold.category_metrics;
# MAGIC DROP TABLE IF EXISTS gold.daily_metrics;
# MAGIC
# MAGIC CREATE TABLE gold.product_metrics AS
# MAGIC SELECT * FROM workspace.default.gold_df_product_nov;
# MAGIC
# MAGIC CREATE TABLE gold.category_metrics AS
# MAGIC SELECT * FROM workspace.default.gold_df_category_nov;
# MAGIC
# MAGIC CREATE TABLE gold.daily_metrics AS
# MAGIC SELECT * FROM workspace.default.gold_df_daily_nov;
# MAGIC
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.top_products AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   brand,
# MAGIC   revenue,
# MAGIC   conversion_rate
# MAGIC FROM gold.product_metrics
# MAGIC WHERE revenue > 0
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 100;
# MAGIC
# MAGIC SELECT * FROM gold.top_products;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED gold.top_products;

# COMMAND ----------

