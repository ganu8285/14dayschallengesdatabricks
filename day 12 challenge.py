# Databricks notebook source
oct_df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv")
nov_df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv")

raw_df = oct_df.unionByName(nov_df)
raw_df.createOrReplaceTempView("raw_events")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

from pyspark.sql.functions import sum, count

gold_df = raw_df.groupBy("product_id").agg(
    count("event_type").alias("views"),
    sum((raw_df.event_type == 'cart').cast('int')).alias("cart_adds"),
    sum((raw_df.event_type == 'purchase').cast('int')).alias("purchases")
)

gold_df.write.format("delta").mode("overwrite").saveAsTable("gold.product_ml_features")


# COMMAND ----------

df = spark.table("gold.product_ml_features").toPandas()
X = df[["views", "cart_adds"]]
y = df["purchases"]


# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

mlflow.set_experiment("/Shared/Day12_MLFlow_Regression")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

with mlflow.start_run(run_name="linear_regression_v1"):
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("test_size", 0.2)
    
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    r2 = model.score(X_test, y_test)
    mlflow.log_metric("r2_score", r2)
    
    mlflow.sklearn.log_model(model, "model")
    
    print(f"R2 Score: {r2:.4f}")

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor

with mlflow.start_run(run_name="random_forest_v1"):
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("n_estimators", 100)
    
    rf = RandomForestRegressor(n_estimators=100, random_state=42)
    rf.fit(X_train, y_train)
    
    r2 = rf.score(X_test, y_test)
    mlflow.log_metric("r2_score", r2)
    
    mlflow.sklearn.log_model(rf, "model")
    
    print(f"RF R2 Score: {r2:.4f}")