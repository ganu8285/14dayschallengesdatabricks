# Databricks notebook source
# DBTITLE 1,Cell 1
# Databricks notebook source
#Required Imports (Common)
import mlflow
import mlflow.sklearn

from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare Training Data (sklearn)

# COMMAND ----------

# DBTITLE 1,Cell 2
df = spark.table("workspace.gold.product_ml_features").toPandas()

X = df[["views"]]
y = df["purchases"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


# COMMAND ----------


mlflow.set_experiment("/Shared/multi_model_comparison")

for name, model in models.items():
    with mlflow.start_run(run_name=f"{name}_model"):

        # Log parameters
        mlflow.log_param("model_type", name)

        if name == "decision_tree":
            mlflow.log_param("max_depth", 5)
        if name == "random_forest":
            mlflow.log_param("n_estimators", 100)

        # Train
        model.fit(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        score = r2_score(y_test, y_pred)

        # Log metric
        mlflow.log_metric("r2_score", score)

        # Log model
        mlflow.sklearn.log_model(model, artifact_path="model")

        print(f"{name}: R² = {score:.4f}")


# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression as SparkLR

assembler = VectorAssembler(inputCols=["views","cart_adds"], outputCol="features")
lr = SparkLR(featuresCol="features", labelCol="purchases")
pipeline = Pipeline(stages=[assembler, lr])

spark_df = spark.table("gold.product_ml_features")
train, test = spark_df.randomSplit([0.8, 0.2])
model = pipeline.fit(train)

# COMMAND ----------

