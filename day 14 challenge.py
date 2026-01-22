# Databricks notebook source
# MAGIC %pip install transformers

# COMMAND ----------

# MAGIC %pip install torch

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

#Task 2: Mosaic AI using [Transformers]
import torch
from transformers import pipeline

# Load sentiment analysis pipeline
sentiment_pipeline = pipeline(
  "sentiment-analysis",
  model="distilbert-base-uncased-finetuned-sst-2-english"
)

texts=[
    "checkout was smooth and fast",
    "product quality is very bad",
    "customer support was okay",
    "highly recommend this product",
    "Hello"
]

results=sentiment_pipeline(texts)

for text, result in zip(texts, results):
  print(f"{text} → {result['label']} ({result['score']:.2f})")

# COMMAND ----------

import mlflow
mlflow.set_experiment("/Shared/day14_ai_powered_analytics")

with mlflow.start_run(run_name="day14_transformers_sentiment"):
    mlflow.log_param("model", "distilbert-base-uncased-finetuned-sst-2-english")
    mlflow.log_param("ai_framework", "huggingface_transformers")
    mlflow.log_metric("num_text_samples", len(texts))

    # Log predictions
    predictions = {
        text: {
            "label": res["label"],
            "score": float(res["score"])
        }
        for text, res in zip(texts, results)
    }

    mlflow.log_dict(predictions, "sentiment_predictions.json")

print("✅ MLflow run logged with Transformers-based AI")

# COMMAND ----------

import torch
from transformers import pipeline
import mlflow

# Load pretrained sentiment analysis pipeline (CPU inference)
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english"
)

texts = [
    "checkout was smooth and fast",
    "product quality is very bad",
    "customer support was okay",
    "highly recommend this product",
    "Hello"
]

# Run sentiment analysis
results = sentiment_pipeline(texts)

# Log AI workflow to MLflow
mlflow.set_experiment("/Shared/day14_ai_powered_analytics")

with mlflow.start_run(run_name="day14_transformers_sentiment"):
    mlflow.log_param("model", "distilbert-base-uncased-finetuned-sst-2-english")
    mlflow.log_param("ai_framework", "huggingface_transformers")
    mlflow.log_param("device", "cpu")
    mlflow.log_metric("num_text_samples", len(texts))

    predictions = {
        text: {
            "label": res["label"],
            "score": float(res["score"])
        }
        for text, res in zip(texts, results)
    }

    mlflow.log_dict(predictions, "sentiment_predictions.json")

for text, result in zip(texts, results):
    print(f"{text} → {result['label']} ({result['score']:.2f})")

# COMMAND ----------

