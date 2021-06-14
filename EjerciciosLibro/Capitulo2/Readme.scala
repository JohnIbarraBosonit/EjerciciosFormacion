// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val url = "/databricks-datasets/learning-spark-v2/SPARK_README.md"
val strings = spark.read.text(url)
val filtered = strings.filter(col("value").contains("Spark"))

// COMMAND ----------

filtered.count()

// COMMAND ----------

filtered.show(false)

// COMMAND ----------


