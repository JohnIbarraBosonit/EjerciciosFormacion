// Databricks notebook source
val file="""/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"""
val df = spark.read.format("parquet").load(file)
df.show(10)

// COMMAND ----------

//cargar CSV
val file_csv ="/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
val df3 = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .option("mode", "PERMISSIVE")
 .load(file_csv).show(10)

// COMMAND ----------

val file_json = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
val df4 = spark.read.format("json").load(file_json)

// COMMAND ----------

val location = "/Users/johnerik.ibarra@bosonit.com/Datasets/file.json"
df4.write.format("json").mode("overwrite").save(location)
