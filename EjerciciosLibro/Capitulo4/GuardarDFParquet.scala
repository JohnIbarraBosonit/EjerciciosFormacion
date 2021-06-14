// Databricks notebook source
//Lectura de archivos parquet
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/"""
val df = spark.read.format("parquet").load(file)
df.show(10)

// COMMAND ----------

spark.sql("CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING parquet OPTIONS(path '/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/')")

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

//Escribir un dataframe en formato parquet
df.write.format("parquet").mode("overwrite").option("compression", "snappy").save("/tmp/data/parquet/df_parquet")

// COMMAND ----------


