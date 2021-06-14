// Databricks notebook source
//Lectura de fichero AVRO
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
val df = spark.read.format("avro").load(file)
df.show()

// COMMAND ----------

//Leer avro dentro de tabla SQL
spark.sql("CREATE OR REPLACE TEMPORARY VIEW episode_tbl USING avro OPTIONS(path '/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*')")

// COMMAND ----------

spark.sql("SELECT * FROM episode_tbl").show(false)

// COMMAND ----------

//Escribir avro en archivo
df.write.format("avro").mode("overwrite").save("/tmp/data/avro/df_avro")

// COMMAND ----------


