// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val url = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

// COMMAND ----------

val df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(url)

// COMMAND ----------

df.show(10)

// COMMAND ----------

df.select("State", "Color", "Count").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy( desc("Total")).show(20)

// COMMAND ----------

df.select("State", "Color", "Count").groupBy("State", "Color").agg(max("Count").alias("Maximo")).orderBy( desc("Maximo")).show(20)

// COMMAND ----------

df.select("State", "Color", "Count").groupBy("State", "Color").agg(min("Count").alias("Minimo")).orderBy( asc("State")).show(20)

// COMMAND ----------

df.select("State", "Color", "Count").groupBy("State", "Color").agg(avg("Count").alias("Total")).orderBy( desc("State")).show(20)

// COMMAND ----------

df.select("State", "Color", "Count").where(col("State") === "CA").limit(5).groupBy("State", "Color").agg(avg("Count").alias("AVG")).show(20)

// COMMAND ----------

df.select("State", "Color", "Count").groupBy("State", "Color").agg(max("Count").alias("Maximo"), min("Count").alias("Minimo"), avg("Count").alias("AVG")).orderBy( asc("State")).show(20)

// COMMAND ----------


