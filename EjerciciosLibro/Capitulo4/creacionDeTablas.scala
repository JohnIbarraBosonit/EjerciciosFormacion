// Databricks notebook source
spark.sql("CREATE DATABASE learn_spark_db")

// COMMAND ----------

spark.sql("USE learn_spark_db")

// COMMAND ----------

spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

// COMMAND ----------

spark.sql("SHOW TABLES")

// COMMAND ----------

spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS(PATH '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')""")

// COMMAND ----------

spark.sql("""SHOW TABLES""")

// COMMAND ----------

spark.sql("""SELECT (*) FROM us_delay_flights_tbl""").show(9)

// COMMAND ----------

//CREACION DE UNA VISTA CON SQL
spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

//CREACION DE UNA VISTA CON SQL
spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_JFK_global_tmp_view AS SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

// COMMAND ----------

val df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

val df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_temp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

// COMMAND ----------

spark.sql("""SELECT * FROM us_origin_airport_JFK_tmp_view""").show(10)

// COMMAND ----------

spark.catalog.listTables().show()

// COMMAND ----------


