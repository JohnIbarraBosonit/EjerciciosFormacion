// Databricks notebook source
import org.apache.spark.sql.SparkSession

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

//Creamos un sparkSession
val spark=SparkSession
.builder
.appName("SparkSqlExampleApp")
.getOrCreate()

// COMMAND ----------

//Cargamos ela ruta al csv y el esquema que vamos a usar
val ruta = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

// COMMAND ----------

//Leemos el archivo CSV con las opciones de mostrar cabeceras e inferir tipo de datos
val df = spark.read
.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load(ruta)

// COMMAND ----------

//Creamos una vista temporal para hacer las consultas necesarias
df.createOrReplaceTempView("vuelos")

// COMMAND ----------

df.describe()

// COMMAND ----------

//CONSULTA VERSION SQL
val d_mayor_mil = spark.sql("""SELECT origin, destination, distance FROM vuelos WHERE distance > 1000 ORDER BY distance DESC""")
d_mayor_mil.show()

// COMMAND ----------

//CONSULTA VERSION SQL
val c2 = spark.sql("""SELECT origin, destination, date, delay FROM vuelos WHERE origin = 'SFO' AND destination = 'ORD' AND delay >= 120 ORDER BY delay DESC""")

// COMMAND ----------

c2.show()

// COMMAND ----------

//CONSULTA VERSION SQL
spark.sql("SELECT origin, destination, delay, CASE WHEN delay > 360 THEN 'Very long delays' WHEN delay > 120 AND delay < 360 THEN 'Long delays' WHEN delay > 60 AND delay < 120 THEN 'Short delays' WHEN delay > 0 AND delay < 60 THEN 'Torelable Delays' WHEN delay = 0 THEN 'No delays' ELSE 'EARLY' END AS Flight_Delays FROM vuelos ORDER BY origin, delay DESC").show(10)

// COMMAND ----------

//Misma consulta con API dataframe
(df.select("origin", "destination", "distance")
.where("distance > 1000")
.orderBy(desc("distance")) 
.show(10))

// COMMAND ----------

//Misma consulta con API dataframe
df.select("origin", "destination", "date", "delay")
.where("origin == 'SFO' AND destination = 'ORD' AND delay >= 120")
.orderBy(desc("delay")).show()

// COMMAND ----------

//Misma consulta con API dataframe
df.select($"delay", $"origin", $"destination")
 .withColumn("Flight_Delays", when($"delay" > 360, "Very Long Delays")
             .when($"delay" > 120 && $"delay" < 360, "Long Delays")
             .when($"delay" > 60 && $"delay" < 120, "Short Delays")
             .when($"delay" > 0 && $"delay" < 600, "Tolerable Delays")
             .when($"delay" === 0, "No Delays")
             .otherwise("Early"))
 .orderBy(col("origin"), col("delay").desc).show()

// COMMAND ----------


