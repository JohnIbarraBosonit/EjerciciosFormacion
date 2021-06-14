// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val urlDelays = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val urlAirport = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

// COMMAND ----------

val airportDf = spark.read.format("csv")
  .option("header", "true")
  .option("inferschema", "true")
  .option("delimiter", "\t")
  .load(urlAirport)
airportDf.createOrReplaceTempView("airports_na")

// COMMAND ----------

val delayDF = spark.read
  .option("header", "true")
  .csv(urlDelays)
  .withColumn("delay", expr("CAST(delay as INT) as delay"))
  .withColumn("distance", expr("CAST(distance as INT) as distance"))
delayDF.createOrReplaceTempView("departureDelays")

// COMMAND ----------

val foo = delayDF.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0"""))
foo.createOrReplaceTempView("foo")

// COMMAND ----------

spark.sql("SELECT * FROM airports_na LIMIT 10").show()

// COMMAND ----------

spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

// COMMAND ----------

spark.sql("SELECT * FROM foo").show()

// COMMAND ----------

val bar = delayDF.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

// COMMAND ----------

foo.join(airportDf.as("air"), $"air.IATA" === $"origin").select("City","State","date","delay","distance","destination").show()

// COMMAND ----------

spark.sql("""
SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination 
 FROM foo f
 JOIN airports_na a
 ON a.IATA = f.origin
""").show()

// COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS departureDelaysWindow;""")

// COMMAND ----------

spark.sql("""CREATE TABLE departureDelaysWindow AS SELECT origin, destination, SUM(delay) AS TotalDelays FROM departureDelays WHERE origin IN ('SEA', 'SFO', 'JFK') AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') GROUP BY origin, destination;""")

// COMMAND ----------

spark.sql("""SELECT * FROM departureDelaysWindow""").show()

// COMMAND ----------

spark.sql("""SELECT origin, destination, TotalDelays, rank 
 FROM ( 
 SELECT origin, destination, TotalDelays, dense_rank() 
 OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
 FROM departureDelaysWindow
 ) t 
 WHERE rank <= 3;""").show()

// COMMAND ----------


