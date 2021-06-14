// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}

// COMMAND ----------

//Creamos un esquema para la carga
val schema = StructType(Array(StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDiposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("NeighborHood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true)
))

// COMMAND ----------

val sf_fire_file = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

// COMMAND ----------

//Leemos el csv y le pasamos el esquema previamente calculado
val fireDF = spark.read.format("csv").option("header", "true").schema(schema).load(sf_fire_file)

// COMMAND ----------

//Mostramos 4 registros
fireDF.take(4)

// COMMAND ----------

val parquetPath = "/tmp/data/parquet/df_parquet"

// COMMAND ----------

//Consulta usando API
val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where($"CallType" =!= "Medical Incident")
fewFireDF.show(5, false)

// COMMAND ----------

val df = fireDF.select("CallType").where(col("CallType").isNotNull).agg(countDistinct('CallType) as 'DistinctCallTypes)

// COMMAND ----------

//Cambiamos el nombre a la columna
val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF
 .select("ResponseDelayedinMins")
 .where($"ResponseDelayedinMins" > 5)
 .show(5, false)

// COMMAND ----------

val fireTsDF = newFireDF
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm")

// COMMAND ----------

fireTsDF
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, false)

// COMMAND ----------

fireTsDF
 .select(year($"IncidentDate"))
 .distinct()
 .orderBy(year($"IncidentDate"))
 .show()

// COMMAND ----------

fireTsDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .groupBy("CallType")
 .count()
 .orderBy(desc("count"))
 .show(10, false)

// COMMAND ----------

fireTsDF
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
 F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show()

// COMMAND ----------


