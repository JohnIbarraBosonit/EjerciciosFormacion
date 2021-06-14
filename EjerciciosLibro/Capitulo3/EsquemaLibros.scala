// Databricks notebook source
import org.apache.spark.sql.types._

// COMMAND ----------

val schema = StructType(Array(StructField("author", StringType, false), StructField("title", StringType, false), StructField("pages", IntegerType, false)))

// COMMAND ----------

//ESQUEMA USANDO DDL
val schema = "author STRING, title STRING, pages INT"

// COMMAND ----------


