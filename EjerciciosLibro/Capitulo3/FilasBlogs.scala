// Databricks notebook source
import org.apache.spark.sql.Row

// COMMAND ----------

val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",Array("twitter", "LinkedIn"))

// COMMAND ----------

blogRow(1)

// COMMAND ----------

val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))

// COMMAND ----------

val authorsDF = rows.toDF("Author", "State").show()
