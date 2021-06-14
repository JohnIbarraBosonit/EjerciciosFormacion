// Databricks notebook source
//Importamos las funciones de slq
import org.apache.spark.sql.functions._

// COMMAND ----------

//Creamos un dataframe con un array de pares
val df = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
 ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

// COMMAND ----------

//Calculamos el avg con funciones de agregacion
val avgDF = df.groupBy("name").agg(avg("age"))

// COMMAND ----------

//Mostramos el AVG
avgDF.show()


// COMMAND ----------


