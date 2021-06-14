// Databricks notebook source
//Creamos la funcion
val cubed = (s:Long) => {
  s*s*s
}

// COMMAND ----------

spark.udf.register("cubed", cubed)

// COMMAND ----------

spark.range(1,9).createOrReplaceTempView("udf_test")

// COMMAND ----------

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
