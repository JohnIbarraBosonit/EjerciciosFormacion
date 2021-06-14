// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

val jsonFile = "/databricks-datasets/learning-spark-v2/blogs.json"

// COMMAND ----------

//Creamos el esquema que vamos a usar en el JSON
val schema = StructType(Array(StructField("Id", IntegerType, false), StructField("First", StringType, false), StructField("Last", StringType, false), StructField("Url", StringType, false), StructField("Published", StringType, false), StructField("Hits", IntegerType, false), StructField("Campaigns", ArrayType(StringType), false)))

// COMMAND ----------

//Leemos el JSON pasandole el esquema previamente creado
val blogsDF = spark.read.schema(schema).json(jsonFile)

// COMMAND ----------

//Mostramos el df
blogsDF.show(false)

// COMMAND ----------

//Mostramos el esquema
println(blogsDF.printSchema)
println(blogsDF.schema)

// COMMAND ----------

blogsDF.columns

// COMMAND ----------

//Mostramos la columna id
blogsDF.col("Id")

// COMMAND ----------

//Mostramos la columna hits * 2
blogsDF.select(expr("Hits * 2")).show(2)

// COMMAND ----------

//Otra forma fe mostrar la columna hits * 2
blogsDF.select(col("Hits") * 2).show(2)

// COMMAND ----------

//Agregamos una nueva columna don aquellos registros con hits > 10000 a true y sino a false
blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

// COMMAND ----------

//Agrega una nueva columna con la concatenacion de 3 columnas mas y la muestra
blogsDF.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(col("AuthorsId")).show(4)

// COMMAND ----------

blogsDF.select(expr("Hits")).show(2)
blogsDF.select(col("Hits")).show(2)
blogsDF.select("Hits").show(2)

// COMMAND ----------

blogsDF.sort(col("Id").desc).show()
blogsDF.sort($"Id".desc).show()

// COMMAND ----------


