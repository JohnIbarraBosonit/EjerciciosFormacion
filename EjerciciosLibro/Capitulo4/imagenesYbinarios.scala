// Databricks notebook source
import org.apache.spark.ml.source.image

// COMMAND ----------

val imgDir = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val imagenDF = spark.read.format("image").load(imgDir)

// COMMAND ----------

imagenDF.printSchema

// COMMAND ----------

imagenDF.select("image.height","image.width", "image.nChannels","image.mode").show(5, false)

// COMMAND ----------

//Lectura de binario
val path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val bin_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.jpg").option("recursiveFile", "true").load(path)
bin_df.show(5)

// COMMAND ----------


