// Databricks notebook source
val t1 = Array(35,36,32,30,40,42,38)
val t2 = Array(31,32,34,55,56)
val tC = Seq(t1,t2).toDF("Celsius")
tC.createOrReplaceTempView("tC")

// COMMAND ----------

tC.show()


// COMMAND ----------

spark.sql("""SELECT Celsius, transform(Celsius, t->((t*9) div 5)+32) as Fahrenheit FROM tC""").show()

// COMMAND ----------

spark.sql("""SELECT Celsius, filter(celsius, t-> t > 38) as high FROM tC""").show()

// COMMAND ----------

spark.sql("""SELECT Celsius, exists(Celsius, t -> t = 38) as threshold FROM tC""").show()

// COMMAND ----------

spark.sql("""
SELECT celsius, 
 reduce(
 celsius, 
 0, 
 (t, acc) -> t + acc, 
 acc -> (acc div size(celsius) * 9 div 5) + 32
 ) as avgFahrenheit 
 FROM tC
""").show()

// COMMAND ----------

spark.sql("SELECT celsius, reduce(celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit FROM tC").show()

// COMMAND ----------


