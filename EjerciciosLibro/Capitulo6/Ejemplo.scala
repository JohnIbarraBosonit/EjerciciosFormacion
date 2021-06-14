// Databricks notebook source
import scala.util.Random._
import org.apache.spark.sql.functions._

// COMMAND ----------

case class Usage(uid:Int, uname:String, usage: Int)

// COMMAND ----------

val r = new scala.util.Random(42)

// COMMAND ----------

val data = for(i <- 0 to 1000)
  yield(Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

// COMMAND ----------

val dsUsage = spark.createDataset(data)

// COMMAND ----------

dsUsage.show(10)

// COMMAND ----------

dsUsage.filter(x => x.usage > 900).orderBy(desc("usage")).show(5, false)

// COMMAND ----------

dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50}).show(5, false)

// COMMAND ----------

case class UsageCost(uid: Int, uname:String, suage: Int, cost:Double)

// COMMAND ----------

def computerCostUsage(u: Usage): UsageCost = {
  val v = if(u.usage > 750) u.usage * 0.15 else u.usage * 0.5
  UsageCost(u.uid, u.uname, u.usage, v)
}

// COMMAND ----------

dsUsage.map(u => {computerCostUsage(u)}).show(5)

// COMMAND ----------

val bloggers = "../data/bloggers.json"
val bloggersDS = spark
 .read
 .format("json")
 .option("path", bloggers)
 .load()
 .as[Bloggers]

// COMMAND ----------


