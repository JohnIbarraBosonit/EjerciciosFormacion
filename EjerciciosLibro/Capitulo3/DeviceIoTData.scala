// Databricks notebook source
//Creamos la clase deviceIoTdata
case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)

// COMMAND ----------

//Leemos el fichero JSON
val ds = spark.read.json("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json").as[DeviceIoTData]

// COMMAND ----------

//Mostramos 5 registros sin truncamiento
ds.show(5, false)

// COMMAND ----------

//Filtramos aquellos registros cuya temperatura es superior a 30 y la humedad es mayor que 70
val filter = ds.filter{x => (x.temp > 30 && x.humidity > 70)}

// COMMAND ----------

filter.show()

// COMMAND ----------

//Realizamos el mismo proceso para la clase temperatura por pais
case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,cca3: String)

val dsTemp = ds
 .filter(d => {d.temp > 25})
 .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
 .toDF("temp", "device_name", "device_id", "cca3").as[DeviceTempByCountry]
dsTemp.show(5, false)

// COMMAND ----------

//Mostramos el primer dato
val device = dsTemp.first()
println(device)

// COMMAND ----------

val dsTemp2 = ds
 .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
 .where("temp > 25")
 .as[DeviceTempByCountry]

// COMMAND ----------

//Misma consulta usando el API dataframes
dsTemp2.show(5, false)
