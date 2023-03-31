// Databricks notebook source
// MAGIC %md
// MAGIC Patryk Sroka 406788

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/weather"))

// COMMAND ----------

display(spark.read.text("dbfs:/databricks-datasets/weather/high_temps"))

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType,IntegerType,StringType,StructType,StructField,ArrayType}

val schema = StructType(Array(StructField("date", DateType, true), StructField("temp", IntegerType, true)))
val filePath = "dbfs:/databricks-datasets/weather/high_temps"

val dataFrame = spark.read.format("csv")
  .option("header", "true")
  .schema(schema)
  .load(filePath)

// COMMAND ----------

display(dataFrame)

// COMMAND ----------

// MAGIC %md 
// MAGIC Zad 3 - tworzenie pliku JSON

// COMMAND ----------

val jsonStr = """
[
{
  "date": "2020-01-01",
  "temp": 10
},
{
  "date": "2020-01-02",
  "temp": 14
},
{
  "date": "2020-01-03",
  "temp": 15
},
{
  "date": "2020-01-04",
  "temp": 9
}
]
"""

val jsonSet = spark.read.json(spark.sparkContext.parallelize(jsonStr :: Nil))
jsonSet.write.mode("overwrite").json("set.json")

val schemaJson = StructType(Array(StructField("date", DateType, true), StructField("temp", IntegerType, true)))
val jsonDf = spark.read.schema(schemaJson).json("dbfs:/set.json")

display(jsonDf)

// COMMAND ----------

// MAGIC %md
// MAGIC Zad 4 - użycie Read Modes

// COMMAND ----------

val badSchema = StructType(Array(StructField("date", IntegerType, true), StructField("temp", IntegerType, true)))

val filePath = "dbfs:/databricks-datasets/weather/high_temps"

// without mode
val badDF1 = spark.read.format("csv")
  .option("header", "true")
  .schema(badSchema)
  .load(filePath)

//display(badDF1)

// permissive - null
val badDF2 = spark.read.format("csv")
  .option("header", "true")
  .schema(badSchema)
  .option("mode", "PERMISSIVE")
  .load(filePath)

display(badDF2)

// failfast - stop reading process
val badDF3 = spark.read.format("csv")
  .option("header", "true")
  .schema(badSchema)
  .option("mode", "FAILFAST")
  .load(filePath)

//display(badDF3)

// dropmalformed - no results - all are dropped
val badDF4 = spark.read.format("csv")
  .option("header", "true")
  .schema(badSchema)
  .option("mode", "DROPMALFORMED")
  .load(filePath)

//display(badDF4)

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 5 - użycie DataFrameWriter i DataFrameReader

// COMMAND ----------

dataFrame.write.mode("overwrite").parquet("df_parquet.parquet")
val dfParquet = spark.read.parquet("dbfs:/df_parquet.parquet")

display(dfParquet)

// COMMAND ----------

dataFrame.write.mode("overwrite").json("df_json.json")
val dfJson = spark.read.json("dbfs:/df_json.json")

display(dfJson)

// COMMAND ----------


