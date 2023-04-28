// Databricks notebook source

## Patryk Sroka 406788

// COMMAND ----------

//Zadanie 1
import org.apache.spark.sql.functions._

val df = spark.read.format("json")
  .option("multiline", "true")
  .load("dbfs:/FileStore/tables/badjson.json")
  .selectExpr(
    "jobDetails.jobId",
    "jobDetails.jobName",
    "jobDetails.jobType",
    "jobDetails.workspaceId",
    "jobDetails.changesTimestamp",
    "numberOfFeatures",
    "explode(features) as feature",
    "maiaExtractProcessDetails",
    "feature.geometry",
    "feature.properties.featureGroup as featureGroup"
  )

display(df)
