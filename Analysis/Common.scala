// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

var analysisFile = dbutils.fs.ls("/mnt/ytblob/analysis").sortBy(_.path).reverse.head
val analysisPath = analysisFile.path
val resultDir = analysisFile.name
val resultPath = s"/mnt/ytblob/results"

lazy val channels = spark.read.parquet(s"$analysisPath/Channels.parquet").cache
lazy val recommends = spark.read.parquet(s"$analysisPath/Recommends.*.parquet").cache
lazy val videos = spark.read.parquet(s"$analysisPath/Videos.*.parquet").cache

def saveSingleCsv(df:Dataset[Row], path:String):String = {
  df.coalesce(1) 
  .write.mode("overwrite")
    .option("header", "true")
    .option("quoteMode", "ALL")
    .option("compression","gzip")
  .csv(s"$path")

  var outCsv = dbutils.fs.ls(s"$path").filter(_.name.endsWith("csv.gz")).head.path
  var finalPath = s"$path.csv.gz"
  dbutils.fs.mv(outCsv, finalPath)
  dbutils.fs.rm(s"$path", true)
  s"$finalPath.csv"
}


def saveSinglePqt(df:Dataset[Row], path:String):String = { 
  df.coalesce(1) 
  .write.mode("overwrite")
  .parquet(s"$path")

  var res = s"$path.parquet"
  var outPqt = dbutils.fs.ls(s"$path").filter(_.name.endsWith("parquet")).head.path
  dbutils.fs.mv(outPqt, res)
  dbutils.fs.rm(s"$path", true)
  res
}

// COMMAND ----------


