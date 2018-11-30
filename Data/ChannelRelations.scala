// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

val resultPath = dbutils.fs.ls("/mnt/ytblob/analysis").sortBy(_.path).map(_.path).reverse.head

lazy val channels = spark.read.parquet(s"$resultPath/Channels.parquet").cache
lazy val recommends = spark.read.parquet(s"$resultPath/Recommends.parquet").cache
lazy val videos = spark.read.parquet(s"$resultPath/Videos.parquet").cache


def saveSingleCsv(df:Dataset[Row], path:String):String = { 
  df.coalesce(1) 
  .write.mode("overwrite").option("header", "true") 
  .csv(s"$path")

  var outCsv = dbutils.fs.ls(s"$path").filter(_.name.endsWith("csv")).head.path
  dbutils.fs.mv(outCsv, s"$path.csv")
  dbutils.fs.rm(s"$path", true)
  s"$path.csv"
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC **How recommendation views are calculated**
// MAGIC 
// MAGIC Consider these video recommendations
// MAGIC <table>
// MAGIC   <tr><th>From (channel.video)</th><th>To</th><th>FromVideoViews</th></tr>
// MAGIC   <tr><td>A.1</td><td>B.1</td><td>120</td></tr>
// MAGIC   <tr><td>A.1</td><td>B.2</td><td>120</td></tr>
// MAGIC   <tr><td>A.1</td><td>C.1</td><td>120</td></tr>
// MAGIC </table>
// MAGIC 
// MAGIC 
// MAGIC When aggregating to relations between channels, we want to estimate a portion of the video views from the recommending channel. If you are to add all of the RecommendationViewFlow it will equal the number of from video views
// MAGIC 
// MAGIC <table>
// MAGIC   <tr><th>From (channel)</th><th>To</th><th>RecommendationViewFlow</th></tr>
// MAGIC   <tr><td>A</td><td>B</td><td>80</td></tr>
// MAGIC   <tr><td>A</td><td>C</td><td>40</td></tr>
// MAGIC </table>

// COMMAND ----------

case class ChannelKV(ChannelId:String, Title:String)

lazy val visChannels = channels
  .join(videos.groupBy('ChannelId).agg(sum('Views) as 'ChannelVideoViews, min('PublishedAt) as 'PublishedFrom, max('PublishedAt) as 'PublishedTo), "ChannelId")

val channelIds = sc.broadcast(visChannels.select('ChannelId, 'Title).as[ChannelKV].collect.map(c => (c.ChannelId, c.Title)).toMap)
val channelVisible = udf((ChannelId:String) => channelIds.value.contains(ChannelId))

lazy val recommendsWithinSeeds = recommends.filter(channelVisible('ChannelId) and channelVisible('FromChannelId))

// calculate the number of video views at the granularity of a from video
lazy val videoRecommends = recommendsWithinSeeds.groupBy('FromVideoId)
  .agg(count(lit(1)) as "VideoRecommendsCount")
  .join(videos.select('VideoId as 'FromVideoId, 'Views as 'VideoViews), "FromVideoId")
  .withColumn("RecommendationViewPortion", 'VideoViews / 'VideoRecommendsCount)

def relations(recommends:DataFrame):DataFrame = {
  recommends
  // devide from video views by total reocmmended vies from this video to estimate the portion of views given to this particular reocmmendation 
  .join(videoRecommends, "FromVideoId")
  .groupBy('FromChannelId, 'ChannelId) //'FromChannelTitle, 'ChannelTitle,
  .agg(
     sum('RecommendationViewPortion) as "RecommendsViewFlow",
     count(lit(1)) as "Recommends", 
     countDistinct(concat('VideoId, 'FromVideoId)) as "UniqueRecommends",
      max('UpdatedAt) as 'UpdatedAt
  )
  .join(channels.select('ChannelId as 'FromChannelId, 'Title as 'FromChannelTitle), "FromChannelId")
  .join(channels.select('ChannelId, 'Title as 'ChannelTitle), "ChannelId")
  .join(recommends.groupBy('FromChannelId).agg(count(lit(1)) as "ChannelRecommendsCount"), "FromChannelId")
  .withColumn("RecommendsPercent", 'Recommends / 'ChannelRecommendsCount)
}

lazy val visRelations = relations(recommendsWithinSeeds)

// COMMAND ----------

lazy val tagVideos = videos.groupBy('ChannelId).count().withColumnRenamed("count", "ChannelVideos")
lazy val channelLR = visChannels.select('ChannelId, 'LR)

lazy val videoTags = videos.withColumn("Tag", explode('Tags))
  .groupBy('ChannelId, 'ChannelTitle, 'Tag).agg(count(lit(1)) as 'TagCount)
  .join(channelLR, "ChannelId")
  .groupBy('LR, 'Tag).agg(sum('TagCount) as 'TagCount, sum('ChannelVideos) as 'ChannelVideos)
  .withColumn("TagPercent", 'TagCount / 'ChannelVideos)
  .orderBy('TagPercent.desc)

lazy val lrTags = videos.withColumn("Tag", explode('Tags))
  .groupBy('ChannelId, 'ChannelTitle, 'Tag).agg(count(lit(1)) as 'TagCount)
  .join(channelLR, "ChannelId")
  .groupBy('LR, 'Tag).agg(sum('TagCount) as 'TagCount)
  .orderBy('TagCount.desc)


// COMMAND ----------

lazy val reviewChannels = relations(recommends.filter(!channelVisible('ChannelId)))
  .groupBy('ChannelId, 'ChannelTitle)
  .agg(sum('RecommendsViewFlow) as 'RecommendsViewFlow, avg('RecommendsPercent) as 'AvgRecommendsPercent)
  .withColumn("Url", concat(lit("https://www.youtube.com/channel/"), 'ChannelId))
  .filter('RecommendsViewFlow > 10000)
  .orderBy('AvgRecommendsPercent.desc)

display(reviewChannels)

// COMMAND ----------

visChannels.coalesce(1).write.mode("overwrite").saveAsTable("Channels")
visRelations.coalesce(4).write.mode("overwrite").saveAsTable("Relations")
videos.coalesce(4).write.mode("overwrite").saveAsTable("Videos")
//recommends.write.mode("overwrite").saveAsTable("Recommends")

// COMMAND ----------

visChannels.write.format("delta").mode("overwrite").saveAsTable("Channels2")

// COMMAND ----------

saveSingleCsv(visChannels, s"$resultPath/VisChannels")
saveSingleCsv(visRelations, s"$resultPath/VisRelations")
//saveSingleCsv(videos, s"$resultPath/VisVideos")

// COMMAND ----------

saveSingleCsv(visChannels, s"/mnt/ytblob/results/VisChannels")
saveSingleCsv(visRelations, s"/mnt/ytblob/results/VisRelations")

// COMMAND ----------


