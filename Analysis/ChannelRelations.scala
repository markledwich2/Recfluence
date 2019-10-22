// Databricks notebook source
// MAGIC %run ./Common

// COMMAND ----------

// DBTITLE 1,Recommendations Between Visible Channels
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
  .join(videos.groupBy('ChannelId).agg(sum('Views) as 'ChannelVideoViews, min('PublishedAt) as 'PublishedFrom, max('PublishedAt) as 'PublishedTo), "ChannelId").cache

lazy val recommendsWithinSeeds = recommends
  .join(visChannels.select('ChannelId), "ChannelId")
  .join(visChannels.select('ChannelId as 'FromChannelId), "FromChannelId")
        
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
      max('UpdatedAt) as 'UpdatedAt
  )
  .join(visChannels.select('ChannelId as 'FromChannelId, 'Title as 'FromChannelTitle, 'ChannelVideoViews), "FromChannelId")
  .join(visChannels.select('ChannelId, 'Title as 'ChannelTitle), "ChannelId")
  .join(recommends.groupBy('FromChannelId).agg(count(lit(1)) as "ChannelRecommendsCount"), "FromChannelId")
  .withColumn("RecommendsPercent", 'Recommends / 'ChannelRecommendsCount)
  .withColumn("RecommendsFlowPercent", 'RecommendsViewFlow / 'ChannelVideoViews)
  .drop('ChannelRecommendsCount).drop('ChannelVideoViews)
}

lazy val visRelations = relations(recommendsWithinSeeds)

// COMMAND ----------

// DBTITLE 1,Update Spark Tables
visChannels.write.format("delta").mode("overwrite").saveAsTable("Channels")
visRelations.write.format("delta").mode("overwrite").saveAsTable("ChannelRelations")
videos.write.format("delta").mode("overwrite").saveAsTable("Videos")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from VizRelations

// COMMAND ----------

// DBTITLE 1,Save Final Vis Csv's
saveSingleCsv(spark.table("Channels"), s"/mnt/ytblob/results/$resultDir/Channels")
saveSingleCsv(spark.table("VizRelations"), s"/mnt/ytblob/results/$resultDir/Relations")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select date_format(v.PublishedAt, "yyyy-MM") as Month, c.Title, SUM(Views) as Views
// MAGIC from Videos v
// MAGIC left join Channels c on c.ChannelId = v.ChannelId
// MAGIC where c.Title = 'Fox News'
// MAGIC group by v.ChannelId, c.Title, date_format(v.PublishedAt, "yyyy-MM")
// MAGIC order by Month asc

// COMMAND ----------


