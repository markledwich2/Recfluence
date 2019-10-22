// Databricks notebook source
// MAGIC %run ./Common

// COMMAND ----------

// because the colleciton of reocmmends has not been even (e.g. I have changed to record more often, and they are lumpy depending on when videos are first found) 
// the figures should be calculated as a portion of that days collection

import org.apache.spark.sql.expressions.Window._

val removeDoubleQuotes = udf((s:String) => s.replace("\"","'")) // tableau doesn't handle escaping quotes in csv. So just replace with single quotes

val recommendsTable = recommends
  .filter('Rank <= 10)
  .join(channels.select('ChannelId as 'FromChannelId, 'Title as 'FromChannelTitle, 'LR as 'FromLR), "FromChannelId")
  .join(channels.select('ChannelId as 'ChannelId, 'Title as 'ChannelTitle, 'LR as 'ToLR), "ChannelId")
  .join(videos.select('VideoId, removeDoubleQuotes('Title) as 'Title, 'PublishedAt, 'Views), "VideoId")
  .join(videos.select('VideoId as 'FromVideoId, 'Views as 'FromViews), "FromVideoId")

// calculate the number of video views at the granularity of a from video
val videoRecommends = recommendsTable.groupBy('FromVideoId)
  .agg(count(lit(1)) as "VideoRecommendsCount")
  .join(videos.select('VideoId as 'FromVideoId, 'Views as 'VideoViews), "FromVideoId")
  .withColumn("RecommendationViewPortion", 'VideoViews / 'VideoRecommendsCount) // the number of views given to a single recommendation from this video

val dailyRecommends = recommendsTable
  .groupBy('UpdatedAt).agg(count(lit(1)) as 'Recommends)

val dailyVideoRecommends = recommendsTable
  .join(videoRecommends, "FromVideoId")
  .groupBy('FromChannelTitle, 'FromChannelId, 'ChannelTitle, 'ChannelId, 'VideoId, 'Title, 'UpdatedAt, 'PublishedAt)
  .agg(
      count(lit(1)) as 'Recommends,
      sum('RecommendationViewPortion) as "RecommendsViewFlow", // the views to this video if everyone click one of the top 10 recommends randomly
      avg('FromViews) as "FromViews",
      avg('Views) as "Views" // number of views for this video. Don't just sum this, as it is not at the correct granularity
  )  

// COMMAND ----------

dailyVideoRecommends.write.format("delta").mode("overwrite").saveAsTable("DailyVideoRecommends")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC optimize DailyVideoRecommends

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(*), sum(RecommendsViewFlow) as ViewedRecommends from DailyVideoRecommends
// MAGIC where FromChannelId = 'UCm5CkXzGXb-A2XX0nuTctMQ'

// COMMAND ----------

videos.write.format("delta").mode("overwrite").saveAsTable("Videos")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select sum(Views) from Videos
// MAGIC where ChannelId = 'UCm5CkXzGXb-A2XX0nuTctMQ'

// COMMAND ----------

saveSingleCsv(spark.table("Videos"), s"$resultPath/$resultDir/Videos")

// COMMAND ----------

saveSingleCsv(spark.table("DailyVideoRecommends"), s"$resultPath/$resultDir/DailyVideoRecommends")

// COMMAND ----------


