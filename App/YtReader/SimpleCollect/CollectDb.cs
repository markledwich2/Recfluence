using System.Data;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using YtReader.Store;
using YtReader.Yt;

namespace YtReader.SimpleCollect; 

public record CollectDbCtx(ILoggedConnection<IDbConnection> Db, Platform Platform, ICommonCollectCfg Cfg) : IDisposable {
  public void Dispose() => Db?.Dispose();
}

public static class CollectDb {
  public static string SqlList<T>(this IEnumerable<T> items) => items.Join(",", i => i.ToString().SingleQuote());
  static string SqlList(this IReadOnlyCollection<Channel> channels) => channels.Join(",", c => c.ChannelId.SingleQuote());

  /// <summary>Existing reviewed channels with information on the last updates to extra parts.
  ///   <param name="channelSelect">By default will return channels that meet review criteria. To override, specify a select
  ///     query that returns rows with a column named channel_id</param>
  /// </summary>
  public static async Task<IReadOnlyCollection<ChannelUpdatePlan>> ChannelUpdateStats(this CollectDbCtx ctx,
    IReadOnlyCollection<string> chans = null, string channelSelect = null) {
    channelSelect ??= @$"
select channel_id from channel_latest  
where platform = '{ctx.Platform.EnumString()}' and status <> 'Dupe' and {(chans.None() ? "meets_review_criteria" : $"channel_id in ({SqlList(chans)})")}";

    var channels = await ctx.Db.Query<(string j, long? daysBack,
      DateTime? lastVideoUpdate, DateTime? lastCaptionUpdate, DateTime? lastRecUpdate, DateTime? lastCommentUpdate)>(
      "channels - previous",
      $@"
with channels_raw as (
  select distinct channel_id from ({channelSelect})
  where channel_id is not null
)
, stage_latest as (
  select v
  from channel_stage -- query from stage because it can be deserialized without modification
  where exists(select * from channels_raw r where r.channel_id=v:ChannelId)
    qualify row_number() over (partition by v:ChannelId::string order by v:Updated::timestamp_ntz desc)=1
)
select coalesce(v, object_construct('ChannelId', r.channel_id)) channel_json
     , b.daily_update_days_back
     , (select max(v:Updated::timestamp_ntz) from video_stage where v:ChannelId=r.channel_id) last_video_update
     , (select max(v:Updated::timestamp_ntz) from caption_stage where v:ChannelId=r.channel_id) last_caption_update
     , (select max(v:Updated::timestamp_ntz) from rec_stage where v:FromChannelId=r.channel_id) last_rec_update
      , (select max(v:Updated::timestamp_ntz) from comment_stage where v:ChannelId=r.channel_id) last_comment_update
from channels_raw r
       left join stage_latest on v:ChannelId=r.channel_id
       left join channel_collection_days_back b on b.channel_id=v:ChannelId
");
    return channels.Select(r => new ChannelUpdatePlan {
      Channel = r.j.ToObject<Channel>(IJsonlStore.JCfg),
      VideosFrom = r.daysBack != null ? DateTime.UtcNow - r.daysBack.Value.Days() : null,
      LastVideoUpdate = r.lastVideoUpdate,
      LastCaptionUpdate = r.lastCaptionUpdate,
      LastRecUpdate = r.lastRecUpdate,
      LastCommentUpdate = r.lastCommentUpdate
    }).ToArray();
  }

  /// <summary>Videos to help plan which to refresh extra for (we don't actualy refresh every one of these). We detect dead
  ///   videos by having all non dead video's at hand since daily_update_days_back, and the date extra was last refreshed for
  ///   it</summary>
  public static async Task<IReadOnlyCollection<VideoForUpdate>> VideosForUpdate(this CollectDbCtx ctx, IReadOnlyCollection<Channel> channels) {
    var ids = await ctx.Db.Query<VideoForUpdate>("videos for update", $@"
with chans as (
  select channel_id
  from channel_latest
  where status_msg<>'Dead'
    and channel_id in ({SqlList(channels)})
)
select v.channel_id ChannelId
     , v.video_id VideoId
     , v.updated
     , v.upload_date UploadDate
     , v.extra_updated ExtraUpdated
      , exists(select * from comment t where t.video_id = v.video_id) HasComment
      , v.source_id SourceId
     , v.platform
from video_latest v
join chans c on v.channel_id = c.channel_id
where v.error_type is null -- removed video's updated separately
and platform = :platform
qualify row_number() over (partition by v.channel_id order by upload_date desc) <= :videosPerChannel
", new {platform = ctx.Platform.EnumString(), videosPerChannel = ctx.Cfg.MaxChannelFullVideos});
    return ids;
  }

  public static async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> MissingComments(this CollectDbCtx ctx,
    IReadOnlyCollection<Channel> channels) =>
    await ctx.Db.Query<(string ChannelId, string VideoId)>("missing comments", $@"
select channel_id, video_id
from video_latest v
where not exists(select * from comment_stage c where c.v:VideoId=v.video_id) and error_type is null
qualify row_number() over (partition by channel_id order by random() desc)<=:max_comments
  and channel_id in ({SqlList(channels)})
                ", new {max_comments = ctx.Cfg.MaxChannelComments});

  public static async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> MissingCaptions(this CollectDbCtx ctx,
    IReadOnlyCollection<Channel> channels) =>
    await ctx.Db.Query<(string ChannelId, string VideoId)>("missing captions", $@"
                select channel_id, video_id
                from video_latest v
                where not exists(select * from caption_stage c where c.v:VideoId=v.video_id)
                  qualify row_number() over (partition by channel_id order by views desc)<=400
                    and channel_id in ({SqlList(channels)})
                ");
}