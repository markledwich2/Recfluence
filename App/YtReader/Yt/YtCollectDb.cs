using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;

// ReSharper disable InconsistentNaming

namespace YtReader.Yt {
  public record YtCollectDbCtx(YtCollectCfg Cfg, ILoggedConnection<IDbConnection> Db, ILogger Log) : IDisposable {
    public void Dispose() => Db?.Dispose();
  }

  public record ChannelUpdatePlan {
    public Channel           Channel           { get; set; }
    public UpdateChannelType Update            { get; set; }
    public DateTime?         VideosFrom        { get; set; }
    public DateTime?         LastVideoUpdate   { get; set; }
    public DateTime?         LastCaptionUpdate { get; set; }
    public DateTime?         LastCommentUpdate { get; set; }
    public DateTime?         LastRecUpdate     { get; set; }
  }

  public static class YtCollectDb {
    public static string SqlList<T>(IEnumerable<T> items) => items.Join(",", i => i.ToString().SingleQuote());

    /// <summary>Existing reviewed channels with information on the last updates to extra parts.
    ///   <param name="channelSelect">By default will return channels that meet review criteria. To override, specify a select
    ///     query that returns rows with a column named channel_id</param>
    /// </summary>
    public static async Task<IReadOnlyCollection<ChannelUpdatePlan>> ChannelUpdateStats(this YtCollectDbCtx ctx,
      IReadOnlyCollection<string> chans = null, string channelSelect = null) {
      channelSelect ??= @$"
select channel_id from channel_latest  
where platform = 'YouTube' and {(chans.None() ? "meets_review_criteria" : $"channel_id in ({SqlList(chans)})")}";

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
        VideosFrom = r.daysBack != null ? DateTime.UtcNow - r.daysBack.Value.Days() : (DateTime?) null,
        LastVideoUpdate = r.lastVideoUpdate,
        LastCaptionUpdate = r.lastCaptionUpdate,
        LastRecUpdate = r.lastRecUpdate,
        LastCommentUpdate = r.lastCommentUpdate
      }).ToArray();
    }

    public static Task<IReadOnlyCollection<string>> MissingUsers(this YtCollectDbCtx ctx) =>
      ctx.Db.Query<string>("missing users", @$"
select distinct author_channel_id
from comment t
join video_latest v on v.video_id = t.video_id
where
 platform = 'YouTube'
 and not exists(select * from user where user_id = author_channel_id)
{ctx.Cfg.MaxMissingUsers?.Do(i => $"limit {i}") ?? ""}
");

    /// <summary>Videos to help plan which to refresh extra for (we don't actualy refresh every one of these). We detect dead
    ///   videos by having all non dead video's at hand since daily_update_days_back, and the date extra was last refreshed for
    ///   it</summary>
    public static async Task<IReadOnlyCollection<VideoForUpdate>> VideosForUpdate(this YtCollectDbCtx ctx, IReadOnlyCollection<Channel> channels) {
      var ids = await ctx.Db.Query<VideoForUpdate>("videos for update", $@"
with chans as (
  select channel_id
  from channel_accepted
  where status_msg<>'Dead'
    and channel_id in ({channels.Join(",", c => $"'{c.ChannelId}'")})
)
select v.channel_id ChannelId
     , v.video_id VideoId
     , v.updated Updated
     , v.upload_date UploadDate
     , v.extra_updated ExtraUpdated
from video_latest v
join chans c on v.channel_id = c.channel_id
join channel_collection_days_back b on b.channel_id = v.channel_id
where 
    v.upload_date is null -- update extra if we are missing upload
    or v.error_type is null -- removed video's updated separately
qualify row_number() over (partition by b.channel_id order by upload_date desc) <= :videosPerChannel
", new {videosPerChannel = ctx.Cfg.MaxChannelFullVideos});
      return ids;
    }

    public static async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> MissingComments(this YtCollectDbCtx ctx,
      IReadOnlyCollection<Channel> channels) =>
      await ctx.Db.Query<(string ChannelId, string VideoId)>("missing comments", $@"
select channel_id, video_id
from video_latest v
where not exists(select * from comment_stage c where c.v:VideoId=v.video_id) and error_type is null
qualify row_number() over (partition by channel_id order by random() desc)<=:max_comments
  and channel_id in ({channels.Join(",", c => $"'{c.ChannelId}'")})
                ", new {max_comments = ctx.Cfg.MaxChannelComments});

    public static async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> MissingCaptions(this YtCollectDbCtx ctx,
      IReadOnlyCollection<Channel> channels) =>
      await ctx.Db.Query<(string ChannelId, string VideoId)>("missing captions", $@"
                select channel_id, video_id
                from video_latest v
                where not exists(select * from caption_stage c where c.v:VideoId=v.video_id)
                  qualify row_number() over (partition by channel_id order by views desc)<=400
                    and channel_id in ({channels.Join(",", c => $"'{c.ChannelId}'")})
                ");

    public static async Task<ChannelUpdatePlan[]> ChannelsToDiscover(this YtCollectDbCtx ctx) {
      var toAdd = await ctx.Db.Query<(string channel_id, string channel_title, string source)>("channels to classify",
        @"with review_channels as (
  select channel_id
       , channel_title -- probably missing values. reviews without channels don't have titles
  from channel_review r
  where not exists(select * from channel_stage c where c.v:ChannelId::string=r.channel_id)
)
   , rec_channels as (
  select to_channel_id as channel_id, any_value(to_channel_title) as channel_title
  from rec r
  where to_channel_id is not null
    and not exists(select * from channel_stage c where c.v:ChannelId::string=r.to_channel_id)
  group by to_channel_id
)
   , s as (
  select channel_id, channel_title, 'review' as source
  from review_channels sample (:remaining rows)
  union all
  select channel_id, channel_title, 'rec' as source
  from rec_channels sample (:remaining rows)
)
select *
from s
limit :remaining", new {remaining = ctx.Cfg.DiscoverChannels});

      ctx.Log.Debug("Collect - found {Channels} new channels for discovery", toAdd.Count);

      var toDiscover = toAdd
        .Select(c => new ChannelUpdatePlan {
          Channel = new() {
            ChannelId = c.channel_id,
            ChannelTitle = c.channel_title
          },
          Update = c.source == "review" ? UpdateChannelType.Standard : UpdateChannelType.Discover
        })
        .ToArray();

      return toDiscover;
    }
  }
}