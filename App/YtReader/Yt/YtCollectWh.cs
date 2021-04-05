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
using YtReader.Store;

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
    public DateTime?         LastRecUpdate     { get; set; }
  }
  
  public static class YtCollectWh {
    public static string SqlList<T>(IEnumerable<T> items) => items.Join(",", i => i.ToString().SingleQuote());
    
    public static Task<IReadOnlyCollection<string>> StaleOrCaptionlessChannels(this YtCollectDbCtx ctx,CollectOptions options, TimeSpan refreshPeriod) =>
      ctx.Db.Query<string>("stale or captionless channels", @$"
with
  raw_vids as ({@GetVideoChannelSelect(options)})
  , chans as (
      select * from (
        select distinct coalesce(d.channel_id,v.channel_id) channel_id
        from raw_vids d
               left join video_latest v on v.video_id=d.video_id
      ) c
      {(options.LimitChannels.None() ? "" : $"where c.channel_id in ({SqlList(options.LimitChannels)})")}
  )
  , chan_refresh as (
  select c.channel_id
       , count(*) videos
       , count_if(v.updated > :newer_than) recent_videos
       , div0(count_if(s.video_id is null), videos) caption_not_attempted_pct
  from chans c
         left join video_latest v on v.channel_id=c.channel_id
         left join caption_load s on s.video_id=v.video_id
  group by 1
  having caption_not_attempted_pct > 0.2 or recent_videos = 0
)
select channel_id from chan_refresh
", new {newer_than = DateTime.UtcNow - refreshPeriod});

    public static async Task<IKeyedCollection<string, ChannelUpdatePlan>> ExistingChannels(this YtCollectDbCtx ctx, HashSet<string> explicitChannels) {
      var channels = await ctx.Db.Query<(string j, long? daysBack, DateTime? lastVideoUpdate, DateTime? lastCaptionUpdate, DateTime? lastRecUpdate)>(
        "channels - previous",
        $@"
with review_filtered as (
  select channel_id, channel_title
  from channel_latest
  where platform = 'YouTube'
  and {(explicitChannels.None() ? "meets_review_criteria" : $"channel_id in ({SqlList(explicitChannels)})")} --only explicit channel when provided
)
   , stage_latest as (
  select v
  from channel_stage -- query from stage because it can be deserialized without modification
  where exists(select * from review_filtered r where r.channel_id=v:ChannelId)
    qualify row_number() over (partition by v:ChannelId::string order by v:Updated::timestamp_ntz desc)=1
)
, s as (
select coalesce(v, object_construct('ChannelId', r.channel_id)) channel_json
     , b.daily_update_days_back
     , (select max(vs.v:Updated::timestamp_ntz) from video_stage vs where vs.v:ChannelId=r.channel_id) last_video_update
     , (select max(cs.v:Updated::timestamp_ntz) from caption_stage cs where cs.v:ChannelId=r.channel_id) last_caption_update
     , (select max(rs.v:Updated::timestamp_ntz) from rec_stage rs where rs.v:FromChannelId=r.channel_id) last_rec_update
from review_filtered r
       left join stage_latest on v:ChannelId=r.channel_id
       left join channel_collection_days_back b on b.channel_id=v:ChannelId
  )
select * from s
");
      return channels.Select(r => new ChannelUpdatePlan {
          Channel = r.j.ToObject<Channel>(IJsonlStore.JCfg),
          VideosFrom = r.daysBack != null ? DateTime.UtcNow - r.daysBack.Value.Days() : (DateTime?) null,
          LastVideoUpdate = r.lastVideoUpdate,
          LastCaptionUpdate = r.lastCaptionUpdate,
          LastRecUpdate = r.lastRecUpdate
        })
        .ToKeyedCollection(r => r.Channel.ChannelId);
    }
    
    
    public record DiscoverRow(string video_id, string channel_id, DateTime? extra_updated, bool caption_exists);
    
    static string GetVideoChannelSelect(CollectOptions options) {
      var (type, value) = options.CollectFrom;
      var select = type switch {
        CollectFromType.ChannelsPath => $"select null video_id, $1::string channel_id from @public.yt_data/{value} (file_format => tsv)",
        CollectFromType.VideosPath =>
          $"select $1::string video_id, $2::string channel_id  from @public.yt_data/{value} (file_format => tsv)",
        CollectFromType.VideosView => $"select video_id, channel_id from {value}",
        _ => throw new NotImplementedException($"CollectFrom {options.CollectFrom} not supported")
      };
      return @select;
    }

    public static Task<IReadOnlyCollection<DiscoverRow>> MissingVideos(this YtCollectDbCtx db, CollectOptions options) =>
      db.Db.Query<DiscoverRow>("missing video's", @$"
with raw_vids as ({GetVideoChannelSelect(options)})
, s as (
  select v.video_id
       , coalesce(v.channel_id, e.channel_id) channel_id
       , e.updated extra_updated
       , exists(select * from caption s where s.video_id=v.video_id) caption_exists
  from raw_vids v
         left join video_extra e on v.video_id=e.video_id
  where v.video_id is not null and extra_updated is null
)
select * from s
{(options.LimitChannels.None() ? "" : $"where channel_id in ({SqlList(options.LimitChannels)})")}
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

    /// <summary>Find videos that we should update to collect comments (chrome update). We do this once x days after a video is
    ///   uploaded.</summary>
    public static async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> VideosForCommentUpdate(this YtCollectDbCtx ctx,
      IReadOnlyCollection<Channel> channels) {
      var ids = await ctx.Db.Query<(string ChannelId, string VideoId)>("videos sans-comments",
        $@"with chrome_extra_latest as (
  select video_id
       , updated
       , row_number() over (partition by video_id order by updated desc) as age_no
       , count(1) over (partition by video_id) as extras
  from video_extra v
  where source='Chrome'
    qualify age_no=1
)
   , videos_to_update as (
  select *
       , row_number() over (partition by channel_id order by views desc) as channel_rank
  from (
         select v.video_id
              , v.channel_id
              , v.views
              , datediff(d, e.updated, convert_timezone('UTC', current_timestamp())) as extra_ago
              , datediff(d, v.upload_date, convert_timezone('UTC', current_timestamp())) as upload_ago

         from video_latest v
                left join chrome_extra_latest e on e.video_id=v.video_id
         where v.channel_id in ({channels.Join(",", c => $"'{c.ChannelId}'")})
           and upload_ago>7
           and e.updated is null -- update only 7 days after being uploaded
       )
    qualify channel_rank<=:videos_per_channel
)
select channel_id, video_id
from videos_to_update",
        new {videos_per_channel = ctx.Cfg.PopulateMissingCommentsLimit});
      return ids;
    }

    public static async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> MissingCaptions(this YtCollectDbCtx ctx,
      IReadOnlyCollection<Channel> channels) =>
      await ctx.Db.Query<(string ChannelId, string VideoId)>("missing captions", $@"
                select channel_id, video_id
                from video_latest v
                where not exists(select * from caption_stage c where c.v:VideoId=v.video_id)
                  qualify row_number() over (partition by channel_id order by upload_date desc)<=400
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