using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Mutuo.Etl.Db;
using Serilog;
using SysExtensions;
using YtReader.SimpleCollect;
using YtReader.Store;

// ReSharper disable InconsistentNaming

namespace YtReader.Yt; 

public record YtCollectDbCtx(YtCollectCfg YtCfg, ILoggedConnection<IDbConnection> Db, ILogger Log) : CollectDbCtx(Db, Platform.YouTube, YtCfg);

public record ChannelUpdatePlan {
  public Channel           Channel           { get; set; }
  public UpdateChannelType Update            { get; set; }
  public DateTime?         VideosFrom        { get; set; }
  public DateTime?         LastVideoUpdate   { get; set; }
  public DateTime?         LastCaptionUpdate { get; set; }
  public DateTime?         LastCommentUpdate { get; set; }
  public DateTime?         LastRecUpdate     { get; set; }

  public string ChannelId => Channel.ChannelId;
}

public static class YtCollectDb {
  public static Task<IReadOnlyCollection<string>> MissingUsers(this YtCollectDbCtx ctx) =>
    ctx.Db.Query<string>("missing users", @$"
select distinct author_channel_id
from comment t
join video_latest v on v.video_id = t.video_id
where
 platform = 'YouTube'
 and not exists(select * from user where user_id = author_channel_id)
{ctx.YtCfg.MaxMissingUsers.Do(i => $"limit {i}")}
");

  public static async Task<ChannelUpdatePlan[]> DiscoverChannelsViaRecs(this YtCollectDbCtx ctx) {
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
limit :remaining", new {remaining = ctx.YtCfg.DiscoverChannels});

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