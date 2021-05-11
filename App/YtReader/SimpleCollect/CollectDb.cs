using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Snowflake.Data.Client;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using YtReader.Store;
using YtReader.Yt;

namespace YtReader.SimpleCollect {

  public record CollectDbCtx(ILoggedConnection<SnowflakeDbConnection> Db, Platform Platform) : IDisposable {
    public void Dispose() => Db?.Dispose();
  }
  
  public static class CollectDb {
    static string SqlList(IReadOnlyCollection<Channel> channels) => channels.Join(",", c => c.ChannelId.SingleQuote());
    
    public static async Task<IReadOnlyCollection<DiscoverChannelOrVid>> DiscoverChannelsAndVideos(this CollectDbCtx ctx) => 
      await ctx.Db.Query<DiscoverChannelOrVid>("discover new channels and videos", $@"
select distinct link_type LinkType, link_id LinkId, platform_from FromPlatform
from link_detail l
       left join parler_posts p on l.post_id_from=p.id
where not link_found
  and platform_to=:platform
  -- for posts only take those that have Q tags
  and (post_id_from is null or arrays_overlap(array_construct('qanon','q','qanons','wwg1wga','wwg','thegreatawakening'),p.hashtags))
", new { platform=ctx.Platform.EnumString()});

    public static async Task<IReadOnlyCollection<Channel>> ExistingChannels(this CollectDbCtx ctx,
      IReadOnlyCollection<string> explicitIds = null) {
      var existing = await ctx.Db.Query<string>(@"get channels", @$"
with latest as (
  -- use channel table as a filter for staging data. it filters out bad records.
  select channel_id, updated from channel_latest
  where platform=:platform
  {(explicitIds.HasItems() ? $"and channel_id in ({explicitIds.Join(",", c => $"'{c}'")})" : "")}
)
select s.v
from latest c join channel_stage s on s.v:ChannelId = c.channel_id and s.v:Updated = c.updated
", new { platform=ctx.Platform.EnumString()});
      return existing.Select(e => e.ToObject<Channel>(IJsonlStore.JCfg)).ToArray();
    }
    
    //public record VideoPlan(string ChannelId, string VideoId, DateTime Updated, DateTime UploadDate, DateTime ExtraUpdated);
  
    public static async Task<VideoForUpdate[]> VideoPlans(this CollectDbCtx ctx, Channel[] channels) {
      var ids = await ctx.Db.QueryAsync<VideoForUpdate>("videos for update", $@"
  with chans as (
    select channel_id
    from channel_latest
    where status_msg<>'Dead'
      and channel_id in ({SqlList(channels)})
  )
  select v.channel_id ChannelId
       , v.video_id VideoId
       , v.source_id SourceId
       , v.updated Updated
       , v.upload_date UploadDate
       , v.extra_updated ExtraUpdated
  from video_latest v
  join chans c on v.channel_id = c.channel_id
  where v.error_type is null -- removed video's updated separately
and platform = :platform
  ", new { platform = ctx.Platform.EnumString()}).ToArrayAsync();
      return ids;
    }
  }


      
}