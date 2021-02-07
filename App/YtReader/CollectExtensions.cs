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

namespace YtReader {
  public enum StandardCollectPart {
    Channel,
    DiscoverChannels,
    Video
  }

  public enum LinkType {
    Channel,
    Video
  }

  public record DiscoverChannelOrVid(LinkType LinkType, string LinkId, Platform FromPlatform);

  public static class CollectExtensions {
    public static DiscoverSource ToDiscoverSource(this DiscoverChannelOrVid l) => new(l.LinkType switch {
      LinkType.Channel => ChannelSourceType.ChannelLink,
      LinkType.Video => ChannelSourceType.VideoLink,
      _ => null
    }, l.LinkId, l.FromPlatform);
    
    public static bool ForUpdate(this Channel c, string[] explicitSourceIds = null) {
      var sourceIds = explicitSourceIds?.ToHashSet();
      var enoughSubs = c.Subs == null || c.Subs > 1000;
      var alive = c.Status.NotIn(ChannelStatus.NotFound, ChannelStatus.Blocked, ChannelStatus.Dead);
      return (sourceIds == null || sourceIds.Contains(c.SourceId)) && alive && enoughSubs;
    }

    public static async Task<IReadOnlyCollection<DiscoverChannelOrVid>> DiscoverChannelsAndVideos(this ILoggedConnection<SnowflakeDbConnection> db, Platform platform) {
      var res = await db.Query<DiscoverChannelOrVid>("discover new videos", $@"
select distinct link_type LinkType, link_id LinkId, platform_from FromPlatform
from link_detail l
       left join parler_posts p on l.post_id_from=p.id
where not link_found
  and platform_to='{platform}'
  -- for posts only take those that have Q tags
  and (post_id_from is null or arrays_overlap(array_construct('qanon','q','qanons','wwg1wga','wwg','thegreatawakening'),p.hashtags))
");
      return res;
    }
    
    public static async Task<IReadOnlyCollection<Channel>> ExistingChannels(this ILoggedConnection<SnowflakeDbConnection> db, Platform platform,
      IReadOnlyCollection<string> explicitIds = null) {
      var existing = await db.Query<string>(@"get channels", @$"
  select v
  from channel_stage
  where v:Platform = '{platform}' and v:SourceId is not null
  {(explicitIds.HasItems() ? $"and v:ChannelId in ({explicitIds.Join(",", c => $"'{c}'")})" : "")}
    qualify row_number() over (partition by v:ChannelId order by v:Updated::timestamp_ntz desc)=1
");
      return existing.Select(e => e.ToObject<Channel>(IJsonlStore.JCfg)).ToArray();
    }
  }
}