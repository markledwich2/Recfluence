using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Snowflake.Data.Client;
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
  
  public static class CollectHelper {
    public static async Task<IReadOnlyCollection<Channel>> ExistingChannels(this ILoggedConnection<SnowflakeDbConnection> db, Platform platform,
      IReadOnlyCollection<string> explicitIds = null) {
      var existing = await db.Query<string>(@"get channels", @$"
with s as (
  select v
  from channel_stage
  where v:Platform = '{platform}'
  {(explicitIds.HasItems() ? $"and v:ChannelId in ({explicitIds.Join(",", c => $"'{c}'")})" : "")}
    qualify row_number() over (partition by v:ChannelId order by v:Updated::timestamp_ntz desc)=1
)
select v from s
where v:SourceId is not null and v:Status <> 'NotFound'
");
      return existing.Select(e => e.ToObject<Channel>(IJsonlStore.JCfg)).ToArray();
    }
  }
}