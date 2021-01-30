using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static YtReader.BitChute.BcCollectPart;

namespace YtReader.BitChute {
  public enum BcLinkType {
    Video,
    Channel
  }

  public enum BcCollectPart {
    Channel,
    DiscoverChannels,
    Video
  }

  public class BcCollect {
    readonly SnowflakeConnectionProvider Sf;
    readonly BcWeb                       Web;
    readonly BitChuteCfg                 Cfg;
    readonly YtStore                     Db;

    public BcCollect(YtStores stores, SnowflakeConnectionProvider sf, BcWeb web, BitChuteCfg cfg, ILogger log) {
      Db = new(stores.Store(DataStoreType.Db), log);
      Sf = sf;
      Web = web;
      Cfg = cfg;
    }

    public async Task Collect(string[] explicitChannels, BcCollectPart[] parts, ILogger log, CancellationToken cancel) {
      var toUpdate = new KeyedCollection<string, Channel>(s => s.ChannelId);

      // add to update if it doesn't exist
      void ToUpdate(string desc, IReadOnlyCollection<Channel> channels) {
        toUpdate.AddRange(channels.NotNull().Where(c => !toUpdate.ContainsKey(c.ChannelId)));
        log.Debug("BcCollect loaded {Channel} ({Desc}) channels for update", channels.Count, desc);
      }

      {
        using var db = await Sf.Open(log);
        var existing = await db.Query<string>(@"get channels", @$"
select v
from bc_channel_stage
{(explicitChannels?.Any() == true ? $"where v:ChannelId in ({explicitChannels.Join(",", c => $"'{c}'")})" : "")}
qualify row_number() over (partition by v:ChannelId order by v:Update::timestamp_ntz desc) = 1
");

        ToUpdate("existing", existing.Select(e => e.ToObject<Channel>()).ToArray());
        ToUpdate("explicit", explicitChannels.NotNull().Select(c => new Channel(c) {Source = new(ChannelSourceType.Manual, DestId: c)}).ToArray());

        if (parts.ShouldRun(DiscoverChannels)) {
          var discovered = await db.Query<(string idOrName, string sourceId)>("bitchute links", @"
with existing as (
  select v:ChannelId::string channel_id, v:Source::object source
  from bc_channel_stage qualify row_number() over (partition by v:ChannelId order by v:Updated::timestamp_ntz desc) = 1
)

  select l.dest_channel_id
       , l.source_channel_id
  from bc_links l
         left join channel_latest yt_chan on l.source_channel_id=yt_chan.channel_id
          left join existing e on e.source:Type = 'YouTubeChannel' and e.source:DestId = l.dest_channel_id
  where l.type ='channel'
    and e.channel_id is null -- don't discover existing channels with the same id/name
    and array_contains('QAnon'::variant, yt_chan.tags)
");

          ToUpdate("discovered", discovered.Select(l => new Channel(l.idOrName)
            {Source = new(ChannelSourceType.YouTubeChannel, l.sourceId, l.idOrName)}).ToArray());
        }
      }

      await toUpdate.WithIndex().BlockAction(async item => {
        var(c,i) = item;
        var ((freshChan, getVideos), ex) = await Def.F(() => Web.ChannelAndVideos(c.ChannelId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Unable to load channel {c}: {Message}", c, ex.Message);
          freshChan = new(c.ChannelId) {Status = ChannelStatus.NotFound};
        }

        var chan = c.JsonMerge(freshChan); // keep existing values like Source, but replace whatever comes from the web update

        await Db.BcChannels.Append(chan, log);
        log.Information("BcCollect - saved {Channel} {Num}/{Total}", chan.ChannelTitle, i+1, toUpdate.Count);

        if (parts.ShouldRun(Video) && getVideos != null) {
          var videos = await getVideos.SelectManyList();
          await Db.BvVideos.Append(videos);
          log.Information("BcCollect - saved {Videos} videos for {Channel}", videos.Count, freshChan.ChannelTitle);
        }
      }, Cfg.CollectParallel, cancel: cancel); //Cfg.DefaultParallel
    }
  }
}