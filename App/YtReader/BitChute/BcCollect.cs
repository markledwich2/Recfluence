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
using static YtReader.Store.Platform;

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

    public BcCollect(BlobStores stores, SnowflakeConnectionProvider sf, BcWeb web, BitChuteCfg cfg, ILogger log) {
      Db = new(stores.Store(DataStoreType.DbStage), log);
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
from channel_stage
{(explicitChannels?.Any() == true ? $"where ChannelId in ({explicitChannels.Join(",", c => $"'{Platform.BitChute.FullId(c)}'")})" : "")}
where v:Platform = 'BitChute'
qualify row_number() over (partition by v:ChannelId order by v:Update::timestamp_ntz desc) = 1
");

        ToUpdate("existing", existing.Select(e => e.ToObject<Channel>(Db.Channels.JCfg)).ToArray());
        ToUpdate("explicit", explicitChannels.NotNull().Select(c => new Channel(Platform.BitChute, c) {DiscoverSource = new(ChannelSourceType.Manual, DestId: c)}).ToArray());

        if (parts.ShouldRun(DiscoverChannels) && explicitChannels?.Any() != true) {
          var discovered = await db.Query<(string idOrName, string sourceId)>("bitchute links", @"
with existing as (
  select v:DiscoverSource:DestId::string discover_id
       , v:DiscoverSource:Type::string discover_type
  from channel_stage
  where v:Platform::string='BitChute'
    qualify row_number() over (partition by v:ChannelId order by v:Updated::timestamp_ntz desc)=1
)
   , new as (
  select l.bc_channel_id
       , l.yt_channel_id
  from bc_links l
         left join channel_latest yt_chan on l.yt_channel_id=yt_chan.channel_id
         left join existing e on discover_type='YouTubeChannel' and e.discover_id=l.bc_channel_id
  where l.type='channel'
    and e.discover_id is null -- don't discover existing channels with the same id/name
    and array_contains('QAnon'::variant
    , yt_chan.tags)
)
select *
from new
");

          ToUpdate("discovered", discovered.Select(l => new Channel(Platform.BitChute, l.idOrName)
            {DiscoverSource = new(ChannelSourceType.YouTubeChannelLink, l.sourceId, l.idOrName)}).ToArray());
        }
      }

      await toUpdate.WithIndex().BlockAction(async item => {
        var (c, i) = item;
        var ((freshChan, getVideos), ex) = await Def.F(() => Web.ChannelAndVideos(c.SourceId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Unable to load channel {c}: {Message}", c, ex.Message);
          freshChan = new() { Status = ChannelStatus.NotFound };
        }

        var chan = c.JsonMerge(freshChan); // keep existing values like DiscoverSource, but replace whatever comes from the web update

        await Db.Channels.Append(chan, log);
        log.Information("BcCollect - saved {Channel} {Num}/{Total}", chan.ChannelTitle, i + 1, toUpdate.Count);

        if (parts.ShouldRun(Video) && getVideos != null) {
          var videos = await getVideos.SelectManyList();
          await Db.Videos.Append(videos);
          log.Information("BcCollect - saved {Videos} videos for {Channel}", videos.Count, freshChan.ChannelTitle);
        }
      }, Cfg.CollectParallel, cancel: cancel); //Cfg.DefaultParallel
    }
  }
}