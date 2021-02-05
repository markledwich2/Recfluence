using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static YtReader.StandardCollectPart;

namespace YtReader.BitChute {
  public class BcCollect {
    readonly SnowflakeConnectionProvider Sf;
    readonly BcWeb                       Web;
    readonly BitChuteCfg                 Cfg;
    readonly YtStore                     Db;

    const Platform P = Platform.BitChute;

    public BcCollect(BlobStores stores, SnowflakeConnectionProvider sf, BcWeb web, BitChuteCfg cfg, ILogger log) {
      Db = new(stores.Store(DataStoreType.DbStage), log);
      Sf = sf;
      Web = web;
      Cfg = cfg;
    }

    public static Channel NewChan(string sourceId) => new() {
      Platform = P,
      ChannelId = P.FullId(sourceId),
      SourceId = sourceId
    };

    public async Task Collect(string[] explicitChannels, StandardCollectPart[] parts, ILogger log, CancellationToken cancel) {
      var toUpdate = new KeyedCollection<string, Channel>(s => s.ChannelId);

      // add to update if it doesn't exist
      void ToUpdate(string desc, IReadOnlyCollection<Channel> channels) {
        toUpdate.AddRange(channels.NotNull().Where(c => !toUpdate.ContainsKey(c.ChannelId)));
        log.Information("BcCollect - planned {Channels} ({Desc}) channels for update", channels.Count, desc);
      }

      {
        using var db = await Sf.Open(log);
        var existing = await db.ExistingChannels(P, explicitChannels?.Select(n => P.FullId(n)).ToArray());

        ToUpdate("existing", existing);
        ToUpdate("explicit", explicitChannels.NotNull().Select(c => NewChan(c) with {DiscoverSource = new(ChannelSourceType.Manual, DestId: c)}).ToArray());

        if (parts.ShouldRun(DiscoverChannels) && explicitChannels?.Any() != true) {
          var discovered = await db.DiscoverNewChannelLinks(P);;
          ToUpdate("discovered",
            discovered.Select(l => NewChan(l.LinkId) with {DiscoverSource = new(ChannelSourceType.ChannelLink, l.ChannelIdFrom, l.LinkId)}).ToArray());
        }
      }

      await toUpdate.WithIndex().BlockAction(async item => {
        var (c, i) = item;
        var ((freshChan, getVideos), ex) = await Def.F(() => Web.ChannelAndVideos(c.SourceId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Unable to load channel {c}: {Message}", c, ex.Message);
          freshChan = new() {Status = ChannelStatus.NotFound};
        }

        var chan = c.JsonMerge(freshChan); // keep existing values like DiscoverSource, but replace whatever comes from the web update

        await Db.Channels.Append(chan, log);
        log.Information("BcCollect - saved {Channel} {Num}/{Total}", chan.ToString(), i + 1, toUpdate.Count);

        if (parts.ShouldRun(Video) && getVideos != null) {
          var (videos, vEx) = await getVideos.SelectManyList().Try();
          if (vEx != null) {
            log.Warning(vEx, "Unable to load videos for channel {Channel}: {Message}", chan.ToString(), vEx.Message);
            return;
          }
          await Db.Videos.Append(videos);
          log.Information("BcCollect - saved {Videos} videos for {Channel}", videos.Count, chan.ToString());
        }
      }, Cfg.CollectParallel, cancel: cancel); //Cfg.DefaultParallel
    }
  }
}