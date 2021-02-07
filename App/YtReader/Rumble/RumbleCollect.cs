using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Flurl;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static YtReader.StandardCollectPart;

namespace YtReader.Rumble {
  public record RumbleCollect(RumbleWeb Web, BlobStores Stores, SnowflakeConnectionProvider Sf, RumbleCfg Cfg) {
    const Platform P = Platform.Rumble;
    
    public async Task Collect(string[] explicitChannels, StandardCollectPart[] parts, ILogger log, CancellationToken cancel) {
      var toUpdate = new KeyedCollection<string, Channel>(s => s.ChannelId);

      // add to update if it doesn't exist
      void ToUpdate(string desc, IReadOnlyCollection<Channel> channels) {
        toUpdate.AddRange(channels.NotNull().Where(c => !toUpdate.ContainsKey(c.ChannelId)));
        log.Debug("RumbleCollect planned {Channel} ({Desc}) channels for update", channels.Count, desc);
      }

      {
        using var db = await Sf.Open(log);
        
        var existing = await db.ExistingChannels(P, explicitChannels);
        ToUpdate("existing", existing.Where(c => c.ForUpdate(explicitChannels)).ToArray());
        ToUpdate("explicit",
          explicitChannels.NotNull().Select(c => new Channel(P, c) {DiscoverSource = new(ChannelSourceType.Manual, c)}).ToArray());

        if (parts.ShouldRun(DiscoverChannels) && explicitChannels?.Any() != true) {
          var discovered = await db.DiscoverChannelsAndVideos(P);

          string ChannelUrl(string id) => RumbleWeb.RumbleDotCom.AppendPathSegments("c", id);
          
          ToUpdate("discovered",
            discovered.Where(d => d.LinkType == LinkType.Channel)
              .Select(l => new Channel(P, ChannelUrl(l.LinkId)) {DiscoverSource = l.ToDiscoverSource()}).ToArray());
        }
      }

      var store = new YtStore(Stores.Store(DataStoreType.DbStage), log);

      await toUpdate.WithIndex().BlockAction(async item => {
        var (c, i) = item;
        var ((freshChan, getVideos), ex) = await Def.F(() => Web.ChannelAndVideos(c.SourceId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Unable to load channel {c}: {Message}", c, ex.Message);
          freshChan = new() {Status = ChannelStatus.NotFound};
        }
        var chan = c.JsonMerge(freshChan); // keep existing values like DiscoverSource, but replace whatever comes from the web update

        await store.Channels.Append(chan, log);
        log.Information("RumbleCollect - saved {Channel} {Num}/{Total}", chan.ChannelTitle ?? chan.ChannelId, i + 1, toUpdate.Count);

        if (parts.ShouldRun(Video) && getVideos != null) {
          var (videos, vidEx) = await getVideos.SelectManyList().Try();
          if (vidEx != null) {
            log.Warning(vidEx, "Unable to load videos for channel {Channel}: {Message}", chan.ToString(), vidEx.Message);
            return;
          }
          await store.Videos.Append(videos);
          log.Information("RumbleCollect - saved {Videos} videos for {Channel}", videos.Count, chan.ChannelTitle ?? chan.ChannelId);
        }
      }, Cfg.CollectParallel, cancel: cancel); //Cfg.DefaultParallel
    }
  }
}