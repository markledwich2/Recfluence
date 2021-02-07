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
using static YtReader.StandardCollectPart;
using static YtReader.Store.ChannelSourceType;
using static YtReader.Store.ChannelStatus;

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

    public static Channel ChanFromSource(string sourceId, DiscoverSource source = null) => new() {
      Platform = P,
      ChannelId = P.FullId(sourceId),
      SourceId = sourceId,
      DiscoverSource = source
    };

    public async Task Collect(string[] explicitChannels, StandardCollectPart[] parts, ILogger log, CancellationToken cancel) {
      var toUpdate = new KeyedCollection<string, Channel>(s => s.ChannelId);
      var allExisting = new KeyedCollection<string, Channel>(s => s.ChannelId);
      var videosToCrawl = new List<DiscoverSource>();

      // add to update if it doesn't exist
      void ToUpdate(string desc, IReadOnlyCollection<Channel> channels) {
        toUpdate.AddRange(channels.NotNull().Where(c => !toUpdate.ContainsKey(c.ChannelId)));
        log.Information("BcCollect - planned {Channels} ({Desc}) channels for update", channels.Count, desc);
      }

      {
        using var db = await Sf.Open(log);
        allExisting.AddRange(await db.ExistingChannels(P));

        if(parts.ShouldRun(ExistingChannel)) // ad existing channels limit to explicit
          ToUpdate("existing", allExisting.Where(c => c.ForUpdate(explicitChannels)).ToArray());

        // add explicit channel, no need to lookup existing, because that will already be in the list
        ToUpdate("explicit", explicitChannels.NotNull().Select(c => ChanFromSource(c, new(Manual, c))).ToArray());

        if (parts.ShouldRunAny(DiscoverChannels, DiscoverChannelsViaVideos) && explicitChannels?.Any() != true) {
          var discovered = await db.DiscoverChannelsAndVideos(P);
          
          if (parts.ShouldRun(DiscoverChannels))
            ToUpdate("discovered", discovered.Where(d => d.LinkType == LinkType.Channel)
                .Select(selector: l => ChanFromSource(l.LinkId) with {DiscoverSource = l.ToDiscoverSource()}).ToArray());

          if (parts.ShouldRun(DiscoverChannelsViaVideos)) {
            videosToCrawl.AddRange(discovered.Where(d => d.LinkType == LinkType.Video)
              .Select(l => new DiscoverSource(VideoLink, l.LinkId, l.FromPlatform)).Take(50));
            log.Information("BcCollect - planned {Videos} videos for crawl", videosToCrawl.Count);
          }
        }
      }

      var crawledVideos = await videosToCrawl.BlockTrans(async (discover, i) => {
        var video = await Web.Video(discover.LinkId, log)
          .WithSwallow(e => log.Error(e, "BcCollect - error crawling video {Video}: {Error}", discover.LinkId, e.Message));
        log.Debug("BcCollect - crawled video {VideoId} {Vid}/{Total}", video?.VideoId, i, videosToCrawl.Count);
        return (discover, video);
      }, Cfg.CollectParallel).ToListAsync();

      await Db.Videos.Append(crawledVideos.Select(r => r.video).NotNull().ToArray());
      log.Information("BcCollect video crawl - saved {Videos} videos", crawledVideos.Count);

      var crawledChannels = crawledVideos.Where(v => v.video?.ChannelId.HasValue() == true)
        .Select(v => new Channel {
          ChannelId = v.video.ChannelId,
          SourceId = v.video.ChannelSourceId,
          ChannelTitle = v.video.ChannelTitle,
          DiscoverSource = v.discover,
          Platform = P
      }).Distinct().ToArray();
      
      ToUpdate("channels from crawl", crawledChannels);

      await toUpdate.WithIndex().BlockAction(async item => {
        var (c, i) = item;
        var ((freshChan, getVideos), ex) = await Def.F(() => Web.ChannelAndVideos(c.SourceId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Unable to load channel {c}: {Message}", c, ex.Message);
          freshChan = new() {Status = NotFound};
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