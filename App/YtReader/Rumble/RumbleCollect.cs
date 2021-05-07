using System.Threading;
using System.Threading.Tasks;
using Serilog;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;

namespace YtReader.Rumble {
  /// <summary>Collects Rumble data. Very similar to BitChute flow, but seperate because there is not reason the proes</summary>
  public record RumbleCollect(RumbleWeb Web, YtStore Stores, SnowflakeConnectionProvider Sf, RumbleCfg Cfg) {
    public async Task Collect(SimpleCollectOptions options, ILogger log, CancellationToken cancel) =>
      await options.PlanSimpleCollect(Sf, Web, log, cancel)
        //.Then(c => c.CrawlVideoLinksToFindNewChannels(Stores.Videos))
        .Then(c => c.CollectChannelAndVideos(Stores));
  }
}