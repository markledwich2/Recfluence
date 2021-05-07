using System.Threading;
using System.Threading.Tasks;
using Serilog;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;

namespace YtReader.BitChute {
  public record BcCollect(YtStore Stores, SnowflakeConnectionProvider Sf, BcWeb Web, BitChuteCfg Cfg, ILogger Log) {
    public async Task Collect(SimpleCollectOptions options, ILogger log, CancellationToken cancel) =>
      await options.PlanSimpleCollect(Sf, Web, log, cancel)
        //.Then(c => c.CrawlVideoLinksToFindNewChannels(Stores.Videos))
        .Then(c => c.CollectChannelAndVideos(Stores));
  }
}