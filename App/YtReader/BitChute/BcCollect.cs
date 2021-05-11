using System.Threading;
using System.Threading.Tasks;
using Serilog;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.SimpleCollect;
using YtReader.Store;

namespace YtReader.BitChute {
  public record BcCollect(SimpleCollector Collector) {
    public async Task Collect(SimpleCollectOptions options, ILogger log, CancellationToken cancel) {
      options = options with {Platform = Platform.Rumble};
      var plan = await Collector.PlanSimpleCollect(options, log, cancel);
      await Collector.CollectChannelAndVideos(plan, log, cancel);
    }
  }
}