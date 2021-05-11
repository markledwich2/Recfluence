using System.Threading;
using System.Threading.Tasks;
using Serilog;
using YtReader.SimpleCollect;
using YtReader.Store;

namespace YtReader.Rumble {
  /// <summary>Collects Rumble data</summary>
  public record RumbleCollect(SimpleCollector Collector) {
    public async Task Collect(SimpleCollectOptions options, ILogger log, CancellationToken cancel) {
      options = options with {Platform = Platform.Rumble};
      var plan = await Collector.PlanSimpleCollect(options, log, cancel);
      await Collector.CollectChannelAndVideos(plan, log, cancel);
    }
  }
}