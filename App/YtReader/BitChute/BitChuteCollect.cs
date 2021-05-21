using System.Threading;
using System.Threading.Tasks;
using Serilog;
using YtReader.SimpleCollect;
using YtReader.Store;

namespace YtReader.BitChute {
  public record BitChuteCollect(SimpleCollector Collector) {
    public async Task Collect(SimpleCollectOptions options, ILogger log, CancellationToken cancel) {
      log = log.ForContext("Function", nameof(BitChuteCollect));
      options = options with {Platform = Platform.BitChute};
      var plan = await Collector.PlanSimpleCollect(options, log, cancel);
      await Collector.Discover(plan, log, cancel);
    }
  }
}