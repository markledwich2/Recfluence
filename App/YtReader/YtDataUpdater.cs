using System.Threading.Tasks;
using Serilog;
using YtReader.Search;
using YtReader.Store;

namespace YtReader {
  /// <summary>Updates all data daily. i.e. Collects from YT, updates warehouse, updates blob results for website, indexes
  ///   caption search</summary>
  public class YtDataUpdater {
    readonly ILogger          Log;
    readonly YtDataCollector  Collector;
    readonly WarehouseUpdater Warehouse;
    readonly YtSearch         Search;
    readonly YtResults        Results;
    readonly Dataform         Dataform;

    public YtDataUpdater(ILogger log, YtDataCollector collector, WarehouseUpdater warehouse, YtSearch search, YtResults results, Dataform dataform) {
      Log = log;
      Collector = collector;
      Warehouse = warehouse;
      Search = search;
      Results = results;
      Dataform = dataform;
    }

    public async Task Update(ILogger log) {
      await Collector.Update(log);
      await Warehouse.WarehouseUpdate();
      await Task.WhenAll(
        Search.SyncToElastic(),
        Results.SaveBlobResults(),
        Dataform.Update(log));
    }
  }
}