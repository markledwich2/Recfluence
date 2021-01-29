using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.BitChute;
using YtReader.Search;
using YtReader.Store;

namespace YtReader {
  public class YtUpdaterCfg {
    public int Parallel { get; set; } = 4;
  }

  public class UpdateOptions {
    public bool                               FullLoad               { get; set; }
    public string[]                           Actions                { get; set; }
    public string[]                           Tables                 { get; set; }
    public string[]                           StageTables            { get; set; }
    public string[]                           Results                { get; set; }
    public string[]                           Channels               { get; set; }
    public bool                               DisableChannelDiscover { get; set; }
    public bool                               UserScrapeInit         { get; set; }
    public string                             UserScrapeTrial        { get; set; }
    public (string index, string condition)[] SearchConditions       { get; set; }
    public string[]                           SearchIndexes          { get; set; }
    public string[]                           UserScrapeAccounts     { get; set; }
    public string[]                           Indexes                { get; set; }
    public CollectPart[]                      Parts                  { get; set; }
    public string                             CollectVideosPath      { get; set; }
    public bool                               DataformDeps           { get; set; }
    public BcCollectPart[]                      BcParts                { get; set; }
  }

  /// <summary>Updates all data daily. i.e. Collects from YT, updates warehouse, updates blob results for website, indexes
  ///   caption search. Many missing features (resume, better recording of tasks etc..). I intend to replace with dagster or
  ///   make Mutuo.Etl into a data application runner once I have evaluated it.</summary>
  public class YtUpdater {
    readonly YtUpdaterCfg   Cfg;
    readonly ILogger        Log;
    readonly YtCollector    _collector;
    readonly YtStage        _warehouse;
    readonly YtSearch       _search;
    readonly YtResults      _results;
    readonly YtDataform     YtDataform;
    readonly YtBackup       _backup;
    readonly string         _updated;
    readonly UserScrape     _userScrape;
    readonly YtIndexResults _index;
    readonly BcCollect               _bcCollect;

    public YtUpdater(YtUpdaterCfg cfg, ILogger log, YtCollector collector, YtStage warehouse, YtSearch search,
      YtResults results, YtDataform ytDataform, YtBackup backup, UserScrape userScrape, YtIndexResults index, BcCollect bcCollect) {
      _bcCollect = bcCollect;
      Cfg = cfg;
      _updated = Guid.NewGuid().ToShortString(6);
      Log = log.ForContext("UpdateId", _updated);
      _collector = collector;
      _warehouse = warehouse;
      _search = search;
      _results = results;
      YtDataform = ytDataform;
      _backup = backup;
      _userScrape = userScrape;
      _index = index;
    }

    Task Collect(string[] channels, CollectPart[] parts, string collectVideoPath, ILogger logger, CancellationToken cancel) =>
      _collector.Collect(logger, channels, parts, collectVideoPath, cancel);
    
    Task BcCollect(string[] channels, BcCollectPart[] parts, ILogger logger, CancellationToken cancel) =>
      _bcCollect.Collect(channels, parts, logger, cancel);

    [GraphTask(nameof(Collect), nameof(BcCollect))]
    Task Stage(bool fullLoad, string[] tables, ILogger logger) =>
      _warehouse.StageUpdate(logger, fullLoad, tables);

    [GraphTask(nameof(Stage))]
    Task Dataform(bool fullLoad, string[] tables, bool includeDeps, ILogger logger, CancellationToken cancel) =>
      YtDataform.Update(logger, fullLoad, tables, includeDeps, cancel);

    [GraphTask(nameof(Dataform))]
    Task Search(bool fullLoad, string[] optionsSearchIndexes, (string index, string condition)[] conditions, ILogger logger, CancellationToken cancel) =>
      _search.SyncToElastic(logger, fullLoad, indexes: optionsSearchIndexes, conditions, cancel: cancel);

    [GraphTask(nameof(Dataform))]
    Task Result(string[] results, ILogger logger, CancellationToken cancel) =>
      _results.SaveBlobResults(logger, results, cancel);

    [GraphTask(nameof(Dataform))]
    Task Index(string[] tables, ILogger logger, CancellationToken cancel) =>
      _index.Run(tables, logger, cancel);

    [GraphTask(nameof(Stage))]
    Task Backup(ILogger logger) =>
      _backup.Backup(logger);

    [GraphTask(nameof(Result), nameof(Collect), nameof(Dataform))]
    Task UserScrape(bool init, string trial, string[] accounts, ILogger logger, CancellationToken cancel) =>
      _userScrape.Run(Log, init, trial, accounts, cancel);

    [Pipe]
    public async Task Update(UpdateOptions options = null, CancellationToken cancel = default) {
      options ??= new ();
      var sw = Stopwatch.StartNew();
      Log.Information("Update {RunId} - started", _updated);

      var fullLoad = options.FullLoad;

      var actionMethods = TaskGraph.FromMethods(
        (l, c) => Collect(options.Channels, options.Parts, options.CollectVideosPath, l, c),
        (l, c) => BcCollect(options.Channels, options.BcParts, l, c),
        (l, c) => Stage(fullLoad, options.StageTables, l),
        (l, c) => Search(options.FullLoad, options.SearchIndexes, options.SearchConditions, l, c),
        (l, c) => Result(options.Results, l, c),
        (l, c) => Index(options.Indexes, l, c),
        //(l, c) => UserScrape(options.UserScrapeInit, options.UserScrapeTrial, options.UserScrapeAccounts, l, c),
        (l, c) => Dataform(fullLoad, options.Tables, options.DataformDeps, l, c),
        (l,c) => Backup(l)
      );

      var actions = options.Actions;
      if (actions?.Any() == true) {
        var missing = actions.Where(a => actionMethods[a] == null).ToArray();
        if (missing.Any())
          throw new InvalidOperationException($"no such action(s) ({missing.Join("|")}), available: {actionMethods.All.Join("|", a => a.Name)}");

        foreach (var m in actionMethods.All.Where(m => !actions.Contains(m.Name)))
          m.Status = GraphTaskStatus.Ignored;
      }

      // TODO: tasks should have frequencies within a dependency graph. But for now, full backups only on sundays, or if explicit
      var backup = actionMethods[nameof(Backup)];
      backup.Status = GraphTaskStatus.Ignored;
      // too costly. TODO: update to incremtnal backup or de-partition the big db2 directories
      //if (backup.Status != GraphTaskStatus.Ignored && DateTime.UtcNow.DayOfWeek != DayOfWeek.Sunday)
        
      var res = await actionMethods.Run(Cfg.Parallel, Log, cancel);

      var errors = res.Where(r => r.Error).ToArray();
      if (errors.Any())
        Log.Error("Update {RunId} - failed in {Duration}: {@TaskResults}", _updated, sw.Elapsed.HumanizeShort(), res.Join("\n"));
      else
        Log.Information("Update {RunId} - completed in {Duration}: {TaskResults}", _updated, sw.Elapsed.HumanizeShort(), res.Join("\n"));
    }
  }
  
  public static class YtUpdaterEx {
    public static bool ShouldRunAny<T>(this T[] parts, params T[] toRun)  where T : Enum => parts == null || toRun.Any(parts.Contains);
    public static bool ShouldRun<T>(this T[] parts, T part) where T : Enum => parts == null || parts.Contains(part);
  }
}