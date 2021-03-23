using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.BitChute;
using YtReader.Rumble;
using YtReader.Search;
using YtReader.Store;

namespace YtReader {
  public class YtUpdaterCfg {
    public int Parallel { get; set; } = 4;
  }

  public record UpdateOptions {
    public bool     FullLoad        { get; init; }
    public string[] Actions         { get; init; }
    public string[] WarehouseTables { get; init; }
    public string[] StageTables     { get; init; }
    public string[] Results         { get; init; }

    public CollectOptions Collect { get; init; }

    public bool                               DisableChannelDiscover { get; init; }
    public bool                               UserScrapeInit         { get; init; }
    public string                             UserScrapeTrial        { get; init; }
    public (string index, string condition)[] SearchConditions       { get; init; }
    public string[]                           SearchIndexes          { get; init; }
    public string[]                           UserScrapeAccounts     { get; init; }
    public string[]                           Indexes                { get; init; }

    public bool                  DataformDeps     { get; init; }
    public StandardCollectPart[] StandardParts    { get; init; }
    public string[]              Videos           { get; init; }
    public SearchMode            SearchMode       { get; init; }
    public string[]              Tags             { get; init; }
    public string                DataScriptsRunId { get; init; }
  }

  /// <summary>Updates all data daily. i.e. Collects from YT, updates warehouse, updates blob results for website, indexes
  ///   caption search. Many missing features (resume, better recording of tasks etc..). I intend to replace with dagster or
  ///   make Mutuo.Etl into a data application runner once I have evaluated it.</summary>
  public class YtUpdater {
    readonly YtUpdaterCfg   Cfg;
    readonly ILogger        Log;
    readonly YtCollector    _collector;
    readonly Stage          _warehouse;
    readonly YtSearch       _search;
    readonly YtResults      _results;
    readonly YtDataform     YtDataform;
    readonly YtBackup       _backup;
    readonly string         _updated;
    readonly UserScrape     _userScrape;
    readonly YtIndexResults _index;
    readonly BcCollect      _bcCollect;
    readonly RumbleCollect  _rumbleCollect;
    readonly DataScripts    _dataScripts;

    public YtUpdater(YtUpdaterCfg cfg, ILogger log, YtCollector collector, Stage warehouse, YtSearch search,
      YtResults results, YtDataform ytDataform, YtBackup backup, UserScrape userScrape, YtIndexResults index, BcCollect bcCollect,
      RumbleCollect rumbleCollect, DataScripts dataScripts) {
      _dataScripts = dataScripts;
      _rumbleCollect = rumbleCollect;
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

    Task Collect(CollectOptions options, ILogger logger, CancellationToken cancel) =>
      _collector.Collect(logger, options, cancel);

    Task BcCollect(SimpleCollectOptions options, ILogger logger, CancellationToken cancel) =>
      _bcCollect.Collect(options, logger, cancel);

    Task RumbleCollect(SimpleCollectOptions options, ILogger logger, CancellationToken cancel) =>
      _rumbleCollect.Collect(options, logger, cancel);

    [GraphTask(nameof(Collect), nameof(BcCollect), nameof(RumbleCollect))]
    Task Stage(bool fullLoad, string[] tables, ILogger logger) =>
      _warehouse.StageUpdate(logger, fullLoad, tables);

    [GraphTask(nameof(Stage))]
    Task Dataform(bool fullLoad, string[] tables, bool includeDeps, ILogger logger, CancellationToken cancel) =>
      YtDataform.Update(logger, fullLoad, tables, includeDeps, cancel);

    [GraphTask(nameof(Dataform))]
    Task Search(SearchMode mode, string[] optionsSearchIndexes, (string index, string condition)[] conditions, ILogger logger, CancellationToken cancel) =>
      _search.SyncToElastic(logger, mode, indexes: optionsSearchIndexes, conditions, cancel: cancel);

    [GraphTask(nameof(Dataform))]
    Task Result(string[] results, ILogger logger, CancellationToken cancel) =>
      _results.SaveBlobResults(logger, results, cancel);

    [GraphTask(nameof(Dataform))]
    Task Index(string[] tables, string[] tags, ILogger logger, CancellationToken cancel) =>
      _index.Run(tables, tags, logger, cancel);

    [GraphTask(nameof(Dataform))]
    Task DataScripts(ILogger logger, CancellationToken cancel, string runId = null) =>
      _dataScripts.Run(logger, cancel, runId);

    [GraphTask(nameof(Stage))]
    Task Backup(ILogger logger) =>
      _backup.Backup(logger);

    [GraphTask(nameof(Result), nameof(Collect), nameof(Dataform))]
    Task UserScrape(bool init, string trial, string[] accounts, ILogger logger, CancellationToken cancel) =>
      _userScrape.Run(Log, init, trial, accounts, cancel);

    [Pipe]
    public async Task Update(UpdateOptions options = null, CancellationToken cancel = default) {
      options ??= new();
      var sw = Stopwatch.StartNew();
      Log.Information("Update {RunId} - started", _updated);

      var fullLoad = options.FullLoad;

      var collectOptions = new SimpleCollectOptions
        {Parts = options.StandardParts, ExplicitChannels = options.Collect?.LimitChannels, ExplicitVideos = options.Videos};

      var actionMethods = TaskGraph.FromMethods(
        (l, c) => Collect(options.Collect, l, c),
        (l, c) => BcCollect(collectOptions, l, c),
        (l, c) => RumbleCollect(collectOptions, l, c),
        (l, c) => Stage(fullLoad, options.StageTables, l),
        (l, c) => Search(options.SearchMode, options.SearchIndexes, options.SearchConditions, l, c),
        (l, c) => Result(options.Results, l, c),
        (l, c) => Index(options.Indexes, options.Tags, l, c),
        //(l, c) => UserScrape(options.UserScrapeInit, options.UserScrapeTrial, options.UserScrapeAccounts, l, c),
        (l, c) => Dataform(fullLoad, options.WarehouseTables, options.DataformDeps, l, c),
        (l, c) => DataScripts(l, c, options.DataScriptsRunId),
        (l, c) => Backup(l)
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
    public static bool ShouldRunAny<T>(this T[] parts, params T[] toRun) where T : struct, Enum => toRun.Any(parts.ShouldRun);

    public static bool ShouldRun<T>(this T[] parts, T part) where T : struct, Enum {
      var name = Enum.GetName(part) ?? part.ToString();
      var ignore = part.GetType().GetField(name)?.GetCustomAttribute<CollectPartAttribute>()?.Explicit;
      return ignore == true ? parts?.Contains(part) == true : parts?.Contains(part) != false;
    }
  }
}