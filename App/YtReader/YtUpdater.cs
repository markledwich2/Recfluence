using System.Diagnostics;
using System.Reflection;
using Mutuo.Etl.Pipe;
using YtReader.Collect;
using YtReader.Data;
using YtReader.Search;
using YtReader.Store;
using YtReader.Yt;
using static Mutuo.Etl.Pipe.GraphTaskStatus;

// ReSharper disable InconsistentNaming

namespace YtReader;

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

  public bool                  DataformDeps  { get; init; }
  public StandardCollectPart[] StandardParts { get; init; }
  public string[]              Videos        { get; init; }
  public SearchMode            SearchMode    { get; init; }
  public string[]              Tags          { get; init; }
  public DataScriptOptions     DataScript    { get; set; }
}

/// <summary>Updates all data daily. i.e. Collects from YT, updates warehouse, updates blob results for website, indexes
///   caption search. Many missing features (resume, better recording of tasks etc..). I intend to replace with dagster or
///   make Mutuo.Etl into a data application runner once I have evaluated it.</summary>
public record YtUpdater(YtUpdaterCfg Cfg, ILogger Log, YtCollector YtCollect, Stage _stage, YtSearch _search,
  YtResults _results, YtDataform YtDataform, YtBackup _backup, UserScrape _userScrape, YtIndexResults _index, DataScripts _dataScripts) {
  Task Collect(CollectOptions options, ILogger logger, CancellationToken cancel) =>
    YtCollect.Collect(logger, options, cancel);

  [GraphTask(nameof(Collect))]
  Task Stage(bool fullLoad, string[] tables, ILogger logger) =>
    _stage.StageUpdate(logger, fullLoad, tables);

  [GraphTask(nameof(Stage))]
  Task Dataform(bool fullLoad, string[] tables, bool includeDeps, ILogger logger, CancellationToken cancel) =>
    YtDataform.Update(logger, fullLoad, tables, includeDeps, cancel);

  [GraphTask(Ignored, nameof(Dataform))] // ignored by default because I shut down search to save money
  Task Search(SearchMode mode, string[] optionsSearchIndexes, (string index, string condition)[] conditions, ILogger logger, CancellationToken cancel) =>
    _search.SyncToElastic(logger, mode, optionsSearchIndexes, conditions, cancel);

  [GraphTask(nameof(Dataform))]
  Task Result(string[] results, ILogger logger, CancellationToken cancel) =>
    _results.SaveBlobResults(logger, results, cancel);

  [GraphTask(nameof(Dataform))]
  Task Index(string[] tables, string[] tags, ILogger logger, CancellationToken cancel) =>
    _index.Run(tables, tags, logger, cancel);
  

  [Pipe]
  public async Task Update(UpdateOptions options = null, CancellationToken cancel = default) {
    options ??= new();
    var sw = Stopwatch.StartNew();
    var updateId = Guid.NewGuid().ToShortString(6);
    var log = Log.ForContext("UpdateId", updateId);
    log.Information("Update {RunId} - started", updateId);

    var fullLoad = options.FullLoad;
    var actionMethods = TaskGraph.FromMethods(
      (l, c) => Collect(options.Collect, l, c),
      (l, c) => Stage(fullLoad, options.StageTables, l),
      (l, c) => Search(options.SearchMode, options.SearchIndexes, options.SearchConditions, l, c),
      (l, c) => Result(options.Results, l, c),
      (l, c) => Index(options.Indexes, options.Tags, l, c),
      (l, c) => Dataform(fullLoad, options.WarehouseTables, options.DataformDeps, l, c)
    );

    var actions = options.Actions;
    if (actions?.Any() == true) {
      var missing = actions.Where(a => actionMethods[a] == null).ToArray();
      if (missing.Any())
        throw new InvalidOperationException($"no such action(s) ({missing.Join("|")}), available: {actionMethods.All.Join("|", a => a.Name)}");
      foreach (var m in actionMethods.All) m.Status = actions.Contains(m.Name) ? Available : Ignored;
    }

    var res = await actionMethods.Run(Cfg.Parallel, log, cancel);
    var errors = res.Where(r => r.Error).ToArray();
    if (errors.Any())
      Log.Error("Update {RunId} - failed in {Duration}: {@TaskResults}", updateId, sw.Elapsed.HumanizeShort(), res.Join("\n"));
    else
      Log.Information("Update {RunId} - completed in {Duration}: {TaskResults}", updateId, sw.Elapsed.HumanizeShort(), res.Join("\n"));
  }
}

public static class YtUpdaterEx {
  public static bool ShouldRunAny<T>(this T[] parts, params T[] toRun) where T : struct, Enum => toRun.Any(parts.ShouldRun);

  public static bool ShouldRun<T>(this T[] parts, T part) where T : struct, Enum {
    var name = Enum.GetName(part) ?? part.ToString();
    var ignore = part.GetType().GetField(name)?.GetCustomAttribute<RunPartAttribute>()?.Explicit;
    return ignore == true ? parts?.Contains(part) == true : parts?.Contains(part) != false;
  }
}