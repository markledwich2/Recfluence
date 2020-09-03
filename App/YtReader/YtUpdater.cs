using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.Search;
using YtReader.Store;

namespace YtReader {
  public class YtUpdaterCfg {
    public int Parallel { get; set; } = 4;
  }

  public class UpdateOptions {
    public bool                                    FullLoad               { get; set; }
    public string[]                                Actions                { get; set; }
    public string[]                                Tables                 { get; set; }
    public string[]                                Results                { get; set; }
    public string[]                                Channels               { get; set; }
    public bool                                    DisableChannelDiscover { get; set; }
    public bool                                    UserScrapeInit         { get; set; }
    public string                                  UserScrapeTrial        { get; set; }
    public (SearchIndex index, string condition)[] SearchConditions       { get; set; }
    public string[]                                UserScrapeAccounts     { get; set; }
  }

  /// <summary>Updates all data daily. i.e. Collects from YT, updates warehouse, updates blob results for website, indexes
  ///   caption search. Many missing features (resume, better recording of tasks etc..). I intend to replace with dagster or
  ///   make Mutuo.Etl into a data application runner once I have evaluated it.</summary>
  public class YtUpdater {
    readonly YtUpdaterCfg Cfg;
    readonly ILogger      Log;
    readonly YtCollector  _collector;
    readonly YtStage      _warehouse;
    readonly YtSearch     _search;
    readonly YtResults    _results;
    readonly YtDataform   YtDataform;
    readonly YtBackup     _backup;
    readonly string       _updated;
    readonly UserScrape   _userScrape;

    public YtUpdater(YtUpdaterCfg cfg, ILogger log, YtCollector collector, YtStage warehouse, YtSearch search,
      YtResults results, YtDataform ytDataform, YtBackup backup, UserScrape userScrape) {
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
    }

    Task Collect(bool fullLoad, bool disableDiscover, string[] channels, CancellationToken cancel) =>
      _collector.Collect(Log, forceUpdate: fullLoad, disableDiscover, channels, cancel);

    [DependsOn(nameof(Collect))]
    Task Stage(bool fullLoad, string[] tables) =>
      _warehouse.StageUpdate(Log, fullLoad, tables);

    [DependsOn(nameof(Stage))]
    Task Dataform(bool fullLoad, string[] tables, CancellationToken cancel) =>
      YtDataform.Update(Log, fullLoad, tables, cancel);

    [DependsOn(nameof(Dataform))]
    Task Search(bool fullLoad, (SearchIndex index, string condition)[] conditions, CancellationToken cancel) =>
      _search.SyncToElastic(Log, fullLoad, limit: null, conditions, cancel: cancel);

    [DependsOn(nameof(Dataform))]
    Task Results(string[] results) =>
      _results.SaveBlobResults(Log, results);

    [DependsOn(nameof(Collect))]
    Task Backup() =>
      _backup.Backup(Log);

    [DependsOn(nameof(Results), nameof(Collect), nameof(Dataform))]
    Task UserScrape(bool init, string trial, string[] accounts, CancellationToken cancel) =>
      _userScrape.Run(Log, init, trial, accounts, cancel);

    [Pipe]
    public async Task Update(UpdateOptions options = null, CancellationToken cancel = default) {
      options ??= new UpdateOptions();
      var sw = Stopwatch.StartNew();
      Log.Information("Update {RunId} - started", _updated);

      var fullLoad = options.FullLoad;

      var actionMethods = TaskGraph.FromMethods(
        c => Collect(fullLoad, options.DisableChannelDiscover, options.Channels, c),
        c => Stage(fullLoad, options.Tables),
        c => Search(fullLoad, options.SearchConditions, c),
        c => Results(options.Results),
        c => UserScrape(options.UserScrapeInit, options.UserScrapeTrial, options.UserScrapeAccounts, c),
        c => Dataform(fullLoad, options.Tables, c),
        c => Backup());

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
      if (backup.Status != GraphTaskStatus.Ignored && DateTime.UtcNow.DayOfWeek != DayOfWeek.Sunday)
        backup.Status = GraphTaskStatus.Ignored;

      var res = await actionMethods.Run(Cfg.Parallel, Log, cancel);

      var errors = res.Where(r => r.Error).ToArray();
      if (errors.Any())
        Log.Error("Update {RunId} - failed in {Duration}: {@TaskResults}", _updated, sw.Elapsed.HumanizeShort(), res.Join("\n"));
      else
        Log.Information("Update {RunId} - completed in {Duration}: {TaskResults}", _updated, sw.Elapsed.HumanizeShort(), res.Join("\n"));
    }
  }
}