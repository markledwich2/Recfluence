using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.Search;
using YtReader.Store;

namespace YtReader {
  public class YtUpdaterCfg {
    public int Parallel { get; set; } = 4;
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

    public YtUpdater(YtUpdaterCfg cfg, ILogger log, YtCollector collector, YtStage warehouse, YtSearch search,
      YtResults results, YtDataform ytDataform, YtBackup backup) {
      Cfg = cfg;
      _updated = Guid.NewGuid().ToShortString(6);
      Log = log.ForContext("UpdateId", _updated);
      _collector = collector;
      _warehouse = warehouse;
      _search = search;
      _results = results;
      YtDataform = ytDataform;
      _backup = backup;
    }

    Task Collect(bool fullLoad) => _collector.Collect(Log, forceUpdate: fullLoad);
    [DependsOn(nameof(Collect))] Task Stage(bool fullLoad) => _warehouse.StageUpdate(Log, fullLoad);

    [DependsOn(nameof(Stage))] Task Dataform(bool fullLoad) => YtDataform.Update(Log, fullLoad);

    [DependsOn(nameof(Dataform))] Task Search(bool fullLoad) => _search.SyncToElastic(Log, fullLoad);

    [DependsOn(nameof(Dataform))] Task Results() => _results.SaveBlobResults(Log);

    [DependsOn(nameof(Collect))] Task Backup() => _backup.Backup(Log);

    [Pipe]
    public async Task Update(string[] actions = null, bool fullLoad = false) {
      actions ??= new string[]{};
      var sw = Stopwatch.StartNew();
      Log.Information("Update {RunId} - started", _updated);
      
      var actionMethods = TaskGraph.FromMethods(
        () => Collect(fullLoad),
        () => Stage(fullLoad),
        () => Search(fullLoad),
        () => Results(),
        () => Dataform(fullLoad),
        () => Backup());

      if (actions.Any()) {
        var missing = actions.Where(a => actionMethods[a] == null).ToArray();
        if (missing.Any())
          throw new InvalidOperationException($"no such action(s) ({missing.Join("|")}), available: {actionMethods.All.Join("|", a => a.Name)}");

        foreach (var m in actionMethods.All.Where(m => !actions.Contains(m.Name)))
          m.Status = GraphTaskStatus.Ignored;
      }
      
      // TODO: tasks should have frequencies within a dependency graph. But for now, full backups only on sundays, or if explicit
      var backup = actionMethods[nameof(Backup)];
      if (!actions.Contains(nameof(Backup)) && DateTime.UtcNow.DayOfWeek != DayOfWeek.Sunday)
        backup.Status = GraphTaskStatus.Ignored;
      
      var res = await actionMethods.Run(Cfg.Parallel, Log, CancellationToken.None);

      var errors = res.Where(r => r.Error).ToArray();
      if (errors.Any())
        Log.Error("Update {RunId} - failed in {Duration}: {@TaskResults}", _updated, sw.Elapsed.HumanizeShort(), res);
      else
        Log.Information("Update {RunId} - completed in {Duration}: {TaskResults}", _updated, sw.Elapsed.HumanizeShort(), res.Join("\n"));
    }
  }
}