using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Humanizer.Bytes;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static Mutuo.Etl.Pipe.PipeArg;

namespace YtReader {
  public class WarehouseCfg {
    [Required] public string      Stage              { get; set; } = "yt_data";
    [Required] public string      Private            { get; set; } = "yt_private";
    [Required] public OptimiseCfg Optimise           { get; set; } = new ();
    [Required] public int         LoadTablesParallel { get; set; } = 4;
    public            string[]    Roles              { get; set; } = {"sysadmin", "recfluence"};
    public            int         MetadataParallel   { get; set; } = 8;
    public            int         FileMb             { get; set; } = 80;
  }

  public class YtStage {
    readonly YtStores                    Stores;
    readonly StorageCfg                  StorageCfg;
    readonly SnowflakeConnectionProvider Conn;
    readonly WarehouseCfg                Cfg;
    readonly IPipeCtx                    PipeCtx;

    public YtStage(YtStores stores, StorageCfg storageCfg, SnowflakeConnectionProvider conn, WarehouseCfg cfg, IPipeCtx pipeCtx) {
      Stores = stores;
      StorageCfg = storageCfg;
      Conn = conn;
      Cfg = cfg;
      PipeCtx = pipeCtx;
    }

    public async Task StageUpdate(ILogger log, bool fullLoad = false, string[] tableNames = null) {
      log = log.ForContext("db", Conn.Cfg.DbName());
      log.Information("StageUpdate - started for snowflake host '{Host}', db '{Db}'", Conn.Cfg.Host, Conn.Cfg.DbName());
      var sw = Stopwatch.StartNew();
      var tables = YtWarehouse.AllTables.Where(t => tableNames.None() || tableNames?.Contains(t.Table, StringComparer.OrdinalIgnoreCase) == true).ToArray();
      await tables.BlockAction(async t => {
        var table = t.Table;
        using var db = await Conn.Open(log);
        await db.Execute("create table", $"create table if not exists {table} (v Variant)");

        if (t.Dir != null) {
          log.Information("StageUpdate - {Table} ({LoadType})", table, fullLoad ? "full" : "incremental");
          var latestTs = fullLoad ? null : await db.ExecuteScalar<DateTime?>("latest timestamp", $"select max(v:{t.TsCol}::timestamp_ntz) from {table}");
          if (latestTs == null)
            await FullLoad(db, table, t);
          else
            await Incremental(db, table, t, latestTs.Value);
        }
      }, Cfg.LoadTablesParallel);
      log.Information("StageUpdate - {Tables} updated in {Duration}", tables.Join("|", t => t.Table), sw.Elapsed.HumanizeShort());
    }

    AzureBlobFileStore Store(StageTableCfg t) => Stores.Store(t.StoreType);

    async Task Incremental(ILoggedConnection<IDbConnection> db, string table, StageTableCfg t, DateTime latestTs) {
      var store = Store(t);
      await store.Optimise(Cfg.Optimise, t.Dir, latestTs.FileSafeTimestamp(), db.Log); // optimise files newer than the last load
      var ((_, rows, size), dur) = await CopyInto(db, table, t).WithDuration();
      db.Log.Information("StageUpdate - {Table} incremental load of {Rows} rows ({Size}) took {Duration}",
        table, rows, size.Humanize("#.#"), dur.HumanizeShort());
    }

    [Pipe]
    public async Task<bool> ProcessOptimisePlan(IReadOnlyCollection<OptimiseBatch> plan, string path, ILogger log) {
      var runId = ShortGuid.Create(4);
      log = log.ForContext("RunId", runId);
      log.Information("YtStage - starting optimisation plan ({RunId}) first path '{Dest}' out of {TotalBatches}", runId, plan.First().Dest, plan.Count);
      var store = Stores.Store(path);
      await store.Optimise(Cfg.Optimise, plan, log);
      return true;
    }

    async Task FullLoad(ILoggedConnection<IDbConnection> db, string table, StageTableCfg t, CancellationToken cancel = default) {
      var store = Store(t);
      if (t.IsNativeStore) {
        var plan = await store.OptimisePlan(Cfg.Optimise, t.Dir, ts: null, db.Log);
        if (plan.Count < 10) // if the plan is small, run locally, otherwise on many machines
          await store.Optimise(Cfg.Optimise, plan, db.Log);
        else
          await plan.Process(PipeCtx, b => ProcessOptimisePlan(b, Stores.StoragePath(t.StoreType), Inject<ILogger>())
            , new() { MaxParallel = 8, MinWorkItems = 1 }, db.Log, cancel);
      }
      await db.Execute("truncate table", $"truncate table {table}"); // no transaction, stage tables aren't reported on so don't need to be available
      var ((_, rows, size), dur) = await CopyInto(db, table, t).WithDuration();
      db.Log.Information("StageUpdate - {Table} full load of {Rows} rows ({Size}) took {Duration}",
        table, rows, size.Humanize("#.#"), dur.HumanizeShort());
    }

    async Task<(string[] files, long rows, ByteSize size)> CopyInto(ILoggedConnection<IDbConnection> db, string table, StageTableCfg t) {
      var startTime = await db.ExecuteScalar<string>("current time", "select current_timestamp()::string");
      var (stage, path) = t.StoreType switch {
        DataStoreType.Db => (Cfg.Stage, StorageCfg.DbPath),
        DataStoreType.Private => (Cfg.Private, null),
        _ => throw new InvalidOperationException($"No warehouse stage for store type {t.StoreType}")
      };

      var sql = $"copy into {table} from @{new[] {stage, path}.Concat(t.Dir.Tokens).NotNull().Join("/")}/ file_format=(type=json)";
      await db.Execute("copy into", sql);

      // sf should return this info form copy_into (its in their UI, but not in .net or jdbc drivers)
      // the int that is return is the # of rows form the first file loaded. So we go get this ourselves
      var copyResults = await db.Query<(string fileName, long rows, long size)>("copy results",
        "select file_name, row_count, file_size " +
        $"from table(information_schema.copy_history(table_name=>'{table}', start_time=>'{startTime}'::timestamp_ltz))");

      var res = (copyResults.Select(r => r.fileName).ToArray(), copyResults.Sum(r => r.rows), copyResults.Sum(r => r.size).Bytes());
      return res;
    }
  }

  public static class YtWarehouse {
    public static string DbName(this SnowflakeCfg cfg) => cfg.DbSuffix.HasValue() ? $"{cfg.Db}_{cfg.DbSuffix}" : cfg.Db;

    static StageTableCfg UsTable(string name) =>
      new ($"userscrape/results/{name}", $"us_{name}_stage", isNativeStore: false, tsCol: "updated");

    public static readonly StageTableCfg[] AllTables = {
      UsTable("rec"),
      UsTable("feed"),
      UsTable("watch"),
      UsTable("ad"),
      new("channels", "channel_stage"),
      new("channel_reviews", "channel_review_stage"),
      new("videos", "video_stage"),
      new("recs", "rec_stage"),
      new("video_extra", "video_extra_stage"),
      new("searches", "search_stage"),
      new("captions", "caption_stage"),
      new("bc_videos", "bc_video_stage"),
      new("bc_channels", "bc_channel_stage"),
      new("rec_exports_processed", "rec_export_stage", storeType: DataStoreType.Private),
      new(dir: null, "dbv1_video_stage", isNativeStore: false),
      new(dir: null, "dbv1_rec_stage", isNativeStore: false),
    };
  }

  public class StageTableCfg {
    public StageTableCfg(string dir, string table, bool isNativeStore = true, string tsCol = null, DataStoreType storeType = DataStoreType.Db) {
      Dir = dir;
      Table = table;
      IsNativeStore = isNativeStore;
      StoreType = storeType;
      TsCol = tsCol ?? (isNativeStore ? "Updated" : null);
    }

    public StringPath    Dir           { get; }
    public string        Table         { get; }
    public bool          IsNativeStore { get; }
    public DataStoreType StoreType     { get; }
    public string        TsCol         { get; }
  }
}