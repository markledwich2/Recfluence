using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Diagnostics;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Snowflake.Data.Client;
using YtReader.Db;
using YtReader.Store;
using static Mutuo.Etl.Pipe.PipeArg;
using static System.StringComparer;
using static YtReader.Data.Stage.Table.Tag;

namespace YtReader.Data;

public class WarehouseCfg {
  public            WarehouseMode Mode               { get; set; } = WarehouseMode.Branch;
  [Required] public string        Stage              { get; set; } = "yt_data";
  [Required] public string        Private            { get; set; } = "yt_private";
  [Required] public OptimiseCfg   Optimise           { get; set; } = new();
  [Required] public int           LoadTablesParallel { get; set; } = 4;
  public            string[]      AdminRoles         { get; set; } = { "recfluence", "sysadmin" };
  public            string[]      ReadRoles          { get; set; } = { "reader" };
  public            int           MetadataParallel   { get; set; } = 8;
  public            int           FileMb             { get; set; } = 80;
}

/// <summary>Operations to perform on the staging files & warehouse staging tables. Deprecated. Use DataService instead.</summary>
public record Stage(BlobStores Stores, SnowflakeConnectionProvider Conn, WarehouseCfg Cfg, IPipeCtx PipeCtx, RootCfg RootCfg) {
  public async Task StageUpdate(ILogger log, bool fullLoad = false, string[] tableNames = null, string[] tags = null) {
    var dbName = Conn.Cfg.DbName();
    log = log.ForContext("db", dbName);
    log.Information("StageUpdate - started for snowflake host {Host}, db {Db}, role {Role}", Conn.Cfg.Host, dbName, Conn.Cfg.Role);
    if (Cfg.Mode == WarehouseMode.ProdReadIfDev && !RootCfg.IsProd())
      throw new("Won't write to stage from a development environment when warehouse in readonly mode");
    var sw = Stopwatch.StartNew();
    var tables = Table.All
      .Where(t => tableNames?.Contains(t.Table, OrdinalIgnoreCase) == true
        || tags?.Any(s => t.Tags?.Contains(s, OrdinalIgnoreCase) == true) == true
        || tableNames.None() && tags.None()).ToArray();
    await tables.BlockDo(async t => { await UpdateTable(t, fullLoad, log.ForContext("table", t.Table)); }, Cfg.LoadTablesParallel);
    log.Information("StageUpdate - {Tables} updated in {Duration}", tables.Join("|", t => t.Table), sw.Elapsed.HumanizeShort());
  }

  public async Task UpdateTable(StageTableCfg t, bool fullLoad, ILogger log) {
    DateTime? latestTs;
    using (var db = await Conn.Open(log)) {
      await EnsureTableCreated(db, t.Table);
      if (t.Dir == null) return;

      log.Information("StageUpdate - {Table} ({LoadType})", t.Table, fullLoad ? "full" : "incremental");
      latestTs = fullLoad ? null : await t.LatestTimestamp(db);
    }

    if (latestTs == null)
      await FullLoad(t, log);
    else
      await Incremental(t, latestTs.Value, log);
  }

  /// <summary>Creates the given table if it doesn't exist.</summary>
  /// <returns>true if the table needed to be created</returns>
  public static async Task EnsureTableCreated(ILoggedConnection<SnowflakeDbConnection> db, string table) =>
    await db.Execute("create table", $"create table if not exists {table} (v Variant)");

  ISimpleFileStore Store(StageTableCfg t) => Stores.Store(t.StoreType);

  async Task Incremental(StageTableCfg t, DateTime latestTs, ILogger log) {
    var filesToCopy = await Optimize(t, latestTs.FileSafeTimestamp(), log); // optimise files newer than the last load
    if (filesToCopy.None()) {
      log.Debug("{Scope} - incremental load has no files to copy", nameof(Stage));
      return;
    }
    using var db = await Conn.Open(log);
    if (filesToCopy.Length > 1000) throw new("copying 1k+ files not implemented");
    var (history, dur) = await CopyInto(db, t, filesToCopy, log).WithDuration();
    log.Information("StageUpdate - {Table} incremental load of {Rows} rows ({Size}) took {Duration}",
      t.Table, history.Rows.ToMetricShort(), history.Size.HumanizeShort(), dur.HumanizeShort());
  }

  async Task FullLoad(StageTableCfg t, ILogger log, CancellationToken cancel = default) {
    if (t.IsNativeStore)
      await Optimize(t, ts: null, log, cancel);
    using var db = await Conn.Open(log);
    await db.Execute("truncate table", $"truncate table {t.Table}"); // no transaction, stage tables aren't reported on so don't need to be available
    var (history, dur) = await CopyInto(db, t, filesToCopy: null, log).WithDuration();
    log.Information("StageUpdate - {Table} full load of {Rows} rows ({Size}) took {Duration}",
      t.Table, history.Rows.ToMetricShort(), history.Size.HumanizeShort(), dur.HumanizeShort());
  }

  public async Task<SPath[]> Optimize(StageTableCfg t, string ts = null, ILogger log = null, CancellationToken cancel = default) {
    var store = Store(t);
    var plan = await store.OptimisePlan(Cfg.Optimise, t.Dir, ts, log);
    var execPlan = plan.Where(p => p.Files.Any()).ToArray();
    await execPlan.Pipe(PipeCtx, b => ProcessOptimisePlan(b, t.StoreType, Inject<ILogger>()), log: log, cancel: cancel);
    return plan.Select(p => p.DestFile).ToArray();
  }

  [Pipe]
  public async Task<bool> ProcessOptimisePlan(IReadOnlyCollection<OptimiseBatch> plan, DataStoreType storeType, ILogger log) {
    var runId = ShortGuid.Create(4);
    var store = Stores.Store(storeType);
    log = log.ForContext("RunId", runId);
    log.Debug("YtStage - starting optimisation plan ({RunId}) first path '{Dest}' out of {TotalBatches}", runId, plan.First().DestFile, plan.Count);
    await store.Optimise(Cfg.Optimise, plan, log);
    return true;
  }

  async Task<CopyIntoResult> CopyInto(ILoggedConnection<IDbConnection> db, StageTableCfg t, SPath[] filesToCopy, ILogger log) {
    var (stage, store) = t.StoreType switch {
      DataStoreType.DbStage => (Cfg.Stage, Stores.Store(t.StoreType)),
      DataStoreType.Private => (Cfg.Private, Stores.Store(t.StoreType)),
      _ => throw new InvalidOperationException($"No warehouse stage for store type {t.StoreType}")
    };
    return await t.CopyInto(store, stage, db, filesToCopy, log).Then(r => r.Summary());
  }

  public static class Table {
    public static readonly StageTableCfg[] All = {
      new("channels", "channel_stage") { Tags = new[] { Standard } },
      new("users", "user_stage") { Tags = new[] { Standard } },
      new("channel_reviews", "channel_review_stage"),
      new("videos", "video_stage") { Tags = new[] { Standard } },
      new("recs", "rec_stage") { Tags = new[] { Standard } },
      new("video_extra", "video_extra_stage") { Tags = new[] { Standard } },
      new("searches", "search_stage"),
      new("captions", "caption_stage") { Tags = new[] { Standard } },
      new("comments", "comment_stage") { Tags = new[] { Standard } },
      new("rec_exports_processed", "rec_export_stage") { StoreType = DataStoreType.Private, TsCol = nameof(TrafficSourceRow.FileUpdated) },
      new("link_meta", "link_meta_stage")
    };

    public static class Tag {
      public const string Standard = "standard";
      public const string Pod      = "pod";
      public const string Caption  = "caption";
    }
  }
}

public record StageTableCfg(SPath Dir, string Table) {
  public bool          IsNativeStore { get; init; } = true;
  public string        TsCol         { get; init; }
  public DataStoreType StoreType     { get; init; } = DataStoreType.DbStage;
  public string[]      Tags          { get; init; } = Array.Empty<string>();
}