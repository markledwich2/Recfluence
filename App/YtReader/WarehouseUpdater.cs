using System;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;

namespace YtReader {
  public class WarehouseCfg {
    [Required] public string      Stage    { get; set; }
    [Required] public OptimiseCfg Optimise { get; set; } = new OptimiseCfg();
    [Required] public int LoadTablesParallel { get; set; } = 4;
  }

  public class WarehouseUpdater {
    readonly ISimpleFileStore   Store;
    readonly StorageCfg StorageCfg;
    readonly ConnectionProvider Conn;
    readonly ILogger            Log;
    readonly WarehouseCfg       Cfg;

    public WarehouseUpdater(ISimpleFileStore store, StorageCfg storageCfg, ConnectionProvider conn, WarehouseCfg cfg, ILogger log) {
      Store = store;
      StorageCfg = storageCfg;
      Conn = conn;
      Cfg = cfg;
      Log = log.ForContext("db", conn.Name);
    }

    public async Task WarehouseUpdate(bool fullLoad = false, string[] tableNames = null) {
      var sw = Stopwatch.StartNew();
      var tables = YtWarehouse.AllTables.Where(t => tableNames.None() || tableNames?.Contains(t.Table) == true).ToArray();
      await tables.BlockAction(async t => {
        var table = t.Table;
        //var files = await Store.Files(t.Dir, true);
        using var db = await Conn.OpenLoggedConnection(Log);
        await db.Execute("create table", $"create table if not exists {table} (v Variant)");
        Log.Information("WarehouseUpdate {Table} - ({LoadType})", table, fullLoad ? "full" : "incremental");
        var latestTs = fullLoad ? null : await db.ExecuteScalar<DateTime?>("latest timestamp", $"select max(v:{t.TsColumn}::timestamp_ntz) from {table}");
        if (latestTs == null) 
          await FullLoad(db, table, t);
        else
          await Incremental(db, table, t, latestTs.Value);
      }, Cfg.LoadTablesParallel);
      Log.Information("WarehouseUpdate - {Tables} updated in {Duration}", tables.Join("|", t => t.Table), sw.Elapsed.HumanizeShort());
    }

    async Task Incremental(LoggedConnection db, string table, StageTableCfg t, DateTime latestTs) {
      await Store.Optimise(Cfg.Optimise, t.Dir, latestTs.FileSafeTimestamp(), Log); // optimise files newer than the last load
      var (rows, dur) = await CopyInto(db, table, t).WithDuration();
      Log.Information("WarehouseUpdate {Table} - incremental load of {Rows} rows took {Duration}",
        table, rows, dur.HumanizeShort());
    }

    async Task FullLoad(LoggedConnection db, string table, StageTableCfg t) {
      var trans = await db.Conn.BeginTransactionAsync();
      try {
        var sw = Stopwatch.StartNew();
        await db.Execute("truncate table", $"truncate table {table}", transaction: trans);
        await Store.Optimise(Cfg.Optimise, t.Dir, null, Log); // optimise all files when performing a full load
        var rows = await CopyInto(db, table, t, trans);
        await trans.CommitAsync();
        Log.Information("WarehouseUpdate {Table} - full load of {Rows} took {Duration}", table, rows, sw.Elapsed.HumanizeShort());
      }
      catch {
        await trans.RollbackAsync();
        throw;
      }
    }

    async Task<int> CopyInto(LoggedConnection db, string table, StageTableCfg t, DbTransaction trans = null) {
      var sql = $"copy into {table} from @{Cfg.Stage}/{StorageCfg.DbPath}/{t.Dir}/ file_format=(type=json)";
      return await db.Execute("copy into", sql, transaction: trans);
    }
  }

  public static class YtWarehouse {
    public static StageTableCfg[] AllTables => new[] {Tables.Channels, Tables.Videos, Tables.Recs, Tables.VideoExtra, Tables.Searches, Tables.Captions};

    public static class Tables {
      public static readonly StageTableCfg Channels   = new StageTableCfg("channels", "channel_stage");
      public static readonly StageTableCfg Videos     = new StageTableCfg("videos", "video_stage");
      public static readonly StageTableCfg Recs       = new StageTableCfg("recs", "rec_stage");
      public static readonly StageTableCfg VideoExtra = new StageTableCfg("video_extra", "video_extra_stage");
      public static readonly StageTableCfg Searches   = new StageTableCfg("searches", "search_stage");
      public static readonly StageTableCfg Captions   = new StageTableCfg("captions", "caption_stage");
    }
  }

  public class StageTableCfg {
    public StageTableCfg(string dir, string table) {
      Dir = dir;
      Table = table;
    }

    public string Dir      { get; }
    public string Table    { get; }
    public string TsColumn { get; set; } = "Updated";
  }
}