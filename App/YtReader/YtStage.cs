﻿using System;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Humanizer.Bytes;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;

namespace YtReader {
  public class WarehouseCfg {
    [Required] public string      Stage              { get; set; } = "yt_data";
    [Required] public OptimiseCfg Optimise           { get; set; } = new OptimiseCfg();
    [Required] public int         LoadTablesParallel { get; set; } = 4;
    public            string[]    Roles              { get; set; } = {"dataform", "sysadmin"};
    public            int         MetadataParallel   { get; set; } = 8;
  }

  public class YtStage {
    readonly YtStores                    Stores;
    readonly StorageCfg                  StorageCfg;
    readonly SnowflakeConnectionProvider Conn;
    readonly WarehouseCfg                Cfg;
    readonly AzureBlobFileStore          DbStore;

    public YtStage(YtStores stores, StorageCfg storageCfg, SnowflakeConnectionProvider conn, WarehouseCfg cfg) {
      Stores = stores;
      DbStore = stores.Store(DataStoreType.Db);
      StorageCfg = storageCfg;
      Conn = conn;
      Cfg = cfg;
    }

    public async Task StageUpdate(ILogger log, bool fullLoad = false, string[] tableNames = null) {
      log = log.ForContext("db", Conn.Cfg.DbName());
      log.Information("StageUpdate - started for db {Db}", Conn.Cfg.DbName());
      var sw = Stopwatch.StartNew();
      var tables = YtWarehouse.AllTables.Where(t => tableNames.None() || tableNames?.Contains(t.Table, StringComparer.OrdinalIgnoreCase) == true).ToArray();
      await tables.BlockAction(async t => {
        var table = t.Table;
        using var db = await Conn.OpenConnection(log);
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

    async Task Incremental(LoggedConnection db, string table, StageTableCfg t, DateTime latestTs) {
      await DbStore.Optimise(Cfg.Optimise, t.Dir, latestTs.FileSafeTimestamp(), db.Log); // optimise files newer than the last load
      var ((_, rows, size), dur) = await CopyInto(db, table, t).WithDuration();
      db.Log.Information("StageUpdate - {Table} incremental load of {Rows} rows ({Size}) took {Duration}",
        table, rows, size.Humanize("#.#"), dur.HumanizeShort());
    }

    async Task FullLoad(LoggedConnection db, string table, StageTableCfg t) {
      if (t.IsNativeStore)
        await DbStore.Optimise(Cfg.Optimise, t.Dir, null, db.Log); // optimise all files when performing a full load
      await db.Execute("truncate table", $"truncate table {table}"); // no transaction, stage tables aren't reported on so don't need to be available
      var ((_, rows, size), dur) = await CopyInto(db, table, t).WithDuration();
      db.Log.Information("StageUpdate - {Table} full load of {Rows} rows ({Size}) took {Duration}",
        table, rows, size.Humanize("#.#"), dur.HumanizeShort());
    }

    async Task<(string[] files, long rows, ByteSize size)> CopyInto(LoggedConnection db, string table, StageTableCfg t) {
      var startTime = await db.ExecuteScalar<string>("current time", "select current_timestamp()::string");

      var sql = $"copy into {table} from @{Cfg.Stage}/{StorageCfg.DbPath}/{t.Dir}/ file_format=(type=json)";
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
      new StageTableCfg($"userscrape/results/{name}", $"us_{name}_stage", isNativeStore: false, tsCol: "updated");

    public static readonly StageTableCfg[] AllTables = {
      UsTable("rec"),
      UsTable("feed"),
      UsTable("watch"),
      UsTable("ad"),
      new StageTableCfg("channels", "channel_stage"),
      new StageTableCfg("videos", "video_stage"),
      new StageTableCfg("recs", "rec_stage"),
      new StageTableCfg("video_extra", "video_extra_stage"),
      new StageTableCfg("searches", "search_stage"),
      new StageTableCfg("captions", "caption_stage"),
      new StageTableCfg(dir: null, "dbv1_video_stage", isNativeStore: false),
      new StageTableCfg(dir: null, "dbv1_rec_stage", isNativeStore: false),
    };
  }

  public class StageTableCfg {
    public StageTableCfg(string dir, string table, bool isNativeStore = true, string tsCol = null) {
      Dir = dir;
      Table = table;
      IsNativeStore = isNativeStore;
      TsCol = tsCol ?? (isNativeStore ? "Updated" : null);
    }

    public StringPath Dir           { get; }
    public string     Table         { get; }
    public bool       IsNativeStore { get; }
    public string     TsCol         { get; }
  }
}