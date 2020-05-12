using System;
using System.Linq;
using System.Threading.Tasks;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;

namespace YtReader {
  public class WarehouseUpdater {
    readonly AppDb        Db;
    readonly ILogger      Log;
    readonly SnowflakeCfg Snowflake;
    readonly YtStore      Store;

    public WarehouseUpdater(AppDb db, SnowflakeCfg snowflake, YtStore store, ILogger log) {
      Db = db;
      Snowflake = snowflake;
      Store = store;
      Log = log;
    }

    public async Task WarehouseUpdate() {
      await Store.AllStores.BlockAction(s => s.Optimise(Log));

      await YtWarehouse.AllTables.BlockAction(async t => {
        var table = t.Table + "2";
        var files = await Store.Store.List(t.Dir, true, Log).SelectManyList();
        using var db = await Db.OpenLoggedConnection(Log);
        await db.ExecuteScalarAsync<int>("create table", $"create table if not exists {table} (v Variant)");
        var latestTs = await db.ExecuteScalarAsync<string>("latest timestamp", $"select max({t.TsColumn}) from {table}");

        if (latestTs == null) {
          await db.ExecuteScalarAsync<int>("full load copy into", $"copy into {table} from {Snowflake.Stage}/{t.Dir}/ file_format=(type=json)");
        }
        else {
          // filter to later files and records. We cna't rely on the default copy to because we are changing/optimisng files
          // its worth it because we want our blob storage to be performant/neat, and the incremental to be explicit
          var newerFiles = files.Select(StoreFileMd.FromFileItem).Where(f => string.Compare(f.Ts, latestTs, StringComparison.Ordinal) > 0).ToArray();
          await newerFiles.GroupBy(l => l.Path.Parent).BlockAction(async g => {
            var selectSql = $"select * from {Snowflake.Stage}/{g.Key} where v{t.TsColumn} > :latestTs) \n" +
                            $"file_format=(type=json) files({newerFiles.Join("\n ", f => f.Path.Name)})";
            await db.ExecuteScalarAsync<int>("incremental new data", $"copy into {table} from ({selectSql})", new {latestTs});
          });
        }
      });
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
    public string TsColumn { get; set; } = "updated";
  }
}