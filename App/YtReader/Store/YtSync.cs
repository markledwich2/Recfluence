using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Semver;
using Serilog;
using Snowflake.Data.Client;
using SysExtensions;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;

namespace YtReader.Store {
  public class YtSync {
    readonly SnowflakeConnectionProvider Snowflake;
    readonly SqlServerCfg                SqlServerCfg;
    readonly SyncDbCfg                   Cfg;
    readonly WarehouseCfg                WhCfg;
    readonly SemVersion                  Version;
    readonly AzureBlobFileStore          Store;

    public YtSync(SnowflakeConnectionProvider snowflake, SqlServerCfg sqlServerCfg, SyncDbCfg cfg, YtStores stores, WarehouseCfg whCfg, SemVersion version) {
      Snowflake = snowflake;
      SqlServerCfg = sqlServerCfg;
      Cfg = cfg;
      WhCfg = whCfg;
      Version = version;
      Store = stores.Store(DataStoreType.Root);
    }

    public async Task SyncDb(ILogger log, CancellationToken cancel, IReadOnlyCollection<string> restrictTables = null, bool fullLoad = false,
      int optionLimit = 0) {
      var tables = new[] {
        new SyncTableCfg("video_stats",
          new SyncColCfg("video_id") {Id = true, SqlType = "varchar(20)"}, // this is a big table, so optimising for perf with types more than normal
          new SyncColCfg("date") {Id = true, SqlType = "date"},
          new SyncColCfg("channel_id") {SqlType = "varchar(30)"},
          new SyncColCfg("views") {SqlType = "float"},
          new SyncColCfg("watch_hours") {SqlType = "float"},
          new SyncColCfg("tags") {SqlType = "varchar(1000)"},
          new SyncColCfg("lr") {SqlType = "varchar(10)"}
        ) {
          Sql = @"select d.*, array_to_string(cl.tags, '|') as tags, cl.lr 
from video_stats_daily d
inner join channel_latest cl on d.channel_id = cl.channel_id"
        }
      };

      var toRun = tables.Where(t => restrictTables == null || !restrictTables.Any() || restrictTables.Contains(t.Name)).ToArray();
      var dur = await toRun.BlockAction(async t => {
        var tableLog = log.ForContext("Table", t.Name);

        tableLog.Information("Table Sync {Table} - started", t.Name);
        using var sourceConn = await Snowflake.OpenConnection(log);
        using var destConn = await SqlServerCfg.OpenConnection(tableLog);
        var sync = new DbSync(
          new SnowflakeSourceDb(sourceConn.Conn, Snowflake.Cfg.Schema, WhCfg.Stage, WhCfg.FileMb.Megabytes(), tableLog),
          new MsSqlDestDb(destConn.Conn, Version.Prerelease.HasValue() ? Version.Prerelease : SqlServerCfg.DefaultSchema, tableLog), Store);

        var res = await sync.UpdateTable(t, tableLog, cancel, fullLoad, optionLimit)
          .WithWrappedException($"sync table '{t.Name}'", tableLog) // log the error and rethrow. Won't interrupt untill other sync have completed
          .WithDuration();

        tableLog.Information("Table sync {Table} - completed in {Duration}", t.Name, res.HumanizeShort());
      }, Cfg.Parallel).WithDuration();
      log.Information("Completed loading {Tables} in {Duration}", toRun.Select(t => t.Name), dur.Duration.HumanizeShort());
    }
  }
}