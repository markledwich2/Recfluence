using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Mutuo.Etl.Db;
using Serilog;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
  public class YtSync {
    readonly SnowflakeCfg SnowflakeCfg;
    readonly SqlServerCfg SqlServerCfg;

    public YtSync(SnowflakeCfg snowflakeCfg, SqlServerCfg sqlServerCfg, ILogger log) {
      SnowflakeCfg = snowflakeCfg;
      SqlServerCfg = sqlServerCfg;
    }

    public async Task SyncDb(SyncDbCfg cfg, ILogger log, IReadOnlyCollection<string> tables = null, bool fullLoad = false, int optionLimit = 0) {
      var toRun = cfg.Tables.Where(t => tables == null || !tables.Any() || tables.Contains(t.Name)).ToArray();
      var dur = await toRun.BlockAction(async t => {
        var tableLog = log.ForContext("Table", t.Name);

        tableLog.Information("Table Sync {Table} - started", t.Name);
        using var sourceConn = await SnowflakeCfg.OpenConnection();
        using var destConn = await SqlServerCfg.OpenConnection(tableLog);
        var sync = new DbSync(
          new SnowflakeSourceDb(sourceConn, SnowflakeCfg.Schema, tableLog),
          new MsSqlDestDb(destConn, SqlServerCfg.DefaultSchema, t.FullTextCatalog, tableLog));
        
        var res = await sync.UpdateTable(t, tableLog, fullLoad, optionLimit)
          .WithWrappedException($"sync table '{t.Name}'", tableLog) // log the error and rethrow. Won't interrupt untill other sync have completed
          .WithDuration();

        tableLog.Information("Talbe sync {Table} - completed in {Duration}", t.Name, res.HumanizeShort());

      }, cfg.Parallel).WithDuration();
      log.Information("Completed loading {Tables} in {Duration}", toRun.Select(t => t.Name), dur.HumanizeShort());
    }
  }
}