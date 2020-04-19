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
        log.Information("Starting sync of table {Table}", t.Name);
        using var sourceConn = await SnowflakeCfg.OpenConnection();
        using var destConn = await SqlServerCfg.OpenConnection(log);
        var sync = new DbSync(
          new SnowflakeSourceDb(sourceConn, SnowflakeCfg.Schema, log),
          new MsSqlDestDb(destConn, SqlServerCfg.DefaultSchema, t.FullTextCatalog, log));
        var dur = await sync.UpdateTable(t, log, fullLoad, optionLimit).WithDuration();
      }, cfg.Parallel).WithDuration();
      log.Information("Completed loading {Tables} in {Duration}", toRun.Select(t => t.Name), dur.HumanizeShort());
    }
  }
}