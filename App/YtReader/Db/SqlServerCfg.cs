using System.ComponentModel.DataAnnotations;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Mutuo.Etl.Db;
using Polly;
using Serilog;
using SysExtensions.Net;
using SysExtensions.Security;

namespace YtReader.Db {
  public class SqlServerCfg {
    [Required] public string     Host          { get; set; }
    [Required] public string     Db            { get; set; }
    [Required] public NameSecret Creds         { get; set; }
    [Required] public string     DefaultSchema { get; set; } = "dbo";
  }

  public static class MsSqlEx {
    public static async Task<ILoggedConnection<SqlConnection>> OpenConnection(this SqlServerCfg cfg, ILogger log) {
      var policy = Policy.Handle<SqlException>(e => e.Number == 40143).RetryWithBackoff("connecting to sql server", 3, log);
      var conn = cfg.Connection();
      await policy.ExecuteAsync(() => conn.OpenAsync());
      return conn.AsLogged(log);
    }

    public static SqlConnection Connection(this SqlServerCfg cfg) =>
      new SqlConnection(cfg.ConnectionStirng(), new SqlCredential(cfg.Creds.Name, cfg.Creds.SecureString()));

    public static string ConnectionStirng(this SqlServerCfg cfg) =>
      $"Server={cfg.Host};Initial Catalog={cfg.Db};Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=10;";
  }
}