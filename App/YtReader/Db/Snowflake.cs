using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using SysExtensions.Security;

namespace YtReader {
  public class SnowflakeCfg {
    public NameSecret Creds     { get; set; } = new NameSecret();
    [Required] public string     Account   { get; set; }
    [Required] public string     Warehouse { get; set; }
    [Required] public string     Db        { get; set; }
    [Required] public string     Schema    { get; set; }
    
    
  }
  
  

  public static class SnowflakeEx {
    public static async Task<SnowflakeDbConnection> OpenConnection(this SnowflakeCfg cfg) {
      var conn = cfg.Connection();
      await conn.OpenAsync();
      return conn;
    }

    public static SnowflakeDbConnection Connection(this SnowflakeCfg cfg) => new SnowflakeDbConnection
      {ConnectionString = cfg.ConnectionStirng(), Password = cfg.Creds.SecureString()};

    public static string ConnectionStirng(this SnowflakeCfg cfg) =>
      $"account={cfg.Account};user={cfg.Creds.Name};db={cfg.Db};schema={cfg.Schema};warehouse={cfg.Warehouse}";
  }
}