using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Mutuo.Etl.Db;
using Serilog;
using Snowflake.Data.Client;
using SysExtensions;
using SysExtensions.Security;
using SysExtensions.Text;

namespace YtReader.Db {
  public class SnowflakeCfg {
    public            NameSecret Creds     { get; set; } = new NameSecret();
    [Required] public string     Host      { get; set; }
    public            string     Warehouse { get; set; }
    public            string     Db        { get; set; }
    public            string     Schema    { get; set; }
    public            string     Role      { get; set; }
    public            string     DbSuffix  { get; set; }
  }

  public class SnowflakeConnectionProvider {
    public SnowflakeCfg Cfg { get; }

    public SnowflakeConnectionProvider(SnowflakeCfg cfg) => Cfg = cfg;

    public async Task<ILoggedConnection<SnowflakeDbConnection>> Open(ILogger log, string db = null, string schema = null, string role = null) {
      var conn = Cfg.Connection(db, schema, role);
      await conn.OpenAsync();
      return conn.AsLogged(log);
    }
  }

  public static class SnowflakeConnectionEx {
    public static SnowflakeDbConnection Connection(this SnowflakeCfg cfg, string db = null, string schema = null, string role = null) =>
      new SnowflakeDbConnection
        {ConnectionString = cfg.Cs(db, schema, role), Password = cfg.Creds.SecureString()};

    public static string Cs(this SnowflakeCfg cfg, string db = null, string schema = null, string role = null) =>
      Cs(new (string name, string value)[] {
        ("account", cfg.Host.Split('.').FirstOrDefault()),
        ("host", cfg.Host),
        ("user", cfg.Creds.Name),
        ("db", db ?? cfg.DbName()),
        ("schema", schema ?? cfg.Schema),
        ("warehouse", cfg.Warehouse),
        ("role", role ?? cfg.Role)
      }.Where(v => v.value.HasValue()).ToArray());

    public static string Cs(params (string name, string value)[] values) => values.Join(";", v => $"{v.name}={v.value}");

    public static async Task SetSessionParams(this ILoggedConnection<SnowflakeDbConnection> db, params (SfParam param, object value)[] @params) =>
      await db.Execute("alter session",
        $"alter session set {@params.Join(" ", v => $"{v.param.EnumString()}={ValueSql(v.value)}")}"); // reduce mem usage (default 4)

    static string ValueSql(object value) =>
      value switch {
        string s => $"'{s}'",
        int i => i.ToString(),
        _ => throw new NotImplementedException()
      };
  }

  public enum SfParam {
    [EnumMember(Value = "CLIENT_PREFETCH_THREADS")]
    ClientPrefetchThreads,
    [EnumMember(Value = "TIMEZONE")] Timezone,
    [EnumMember(Value = "CLIENT_TIMESTAMP_TYPE_MAPPING")]
    ClientTimestampTypeMapping
  }
}