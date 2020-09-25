using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Humanizer.Bytes;
using Serilog;
using Snowflake.Data.Client;
using SysExtensions.Collections;
using SysExtensions.Reflection;
using SysExtensions.Text;

namespace Mutuo.Etl.Db {
  public interface ISourceDb : ICommonDb {
    Task<DbDataReader> Read(string selectSql, SyncTableCfg tableCfg, object tsValue = null, int? limit = null);

    Task CopyTo(string path, string selectSql, SyncTableCfg tableCfg, object tsValue = null,
      int? limit = null);
  }

  public class SnowflakeSourceDb : ISourceDb {
    readonly        string   StageName;
    readonly        ByteSize FileSize;
    static readonly Regex    SafeRegex = new Regex("^[A-Za-z_]+$", RegexOptions.Compiled);

    public SnowflakeSourceDb(SnowflakeDbConnection conn, string defaultSchema, string stageName, ByteSize fileSize, ILogger log) {
      if (conn.State == ConnectionState.Closed) throw new InvalidOperationException("requires an open connection");
      StageName = stageName;
      FileSize = fileSize;
      Conn = conn.AsLogged(log);
      DefaultSchema = defaultSchema;
    }

    public ILoggedConnection<IDbConnection> Conn          { get; }
    public string                          DefaultSchema { get; }

    public async Task<DbDataReader> Read(string selectSql, SyncTableCfg tableCfg, object tsValue = null, int? limit = null) {
      var sql = SelectSql(selectSql, tableCfg, tsValue, limit);
      return await Conn.ExecuteReader(nameof(Read), sql, new {maxTs = tsValue, limit});
    }

    string SelectSql(string selectSql, SyncTableCfg tableCfg, object tsValue, int? limit) {
      var incremental = tsValue != null && tsValue.GetType().DefaultForType() != tsValue;
      var whereParts = new List<string>();
      if (incremental) whereParts.Add($"{Sql(tableCfg.TsCol ?? throw new InvalidOperationException("tsValue specified without a column"))} > :maxTs");
      if (tableCfg.Filter.HasValue()) whereParts.Add($"({tableCfg.Filter})");
      var orderBySql = incremental ? $"order by {tableCfg.TsCol} asc" : null;
      var limitSql = limit == null ? null : " limit :limit";
      var sql = new[] {
        selectSql,
        whereParts.None() ? null : "where " + whereParts.Join(" and \n\t"),
        orderBySql,
        limitSql
      }.NotNull().Join("\n");
      return sql;
    }

    public async Task CopyTo(string path, string selectSql, SyncTableCfg tableCfg, object tsValue = null, int? limit = null) {
      var sql = SelectSql(selectSql, tableCfg, tsValue, limit);
      var copySql = $@"copy into @{StageName}/{path}/ from ({sql})
file_format = (TYPE=CSV, COMPRESSION=NONE, FIELD_OPTIONALLY_ENCLOSED_BY ='""', RECORD_DELIMITER ='\r\n', NULL_IF=(''))
MAX_FILE_SIZE = {FileSize.Bytes:#}
      ";
      await Conn.Execute(nameof(CopyTo), copySql);
    }

    public string Sql(string name) => QuoteIfRequired(name);

    public string Sql(TableId table) =>
      new[] {table.Schema, table.Table}.NotNull().Join(".", QuoteIfRequired);

    static string QuoteIfRequired(string s) => SafeRegex.IsMatch(s) ? s : s.DoubleQuote();
  }
}