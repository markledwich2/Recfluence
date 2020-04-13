using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using Snowflake.Data.Client;
using SysExtensions.Collections;
using SysExtensions.Reflection;
using SysExtensions.Text;

namespace Mutuo.Etl.Db {
  public interface ISourceDb : ICommonDb {
    Task<DbDataReader> Read(TableId table, SyncTableCfg tableCfg, object tsValue = null, int limit = 0);
  }

  public class SnowflakeSourceDb : ISourceDb, ICommonDb {
    static readonly Regex SafeRegex = new Regex("^[A-Za-z_]+$", RegexOptions.Compiled);

    public SnowflakeSourceDb(SnowflakeDbConnection conn, string defaultSchema) {
      if (conn.State == ConnectionState.Closed) throw new InvalidOperationException("requires an open connection");
      Connection = conn;
      DefaultSchema = defaultSchema;
    }

    public DbConnection Connection    { get; }
    public string       DefaultSchema { get; }

    public async Task<DbDataReader> Read(TableId table, SyncTableCfg tableCfg, object tsValue = null, int limit = 0) {
      var colList = tableCfg.SelectedCols.Any()
        ? tableCfg.SelectedCols
          .Concat(tableCfg.TsCol, tableCfg.IdCol).NotNull().Distinct() // always include the special cols if they are specified
          .Select(Sql)
        : new[] {"*"};
      var selectSql = $"select {colList.Join(", ")} from {Sql(table)}";
      var whereSql = tsValue == null || tsValue.GetType().DefaultForType() == tsValue
        ? ""
        : $" where {Sql(tableCfg.TsCol ?? throw new InvalidOperationException("tsValue specified without a column"))} > :maxTs";
      var limitSql = limit == 0 ? "" : " limit :limit";
      var statements = new[] {selectSql, whereSql, limitSql};
      return await Connection.ExecuteReaderAsync(statements.Join("\n"), new {maxTs = tsValue, limit});
    }

    public string Sql(string name) => QuoteIfRequired(name);

    public string Sql(TableId table) =>
      new[] {table.Schema, table.Table}.NotNull().Join(".", QuoteIfRequired);

    static string QuoteIfRequired(string s) => SafeRegex.IsMatch(s) ? s : s.DoubleQuote();
  }
}