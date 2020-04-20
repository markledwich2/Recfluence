using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Dapper;
using Humanizer.Localisation;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Db {
  public class LoggedConnection {
    public LoggedConnection(DbConnection conn, ILogger log) {
      Conn = conn;
      Log = log;
    }

    public DbConnection Conn { get; }
    public ILogger      Log  { get; }

    public async Task Execute(string operation, string sql, DbTransaction transaction = null) =>
      await ExecWithLog(() => Conn.ExecuteAsync(sql, transaction: transaction), sql, operation);

    public async Task<IEnumerable<T>> Query<T>(string operation, string sql, DbTransaction transaction = null) =>
      await ExecWithLog(() => Conn.QueryAsync<T>(sql, transaction), sql, operation);

    public async Task<T> ExecuteScalar<T>(string operation, string sql, object param = null, DbTransaction transaction = null, TimeSpan? timeout = null) =>
      await ExecWithLog(() => Conn.ExecuteScalarAsync<T>(sql, param, transaction, timeout?.TotalSeconds.RoundToInt()), sql, operation);

    public async Task<DbDataReader> ExecuteReader(string operation, string sql, object param, DbTransaction transaction = null) =>
      await ExecWithLog(() => Conn.ExecuteReaderAsync(sql, param, transaction), sql, operation);

    async Task<T> ExecWithLog<T>(Func<Task<T>> exec, string sql, string operation) {
      TimedResult<T> res;
      try {
        Log.Debug("{Operation} - started: {Sql}", operation, sql);
        res = await exec().WithDuration();
      }
      catch (Exception ex) {
        Log.Error(ex, "{Operation} - Error ({Error}) with sql: {Sql}", operation, ex.Message, sql);
        throw;
      }
      Log.Debug("{Operation} - completed in {Duration}: {Sql}", operation, res.Duration.HumanizeShort(minUnit: TimeUnit.Millisecond), sql);
      return res.Result;
    }
  }

  public static class LoggedConnectionEx {
    public static LoggedConnection AsLogged(this DbConnection conn, ILogger log) => new LoggedConnection(conn, log);
  }
}