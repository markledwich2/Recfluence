using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;
using Dapper;
using Humanizer.Localisation;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Db {
  public class LoggedConnection : IDisposable {
    readonly bool CloseConnection;

    /// <summary>Wraps a connection with logging.</summary>
    public LoggedConnection(DbConnection conn, ILogger log, bool closeConnection = true) {
      CloseConnection = closeConnection;
      Conn = conn;
      Log = log;
    }

    public DbConnection Conn { get; }
    public ILogger      Log  { get; }

    public void Dispose() {
      if (CloseConnection) Conn?.Dispose();
    }

    public async Task Execute(string operation, string sql, DbTransaction transaction = null) =>
      await ExecWithLog(() => Conn.ExecuteAsync(sql, transaction: transaction), sql, operation);

    public IEnumerable<T> Query<T>(string operation, string sql,
      object param = null, DbTransaction transaction = null, TimeSpan? timeout = null, bool buffered = true) =>
      ExecWithLog(() => Conn.Query<T>(sql, param, transaction, commandTimeout: timeout?.TotalSeconds.RoundToInt(), buffered: buffered), sql, operation);

    public async Task<IEnumerable<T>> QueryAsync<T>(string operation, string sql,
      object param = null, DbTransaction transaction = null, TimeSpan? timeout = null) =>
      await ExecWithLog(() => Conn.QueryAsync<T>(sql, param, transaction, timeout?.TotalSeconds.RoundToInt()), sql, operation);

    public async Task<T> ExecuteScalar<T>(string operation, string sql, object param = null, DbTransaction transaction = null, TimeSpan? timeout = null) =>
      await ExecWithLog(() => Conn.ExecuteScalarAsync<T>(sql, param, transaction, timeout?.TotalSeconds.RoundToInt()), sql, operation);

    public async Task<DbDataReader> ExecuteReaderAsync(string operation, string sql, object param, DbTransaction transaction = null) =>
      await ExecWithLog(() => Conn.ExecuteReaderAsync(sql, param, transaction), sql, operation);

    T ExecWithLog<T>(Func<T> exec, string sql, string operation) {
      T res;
      var sw = Stopwatch.StartNew();
      try {
        res = exec();
      }
      catch (Exception ex) {
        Log.Error(ex, "{Operation} - Error ({Error}) with sql: {Sql}", operation, ex.Message, sql);
        throw;
      }
      Log.Debug("{Operation} - completed in {Duration}: {Sql}", operation, sw.Elapsed.HumanizeShort(minUnit: TimeUnit.Millisecond), sql);
      return res;
    }

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