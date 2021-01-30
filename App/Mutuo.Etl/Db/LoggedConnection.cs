using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Db {

  public interface ILoggedConnection<out TC> : IDisposable where TC : IDbConnection  {
    TC      Conn { get; }
    ILogger Log  { get; }
    Task<long> Execute(string desc, string sql, object param = null, DbTransaction transaction = null, TimeSpan? timeout = null);
    /// <summary>Like the dapper Query function. use when you need to stream the rows non-greedily</summary>
    IEnumerable<T> QueryBlocking<T>(string operation, string sql,
      object param = null, DbTransaction transaction = null, TimeSpan? timeout = null, bool buffered = false);
    Task<IReadOnlyCollection<T>> Query<T>(string desc, string sql,
      object param = null, DbTransaction transaction = null, TimeSpan? timeout = null);
    /// <summary>Wrapper for dappers ExecuteScalarAsync</summary>
    /// <param name="operation">a descriptoin of the operation (for logging/correlation purposes)</param>
    Task<T> ExecuteScalar<T>(string operation, string sql, object param = null, DbTransaction transaction = null, TimeSpan? timeout = null);
    Task<DbDataReader> ExecuteReader(string operation, string sql, object param = null, DbTransaction transaction = null);
  }
  
  public class LoggedConnection<TC> : ILoggedConnection<TC>  where TC : IDbConnection   {
    readonly bool CloseConnection;

    /// <summary>Wraps a connection with logging.</summary>
    public LoggedConnection(TC conn, ILogger log, bool closeConnection = true) {
      CloseConnection = closeConnection;
      Conn = conn;
      Log = log;
    }

    public TC Conn { get; }
    public ILogger      Log  { get; }

    public void Dispose() {
      if (CloseConnection) Conn?.Dispose();
    }

    public async Task<long> Execute(string desc, string sql, object param = null, DbTransaction transaction = null, TimeSpan? timeout = null) =>
      await ExecWithLog(() => Conn.ExecuteAsync(sql, param, transaction, commandTimeout: timeout?.TotalSeconds.RoundToInt()), sql, desc, param);

    /// <summary>Like the dapper Query function. use when you need to stream the rows non-greedily</summary>
    public IEnumerable<T> QueryBlocking<T>(string operation, string sql,
      object param = null, DbTransaction transaction = null, TimeSpan? timeout = null, bool buffered = false) =>
      ExecWithLog(() => Conn.Query<T>(sql, param, transaction,
        commandTimeout: timeout?.TotalSeconds.RoundToInt(), buffered: buffered), sql, operation, param);

    public async Task<IReadOnlyCollection<T>> Query<T>(string desc, string sql,
      object param = null, DbTransaction transaction = null, TimeSpan? timeout = null) =>
      (await ExecWithLog(() => Conn.QueryAsync<T>(sql, param, transaction, timeout?.TotalSeconds.RoundToInt()), sql, desc, param))
      .ToArray(); // make greedy load explicit load because that is what dapper does under the covers for async anyway.

    /// <summary>Wrapper for dappers ExecuteScalarAsync</summary>
    /// <param name="operation">a descriptoin of the operation (for logging/correlation purposes)</param>
    public async Task<T> ExecuteScalar<T>(string operation, string sql, object param = null, DbTransaction transaction = null, TimeSpan? timeout = null) =>
      await ExecWithLog(() => Conn.ExecuteScalarAsync<T>(sql, param, transaction, timeout?.TotalSeconds.RoundToInt()), sql, operation, param);

    public async Task<DbDataReader> ExecuteReader(string operation, string sql, object param, DbTransaction transaction = null) =>
      await ExecWithLog(() => (Conn as DbConnection).ExecuteReaderAsync(sql, param, transaction), sql, operation, param);

    T ExecWithLog<T>(Func<T> exec, string sql, string operation, object param) {
      T res;
      var sw = Stopwatch.StartNew();
      try {
        res = exec();
      }
      catch (Exception ex) {
        Log.Error(ex, "{Operation} - Error ({Error}) with sql: {Sql}", operation, ex.Message, sql);
        throw;
      }
      Log.Debug("{Operation} - completed in {Duration}: {Sql}\nparams:{@Params}", operation, sw.Elapsed.HumanizeShort(), sql, param);
      return res;
    }

    async Task<T> ExecWithLog<T>(Func<Task<T>> exec, string sql, string operation, object param) {
      T res;
      TimeSpan duration;
      try {
        Log.Debug("{Operation} - started: {Sql}", operation, sql);
        (res, duration) = await exec().WithDuration();
      }
      catch (Exception ex) {
        Log.Error(ex, "{Operation} - Error ({Error}) with sql: {Sql}", operation, ex.Message, sql);
        throw;
      }
      Log.Debug("{Operation} - completed in {Duration}: {Sql}\nparams:{@Params}", operation, duration.HumanizeShort(), sql, param);
      return res;
    }
  }

  public static class LoggedConnectionEx {
    public static ILoggedConnection<T> AsLogged<T>(this T conn, ILogger log) where T : DbConnection  => new LoggedConnection<T>(conn, log);
  }
}