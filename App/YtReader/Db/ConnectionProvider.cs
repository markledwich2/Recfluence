using System;
using System.Data.Common;
using System.Threading.Tasks;
using Mutuo.Etl.Db;
using Serilog;

namespace YtReader.Db {
  public class ConnectionProvider {
    public string Name { get; }
    readonly Func<Task<DbConnection>> GetConnection;

    public ConnectionProvider(Func<Task<DbConnection>> getConnection, string name) {
      Name = name;
      GetConnection = getConnection;
    }

    public Task<DbConnection> OpenConnection() => GetConnection();
    public async Task<LoggedConnection> OpenLoggedConnection(ILogger log) => (await GetConnection()).AsLogged(log);
  }


}