using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using Mutuo.Etl;
using Serilog;
using Snowflake.Data.Client;
using SysExtensions.Text;

namespace YtReader.Results {
  public class YtResults {
    readonly SnowflakeCfg     Cfg;
    readonly ISimpleFileStore Store;
    readonly ILogger          Log;

    public YtResults(SnowflakeCfg cfg, ISimpleFileStore store, ILogger log) {
      Cfg = cfg;
      Store = store;
      Log = log;
    }

    public async Task SaveResults() {
      using var db = await OpenConnection();
      await Task.WhenAll(
        SaveResult(db, "channel_stats", @"select channel_id,
      channel_title,
      channel_views,
      country,
      relevance,
      subs,
      daily_views,
      relevant_daily_views,
      views,
      from_date,
      to_date,
      lr,
      tags,
      ideology,
      media,
      manoel,
      ain,
      logo_url from channel_stats"),
        
        SaveResult(db, "channel_recs", @"
    select from_channel_id, to_channel_id, relevant_impressions,  rec_view_channel_percent
  from channel_recs where rec_view_channel_percent > 0.005"),
        
        SaveResult(db, "channel_classification"));
    }

    async Task SaveResult(IDbConnection db, string name, string query = null) {
      query ??= $"select * from {name}";
      var sw = Stopwatch.StartNew();
      Log.Information("Saving result {Name}: {Query}", name, query);
      var cmd = db.CreateCommand();
      cmd.CommandText = query;
      var reader = cmd.ExecuteReader();
      var path = await reader.WriteToCsvGz(Store, DateTime.UtcNow.ToString("yyyy-MM-dd"), name);
      Log.Information("Complete saving result {Name} to {Path} in {Duration}", name, path, sw.Elapsed);
    }

    async Task<SnowflakeDbConnection> OpenConnection() {
      var conn = new SnowflakeDbConnection {ConnectionString = Cfg.ConnectionStirng(), Password = Cfg.Creds.SecureString()};
      await conn.OpenAsync();
      return conn;
    }
  }

  public static class SnowflakeResultHelper {
    public static string ConnectionStirng(this SnowflakeCfg cfg) =>
      $"account={cfg.Account};user={cfg.Creds.Name};db={cfg.Db};schema={cfg.Schema};warehouse={cfg.Warehouse}";

    public static async Task<StringPath> WriteToCsvGz(this IDataReader reader, ISimpleFileStore store, StringPath dir, string name) {
      var path = dir.Add($"{name}.csv.gz");
      var writer = await store.OpenForWrite(path, new FileProps {ContentType = "text/css; charset=utf8", Encoding = "gzip"});
      await using var zipWriter = new GZipStream(writer, CompressionLevel.Optimal);
      await using var streamWriter = new StreamWriter(zipWriter);
      using var csvWriter = new CsvWriter(streamWriter);

      foreach (var col in reader.FieldRange().Select(reader.GetName))
        csvWriter.WriteField(col);
      csvWriter.NextRecord();

      while (reader.Read()) {
        foreach (var i in reader.FieldRange())
          csvWriter.WriteField(reader[i]);
        csvWriter.NextRecord();
      }
      

      return path;
    }

    static IEnumerable<int> FieldRange(this IDataRecord reader) => Enumerable.Range(0, reader.FieldCount);
  }
}