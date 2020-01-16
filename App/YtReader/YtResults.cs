using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using CsvHelper;
using Dapper;
using Mutuo.Etl;
using Serilog;
using SysExtensions;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Net;
using SysExtensions.Text;
using SysExtensions.Threading;

class ResQuery {
  public string Name       { get; }
  public string Query      { get; }
  public string Desc       { get; }
  public object Parameters { get; }

  public ResQuery(string name, string query = null, string desc = null, object parameters = null) {
    Name = name;
    Query = query;
    Desc = desc;
    Parameters = parameters;
  }
}

class FileQuery : ResQuery {
  public StringPath Path { get; set; }

  public FileQuery(string name, StringPath path, string desc = null, object parameters = null) : base(name, desc: desc, parameters: parameters) =>
    Path = path;
}

namespace YtReader {
  public class YtResults {
    readonly SnowflakeCfg     SnowflakeCfg;
    readonly ResultsCfg       ResCfg;
    readonly ISimpleFileStore Store;
    readonly ILogger          Log;
    readonly HttpClient       Http = new HttpClient();

    const string Version = "v2.3";

    public YtResults(SnowflakeCfg snowflakeCfg, ResultsCfg resCfg, ISimpleFileStore store, ILogger log) {
      SnowflakeCfg = snowflakeCfg;
      ResCfg = resCfg;
      Store = store;
      Log = log;
    }

    public async Task SaveResults() {
      using var db = await SnowflakeCfg.OpenConnection();

      var dateRangeParams = new {@from = "2019-11-01", to = "2020-01-14"};
      var queries = new[] {
        new FileQuery("vis_channel_stats", "sql/vis_channel_stats.sql", 
          "data combined from classifications + information (from the YouTube API)", dateRangeParams),
        
        new FileQuery("vis_category_recs", "sql/vis_category_recs.sql", 
          "aggregate recommendations between all combinations of the categories available on recfluence.net", dateRangeParams),
        
        new FileQuery("vis_channel_recs", "sql/vis_channel_recs.sql", 
          "aggregated recommendations between channels (scraped form the YouTube website)", dateRangeParams),
        
        new ResQuery("channel_classification", desc: "each reviewers classifications and the calculated majority view (data entered independently from reviewers)"),
        new ResQuery("icc_tags", desc: "channel classifications in a format used to calculate how consistent reviewers are when tagging"),
        new ResQuery("icc_lr", desc: "channel classifications in a format used to calculate how consistent reviewers are when deciding left/right/center"),

        // videos & recs are too large to share in a file. Use snowflake directly to share this data at-cost.
        /*("video_latest", @"select video_id, video_title, channel_id, channel_title,
         upload_date, avg_rating, likes, dislikes, views, thumb_standard from video_latest")*/
      };

      var tmpDir = TempDir();

      var results = await queries.BlockTransform(async q => new {File = await SaveResult(db, tmpDir, q), Query = q}, 4);

      var sw = Stopwatch.StartNew();
      var zipPath = results.First().File.Parent().Combine("recfluence_shared_data.zip");
      using (var zipFile = ZipFile.Open(zipPath.FullPath, ZipArchiveMode.Create)) {
        var readmeFile = TempDir().CreateFile("readme.txt", $@"Recfluence data generated {DateTime.UtcNow.ToString("yyyy-MM-dd")}

{results.Join("\n\n", r => $"*{r.Query.Name}*\n  {r.Query.Desc}")}
        ");
        zipFile.CreateEntryFromFile(readmeFile.FullPath, readmeFile.FileName);

        foreach (var f in results.Select(r => r.File)) {
          var name = f.FileNameWithoutExtension;
          var e = zipFile.CreateEntry(name);
          using var ew = e.Open();
          var fr = f.Open(FileMode.Open, FileAccess.Read);
          var gz = new GZipStream(fr, CompressionMode.Decompress);
          await gz.CopyToAsync(ew);
        }
      }

      await SaveToLatestAndDateDirs(zipPath.FileName, zipPath);
      Log.Information("Complete saving zip {Name} in {Duration}", zipPath.FileName, sw.Elapsed);
    }

    static FPath TempDir() {
      var path = Path.GetTempPath().AsPath().Combine(Guid.NewGuid().ToShortString());
      if (!path.Exists)
        path.CreateDirectory();
      return path;
    }

    /// <summary>
    ///   Saves the result for the given query to Storage and a local tmp file
    /// </summary>
    async Task<FPath> SaveResult(IDbConnection db, FPath tempDir, ResQuery q) {
      var sw = Stopwatch.StartNew();
      var reader = await ResQuery(db, q);
      var fileName = $"{q.Name}.csv.gz";
      var tempFile = tempDir.Combine(fileName);
      using (var fileWriter = tempFile.Open(FileMode.Create, FileAccess.Write))
        await reader.WriteCsvGz(fileWriter, fileName, Log);

      // save to both latest and the current date 
      await SaveToLatestAndDateDirs(fileName, tempFile);

      Log.Information("Complete saving result {Name} in {Duration}", q.Name, sw.Elapsed);
      return tempFile;
    }

    async Task<IDataReader> ResQuery(IDbConnection db, ResQuery q) {
      var query = q.Query ?? $"select * from {q.Name}";
      if (q is FileQuery f) {
        var req = new Uri(ResCfg.FileQueryUri + "/" + f.Path.StringValue).Get()
          .AddHeader("Cache-Control", "no-cache");
        
        var res = await Http.SendAsync(req);
        query = await res.ContentAsString();
      }
      Log.Information("Saving result {Name}: {Query}", q.Name, query);
      try {
        var reader = await db.ExecuteReaderAsync(query, q.Parameters, commandType: CommandType.Text);
        return reader;
      }
      catch (Exception ex) {
        throw new InvalidOperationException($"Error when executing '{q.Name}': {ex.Message}", ex);
      }
    }

    async Task SaveToLatestAndDateDirs(string fileName, FPath tempFile) =>
      await Task.WhenAll(
        Store.Save(StringPath.Relative(Version, DateTime.UtcNow.ToString("yyyy-MM-dd")).Add(fileName), tempFile),
        Store.Save(StringPath.Relative(Version, "latest").Add(fileName), tempFile),
        Store.Save(StringPath.Relative("latest").Add(fileName), tempFile)
      );
  }

  public static class SnowflakeResultHelper {
    public static async Task WriteCsvGz(this IDataReader reader, Stream stream, string desc, ILogger log) {
      await using var zipWriter = new GZipStream(stream, CompressionLevel.Optimal);
      await using var streamWriter = new StreamWriter(zipWriter);
      using var csvWriter = new CsvWriter(streamWriter);

      foreach (var col in reader.FieldRange().Select(reader.GetName)) csvWriter.WriteField(col);
      csvWriter.NextRecord();

      var lines = 0L;
      while (reader.Read()) {
        foreach (var i in reader.FieldRange()) {
          var o = reader[i];
          if (o is DateTime d)
            csvWriter.WriteField(d.ToString("O"));
          else
            csvWriter.WriteField(o);
        }
        csvWriter.NextRecord();
        if (lines > 0 && lines % 10000 == 0)
          log.Debug("written {Rows} rows to {Desc} ", lines, desc);
        lines++;
      }
    }

    static IEnumerable<int> FieldRange(this IDataRecord reader) => Enumerable.Range(0, reader.FieldCount);
  }
}