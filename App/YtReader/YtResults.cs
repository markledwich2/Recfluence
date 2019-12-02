using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using Dapper;
using Mutuo.Etl;
using Serilog;
using SysExtensions;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;
using SysExtensions.Threading;

class ResultQuery {
  public string Name  { get; }
  public string Query { get; }
  public string Desc { get; }

  public ResultQuery(string name, string query = null, string desc = null) {
    Name = name;
    Query = query;
    Desc = desc;
  }
}

namespace YtReader {
  public class YtResults {
    readonly SnowflakeCfg     Cfg;
    readonly ISimpleFileStore Store;
    readonly ILogger          Log;

    const string Version = "v2";

    public YtResults(SnowflakeCfg cfg, ISimpleFileStore store, ILogger log) {
      Cfg = cfg;
      Store = store;
      Log = log;
    }

    public async Task SaveResults() {
      using var db = await Cfg.OpenConnection();

      ResultQuery Q(string name, string query = null, string desc = null) => new ResultQuery(name, query, desc);

      var queries = new[] {
        Q("vis_channel_stats", desc: "data combined from classifications + information (from the YouTube API)"),

        Q("vis_channel_recs", desc: "aggregated recommendations between channels (scraped form the YouTube website)"),

        Q("vis_category_recs", desc: "aggregate recommendations between all combinations of the categories  available on recfluence.net"),

        Q("channel_classification", desc: "each reviewers classifications and the calculated majority view (data entered independently from reviewers)"),

        Q("icc_tags", desc: "channel classifications in a format used to calculate how consistent reviewers are when tagging"),

        Q("icc_lr", desc: "channel classifications in a format used to calculate how consistent reviewers are when deciding left/right/center"),
        // videos & recs are too large to share in a file. Use snowflake directly to share this data at-cost.
        /*("video_latest", @"select video_id, video_title, channel_id, channel_title,
         upload_date, avg_rating, likes, dislikes, views, thumb_standard from video_latest")*/
      };

      var tmpDir = TempDir();

      var results = await queries.BlockTransform(async q => new {File=await SaveResult(db, tmpDir, q), Query=q}, 4);

      var sw = Stopwatch.StartNew();
      var zipPath = results.First().File.Parent().Combine("recfluence_shared_data.zip");
      using (var zipFile = ZipFile.Open(zipPath.FullPath, ZipArchiveMode.Create)) {
        var readmeFile = TempDir().CreateFile("readme.txt", $@"Recfluence data generated {DateTime.UtcNow.ToString("yyyy-MM-dd")}

{results.Join("\n\n", r => $"*{r.Query.Name}*\n  {r.Query.Desc}" )}
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
    async Task<FPath> SaveResult(IDbConnection db, FPath tempDir, ResultQuery q) {
      var query = q.Query ?? $"select * from {q.Name}";
      var sw = Stopwatch.StartNew();
      Log.Information("Saving result {Name}: {Query}", q.Name, query);
      var reader = Reader(db, query);

      var fileName = $"{q.Name}.csv.gz";
      var tempFile = tempDir.Combine(fileName);
      using (var fileWriter = tempFile.Open(FileMode.Create, FileAccess.Write))
        await reader.WriteCsvGz(fileWriter, fileName, Log);

      // save to both latest and the current date 
      await SaveToLatestAndDateDirs(fileName, tempFile);
      
      Log.Information("Complete saving result {Name} in {Duration}", q.Name, sw.Elapsed);
      return tempFile;
    }

    async Task SaveToLatestAndDateDirs(string fileName, FPath tempFile) =>
      await Task.WhenAll(
        Store.Save(StringPath.Relative(Version, DateTime.UtcNow.ToString("yyyy-MM-dd")).Add(fileName), tempFile),
        Store.Save(StringPath.Relative(Version, "latest").Add(fileName), tempFile)
      );

    static IDataReader Reader(IDbConnection db, string query) {
      var cmd = db.CreateCommand();
      cmd.CommandText = query;
      var reader = cmd.ExecuteReader();
      return reader;
    }
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