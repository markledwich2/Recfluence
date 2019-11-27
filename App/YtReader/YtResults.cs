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
using SysExtensions;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
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
      using var db = await Cfg.OpenConnection();

      var queries = new[] {
        ("channel_stats", @"select * from vis_channel_stats"),
        
        ("channel_recs", @"select * from vis_channel_recs"),

        ("channel_classification", null),
        
        // videos & recs are too large to share in a file. Use snowflake directly to share this data at-cost.
        /*("video_latest", @"select video_id, video_title, channel_id, channel_title,
         upload_date, avg_rating, likes, dislikes, views, thumb_standard from video_latest")*/
      };

      var tmpDir = TempDir();
      
      var resultFiles = await queries.BlockTransform(q => SaveResult(db, tmpDir, q.Item1, q.Item2), 4);

      var sw = Stopwatch.StartNew();
      var zipPath = resultFiles.First().Parent().Combine("recfluence_shared_data.zip");
      using (var zipFile = ZipFile.Open(zipPath.FullPath, ZipArchiveMode.Create)) {
        
        var readmeFile = TempDir().CreateFile("readme.txt", $@"Recfluence data generated {DateTime.UtcNow.ToString("yyyy-MM-dd")}

channel_stats.csv: data combined from classifications + information (from the YouTube API)
channel_recs.csv: aggregated recommendations between channels (scraped form the YouTube website)
channel_classification.csv: each reviewers classifications and the calculated majority view (data entered independently from reviewers)
        ");
        zipFile.CreateEntryFromFile(readmeFile.FullPath, readmeFile.FileName);
        
        foreach (var f in resultFiles) {
          var name = f.FileNameWithoutExtension;
          var e = zipFile.CreateEntry(name);
          using var ew = e.Open();
          var fr = f.Open(FileMode.Open, FileAccess.Read);
          var gz = new GZipStream(fr, CompressionMode.Decompress);
          await gz.CopyToAsync(ew);
        }
      }
      await Store.Save(StringPath.Relative("latest").Add(zipPath.FileName), zipPath);
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
    async Task<FPath> SaveResult(IDbConnection db, FPath tempDir, string name, string query = null) {
      query ??= $"select * from {name}";
      var sw = Stopwatch.StartNew();
      Log.Information("Saving result {Name}: {Query}", name, query);
      var cmd = db.CreateCommand();
      cmd.CommandText = query;
      var reader = cmd.ExecuteReader();

      var fileName = $"{name}.csv.gz";
      var tempFile = tempDir.Combine(fileName);
      using (var fileWriter = tempFile.Open(FileMode.Create, FileAccess.Write))
        await reader.WriteCsvGz(fileWriter, fileName, Log);

      // save to both latest and the current date 
      await Task.WhenAll(
        Store.Save(StringPath.Relative(DateTime.UtcNow.ToString("yyyy-MM-dd")).Add(fileName), tempFile),
        Store.Save(StringPath.Relative("latest").Add(fileName), tempFile)
      );

/*      var writer = await Store.OpenForWrite(path, new FileProps {ContentType = "text/css; charset=utf8", Encoding = "gzip"});
      await reader.WriteCsvGz(writer);*/
      Log.Information("Complete saving result {Name} in {Duration}", name, sw.Elapsed);
      return tempFile;
    }


  }

  public static class SnowflakeResultHelper {

    public static async Task WriteCsvGz(this IDataReader reader, Stream stream, string desc, ILogger log) {
      await using var zipWriter = new GZipStream(stream, CompressionLevel.Optimal);
      await using var streamWriter = new StreamWriter(zipWriter);
      using var csvWriter = new CsvWriter(streamWriter);

      foreach (var col in reader.FieldRange().Select(reader.GetName))
        csvWriter.WriteField(col);
      csvWriter.NextRecord();

      var lines = 0L;
      while (reader.Read()) {
        foreach (var i in reader.FieldRange())
          csvWriter.WriteField(reader[i]);
        csvWriter.NextRecord();
        if(lines > 0 && lines % 10000 == 0)
          log.Debug("written {Rows} rows to {Desc} ",lines, desc);
        lines++;
      }
    }

    static IEnumerable<int> FieldRange(this IDataRecord reader) => Enumerable.Range(0, reader.FieldCount);
  }
}