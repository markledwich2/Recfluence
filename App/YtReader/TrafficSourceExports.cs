using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.IO;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.YtWebsite;

namespace YtReader {
  public static class TrafficSourceExports {
    public static async Task Process(ISimpleFileStore store, WebScraper web, ILogger log) {
      var blobs = await store.List("rec_exports").SelectManyList();
      //blobs = blobs.Where(b => b.Path == "rec_exports/Traffic source 2019-07-01_2019-08-01 David Pakman Show.zip").ToList();

      var fileInfoRegex = new Regex("^Traffic source (?'from'\\d+-\\d+-\\d+)_(?'to'\\d+-\\d+-\\d+) (?'channel'[^.]+)", RegexOptions.Compiled);

      var appendStore = new JsonlStore<TrafficSourceRow>(store, "rec_exports_processed", r => r.FileUpdated.FileSafeTimestamp(), log);

      var md = await appendStore.LatestFile();
      var latestModified = md?.Ts.ParseFileSafeTimestamp();

      var newBlobs = latestModified != null
        ? blobs.Where(b => b.Modified > latestModified).ToList()
        : blobs;

      log.Information("Processing {NewExports}/{AllExports} exports", newBlobs.Count, blobs.Count);

      foreach (var b in newBlobs) {
        log.Information("Processing {Path}", b.Path);

        var m = fileInfoRegex.Match(b.Path.Name);
        if (m.Groups.Count < 3)
          throw new InvalidOperationException($"unable to parse export info from file name '{b.Path.Name}'");
        var exportInfo = new {
          Channel = m.Groups["channel"].Value,
          From = m.Groups["from"].Value.ParseDate(),
          To = m.Groups["to"].Value.ParseDate()
        };

        var stream = await store.Load(b.Path);
        var zip = new ZipArchive(stream);
        using var csvStream = new StreamReader(
          zip.GetEntry("Table data.csv")?.Open() ?? throw new InvalidOperationException("expected export to have 'Table data.csv'"),
          Encoding.UTF8);
        using var csvReader = new CsvReader(csvStream, CsvExtensions.DefaultConfig);

        var records = csvReader.GetRecords<TrafficSourceExportRow>().ToList();
        var rows = (await records.BlockFunc(Process, 4,
            progressUpdate: p => log.Debug("Processing traffic sources for {Path}: {Rows}/{TotalRows}", b.Path, p.Completed, records.Count)))
          .NotNull().ToList();

        await appendStore.Append(rows, log);
        log.Information("Completed processing traffic source exports for {Path}", b.Path);

        async Task<TrafficSourceRow> Process(TrafficSourceExportRow row) {
          var source = row.Source.Split(".");
          if (source.Length != 2 || source[0] != "YT_RELATED")
            return null; // total at the top or otherwise. not interested
          var videoId = source[1];
          var fromVideo = await web.GetVideo(videoId, log);

          return new TrafficSourceRow {
            ToChannelTitle = exportInfo.Channel,
            From = exportInfo.From,
            To = exportInfo.To,
            Impressions = row.Impressions,
            Source = row.Source,
            AvgViewDuration = row.AvgViewDuration,
            Views = row.Views,
            SourceType = row.SourceType,
            FromChannelId = fromVideo?.ChannelId,
            FromChannelTitle = fromVideo?.ChannelTitle,
            FromVideoId = fromVideo?.Id,
            FromVideoTitle = fromVideo?.Title,
            ImpressionClickThrough = row.ImpressionClickThrough,
            WatchTimeHrsTotal = row.WatchTimeHrsTotal,
            FileUpdated = b.Modified?.UtcDateTime ?? DateTime.MinValue
          };
        }
      }

      log.Information("Completed processing traffic source exports");
    }
  }

  public class TrafficSourceRow : TrafficSourceExportRow {
    public string   FromVideoId      { get; set; }
    public string   FromChannelId    { get; set; }
    public string   FromChannelTitle { get; set; }
    public string   ToChannelTitle   { get; set; }
    public DateTime From             { get; set; }
    public DateTime To               { get; set; }
    public DateTime FileUpdated      { get; set; }
  }

  public class TrafficSourceExportRow {
    [Name("Traffic source")] public string Source         { get; set; }
    [Name("Source type")]    public string SourceType     { get; set; }
    [Name("Source title")]   public string FromVideoTitle { get; set; }
    public                          long?  Impressions    { get; set; }
    [Name("Impressions click-through rate (%)")]
    public decimal? ImpressionClickThrough { get;                             set; }
    public                                 long?     Views             { get; set; }
    [Name("Average view duration")] public TimeSpan? AvgViewDuration   { get; set; }
    [Name("Watch time (hours)")]    public decimal?  WatchTimeHrsTotal { get; set; }
  }
}