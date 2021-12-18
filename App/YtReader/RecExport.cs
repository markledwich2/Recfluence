using System.IO;
using System.IO.Compression;
using System.Text;
using CliFx;
using CliFx.Attributes;
using CliFx.Infrastructure;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using Mutuo.Etl.Blob;
using SysExtensions.IO;
using YtReader.Store;
using YtReader.Yt;

namespace YtReader;

[Command("rec-export", Description = "Process recommendation exports")]
public record RecExportCmd(ILogger Log, BlobStores Stores, YtWeb Yt) : ICommand {
  public async ValueTask ExecuteAsync(IConsole console) => await RecExport.ProcessRecExports(Stores.Store(DataStoreType.Private), Yt, Log);
}

public static class RecExport {
  static readonly Regex FileInfoRegex = new("^Traffic source (?'from'\\d+-\\d+-\\d+)_(?'to'\\d+-\\d+-\\d+) (?'channel'[^.]+)", RegexOptions.Compiled);

  record ExportFileInfo(string Channel, DateTime From, DateTime To, DateTime? Modified);

  static ExportFileInfo GetExportFileInfo(this FileListItem f) {
    var m = FileInfoRegex.Match(f.Path.Name);
    if (m.Groups.Count < 3)
      throw new InvalidOperationException($"unable to parse export info from file name '{f.Path.Name}'");
    return new(m.Groups["channel"].Value, m.Groups["from"].Value.ParseDate(), m.Groups["to"].Value.ParseDate(), f.Modified?.UtcDateTime);
  }

  enum SourceExportType { Rec, Cat }

  public static async Task ProcessRecExports(ISimpleFileStore store, YtWeb ytWeb, ILogger log) {
    await using var sink = new JsonlSink<TrafficSourceRow>(store, "rec_exports_processed", r => r.FileUpdated.FileSafeTimestamp(), new(), log);

    var md = await sink.LatestFile();
    var latestModified = md?.Ts.ParseFileSafeTimestamp();

    var blobs = await store.List("rec_exports").SelectManyList();
    var newBlobs = latestModified != null ? blobs.Where(b => b.Modified > latestModified).ToList() : blobs;

    log.Information("Processing {NewExports}/{AllExports} exports", newBlobs.Count, blobs.Count);

    string firstVideoId = null;

    var allRows = newBlobs.BlockDo(async b => {
      log.Information("Processing {Path}", b.Path);
      using var csvReader = await GetExportTableCsv(store, b);
      var exportInfo = b.GetExportFileInfo();
      var rows = csvReader.GetRecords<TrafficSourceRecExportRow>()
        .Select(row => {
          var source = row.Source.Split(".");
          var sourceType = source.Length != 2 || source[0] != "YT_RELATED" ? SourceExportType.Cat : SourceExportType.Rec;
          var videoId = sourceType == SourceExportType.Rec ? source[1] : null;
          firstVideoId ??= videoId;
          return (row, videoId, exportInfo);
        });
      log.Information("Completed processing traffic source exports for {Path}", b.Path);
      return rows.ToArray();
    }, parallel: 4).SelectMany();

    var innerTube = await ytWeb.InnerTubeFromVideoPage(firstVideoId, log);

    await allRows.BlockDo(async r => {
      var (row, videoId, exportInfo) = r;
      var fromVideo = videoId == null ? null : await ytWeb.GetExtra(log, innerTube, videoId, new[] { ExtraPart.EExtra }, maxComments: 0).Then(e => e.Extra);
      await sink.Append(new TrafficSourceRow {
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
        FromVideoId = fromVideo?.VideoId,
        FromVideoTitle = fromVideo?.Title,
        ImpressionClickThrough = row.ImpressionClickThrough,
        WatchTimeHrsTotal = row.WatchTimeHrsTotal,
        FileUpdated = exportInfo.Modified ?? DateTime.MinValue
      });
    }, parallel: 8);

    log.Information("Completed processing traffic source exports");
  }

  static async Task<CsvReader> GetExportTableCsv(ISimpleFileStore store, FileListItem b) {
    var stream = await store.Load(b.Path);
    var zip = new ZipArchive(stream);
    using var csvStream = new StreamReader(
      zip.GetEntry("Table data.csv")?.Open() ?? throw new InvalidOperationException("expected export to have 'Table data.csv'"),
      Encoding.UTF8);
    var csvReader = new CsvReader(csvStream, CsvExtensions.DefaultConfig);
    return csvReader;
  }
}

public record TrafficSourceRow : TrafficSourceRecExportRow {
  public string   FromVideoId      { get; init; }
  public string   FromChannelId    { get; init; }
  public string   FromChannelTitle { get; init; }
  public string   ToChannelTitle   { get; init; }
  public DateTime From             { get; init; }
  public DateTime To               { get; init; }
  public DateTime FileUpdated      { get; init; }
}

public record TrafficSourceRecExportRow {
  [Name("Traffic source")] public string Source         { get; init; }
  [Name("Source type")]    public string SourceType     { get; init; }
  [Name("Source title")]   public string FromVideoTitle { get; init; }
  public                          long?  Impressions    { get; init; }
  [Name("Impressions click-through rate (%)")]
  public decimal? ImpressionClickThrough { get;                             init; }
  public                                 long?     Views             { get; init; }
  [Name("Average view duration")] public TimeSpan? AvgViewDuration   { get; init; }
  [Name("Watch time (hours)")]    public decimal?  WatchTimeHrsTotal { get; init; }
}