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
using YtReader.Data;
using YtReader.Store;
using YtReader.Yt;

namespace YtReader;

[Command("rec-export", Description = "Process recommendation exports")]
public record RecExportCmd(ILogger Log, RecExport Export) : ICommand {
  [CommandOption("parts", shortName: 'p')]
  public string Parts { init; get; }

  public async ValueTask ExecuteAsync(IConsole console) =>
    await Export.ProcessRecExports(Parts.ParseEnums<RecExport.Part>(), Log, console.RegisterCancellationHandler());
}

public record RecExport(BlobStores Stores, YtWeb Yt, YtDataform Dataform, Stage Stage) {
  public enum Part { Process, Stage }
  
  static readonly Regex FileInfoRegex = new("^Traffic source (?'from'\\d+-\\d+-\\d+)_(?'to'\\d+-\\d+-\\d+) (?'channel'[^.]+)", RegexOptions.Compiled);

  record ExportFileInfo(string Channel, DateTime From, DateTime To, DateTime? Modified);

  static ExportFileInfo GetExportFileInfo(FileListItem f) {
    var m = FileInfoRegex.Match(f.Path.Name);
    if (m.Groups.Count < 3)
      throw new InvalidOperationException($"unable to parse export info from file name '{f.Path.Name}'");
    return new(m.Groups["channel"].Value, m.Groups["from"].Value.ParseDate(), m.Groups["to"].Value.ParseDate(), f.Modified?.UtcDateTime);
  }

  enum SourceExportType { Rec, Cat }

  public async Task ProcessRecExports(Part[] parts, ILogger log, CancellationToken cancel) {
    if (parts.ShouldRun(Part.Process)) {
      var store = Stores.Store(DataStoreType.Private);
      await using (var sink = new JsonlSink<TrafficSourceRow>(store, "rec_exports_processed", r => r.FileUpdated.FileSafeTimestamp(), new(), log)) {
        var md = await sink.LatestFile();
        var latestModified = md?.Ts.ParseFileSafeTimestamp();
        var blobs = await store.List("rec_exports").SelectManyList();
        var newBlobs = latestModified != null ? blobs.Where(b => b.Modified > latestModified).ToList() : blobs;

        log.Information("Processing {NewExports}/{AllExports} exports", newBlobs.Count, blobs.Count);

        await newBlobs.BlockDo(async b => {
          log.Information("Processing {Path}", b.Path);
          using var csvReader = await GetExportTableCsv(store, b);
          var exportInfo = GetExportFileInfo(b);
          await csvReader.GetRecords<TrafficSourceRecExportRow>()
            .BlockDo(async row => {
              var source = row.Source.Split(".");
              var sourceType = source.Length != 2 || source[0] != "YT_RELATED" ? SourceExportType.Cat : SourceExportType.Rec;
              var videoId = sourceType == SourceExportType.Rec ? source[1] : null;
              await sink.Append(new TrafficSourceRow {
                From = exportInfo.From,
                To = exportInfo.To,
                ToChannelTitle = exportInfo.Channel,
                Impressions = row.Impressions,
                Source = row.Source,
                AvgViewDuration = row.AvgViewDuration,
                Views = row.Views,
                SourceType = sourceType.EnumString(),
                ImpressionClickThrough = row.ImpressionClickThrough,
                WatchTimeHrsTotal = row.WatchTimeHrsTotal,
                FileUpdated = exportInfo.Modified ?? DateTime.MinValue,
                FromVideoTitle = row.FromVideoTitle,
                FromVideoId = videoId
              });
            });
          log.Information("Completed processing traffic source exports for {Path}", b.Path);
        }, parallel: 4);
      }
      log.Information("Completed processing traffic source exports");
    }

    if (parts.ShouldRun(Part.Stage)) {
      //await Dataform.Update(log, actions: new[] { "rec-export" });
      await Stage.StageUpdate(log, fullLoad: true, new[] { "rec_export_stage" });
      log.Information("Staged traffic source exports");
    }
  }

  static async Task<CsvReader> GetExportTableCsv(ISimpleFileStore store, FileListItem b) {
    var stream = await store.Load(b.Path);
    var zip = new ZipArchive(stream, ZipArchiveMode.Read, leaveOpen: false);
    var csvStream = new StreamReader(
      zip.GetEntry("Table data.csv")?.Open() ?? throw new InvalidOperationException("expected export to have 'Table data.csv'"),
      Encoding.UTF8);
    var csvReader = new CsvReader(csvStream, CsvExtensions.DefaultConfig with { HeaderValidated = null });
    return csvReader;
  }
}

public record TrafficSourceRow : TrafficSourceRecExportRow {
  public DateTime From           { get; init; }
  public DateTime To             { get; init; }
  public DateTime FileUpdated    { get; init; }
  public string   FromVideoId    { get; set; }
  public string   ToChannelTitle { get; set; }
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