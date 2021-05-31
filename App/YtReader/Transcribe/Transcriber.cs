using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.TranscribeService;
using Amazon.TranscribeService.Model;
using Flurl.Http;
using Humanizer;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using YtReader.Web;
using YtReader.Yt;
using static System.Net.Http.HttpCompletionOption;
using static Amazon.TranscribeService.TranscriptionJobStatus;
using static YtReader.Transcribe.TranscribeParts;

// ReSharper disable InconsistentNaming

namespace YtReader.Transcribe {
  public enum TranscribeParts {
    [EnumMember(Value = "transcribe")] PTranscribe,
    [EnumMember(Value = "stage")]      PStage
  }

  public record Transcriber(BlobStores Stores, SnowflakeConnectionProvider Sf, AwsCfg Aws, YtStore StoreDb, Stage Stage) {
    static readonly Regex                         SafeNameRe  = new("[^\\w0-9]", RegexOptions.Compiled);
    readonly        S3Store                       StoreMedia  = new(Aws.S3, "media");
    readonly        S3Store                       StoreTrans  = new(Aws.S3, "transcripts");
    readonly        AmazonTranscribeServiceClient TransClient = new(Aws.CredsBasic, Aws.RegionEndpoint);
    string SafeName(string name) => SafeNameRe.Replace(name, "");

    StringPath BlobPath(Platform? platform, string sourceId, string extension) =>
      //var extension = Extension(platform);
      StringPath.Relative(
        platform.EnumString(), $"{SafeName(sourceId)}.{extension}");

    public async Task TranscribeVideos(ILogger log, CancellationToken cancel = default, Platform? platform = null,
      int? limit = null, string queryName = null, TranscribeParts[] parts = null) {
      log = log.ForContext("Function", nameof(TranscribeVideos));


      if (parts.ShouldRun(PTranscribe)) {
        using var db = await Sf.Open(log);
        var tempDir = YtResults.TempDir();

        var videos = await db.QueryAsync<VideoData>("video media_url", $@"
with vids as ({(queryName == null ? "select * from video_extra" : TranscribeSql.Sql[queryName])})
select q.video_id, e.source_id, e.media_url, e.channel_id, e.platform
from vids q
join video_extra e on e.video_id = q.video_id 
where media_url is not null {platform.Do(p => $"and platform = {p.EnumString().SingleQuote()}")}
and not exists (select * from caption s where s.video_id = q.video_id)
order by views desc nulls last
{limit.Do(l => $"limit {l}")}
").ToListAsync();

        await videos
          .BlockMap(async v => (mediaPath: await CopyVideo(log, v, tempDir, cancel), v)
            , parallel: 4, cancel: cancel)
          .BlockMap(async p => {
            var (transPath, job, trans) = await StartTrans(p.mediaPath, log);
            return (transPath, job, trans, p.v);
          }, parallel: 4)
          .BlockMap(async r => {
            var job = await WaitForCompletedTrans(log, r.job);
            return (r.transPath, job, r.v, trans: r.trans ?? await LoadTrans(r.transPath, log));
          }, parallel: 4, cancel: cancel)
          .Select(r => r.trans.AwsToVideoCaption(r.job, r.v.video_id, r.v.channel_id, r.v.platform))
          .Batch(10)
          .BlockDo(c => StoreDb.Captions.Append(c, log));
      }

      if (parts.ShouldRun(PStage))
        await Stage.StageUpdate(log, tableNames: new[] {"caption_stage"});

      // todo save standard VideoCaptions from media storage into standard caption location
      log.Information("Transcribe - completed transcribing");
    }

    async Task<StringPath> CopyVideo(ILogger log, VideoData v, FPath tempDir, CancellationToken cancel) {
      var mediaUrl = v.media_url.AsUrl();
      var ext = mediaUrl.PathSegments.LastOrDefault()?.Split(".").LastOrDefault() ?? throw new("not implemented. Currently relying on extension in url");
      var blobPath = BlobPath(v.platform, v.source_id, ext);
      var existing = await StoreMedia.Info(blobPath);
      if (existing != null) {
        log.Debug("Transcribe - {BlobPath} exists, ignoring", blobPath);
        return blobPath;
      }
      log.Debug("Transcribe - loading media from {Url} to {MediaUrl}", mediaUrl, StoreMedia.S3Uri(blobPath));

      var localFile = tempDir.Combine(blobPath.Tokens.ToArray());
      localFile.EnsureDirectoryExists();

      try {
        using var res = await v.media_url.WithTimeout(30.Minutes()).SendWithRetry("get media", log: log, completionOption: ResponseHeadersRead);
        using var rs = await res.GetStreamAsync();
        using var ws = localFile.Open(FileMode.Create);
        var totalBytes = res.Headers.TryGetFirst("Content-Length", out var l) ? l.TryParseInt() : null;
        await rs.CopyToAsync(ws, b => log.Debug("Transcribe - loading {Url} - {Transferred}/{Total}",
            mediaUrl, b.Bytes().Humanize("#.#"), totalBytes?.Bytes().Humanize("#.#") ?? "unknown bytes"),
          cancel, 100.Kilobytes(), 10.Seconds());
      }
      catch (Exception ex) {
        log.Error(ex, "Transcribe - failed loading {Url}", mediaUrl);
        throw;
      }

      await StoreMedia.Save(blobPath, localFile, log);
      log.Debug("Transcribe - saved media {MediaUrl}", StoreMedia.S3Uri(blobPath).ToString());
      localFile.Delete();
      return blobPath;
    }

    async Task<(StringPath path, TranscriptionJob job, TransRoot trans)> StartTrans(StringPath p, ILogger log) {
      var transPath = p.WithExtension(".json");
      if (await StoreTrans.Exists(transPath)) {
        var trans = await LoadTrans(transPath, log);
        var job = await TransClient.GetTranscriptionJobAsync(new() {TranscriptionJobName = trans.jobName});
        return (transPath, job.TranscriptionJob, trans);
      }
      var res = await TransClient.StartTranscriptionJobAsync(new() {
        IdentifyLanguage = true,
        MediaFormat = p.ExtensionsString,
        Media = new() {
          MediaFileUri = StoreMedia.S3Uri(p).ToString()
        },
        TranscriptionJobName = $"{SafeName(p.NameSansExtension)}-{ShortGuid.Create(4)}",
        OutputBucketName = StoreTrans.Cfg.Bucket,
        OutputKey = StoreTrans.BasePath.Add(transPath),
        Settings = new() {
          ShowSpeakerLabels = true,
          MaxSpeakerLabels = 4
        }
      });
      log.Debug("Transcribe - started transcription {MediaUrl}", res.TranscriptionJob?.Media?.MediaFileUri);
      return (transPath, res.TranscriptionJob, null);
    }

    //static readonly JsonSerializerSettings JSettings = TransRoot.JsonSettings();
    async Task<TransRoot> LoadTrans(StringPath path, ILogger log) => await StoreTrans.Load(path, log).Then(s => s.ToObject<TransRoot>());

    async Task<TranscriptionJob> WaitForCompletedTrans(ILogger log, TranscriptionJob startJob) {
      if (startJob == null) return null;
      var lastLog = DateTime.UtcNow;
      while (true) {
        var job = await TransClient.GetTranscriptionJobAsync(new() {TranscriptionJobName = startJob.TranscriptionJobName});
        var tj = job.TranscriptionJob;
        if (tj.TranscriptionJobStatus == IN_PROGRESS && lastLog.OlderThan(1.Minutes())) {
          log.Debug("Transcribe - waiting on transcription job '{Job}' to complete (Age {Duration})",
            tj.TranscriptionJobName, (DateTime.UtcNow - tj.StartTime).HumanizeShort());
          lastLog = DateTime.UtcNow;
        }
        if (tj.TranscriptionJobStatus.Value.In(COMPLETED.Value, FAILED.Value) || tj.StartTime.OlderThan(30.Minutes())) {
          if (tj.TranscriptionJobStatus == FAILED) log.Warning("Transcribe - failed: {@Job}", tj);
          else log.Information("Transcribe - {Url} - {Status}", tj.Transcript.TranscriptFileUri, tj.TranscriptionJobStatus);
          return tj;
        }
        await 10.Seconds().Delay();
      }
    }

    record VideoData(string video_id, string source_id, string media_url, string channel_id, Platform platform);
  }
}