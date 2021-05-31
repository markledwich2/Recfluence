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
using Polly;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Net;
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

  public enum TranscribeMode {
    Query,
    Media
  }

  public record Transcriber(BlobStores Stores, SnowflakeConnectionProvider Sf, AwsCfg Aws, YtStore StoreDb, Stage Stage) {
    static readonly Regex                         SafeNameRe   = new("[^\\w0-9]", RegexOptions.Compiled);
    readonly        ISimpleFileStore              StoreForLoad = Stores.Store("import/temp");
    readonly        S3Store                       StoreMedia   = new(Aws.S3, "media");
    readonly        S3Store                       StoreTrans   = new(Aws.S3, "transcripts");
    readonly        AmazonTranscribeServiceClient TransClient  = new(Aws.CredsBasic, Aws.RegionEndpoint);

    string SafeName(string name) => SafeNameRe.Replace(name, "");

    StringPath BlobPath(Platform? platform, string sourceId, string extension) =>
      //var extension = Extension(platform);
      StringPath.Relative(
        platform.EnumString(), $"{SafeName(sourceId)}.{extension}");

    public async Task TranscribeVideos(ILogger log, CancellationToken cancel = default, Platform? platform = null,
      int? limit = null, string queryName = null, TranscribeParts[] parts = null, TranscribeMode mode = default, string[] sourceIds = null) {
      log = log.ForContext("Function", nameof(TranscribeVideos));

      if (parts.ShouldRun(PTranscribe)) {
        using var db = await Sf.Open(log);
        var tempDir = YtResults.TempDir();
        VideoData[] videos;
        if (mode == TranscribeMode.Query) {
          videos = await db.QueryAsync<VideoData>("video media_url", $@"
with vids as ({(queryName == null ? "select * from video_extra" : TranscribeSql.Sql[queryName])})
select q.video_id, e.source_id, e.media_url, e.channel_id, e.platform
from vids q
join video_extra e on e.video_id = q.video_id 
where media_url is not null {platform.Do(p => $"and platform = {p.EnumString().SingleQuote()}")}
and not exists (select * from caption s where s.video_id = q.video_id)
order by views desc nulls last
{limit.Do(l => $"limit {l}")}
").BlockMap(async v => v with {media_path = await CopyVideo(log, v, tempDir, cancel)}, parallel: 8, cancel: cancel).ToArrayAsync();
        }
        else {
          var mediaLoadPath = $"media-load/{ShortGuid.Create(6)}";
          var mediaLoadFiles = await StoreMedia.List("", allDirectories: true)
            .SelectMany()
            .Select(f => new {
              Platform = f.Path.Tokens.First().ParseEnum<Platform>(),
              SourceId = f.Path.NameSansExtension,
              MediaPath = f.Path
            })
            .Where(v => sourceIds == null || sourceIds.Contains(v.SourceId))
            .Batch(10000)
            .BlockMap(async (b, i) => {
              StringPath path = $"{mediaLoadPath}/{i}.jsonl.gz";
              await StoreForLoad.Save(path, await b.ToJsonlGzStream(IJsonlStore.JCfg), log);
              return path;
            }).ToArrayAsync();
          videos = await db.QueryAsync<VideoData>("media-loaded-sans-caption", $@"
with media as (
  with raw as (select $1::object v from @yt_data/{StoreForLoad.BasePathSansContainer()}/{mediaLoadPath}/)
  select v:Platform::string platform, v:SourceId::string source_id, v:MediaPath::string media_path from raw
)
select e.video_id, e.source_id, e.media_url, e.channel_id, e.platform, q.media_path
from media q
join video_extra e on e.source_id = q.source_id and e.platform = q.platform 
where not exists (select * from caption s where s.video_id = e.video_id)
order by e.views desc nulls last
").ToArrayAsync();
          await mediaLoadFiles.BlockDo(f => StoreForLoad.Delete(f), parallel: 8);
        }

        await videos
          .BlockMap(async v => {
            var detectLanguage = true;
            TranscriptionJob job;
            while (true) {
              var startTrans = await GetOrStartTrans(v.media_path, detectLanguage, log);
              if (startTrans == default) return default;
              job = await WaitForCompletedTrans(log, startTrans);
              if (job.TranscriptionJobStatus == FAILED && job.FailureReason.StartsWith("Your audio file must have a speech segment long enough") &&
                detectLanguage) {
                detectLanguage = false;
                continue;
              }
              break;
            }
            var transPath = job.Transcript.TranscriptFileUri?.AsUrl().PathSegments.Skip(2).Do(s => new StringPath(s));
            var trans = job.TranscriptionJobStatus == COMPLETED ? await LoadTrans(transPath, log) : default;
            var vidCaption = trans?.AwsToVideoCaption(job, v.video_id, v.channel_id, v.platform);
            return vidCaption;
          }, parallel: 20)
          .NotNull()
          .BlockDo(async caption => {
            await StoreDb.Captions.Append(caption, log);
            // transcribing is expensive and slow, save each one rather than batching
            log.Information("Transcribe - saved caption for video: {Videos}", caption.VideoId);
          });
      }

      if (parts.ShouldRun(PStage)) await Stage.StageUpdate(log, tableNames: new[] {"caption_stage"});
      // TODO use dataform API to quickly also update the caption table

      log.Information("Transcribe - completed transcribing");
    }

    async Task<StringPath> CopyVideo(ILogger log, VideoData v, FPath tempDir, CancellationToken cancel) {
      var mediaUrl = v.media_url.AsUrl();
      var ext = mediaUrl.PathSegments.LastOrDefault()?.Split(".").LastOrDefault() ?? throw new("not implemented. Currently relying on extension in url");
      var blobPath = BlobPath(v.platform, v.source_id, ext);
      var existing = await StoreMedia.Info(blobPath);
      if (existing != null) {
        log.Debug("Transcribe - using existing media {MediaUrl}", StoreMedia.S3Uri(blobPath));
        return blobPath;
      }
      log.Debug("Transcribe - loading media from {Url} to {MediaUrl}", mediaUrl, StoreMedia.S3Uri(blobPath));

      var localFile = tempDir.Combine(blobPath.Tokens.ToArray());
      localFile.EnsureDirectoryExists();

      try {
        // ReSharper disable once ConvertToUsingDeclaration - need to ensure this is closed & flushed before copying to s3
        using (var res = await v.media_url.WithTimeout(30.Minutes()).SendWithRetry("get media", log: log, completionOption: ResponseHeadersRead))
        using (var rs = await res.GetStreamAsync())
        using (var ws = localFile.Open(FileMode.Create)) {
          var totalBytes = res.Headers.TryGetFirst("Content-Length", out var l) ? l.TryParseInt() : null;
          await rs.CopyToAsync(ws, b => log.Debug("Transcribe - loading {Url} - {Transferred}/{Total}",
              mediaUrl, b.Bytes().Humanize("#.#"), totalBytes?.Bytes().Humanize("#.#") ?? "unknown bytes"),
            cancel, 100.Kilobytes(), 10.Seconds());
        }
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

    async Task<TranscriptionJob> GetOrStartTrans(StringPath p, bool identifyLanguage, ILogger log) {
      var transPath = p.WithExtension(".json");
      var mediaUrl = StoreMedia.S3Uri(p).ToString();
      var existingJobs = await TransClient.ListTranscriptionJobsAsync(new() {JobNameContains = p.NameSansExtension, MaxResults = 100});
      var existingJob = await existingJobs.TranscriptionJobSummaries
        .BlockMap(j => TransClient.GetTranscriptionJobAsync(new() {TranscriptionJobName = j.TranscriptionJobName}))
        .Where(j => j.TranscriptionJob.Media.MediaFileUri == mediaUrl)
        .OrderBy(j => j.TranscriptionJob.TranscriptionJobStatus.Value switch {
          nameof(COMPLETED) => 0,
          nameof(IN_PROGRESS) => 1,
          nameof(QUEUED) => 2,
          _ => 3
        })
        .FirstOrDefaultAsync().Then(j => j?.TranscriptionJob);

      if (existingJob != null && existingJob.IdentifyLanguage == identifyLanguage) {
        log.Debug("Transcribe - using existing job {JobName}", existingJob.TranscriptionJobName);
        return existingJob;
      }

      StartTranscriptionJobRequest req = new() {
        IdentifyLanguage = identifyLanguage,
        LanguageCode = identifyLanguage ? null : LanguageCode.EnUS,
        MediaFormat = p.ExtensionsString,
        Media = new() {
          MediaFileUri = mediaUrl
        },
        TranscriptionJobName = $"{SafeName(p.NameSansExtension)}-{ShortGuid.Create(6)}",
        OutputBucketName = StoreTrans.Cfg.Bucket,
        OutputKey = StoreTrans.BasePath.Add(transPath),
        Settings = new() {
          ShowSpeakerLabels = true,
          MaxSpeakerLabels = 4
        }
      };

      var retry = Policy.Handle<LimitExceededException>().RetryBackoff("start job", retryCount: 5, log);
      var (res, ex) = await retry.ExecuteAsync(() => TransClient.StartTranscriptionJobAsync(req)).Try();
      if (ex != null) {
        log.Error(ex, "Transcribe unable to start transcription - {@Job}: {Error}", req, ex.Message);
        return default;
      }
      log.Debug("Transcribe - started transcription {MediaUrl}", res.TranscriptionJob?.Media?.MediaFileUri);
      return res.TranscriptionJob;
    }

    //static readonly JsonSerializerSettings JSettings = TransRoot.JsonSettings();
    async Task<TransRoot> LoadTrans(StringPath path, ILogger log) => await StoreTrans.Load(path, log).Then(s => s.ToObject<TransRoot>());

    async Task<TranscriptionJob> WaitForCompletedTrans(ILogger log, TranscriptionJob startJob) {
      var lastLog = DateTime.UtcNow;
      while (true) {
        var job = await TransClient.GetTranscriptionJobAsync(new() {TranscriptionJobName = startJob.TranscriptionJobName});
        var tj = job.TranscriptionJob;
        if (tj.TranscriptionJobStatus == IN_PROGRESS && lastLog.OlderThan(1.Minutes())) {
          log.Debug("Transcribe - waiting on transcription job '{Job}' to complete (Age {Duration})",
            tj.TranscriptionJobName, (DateTime.UtcNow - tj.StartTime).HumanizeShort());
          lastLog = DateTime.UtcNow;
        }
        if (tj.TranscriptionJobStatus.Value.In(COMPLETED.Value, FAILED.Value) || tj.StartTime.OlderThan(2.Hours())) {
          if (tj.TranscriptionJobStatus == FAILED) log.Warning("Transcribe - failed: {@Job}", tj);
          else log.Information("Transcribe - {Url} - {Status}", tj.Transcript.TranscriptFileUri, tj.TranscriptionJobStatus);
          return tj;
        }
        await 10.Seconds().Delay();
      }
    }

    record VideoData {
      public string   video_id   { get; init; }
      public string   source_id  { get; init; }
      public string   media_url  { get; init; }
      public string   channel_id { get; init; }
      public Platform platform   { get; init; }
      public string   media_path { get; init; }
    }
  }
}