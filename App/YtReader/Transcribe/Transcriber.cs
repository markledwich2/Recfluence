using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Amazon;
using Amazon.Runtime;
using Amazon.TranscribeService;
using Amazon.TranscribeService.Model;
using Flurl.Http;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Polly;
using Polly.Retry;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using YtReader.Db;
using YtReader.Store;
using YtReader.Web;
using YtReader.Yt;
using static System.Net.Http.HttpCompletionOption;
using static Amazon.TranscribeService.TranscriptionJobStatus;
using static YtReader.Transcribe.TranscribeParts;

// ReSharper disable InconsistentNaming

namespace YtReader.Transcribe; 

public enum TranscribeParts {
  [EnumMember(Value = "transcribe")] PTranscribe,
  [EnumMember(Value = "stage")]      PStage
}

public enum TranscribeMode {
  Query,
  Media
}

public record TranscribeCfg {
  public int ParallelTranscribe { get; init; } = 80; // service limit is 100, so leave some room
  public int Parallel           { get; init; } = 8;
}

public static class AwsCfgExtensions {
  public static BasicAWSCredentials Creds(this AwsCfg cfg) => new(cfg.Creds.Name, cfg.Creds.Secret);
  public static RegionEndpoint Region(this AwsCfg cfg) => RegionEndpoint.GetBySystemName(cfg.Region);
}

public record TranscribeOptions(Platform? Platform = null, int? Limit = null, string QueryName = null, TranscribeParts[] Parts = null,
  TranscribeMode Mode = default, string[] SourceIds = null);

public record Transcriber(TranscribeCfg Cfg, BlobStores Stores, SnowflakeConnectionProvider Sf, AwsCfg Aws, YtStore StoreDb, Stage Stage) {
  static readonly Regex                         SafeNameRe   = new("[^\\w0-9]", RegexOptions.Compiled);
  readonly        ISimpleFileStore              StoreForLoad = Stores.Store("import/temp");
  readonly        S3Store                       StoreMedia   = new(Aws.S3, "media");
  readonly        S3Store                       StoreTrans   = new(Aws.S3, "transcripts");
  readonly        AmazonTranscribeServiceClient TransClient  = new(Aws.Creds(), Aws.Region());

  string SafeName(string name) => SafeNameRe.Replace(name, "");

  SPath BlobPath(Platform? platform, string sourceId, string extension) => SPath.Relative(platform.EnumString(), $"{SafeName(sourceId)}.{extension}");

  public async Task Transcribe(TranscribeOptions options, ILogger log, CancellationToken cancel = default) {
    log = log.ForContext("Function", nameof(Transcribe));
    if (options.Parts.ShouldRun(PTranscribe)) await TranscribeVideos(LoadMedia(options, log, cancel), log);
    if (options.Parts.ShouldRun(PStage)) await Stage.StageUpdate(log, tableNames: new[] {"caption_stage"});
    log.Information("Transcribe - completed transcribing");
  }

  public async Task TranscribeVideos(IAsyncEnumerable<VideoToTranscribe> videos, ILogger log, CancellationToken cancel = default) {
    var jobs = await ExistingJobs();
    var errors = 0;
    var tempDir = YtResults.TempDir();
    await videos.BlockMap(async v => {
        // the media might be downloaded or not. Load it as required
        if (v.media_path == null) return v with {media_path = await CopyMedia(log, v, tempDir, cancel)};
        return v;
      })
      .BlockMap(async v => {
          var (res, ex) = await Transcribe(v, jobs, log).Try();
          if (ex == null) return res;
          log.Warning(ex, "Transcribe - unhandled error transcribing: {Error}", ex.Message);
          if (Interlocked.Increment(ref errors) > 20) throw new("Transcribe - too many errors, probably a bug");
          return res;
        },
        Cfg.ParallelTranscribe)
      .NotNull()
      .BlockDo(async (caption, i) => {
        await StoreDb.Captions.Append(caption, log);
        // transcribing is expensive and slow, save each one immediately rather than batching
        log.Information("Transcribe - saved caption for video {Video} ({Num})", caption.VideoId, i);
      });
  }

  public async Task<TranscriptionJobSummary[]> ExistingJobs() => await TransClient.Jobs().SelectMany().ToArrayAsync();

  async Task<VideoCaption> Transcribe(VideoToTranscribe v, TranscriptionJobSummary[] existingJobs, ILogger log) {
    var detectLanguage = true;
    TransRoot transResult = null;
    var transPath = v.media_path.WithExtension("json");
    if (await StoreTrans.Exists(transPath)) {
      transResult = await LoadTrans(transPath, log);
      log.Debug("Transcribe - loaded existing result {TransUrl}", StoreTrans.S3Uri(transPath));
    }
    else {
      TranscriptionJob job;
      while (true) {
        var startTrans = await GetOrStartTrans(existingJobs, v.media_path, transPath, detectLanguage, log);
        if (startTrans == default) return default;
        job = await WaitForCompletedTrans(log, startTrans);
        if (job.TranscriptionJobStatus == FAILED && job.FailureReason.StartsWith("Your audio file must have a speech segment long") && detectLanguage) {
          detectLanguage = false;
          continue;
        }
        break;
      }
      if (job.TranscriptionJobStatus == COMPLETED)
        transResult = await LoadTrans(transPath, log)
          .Swallow(e => log.Warning(e, "Transcribe - can't load completed transcription file {TransUrl}. Error: {Error}", StoreTrans.S3Uri(transPath),
            e.Message));
    }
    var vidCaption = transResult?.AwsToVideoCaption(v.video_id, v.channel_id, v.platform);
    return vidCaption;
  }

  async IAsyncEnumerable<VideoToTranscribe> LoadMedia(TranscribeOptions options, ILogger log, [EnumeratorCancellation] CancellationToken cancel) {
    using var db = await Sf.Open(log);
    if (options.Mode == TranscribeMode.Query) {
      var tempDir = YtResults.TempDir();
      await foreach (var v in db.QueryAsync<VideoToTranscribe>("video media_url", $@"
with vids as ({(options.QueryName == null ? "select * from video_extra" : TranscribeSql.Sql[options.QueryName])})
select q.video_id, e.source_id, e.media_url, e.channel_id, e.platform
from vids q
join video_extra e on e.video_id = q.video_id 
where e.media_url is not null {options.Platform.Do(p => $"and platform = {p.EnumString().SingleQuote()}")}
and not exists (select * from caption s where s.video_id = q.video_id)
order by views desc nulls last
{options.Limit.Do(l => $"limit {l}")}
").BlockMap(async v => v with {media_path = await CopyMedia(log, v, tempDir, cancel)}, Cfg.Parallel, cancel: cancel))
        yield return v;
    }
    else {
      // load allrady downloaded media that doesn't exist in the warehouse
      var mediaLoadPath = $"media-load/{ShortGuid.Create(6)}";
      var mediaLoadFiles = await StoreMedia.List("", allDirectories: true)
        .SelectMany()
        .Select(f => new {
          Platform = f.Path.Tokens.First().ParseEnum<Platform>(),
          SourceId = f.Path.NameSansExtension,
          MediaPath = f.Path
        })
        .Where(v => options.SourceIds == null || options.SourceIds.Contains(v.SourceId))
        .Batch(10000)
        .BlockMap(async (b, i) => {
          SPath path = $"{mediaLoadPath}/{i}.jsonl.gz";
          await StoreForLoad.Save(path, await b.ToJsonlGzStream(IJsonlStore.JCfg), log);
          return path;
        }).ToArrayAsync(cancellationToken: cancel);
      await foreach (var v in db.QueryAsync<VideoToTranscribe>("media-loaded-sans-caption", $@"
with media as (
  with raw as (select $1::object v from @yt_data/{StoreForLoad.BasePathSansContainer()}/{mediaLoadPath}/)
  select v:Platform::string platform, v:SourceId::string source_id, v:MediaPath::string media_path from raw
)
select e.video_id, e.source_id, e.media_url, e.channel_id, e.platform, q.media_path
from media q
join video_extra e on e.source_id = q.source_id and e.platform = q.platform 
where not exists (select * from caption s where s.video_id = e.video_id)
order by e.views desc nulls last
{options.Limit.Do(l => $"limit {l}")}
").WithCancellation(cancel))
        yield return v;
      await mediaLoadFiles.BlockDo(f => StoreForLoad.Delete(f), Cfg.Parallel, cancel: cancel); // delete files used for load
    }
  }

  async Task<SPath> CopyMedia(ILogger log, VideoToTranscribe v, FPath tempDir, CancellationToken cancel) {
    var sourceMediaUrl = v.media_url.AsUrl();
    var ext = sourceMediaUrl.PathSegments.LastOrDefault()?.Split(".").LastOrDefault() ?? throw new("not implemented. Currently relying on extension in url");
    var blobPath = BlobPath(v.platform, v.source_id, ext);
    var existing = await StoreMedia.Info(blobPath);
    if (existing != null) {
      log.Debug("Transcribe - using existing media {MediaUrl}", StoreMedia.S3Uri(blobPath));
      return blobPath;
    }
    log.Debug("Transcribe - loading media from {Url} to {MediaUrl}", sourceMediaUrl, StoreMedia.S3Uri(blobPath));

    var localFile = tempDir.Combine(blobPath.Tokens.ToArray());
    localFile.EnsureDirectoryExists();

    try {
      // ReSharper disable once ConvertToUsingDeclaration - need to ensure this is closed & flushed before copying to s3
      using (var res = await v.media_url.WithTimeout(30.Minutes()).SendWithRetry("get media", log: log, completionOption: ResponseHeadersRead))
      using (var rs = await res.GetStreamAsync())
      using (var ws = localFile.Open(FileMode.Create)) {
        var totalBytes = res.Headers.TryGetFirst("Content-Length", out var l) ? l.TryParseInt() : null;
        await rs.CopyToAsync(ws, b => log.Debug("Transcribe - loading {Url} - {Transferred}/{Total}",
            sourceMediaUrl, b.Bytes().Humanize("#.#"), totalBytes?.Bytes().Humanize("#.#") ?? "unknown bytes"),
          cancel, 100.Kilobytes(), 10.Seconds());
      }
    }
    catch (Exception ex) {
      log.Error(ex, "Transcribe - failed loading {MediaUrl}", sourceMediaUrl);
      throw;
    }

    var mediaUrl = StoreMedia.S3Uri(blobPath).ToString();
    var dur = await StoreMedia.Save(blobPath, localFile, log).WithDuration();
    log.Debug("Transcribe - saved media {MediaUrl} in {Dur}", mediaUrl, dur.HumanizeShort());
    localFile.Delete();
    return blobPath;
  }

  /// <summary>Policy for using the AWS transcription service. It has its own retry, but still gives by rate exceeded errors</summary>
  static AsyncRetryPolicy AwsRetry(ILogger log) =>
    Policy
      .Handle<AmazonTranscribeServiceException>(e => e.Message == "Rate exceeded" || e.Retryable != null)
      .RetryBackoff("start job", retryCount: 4, 10.Seconds(), log);

  async Task<TranscriptionJob> GetOrStartTrans(TranscriptionJobSummary[] existingJobs, SPath mediaPath, SPath transPath, bool identifyLanguage,
    ILogger log) {
    var mediaUrl = StoreMedia.S3Uri(mediaPath).ToString();

    var existingJob = await existingJobs
      .Where(j => j.TranscriptionJobName.StartsWith(transPath.NameSansExtension))
      .BlockMap(j => AwsRetry(log).ExecuteAsync(() => TransClient.GetTranscriptionJobAsync(new() {TranscriptionJobName = j.TranscriptionJobName})))
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
      MediaFormat = mediaPath.ExtensionsString,
      Media = new() {
        MediaFileUri = mediaUrl
      },
      TranscriptionJobName = $"{SafeName(transPath.NameSansExtension)}-{ShortGuid.Create(6)}",
      OutputBucketName = StoreTrans.Cfg.Bucket,
      OutputKey = StoreTrans.BasePath.Add(transPath),
      Settings = new() {
        ShowSpeakerLabels = true,
        MaxSpeakerLabels = 4
      }
    };


    var (res, ex) = await AwsRetry(log).ExecuteAsync(() => TransClient.StartTranscriptionJobAsync(req)).Try();
    if (ex != null) {
      log.Warning(ex, "Transcribe unable to start transcription - {@Job}: {Error}", req, ex.Message);
      return default;
    }
    log.Debug("Transcribe - started transcription {MediaUrl}", res.TranscriptionJob?.Media?.MediaFileUri);
    return res.TranscriptionJob;
  }

  async Task<TransRoot> LoadTrans(SPath path, ILogger log) => await StoreTrans.Load(path, log).Then(s => s.ToObject<TransRoot>());

  async Task<TranscriptionJob> WaitForCompletedTrans(ILogger log, TranscriptionJob startJob) {
    var lastLog = DateTime.UtcNow;
    while (true) {
      var job = await AwsRetry(log).ExecuteAsync(() => TransClient.GetTranscriptionJobAsync(new() {TranscriptionJobName = startJob.TranscriptionJobName}));
      var tj = job.TranscriptionJob;
      if (tj.TranscriptionJobStatus == IN_PROGRESS && lastLog.OlderThan(2.Minutes())) {
        log.Debug("Transcribe - waiting on transcription job '{Job}' to complete (Age {Duration})",
          tj.TranscriptionJobName, (DateTime.UtcNow - tj.StartTime).HumanizeShort());
        lastLog = DateTime.UtcNow;
      }
      if (tj.TranscriptionJobStatus.Value.In(COMPLETED.Value, FAILED.Value) || tj.StartTime.OlderThan(2.Hours())) {
        if (tj.TranscriptionJobStatus == FAILED) log.Warning("Transcribe - failed: {@Job}", tj);
        else log.Debug("Transcribe - {Url} - {Status}", tj.Transcript.TranscriptFileUri, tj.TranscriptionJobStatus);
        return tj;
      }
      await 10.Seconds().Delay();
    }
  }
}

public record VideoToTranscribe {
  public string   video_id   { get; init; }
  public string   source_id  { get; init; }
  public string   media_url  { get; init; }
  public string   channel_id { get; init; }
  public Platform platform   { get; init; }
  public SPath    media_path { get; init; }
}