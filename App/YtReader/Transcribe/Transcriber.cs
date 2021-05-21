using System.IO;
using System.Linq;
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

// ReSharper disable InconsistentNaming

namespace YtReader.Transcribe {
  public record Transcriber(BlobStores Stores, SnowflakeConnectionProvider Sf, AwsCfg Aws, YtStore StoreDb, Stage Stage) {
    readonly AmazonTranscribeServiceClient TransClient = new(Aws.CredsBasic, Aws.RegionEndpoint);
    readonly S3Store                       StoreMedia  = new(Aws.S3, "media");
    readonly S3Store                       StoreTrans  = new(Aws.S3, "transcripts");

    /*
    MediaFormat Extension(Platform? platform) => platform switch {
      Platform.Rumble => "mp4",
      _ => throw new($"Platform {platform} not supported for media transcription")
    };
    */

    static readonly Regex SafeNameRe = new("[^\\w0-9]", RegexOptions.Compiled);
    string SafeName(string name) => SafeNameRe.Replace(name, "");

    StringPath BlobPath(Platform? platform, string sourceId, string extension) =>
      //var extension = Extension(platform);
      StringPath.Relative(
        platform.EnumString(), $"{SafeName(sourceId)}.{extension}");

    record VideoData(string video_id, string source_id, string media_url, string channel_id, Platform platform);

    public async Task TranscribeVideos(ILogger log, CancellationToken cancel = default, Platform? platform = null, int? limit = null) {
      log = log.ForContext("Function", nameof(TranscribeVideos));
      using var db = await Sf.Open(log);
      var tempDir = YtResults.TempDir();
      var videos = await db.QueryAsync<VideoData>("video media_url", $@"
select video_id, source_id, media_url, channel_id, platform
from video_extra
where media_url is not null {platform.Do(p => $"and platform = {p.EnumString().SingleQuote()}")}
and source_id not in ('vgiqjx') -- TODO remove. Taking too long. Might need to use lower quality video to reduce download size.
order by views desc
{limit.Do(l => $"limit {l}")}
").ToListAsync();

      await videos
        .BlockMap(async v => (mediaPath: await CopyVideo(log, v, platform, tempDir, cancel), v)
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

      await Stage.StageUpdate(log, tableNames: new[] {"caption_stage"});

      // todo save standard VideoCaptions from media storage into standard caption location
      log.Information("Transcribe - completed transcribing");
    }

    async Task<StringPath> CopyVideo(ILogger log, VideoData v, Platform? platform, FPath tempDir, CancellationToken cancel) {
      var mediaUrl = v.media_url.AsUrl();
      var ext = mediaUrl.PathSegments.LastOrDefault()?.Split(".").LastOrDefault() ?? throw new("not implemented. Currently relying on extension in url");
      var blobPath = BlobPath(platform, v.source_id, ext);
      var existing = await StoreMedia.Info(blobPath);
      if (existing != null) {
        log.Debug("Transcribe - {BlobPath} exists, ignoring", blobPath);
        return blobPath;
      }
      log.Debug("Transcribe - loading media from {Url} to {BlobPath}", mediaUrl, blobPath);

      var localFile = tempDir.Combine(blobPath.Tokens.ToArray());
      localFile.EnsureDirectoryExists();

      using (var res = await v.media_url.WithTimeout(30.Minutes()).SendWithRetry("get media", log: log, completionOption: ResponseHeadersRead))
      using (var rs = await res.GetStreamAsync()) {
        using var ws = localFile.Open(FileMode.Create);
        await rs.CopyToAsync(ws, cancel);
      }

      await StoreMedia.Save(blobPath, localFile, log);
      log.Debug("Transcribe - saved media {BlobPath}", blobPath);
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
      return (transPath, res.TranscriptionJob, null);
    }

    //static readonly JsonSerializerSettings JSettings = TransRoot.JsonSettings();
    async Task<TransRoot> LoadTrans(StringPath path, ILogger log) => await StoreTrans.Load(path, log).Then(s => s.ToObject<TransRoot>());

    async Task<TranscriptionJob> WaitForCompletedTrans(ILogger log, TranscriptionJob startJob) {
      if (startJob == null) return null;
      while (true) {
        var job = await TransClient.GetTranscriptionJobAsync(new() {TranscriptionJobName = startJob.TranscriptionJobName});
        var tj = job.TranscriptionJob;
        if (tj.TranscriptionJobStatus.Value.In(COMPLETED.Value, FAILED.Value) || tj.StartTime.OlderThan(30.Minutes())) {
          if (tj.TranscriptionJobStatus == FAILED) log.Warning("Transcribe - failed: {@Job}", tj);
          else log.Information("Transcribe - {Url} - {Status}", tj.Transcript.TranscriptFileUri, tj.TranscriptionJobStatus);
          return tj;
        }
        await 10.Seconds().Delay();
      }
    }
  }
}