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
using YtReader.Yt;
using static Amazon.TranscribeService.TranscriptionJobStatus;
// ReSharper disable InconsistentNaming

namespace YtReader.Transcribe {
  public record Transcriber(BlobStores Stores, SnowflakeConnectionProvider Sf, AwsCfg Aws, YtStore StoreDb, Stage Stage) {
    
    readonly AmazonTranscribeServiceClient TransClient = new(Aws.CredsBasic, Aws.RegionEndpoint);
    readonly S3Store                       StoreMedia  = new (Aws.S3, "media");
    readonly S3Store                       StoreTrans  = new (Aws.S3, "transcripts");
    
    MediaFormat Extension(Platform? platform) => platform switch {
      Platform.Rumble => "mp4",
      _ => throw new($"Platform {platform} not supported for media transcription")
    };
    
    static readonly Regex SafeNameRe = new("[^\\w0-9]", RegexOptions.Compiled);
    string SafeName(string name) => SafeNameRe.Replace(name, "");

    StringPath BlobPath(Platform? platform, string sourceId) {
      var extension = Extension(platform);
      return StringPath.Relative(
        platform.EnumString(), $"{SafeName(sourceId)}.{extension}");
    }

    record VideoData(string video_id, string source_id, string media_url, string channel_id);
    
    public async Task TranscribeVideos(ILogger log, CancellationToken cancel = default, Platform? platform = null, int? limit = null) {
      using var db = await Sf.Open(log);
      var tempDir = YtResults.TempDir();
      var videos = await db.QueryAsync<VideoData>("video media_url", $@"
select video_id, source_id, media_url, channel_id
from video_extra
where media_url is not null {platform.Do(p => $"and platform = {p.EnumString().SingleQuote()}")}
order by views desc
{limit.Do(l => $"limit {l}")}
").ToListAsync();

      await videos
        .BlockMap(async v => (mediaPath:await CopyVideo(log, v, platform, tempDir), v)
          , parallel: 1, cancel:cancel)
        .BlockMap(async p => {
          var (mediaPath, v) = p;
          var (transPath, job) = await StartTrans(platform, mediaPath);
          return (transPath, job, v);
        })
        .BlockMap(async r => {
          var (transPath, startJob, v) = r;
          var job = await WaitForCompletedTrans(log, startJob);
          return (transPath, job, v);
        }, parallel: 1, cancel:cancel)
        .BlockMap(r => ToVideoCaption(log, r.transPath, r.job, r.v.video_id, r.v.channel_id))
        .Batch(10)
        .BlockDo(c => StoreDb.Captions.Append(c, log));

      await Stage.StageUpdate(log, tableNames: new[] {"caption_stage"});

      // todo save standard VideoCaptions from media storage into standard caption location
      log.Information("Transcribe - completed transcribing");
    }

    async Task<StringPath> CopyVideo(ILogger log, VideoData v, Platform? platform, FPath tempDir) {
      var blobPath = BlobPath(platform, v.source_id);
      var existing = await StoreMedia.Info(blobPath);
      if (existing != null) {
        log.Debug("Transcribe - {File} exists, ignoring", blobPath);
        return blobPath;
      }
      log.Debug("Transcribe - loading media {File}", blobPath);

      var localFile = tempDir.Combine(blobPath.Tokens.ToArray());
      localFile.EnsureDirectoryExists();

      using (var rs = await v.media_url.GetStreamAsync()) {
        using var ws = localFile.Open(FileMode.Create);
        await rs.CopyToAsync(ws);
      }

      await StoreMedia.Save(blobPath, localFile, log);
      log.Debug("Transcribe - saved media {File}", blobPath);
      localFile.Delete();
      return blobPath;
    }

    async Task<(StringPath path, StartTranscriptionJobResponse job)> StartTrans(Platform? platform, StringPath p) {
      var transPath = p.WithExtension(".json");
      if (await StoreTrans.Exists(transPath)) return (transPath, null);
      var res = await TransClient.StartTranscriptionJobAsync(new () {
        LanguageCode = LanguageCode.EnUS,
        MediaFormat = Extension(platform),
        Media = new() {
          MediaFileUri = StoreMedia.S3Uri(p).ToString()
        },
        TranscriptionJobName = $"{SafeName(p.NameSansExtension)}-{ShortGuid.Create(4)}" ,
        OutputBucketName = StoreTrans.Cfg.Bucket,
        OutputKey = StoreTrans.BasePath.Add(transPath),
        Settings = new() {
          ShowSpeakerLabels = true,
          MaxSpeakerLabels = 4
        }
      });
      return (transPath, res);
    }

    async Task<TranscriptionJob> WaitForCompletedTrans(ILogger log, StartTranscriptionJobResponse startJob) {
      if (startJob == null) return (null);
      while (true) {
        var job = await TransClient.GetTranscriptionJobAsync(new() {TranscriptionJobName = startJob.TranscriptionJob.TranscriptionJobName});
        var tj = job.TranscriptionJob;
        if (tj.TranscriptionJobStatus.Value.In(COMPLETED.Value, FAILED.Value) || tj.StartTime.OlderThan(30.Minutes())) {
          if(tj.TranscriptionJobStatus == FAILED) log.Warning("Transcribe - failed: {@Job}", tj);
          else log.Information("Transcribe - {Url} - {Status}", tj.Transcript.TranscriptFileUri, tj.TranscriptionJobStatus);
          return tj;
        }
        await 10.Seconds().Delay();
      }
    }


    public async Task<VideoCaption> ToVideoCaption(ILogger log, StringPath path, TranscriptionJob job, string videoId, string channelId) {
      var t = await StoreTrans.Load(path, log).Then(s => s.ToObject<TransJob>());
      var cap = t.AwsToVideoCaption(job, videoId, channelId);
      return cap;
    }
  }

  
}