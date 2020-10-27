using System;
using System.Collections.Generic;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Mutuo.Etl.Blob;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.YtApi;
using YtReader.YtWebsite;

namespace YtReader.Store {
  public enum DataStoreType {
    Pipe,
    Db,
    Results,
    Private,
    Backup,
    Logs,
    Root
  }

  /// <summary>Access to any of the stores</summary>
  public class YtStores {
    readonly StorageCfg Cfg;
    readonly SemVersion Version;
    readonly ILogger    Log;

    public YtStores(StorageCfg cfg, SemVersion version, ILogger log) {
      Cfg = cfg;
      Version = version;
      Log = log;
    }

    public AzureBlobFileStore Store(DataStoreType type) => type switch {
      DataStoreType.Backup => Version.Prerelease.HasValue() ? null : new AzureBlobFileStore(Cfg.BackupCs, Cfg.BackupRootPath, Log),
      _ => new AzureBlobFileStore(Cfg.DataStorageCs, StoragePath(type), Log)
    };

    public StringPath StoragePath(DataStoreType type) {
      var root = Cfg.RootPath(Version);
      if (type == DataStoreType.Root) return root;
      return root + "/" + type switch {
        DataStoreType.Pipe => Cfg.PipePath,
        DataStoreType.Db => Cfg.DbPath,
        DataStoreType.Private => Cfg.PrivatePath,
        DataStoreType.Results => Cfg.ResultsPath,
        DataStoreType.Logs => Cfg.LogsPath,
        _ => throw new NotImplementedException($"StoryType {type} not supported")
      };
    }
  }

  public static class StoreEx {
    public static CloudBlobContainer Container(this StorageCfg cfg, SemVersion version) {
      var storage = CloudStorageAccount.Parse(cfg.DataStorageCs);
      var client = new CloudBlobClient(storage.BlobEndpoint, storage.Credentials);
      var container = client.GetContainerReference(cfg.RootPath(version.Prerelease));
      return container;
    }

    public static string RootPath(this StorageCfg cfg, SemVersion version) => cfg.RootPath(version.Prerelease);
    public static string RootPath(this StorageCfg cfg, string prefix) => prefix.HasValue() ? $"{cfg.Container}-{prefix}" : cfg.Container;
  }

  /// <summary>Typed access to jsonl blob collections</summary>
  public class YtStore {
    public static readonly int     StoreVersion = 1;
    readonly               ILogger Log;

    public YtStore(ISimpleFileStore store, ILogger log) {
      Store = store;
      Log = log;
      Channels = CreateStore<ChannelStored2>("channels");
      Searches = CreateStore<UserSearchWithUpdated>("searches");
      Videos = CreateStore<VideoStored2>("videos", v => v.ChannelId);
      VideoExtra = CreateStore<VideoExtraStored2>("video_extra");
      Recs = CreateStore<RecStored2>("recs", r => r.FromChannelId);
      Captions = CreateStore<VideoCaptionStored2>("captions", c => c.ChannelId);
      ChannelReviews = CreateStore<UserChannelReview>("channel_reviews", r => r.Email);
    }

    public ISimpleFileStore Store { get; }

    public JsonlStore<ChannelStored2>        Channels   { get; }
    public JsonlStore<UserSearchWithUpdated> Searches   { get; }
    public JsonlStore<VideoStored2>          Videos     { get; }
    public JsonlStore<VideoExtraStored2>     VideoExtra { get; }
    public JsonlStore<RecStored2>            Recs       { get; }
    public JsonlStore<VideoCaptionStored2>   Captions   { get; }

    public JsonlStore<UserChannelReview> ChannelReviews { get; }

    public IJsonlStore[] AllStores => new IJsonlStore[] {Channels, Searches, Videos, VideoExtra, Recs, Captions};

    JsonlStore<T> CreateStore<T>(string name, Func<T, string> getPartition = null) where T : IHasUpdated =>
      new JsonlStore<T>(Store, name, c => c.Updated.FileSafeTimestamp(), Log, StoreVersion.ToString(), getPartition);
  }

  public enum ChannelStatus {
    None,
    Alive,
    Dead
  }

  public enum ChannelReviewStatus {
    None,
    Pending,
    ManualAccepted,
    ManualRejected,
    AlgoAccepted,
    AlgoRejected
  }

  public static class ChannelReviewStatusEx {
    public static bool Accepted(this ChannelReviewStatus s) => s.In(ChannelReviewStatus.ManualAccepted, ChannelReviewStatus.AlgoAccepted);
  }

  public class ChannelStored2 : WithUpdatedItem {
    public string                ChannelId          { get; set; }
    public string                ChannelTitle       { get; set; }
    public string                MainChannelId      { get; set; }
    public string                Description        { get; set; }
    public string                LogoUrl            { get; set; }
    public double?               Relevance          { get; set; }
    public string                LR                 { get; set; }
    public ulong?                Subs               { get; set; }
    public ulong?                ChannelViews       { get; set; }
    public string                Country            { get; set; }
    public string[]              FeaturedChannelIds { get; set; }
    public string                DefaultLanguage    { get; set; }
    public string                Keywords           { get; set; }
    public ChannelSubscription[] Subscriptions      { get; set; }

    public ChannelStatus Status { get; set; }

    public IReadOnlyCollection<string>            HardTags     { get; set; }
    public IReadOnlyCollection<string>            SoftTags     { get; set; }
    public IReadOnlyCollection<UserChannelReview> UserChannels { get; set; }

    public string   StatusMessage  { get; set; }
    public DateTime LastFullUpdate { get; set; }
    public override string ToString() => ChannelTitle ?? ChannelId;
  }

  public class UserChannelReviewCommon : IHasUpdated {
    public string                      LR                  { get; set; }
    public int                         Relevance           { get; set; }
    public IReadOnlyCollection<string> SoftTags            { get; set; } = new List<string>();
    public string                      Notes               { get; set; }
    public string                      PublicReviewerNotes { get; set; }
    public string                      PublicCreatorNotes  { get; set; }
    public DateTime                    Updated             { get; set; }
    public string                      MainChannelId       { get; set; }
  }

  public class UserChannelReview : UserChannelReviewCommon {
    public string ChannelId { get; set; }
    public string Email     { get; set; }
  }

  public class VideoStored2 : WithUpdatedItem {
    public string                VideoId      { get; set; }
    public string                Title        { get; set; }
    public string                ChannelId    { get; set; }
    public string                ChannelTitle { get; set; }
    public DateTime?             UploadDate   { get; set; }
    public DateTime?             AddedDate    { get; set; }
    public string                Description  { get; set; }
    public TimeSpan?             Duration     { get; set; }
    public IReadOnlyList<string> Keywords     { get; set; } = new List<string>();
    public Statistics            Statistics   { get; set; }
    public override string ToString() => $"{Title}";
  }

  public class VideoThumbnail {
    public static VideoThumbnail FromVideoId(string videoId) {
      var t = new ThumbnailSet(videoId);
      return new VideoThumbnail {
        LowResUrl = t.LowResUrl,
        StandardResUrl = t.StandardResUrl,
        HighResUrl = t.HighResUrl,
        MaxResUrl = t.MaxResUrl
      };
    }

    public string LowResUrl      { get; set; }
    public string HighResUrl     { get; set; }
    public string MaxResUrl      { get; set; }
    public string StandardResUrl { get; set; }
  }

  public class VideoCommentStored2 {
    public string    ChannelId       { get; set; }
    public string    VideoId         { get; set; }
    public string    Author          { get; set; }
    public string    AuthorChannelId { get; set; }
    public string    Comment         { get; set; }
    public DateTime? Created         { get; set; }
  }

  public class RecStored2 : Rec, IHasUpdated {
    public string   FromVideoId    { get; set; }
    public string   FromVideoTitle { get; set; }
    public string   FromChannelId  { get; set; }
    public DateTime Updated        { get; set; }

    public override string ToString() => $"{FromVideoTitle} -> {ToVideoTitle}";
  }

  public class VideoCaptionStored2 : WithUpdatedItem {
    public string                             ChannelId  { get; set; }
    public string                             VideoId    { get; set; }
    public DateTime                           UploadDate { get; set; }
    public ClosedCaptionTrackInfo             Info       { get; set; }
    public IReadOnlyCollection<ClosedCaption> Captions   { get; set; } = new List<ClosedCaption>();
  }

  public class VideoExtraStored2 : VideoStored2 {
    public bool?                 HasAd        { get; set; }
    public string                Error        { get; set; }
    public string                SubError     { get; set; }
    public VideoCommentStored2[] Comments     { get; set; }
    public string                Ad           { get; set; }
    public string                CommentsMsg  { get; set; }
    public ScrapeSource          Source       { get; set; }
    public long?                 CommentCount { get; set; }
  }

  public interface IHasUpdated {
    DateTime Updated { get; }
  }

  public abstract class WithUpdatedItem : IHasUpdated {
    public DateTime Updated { get; set; }
  }

  public class UserSearchWithUpdated : WithUpdatedItem {
    public string Origin { get; set; }
    /// <summary>Email of the user performing the search</summary>
    public string Email { get;        set; }
    public string   Query      { get; set; }
    public string[] Ideologies { get; set; }
    public string[] Channels   { get; set; }
  }
}