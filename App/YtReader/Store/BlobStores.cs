using Mutuo.Etl.Blob;
using Semver;
using YtReader.AmazonSite;
using YtReader.Yt;
using static YtReader.Store.DataStoreType;
using static YtReader.Store.StoreTier;

namespace YtReader.Store; 

public enum DataStoreType {
  Pipe,
  /// <summary>Place where data is stored mirroring the warehouse staging tables. Cold tier.</summary>
  DbStage,
  /// <summary>Where data lands while performing operations. Optimised into DB and discarded. Premium tier.</summary>
  //DbLand,
  /// <summary>Results for the website and data sharing. Premium tier</summary>
  Results,
  /// <summary>Data which is not meant to be shared publicly</summary>
  Private,
  Backup,
  Logs,
  Root,
  RootStandard,
  RootS3
}

public enum StoreTier {
  Standard,
  Premium,
  Backup
}

/// <summary>Access to any of the stores</summary>
public record BlobStores(StorageCfg Cfg, S3Cfg S3Cfg, SemVersion Version, ILogger Log) {
  public ISimpleFileStore Store(SPath path = null, StoreTier tier = Premium, SemVersion version = null) {
    var p = new SPath(Cfg.RootPath(version ?? Version));
    if (path != null) p = p.Add(path);
    var store = new AzureBlobFileStore(tier switch {
      StoreTier.Backup => Cfg.BackupCs,
      Premium => Cfg.PremiumDataStorageCs,
      _ => Cfg.DataStorageCs
    }, p, Log);
    return store;
  }

  public ISimpleFileStore Store(DataStoreType type) => type switch {
    DataStoreType.Backup => Store("pipe", StoreTier.Backup),
    Results => Store("results"),
    Pipe => Store("pipe"),
    DbStage => Store("db2"),
    Private => new AzureBlobFileStore(Cfg.DataStorageCs, StoreEx.RootPath("private", Version.Prerelease), Log),
    Logs => Store("logs"),
    Root => Store(tier: Premium),
    RootS3 => new S3Store(S3Cfg, "media"),
    _ => throw new NotImplementedException($"No store for type '{type}'")
  };
}

public static class StoreEx {
  public static string RootPath(this StorageCfg cfg, SemVersion version) => cfg.RootPath(version.Prerelease);
  public static string RootPath(this StorageCfg cfg, string prefix) => RootPath(cfg.Container, prefix);
  public static string RootPath(string container, string prefix) => prefix.HasValue() ? $"{container}-{prefix}" : container;
}

/// <summary>Typed access to jsonl blob collections</summary>
public class YtStore {
  public static readonly int     StoreVersion = 1;
  readonly               ILogger Log;

  public YtStore(ISimpleFileStore store, ILogger log) {
    Store = store;
    Log = log;
    Channels = CreateStore<Channel>("channels");
    Users = CreateStore<User>("users");
    Searches = CreateStore<UserSearchWithUpdated>("searches");
    Videos = CreateStore<Video>("videos");
    VideoExtra = CreateStore<VideoExtra>("video_extra");
    Recs = CreateStore<RecStored>("recs");
    Captions = CreateStore<VideoCaption>("captions");
    ChannelReviews = CreateStore<UserChannelReview>("channel_reviews", r => r.Email);
    Comments = CreateStore<VideoComment>("comments");
    AmazonLink = CreateStore<AmazonLink>("link_meta/amazon");
  }

  public ISimpleFileStore Store { get; }

  public JsonlStore<Channel>               Channels       { get; }
  public JsonlStore<User>                  Users          { get; }
  public JsonlStore<UserSearchWithUpdated> Searches       { get; }
  public JsonlStore<Video>                 Videos         { get; }
  public JsonlStore<VideoExtra>            VideoExtra     { get; }
  public JsonlStore<RecStored>             Recs           { get; }
  public JsonlStore<VideoCaption>          Captions       { get; }
  public JsonlStore<UserChannelReview>     ChannelReviews { get; }
  public JsonlStore<VideoComment>          Comments       { get; }
  public JsonlStore<AmazonLink>            AmazonLink     { get; }

  JsonlStore<T> CreateStore<T>(string name, Func<T, string> getPartition = null) where T : IHasUpdated =>
    new(Store, name, c => c.Updated.FileSafeTimestamp(), Log, StoreVersion.ToString(), getPartition);
}

public enum ChannelStatus {
  None,
  Alive,
  Dead,
  NotFound,
  Blocked,
  Dupe
}

public enum DiscoverSourceType {
  [Obsolete] YouTubeChannelLink,
  /// <summary>Link to a channel</summary>
  ChannelLink,
  /// <summary>Link to a video</summary>
  VideoLink,
  Home,
  Manual
}

public record DiscoverSource(DiscoverSourceType? Type, string LinkId = null, Platform? FromPlatform = null);

public record User : WithUpdatedItem {
  public string                                   UserId          { get; init; }
  public string                                   Name            { get; init; }
  public Platform                                 Platform        { get; init; }
  public string                                   ProfileUrl      { get; init; }
  public IReadOnlyCollection<ChannelSubscription> Subscriptions   { get; init; }
  public long?                                    SubscriberCount { get; init; }
}

public record Channel : WithUpdatedItem {
  public Channel() { }

  public Channel(Platform platform, string channelId, string sourceId = null) {
    Platform = platform;
    ChannelId = channelId;
    SourceId = sourceId ?? channelId;
  }

  /// <summary>Unique id across all paltforms. For YouTube this is the vanilla PlatformId, for other platforms this is the
  ///   <Platform>|<PlatformId></summary>
  public string ChannelId { get; set; }

  /// <summary>The id in the original platform. Might not be unique across platforms</summary>
  public string SourceId { get; set; }

  public string[] SourceIdAlts { get; set; }

  public string                ChannelTitle       { get; set; }
  public string                ChannelName        { get; set; }
  public string                Description        { get; set; }
  public string                LogoUrl            { get; set; }
  public ulong?                Subs               { get; set; }
  public ulong?                ChannelViews       { get; set; }
  public string                Country            { get; set; }
  public string[]              FeaturedChannelIds { get; set; }
  public string                DefaultLanguage    { get; set; }
  public string                Keywords           { get; set; }
  public ChannelSubscription[] Subscriptions      { get; set; }

  public DiscoverSource DiscoverSource { get; set; }

  public Platform Platform { get; set; }

  public string ProfileId   { get; set; }
  public string ProfileName { get; set; }

  public ChannelStatus Status { get; set; }

  public string    StatusMessage  { get; set; }
  public DateTime? LastFullUpdate { get; set; }
  public DateTime? Created        { get; set; }

  public override string ToString() => ChannelTitle ?? ChannelId;
}

public class UserChannelReviewCommon : IHasUpdated {
  public string                      LR                  { get; set; }
  public int                         Relevance           { get; set; }
  public IReadOnlyCollection<string> SoftTags            { get; set; } = new List<string>();
  public string                      Notes               { get; set; }
  public string                      PublicReviewerNotes { get; set; }
  public string                      PublicCreatorNotes  { get; set; }
  public string                      MainChannelId       { get; set; }
  public DateTime                    Updated             { get; set; }
}

public class UserChannelReview : UserChannelReviewCommon {
  public string ChannelId { get; set; }
  public string Email     { get; set; }
}

public enum Platform {
  YouTube,
  Parler
}

public static class PlatformEx {
  public static string FullId(this Platform p, string id) => p switch {
    Platform.YouTube => id,
    _ => id == null ? null : $"{p}|{id}"
  };
}

public enum VideoStatus {
  NotFound,
  Removed,
  Restricted,
  Private
}

public record Video : WithUpdatedItem {
  public Video() { }

  public Video(Platform platform, string id, string sourceId) {
    Platform = platform;
    VideoId = id;
    SourceId = sourceId;
  }

  public Platform Platform { get; init; }

  /// <summary>Globally unique id for the video. Using a canonical url is best</summary>
  public string VideoId { get; init; }

  /// <summary>Id native to the originating platform, doesn't need to be gloablly unique.</summary>
  public string SourceId { get; init; }

  public string Title           { get; init; }
  public string ChannelId       { get; init; }
  public string ChannelSourceId { get; init; }
  public string ChannelTitle    { get; init; }

  /// <summary>The date the video was uploaded. This is the primary record for this. AddedDate is a fallback with YouTube</summary>
  public DateTime? UploadDate { get;                                init; }
  public DateTime?                            AddedDate      { get; init; }
  public string                               Description    { get; init; }
  public TimeSpan?                            Duration       { get; init; }
  public IReadOnlyList<string>                Keywords       { get; init; }
  public Statistics                           Statistics     { get; init; }
  public string                               Thumb          { get; init; }
  public decimal?                             Earned         { get; init; }
  public VideoStatus?                         Status         { get; init; }
  public MultiValueDictionary<string, string> Tags           { get; init; }
  public DiscoverSource                       DiscoverSource { get; init; }
  public ScrapeSource                         Source         { get; init; }
  public override string ToString() => $"{Title}";
}

public record VideoExtra : Video {
  public VideoExtra() { }
  public VideoExtra(Platform platform, string id, string sourceId) : base(platform, id, sourceId) { }
  public bool?  HasAd       { get; init; }
  public string Error       { get; set; }
  public string SubError    { get; set; }
  public string Ad          { get; init; }
  public string CommentsMsg { get; init; }
  public string MediaUrl    { get; init; }
}

public record VideoComment : IHasUpdated {
  public string    CommentId        { get; init; }
  public string    ReplyToCommentId { get; init; }
  public string    VideoId          { get; init; }
  public string    AuthorThumb      { get; init; }
  public string    Author           { get; init; }
  public string    AuthorId         { get; init; }
  public string    AuthorChannelId  { get; init; }
  public string    Comment          { get; init; }
  public DateTime? Created          { get; init; }
  public int?      Likes            { get; init; }
  public bool      IsChannelOwner   { get; init; }
  public DateTime? Modified         { get; init; }
  public Platform  Platform         { get; init; }
  public DateTime  Updated          { get; init; }
}

public record RecStored : Rec, IHasUpdated {
  public string   FromVideoId    { get; set; }
  public string   FromVideoTitle { get; set; }
  public string   FromChannelId  { get; set; }
  public DateTime Updated        { get; set; }

  public override string ToString() => $"{FromVideoTitle} -> {ToVideoTitle}";
}

public record VideoCaption : WithUpdatedItem {
  public string                             ChannelId { get; set; }
  public string                             VideoId   { get; set; }
  public ClosedCaptionTrackInfo             Info      { get; set; }
  public IReadOnlyCollection<ClosedCaption> Captions  { get; set; } = new List<ClosedCaption>();
  public Platform                           Platform  { get; set; }
}

public interface IHasUpdated {
  DateTime Updated { get; }
}

public abstract record WithUpdatedItem : IHasUpdated {
  public DateTime Updated { get; set; }
}

public record UserSearchWithUpdated : WithUpdatedItem {
  public string Origin { get; set; }
  /// <summary>Email of the user performing the search</summary>
  public string Email { get;        set; }
  public string   Query      { get; set; }
  public string[] Ideologies { get; set; }
  public string[] Channels   { get; set; }
}