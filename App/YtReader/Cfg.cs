using System.ComponentModel.DataAnnotations;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Serilog.Events;
using SysExtensions.Configuration;
using SysExtensions.Security;
using YtReader.Data;
using YtReader.Db;

namespace YtReader;

public class RootCfg {
  /*
  /// <summary>The azure blobl SAS Uri to the blob container hosting secrets.rootCfg.json</summary>
  [Required]
  public Uri AppCfgSas { get; set; }
  */

  // connection string to the configuration directory
  [Required] public string AppStoreCs { get; set; }

  // name of environment (Prod/Dev etc..). used to choose appropreate cfg
  [Required] public string Env { get; set; }

  // if specified, used to override default behavior (including when in prod) of the environment branch/prefix
  public string BranchEnv { get; set; }
}

public class Cfg {
  public AppCfg  App  { get; set; }
  public RootCfg Root { get; set; }
}

public enum WarehouseMode {
  /// <summary>will connect the development branch warehouse</summary>
  Branch,
  /// <summary>In dev environments will connect to prod in read only mode</summary>
  ProdReadIfDev,
}

public class AppCfg {
  public            string          AppInsightsKey        { get; set; }
  public            int             DefaultParallel       { get; set; } = 8;
  public            LogEventLevel   LogLevel              { get; set; } = LogEventLevel.Debug;
  [Required] public BranchEnvCfg    EnvCfg                { get; set; } = new();
  [Required] public YtCollectCfg    Collect               { get; set; } = new();
  [Required] public StorageCfg      Storage               { get; set; } = new();
  [Required] public YtApiCfg        YtApi                 { get; set; } = new();
  [Required] public HashSet<string> LimitedToSeedChannels { get; set; } = new();
  [Required] public SeqCfg          Seq                   { get; set; } = new();
  [Required] public ProxyCfg        Proxy                 { get; set; } = new();
  [Required] public SnowflakeCfg    Snowflake             { get; set; } = new();
  [Required] public WarehouseCfg    Warehouse             { get; set; } = new();
  [Required] public ResultsCfg      Results               { get; set; } = new();
  [Required] public PipeAppCfg      Pipe                  { get; set; } = new();
  [Required] public DataformCfg     Dataform              { get; set; } = new();
  [Required] public ElasticCfg      Elastic               { get; set; }
  [Required] public SyncDbCfg       SyncDb                { get; set; } = new();
  [Required] public AzureCleanerCfg Cleaner               { get; set; } = new();
  [Required] public YtUpdaterCfg    Updater               { get; set; } = new();
  [Required] public UserScrapeCfg   UserScrape            { get; set; } = new();
  [Required] public SearchCfg       Search                { get; set; } = new();
  [Required] public BitChuteCfg     BitChute              { get; set; } = new();
  [Required] public RumbleCfg       Rumble                { get; set; } = new();
  [Required] public GoogleCfg       Google                { get; set; } = new();
  [Required] public DataScriptsCfg  DataScripts           { get; set; } = new();
  [Required] public AwsCfg          Aws                   { get; set; } = new();
}

public class GoogleCfg {
  [SkipRecursiveValidation] public JObject Creds { get; set; }
  public                           string  Test  { get; set; }
}

public class YtApiCfg {
  [Required] public ICollection<string> Keys { get; set; } = new List<string>();
}

public record AwsCfg {
  public string     Region { get; set; }
  public S3Cfg      S3     { get; init; }
  public NameSecret Creds  { get; set; }
}

public class ElasticCfg {
  public string CloudId { get; set; } =
    "recfluence:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo5MjQzJDI1MGIyMDQ1MmRmMDQzNGQ4MDBjNjFiYmI1NDlhMjQ4JDU0OGQzODFhMDE5NDQ5NmE5Y2FiMDVjM2NiZmYzZWZh";
  [Required] public string     Url         { get; set; } = "https://recfluence.es.eastus2.azure.elastic-cloud.com:9243";
  [Required] public NameSecret Creds       { get; set; }
  [Required] public NameSecret PublicCreds { get; set; }
  public            string     IndexPrefix { get; set; }
}

public class SyncDbCfg {
  public int Parallel { get; set; } = 4;
}

public enum MergeStrategy {
  DeleteFirst,
  MergeStatement
}

public class ResultsCfg {
  [Required] public string FileQueryUri { get; set; } = "https://raw.githubusercontent.com/markledwich2/YouTubeNetworks_Dataform/master";
  [Required] public int    Parallel     { get; set; } = 4;
}

public record ProxyCfg {
  [Required] public ProxyConnectionCfg[] Proxies        { get; init; } = Array.Empty<ProxyConnectionCfg>();
  public            int                  TimeoutSeconds { get; init; } = 40;
  public            int                  Retry          { get; init; } = 5;
  public            bool                 AlwaysUseProxy { get; init; }
}

public enum ProxyType {
  Datacenter,
  Residential
}

public record ProxyConnectionCfg {
  [Required] public string     Url   { get; init; }
  [Required] public NameSecret Creds { get; init; }
  public            ProxyType  Type  { get; set; }
}

public interface ICommonCollectCfg {
  int WebParallel          { get; }
  int MaxChannelFullVideos { get; set; }
  int MaxChannelComments   { get; set; }
}

public record SimpleCollectCfg : ICommonCollectCfg {
  public int      OuterParallel        { get; set; } = 4; // default parallelism for something the partitions other async work
  public int?     HomeVidLimit         { get; init; }
  public string[] HomeCats             { get; init; }
  public int      Retries              { get; init; } = 4;
  public int      WebParallel          { get; set; }  = 24; // parallelism for plain html scraping of video's, recs
  public int      MaxChannelFullVideos { get; set; }  = 40_000;
  public int      MaxChannelComments   { get; set; }  = 4;
}

public record RumbleCfg : SimpleCollectCfg { }
public record BitChuteCfg : SimpleCollectCfg { }

public record YtCollectCfg : ICommonCollectCfg {
  public DateTime? To { get; set; }

  /// <summary>How old a video before we stop collecting video stats. This is cheap, due to video stats being returned in a
  ///   video's playlist</summary>
  public TimeSpan RefreshVideosWithinDaily { get; set; } = 360.Days();
  public TimeSpan RefreshVideosWithinNew { get;   set; } = (360 * 10).Days();

  /// <summary>How old a video before we stop collecting recs this is fairly expensive so we keep it within</summary>
  public TimeSpan RefreshRecsWithin { get; set; } = 30.Days();

  public TimeSpan RefreshChannelDetailDebounce { get; set; } = 12.Hours();
  public TimeSpan RefreshExtraDebounce         { get; set; } = 12.Hours();

  /// <summary>We want to keep monitoring YouTube influence even if no new videos have been created (min). Get at least this
  ///   number of recs per channel</summary>
  public int RefreshRecsMin { get; set; } = 2;

  /// <summary>The maximum number of videos to collect recs from a channel on any given day</summary>
  public int RefreshRecsMax { get; set; } = 10;

  public bool AlwaysUseProxy { get; set; }

  /// <summary>the max number of channels to discover each collect</summary>
  public int DiscoverChannels { get; set; } = 10;

  /// <summary>the number of vids to populate with data when discovering new channels (i.e. preparing data to be classified)</summary>
  public int DiscoverChannelVids { get; set; } = 3;

  public int ParallelChannels { get; set; } = 12;
  public int CaptionParallel  { get; set; } = 3; // smaller than Web to try and reduce changes of out-of-mem failures
  public int WebParallel      { get; set; } = 8; // parallelism for plain html scraping of video's, recs

  /// <summary>These thresholds shortcut the collection for channels to save costs. Bellow the analysis thresholds because we
  ///   want to collect ones that might tip over</summary>
  public ulong MinChannelSubs { get;  set; } = 1000;
  public ulong MinChannelViews { get; set; } = 1_000_000; // some channels don't have subs, so we fallback to a minimum views for the channels. 

  /// <summary>Number of video extra's to collect that are missing per channel. Since YT removed a nice endpoint, we need to
  ///   go and backfill information from the video itself</summary>
  public int ChannelBatchSize { get;      set; } = 100;
  public int MaxChannelDailyVideos { get; set; } = 10_000;

  public int  UserBatchSize           { get; set; } = 1000;
  public int? MaxMissingUsers         { get; set; } = 100_000;
  public int? MaxExtraErrorsInChannel { get; set; } = 10;
  

  /// <summary>maximum number of videos to load when updating a channel.</summary>
  public int MaxChannelComments { get; set; } = 1;

  public int MaxChannelFullVideos { get; set; } = 40_000;

  /// <summary>This limits how many number of subscriptions to load per user. Some have 100k plus of generated subs which we
  ///   aren't interested in/summary>
  public int MaxSubscriptionsToSave { get; set; } = 1_000;

  /// <summary>Number of comments load ot load for a particular video</summary>
  public int MaxCommentsPerVid { get; set; } = 200;

  /// <summary>This limits how many lines of caption data to find in a single video. Audiobooks and 24hr news or fake videos
  ///   may become too large to load in our array format</summary>
  public int MaxCaptionLinesToSave = 50_000;
}

public class StorageCfg {
  [Required] public string Container        { get; set; } = "data";
  [Required] public string PublicContainer  { get; set; } = "public";
  [Required] public string PrivateContainer { get; set; } = "private";

  [Required] public string DataStorageCs        { get; set; }
  public            string PremiumDataStorageCs { get; set; }

  [Required] public string BackupCs { get; set; }
}

public class SeqCfg {
  public Uri    SeqUrl             { get; set; }
  public string ContainerGroupName { get; set; } = "seq";
}

public class SearchCfg {
  public int Retries   { get; set; } = 5;
  public int BatchSize { get; set; } = 2_000;
  public int Parallel  { get; set; } = 1;
}