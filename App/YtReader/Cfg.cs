using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Humanizer;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Serilog.Events;
using SysExtensions.Collections;
using SysExtensions.Configuration;
using SysExtensions.Security;
using SysExtensions.Text;
using YtReader.Db;
using YtReader.Narrative;

namespace YtReader {
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
    [Required] public AirtableCfg     Airtable              { get; set; } = new();
    [Required] public NarrativesCfg   Narratives            { get; set; } = new();
    [Required] public DataScriptsCfg  DataScripts           { get; set; } = new();
  }

  public class GoogleCfg {
    [SkipRecursiveValidation] public JObject Creds { get; set; }
    public                           string  Test  { get; set; }
  }

  public record RumbleCfg(int CollectParallel = 24);

  public record BitChuteCfg(int CollectParallel = 24);

  public class YtApiCfg {
    [Required] public ICollection<string> Keys { get; set; } = new List<string>();
  }

  public class ElasticCfg {
    public string     CloudId     { get; set; }
    public NameSecret Creds       { get; set; }
    public string     IndexPrefix { get; set; }
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

  public class ProxyCfg {
    [Required] public ProxyConnectionCfg[] Proxies        { get; set; } = { };
    public            int                  TimeoutSeconds { get; set; } = 40;
    public            int                  Retry          { get; set; } = 10;
    public            bool                 AlwaysUseProxy { get; set; }
  }

  public static class ProxyCfgEx {
    public static ProxyConnectionCfg[] DirectAndProxies(this ProxyCfg cfg) => new[] {new ProxyConnectionCfg()}.Concat(cfg.Proxies).ToArray();
  }

  public class ProxyConnectionCfg {
    [Required] public string     Url   { get; set; }
    [Required] public NameSecret Creds { get; set; }

    public bool IsDirect() => Url.NullOrEmpty();
  }

  public class YtCollectCfg {
    public DateTime? To { get; set; }

    /// <summary>How old a video before we stop collecting video stats. This is cheap, due to video stats being returned in a
    ///   video's playlist</summary>
    public TimeSpan RefreshVideosWithinDaily { get; set; } = 360.Days();
    public TimeSpan RefreshVideosWithinNew { get;   set; } = (360 * 10).Days();

    /// <summary>How old a video before we stop collecting recs this is fairly expensive so we keep it within</summary>
    public TimeSpan RefreshRecsWithin { get; set; } = 30.Days();

    public TimeSpan RefreshChannelDetailDebounce { get; set; } = 12.Hours();

    /// <summary>We want to keep monitoring YouTube influence even if no new videos have been created (min). Get at least this
    ///   number of recs per channel</summary>
    public int RefreshRecsMin { get; set; } = 2;

    /// <summary>The maximum number of videos to collect recs from a channel on any given day</summary>
    public int RefreshRecsMax { get; set; } = 40;

    public bool AlwaysUseProxy { get; set; }
    public bool Headless       { get; set; } = true;

    /// <summary>the max number of channels to discover each collect</summary>
    public int DiscoverChannels { get; set; } = 10;

    /// <summary>the number of vids to populate with data when discovering new channels (i.e. preparing data to be classified)</summary>
    public int DiscoverChannelVids { get; set; } = 3;

    public int ParallelChannels { get; set; } = 4;

    public int WebParallel     { get; set; } = 6; // parallelism for plain html scraping of video's, recs
    public int CaptionParallel { get; set; } = 3; // smaller than Web to try and reduce changes of out-of-mem failures

    /// <summary>maximum number of videos to load when updating a channel.</summary>
    public int MaxChannelComments { get; set; } = 4;

    /// <summary>These thresholds shortcut the collection for channels to save costs. Bellow the analysis thresholds because we
    ///   want to collect ones that might tip over</summary>
    public ulong MinChannelSubs { get;  set; } = 8000;
    public ulong MinChannelViews { get; set; } = 1_000_000; // some channels don't have subs, so we fallback to a minimum views for the channels. 

    /// <summary>Number of video extra's to collect that are missing per channel. Since YT removed a nice endpoint, we need to
    ///   go and backfill information from the video itself</summary>
    public int ChannelBatchSize { get;      set; } = 100;
    public int MaxChannelDailyVideos { get; set; } = 10_000;
    public int MaxChannelFullVideos  { get; set; } = 40_000;

    public int? MaxMissingUserChannels { get; set; } = 10_000;
  }

  public class StorageCfg {
    [Required] public string Container            { get; set; } = "data";
    [Required] public string PremiumDataStorageCs { get; set; }
    [Required] public string DataStorageCs        { get; set; }

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
}