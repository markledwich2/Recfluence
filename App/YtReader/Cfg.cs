using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Humanizer;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using SysExtensions.Configuration;
using SysExtensions.Security;
using YtReader.Db;

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
    public            string              AppInsightsKey        { get; set; }
    public            int                 DefaultParallel       { get; set; } = 8;
    [Required] public BranchEnvCfg        Env                   { get; set; } = new BranchEnvCfg();
    [Required] public YtCollectCfg        Collect               { get; set; } = new YtCollectCfg();
    [Required] public StorageCfg          Storage               { get; set; } = new StorageCfg();
    [Required] public ICollection<string> YTApiKeys             { get; set; } = new List<string>();
    [Required] public HashSet<string>     LimitedToSeedChannels { get; set; } = new HashSet<string>();
    [Required] public SeqCfg              Seq                   { get; set; } = new SeqCfg();
    [Required] public SheetsCfg           Sheets                { get; set; } = new SheetsCfg();
    [Required] public ProxyCfg            Proxy                 { get; set; } = new ProxyCfg();
    [Required] public SnowflakeCfg        Snowflake             { get; set; } = new SnowflakeCfg();
    [Required] public WarehouseCfg        Warehouse             { get; set; } = new WarehouseCfg();
    [Required] public SqlServerCfg        AppDb                 { get; set; } = new SqlServerCfg();
    [Required] public ResultsCfg          Results               { get; set; } = new ResultsCfg();
    [Required] public PipeAppCfg          Pipe                  { get; set; } = new PipeAppCfg();
    [Required] public DataformCfg         Dataform              { get; set; } = new DataformCfg();
    [Required] public ElasticCfg          Elastic               { get; set; }
    [Required] public SyncDbCfg           SyncDb                { get; set; } = new SyncDbCfg();
    [Required] public AzureCleanerCfg     Cleaner               { get; set; } = new AzureCleanerCfg();
    [Required] public YtUpdaterCfg        Updater               { get; set; } = new YtUpdaterCfg();
    [Required] public UserScrapeCfg       UserScrape            { get; set; } = new UserScrapeCfg();
  }

  public class ElasticCfg {
    public string     CloudId     { get; set; }
    public NameSecret Creds       { get; set; }
    public string     IndexPrefix { get; set; }
  }

  public class SyncDbCfg {
    public SyncTableCfg[] Tables       { get; set; } = { };
    public string         DefaultTsCol { get; set; } = "updated";
    public int            Parallel     { get; set; } = 4;
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
    [Required] public string     Url            { get; set; }
    [Required] public NameSecret Creds          { get; set; }
    public            int        TimeoutSeconds { get; set; } = 40;
    public            int        Retry          { get; set; } = 10;
    public            bool       AlwaysUseProxy { get; set; }
  }

  public class SheetsCfg {
    [Required] [SkipRecursiveValidation] public JObject             CredJson            { get; set; }
    [Required]                           public string              MainChannelSheetId  { get; set; }
    [Required]                           public ICollection<string> UserChannelSheetIds { get; set; }
    [Required]                           public int                 Parallel            { get; set; } = 4;
  }

  public class YtCollectCfg {
    public DateTime  From { get; set; }
    public DateTime? To   { get; set; }

    /// <summary>How old a video before we stop collecting video stats. This is cheap, due to video stats being returned in a
    ///   video's playlist</summary>
    public TimeSpan RefreshVideosWithin { get; set; } = 120.Days();

    /// <summary>How old a video before we stop collecting recs this is fairly expensive so we keep it within</summary>
    public TimeSpan RefreshRecsWithin { get; set; } = 30.Days();

    /// <summary>We want to keep monitoring YouTube influence even if no new videos have been created (min). Get at least this
    ///   number of recs per channel</summary>
    public int RefreshRecsMin { get; set; } = 2;

    /// <summary>The maximum number of recs to collect from a channel on any given day</summary>
    public int RefreshRecsMax { get; set; } = 30;

    /// <summary>How frequently to refresh channel & video stats</summary>
    public TimeSpan RefreshAllAfter { get; set; } = 23.Hours();

    public bool AlwaysUseProxy { get; set; }
    public bool Headless       { get; set; } = true;

    /// <summary>the number of channels to have in the pending state to be classified</summary>
    public int DiscoverChannels { get; set; } = 100;

    /// <summary>the number of vids to populate with data when discovering new channels (i.e. preparing data to be classified)</summary>
    public int DiscoverChannelVids { get; set; } = 5;

    /// <summary>The maximum number of videos to refresh exta info on (per run) because they have no comments (we didn't used
    ///   to collect them)</summary>
    public int PopulateMissingCommentsLimit { get; set; } = 4;
    public int ParallelChannels     { get;         set; } = 1;
    public int ChannelsPerContainer { get;         set; } = 150;

    public int ChromeParallel { get; set; } = 1;
    public int WebParallel    { get; set; } = 1;
    public int ChromeAttempts { get; set; } = 3;
  }

  public class StorageCfg {
    [Required] public string Container     { get; set; } = "data";
    [Required] public string DataStorageCs { get; set; }
    [Required] public string DbPath        { get; set; } = "db2";
    [Required] public string ResultsPath   { get; set; } = "results";
    [Required] public string PrivatePath   { get; set; } = "private";
    [Required] public string PipePath      { get; set; } = "pipe";
    [Required] public string LogsPath      { get; set; } = "logs";

    [Required] public string BackupCs       { get; set; }
    [Required] public string BackupRootPath { get; set; }
  }

  public class SeqCfg {
    public Uri    SeqUrl             { get; set; }
    public string ContainerGroupName { get; set; } = "seq";
  }
}