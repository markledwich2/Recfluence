using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Humanizer;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using SysExtensions.Security;

namespace YtReader {
  public class RootCfg {
    /// <summary>The azure blobl SAS Uri to the blob container hosting secrets.rootCfg.json</summary>
    [Required]
    public Uri AppCfgSas { get; set; }

    // connection string to the configuration directory
    [Required] public string AzureStorageCs { get; set; }

    // name of environment (Prod/Dev etc..). used to choose appropreate cfg
    [Required] public string Env { get; set; }
  }

  public class Cfg {
    public AppCfg  App  { get; set; }
    public RootCfg Root { get; set; }
  }

  public class AppCfg {
    public            string              AppInsightsKey        { get; set; }
    public            int                 ParallelChannels      { get; set; } = 4;
    public            int                 DefaultParallel       { get; set; } = 8;
    public            int                 ChannelsPerContainer  { get; set; } = 150;
    [Required] public string              ResourceGroup         { get; set; }
    [Required] public YtReaderCfg         YtReader              { get; set; } = new YtReaderCfg();
    [Required] public StorageCfg          Storage               { get; set; } = new StorageCfg();
    [Required] public ICollection<string> YTApiKeys             { get; set; } = new List<string>();
    [Required] public HashSet<string>     LimitedToSeedChannels { get; set; } = new HashSet<string>();
    [Required] public string              SubscriptionId        { get; set; }
    [Required] public ServicePrincipalCfg ServicePrincipal      { get; set; } = new ServicePrincipalCfg();
    [Required] public Uri                 SeqUrl                { get; set; }
    [Required] public SeqHostCfg          SeqHost               { get; set; } = new SeqHostCfg();
    [Required] public SheetsCfg           Sheets                { get; set; } = new SheetsCfg();
    [Required] public ScraperCfg          Scraper               { get; set; } = new ScraperCfg();
    [Required] public SnowflakeCfg        Snowflake             { get; set; } = new SnowflakeCfg();
    [Required] public SqlServerCfg        AppDb                 { get; set; } = new SqlServerCfg();
    [Required] public ResultsCfg          Results               { get; set; } = new ResultsCfg();
    [Required] public PipeAppCfg          Pipe                  { get; set; } = new PipeAppCfg();
    public            SolrCfg             Solr                  { get; set; } = new SolrCfg();
    public            AlgoliaCfg          Algolia               { get; set; } = new AlgoliaCfg();
    public            ElasticCfg          Elastic               { get; set; }
    public            SyncDbCfg           SyncDb                { get; set; } = new SyncDbCfg();
  }

  public class ElasticCfg {
    public string     CloudId { get; set; }
    public NameSecret Creds   { get; set; }
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
    public string FileQueryUri { get; set; } = "https://raw.githubusercontent.com/markledwich2/YouTubeNetworks_Dataform/master";
  }

  public class ScraperCfg {
    [Required] public string     Url            { get; set; }
    [Required] public NameSecret Creds          { get; set; }
    public            int        TimeoutSeconds { get; set; } = 40;
    public            int        Retry          { get; set; } = 10;
    public            bool       AlwaysUseProxy { get; set; }
  }

  public class SheetsCfg {
    [Required] public JObject             CredJson            { get; set; }
    [Required] public string              MainChannelSheetId  { get; set; }
    [Required] public ICollection<string> UserChannelSheetIds { get; set; }
  }

  public class YtReaderCfg {
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

    /// <summary>How frequently to refresh channel & video stats</summary>
    public TimeSpan RefreshAllAfter { get; set; } = 23.Hours();

    public bool AlwaysUseProxy { get; set; }
  }

  public class StorageCfg {
    [Required] public string DataStorageCs { get; set; }
    [Required] public string DbPath        { get; set; }
    [Required] public string ResultsPath   { get; set; }
    [Required] public string PrivatePath   { get; set; }
    [Required] public string PipePath      { get; set; }
  }

  public class SeqHostCfg {
    public string IdleQuery          { get; set; } = "@Timestamp > Now() - 1h and App != 'YtFunctions'";
    public string ContainerGroupName { get; set; } = "seq";
  }

  public class AlgoliaCfg {
    public NameSecret Creds { get; set; }
  }

  public class SolrCfg {
    public Uri Url { get; set; }
  }
}