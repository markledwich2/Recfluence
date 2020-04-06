using System;
using System.Collections.Generic;
using Humanizer;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using SysExtensions.Security;

namespace YtReader {
  public class RootCfg {
    /// <summary>The azure blobl SAS Uri to the blob container hosting secrets.rootCfg.json</summary>
    public Uri AppCfgSas { get; set; }

    // connection string to the configuration directory
    public string AzureStorageCs { get; set; }

    // name of environment (Prod/Dev/MarkDev etc..). used to choose appropreate cfg
    public string Env { get; set; }
  }

  public class Cfg {
    public AppCfg  App  { get; set; }
    public RootCfg Root { get; set; }
  }

  public class AppCfg {
    public string              AppInsightsKey        { get; set; }
    public int                 ParallelChannels      { get; set; } = 4;
    public int                 DefaultParallel       { get; set; } = 8;
    public int                 ChannelsPerContainer  { get; set; } = 150;
    public string              ResourceGroup         { get; set; } = "ytnetworks";
    public YtReaderCfg         YtReader              { get; set; } = new YtReaderCfg();
    public StorageCfg          Storage               { get; set; } = new StorageCfg();
    public ICollection<string> YTApiKeys             { get; set; }
    public HashSet<string>     LimitedToSeedChannels { get; set; }
    public string              SubscriptionId        { get; set; }
    public ServicePrincipalCfg ServicePrincipal      { get; set; } = new ServicePrincipalCfg();
    public string              SeqUrl                { get; set; }
    public SheetsCfg           Sheets                { get; set; }
    public ScraperCfg          Scraper               { get; set; } = new ScraperCfg();
    public SnowflakeCfg        Snowflake             { get; set; } = new SnowflakeCfg();
    public ResultsCfg          Results               { get; set; } = new ResultsCfg();
    public PipeAppCfg          Pipe                  { get; set; } = new PipeAppCfg();
  }

  public class ResultsCfg {
    public string FileQueryUri { get; set; } = "https://raw.githubusercontent.com/markledwich2/YouTubeNetworks_Dataform/master";
  }

  public class ScraperCfg {
    public string     Url            { get; set; }
    public NameSecret Creds          { get; set; }
    public int        TimeoutSeconds { get; set; } = 40;
    public int        Retry          { get; set; } = 10;
    public bool       AlwaysUseProxy { get; set; }
  }

  public class CredJsonTest {
    public string project_id { get; set; }
  }

  public class SheetsCfg {
    public JObject             CredJson            { get; set; }
    public string              MainChannelSheetId  { get; set; }
    public ICollection<string> UserChannelSheetIds { get; set; }
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
    public string DataStorageCs { get; set; }
    public string DbPath        { get; set; } = "data/db";
    public string ResultsPath   { get; set; } = "data/results";
    public string PrivatePath   { get; set; } = "private";
    public string PipePath      { get; set; } = "pipe";
  }
}