using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Security;
using SysExtensions.Serialization;
using SysExtensions.Text;
using YtReader.Yt;

namespace YtReader {
  public static class Setup {
    public static string AppName = "YouTubeNetworks";
    public static FPath  SolutionDir     => typeof(Setup).LocalAssemblyPath().ParentWithFile("YouTubeNetworks.sln");
    public static FPath  SolutionDataDir => typeof(Setup).LocalAssemblyPath().DirOfParent("Data");
    public static FPath  LocalDataDir    => "Data".AsPath().InAppData(AppName);

    static FPath RootCfgPath => "cfg.json".AsPath().InAppData(AppName);

    public static Logger CreateTestLogger() =>
      new LoggerConfiguration()
        .WriteTo.Seq("http://localhost:5341")
        .CreateLogger();

    public static Logger CreateLogger(string env, AppCfg cfg = null) {
      var c = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information);

      if (cfg?.SeqUrl.HasValue() == true)
        c.WriteTo.Seq(cfg.SeqUrl, LogEventLevel.Debug);

      if (cfg?.AppInsightsKey != null)
        c.WriteTo.ApplicationInsights(new TelemetryConfiguration(cfg.AppInsightsKey), TelemetryConverter.Traces, LogEventLevel.Debug);

      c.MinimumLevel.Debug();
      c.Enrich.WithProperty("Env", env);
      return c.CreateLogger();
    }

    static        string _env;
    public static string Env => _env ??= GetEnv("Env") ?? "Dev";

    static string GetEnv(string name) =>
      Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.User)
      ?? Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);

    public static async Task<(AppCfg App, RootCfg Root)> LoadCfg2() {
      var cfgRoot = new ConfigurationBuilder()
        .AddEnvironmentVariables()
        .AddJsonFile("local.rootcfg.json", true)
        .Build().Get<RootCfg>();

      if (cfgRoot.AppCfgSas == null)
        throw new InvalidOperationException("AppCfgSas not provided in local.appcfg.json or environment variables");

      var envLower = cfgRoot.Env.ToLowerInvariant();
      var blob = new AzureBlobFileStore(cfgRoot.AppCfgSas);
      var secrets = await blob.Load($"{envLower}.appcfg.json");

      var cfg = new ConfigurationBuilder()
        .AddJsonFile("default.appcfg.json")
        .AddJsonFile($"{Env}.appcfg.json", true)
        .AddJsonStream(secrets)
        .AddJsonFile("local.appcfg.json", true)
        .AddEnvironmentVariables().Build();
      return (cfg.Get<AppCfg>(), cfgRoot);
    }

    public static async Task<Cfg> LoadCfg(ILogger log = null) {
      var rootCfg = new RootCfg();
      rootCfg.Env = GetEnv("YtNetworks_Env") ?? "Dev";
      rootCfg.AzureStorageCs = GetEnv("YtNetworks_AzureStorageCs");

      if (rootCfg.AzureStorageCs.NullOrEmpty()) throw new InvalidOperationException("AzureStorageCs variable not provided");

      var storageAccount = CloudStorageAccount.Parse(rootCfg.AzureStorageCs);
      var cloudBlobClient = storageAccount.CreateCloudBlobClient();
      var cfgText = await cloudBlobClient.GetText("cfg", $"{rootCfg.Env}.json");
      var cfg = cfgText.ToObject<AppCfg>();

      return new Cfg {App = cfg, Root = rootCfg};
    }
  }

  public static class ChannelConfigExtensions {
    public static ISimpleFileStore DataStore(this AppCfg cfg, StringPath path = null) =>
      new AzureBlobFileStore(cfg.Storage.DataStorageCs, path ?? cfg.Storage.DbPath);

    public static YtClient YtClient(this AppCfg cfg, ILogger log) => new YtClient(cfg.YTApiKeys, log);

    public static YtStore YtStore(this AppCfg cfg, ILogger log) {
      var ytStore = new YtStore(cfg.DataStore(cfg.Storage.DbPath), log);
      return ytStore;
    }
  }

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
    public ContainerCfg        Container             { get; set; } = new ContainerCfg();
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
  }

  public class StorageCfg {
    public string DataStorageCs { get; set; }
    public string DbPath        { get; set; } = "data/db";
    public string ResultsPath   { get; set; } = "data/results";
    public string PrivatePath   { get; set; } = "private";
    public string PipePath      { get; set; } = "pipe";
  }
}