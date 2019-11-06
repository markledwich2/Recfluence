using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Mutuo.Etl;
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
    public static FPath SolutionDir => typeof(Setup).LocalAssemblyPath().ParentWithFile("YouTubeNetworks.sln");
    public static FPath SolutionDataDir => typeof(Setup).LocalAssemblyPath().DirOfParent("Data");
    public static FPath LocalDataDir => "Data".AsPath().InAppData(AppName);

    static FPath RootCfgPath => "cfg.json".AsPath().InAppData(AppName);

    public static Logger CreateTestLogger() =>
      new LoggerConfiguration()
        .WriteTo.Seq("http://localhost:5341")
        .CreateLogger();

    public static Logger CreateLogger(AppCfg cfg = null) {
      var c = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information);

      if (cfg?.SeqUrl.HasValue() == true)
        c.WriteTo.Seq(cfg.SeqUrl, LogEventLevel.Debug);

      if (cfg?.AppInsightsKey != null)
        c.WriteTo.ApplicationInsights(new TelemetryConfiguration(cfg.AppInsightsKey), TelemetryConverter.Traces, LogEventLevel.Information);
      
      c.MinimumLevel.Debug();
      return c.CreateLogger();
    }

    static string GetEnv(string name) =>
      Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.User)
      ?? Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);

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
    public static ISimpleFileStore DataStore(this Cfg cfg, StringPath path = null) =>
      new AzureBlobFileStore(cfg.App.Storage.DataStorageCs, path ?? cfg.App.Storage.DbPath);

    public static YtClient YtClient(this Cfg cfg, ILogger log) => new YtClient(cfg.App, log);

    public static YtStore YtStore(this Cfg cfg, ILogger log) {
      var ytStore = new YtStore(cfg.DataStore(cfg.App.Storage.DbPath), log);
      return ytStore;
    }
  }

  public class RootCfg {
    // connection string to the configuration directory
    public string AzureStorageCs { get; set; }

    // name of environment (Prod/Dev/MarkDev etc..). used to choose appropreate cfg
    public string Env { get; set; }
  }

  public class Cfg {
    public AppCfg App { get; set; }
    public RootCfg Root { get; set; }
  }

  public class AppCfg {
    public string AppInsightsKey { get; set; }
    public int ParallelChannels { get; set; } = 4;
    public int ParallelGets { get; set; } = 8;

    public string ResourceGroup { get; set; } = "ytnetworks";
    public YtReaderCfg YtReader { get; set; } = new YtReaderCfg();
    public StorageCfg Storage { get; set; } = new StorageCfg();
    public ICollection<string> YTApiKeys { get; set; }
    public HashSet<string> LimitedToSeedChannels { get; set; }

    public string SubscriptionId { get; set; }
    public ServicePrincipalCfg ServicePrincipal { get; set; } = new ServicePrincipalCfg();
    public ContainerCfg Container { get; set; } = new ContainerCfg();
    public string SeqUrl { get; set; }

    public SheetsCfg Sheets { get; set; }

    public ProxyCfg Proxy { get; set; }
  }

  public class ProxyCfg {
    public string Url { get; set; }
    public NameSecret Creds { get; set; }
    public int TimeoutSeconds { get; set; } = 30;
  }

  public class SheetsCfg {
    public JObject CredJson { get; set; }
    public string MainChannelSheetId { get; set; }
    public ICollection<string> UserChannelSheetIds { get; set; }
  }

  public class YtReaderCfg {
    public DateTime From { get; set; }
    public DateTime? To { get; set; }
    
    /// <summary>
    /// How old a video before we stop collecting video stats.
    /// This is cheap, due to video stats being returned in a video's playlist
    /// </summary>
    public TimeSpan RefreshVideosWithin { get; set; } = 120.Days();

    /// <summary>
    /// We want to keep monitoring YouTube influence even if no new videos have been created (min).
    /// Get at least this number of recs per channel
    /// </summary>
    public int RefreshRecsMin { get; set; } = 1;
    
    /// <summary>
    /// The number of videos within RefreshRecsWithin to refresh recommendations for per channel
    /// </summary>
    public int RefreshRecsMax { get; set; } = 10;
    
    /// <summary>
    /// Gets recs for videos younger than this
    /// </summary>
    public TimeSpan RefreshRecsWithin { get; set; } = 30.Days();

    /// <summary>
    /// How frequently to refresh channel & video stats
    /// </summary>
    public TimeSpan RefreshAllAfter { get; set; } = 23.Hours();
  }

  public class StorageCfg {
    public string DataStorageCs { get; set; }
    public string DbPath { get; set; } = "data/db";
    public string AnalysisPath { get; set; } = "data/analysis";
  }

  public class ContainerCfg {
    public string Registry { get; set; } = "ytnetworks.azurecr.io";
    public string Name { get; set; } = "ytnetworks-auto";
    public string ImageName { get; set; } = "ytnetworks";
    public int Cores { get; set; } = 4;
    public double Mem { get; set; } = 16;
    public NameSecret RegistryCreds { get; set; }
  }

  public class ServicePrincipalCfg {
    public string ClientId { get; set; }
    public string Secret { get; set; }
    public string TennantId { get; set; }
  }
}