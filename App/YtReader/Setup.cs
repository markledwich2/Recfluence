using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Humanizer;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Semver;
using Serilog;
using Serilog.Core;
using Serilog.Core.Enrichers;
using Serilog.Events;
using SysExtensions.Build;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Yt;

namespace YtReader {
  public static class Setup {
    public static string AppName = "YouTubeNetworks";

    static readonly AsyncLazy<SemVersion> Version = new AsyncLazy<SemVersion>(() => GitVersionInfo.DiscoverSemVer(typeof(Setup)));
    public static   FPath                 SolutionDir     => typeof(Setup).LocalAssemblyPath().ParentWithFile("YouTubeNetworks.sln");
    public static   FPath                 SolutionDataDir => typeof(Setup).LocalAssemblyPath().DirOfParent("Data");
    public static   FPath                 LocalDataDir    => "Data".AsPath().InAppData(AppName);

    public static Logger CreateTestLogger() =>
      new LoggerConfiguration()
        .WriteTo.Seq("http://localhost:5341")
        .CreateLogger();

    public static ILogger ConsoleLogger(LogEventLevel level = LogEventLevel.Information) =>
      new LoggerConfiguration()
        .WriteTo.Console(level).CreateLogger();

    public static async Task<Logger> CreateLogger(string env, string app, AppCfg cfg = null, bool startSeq = true) {
      var c = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information);

      if (cfg?.AppInsightsKey != null)
        c.WriteTo.ApplicationInsights(new TelemetryConfiguration(cfg.AppInsightsKey), TelemetryConverter.Traces, LogEventLevel.Debug);

      if (cfg != null)
        c = await c.WriteToSeqAndStartIfNeeded(cfg);

      var log = c.MinimumLevel.Debug()
        .YtEnrich(env, app, await GetVersion())
        .CreateLogger();

      Log.Logger = log;
      return log;
    }

    public static LoggerConfiguration YtEnrich(this LoggerConfiguration logCfg, string env, string app, SemVersion v) =>
      logCfg.Enrich.With(
        new PropertyEnricher("App", app),
        new PropertyEnricher("Env", env),
        new PropertyEnricher("Version", v),
        new PropertyEnricher("Machine", Environment.MachineName));

    static async Task<LoggerConfiguration> WriteToSeqAndStartIfNeeded(this LoggerConfiguration loggerCfg, AppCfg cfg) {
      if (cfg?.SeqUrl == null) return loggerCfg;
      var resCfg = loggerCfg.WriteTo.Seq(cfg.SeqUrl.OriginalString, LogEventLevel.Debug);
      await StartSeqIfNeeded(cfg);
      return resCfg;
    }

    /// <summary>Kick of a restart on seq if needed (doesn't wait for it)</summary>
    public static async Task StartSeqIfNeeded(AppCfg cfg) {
      var log = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information).CreateLogger();
      if (cfg.SeqUrl.IsLoopback)
        return;
      try {
        var azure = cfg.Pipe.Azure.GetAzure();
        var seqGroup = await azure.SeqGroup(cfg);
        if (seqGroup.State() != ContainerState.Running) {
          await azure.ContainerGroups.StartAsync(seqGroup.ResourceGroupName, seqGroup.Name);
          var seqStart = await seqGroup.WaitForState(ContainerState.Running).WithTimeout(30.Seconds());
          log.Information(seqStart.Success ? "{SeqUrl} started" : "{SeqUrl} launched but not started yet", cfg.SeqUrl);
        }
        else {
          log.Information("Seq connected on {SeqUrl}", cfg.SeqUrl);
        }
      }
      catch (Exception ex) {
        log.Error(ex, "Error starting seq: {Error}", ex.Message);
      }
    }

    public static Dictionary<string, string> ContainerEnv(this RootCfg rootCfg) =>
      new Dictionary<string, string> {
        {nameof(RootCfg.Env), rootCfg.Env},
        {nameof(RootCfg.AppCfgSas), rootCfg.AppCfgSas.ToString()}
      };

    public static Task<SemVersion> GetVersion() => Version.GetOrCreate();

    static string GetEnv(string name) =>
      Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.User)
      ?? Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);

    public static async Task<(AppCfg App, RootCfg Root)> LoadCfg2(string basePath = null, ILogger rootLogger = null) {
      rootLogger ??= Log.Logger ?? Logger.None;
      basePath ??= Environment.CurrentDirectory;
      var cfgRoot = new ConfigurationBuilder()
        .SetBasePath(basePath)
        .AddEnvironmentVariables()
        .AddJsonFile("local.rootcfg.json", true)
        .Build().Get<RootCfg>();

      if (cfgRoot.AppCfgSas == null)
        throw new InvalidOperationException("AppCfgSas not provided in local.rootcfg.json or environment variables");

      var envLower = cfgRoot.Env.ToLowerInvariant();
      var blob = new AzureBlobFileStore(cfgRoot.AppCfgSas, Logger.None);
      var secrets = (await blob.Load($"{envLower}.appcfg.json")).AsString();

      var cfg = new ConfigurationBuilder()
        .SetBasePath(basePath)
        .AddJsonFile("default.appcfg.json")
        .AddJsonFile($"{cfgRoot.Env}.appcfg.json", true)
        .AddJsonStream(secrets.AsStream())
        .AddJsonFile("local.appcfg.json", true)
        .AddEnvironmentVariables().Build();
      var appCfg = cfg.Get<AppCfg>();

      if (appCfg.Sheets != null)
        appCfg.Sheets.CredJson = secrets.ToJObject().SelectToken("sheets.credJson") as JObject;
      appCfg.Pipe = await GetPipeAppCfg(appCfg);

      var validation = Validate(appCfg);
      if (validation.Any()) {
        rootLogger.Error("Validation errors in app cfg {Errors}", validation);
        throw new InvalidOperationException($"validation errors with app cfg {validation.Join(", ")}");
      }

      return (appCfg, cfgRoot);
    }

    public static IReadOnlyCollection<ValidationResult> Validate(object cfgObject) {
      var context = new ValidationContext(cfgObject, null, null);
      var results = new List<ValidationResult>();
      Validator.TryValidateObject(cfgObject, context, results, true);
      return results;
    }

    static async Task<PipeAppCfg> GetPipeAppCfg(AppCfg cfg) {
      var semver = await GetVersion();

      var pipe = cfg.Pipe.JsonClone();
      pipe.Default.Container.Tag ??= semver.ToString();
      foreach (var p in pipe.Pipes) p.Container.Tag ??= semver.ToString();

      pipe.Store ??= new PipeAppStorageCfg {
        Cs = cfg.Storage.DataStorageCs,
        Path = cfg.Storage.PipePath
      };
      pipe.Azure ??= new PipeAzureCfg {
        ResourceGroup = cfg.ResourceGroup,
        ServicePrincipal = cfg.ServicePrincipal,
        SubscriptionId = cfg.SubscriptionId
      };
      return pipe;
    }

    public static IPipeCtx PipeCtx(RootCfg rootCfg, AppCfg cfg, IComponentContext scope, ILogger log) {
      var envVars = rootCfg.ContainerEnv();
      var pipeCtx = Pipes.CreatePipeCtx(cfg.Pipe, log, scope, new[] {typeof(YtDataUpdater).Assembly}, envVars);
      return pipeCtx;
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

    public static ILifetimeScope BaseScope(RootCfg rootCfg, AppCfg cfg, ILogger log) {
      var scopeHolder = new ScopeHolder();
      var b = new ContainerBuilder();
      b.Register(_ => log).SingleInstance();
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => cfg.YtStore(log)).SingleInstance();
      b.Register<Func<Task<DbConnection>>>(_ => async () => (DbConnection) await cfg.Snowflake.OpenConnection());
      b.Register<Func<IPipeCtx>>(c => () => PipeCtx(rootCfg, cfg, scopeHolder.Scope, log));
      b.Register(_ => cfg.Pipe.Azure.GetAzure());
      b.RegisterType<YtDataUpdater>(); // this will resolve IPipeCtx
      var scope = b.Build().BeginLifetimeScope();
      scopeHolder.Scope = scope;
      return scope;
    }

    public static IPipeCtx ResolvePipeCtx(this ILifetimeScope scope) => scope.Resolve<Func<IPipeCtx>>()();

    public static Task<IContainerGroup> SeqGroup(this IAzure azure, AppCfg cfg) =>
      azure.ContainerGroups.GetByResourceGroupAsync(cfg.ResourceGroup, cfg.SeqHost.ContainerGroupName);

    class ScopeHolder {
      public IComponentContext Scope { get; set; }
    }
  }

  public static class ChannelConfigExtensions {
    public static ISimpleFileStore DataStore(this AppCfg cfg, ILogger log, StringPath path = null) =>
      new AzureBlobFileStore(cfg.Storage.DataStorageCs, path ?? cfg.Storage.DbPath, log);

    public static YtClient YtClient(this AppCfg cfg, ILogger log) => new YtClient(cfg.YTApiKeys, log);

    public static YtStore YtStore(this AppCfg cfg, ILogger log) {
      var ytStore = new YtStore(cfg.DataStore(log, cfg.Storage.DbPath), log);
      return ytStore;
    }

    public static bool IsProd(this RootCfg root) => root.Env.ToLowerInvariant() == "prod";
  }
}