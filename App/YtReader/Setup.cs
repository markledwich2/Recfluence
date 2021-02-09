using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Autofac;
using Autofac.Builder;
using Flurl.Http;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Extensions.Configuration;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Mutuo.Etl.DockerRegistry;
using Mutuo.Etl.Pipe;
using Nest;
using Newtonsoft.Json.Linq;
using Semver;
using Serilog;
using Serilog.Core;
using Serilog.Core.Enrichers;
using Serilog.Events;
using SysExtensions;
using SysExtensions.Configuration;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.BitChute;
using YtReader.Db;
using YtReader.Rumble;
using YtReader.Search;
using YtReader.Store;
using YtReader.YtApi;
using YtReader.YtWebsite;
using static Serilog.Events.LogEventLevel;

namespace YtReader {
  public static class Setup {
    public static string AppName      = "YouTubeNetworks";
    public static string CfgContainer = "cfg";

    public static FPath SolutionDir     => typeof(Setup).LocalAssemblyPath().ParentWithFile("Recfluence.sln");
    public static FPath SolutionDataDir => typeof(Setup).LocalAssemblyPath().DirOfParent("Data");
    public static FPath LocalDataDir    => "Data".AsPath().InAppData(AppName);

    public static Logger CreateLogger(string env, string app, VersionInfo version, AppCfg cfg = null) {
      var c = new LoggerConfiguration()
        .WriteTo.Console(Information);

      if (cfg?.AppInsightsKey != null)
        c.WriteTo.ApplicationInsights(new TelemetryConfiguration(cfg.AppInsightsKey), TelemetryConverter.Traces, cfg.LogLevel);

      if (cfg != null)
        c = c.ConfigureSeq(cfg);

      var log = c.YtEnrich(env, app, version.Version)
        .MinimumLevel.ControlledBy(new (cfg?.LogLevel ?? Debug))
        .CreateLogger();

      FlurlHttp.Configure(settings => {
        settings.OnError = e => log.Debug(e.Exception, "Furl error: {Error}", e.ToString());
        settings.AfterCall = e => { log.Verbose("Furl: {Request}", e.HttpRequestMessage); };
      });

      Log.Logger = log;
      return log;
    }

    public static LoggerConfiguration YtEnrich(this LoggerConfiguration logCfg, string env, string app, SemVersion version) {
      var container = AzureContainers.GetContainerEnv();
      return logCfg.Enrich.With(
        new PropertyEnricher("App", app),
        new PropertyEnricher("Env", env),
        new PropertyEnricher("Machine", container ?? Environment.MachineName),
        new PropertyEnricher("Version", version)
      );
    }

    static LoggerConfiguration ConfigureSeq(this LoggerConfiguration loggerCfg, AppCfg cfg) {
      var seqCfg = cfg?.Seq;
      if (seqCfg?.SeqUrl == null) return loggerCfg;
      var resCfg = loggerCfg.WriteTo.Seq(seqCfg.SeqUrl.OriginalString, cfg.LogLevel);
      return resCfg;
    }

    public static Logger CreateTestLogger() =>
      new LoggerConfiguration()
        .WriteTo.Seq("http://localhost:5341", Debug).MinimumLevel.Debug()
        .WriteTo.Console().MinimumLevel.Debug()
        .CreateLogger();

    public static ILogger ConsoleLogger(LogEventLevel level = Information) =>
      new LoggerConfiguration()
        .WriteTo.Console(level).CreateLogger();

    /// <summary>Will pass root & app config to pipes from the calling process. Only configuration that should come from the
    ///   caller is passed. e.g. all root cfg's and a few app ones</summary>
    /// <param name="rootCfg"></param>
    /// <param name="appCfg"></param>
    /// <returns></returns>
    public static (string name, string value)[] PipeEnv(RootCfg rootCfg, AppCfg appCfg, SemVersion version) =>
      new[] {
        (nameof(RootCfg.Env), rootCfg.Env),
        (nameof(RootCfg.AppStoreCs), rootCfg.AppStoreCs),
        (nameof(RootCfg.BranchEnv), version.Prerelease)
      };

    /// <summary>Loads application configuration, and sets global config for .net</summary>
    public static async Task<(AppCfg App, RootCfg Root, VersionInfo Version)> LoadCfg(string basePath = null, ILogger rootLogger = null) {
      rootLogger ??= Log.Logger ?? Logger.None;
      basePath ??= Environment.CurrentDirectory;
      rootLogger.Information("Loading cfg with base path: {Path}", basePath);
      var cfgRoot = new ConfigurationBuilder()
        .SetBasePath(basePath)
        .AddJsonFile("local.rootcfg.json", true)
        .AddEnvironmentVariables()
        .Build().Get<RootCfg>();

      if (cfgRoot.AppStoreCs == null)
        throw new InvalidOperationException("AppStoreCs not provided in local.rootcfg.json or environment variables");

      var versionProvider = new VersionInfoProvider(rootLogger, cfgRoot);
      var version = await versionProvider.Version();

      var secretStore = new AzureBlobFileStore(cfgRoot.AppStoreCs, CfgContainer, Logger.None);
      var secretNames = cfgRoot.IsProd() ? new[] {"prod"} : new[] {"dev", version.Version.Prerelease};
      var secrets = new List<JObject>();
      foreach (var name in secretNames) {
        var fileName = $"{name}.appcfg.json";
        if (await secretStore.Info(fileName) == null) continue;
        secrets.Add((await secretStore.Load(fileName)).AsString().ParseJObject());
      }

      if (secrets == null) throw new InvalidOperationException("can't find secrets cfg file");

      var appJson = new JObject();
      var mergeSettings = new JsonMergeSettings {MergeNullValueHandling = MergeNullValueHandling.Ignore};
      void MergeAppJson(string path) {
        var p = (basePath ?? ".").AsPath().Combine(path);
        if (!p.Exists) return;
        var newCfg = p.Read().ParseJObject();
        appJson.Merge(newCfg, mergeSettings);
      }
      
      MergeAppJson("default.appcfg.json");
      MergeAppJson($"{cfgRoot.Env}.appcfg.json");
      foreach (var j in secrets)
        appJson.Merge(j, mergeSettings);
      MergeAppJson("local.appcfg.json");
      appJson = appJson.JsonMerge(GetEnvironmentSettings<AppCfg>());
      var appCfg = appJson.ToObject<AppCfg>(JsonExtensions.DefaultSerializer);
      
      PostLoadConfiguration(appCfg, cfgRoot, version.Version);

      var validation = Validate(appCfg);
      if (validation.Any()) {
        rootLogger.Error("Validation errors in app cfg {Errors}", validation);
        throw new InvalidOperationException($"validation errors with app cfg {validation.Join("\n", v => $"{v.MemberNames.Join(".")}: {v.ErrorMessage}")}");
      }

      // as recommended here https://github.com/Azure/azure-storage-net-data-movement, also should be good for performance of our other Http interactions
      ServicePointManager.Expect100Continue = false;
      ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;

      return (appCfg, cfgRoot, version);
    }

    static JObject GetEnvironmentSettings<T>() where T: class {
      var props = typeof(T).GetProperties().Select(p => p.Name).ToHashSet(StringComparer.InvariantCultureIgnoreCase);
      var j = new JObject();
      foreach (DictionaryEntry e in Environment.GetEnvironmentVariables()) {
        var p = e.Key.ToString().ToCamelCase();
        if(!props.Contains(p)) continue;
        var s = e.Value?.ToString()?.Trim() ?? "null";
        JToken t;
        try {
          t = JToken.Parse(s);
        }
        catch (Exception) {
          t = JToken.Parse(s.Replace(@"\", @"\\").SingleQuote('\\'));
        }
        j[p] = t;
      }
      return j;
    }

    static void PostLoadConfiguration(AppCfg appCfg, RootCfg cfgRoot, SemVersion version) {
      appCfg.Snowflake.DbSuffix ??= version.Prerelease;
      appCfg.Elastic.IndexPrefix = EsIndex.IndexPrefix(version);

      // override pipe cfg with equivalent global cfg
      appCfg.Pipe.Store = new() {
        Cs = appCfg.Storage.DataStorageCs,
        Path = "pipe"
      };

      // by default, backup to the app/root storage location
      appCfg.Storage.BackupCs ??= cfgRoot.AppStoreCs;

      // merge default properties from the pipe config
      appCfg.Dataform.Container = appCfg.Pipe.Default.Container.JsonMerge(appCfg.Dataform.Container);
      appCfg.UserScrape.Container = appCfg.Pipe.Default.Container.JsonMerge(appCfg.UserScrape.Container);
    }

    static IReadOnlyCollection<ValidationResult> Validate(object cfgObject) =>
      new DataAnnotationsValidator().TryValidateObjectRecursive(cfgObject).results;

    public static PipeAppCtx PipeAppCtxEmptyScope(RootCfg root, AppCfg appCfg, SemVersion version) =>
      new(new ContainerBuilder().Build().BeginLifetimeScope(), typeof(YtCollector)) {
        EnvironmentVariables = PipeEnv(root, appCfg, version)
      };

    public static ILifetimeScope MainScope(RootCfg rootCfg, AppCfg cfg, PipeAppCtx pipeAppCtx, VersionInfo version, ILogger log, string[] args = null) {
      var scope = new ContainerBuilder().ConfigureScope(rootCfg, cfg, pipeAppCtx, version, log, args)
        .Build().BeginLifetimeScope();
      pipeAppCtx.Scope = scope;
      return scope;
    }
    
    public static ContainerBuilder ConfigureScope(this ContainerBuilder b, RootCfg rootCfg, AppCfg cfg, PipeAppCtx pipeAppCtx, VersionInfo version,
      ILogger log, string[] args) {
      var containerCfg = cfg.Pipe.Default.Container;

      b.Register(_ => version);
      b.Register(_ => version.Version);
      b.Register(_ => log);
      b.Register(_ => new CliEntry(args));
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => rootCfg).SingleInstance();
      b.Register(_ => containerCfg);
      b.Register(_ => cfg.Pipe).SingleInstance();
      b.Register(_ => cfg.Pipe.Azure).SingleInstance();
      b.Register(_ => cfg.Elastic).SingleInstance();
      b.Register(_ => cfg.Snowflake).SingleInstance();
      b.Register(_ => cfg.Warehouse).SingleInstance();
      b.Register(_ => cfg.Storage).SingleInstance();
      b.Register(_ => cfg.Cleaner).SingleInstance();
      b.Register(_ => cfg.EnvCfg).SingleInstance();
      b.Register(_ => cfg.Updater).SingleInstance();
      b.Register(_ => cfg.Results).SingleInstance();
      b.Register(_ => cfg.Dataform).SingleInstance();
      b.Register(_ => cfg.Seq).SingleInstance();
      b.Register(_ => cfg.UserScrape).SingleInstance();
      b.Register(_ => cfg.Proxy).SingleInstance();
      b.Register(_ => cfg.Collect).SingleInstance();
      b.Register(_ => cfg.YtApi).SingleInstance();
      b.Register(_ => cfg.Search).SingleInstance();
      b.Register(_ => cfg.SyncDb).SingleInstance();
      b.Register(_ => cfg.AppDb).SingleInstance();
      b.Register(_ => cfg.BitChute).SingleInstance();
      b.Register(_ => cfg.Rumble).SingleInstance();
      b.Register(_ => cfg.Google).SingleInstance();
      
      b.RegisterType<SnowflakeConnectionProvider>();
      b.Register(_ => cfg.Pipe.Azure.GetAzure());

      b.RegisterType<BlobStores>();
      foreach (var storeType in EnumExtensions.Values<DataStoreType>())
        b.Register(_ => _.Resolve<BlobStores>().Store(storeType)).Keyed<ISimpleFileStore>(storeType);

      b.RegisterType<YtClient>();
      b.Register(_ => new ElasticClient(cfg.Elastic.ElasticConnectionSettings()));
      b.RegisterType<YtStore>().WithKeyedParam(DataStoreType.DbStage, Typ.Of<ISimpleFileStore>());
      b.RegisterType<YtResults>().WithKeyedParam(DataStoreType.Results, Typ.Of<ISimpleFileStore>());
      b.RegisterType<StoreUpgrader>().WithKeyedParam(DataStoreType.DbStage, Typ.Of<ISimpleFileStore>());
      b.RegisterType<Stage>().WithKeyedParam(DataStoreType.DbStage, Typ.Of<ISimpleFileStore>());
      b.RegisterType<WebScraper>().WithKeyedParam(DataStoreType.Logs, Typ.Of<ISimpleFileStore>());
      b.RegisterType<ChromeScraper>().WithKeyedParam(DataStoreType.Logs, Typ.Of<ISimpleFileStore>());

      b.RegisterType<YtSearch>();
      b.RegisterType<YtCollector>();
      b.RegisterType<WarehouseCreator>();
      b.RegisterType<YtBackup>();
      b.RegisterType<AzureCleaner>();
      b.RegisterType<YtUpdater>(); // new instance so it can have a unique runId in its contructor
      b.Register(_ => new RegistryClient(containerCfg.Registry, containerCfg.RegistryCreds));
      b.RegisterType<BranchEnvCreator>();
      b.RegisterType<YtDataform>();
      b.RegisterType<ContainerLauncher>();
      b.RegisterType<AzureContainers>();
      b.RegisterType<LocalPipeWorker>();
      b.RegisterType<UserScrape>();
      b.RegisterType<YtConvertWatchTimeFiles>();
      b.RegisterType<YtIndexResults>();
      b.RegisterType<BcWeb>();
      b.RegisterType<BcCollect>();
      b.RegisterType<Parler>();
      b.RegisterType<YtContainerRunner>();
      b.RegisterType<RumbleWeb>();
      b.RegisterType<RumbleCollect>();
      

      b.Register(_ => pipeAppCtx);
      b.RegisterType<PipeCtx>().WithKeyedParam(DataStoreType.Pipe, Typ.Of<ISimpleFileStore>()).As<IPipeCtx>();

      return b;
    }

    public static IRegistrationBuilder<TLimit, TReflectionActivatorData, TStyle>
      WithKeyedParam<TLimit, TReflectionActivatorData, TStyle, TKey, TParam>(
        this IRegistrationBuilder<TLimit, TReflectionActivatorData, TStyle> registration, TKey key, Of<TParam> param)
      where TReflectionActivatorData : ReflectionActivatorData where TKey : Enum =>
      registration.WithParameter(
        (pi, _) => pi.ParameterType == typeof(TParam),
        (_, ctx) => ctx.ResolveKeyed<TParam>(key));

    public static Task<IContainerGroup> SeqGroup(this IAzure azure, SeqCfg seqCfg, PipeAzureCfg azureCfg) =>
      azure.ContainerGroups.GetByResourceGroupAsync(azureCfg.ResourceGroup, seqCfg.ContainerGroupName);

    public static ISimpleFileStore DataStore(this AppCfg cfg, ILogger log, StringPath path) =>
      new AzureBlobFileStore(cfg.Storage.DataStorageCs, path, log);

    public static bool IsProd(this RootCfg root) => root.Env?.ToLowerInvariant() == "prod";
  }

  public record CliEntry(string[] Args);
}