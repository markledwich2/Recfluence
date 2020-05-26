﻿using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Autofac;
using Autofac.Builder;
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
using YtReader.Db;
using YtReader.Search;
using YtReader.Store;
using YtReader.Yt;

namespace YtReader {
  public static class Setup {
    public static string AppName = "YouTubeNetworks";

    public static FPath SolutionDir     => typeof(Setup).LocalAssemblyPath().ParentWithFile("YtNetworks.sln");
    public static FPath SolutionDataDir => typeof(Setup).LocalAssemblyPath().DirOfParent("Data");
    public static FPath LocalDataDir    => "Data".AsPath().InAppData(AppName);

    public static Logger CreateTestLogger() =>
      new LoggerConfiguration()
        .WriteTo.Seq("http://localhost:5341")
        .WriteTo.Console()
        .CreateLogger();

    public static ILogger ConsoleLogger(LogEventLevel level = LogEventLevel.Information) =>
      new LoggerConfiguration()
        .WriteTo.Console(level).CreateLogger();

    public static async Task<Logger> CreateLogger(string env, string app, VersionInfo version, AppCfg cfg = null) {
      var c = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information);

      if (cfg?.AppInsightsKey != null)
        c.WriteTo.ApplicationInsights(new TelemetryConfiguration(cfg.AppInsightsKey), TelemetryConverter.Traces, LogEventLevel.Debug);

      if (cfg != null)
        c = await c.WriteToSeqAndStartIfNeeded(cfg);


      var log = c.YtEnrich(env, app, version.Version)
        .MinimumLevel.Debug()
        .CreateLogger();

      Log.Logger = log;
      return log;
    }

    public static LoggerConfiguration YtEnrich(this LoggerConfiguration logCfg, string env, string app, SemVersion version) =>
      logCfg.Enrich.With(
        new PropertyEnricher("App", app),
        new PropertyEnricher("Env", env),
        new PropertyEnricher("Machine", Environment.MachineName),
        new PropertyEnricher("Version", version)
      );

    static async Task<LoggerConfiguration> WriteToSeqAndStartIfNeeded(this LoggerConfiguration loggerCfg, AppCfg cfg) {
      var seqCfg = cfg?.Seq;
      if (seqCfg?.SeqUrl == null) return loggerCfg;
      var resCfg = loggerCfg.WriteTo.Seq(seqCfg.SeqUrl.OriginalString, LogEventLevel.Debug);
      await new SeqHost(seqCfg, cfg.Pipe.Azure).StartSeqIfNeeded();
      return resCfg;
    }

    /// <summary>Will pass root & app config to pipes from the calling process. Only configuration that should come from the
    ///   caller is passed. e.g. all root cfg's and a few app ones</summary>
    /// <param name="rootCfg"></param>
    /// <param name="appCfg"></param>
    /// <returns></returns>
    public static (string name, string value)[] PipeEnv(RootCfg rootCfg, AppCfg appCfg) =>
      new[] {
        (nameof(RootCfg.Env), rootCfg.Env),
        (nameof(RootCfg.AppStoreCs), rootCfg.AppStoreCs)
      };

    /// <summary>Loads application configuration, and sets global config for .net</summary>
    public static async Task<(AppCfg App, RootCfg Root, VersionInfo Version)> LoadCfg(string basePath = null, ILogger rootLogger = null) {
      rootLogger ??= Log.Logger ?? Logger.None;
      basePath ??= Environment.CurrentDirectory;
      var cfgRoot = new ConfigurationBuilder()
        .SetBasePath(basePath)
        .AddEnvironmentVariables()
        .AddJsonFile("local.rootcfg.json", true)
        .Build().Get<RootCfg>();

      if (cfgRoot.AppStoreCs == null)
        throw new InvalidOperationException("AppStoreCs not provided in local.rootcfg.json or environment variables");

      var versionProvider = new VersionInfoProvider(rootLogger, cfgRoot);
      var version = await versionProvider.Version();

      var envLower = cfgRoot.Env.ToLowerInvariant();
      var secretStore = new AzureBlobFileStore(cfgRoot.AppStoreCs, "cfg", Logger.None);
      var secretNames = cfgRoot.IsProd() ? new[] {envLower} : new[] {"dev", version.Version.Prerelease};
      var secrets = new List<string>();
      foreach (var name in secretNames) {
        var fileName = $"{name}.appcfg.json";
        if (await secretStore.Info(fileName) == null) continue;
        secrets.Add((await secretStore.Load(fileName)).AsString());
      }

      if (secrets == null) throw new InvalidOperationException("can't find secrets cfg file");

      var cfg = new ConfigurationBuilder()
        .SetBasePath(basePath)
        .AddJsonFile("default.appcfg.json")
        .AddJsonFile($"{cfgRoot.Env}.appcfg.json", true);


      foreach (var s in secrets) cfg.AddJsonStream(s.AsStream());

      var builtCfg = cfg.AddJsonFile("local.appcfg.json", true)
        .AddEnvironmentVariables()
        .Build();

      var appCfg = builtCfg.Get<AppCfg>();


      PostLoadConfiguration(appCfg, cfgRoot, secrets.ToArray(), version.Version);

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

    static void PostLoadConfiguration(AppCfg appCfg, RootCfg cfgRoot, string[] secrets, SemVersion version) {
      appCfg.Snowflake.Db = appCfg.Snowflake.DbName(version);
      appCfg.Elastic.IndexPrefix = EsIndex.IndexPrefix(version);

      // override pipe cfg with equivalent global cfg
      appCfg.Pipe.Store = new PipeAppStorageCfg {
        Cs = appCfg.Storage.DataStorageCs,
        Path = appCfg.Storage.PipePath
      };

      // by default, backup to the app/root storage location
      appCfg.Storage.BackupCs ??= cfgRoot.AppStoreCs;


      // the generic config doesn't know how to load arbitrary json objects. perform the load from secrets manually
      var creds = new JObject();
      foreach (var s in secrets) {
        var credJson = s.ParseJToken().SelectToken("sheets.credJson") as JObject;
        if (credJson != null)
          creds.Merge(credJson);
      }
      appCfg.Sheets.CredJson = creds;

      appCfg.SyncDb.Tables = appCfg.SyncDb.Tables.JsonClone();
      foreach (var table in appCfg.SyncDb.Tables)
        if (table.TsCol == null && table.SyncType != SyncType.Full)
          table.Cols.Add(new SyncColCfg {Name = appCfg.SyncDb.DefaultTsCol, Ts = true});

      // merge default properties from the pipe config
      appCfg.Dataform.Container = appCfg.Pipe.Default.Container.JsonMerge(appCfg.Dataform.Container);
    }

    static IReadOnlyCollection<ValidationResult> Validate(object cfgObject) =>
      new DataAnnotationsValidator().TryValidateObjectRecursive(cfgObject).results;

    public static PipeAppCtx PipeAppCtxEmptyScope(RootCfg root, AppCfg appCfg) =>
      new PipeAppCtx(new ContainerBuilder().Build().BeginLifetimeScope(), typeof(YtCollector)) {
        EnvironmentVariables = PipeEnv(root, appCfg)
      };

    public static ILifetimeScope MainScope(RootCfg rootCfg, AppCfg cfg, PipeAppCtx pipeAppCtx, VersionInfo version, ILogger log) {
      var scope = new ContainerBuilder().ConfigureScope(rootCfg, cfg, pipeAppCtx, version, log)
        .Build().BeginLifetimeScope();
      pipeAppCtx.Scope = scope;
      return scope;
    }

    public static ContainerBuilder ConfigureScope(this ContainerBuilder b, RootCfg rootCfg, AppCfg cfg, PipeAppCtx pipeAppCtx, VersionInfo version,
      ILogger log) {
      var containerCfg = cfg.Pipe.Default.Container;

      b.Register(_ => version);
      b.Register(_ => version.Version);
      b.Register(_ => log);
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => rootCfg).SingleInstance();
      b.Register(_ => cfg.Pipe).SingleInstance();
      b.Register(_ => cfg.Pipe.Azure).SingleInstance();
      b.Register(_ => containerCfg).SingleInstance();
      b.Register(_ => cfg.Elastic).SingleInstance();
      b.Register(_ => cfg.Snowflake).SingleInstance();
      b.Register(_ => cfg.Warehouse).SingleInstance();
      b.Register(_ => cfg.Storage).SingleInstance();
      b.Register(_ => cfg.Cleaner).SingleInstance();
      b.Register(_ => cfg.Env).SingleInstance();
      b.Register(_ => cfg.Updater).SingleInstance();
      b.Register(_ => cfg.Results).SingleInstance();
      b.Register(_ => cfg.Dataform).SingleInstance();
      b.Register(_ => cfg.Seq).SingleInstance();


      b.RegisterType<SnowflakeConnectionProvider>().SingleInstance();
      b.Register(_ => cfg.Pipe.Azure.GetAzure()).SingleInstance();

      b.RegisterType<YtStores>().SingleInstance();
      foreach (var storeType in EnumExtensions.Values<DataStoreType>())
        b.Register(_ => _.Resolve<YtStores>().Store(storeType)).Keyed<ISimpleFileStore>(storeType).SingleInstance();

      b.RegisterType<YtClient>();
      b.Register(_ => new ElasticClient(cfg.Elastic.ElasticConnectionSettings())).SingleInstance();
      b.RegisterType<YtStore>().WithKeyedParam(DataStoreType.Db, Typ.Of<ISimpleFileStore>()).SingleInstance();
      b.RegisterType<YtResults>().WithKeyedParam(DataStoreType.Results, Typ.Of<ISimpleFileStore>()).SingleInstance();
      b.RegisterType<StoreUpgrader>().WithKeyedParam(DataStoreType.Db, Typ.Of<ISimpleFileStore>()).SingleInstance();
      b.RegisterType<YtStage>().WithKeyedParam(DataStoreType.Db, Typ.Of<ISimpleFileStore>()).SingleInstance();

      b.RegisterType<YtSearch>().SingleInstance();
      b.RegisterType<YtCollector>().SingleInstance();
      b.RegisterType<WarehouseCreator>().SingleInstance();
      b.RegisterType<YtBackup>().SingleInstance();
      b.RegisterType<AzureCleaner>().SingleInstance();
      b.RegisterType<YtUpdater>(); // new instance so it can have a unique runId in its contructor
      b.Register(_ => new RegistryClient(containerCfg.Registry, containerCfg.RegistryCreds));
      b.RegisterType<BranchEnvCreator>().SingleInstance();
      b.RegisterType<YtDataform>().SingleInstance();
      b.RegisterType<AzureContainers>().SingleInstance();
      b.RegisterType<LocalPipeWorker>().SingleInstance();
      b.RegisterType<SeqHost>();

      b.Register(_ => pipeAppCtx);
      b.RegisterType<PipeCtx>().WithKeyedParam(DataStoreType.Pipe, Typ.Of<ISimpleFileStore>()).As<IPipeCtx>().SingleInstance();

      return b;
    }

    public static IRegistrationBuilder<TLimit, TReflectionActivatorData, TStyle>
      WithKeyedParam<TLimit, TReflectionActivatorData, TStyle, TKey, TParam>(
        this IRegistrationBuilder<TLimit, TReflectionActivatorData, TStyle> registration, TKey key, Of<TParam> param)
      where TReflectionActivatorData : ReflectionActivatorData where TKey : Enum =>
      registration.WithParameter(
        (pi, ctx) => pi.ParameterType == typeof(TParam),
        (pi, ctx) => ctx.ResolveKeyed<TParam>(key));

    public static Task<IContainerGroup> SeqGroup(this IAzure azure, SeqCfg seqCfg, PipeAzureCfg azureCfg) =>
      azure.ContainerGroups.GetByResourceGroupAsync(azureCfg.ResourceGroup, seqCfg.ContainerGroupName);

    public static ISimpleFileStore DataStore(this AppCfg cfg, ILogger log, StringPath path) =>
      new AzureBlobFileStore(cfg.Storage.DataStorageCs, path, log);

    public static YtClient YtClient(this AppCfg cfg, ILogger log) => new YtClient(cfg.YTApiKeys, log);
    public static bool IsProd(this RootCfg root) => root.Env?.ToLowerInvariant() == "prod";
  }
}