using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Autofac;
using Autofac.Builder;
using Autofac.Util;
using Elasticsearch.Net;
using Humanizer;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Extensions.Configuration;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Nest;
using Newtonsoft.Json.Linq;
using Semver;
using Serilog;
using Serilog.Core;
using Serilog.Core.Enrichers;
using Serilog.Events;
using SysExtensions;
using SysExtensions.Build;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Search;
using YtReader.Store;
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
        .WriteTo.Console()
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

    public static Task<SemVersion> GetVersion() => Version.GetOrCreate();

    static string GetEnv(string name) =>
      Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.User)
      ?? Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);

    /// <summary>Loads application configuration, and sets global config for .net</summary>
    public static async Task<(AppCfg App, RootCfg Root)> LoadCfg(string basePath = null, ILogger rootLogger = null) {
      rootLogger ??= Log.Logger ?? Logger.None;
      basePath ??= Environment.CurrentDirectory;
      var cfgRoot = new ConfigurationBuilder()
        .SetBasePath(basePath)
        .AddEnvironmentVariables()
        .AddJsonFile("local.rootcfg.json", true)
        .Build().Get<RootCfg>();

      if (cfgRoot.AppStoreCs == null)
        throw new InvalidOperationException("AppStoreCs not provided in local.rootcfg.json or environment variables");

      var envLower = cfgRoot.Env.ToLowerInvariant();
      var secretStore = new AzureBlobFileStore(cfgRoot.AppStoreCs, "cfg", Logger.None);
      var secrets = (await secretStore.Load($"{envLower}.appcfg.json")).AsString();

      var cfg = new ConfigurationBuilder()
        .SetBasePath(basePath)
        .AddJsonFile("default.appcfg.json")
        .AddJsonFile($"{cfgRoot.Env}.appcfg.json", true)
        .AddJsonStream(secrets.AsStream())
        .AddJsonFile("local.appcfg.json", true)
        .AddEnvironmentVariables().Build();
      var appCfg = cfg.Get<AppCfg>();

      await SetFallbacks(appCfg, cfgRoot, secrets);

      var validation = Validate(appCfg);
      if (validation.Any()) {
        rootLogger.Error("Validation errors in app cfg {Errors}", validation);
        throw new InvalidOperationException($"validation errors with app cfg {validation.Join(", ")}");
      }

      // as recommended here https://github.com/Azure/azure-storage-net-data-movement, also should be good for performance of our other Http interactions
      ServicePointManager.Expect100Continue = false;
      ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;

      return (appCfg, cfgRoot);
    }

    static async Task SetFallbacks(AppCfg appCfg, RootCfg cfgRoot, string secrets) {
      appCfg.SeqHost.SeqUrl ??= appCfg.SeqUrl;

      // by default, backup to the app/root storage location
      appCfg.Storage.BackupCs ??= cfgRoot.AppStoreCs;

      if (appCfg.Sheets != null)
        appCfg.Sheets.CredJson = secrets.ParseJToken().SelectToken("sheets.credJson") as JObject;
      appCfg.Pipe = await PipeAppCfg(appCfg);


      appCfg.SyncDb.Tables = appCfg.SyncDb.Tables.JsonClone();
      foreach (var table in appCfg.SyncDb.Tables)
        if (table.TsCol == null && table.SyncType != SyncType.Full)
          table.Cols.Add(new SyncColCfg {Name = appCfg.SyncDb.DefaultTsCol, Ts = true});

      // merge default properties from the pipe config
      appCfg.Dataform.Container = appCfg.Pipe.Default.Container.JsonMerge(appCfg.Dataform.Container);
    }

    public static IReadOnlyCollection<ValidationResult> Validate(object cfgObject) {
      var context = new ValidationContext(cfgObject, null, null);
      var results = new List<ValidationResult>();
      Validator.TryValidateObject(cfgObject, context, results, true);
      return results;
    }

    public static PipeAppCtx PipeAppCtxEmptyScope(RootCfg root, AppCfg appCfg) =>
      new PipeAppCtx(new ContainerBuilder().Build().BeginLifetimeScope(), typeof(YtDataCollector)) {
        EnvironmentVariables = PipeEnv(root, appCfg)
      };

    static async Task<PipeAppCfg> PipeAppCfg(AppCfg cfg) {
      var semver = await GetVersion();

      var pipe = cfg.Pipe.JsonClone();
      pipe.Default.Container.Tag ??= semver.ToString();
      foreach (var p in pipe.Pipes) p.Container.Tag ??= semver.ToString();

      // override pipe cfg with equivalent global cfg
      pipe.Store = new PipeAppStorageCfg {
        Cs = cfg.Storage.DataStorageCs,
        Path = cfg.Storage.PipePath
      };
      pipe.Azure = new PipeAzureCfg {
        ResourceGroup = cfg.ResourceGroup,
        ServicePrincipal = cfg.ServicePrincipal,
        SubscriptionId = cfg.SubscriptionId
      };
      return pipe;
    }

    public static ILifetimeScope BaseScope(RootCfg rootCfg, AppCfg cfg, PipeAppCtx pipeAppCtx, ILogger log) {
      var scope = new ContainerBuilder().ConfigureBase(rootCfg, cfg, pipeAppCtx, log)
        .Build().BeginLifetimeScope();
      pipeAppCtx.Scope = scope;
      return scope;
    }

    public static ContainerBuilder ConfigureBase(this ContainerBuilder b, RootCfg rootCfg, AppCfg cfg, PipeAppCtx pipeAppCtx, ILogger log) {
      b.Register(_ => log).SingleInstance();
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => rootCfg).SingleInstance();
      b.Register(_ => cfg.Pipe).SingleInstance();
      b.Register(_ => cfg.Elastic).SingleInstance();
      b.Register(_ => cfg.Snowflake).SingleInstance();
      b.Register(_ => cfg.Warehouse).SingleInstance();
      b.Register(_ => cfg.Storage).SingleInstance();

      b.Register<Func<Task<DbConnection>>>(_ => async () => await cfg.Snowflake.OpenConnection());
      b.Register(_ => new ConnectionProvider(async () => await cfg.Snowflake.OpenConnection(), cfg.Snowflake.Db)).SingleInstance();
      b.Register(_ => cfg.Pipe.Azure.GetAzure()).SingleInstance();

      b.RegisterType<YtStores>().SingleInstance();

      foreach (var storeType in EnumExtensions.Values<DataStoreType>())
        b.Register(_ => _.Resolve<YtStores>().Store(storeType)).Keyed<ISimpleFileStore>(storeType).SingleInstance();

      b.RegisterType<YtClient>();

      b.Register(_ => {
          var esMappngTypes = typeof(EsIndex).Assembly.GetLoadableTypes()
            .Select(t => (t, es: t.GetCustomAttribute<ElasticsearchTypeAttribute>(), table: t.GetCustomAttribute<TableAttribute>()))
            .Where(t => t.es != null)
            .ToArray();
          if (esMappngTypes.Any(t => t.table == null))
            throw new InvalidOperationException("All document types must have a mapping to and index. Add a Table(\"Index name\") attribute.");
          var clrMap = esMappngTypes.Select(t => new ClrTypeMapping(t.t) {IdPropertyName = t.es.IdProperty, IndexName = t.table.Name});
          var cs = new ConnectionSettings(
            cfg.Elastic.CloudId,
            new BasicAuthenticationCredentials(cfg.Elastic.Creds.Name, cfg.Elastic.Creds.Secret)
          ).DefaultMappingFor(clrMap);
          return new ElasticClient(cs);
        }
      ).SingleInstance();

      b.RegisterType<YtStore>().WithKeyedParam(DataStoreType.Db, Typ.Of<ISimpleFileStore>()).SingleInstance();
      b.RegisterType<YtResults>().WithKeyedParam(DataStoreType.Results, Typ.Of<ISimpleFileStore>()).SingleInstance();
      b.RegisterType<StoreUpgrader>().WithKeyedParam(DataStoreType.Db, Typ.Of<ISimpleFileStore>()).SingleInstance();
      b.RegisterType<YtSearch>().SingleInstance();
      b.RegisterType<YtDataCollector>().SingleInstance();
      b.RegisterType<WarehouseUpdater>().WithKeyedParam(DataStoreType.Db, Typ.Of<ISimpleFileStore>()).SingleInstance();
      b.RegisterType<YtBackup>().SingleInstance();

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

    public static Task<IContainerGroup> SeqGroup(this IAzure azure, AppCfg cfg) =>
      azure.ContainerGroups.GetByResourceGroupAsync(cfg.ResourceGroup, cfg.SeqHost.ContainerGroupName);

    public static ISimpleFileStore DataStore(this AppCfg cfg, ILogger log, StringPath path) =>
      new AzureBlobFileStore(cfg.Storage.DataStorageCs, path, log);

    public static YtClient YtClient(this AppCfg cfg, ILogger log) => new YtClient(cfg.YTApiKeys, log);
    public static bool IsProd(this RootCfg root) => root.Env.ToLowerInvariant() == "prod";
  }
}