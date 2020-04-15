﻿using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Autofac.Builder;
using Humanizer;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Extensions.Configuration;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
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

    public static (string name, string value)[] PipeEnv(this RootCfg rootCfg) =>
      new[] {
        (nameof(RootCfg.Env), rootCfg.Env),
        (nameof(RootCfg.AppCfgSas), rootCfg.AppCfgSas.ToString())
      };

    public static Task<SemVersion> GetVersion() => Version.GetOrCreate();

    static string GetEnv(string name) =>
      Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.User)
      ?? Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);

    public static async Task<(AppCfg App, RootCfg Root)> LoadCfg(string basePath = null, ILogger rootLogger = null) {
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
      appCfg.Pipe = await PipeAppCfg(appCfg);


      appCfg.SyncDb.Tables = appCfg.SyncDb.Tables.JsonClone();
      foreach (var table in appCfg.SyncDb.Tables)
        if (table.TsCol == null && table.SyncType != SyncType.Full)
          table.Cols.Add(new SyncColCfg {Name = appCfg.SyncDb.DefaultTsCol, Ts = true});

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

    public static PipeAppCtx PipeAppCtxEmptyScope(RootCfg root) =>
      new PipeAppCtx {
        Assemblies = new[] {typeof(YtDataUpdater).Assembly},
        EnvironmentVariables = root.PipeEnv(),
        Scope = new ContainerBuilder().Build().BeginLifetimeScope()
      };

    static async Task<PipeAppCfg> PipeAppCfg(AppCfg cfg) {
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

      b.Register<Func<Task<DbConnection>>>(_ => async () => await cfg.Snowflake.OpenConnection());
      b.RegisterType<AppDb>().SingleInstance();
      b.Register(_ => cfg.Pipe.Azure.GetAzure()).SingleInstance();
      b.Register(_ => cfg.DataStore(log, cfg.Storage.ResultsPath)).Keyed<ISimpleFileStore>(StoreType.Results).SingleInstance();
      b.Register(_ => cfg.DataStore(log, cfg.Storage.DbPath)).Keyed<ISimpleFileStore>(StoreType.Db).SingleInstance();
      b.Register(_ => cfg.DataStore(log, cfg.Storage.PipePath)).Keyed<ISimpleFileStore>(StoreType.Pipe).SingleInstance();
      b.Register(_ => cfg.DataStore(log, cfg.Storage.PrivatePath)).Keyed<ISimpleFileStore>(StoreType.Private).SingleInstance();


      b.RegisterType<YtClient>();
      b.RegisterType<YtStore>().WithKeyedParam(StoreType.Db, Typ.Of<ISimpleFileStore>());
      b.RegisterType<YtResults>().WithKeyedParam(StoreType.Results, Typ.Of<ISimpleFileStore>());
      b.RegisterType<YtSearch>();
      b.RegisterType<YtDataUpdater>();

      b.Register(_ => pipeAppCtx);
      b.RegisterType<PipeCtx>().WithKeyedParam(StoreType.Pipe, Typ.Of<ISimpleFileStore>()).As<IPipeCtx>().SingleInstance();

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

    public static ISimpleFileStore DataStore(this AppCfg cfg, ILogger log, StringPath path = null) =>
      new AzureBlobFileStore(cfg.Storage.DataStorageCs, path ?? cfg.Storage.DbPath, log);

    public static YtClient YtClient(this AppCfg cfg, ILogger log) => new YtClient(cfg.YTApiKeys, log);
    public static bool IsProd(this RootCfg root) => root.Env.ToLowerInvariant() == "prod";
  }

  public enum StoreType {
    Pipe,
    Db,
    Results,
    Private
  }
}