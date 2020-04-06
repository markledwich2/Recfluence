using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Autofac;
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
using SysExtensions.Build;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
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

    public static Dictionary<string, string> ContainerEnv(this RootCfg rootCfg) =>
      new Dictionary<string, string> {
        {nameof(RootCfg.Env), rootCfg.Env},
        {nameof(RootCfg.AppCfgSas), rootCfg.AppCfgSas.ToString()}
      };

    static        string _env;
    public static string Env => _env ??= GetEnv("Env") ?? "dev";

    static string GetEnv(string name) =>
      Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.User)
      ?? Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);

    public static async Task<(AppCfg App, RootCfg Root)> LoadCfg2() {
      var cfgRoot = new ConfigurationBuilder()
        .SetBasePath(Environment.CurrentDirectory)
        .AddEnvironmentVariables()
        .AddJsonFile("local.rootcfg.json", true)
        .Build().Get<RootCfg>();

      if (cfgRoot.AppCfgSas == null)
        throw new InvalidOperationException("AppCfgSas not provided in local.rootcfg.json or environment variables");

      var envLower = cfgRoot.Env.ToLowerInvariant();
      var blob = new AzureBlobFileStore(cfgRoot.AppCfgSas, Logger.None);
      var secrets = (await blob.Load($"{envLower}.appcfg.json")).AsString();

      var cfg = new ConfigurationBuilder()
        .SetBasePath(Environment.CurrentDirectory)
        .AddJsonFile("default.appcfg.json")
        .AddJsonFile($"{Env}.appcfg.json", true)
        .AddJsonStream(secrets.AsStream())
        .AddJsonFile("local.appcfg.json", true)
        .AddEnvironmentVariables().Build();
      var appCfg = cfg.Get<AppCfg>();
      appCfg.Sheets.CredJson = secrets.ToJObject().SelectToken("sheets.credJson") as JObject;
      appCfg.Pipe = GetPipeAppCfg(appCfg);
      return (appCfg, cfgRoot);
    }

    static PipeAppCfg GetPipeAppCfg(AppCfg cfg) {
      var semver = GitVersionInfo.Semver(typeof(Setup));

      var pipe = cfg.Pipe.JsonClone();
      pipe.Default.Container.Tag ??= semver;
      foreach (var p in pipe.Pipes) p.Container.Tag ??= semver;

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

    class ScopeHolder {
      public IComponentContext Scope { get; set; }
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
      b.RegisterType<YtDataUpdater>(); // this will resolve IPipeCtx
      var scope = b.Build().BeginLifetimeScope();
      scopeHolder.Scope = scope;
      return scope;
    }

    public static IPipeCtx ResolvePipeCtx(this ILifetimeScope scope) => scope.Resolve<Func<IPipeCtx>>()();
  }

  public static class ChannelConfigExtensions {
    public static ISimpleFileStore DataStore(this AppCfg cfg, ILogger log, StringPath path = null) =>
      new AzureBlobFileStore(cfg.Storage.DataStorageCs, path ?? cfg.Storage.DbPath, log);

    public static YtClient YtClient(this AppCfg cfg, ILogger log) => new YtClient(cfg.YTApiKeys, log);

    public static YtStore YtStore(this AppCfg cfg, ILogger log) {
      var ytStore = new YtStore(cfg.DataStore(log, cfg.Storage.DbPath), log);
      return ytStore;
    }
  }
}