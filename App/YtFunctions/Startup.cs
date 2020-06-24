using System;
using System.Threading.Tasks;
using Autofac;
using Autofac.Extensions.DependencyInjection.AzureFunctions;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Azure.WebJobs;
using Mutuo.Etl.Pipe;
using Semver;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.ApplicationInsights.Sinks.ApplicationInsights.TelemetryConverters;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtFunctions;
using YtReader;

[assembly: FunctionsStartup(typeof(Startup))]

namespace YtFunctions {
  public class Startup : FunctionsStartup {
    public override void Configure(IFunctionsHostBuilder builder) =>
      builder.UseAutofacServiceProviderFactory(c => {
        c.Register(_ => new Defer<FuncCtx, ExecutionContext>(FuncCtx.LoadCtx)).SingleInstance();
        c.RegisterType<ApiBackend>();
        c.RegisterType<ApiRecfluence>();
      });
  }

  public class FuncCtx {
    public FuncCtx(ILogger log, PipeAppCtx pipeCtx, RootCfg root, AppCfg cfg, ILifetimeScope scope) {
      Log = log;
      PipeCtx = pipeCtx;
      Root = root;
      Cfg = cfg;
      Scope = scope;
    }

    public ILogger        Log     { get; }
    public PipeAppCtx     PipeCtx { get; }
    public RootCfg        Root    { get; }
    public AppCfg         Cfg     { get; }
    public ILifetimeScope Scope   { get; }

    public FuncCtx WithLog(ILogger log) => new FuncCtx(log, PipeCtx, Root, Cfg, Scope);

    public T Resolve<T>() => Scope.Resolve<T>();

    public static async Task<FuncCtx> LoadCtx(ExecutionContext exec) {
      var cfgDir = Setup.SolutionDir == null ? exec.FunctionAppDirectory : Setup.SolutionDir.Combine("YtCli").FullPath;
      var (app, root, version) = await Setup.LoadCfg(cfgDir);
      var log = Logger(root, app, version.Version);
      var appCtx = Setup.PipeAppCtxEmptyScope(root, app, version.Version);
      var scope = Setup.MainScope(root, app, appCtx, version, log);
      return new FuncCtx(scope.Resolve<ILogger>(), appCtx, root, app, scope);
    }

    static ILogger Logger(RootCfg root, AppCfg cfg, SemVersion version) {
      var logCfg = new LoggerConfiguration();
      logCfg.WriteTo.Console(LogEventLevel.Information);
      if (cfg.AppInsightsKey.HasValue())
        logCfg = logCfg.WriteTo.ApplicationInsights(cfg.AppInsightsKey, new TraceTelemetryConverter());
      if (cfg.Seq.SeqUrl != null)
        logCfg = logCfg.WriteTo.Seq(cfg.Seq.SeqUrl.OriginalString, LogEventLevel.Debug);
      logCfg = logCfg.YtEnrich(root.Env, nameof(YtFunctions), version);
      return logCfg.MinimumLevel.Debug().CreateLogger();
    }
  }

  public static class FunctionHelper {
    public static async Task Run(this Defer<FuncCtx, ExecutionContext> ctx, ExecutionContext exec, Func<FuncCtx, Task> action) =>
      await Run<object>(ctx, exec, async c => {
        await action(c);
        return null;
      });

    /// <summary>Executes a function with error handling and contextual logging</summary>
    public static async Task<T> Run<T>(this Defer<FuncCtx, ExecutionContext> ctx, ExecutionContext exec, Func<FuncCtx, Task<T>> func) {
      var c = await ctx.GetOrCreate(exec);
      c = c.WithLog(c.Log.ForContext("Function", exec.FunctionName));
      try {
        c.Log.Information("{Function} function - started", exec.FunctionName);
        var res = await func(c).WithDuration();
        c.Log.Information("{Function} function - completed in {Duration}", exec.FunctionName, res.Duration.HumanizeShort());
        return res.Result;
      }
      catch (Exception ex) {
        c.Log.Error(ex, "{Function} function - unhandled exception: {Message}", exec.FunctionName, ex.Message);
        throw;
      }
    }
  }
}