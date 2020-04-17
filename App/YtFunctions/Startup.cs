using System;
using System.Threading.Tasks;
using Autofac;
using Autofac.Extensions.DependencyInjection.AzureFunctions;
using Humanizer.Localisation;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Azure.WebJobs;
using Mutuo.Etl.Pipe;
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
        c.Register(_ => new AsyncLazy<FuncCtx, ExecutionContext>(FuncCtx.LoadCtx)).SingleInstance();
        c.RegisterType<YtFunctions>();
        c.RegisterType<YtData>();
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

    public static async Task<FuncCtx> LoadCtx(ExecutionContext exec) {
      var cfg = await Setup.LoadCfg(exec.FunctionAppDirectory);
      var log = await Logger(cfg.Root, cfg.App);
      var appCtx = Setup.PipeAppCtxEmptyScope(cfg.Root);
      var scope = Setup.BaseScope(cfg.Root, cfg.App, appCtx, log);
      return new FuncCtx(log, appCtx, cfg.Root, cfg.App, scope);
    }

    static async Task<ILogger> Logger(RootCfg root, AppCfg cfg) {
      var logCfg = new LoggerConfiguration();
      logCfg.WriteTo.Console(LogEventLevel.Information);
      if (cfg.AppInsightsKey.HasValue())
        logCfg = logCfg.WriteTo.ApplicationInsights(cfg.AppInsightsKey, new TraceTelemetryConverter());
      if (cfg.SeqUrl != null)
        logCfg = logCfg.WriteTo.Seq(cfg.SeqUrl.OriginalString, LogEventLevel.Debug);
      logCfg = logCfg.YtEnrich(root.Env, nameof(YtFunctions), await Setup.GetVersion());
      var startSeqTask = Setup.StartSeqIfNeeded(cfg);
      return logCfg.CreateLogger();
    }
  }

  public static class FunctionHelper {
    public static async Task Run(this AsyncLazy<FuncCtx, ExecutionContext> ctx, ExecutionContext exec, Func<FuncCtx, Task> action) =>
      await Run<object>(ctx, exec, async c => {
        await action(c);
        return null;
      });

    /// <summary>Executes a function with error handling and contextual logging</summary>
    public static async Task<T> Run<T>(this AsyncLazy<FuncCtx, ExecutionContext> ctx, ExecutionContext exec, Func<FuncCtx, Task<T>> func) {
      var c = await ctx.GetOrCreate(exec);
      c = c.WithLog(c.Log.ForContext("Function", exec.FunctionName));
      try {
        c.Log.Information("{Function} - started", exec.FunctionName);
        var res = await func(c).WithDuration();
        c.Log.Information("{Function} - completed in {Duration}", exec.FunctionName, res.Duration.HumanizeShort(2, TimeUnit.Millisecond));
        return res.Result;
      }
      catch (Exception ex) {
        c.Log.Error(ex, "{Function} - unhandled exception: {Message}", exec.FunctionName, ex.Message);
        throw;
      }
    }
  }
}