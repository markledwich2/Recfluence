using System;
using System.Threading.Tasks;
using Autofac;
using Autofac.Extensions.DependencyInjection.AzureFunctions;
using Humanizer.Localisation;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Azure.WebJobs;
using Serilog;
using Serilog.Sinks.ApplicationInsights.Sinks.ApplicationInsights.TelemetryConverters;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtFunctions;
using YtReader;

[assembly: FunctionsStartup(typeof(Startup))]

namespace YtFunctions {
  public class Startup : FunctionsStartup {
    public override void Configure(IFunctionsHostBuilder builder) {
      var cfg = Setup.LoadCfg().GetAwaiter().GetResult();
      var log = Logger(cfg.Root, cfg.App).GetAwaiter().GetResult();
      var appCtx = Setup.PipeAppCtxEmptyScope(cfg.Root);
      builder.UseAutofacServiceProviderFactory(c => {
        c.ConfigureBase(cfg.Root, cfg.App, appCtx, log);
        c.RegisterType<YtFunctions>();
        c.RegisterType<YtData>();
      });
    }

    static async Task<ILogger> Logger(RootCfg root, AppCfg cfg) {
      var logCfg = new LoggerConfiguration();
      if (cfg.AppInsightsKey.HasValue())
        logCfg = logCfg.WriteTo.ApplicationInsights(cfg.AppInsightsKey, new TraceTelemetryConverter());
      if (cfg.SeqUrl != null)
        logCfg = logCfg.WriteTo.Seq(cfg.SeqUrl.OriginalString);
      logCfg = logCfg.YtEnrich(root.Env, nameof(YtFunctions), await Setup.GetVersion());
      var startSeqTask = Setup.StartSeqIfNeeded(cfg);
      return logCfg.CreateLogger();
    }
  }

  public static class FunctionHelper {
    public static async Task Run(this ExecutionContext ctx, ILogger log, Func<ILogger, Task> func) =>
      await ctx.Run<object>(log, l => {
        func(l);
        return null;
      });

    /// <summary>Executes a function with contextual logging</summary>
    public static async Task<T> Run<T>(this ExecutionContext ctx, ILogger log, Func<ILogger, Task<T>> func) {
      var funcLog = log.ForContext("Function", ctx.FunctionName);
      try {
        funcLog.Debug("{Function} - started", ctx.FunctionName);
        var res = await func(funcLog).WithDuration();
        funcLog.Debug("{Function} - completed in {Duration}", ctx.FunctionName, res.Duration.HumanizeShort(2, TimeUnit.Millisecond));
        return res.Result;
      }
      catch (Exception ex) {
        funcLog.Error(ex, "{Function} - unhandled exception: {Message}", ctx.FunctionName, ex.Message);
        throw;
      }
    }
  }
}