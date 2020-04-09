using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Autofac;
using Microsoft.ApplicationInsights;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Mutuo.Etl.Pipe;
using Seq.Api;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.ILogger;
using SysExtensions.Build;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using ILogger = Serilog.ILogger;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

namespace YtFunctions {
  public static class YtFunctions {
    static async Task<ILogger> Logger(RootCfg root, ExecutionContext context, AppCfg cfg, bool dontStartSeq = false, IMSLogger funcLogger = null) {
      var logCfg = new LoggerConfiguration();
      if (cfg.AppInsightsKey.HasValue())
        logCfg = logCfg.WriteTo.ApplicationInsights(new TelemetryClient {InstrumentationKey = cfg.AppInsightsKey}, TelemetryConverter.Traces);
      if (funcLogger != null)
        logCfg = logCfg.WriteTo.ILogger(funcLogger);
      if (cfg.SeqUrl != null)
        logCfg = logCfg.WriteTo.Seq(cfg.SeqUrl.OriginalString);
      logCfg = logCfg.YtEnrich(root.Env, context.FunctionName, await Setup.GetVersion());

      if (!dontStartSeq)
        await Setup.StartSeqIfNeeded(cfg);
      return logCfg.CreateLogger();
    }

    static async Task<(AppCfg Cfg, ILogger Log, RootCfg Root, ILifetimeScope scope)> Init(ExecutionContext context, IMSLogger funcLogger = null,
      bool dontStartSeq = false) {
      var consoleLogger = funcLogger != null ? new LoggerConfiguration().WriteTo.ILogger(funcLogger).CreateLogger() : Setup.ConsoleLogger(LogEventLevel.Debug);
      consoleLogger.Debug("about to log config");
      var (app, root) = await Setup.LoadCfg2(context.FunctionAppDirectory, consoleLogger).WithWrappedException("unable to load cfg");
      var log = await Logger(root, context, app, dontStartSeq, funcLogger).WithWrappedException("unable to load logger");
      var scope = Setup.BaseScope(root, app, log);
      return (app, log, root, scope);
    }

    [FunctionName("StopIdleSeq_Timer")]
    public static async Task StopIdleSeq_Timer([TimerTrigger("0 */15 * ? * *")] TimerInfo myTimer, ExecutionContext context, IMSLogger log) =>
      await StopIdleSeqInner(context, log);

    [FunctionName("StopIdleSeq")]
    public static async Task<IActionResult> StopIdleSeq([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequest req, ExecutionContext context, IMSLogger funcLogger) {
      Console.WriteLine($"funcLogger {funcLogger}");
      funcLogger?.LogInformation("StopIdleSeq called");
      return new OkObjectResult(await StopIdleSeqInner(context, funcLogger));
    }

    [FunctionName("Version")]
    public static Task<IActionResult> Version([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequest req, ExecutionContext context, IMSLogger funcLogger) =>
      Task.FromResult((IActionResult) new OkObjectResult(
        $"Runtime version: ${GitVersionInfo.RuntimeSemVer(typeof(YtDataUpdater))},  FunctionAppDirectory={context.FunctionAppDirectory}, funcLogger={funcLogger}"));

    static async Task<string> StopIdleSeqInner(ExecutionContext context, IMSLogger log) {
      var s = await Init(context, log, true);
      if (!s.Root.IsProd()) return LogReason("not prod");
      var azure = s.scope.Resolve<IAzure>();
      var group = await azure.SeqGroup(s.Cfg);
      if (group.State() != ContainerState.Running) return LogReason("seq container not running");
      var seq = new SeqConnection(s.Cfg.SeqUrl.OriginalString);
      var events = await seq.Events.ListAsync(count: 5, filter: s.Cfg.SeqHost.IdleQuery, render: true);
      if (events.Any()) {
        s.Log.Information("{Events} recent events exist from '{Query}'", events.Count, s.Cfg.SeqHost.IdleQuery);
        return $"recent events exist: {events.Join("\n", e => e.RenderedMessage)}";
      }
      s.Log.Information("No recent events from '{Query}'. Stopping {ContainerGroup}", s.Cfg.SeqHost.IdleQuery, group.Name);
      await group.StopAsync();
      return $"stopped group {group.Name}";

      string LogReason(string reason) {
        s.Log.Information("{Noun} - {reason}", nameof(StopIdleSeq), reason);
        return reason;
      }
    }

    [FunctionName("Update_Timer")]
    public static async Task Update_Timer([TimerTrigger("0 0 21 * * *")] TimerInfo myTimer, ExecutionContext context, IMSLogger log) =>
      await RunUpdate(log, context);

    [FunctionName("Update")]
    public static async Task<IActionResult> Update([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, ExecutionContext context, IMSLogger funcLogger) =>
      new OkObjectResult(await RunUpdate(funcLogger, context).WithWrappedException(e => $"Error running update: {e}"));

    static async Task<string> RunUpdate(IMSLogger funcLogger, ExecutionContext context) {
      var s = await Init(context, funcLogger);
      try {
        var pipeCtx = s.scope.ResolvePipeCtx();
        var res = await pipeCtx.RunPipe(nameof(YtDataUpdater.Update), false, s.Log);
        return res.Error
          ? $"Error starting pipe work: {res.ErrorMessage}"
          : $"Started work on containers(s): {res.Containers.Join(", ", c => $"{c.Image} -> {c.Name}")}";
      }
      catch (Exception ex) {
        s.Log.Error("Error starting container to update data {Error}", ex.Message, ex);
        throw;
      }
    }
  }
}