using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Autofac;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Mutuo.Etl.Pipe;
using Seq.Api;
using Serilog;
using Serilog.Sinks.ILogger;
using SysExtensions.Build;
using SysExtensions.Net;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using ILogger = Serilog.ILogger;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

namespace YtFunctions {
  public static class YtFunctions {
    static async Task<ILogger> Logger(RootCfg root, ExecutionContext context, IMSLogger funcLogger, AppCfg cfg, bool dontStartSeq = false) {
      var logCfg = new LoggerConfiguration();
      if (cfg.AppInsightsKey.HasValue())
        logCfg = logCfg.WriteTo.ApplicationInsights(new TelemetryClient {InstrumentationKey = cfg.AppInsightsKey}, TelemetryConverter.Traces);
      if (funcLogger != null)
        logCfg = logCfg.WriteTo.ILogger(funcLogger);
      if (cfg.SeqUrl != null)
        logCfg = logCfg.WriteTo.Seq(cfg.SeqUrl.OriginalString);

      funcLogger.LogDebug($"Initializing logger {root.Env} {context.FunctionDirectory}");

      logCfg = logCfg.YtEnrich(root.Env, context.FunctionName, await Setup.GetVersion());

      if (!dontStartSeq)
        await Setup.StartSeqIfNeeded(cfg);
      return logCfg.CreateLogger();
    }

    static async Task<(AppCfg Cfg, ILogger Log, RootCfg Root, ILifetimeScope scope)> Init(ExecutionContext context, IMSLogger funcLogger = null,
      bool dontStartSeq = false) {
      var consoleLogger = funcLogger != null ? new LoggerConfiguration().WriteTo.ILogger(funcLogger).CreateLogger() : Setup.ConsoleLogger();
      consoleLogger.Debug("about to log config");
      var (app, root) = await Setup.LoadCfg2(context.FunctionAppDirectory, consoleLogger).WithWrappedException("unable to load cfg");
      var log = await Logger(root, context, funcLogger, app, dontStartSeq).WithWrappedException("unable to load logger");
      var scope = Setup.BaseScope(root, app, log);
      return (app, log, root, scope);
    }

    [FunctionName("StopIdleSeq_Timer")]
    public static async Task StopIdleSeq_Timer([TimerTrigger("*/15,30,45,0 * * * *")] TimerInfo myTimer, ExecutionContext context, IMSLogger log) =>
      await StopIdleSeq(context, log);

    [FunctionName("StopIdleSeq")]
    public static async Task<HttpResponseMessage> StopIdleSeq([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, ExecutionContext context, IMSLogger funcLogger) =>
      req.AsyncResponse(await StopIdleSeq(context, funcLogger));

    [FunctionName("Version")]
    public static async Task<HttpResponseMessage> Version([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, ExecutionContext context, IMSLogger funcLogger) =>
      req.AsyncResponse($"Runtime version: ${GitVersionInfo.RuntimeSemVer(typeof(YtDataUpdater))}");

    static async Task<string> StopIdleSeq(ExecutionContext context, IMSLogger log) {
      log.LogDebug("StopIdleSeq called");
      var s = await Init(context, log, true);
      if (!s.Root.IsProd()) return LogReason("not prod");
      var azure = s.scope.Resolve<IAzure>();
      var group = await azure.SeqGroup(s.Cfg);
      if (group.State() != ContainerState.Running) return LogReason("seq container not running");
      var seq = new SeqConnection(s.Cfg.SeqUrl.OriginalString);
      var events = await seq.Events.ListAsync(count: 5, filter: s.Cfg.SeqHost.IdleQuery, render: true);
      if (events.Any()) {
        s.Log.Information("{Noun} - recent events exist from '{Query}'", nameof(StopIdleSeq), s.Cfg.SeqHost.IdleQuery);
        return $"recent events exist: {events.Join("\n", e => e.RenderedMessage)}";
      }
      s.Log.Information("{Noun} - no recent events from '{Query}'. Stopping {ContainerGroup}", nameof(StopIdleSeq), s.Cfg.SeqHost.IdleQuery, group.Name);
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
    public static async Task<HttpResponseMessage> Update([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, ExecutionContext context, IMSLogger funcLogger) =>
      req.AsyncResponse(await RunUpdate(funcLogger, context));

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