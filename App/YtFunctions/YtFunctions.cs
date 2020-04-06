using System;
using System.Net.Http;
using System.Threading.Tasks;
using AngleSharp.Text;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Mutuo.Etl.Pipe;
using Serilog;
using Serilog.Sinks.ILogger;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using YtReader;
using ILogger = Serilog.ILogger;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

namespace YtFunctions {
  public static class YtFunctions {
    static ILogger Logger(IMSLogger funcLogger, string appInsightsKey) {
      var logCfg = new LoggerConfiguration();
      if (appInsightsKey.HasValue())
        logCfg = logCfg.WriteTo.ApplicationInsights(new TelemetryClient {InstrumentationKey = appInsightsKey}, TelemetryConverter.Traces);
      logCfg = logCfg.WriteTo.ILogger(funcLogger);
      return logCfg.CreateLogger();
    }

    static async Task<(AppCfg Cfg, ILogger Log, RootCfg Root)> Init(IMSLogger funcLogger, ExecutionContext context) {
      funcLogger.LogInformation($"Context: {context.ToJson()}");
      var (app, root) = await Setup.LoadCfg2(context.FunctionAppDirectory);
      var log = Logger(funcLogger, app.AppInsightsKey);
      log.Information("Function {@Context}", context);
      return (app, log, root);
    }

    [FunctionName("Update_Timer")]
    public static async Task Update_Timer([TimerTrigger("0 0 21 * * *")] TimerInfo myTimer, ExecutionContext context, IMSLogger log) =>
      await RunUpdate(log, context);

    [FunctionName("Update")]
    public static async Task<HttpResponseMessage> Update([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, ExecutionContext context, IMSLogger funcLogger) {
      var updateMesssage = await RunUpdate(funcLogger, context);
      return req.AsyncResponse(updateMesssage);
    }

    static async Task<string> RunUpdate(IMSLogger funcLogger, ExecutionContext context) {
      var s = await Init(funcLogger, context);
      s.Log.Information("Function {Function} started in environment {Env}", nameof(RunUpdate), s.Root.Env);

      try {
        var scope = Setup.BaseScope(s.Root, s.Cfg, s.Log);
        var pipeCtx = scope.ResolvePipeCtx();
        var res = await pipeCtx.RunPipe(nameof(YtDataUpdater.Update), false, s.Log);
        return res.Error
          ? $"Error starting container: {res.ErrorMessage}"
          : $"Started containers(s): {res.Containers.Join(", ", c => $"{c.Image} -> {c.Name}")}";
      }
      catch (Exception ex) {
        s.Log.Error("Error starting container to update data {Error}", ex.Message, ex);
        throw;
      }
    }
  }
}