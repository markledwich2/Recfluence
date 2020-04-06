using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Mutuo.Etl.Pipe;
using Serilog;
using Serilog.Sinks.ILogger;
using SysExtensions.Net;
using SysExtensions.Text;
using YtReader;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

namespace YtFunctions {
  public static class YtFunctions {
    static ILogger Logger(TelemetryClient telem, IMSLogger funcLogger) {
      var logCfg = new LoggerConfiguration().WriteTo.Console();

      if (telem.InstrumentationKey.HasValue())
        logCfg = logCfg.WriteTo.ApplicationInsights(telem, TelemetryConverter.Traces);

      if (TelemetryConfiguration.Active.InstrumentationKey.NullOrEmpty()
      ) // only write to logger if AI is not configured (thus allready doing so)
        logCfg = logCfg.WriteTo.ILogger(funcLogger);

      return logCfg
        .CreateLogger();
    }

    static async Task<(AppCfg Cfg, ILogger Log, TelemetryClient Telem, RootCfg Root)> Init(IMSLogger funcLogger) {
      var (app, root) = await Setup.LoadCfg2();
      var telem = new TelemetryClient {InstrumentationKey = app.AppInsightsKey};
      var log = Logger(telem, funcLogger);
      return (app, log, telem, root);
    }

    [FunctionName("Update_Timer")]
    public static async Task Update_Timer([TimerTrigger("0 0 21 * * *")] TimerInfo myTimer, IMSLogger log) =>
      await RunUpdate(log);

    [FunctionName("Update")]
    public static async Task<HttpResponseMessage> Update([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, IMSLogger funcLogger) {
      var updateMesssage = await RunUpdate(funcLogger);
      return req.AsyncResponse(updateMesssage);
    }

    static async Task<string> RunUpdate(IMSLogger funcLogger) {
      var s = await Init(funcLogger);
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