using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Serilog;
using Serilog.Sinks.ILogger;
using SysExtensions.Text;
using YtReader;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

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

    static async Task<(Cfg Cfg, ILogger Log, TelemetryClient Telem)> Init(IMSLogger funcLogger) {
      var cfg = await Setup.LoadCfg();
      var telem = new TelemetryClient {InstrumentationKey = cfg.App.AppInsightsKey};
      var log = Logger(telem, funcLogger);
      return (cfg, log, telem);
    }

    [FunctionName("Update_Timer")]
    public static async Task Update_Timer([TimerTrigger("0 0 21 * * *")] TimerInfo myTimer, IMSLogger log) =>
      await YtCli(log);
    
    [FunctionName("Update")]
    public static async Task<HttpResponseMessage> Update([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, IMSLogger funcLogger) => req.CreateResponse(await YtCli(funcLogger));

    static async Task<string> YtCli(IMSLogger funcLogger) {
      var s = await Init(funcLogger);
      s.Log.Information("Function Update started");


      try {
        var g = await YtContainerRunner.StartFleet(s.Log, s.Cfg, UpdateType.All);
        return $"Started containers: {g.Join(", ", c => c.Name)}";
      }
      catch (Exception ex) {
        s.Log.Error("Error starting container to update data {Error}", ex.Message, ex);
        throw;
      }

/*      g.Refresh();
      var log = await g.GetLogContentAsync(g.Name);
      s.Log.Information("Started container '{ProvisionState}' '{State}': {LogContent}", g.ProvisioningState, g.State, log);
      return $"Started container, last in state '{g.State}'";*/
    }
  }
}