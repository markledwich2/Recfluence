using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Serilog;
using YtReader;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;
using Microsoft.Azure.Management;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.Fluent;
using Humanizer;
using System.Net;
using SysExtensions.Text;
using Serilog.Sinks.ILogger;
using System.Collections.Generic;

namespace YtFunctions {
    public static class YtFunctions {
        static ILogger Logger(TelemetryClient telem, IMSLogger funcLogger) {
            var logCfg = new LoggerConfiguration().WriteTo.Console();

            if (telem.InstrumentationKey.HasValue())
                logCfg = logCfg.WriteTo.ApplicationInsightsTraces(telem);

            if (TelemetryConfiguration.Active.InstrumentationKey.NullOrEmpty()) // only write to logger if AI is not configured (thus allready doing so)
                logCfg = logCfg.WriteTo.ILogger(funcLogger);

            return logCfg
                .CreateLogger();
        }

        static async Task<(Cfg Cfg, ILogger Log, TelemetryClient Telem)> Init(IMSLogger funcLogger) {
            var cfg = await Setup.LoadCfg();

            var telem = new TelemetryClient() { InstrumentationKey = cfg.App.AppInsightsKey };

            var log = Logger(telem, funcLogger);

            return (cfg, log, telem);
        }

        // [FunctionName("UpdateAllChannels_Orchestration")]
        // public static async Task UpdateAllChannels_Orchestration(
        //     [OrchestrationTrigger] DurableOrchestrationContext context, IMSLogger funcLogger) {
        //     var retry = new RetryOptions(TimeSpan.MaxValue, 1);
        //     var channelIds = await context.CallActivityWithRetryAsync<List<string>>("GetAllChannels", retry, context.InstanceId);
        //     var channelTasks = channelIds.Select(c => context.CallActivityWithRetryAsync<string>("UpdateChannel", retry, c)).ToArray();
        //     var channels = await Task.WhenAll(channelTasks);
        //     await context.CallActivityWithRetryAsync("CollectData", retry, context.InstanceId);
        // }

        // [FunctionName("CollectData_HttpStart")]
        // public static async Task<HttpResponseMessage> CollectData_HttpStart(
        //     [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
        //     HttpRequestMessage req,
        //     [OrchestrationClient] DurableOrchestrationClient starter,
        //     IMSLogger log) {
        //     try {
        //         var s = await CollectData(Guid.NewGuid().ToString(), log);
        //         return req.CreateResponse($"CollectData with result: {s}");
        //     } catch (Exception ex) {
        //         return req.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.Message);
        //     }
        // }

        [FunctionName("Update_Timer")]
        public static async Task Update_Timer([TimerTrigger("0 0 21 * * *")] TimerInfo myTimer, IMSLogger log) {
            await Update(log);
        }

        [FunctionName("Update")]
        public static async Task<HttpResponseMessage> Update([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
            HttpRequestMessage req, IMSLogger funcLogger) {
            return req.CreateResponse(await Update(funcLogger));
        }

        static async Task<string> Update(IMSLogger funcLogger) {
            var s = await Init(funcLogger);
            s.Log.Information("Function Update started");

            IContainerGroup g;

            try {
                g = await YtContainerRunner.Start(s.Log, s.Cfg);
            } catch (Exception ex) {
                s.Log.Error("Error starting container to update data {Error}", ex.Message, ex);
                throw;
            }

            var started = DateTime.Now;
            g.Refresh();
            var log = await g.GetLogContentAsync(g.Name);
            s.Log.Information("Started container '{ProvisionState}' '{State}': {LogContent}", g.ProvisioningState, g.State, log);

            return $"Started container, last in state '{g.State}'";
        }

        // [FunctionName("GetAllChannels")]
        // public static async Task<List<string>> GetAllChannels([ActivityTrigger] string instanceId, IMSLogger funcLogger) {
        //     var s = Init(funcLogger).Result;
        //     var channelCfg = await s.Cfg.App.LoadChannelConfig();
        //     return channelCfg.Seeds.Select(c => c.Id).ToList();
        // }

        // [FunctionName("UpdateChannel")]
        // public static async Task<string> UpdateChannel([ActivityTrigger] string channelId, IMSLogger funcLogger) {
        //     var s = await Init(funcLogger);

        //     var reader = new YtClient(s.Cfg.App, s.Log);
        //     var ytStore = new YtStore(reader, s.Cfg.DataStore());
        //     var crawler = new YtDataUpdater(ytStore, s.Cfg.App, s.Log);
        //     var channelCfg = await s.Cfg.App.LoadChannelConfig();
        //     var seed = channelCfg.Seeds[channelId];

        //     try {
        //         await crawler.UpdateChannel(seed);
        //     } catch (Exception ex) {
        //         s.Log.Error(ex, "Error updating {Channel}", seed.Title);
        //         throw;
        //     }

        //     s.Telem.Flush();
        //     return channelId;
        // }

        // [FunctionName("UpdateAllChannels_HttpStart")]
        // public static async Task<HttpResponseMessage> UpdateAllChannels_HttpStart(
        //     [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
        //     HttpRequestMessage req,
        //     [OrchestrationClient] DurableOrchestrationClient starter,
        //     IMSLogger log) {
        //     var instanceId = await starter.StartNewAsync("UpdateAllChannels_Orchestration", "");
        //     return starter.CreateCheckStatusResponse(req, instanceId);
        // }


    }
}