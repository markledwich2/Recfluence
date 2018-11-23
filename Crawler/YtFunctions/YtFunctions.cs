using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Serilog;
using YouTubeReader;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

namespace YtFunctions {
    public static class YtFunctions {
        static ILogger Logger(TelemetryClient telem, ExecutionContext execContext) {
            var log = new LoggerConfiguration()
                //.WriteTo.ILogger(funcLogger) // don't write to logger. It will also write to insights, but without the custom data
                .WriteTo.ApplicationInsightsTraces(telem)
                .WriteTo.Console()
                .CreateLogger()
                .ForContext("InvocationId", execContext.InvocationId);
            return log;
        }

        static async Task<(Cfg Cfg, ILogger Log, TelemetryClient Telem)> Init(IMSLogger funcLogger, ExecutionContext execContext) {
            var telemCfg = TelemetryConfiguration.Active;
            var telem = new TelemetryClient(telemCfg);

            var log = Logger(telem, execContext);
            var cfg = await Setup.LoadCfg(log);
            return (cfg, log, telem);
        }


        [FunctionName("UpdateAllChannels_Orchestration")]
        public static async Task UpdateAllChannels_Orchestration(
            [OrchestrationTrigger] DurableOrchestrationContext context, IMSLogger funcLogger, ExecutionContext execContext) {
            var s = Init(funcLogger, execContext).Result;

            var channelCfg = s.Cfg.App.LoadChannelConfig().Result;

            //var channels = await channelCfg.Seeds.BlockTransform(c => context.CallActivityAsync<string>("UpdateChannel", c.Id), cfg.Parallel);
            var channelTasks = channelCfg.Seeds.Select(c => context.CallActivityAsync<string>("UpdateChannel", c.Id)).ToArray();

            var channels = await Task.WhenAll(channelTasks);

            s.Log.Information("Orchestrator completed for {Channels} channels", channels.Length);

            s.Telem.Flush();
        }


        [FunctionName("UpdateChannel")]
        public static async Task<string> UpdateChannel([ActivityTrigger] string channelId, IMSLogger funcLogger, ExecutionContext execContext) {
            var s = await Init(funcLogger, execContext);


            var reader = new YtReader(s.Cfg.App, s.Log);
            var ytStore = new YtStore(reader, s.Cfg.FileStore());
            var crawler = new YtDataUpdater(ytStore, s.Cfg.App, s.Log);
            var channelCfg = await s.Cfg.App.LoadChannelConfig();
            var seed = channelCfg.Seeds[channelId];

            try {
                await crawler.UpdateChannel(seed);
            }
            catch (Exception ex) {
                s.Log.Error(ex, "Error updating {Channel}", seed.Title);
                throw;
            }

            s.Telem.Flush();
            return channelId;
        }

        [FunctionName("UpdateAllChannels_HttpStart")]
        public static async Task<HttpResponseMessage> UpdateAllChannels_HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
            HttpRequestMessage req,
            [OrchestrationClient] DurableOrchestrationClient starter,
            IMSLogger log) {
            var instanceId = await starter.StartNewAsync("UpdateAllChannels_Orchestration", null);
            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}