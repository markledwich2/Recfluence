using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Mutuo.Etl.Pipe;
using Seq.Api;
using Serilog;
using SysExtensions.Build;
using SysExtensions.Text;
using YtReader;
using YtReader.Db;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

namespace YtFunctions {
  public class YtFunctions {
    readonly IAzure   Azure;
    readonly AppCfg   Cfg;
    readonly ILogger  Log;
    readonly IPipeCtx PipeCtx;
    readonly RootCfg  RootCfg;

    public YtFunctions(ILogger log, IPipeCtx pipeCtx, AppCfg cfg, RootCfg rootCfg, IAzure azure) {
      Log = log;
      PipeCtx = pipeCtx;
      Cfg = cfg;
      RootCfg = rootCfg;
      Azure = azure;
    }

    [FunctionName("StopIdleSeq_Timer")]
    public async Task StopIdleSeq_Timer([TimerTrigger("0 */15 * * * *")] TimerInfo myTimer) =>
      await StopIdleSeqInner();

    [FunctionName("StopIdleSeq")]
    public async Task<IActionResult> StopIdleSeq([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequest req) => new OkObjectResult(await StopIdleSeqInner());

    [FunctionName("Version")]
    public Task<IActionResult> Version([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequest req) {
      var versionText = @$"Version: 
Runtime ${GitVersionInfo.RuntimeSemVer(typeof(YtDataUpdater))}
Discovered ${GitVersionInfo.DiscoverSemVer(typeof(YtDataUpdater))}";
      return Task.FromResult((IActionResult) new OkObjectResult(versionText));
    }

    async Task<string> StopIdleSeqInner() {
      if (!RootCfg.IsProd()) return LogReason("not prod");
      var group = await Azure.SeqGroup(Cfg);
      if (group.State() != ContainerState.Running) return LogReason("seq container not running");
      var seq = new SeqConnection(Cfg.SeqUrl.OriginalString);
      var events = await seq.Events.ListAsync(count: 5, filter: Cfg.SeqHost.IdleQuery, render: true);
      if (events.Any()) {
        Log.Information("{Events} recent events exist from '{Query}'", events.Count, Cfg.SeqHost.IdleQuery);
        return $"recent events exist: {events.Join("\n", e => e.RenderedMessage)}";
      }
      Log.Information("No recent events from '{Query}'. Stopping {ContainerGroup}", Cfg.SeqHost.IdleQuery, group.Name);
      await group.StopAsync();
      return $"stopped group {group.Name}";

      string LogReason(string reason) {
        Log.Information("{Noun} - {reason}", nameof(StopIdleSeq), reason);
        return reason;
      }
    }

    [FunctionName("Update_Timer")]
    public Task Update_Timer([TimerTrigger("0 0 21 * * *")] TimerInfo myTimer, ExecutionContext ctx) =>
      ctx.Run(Log, RunUpdate);

    [FunctionName("Update")]
    public async Task<IActionResult> Update([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, ExecutionContext ctx) =>
      await ctx.Run(Log, async l => new OkObjectResult(await RunUpdate(l)));

    async Task<string> RunUpdate(ILogger log) {
      try {
        var res = await PipeCtx.RunPipe(nameof(YtDataUpdater.Update), false, Log);
        return res.Error
          ? $"Error starting pipe work: {res.ErrorMessage}"
          : $"Started work on containers(s): {res.Containers.Join(", ", c => $"{c.Image} -> {c.Name}")}";
      }
      catch (Exception ex) {
        Log.Error("Error starting container to update data {Error}", ex.Message, ex);
        throw;
      }
    }
  }
}