using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Autofac;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Seq.Api;
using SysExtensions.Build;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

namespace YtFunctions {
  public class YtFunctions {
    readonly AsyncLazy<FuncCtx, ExecutionContext> Ctx;

    public YtFunctions(AsyncLazy<FuncCtx, ExecutionContext> ctx) => Ctx = ctx;

    [FunctionName(nameof(DeleteExpiredResources_Timer))]
    public Task DeleteExpiredResources_Timer([TimerTrigger("0 0 * * * *")] TimerInfo myTimer, ExecutionContext exec) =>
      Ctx.Run(exec, async ctx => { await ctx.Scope.Resolve<IAzure>().DeleteExpiredResources(ctx.Log); });

    [FunctionName("StopIdleSeq_Timer")]
    public async Task StopIdleSeq_Timer([TimerTrigger("0 */15 * * * *")] TimerInfo myTimer, ExecutionContext exec) =>
      await StopIdleSeqInner(exec);

    [FunctionName("StopIdleSeq")]
    public async Task<IActionResult> StopIdleSeq([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequest req, ExecutionContext exec) => new OkObjectResult(await StopIdleSeqInner(exec));

    [FunctionName("Version")]
    public Task<IActionResult> Version([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequest req) {
      var versionText = @$"Version: 
Runtime ${GitVersionInfo.RuntimeSemVer(typeof(YtDataUpdater))}
Discovered ${GitVersionInfo.DiscoverSemVer(typeof(YtDataUpdater))}";
      return Task.FromResult((IActionResult) new OkObjectResult(versionText));
    }

    Task<string> StopIdleSeqInner(ExecutionContext exec) => Ctx.Run(exec, async c => {
      var azue = c.Scope.Resolve<IAzure>();
      if (!c.Root.IsProd()) return LogReason("not prod");
      var group = await azue.SeqGroup(c.Cfg);
      if (group.State() != ContainerState.Running) return LogReason("seq container not running");
      var seq = new SeqConnection(c.Cfg.SeqUrl.OriginalString);
      var events = await seq.Events.ListAsync(count: 5, filter: c.Cfg.SeqHost.IdleQuery, render: true);
      if (events.Any()) {
        c.Log.Information("{Events} recent events exist from '{Query}'", events.Count, c.Cfg.SeqHost.IdleQuery);
        return $"recent events exist: {events.Join("\n", e => e.RenderedMessage)}";
      }
      c.Log.Information("No recent events from '{Query}'. Stopping {ContainerGroup}", c.Cfg.SeqHost.IdleQuery, group.Name);
      await group.StopAsync();
      return $"stopped group {group.Name}";

      string LogReason(string reason) {
        c.Log.Information("{Noun} - {reason}", nameof(StopIdleSeq), reason);
        return reason;
      }
    });

    [FunctionName("Update_Timer")]
    public Task Update_Timer([TimerTrigger("0 0 21 * * *")] TimerInfo myTimer, ExecutionContext exec) =>
      RunUpdate(exec);

    [FunctionName("Update")]
    public async Task<IActionResult> Update([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestMessage req, ExecutionContext exec) {
      var update = await RunUpdate(exec);
      return new OkObjectResult(update);
    }

    Task<string> RunUpdate(ExecutionContext exec) => Ctx.Run(exec, async c => {
      var pipeCtx = c.Scope.Resolve<IPipeCtx>();
      var res = await pipeCtx.RunPipe(nameof(YtDataUpdater.Update), true, c.Log);
      return res.Error
        ? $"Error starting pipe work: {res.ErrorMessage}"
        : $"Started work on containers(s): {res.Containers.Join(", ", c => $"{c.Image} -> {c.Name}")}";
    });
  }
}