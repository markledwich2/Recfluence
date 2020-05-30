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
using Serilog;
using SysExtensions.Build;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

namespace YtFunctions {
  public class ApiBackend {
    readonly AsyncLazy<FuncCtx, ExecutionContext> Ctx;

    public ApiBackend(AsyncLazy<FuncCtx, ExecutionContext> ctx) => Ctx = ctx;

    [FunctionName(nameof(DeleteExpiredResources_Timer))]
    public Task DeleteExpiredResources_Timer([TimerTrigger("0 0 * * * *")] TimerInfo myTimer, ExecutionContext exec) =>
      Ctx.Run(exec, async ctx => {
        var cleaner = ctx.Scope.Resolve<AzureCleaner>();
        await cleaner.DeleteExpiredResources(ctx.Log);
      });

    [FunctionName("Version")]
    public Task<IActionResult> Version([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequest req, ExecutionContext exec) => Ctx.Run(exec, c => {
      var versionText = @$"Version: 
Runtime ${GitVersionInfo.RuntimeSemVer(typeof(YtCollector))}
Discovered ${GitVersionInfo.DiscoverVersion(typeof(YtCollector))}";
      return Task.FromResult((IActionResult) new OkObjectResult(versionText));
    });

    [FunctionName("Update_Timer")]
    public Task Update_Timer([TimerTrigger("0 0 21 * * *")] TimerInfo myTimer, ExecutionContext exec) =>
      RunUpdate(exec);

    [FunctionName("Update")]
    public async Task<IActionResult> Update([HttpTrigger(AuthorizationLevel.Function, "get", "post")]
      HttpRequestMessage req, ExecutionContext exec) {
      var update = await RunUpdate(exec);
      return new OkObjectResult(update);
    }

    Task<string> RunUpdate(ExecutionContext exec) => Ctx.Run(exec, async c => {
      var pipeCtx = c.Scope.Resolve<IPipeCtx>();
      var res = await pipeCtx.Run((YtUpdater u) => u.Update(null, false, null), c.Log, true);
      if (res.Error)
        Log.Error("ApiBackend - Error starting RunUpdate: {Message}", res.ErrorMessage);
      return res.Error
        ? $"Error starting pipe work: {res.ErrorMessage}"
        : $"Started work on containers(s): {res.Containers.Join(", ", c => $"{c.Image} -> {c.Name}")}";
    });
  }
}