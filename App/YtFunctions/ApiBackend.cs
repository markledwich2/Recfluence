using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions.Build;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

// ML 29 Jan 2021: Azure Functions does' work with .NEt5, but support is imminent, in the meantime if function need to be update, this must be done from a branch with .net 4 and deployed manually.

namespace YtFunctions {
  public class ApiBackend {
    readonly Defer<FuncCtx, ExecutionContext> Ctx;

    public ApiBackend(Defer<FuncCtx, ExecutionContext> ctx) => Ctx = ctx;

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
    public Task Update_Timer([TimerTrigger("0 0 0 * * *")] TimerInfo myTimer, ExecutionContext exec) =>
      RunUpdate(exec);

    [FunctionName("Update")]
    public async Task<IActionResult> Update([HttpTrigger(AuthorizationLevel.Function, "get", "post")]
      HttpRequestMessage req, ExecutionContext exec) {
      var update = await RunUpdate(exec);
      return new OkObjectResult(update);
    }

    Task<string> RunUpdate(ExecutionContext exec) => Ctx.Run(exec, async c => {
      var pipeCtx = c.Scope.Resolve<IPipeCtx>();
      var options = new UpdateOptions();
      var res = await pipeCtx.Run((YtUpdater u) => u.Update(options, PipeArg.Inject<CancellationToken>()),
        new PipeRunOptions {
          ReturnOnStarted = true,
          Exclusive = true
        }, c.Log);

      if (res.Error)
        Log.Error("ApiBackend - Error starting RunUpdate: {Message}", res.ErrorMessage);
      return res.Error
        ? $"Error starting pipe work: {res.ErrorMessage}"
        : $"Started work on containers(s): {res.Containers.Join(", ", c => $"{c.Image} -> {c.Name}")}";
    });

    [FunctionName("Sms")]
    public Task<IActionResult> Sms([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequest req, ExecutionContext exec) => Ctx.Run(exec, c => {
      var sms = req.Body.ToObject<TeleSignSms>();

      //talk to discod. for now now
      c.Log.Information("Received SMS from {Phone}: {Msg}", sms.user_response.phone_number, sms.user_response.mo_message);

      return Task.FromResult((IActionResult) new OkResult());
    });
  }
}