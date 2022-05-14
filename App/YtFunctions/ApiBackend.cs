using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Semver;
using Serilog;
using SysExtensions.Text;
using YtReader;
using static YtFunctions.HttpResponseEx;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

namespace YtFunctions;

public record ApiBackend(SemVersion Version, IPipeCtx Ctx, ILogger Log, ContainerCfg ContainerCfg, YtContainerRunner Runner, AzureCleaner AzCleaner) {
  [Function(nameof(DeleteExpiredResources_Timer))]
  public Task DeleteExpiredResources_Timer([TimerTrigger("0 0 * * * *")] TimerInfo myTimer) =>
    F(() => AzCleaner.DeleteExpiredResources(CleanContainerMode.Standard, Log));

  [Function("Version")]
  public HttpResponseData GetVersion([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req) => req.TextResponse(Version.ToString());

  [Function("Update_Timer")] public Task Update_Timer([TimerTrigger("0 0 0 * * SAT")] TimerInfo timer) => F(RunUpdate);

  [Function("Update")]
  public Task<HttpResponseData> Update([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req) => R(async () => {
    var container = await RunUpdate();
    return req.TextResponse($"Update - started container '{container}'");
  });

  async Task<string> RunUpdate() {
    var groupName = $"update{(Version.Prerelease.HasValue() ? $"-{Version.Prerelease}" : "")}";
    await Runner.Run(groupName, returnOnStart: true, args: new[] { "update" });
    return groupName;
  }
}