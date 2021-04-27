using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Semver;
using Serilog;
using SysExtensions.Text;
using static System.Net.HttpStatusCode;
using static YtFunctions.HttpResponseEx;
using YtReader.Yt;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

// ML 29 Jan 2021: Azure Functions does' work with .NEt5, but support is imminent, in the meantime if function need to be update, this must be done from a branch with .net 4 and deployed manually.

namespace YtFunctions {
  public record ApiBackend(SemVersion Version, IPipeCtx Ctx, ILogger Log, ContainerCfg ContainerCfg, YtContainerRunner Runner, AzureCleaner AzCleaner) {
    [FunctionName(nameof(DeleteExpiredResources_Timer))]
    public Task DeleteExpiredResources_Timer([TimerTrigger("0 0 * * * *")] TimerInfo myTimer) => F(() => AzCleaner.DeleteExpiredResources(Log));

    [FunctionName("Version")]
    public HttpResponseData GetVersion([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
      HttpRequestData req) =>
      new(OK, Version.ToString());

    [FunctionName("Update_Timer")] public Task Update_Timer([TimerTrigger("0 0 0 * * *")] TimerInfo timer) => F(RunUpdate);

    [FunctionName("Update")]
    public Task<HttpResponseData> Update([HttpTrigger(AuthorizationLevel.Function, "get", "post")]
      HttpRequestData req) => R(async () => {
      var container = await RunUpdate();
      return new(OK, $"Update - started container '{container}'");
    });

    async Task<string> RunUpdate() {
      var groupName = $"update{(Version.Prerelease.HasValue() ? $"-{Version.Prerelease}" : "")}";
      await Runner.Run(groupName, returnOnStart: true, args: new[] {"update"});
      return groupName;
    }
  }
}