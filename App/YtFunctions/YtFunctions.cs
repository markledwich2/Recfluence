using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json;
using Seq.Api;
using Serilog;
using SysExtensions.Build;
using SysExtensions.Serialization;
using SysExtensions.Text;
using YtReader;
using YtReader.Db;
using IMSLogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable 618

namespace YtFunctions {
  public class YtFunctions {
    readonly IAzure                 Azure;
    readonly AppCfg                 Cfg;
    readonly AppDb                  Db;
    readonly JsonSerializerSettings JSettings;
    readonly ILogger                Log;
    readonly IPipeCtx               PipeCtx;
    readonly RootCfg                RootCfg;

    public YtFunctions(ILogger log, IPipeCtx pipeCtx, AppCfg cfg, RootCfg rootCfg, IAzure azure, AppDb db) {
      Log = log;
      PipeCtx = pipeCtx;
      Cfg = cfg;
      RootCfg = rootCfg;
      Azure = azure;
      Db = db;
      JSettings = JsonExtensions.DefaultSettings();
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

    [FunctionName("Video")]
    public async Task<HttpResponseMessage> Video([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Video/{videoId}")]
      HttpRequest req, string videoId, ExecutionContext context) =>
      await context.Run(Log, async log => {
        var conn = await Db.OpenConnection();
        var videoTask = conn.QueryFirstAsync<DbVideo>("select * from video_latest where video_id = :video_id", new {video_id = videoId});
        var captionsTask = conn.QueryAsync<DbCaption>("select * from caption where video_id = :video_id", new {video_id = videoId});
        var res = new VideoResponse {
          Video = await videoTask ?? new DbVideo {video_title = "(unable to find this video)"},
          Captions = (await captionsTask).ToArray()
        };
        return new HttpResponseMessage(HttpStatusCode.OK) {
          Content = new StringContent(res.ToJson(), Encoding.UTF8, "application/json")
        };
      });

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

    public class VideoResponse {
      public DbVideo     Video    { get; set; }
      public DbCaption[] Captions { get; set; }
    }
  }
}