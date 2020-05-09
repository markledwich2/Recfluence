using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Autofac;
using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Nest;
using Newtonsoft.Json;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Search;
using TimeUnit = Humanizer.Localisation.TimeUnit;

namespace YtFunctions {
  public class YtData {
    /// <summary>Use the Json.net defaults because we want to keep original name casings so that we aren't re-casing the
    ///   db in different formats</summary>
    static readonly JsonSerializerSettings JCfg = new JsonSerializerSettings {Formatting = Formatting.None};
    readonly AsyncLazy<FuncCtx, ExecutionContext> Ctx;

    public YtData(AsyncLazy<FuncCtx, ExecutionContext> ctx) => Ctx = ctx;

    [FunctionName("video")]
    public async Task<HttpResponseMessage> Video([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "video/{videoId}")]
      HttpRequest req, string videoId, ExecutionContext exec) =>
      await Ctx.Run(exec, async c => {
        var es = c.Scope.Resolve<ElasticClient>();
        var videoRes = await es.GetAsync<EsVideo>(videoId);
        var video = videoRes?.Source;
        if (video == null) return new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new StringContent($"video `{videoId}` not found")};
        var channelRes = await es.GetAsync<EsChannel>(video.channel_id);
        var channel = channelRes.Source;
        if(channel == null) return new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new StringContent($"channel `{video.channel_id}` not found")};
        return new { video, channel}.JsonResponse(JCfg);
      });

    [FunctionName("captions")]
    public async Task<HttpResponseMessage> Captions([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "captions/{videoId}")]
      HttpRequest req, string videoId, ExecutionContext exec) =>
      await Ctx.Run(exec, async c => {
        var es = c.Scope.Resolve<ElasticClient>();
        var res = await es.SearchAsync<EsCaption>(s => s
          .Source(sf => sf.Includes(i => i.Fields(
            f => f.caption_id,
            f => f.video_id,
            f => f.caption,
            f => f.offset_seconds
          )))
          .Query(q => q.Term(t => t.video_id, videoId) && q.Term(t => t.part, nameof(CaptionPart.Caption)))
          .Sort(o => o.Ascending(f => f.offset_seconds))
          .Size(5000)
        );
          
        var captions = res.Hits.Select(h => h.Source).ToArray();
        return captions.JsonResponse(JCfg);
      });

    public class VideoResponse {
      public DbVideo   video   { get; set; }
      public DbChannel channel { get; set; }
      public string    error   { get; set; }
    }
  }

  public static class HttpResponseEx {
    public static HttpResponseMessage JsonResponse(this object o, JsonSerializerSettings settings) =>
      new HttpResponseMessage(HttpStatusCode.OK) {
        Content = new StringContent(o.ToJson(settings), Encoding.UTF8, "application/json")
      };
  }
}