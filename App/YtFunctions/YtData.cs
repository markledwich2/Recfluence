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

namespace YtFunctions {
  public class YtData {
    /// <summary>Use the Json.net defaults because we want to keep original name casings so that we aren't re-casing the
    ///   databse in different formats</summary>
    static readonly JsonSerializerSettings JCfg = new JsonSerializerSettings {Formatting = Formatting.None};
    readonly AsyncLazy<FuncCtx, ExecutionContext> Ctx;

    public YtData(AsyncLazy<FuncCtx, ExecutionContext> ctx) => Ctx = ctx;

    [FunctionName("video")]
    public async Task<HttpResponseMessage> Video([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Video/{videoId}")]
      HttpRequest req, string videoId, ExecutionContext exec) =>
      await Ctx.Run(exec, async c => {
        var db = c.Scope.Resolve<AppDb>();
        using var conn = await db.OpenConnection();
        var videoTask = conn.QueryFirstAsync<DbVideo>("select * from video_latest where video_id = :video_id", new {video_id = videoId});
        var captionsTask = conn.QueryAsync<DbCaption>("select * from caption where video_id = :video_id order by offset_seconds", new {video_id = videoId});
        var video = await videoTask;
        var channel = await conn.QueryFirstAsync<DbChannel>("select * from channel_latest where channel_id= :channel_id", new {channel_id = video.CHANNEL_ID});
        channel.TAGS = channel.TAGS.Replace("\n", " ");
        var res = new VideoResponse {
          video = video,
          captions = (await captionsTask).ToArray(),
          channel = channel
        };
        return res.JsonResponse(JCfg);
      });

    [FunctionName("search")]
    public async Task<HttpResponseMessage> Search([HttpTrigger(AuthorizationLevel.Anonymous, "get")]
      HttpRequest req, ExecutionContext exec) => await Ctx.Run(exec, async ctx => {
      var query = req.Query["q"].FirstOrDefault();
      if (!query.HasValue())
        return new SearchResponse<VideoCaption>().JsonResponse(JCfg);

      var elastic = ctx.Scope.Resolve<ElasticClient>();
      var res = await elastic.SearchAsync<VideoCaption>(s => s.Query(q => q.Match(m => m.Field(f => f.caption).Query(query))));
      return res.JsonResponse(JCfg);
    });

    static async Task<HttpResponseMessage> SqlSearch(FuncCtx ctx) {
      var db = ctx.Scope.Resolve<AppDb>();
      using var conn = await db.OpenConnection();
      var captionTask = conn.QueryAsync(@"select top 20 s.rank
        , c.caption_id
        , c.caption
        , c.offset_seconds
        , v.video_id
        , v.video_title
        , v.channel_id
        , v.channel_title
        , v.views
        , v.upload_date
        , v.duration
        , v.thumb_high
        , v.description
      from freetexttable(caption, caption, :query) s
        left join caption c on s.[KEY] = c.caption_id
      left join video_latest v on c.video_id = v.video_id
      order by rank desc");

      var videoTask = conn.QueryAsync(@"select top 20 s.rank, v.video_id
            , v.video_title
            , v.channel_id
            , v.channel_title
            , v.views
            , v.upload_date
            , v.duration
            , v.thumb_high
            , v.description
from freetexttable(video_latest, (video_title), :query) s
left join video_latest v on s.[KEY] = v.video_id
order by rank desc, views desc");

      var combined = (await captionTask).Concat(await videoTask).OrderByDescending(c => c.Rank);
      return combined.JsonResponse(JCfg);
    }

    public class VideoResponse {
      public DbVideo     video    { get; set; }
      public DbCaption[] captions { get; set; }
      public DbChannel   channel  { get; set; }
    }
  }

  public static class HttpResponseEx {
    public static HttpResponseMessage JsonResponse(this object o, JsonSerializerSettings settings) =>
      new HttpResponseMessage(HttpStatusCode.OK) {
        Content = new StringContent(o.ToJson(settings), Encoding.UTF8, "application/json")
      };
  }
}