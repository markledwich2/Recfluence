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
using Newtonsoft.Json;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader.Db;

namespace YtFunctions {
  public class YtData {
    static readonly JsonSerializerSettings               JSettings = new JsonSerializerSettings {Formatting = Formatting.None};
    readonly        AsyncLazy<FuncCtx, ExecutionContext> Ctx;

    public YtData(AsyncLazy<FuncCtx, ExecutionContext> ctx) => Ctx = ctx;

    [FunctionName("Video")]
    public async Task<HttpResponseMessage> Video([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Video/{videoId}")]
      HttpRequest req, string videoId, ExecutionContext exec) =>
      await Ctx.Run(exec, async c => {
        var db = c.Scope.Resolve<AppDb>();
        var conn = await db.OpenConnection();
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

        return new HttpResponseMessage(HttpStatusCode.OK) {
          Content = new StringContent(res.ToJson(JSettings), Encoding.UTF8, "application/json")
        };
      });

    public class VideoResponse {
      public DbVideo     video    { get; set; }
      public DbCaption[] captions { get; set; }
      public DbChannel   channel  { get; set; }
    }
  }
}