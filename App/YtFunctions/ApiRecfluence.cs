using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Autofac;
using Humanizer;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Primitives;
using Mutuo.Etl.Blob;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using YtReader.Db;
using YtReader.Search;
using YtReader.Store;

namespace YtFunctions {
  public class ApiRecfluence {
    /// <summary>Use the Json.net defaults because we want to keep original name casings so that we aren't re-casing the db in
    ///   different formats</summary>
    static readonly JsonSerializerSettings JCfg = new JsonSerializerSettings {Formatting = Formatting.None};
    readonly Defer<FuncCtx, ExecutionContext> Ctx;

    public ApiRecfluence(Defer<FuncCtx, ExecutionContext> ctx) => Ctx = ctx;

    [FunctionName("video")]
    public async Task<HttpResponseMessage> Video([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "video/{videoId}")]
      HttpRequest req, string videoId, ExecutionContext exec) =>
      await Ctx.Run(exec, async c => {
        var es = c.Scope.Resolve<ElasticClient>();
        var videoRes = await es.GetAsync<EsVideo>(videoId);
        var video = videoRes?.Source;
        if (video == null) return new HttpResponseMessage(HttpStatusCode.NotFound) {Content = new StringContent($"video `{videoId}` not found")};
        var channelRes = await es.GetAsync<EsChannel>(video.channel_id);
        var channel = channelRes.Source;
        if (channel == null) return new HttpResponseMessage(HttpStatusCode.NotFound) {Content = new StringContent($"channel `{video.channel_id}` not found")};
        return new {video, channel}.JsonResponse(JCfg);
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

    [FunctionName("search")]
    public async Task<HttpResponseMessage> LogSearch([HttpTrigger(AuthorizationLevel.Anonymous, "put")]
      HttpRequest req, ExecutionContext exec) =>
      await Ctx.Run(exec, async c => {
        // get user query, filters etc.. from query string
        var search = req.Body.ToObject<UserSearchWithUpdated>();
        var store = c.Scope.Resolve<YtStore>();
        await store.Searches.Append(new[] {search});
        return new HttpResponseMessage(HttpStatusCode.OK);
      });

    static readonly JsonSerializerSettings JsSerializer = JsonlExtensions.DefaultSettingsForJs();

    [FunctionName("channel_review")]
    public async Task<HttpResponseMessage> ChannelReview([HttpTrigger(AuthorizationLevel.Anonymous, "put")]
      HttpRequest req, ExecutionContext exec) =>
      await Ctx.Run(exec, async c => {
        var review = req.Body.ToObject<UserChannelReview>(JsSerializer);
        if (review.Email.NullOrEmpty())
          return new HttpResponseMessage(HttpStatusCode.BadRequest) {Content = new StringContent("email must be provided")};
        review.Updated = DateTime.UtcNow;
        var log = c.Resolve<ILogger>();
        var store = c.Scope.Resolve<YtStore>();
        await store.ChannelReviews.Append(new[] {review}, log);

        // if we have lots of small files, clean them up
        var files = await store.ChannelReviews.Files(review.Email).SelectManyList();
        var optimiseCfg = c.Resolve<WarehouseCfg>().Optimise;
        if (files.Count(f => f.Bytes.Bytes() < (optimiseCfg.TargetBytes * 0.9).Bytes()) > 10)
          await store.ChannelReviews.Optimise(optimiseCfg, review.Email, log: log);

        return new HttpResponseMessage(HttpStatusCode.OK);
      });

    [FunctionName("channels_reviewed")]
    public async Task<HttpResponseMessage> ChannelsReviewed([HttpTrigger(AuthorizationLevel.Anonymous, "get")]
      HttpRequest req, ExecutionContext exec) =>
      await Ctx.Run(exec, async ctx => {
        var (email, response) = EmailOrResponse(req);
        if (response != null) return response;
        var store = ctx.Scope.Resolve<YtStore>();
        var reviews = await store.ChannelReviews.Items(email).SelectManyList();
        var json = reviews.SerializeToJToken(JsonlExtensions.DefaultSettingsForJs())
          .ToCamelCaseJToken().ToString(Formatting.None);

        return new HttpResponseMessage(HttpStatusCode.OK) {
          Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
      });

    static (string email, HttpResponseMessage response) EmailOrResponse(HttpRequest req) {
      var email = req.Query["email"];
      if (email == StringValues.Empty)
        return (null, new HttpResponseMessage(HttpStatusCode.Unauthorized) {Content = new StringContent("email must be provided")});
      return (email, null);
    }

    static readonly SHA256 Sha = SHA256.Create();

    [FunctionName("video_views")]
    public async Task<HttpResponseMessage> VideoViews([HttpTrigger(AuthorizationLevel.Anonymous, "get")]
      HttpRequest req, ExecutionContext exec) =>
      await Ctx.Run(exec, async ctx => {
        var p = new {
          channelId = req.Query["channelId"].FirstOrDefault(),
          from = req.Query["from"].FirstOrDefault(),
          to = req.Query["to"].FirstOrDefault(),
          top = req.Query["top"].FirstOrDefault()?.ParseInt() ?? 80
        };

        var conditions = new List<string>();

        void Condition(object value, string expression) {
          if (value == null) return;
          conditions.Add(expression);
        }

        Condition(p.channelId, "channel_id = @channelId");
        Condition(p.from, "date >= @from");
        Condition(p.to, "date <= @to");

        var sql = @$"select top (@top) video_id, sum(views) views, sum(watch_hours) watch_hours
from video_stats {(conditions.Any() ? $"\n where {conditions.Join(" and ")}" : "")}
group by video_id
order by views desc
";

        var store = ctx.Resolve<YtStores>().Store(DataStoreType.Results);
        var cacheHash = Sha.ComputeHash((sql + JObject.FromObject(p)).ToBytesUtf8()).ToBase64String().Replace(oldChar: '/', newChar: '_');
        var path = StringPath.FromString($"cache/{cacheHash}.jsonl.gz");

        var noCache = req.Headers["Cache-Control"].Contains("no-cache");
        var (file, dur) = noCache ? default : await store.Info(path).WithDuration();
        ctx.Log.Debug("checking cache took {Duration}", dur.HumanizeShort());
        if (file == default || DateTimeOffset.UtcNow - file.Modified!.Value > 24.Hours()) {
          var sw = Stopwatch.StartNew();
          var sqlServerCfg = ctx.Resolve<SqlServerCfg>();
          using (var conn = await sqlServerCfg.OpenConnection(ctx.Log)) {
            ctx.Log.Debug("opening connection took {Duration}", sw.Elapsed.HumanizeShort());
            var rows = await conn.Query<dynamic>("video_views", sql, p, timeout: 10.Minutes());
            using var stream = await rows.ToJsonlGzStream();
            await store.Save(path, stream);
            ctx.Log.Debug("video_views - {Query} returned {Rows} took {Duration}", req.QueryString, rows.Count, sw.Elapsed.HumanizeShort());
          }
        }
        else {
          ctx.Log.Debug("video_views - cache hit for: {Query}", req.QueryString.ToString());
        }

        var res = new DataResult {
          JsonlUrl = store.Url(path).ToString()
        };
        return res.JsonResponse();
      });
  }

  public class DataResult {
    public string JsonlUrl { get; set; }
  }

  public static class HttpResponseEx {
    public static HttpResponseMessage ErrorResponse(this object o, JsonSerializerSettings settings = null) =>
      new HttpResponseMessage(HttpStatusCode.OK) {
        Content = new StringContent(o.ToJson(settings), Encoding.UTF8, "application/json")
      };

    public static HttpResponseMessage JsonResponse(this object o, JsonSerializerSettings settings = null) =>
      new HttpResponseMessage(HttpStatusCode.OK) {
        Content = new StringContent(o.ToJson(settings), Encoding.UTF8, "application/json")
      };
  }
}