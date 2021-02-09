using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Mutuo.Etl.Blob;
using Nest;
using Newtonsoft.Json;
using Serilog;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using YtReader.Search;
using YtReader.Store;
using static System.Net.HttpStatusCode;
using static YtFunctions.HttpResponseEx;

namespace YtFunctions {
  public record ApiRecfluence(YtStore Store, WarehouseCfg Wh, ILogger Log, ElasticClient Es) {
    /// <summary>Use the Json.net defaults because we want to keep original name casings so that we aren't re-casing the db in
    ///   different formats</summary>
    static readonly JsonSerializerSettings JPlain = new() { Formatting = Formatting.None };
    static readonly JsonSerializerSettings JDefault = JsonlExtensions.DefaultSettingsForJs();

    [FunctionName("video")]
    public Task<HttpResponseData> Video([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "video/{videoId}")]
      HttpRequestData req) => R(async () => {
        var videoId = req.Params["videoId"];
        var videoRes = await Es.GetAsync<EsVideo>(videoId);
        var video = videoRes?.Source;
        if (video == null) return new(NotFound, $"video `{videoId}` not found");
        var channelRes = await Es.GetAsync<EsChannel>(video.channel_id);
        var channel = channelRes.Source;
        return channel == null ? new(NotFound, $"channel `{video.channel_id}` not found") : new { video, channel }.JsonResponse(JPlain);
      });

    [FunctionName("captions")]
    public Task<HttpResponseData> Captions([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "captions/{videoId}")]
      HttpRequestData req) => R(async () => {
        var videoId = req.Params["videoId"];
        var res = await Es.SearchAsync<EsCaption>(s => s
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
        return captions.JsonResponse(JPlain);
      });

    [FunctionName("search")]
    public Task<HttpResponseData> LogSearch([HttpTrigger(AuthorizationLevel.Anonymous, "put")]
      HttpRequestData req) => R(async () => {
        // get user query, filters etc.. from query string
        var search = req.Body.ToObject<UserSearchWithUpdated>();
        await Store.Searches.Append(new[] { search });
        return new(OK);
      });

    [FunctionName("channel_review")]
    public Task<HttpResponseData> ChannelReview([HttpTrigger(AuthorizationLevel.Anonymous, "put")]
      HttpRequestData req) => R(async () => {
        var review = req.Body.ToObject<UserChannelReview>(JDefault);
        if (review.Email.NullOrEmpty())
          return new(BadRequest, "email must be provided");
        review.Updated = DateTime.UtcNow;
        await Store.ChannelReviews.Append(new[] { review }, Log);
        // if we have lots of small files, clean them up
        var files = await Store.ChannelReviews.Files(review.Email).SelectManyList();
        var optimiseCfg = Wh.Optimise;
        if (files.Count(f => f.Bytes?.Bytes() < (optimiseCfg.TargetBytes * 0.9).Bytes()) > 10)
          await Store.ChannelReviews.Optimise(optimiseCfg, review.Email, log: Log);
        return new(OK);
      });

    [FunctionName("channels_reviewed")]
    public Task<HttpResponseData> ChannelsReviewed([HttpTrigger(AuthorizationLevel.Anonymous, "get")]
      HttpRequestData req) => R(async () => {
        var (email, response) = EmailOrResponse(req);
        if (response != null) return response;

        var reviews = await Store.ChannelReviews.Items(email).SelectManyList();
        var json = reviews.SerializeToJToken(JsonlExtensions.DefaultSettingsForJs())
          .ToCamelCaseJToken().ToString(Formatting.None);

        return new HttpResponseData(OK, json).WithJsonContentHeaders();
      });

    static (string email, HttpResponseData response) EmailOrResponse(HttpRequestData req) => req.Query.TryGetValue("email", out var email)
      ? (email, null) : (null, new(Unauthorized, "email must be provided"));
  }
}