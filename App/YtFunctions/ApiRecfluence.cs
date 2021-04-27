using System;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Mutuo.Etl.Blob;
using Nest;
using Newtonsoft.Json;
using Serilog;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using YtReader.Search;
using YtReader.Store;
using YtReader.Web;
using static System.Net.HttpStatusCode;
using static YtFunctions.HttpResponseEx;

namespace YtFunctions {
  public record ApiRecfluence(YtStore Store, WarehouseCfg Wh, ILogger Log, ElasticClient Es, StorageCfg StoreCfg) {
    /// <summary>Use the Json.net defaults because we want to keep original name casings so that we aren't re-casing the db in
    ///   different formats</summary>
    static readonly JsonSerializerSettings JPlain = new() { Formatting = Formatting.None };
    static readonly JsonSerializerSettings JDefault = JsonlExtensions.DefaultSettingsForJs();

    [Function("video")]
    public Task<HttpResponseData> Video([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "video/{videoId}")]
      HttpRequestData req, string videoId) => R(async () => {
        if (videoId.NullOrEmpty()) return req.TextResponse($"video id not provided", NotFound);
        var videoRes = await Es.GetAsync<EsVideo>(videoId);
        var video = videoRes?.Source;
        if (video == null) return req.TextResponse($"video `{videoId}` not found", NotFound);
        var channelRes = await Es.GetAsync<EsChannel>(video.channel_id);
        var channel = channelRes.Source;
        return channel == null ? req.TextResponse($"channel `{video.channel_id}` not found", NotFound) 
          : req.JsonResponse(new { video, channel }, OK, JPlain);
      });

    [Function("captions")]
    public Task<HttpResponseData> Captions([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "captions/{videoId}")]
      HttpRequestData req, string videoId) => R(async () => {
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
        return req.JsonResponse(captions, OK, JPlain);
      });

    [Function("search")]
    public Task<HttpResponseData> LogSearch([HttpTrigger(AuthorizationLevel.Anonymous, "put")]
      HttpRequestData req) => R(async () => {
        // get user query, filters etc.. from query string
        var search = req.Body.ToObject<UserSearchWithUpdated>();
        await Store.Searches.Append(new[] { search });
        return req.CreateResponse(OK);
    });

    [Function("channel_review")]
    public Task<HttpResponseData> ChannelReview([HttpTrigger(AuthorizationLevel.Anonymous, "put")]
      HttpRequestData req) => R(async () => {
        var review = req.Body.ToObject<UserChannelReview>(JDefault);
        if (review.Email.NullOrEmpty())
          return req.TextResponse( "email must be provided", BadRequest);
        review.Updated = DateTime.UtcNow;
        await Store.ChannelReviews.Append(new[] { review }, Log);
        // if we have lots of small files, clean them up
        var files = await Store.ChannelReviews.Files(review.Email).SelectManyList();
        var optimiseCfg = Wh.Optimise;
        if (files.Count(f => f.Bytes?.Bytes() < (optimiseCfg.TargetBytes * 0.9).Bytes()) > 10)
          await Store.ChannelReviews.Optimise(optimiseCfg, review.Email, log: Log);
        return req.CreateResponse(OK);
      });
    
    record EmailQuery(string Email);

    [Function("channels_reviewed")]
    public Task<HttpResponseData> ChannelsReviewed([HttpTrigger(AuthorizationLevel.Anonymous, "get")]
      HttpRequestData req) => R(async () => {
        var email = req.Url.QueryObject<EmailQuery>()?.Email;
        if (email == null) return req.TextResponse("email must be provided", Unauthorized);
        var reviews = await Store.ChannelReviews.Items(email).SelectManyList();
        var json = reviews.SerializeToJToken(JsonlExtensions.DefaultSettingsForJs())
          .ToCamelCaseJToken().ToString(Formatting.None);
        return req.JsonResponse(json);
      });

    
  }
}