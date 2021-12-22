using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Humanizer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Mutuo.Etl.Blob;
using Nest;
using Newtonsoft.Json;
using Serilog;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;
using YtReader.Data;
using YtReader.Store;
using YtReader.Web;
using static System.Net.Http.HttpMethod;
using static System.Net.HttpStatusCode;
using static YtFunctions.HttpResponseEx;

namespace YtFunctions;

public record ApiRecfluence(YtStore Store, WarehouseCfg Wh, ILogger Log, ElasticClient Es, StorageCfg StoreCfg, ElasticCfg EsCfg) {
  /// <summary>Use the Json.net defaults because we want to keep original name casings so that we aren't re-casing the db in
  ///   different formats</summary>
  static readonly JsonSerializerSettings JPlain = new() { Formatting = Formatting.None };
  static readonly JsonSerializerSettings JDefault = JsonlExtensions.DefaultSettingsForJs();

  /*[Function("video")]
  public Task<HttpResponseData> Video([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "video/{videoId}")] HttpRequestData req, string videoId) => R(
    async () => {
      if (videoId.NullOrEmpty()) return req.TextResponse("video id not provided", NotFound);
      var videoRes = await Es.GetAsync<EsVideo>(videoId);
      var video = videoRes?.Source;
      if (video == null) return req.TextResponse($"video `{videoId}` not found", NotFound);
      var channelRes = await Es.GetAsync<EsChannel>(video.channel_id);
      var channel = channelRes.Source;
      return channel == null
        ? req.TextResponse($"channel `{video.channel_id}` not found", NotFound)
        : req.JsonResponse(new { video, channel }, OK, JPlain);
    });

  [Function("captions")]
  public Task<HttpResponseData>
    Captions([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "captions/{videoId}")] HttpRequestData req, string videoId) => R(async () => {
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
  });*/

  [Function("search")]
  public Task<HttpResponseData> LogSearch([HttpTrigger(AuthorizationLevel.Anonymous, "put")] HttpRequestData req) => R(async () => {
    await using var searchSink = Store.Search();
    // get user query, filters etc.. from query string
    var search = req.Body.ToObject<UserSearchWithUpdated>();
    await searchSink.Append(search);
    return req.CreateResponse(OK);
  });

  readonly JsonlStore<UserChannelReview> ChannelReviewStore = Store.ChannelReview();
  
  [Function("channel_review")]
  public Task<HttpResponseData> ChannelReview([HttpTrigger(AuthorizationLevel.Anonymous, "put")] HttpRequestData req) => R(async () => {
    var review = req.Body.ToObject<UserChannelReview>(JDefault);
    if (review.Email.NullOrEmpty())
      return req.TextResponse("email must be provided", BadRequest);
    review.Updated = DateTime.UtcNow;


    await ChannelReviewStore.Append(review);
    // if we have lots of small files, clean them up
    var files = await ChannelReviewStore.Files(review.Email).SelectManyList();
    var optimiseCfg = Wh.Optimise;
    if (files.Count(f => f.Bytes?.Bytes() < (optimiseCfg.TargetBytes * 0.9).Bytes()) > 10)
      await ChannelReviewStore.Optimise(optimiseCfg, review.Email, log: Log);
    return req.CreateResponse(OK);
  });

  record EmailQuery(string Email);

  [Function("channels_reviewed")]
  public Task<HttpResponseData> ChannelsReviewed([HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req) => R(async () => {
    var email = req.Url.QueryObject<EmailQuery>()?.Email;
    if (email == null) return req.TextResponse("email must be provided", Unauthorized);
    var reviews = await ChannelReviewStore.Items(email).SelectManyList();
    var json = reviews.SerializeToJToken(JsonlExtensions.DefaultSettingsForJs())
      .ToCamelCaseJToken().ToString(Formatting.None);
    return req.JsonResponse(json);
  });

// Pass the handler to httpclient(from you are calling api)

  readonly IFlurlClient Flurl = new FlurlClient(new HttpClient(new HttpClientHandler {
    ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
  }));

  [Function("es")]
  public Task<HttpResponseData> EsRequest([HttpTrigger(AuthorizationLevel.Anonymous, "post", "put", "get", Route = "es/{**path}")] HttpRequestData req,
    string path) => R(async () => {
    if (path.IsNullOrWhiteSpace()) return req.TextResponse("need to include path", BadRequest);

    var url = new Url(EsCfg.Url).AppendPathSegment(path);
    url.Query = req.Url.Query;  
    var esReq = Flurl.Request(url).AllowAnyHttpStatus().WithBasicAuth(EsCfg.PublicCreds);
    foreach (var (k, v) in req.Headers.SelectMany(g => g.Value.Select(v => (k: g.Key, v))).Where(h => h.k.ToLowerInvariant() != "host"))
      esReq.Headers.Add(k, v);

    var body = await req.ReadAsStringAsync().Then(s => s == null ? null : new StringContent(s));
    var verb = req.Method switch {
      "PUT" => Put,
      "GET" => Get,
      "POST" => Post,
      _ => throw new($"method {req.Method} not implemented")
    };
    
    var curl = await esReq.FormatCurl(verb, () => body);
    var sw = Stopwatch.StartNew();
    var res = await esReq.SendAsync(verb, body);
    if (res.StatusCode != 200) {
      var msg = await res.GetStringAsync();
      Log.Warning("{Curl} returned ({Status}) : {Message}", curl, res.StatusCode, msg);
      return req.TextResponse(msg, (HttpStatusCode)res.StatusCode);
    }
    var json = await res.GetStringAsync();
    Log.Debug("es succeeded in {Dur}\n{Curl}", sw.Elapsed.HumanizeShort(), curl);
    return req.JsonResponse(json, (HttpStatusCode)res.StatusCode);
  });
}