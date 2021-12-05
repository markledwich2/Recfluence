using System.Globalization;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Xml;
using System.Xml.Linq;
using Flurl;
using Flurl.Http;
using Flurl.Http.Content;
using LtGt;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Newtonsoft.Json.Linq;
using YtReader.SimpleCollect;
using YtReader.Store;
using YtReader.Web;
using static System.Globalization.DateTimeStyles;
using static System.StringComparison;
using static YtReader.Yt.CommentAction;
using static YtReader.Yt.YtWebExtensions;
using static System.Text.RegularExpressions.RegexOptions;
using static YtReader.Yt.ExtraPart;

// ReSharper disable InconsistentNaming

//// a modified version of https://github.com/Tyrrrz/YoutubeExplode

namespace YtReader.Yt;

public record YtWeb(FlurlProxyClient Client, ISimpleFileStore LogStore) {
  public Task<IFlurlResponse> Send(ILogger log, string desc, IFlurlRequest req, HttpMethod verb = null, Func<HttpContent> content = null,
    Func<IFlurlResponse, bool> isTransient = null) =>
    Client.Send(desc, req, verb, content, isTransient, log);

  async Task<(HtmlDocument Doc, HttpResponseMessage Res)> GetHtml(string desc, Url url, ILogger log) {
    var res = await GetHttp(url, desc, log);
    var doc = Html.ParseDocument(await res.ContentAsString());
    return (doc, res);
  }

  async Task<HttpResponseMessage> GetHttp(string url, string desc, ILogger log, int[] transientStatus = null) {
    var res = await Send(log, desc, url.AsUrl().AsRequest(),
      isTransient: r => Client.DefaultIsTransient(r) || (transientStatus ?? Array.Empty<int>()).Contains(r.StatusCode));
    return res.ResponseMessage;
  }

  #region Channel

  const string YtUrl = "https://www.youtube.com";

  record BpBase(BpContext context);
  record BpContext(BpClient client);
  record BpClient(string hl = "en-US", string clientName = "WEB", string clientVersion = "2.20210210.08.00", int utcOffsetMinutes = 0);
  record BpFirst(string browse_id, string @params) : BpBase(new BpContext(new()));
  record BpContinue(string continuation) : BpBase(new BpContext(new()));

  static readonly string[] TimeFormats = { @"m\:ss", @"mm\:ss", @"h\:mm\:ss", @"hh\:mm\:ss" };
  static readonly Regex    ViewCountRe = new(@"(?<num>[\d,]*) view");

  record ChannelPage(string ChannelId, Url Url, HtmlDocument Doc, HttpResponseHeaders Headers, JObject InitialData, JObject Cfg) {
    public string Error() => InitialData?.Tokens("alerts[*].alertRenderer")?.FirstOrDefault(a => a.Str("type") == "ERROR")?.YtTxt("text")
      ?? InitialData?.YtTxt("contents..channelAgeGateRenderer..mainText");

    public InnerTubeCfg InnerTube() => ParseInnerTube(Headers, Cfg);
  }

  public static InnerTubeCfg ParseInnerTube(HttpResponseHeaders headers, JObject Cfg) {
    var resCookies = headers?.Cookies().KeyBy(c => c.Name);
    return new() {
      Xsrf = Cfg?.Value<string>("XSRF_TOKEN"),
      ClientVersion = Cfg?.Token("INNERTUBE_CONTEXT.client")?.Str("clientVersion"),
      Cookies = new { YSC = resCookies["YSC"].Value, VISITOR_INFO1_LIVE = resCookies["VISITOR_INFO1_LIVE"].Value },
      ApiKey = Cfg?.Str("INNERTUBE_API_KEY")
    };
  }

  public async Task<WebChannel> Channel(ILogger log, string channelId, bool expectingSubs = false) {
    if (!ValidateChannelId(channelId)) throw new($"Invalid YouTube channel ID [{channelId}].");
    var channelUrl = YtUrl.AppendPathSegments("channel", channelId);
    var (doc, res) = await GetHtml("channel page", channelUrl, log);
    var page = new ChannelPage(channelId, channelUrl, doc, res.Headers,
      await JsonFromScript(log, doc, channelUrl, ClientObject.InitialData),
      await JsonFromScript(log, doc, channelUrl, ClientObject.Cfg));
    var chan = await ParseChannel(page, expectingSubs, log);
    if (chan == null) return null;

    var error = chan.Error.HasValue();
    return chan with {
      InnerTubeCfg = !error ? page.InnerTube() : null,
      Subscriptions = !error ? () => ChannelSubscriptions(log, page) : AsyncEnumerable.Empty<ChannelSubscription>,
      Videos = !error ? () => ChannelVideos(page, log) : AsyncEnumerable.Empty<IReadOnlyCollection<YtVideoItem>>
    };
  }

  const string BrowsePath = "/youtubei/v1/browse";

  enum BrowseType {
    [EnumMember(Value = "channels")] Channel,
    [EnumMember(Value = "videos")]   Video
  }

  /// <summary>Iterates through all of the browse pages, parse the JObject to get what you need</summary>
  async IAsyncEnumerable<JObject> BrowseResults(ChannelPage page, BrowseType browseType, ILogger log) {
    var pathSuffix = browseType.EnumString();
    var browse = page.InitialData.SelectTokens(@"$..tabRenderer.endpoint")
      .OfType<JObject>().Select(e => {
        var cmd = e.SelectToken("commandMetadata.webCommandMetadata");
        return cmd == null ? null : new { ApiPath = cmd.Str("apiUrl"), Path = cmd.Str("url"), Param = e.SelectToken("browseEndpoint.params")?.Str() };
      }).NotNull()
      .FirstOrDefault(p => p.Path?.Split('/').LastOrDefault()?.ToLowerInvariant() == pathSuffix);

    if (browse == default) {
      var error = page.Error();
      if (error != null) {
        log.Information("WebScraper - Can't get videos in channel {Channel} because: {Error}", page.ChannelId, error);
        yield break;
      }

      // some channels have no videos (e.g. playlists) and there is no easy way to tell the difference. Log the parsing issue and continue
      await LogStore.LogParseError("error parsing channel page to find browse endpoint", ex: null, page.Url, page.InitialData.ToString(), "json", log);
      yield break;
    }

    var innerTube = page.InnerTube();

    if (browse.Param == null) throw new($"unable to find {pathSuffix} browse endpoint on page: {page.Url}");
    string continueToken = null;
    while (true) {
      object token = continueToken == null ? new BpFirst(page.ChannelId, browse.Param) : new BpContinue(continueToken);
      var req = YtUrl.AppendPathSegments(BrowsePath).SetQueryParam("key", innerTube.ApiKey).AsRequest();
      var desc = browseType switch { BrowseType.Channel => "channel list", BrowseType.Video => "video list", _ => default };
      var j = await Send(log, desc, req, HttpMethod.Post, () => new CapturedJsonContent(token.ToJson())).Then(r => r.JsonObject());
      continueToken = j.SelectToken("..continuationCommand.token").Str();
      yield return j;
      if (continueToken == null) break;
    }
  }

  IAsyncEnumerable<ChannelSubscription> ChannelSubscriptions(ILogger log, ChannelPage page) =>
    BrowseResults(page, BrowseType.Channel, log)
      .Select(j => j
        .SelectTokens("..gridChannelRenderer")
        .Select(c => new ChannelSubscription(c.Str("channelId"), c.YtTxt("title")) { Subs = c.YtTxt("subscriberCountText")?.ParseSubs() }))
      .SelectMany();

  bool AnyTokens(JObject j, params string[] tokens) => tokens.Any(t => j.Token(t) != null);

  bool LikelyChannelIsPlaylist(ChannelPage page) => page.InitialData != null && AnyTokens(page.InitialData,
    "onResponseReceivedActions[*].navigateAction.endpoint.commandMetadata.webCommandMetadata.webPageType",
    "header..topicChannelDetailsRenderer", "header..c4TabbedHeaderRenderer"
  );

  async Task<WebChannel> ParseChannel(ChannelPage page, bool expectingSubs, ILogger log) {
    var error = page.Error();
    var d = page.InitialData?.Token("microformat.microformatDataRenderer");
    if (d == null && error == null) {
      if (LikelyChannelIsPlaylist(page)) {
        log.Debug(
          "YtWeb - {Url} - loaded channel page with missing channel information. It has content that make it likely to be a playlist or topic.", page.Url);
        return null;
      }
      await LogStore.LogParseError("can't find channel data in initialData json", ex: null, page.Url, page.InitialData?.ToString(), "json", log);
      throw new($"Unable to parse channel data from {page.Url}");
    }
    var subs = page.InitialData?.YtTxt("header..subscriberCountText")?.ParseSubs();
    if (expectingSubs && subs == null)
      await LogStore.LogParseError("can't find subscriptions for channel", ex: null, page.Url, page.InitialData?.ToString(), "json", log);
    return new() {
      Id = page.ChannelId,
      Title = d?.Str("title"),
      LogoUrl = d?.Token("thumbnail.thumbnails")?.Select(t => t.Str("url")).LastOrDefault(),
      Subs = subs,
      Error = error,
      Keywords = d?.Token("tags")?.Values().Select(v => v.Str()).Join(" ", s => s.Match(new(@"\s")).Success ? s.InDoubleQuote() : s),
      AvailableCountries = d?.Token("availableCountries")?.Values().Select(v => v.Str()).ToArray()
    };
  }

  IAsyncEnumerable<IReadOnlyCollection<YtVideoItem>> ChannelVideos(ChannelPage page, ILogger log) =>
    BrowseResults(page, BrowseType.Video, log)
      .Select(j => j.SelectTokens("..gridVideoRenderer").Select(t => {
        var viewCountText = t.YtTxt("viewCountText");
        var parsedVideo = new YtVideoItem {
          Id = t.Str("videoId"), Title = t.YtTxt("title"),
          Duration = t.Str("..thumbnailOverlayTimeStatusRenderer.text.simpleText").TryParseTimeSpanExact(TimeFormats) ?? TimeSpan.Zero,
          Statistics =
            new(viewCountText == "No views" ? 0 : viewCountText?.Match(ViewCountRe).Groups["num"].Value.TryParseULong(NumberStyles.AllowThousands)),
          UploadDate = t.YtTxt("publishedTimeText").ParseAgo().Date() // this is very imprecise. We rely on video extra for a reliable upload date
        };
        if (parsedVideo.Statistics.ViewCount == null)
          log.Debug("Can't find views for {Video} in {Json}", parsedVideo.Id, t.ToString());
        return parsedVideo;
      }).ToArray());

  #endregion

  #region Videos

  public async Task<YtHtmlPage> GetVideoWatchPageHtmlAsync(string videoId, ILogger log) {
    var url = $"https://youtube.com/watch?v={videoId}&bpctr=9999999999&hl=en-us";
    var httpRes = await GetHttp(url, "video", log);
    var headers = httpRes.Headers;
    var raw = await httpRes.ContentAsString();
    return new(Html.ParseDocument(raw), raw, url, headers); // think about using parser than can use stream to avoid large strings using mem
  }

  public const string RestrictedVideoError = "Restricted";

  public async Task<ExtraAndParts> GetExtra(ILogger log, InnerTubeCfg innerTube, string videoId, ExtraPart[] parts, int maxComments, string channelId = null,
    string channelTitle = null) {
    var watchPats = parts.Intersect(new[] { EComment, ERec }).ToArray();
    if (watchPats.Any())
      return await GetExtraFromWatchPage(log, videoId, parts, maxComments, channelId, channelTitle) with { CollectSource = "Watch page" };

    if (innerTube == null) throw new($"{nameof(innerTube)} required");
    log = log.ForContext("VideoId", videoId);
    var player = await GetPlayVideo(innerTube, videoId, log);
    var v = await ParsePlayerVideo(player, videoId, channelId, channelTitle, PlayerUrl().AppendPathSegment(videoId), log);
    VideoCaption caption = null;
    if (parts.Contains(ECaption) && (v.Error == null || v.CaptionTracks?.Any() == true))
      caption = await GetCaption(v, log);
    return new(v) { Caption = caption, CollectSource = "Innertube Player" };
  }

  Url PlayerUrl() => YtUrl.AppendPathSegment("youtubei/v1/player");

  async Task<JObject> GetPlayVideo(InnerTubeCfg innerTube, string videoId, ILogger log) {
    var req = PlayerUrl()
      .SetQueryParam("key", innerTube.ApiKey)
      .WithHeader("x-youtube-client-name", "1")
      .WithHeader("x-youtube-client-version", innerTube.ClientVersion)
      .WithCookies(innerTube.Cookies);
    var res = await Send(log, "play video", req, HttpMethod.Post, () => new StringContent(new {
      context = new {
        client = new { clientName = "WEB", clientVersion = innerTube.ClientVersion }
      },
      videoId
    }.ToJson(new())));
    return await res.JsonObject();
  }

  async Task<VideoExtra> ParsePlayerVideo(JObject player, string videoId, string channelId, string channelTitle, Url url, ILogger log) {
    var v = new VideoExtra {
      VideoId = videoId,
      ChannelId = channelId,
      ChannelTitle = channelTitle,
      Updated = DateTime.UtcNow,
      Source = ScrapeSource.Web,
      Platform = Platform.YouTube
    };

    var status = player.Str("playabilityStatus.status");
    if (status != null && status != "OK") {
      var statusJ = player["playabilityStatus"];
      var errorRender = statusJ?.Token("errorScreen.playerErrorMessageRenderer");
      return v with {
        Error = statusJ?.Str("reason") ?? errorRender?.YtTxt("reason"),
        SubError = errorRender?.YtTxt("subreason")
      };
    }

    var d = player.Token("videoDetails");
    var m = player.Token("microformat.playerMicroformatRenderer");

    if (d == null || m == null) {
      await LogStore.LogParseError("can't find videoDetails/microformat.playerMicroformatRenderer", ex: null, url, player.ToString(), "json", log);
      return null;
    }

    string availableError = null;
    if (m.Token("isUnlisted")?.Value<bool>() == true) availableError = "Unlisted";

    return v with {
      // some videos are listed under a channels playlist, but when you click on the vidoe, its channel is under enother (e.g. _iYT8eg1F8s)
      // Record them as the channelId of the playlist.
      ChannelId = d.Str("channelId") ?? channelId,
      ChannelTitle = d.Str("author") ?? channelTitle,
      Description = d.Str("shortDescription"),
      Duration = d.Str("lengthSeconds")?.TryParseInt()?.Seconds(),
      Keywords = d.Token("keywords").NotNull().Values<string>().ToArray(),
      Title = d.Str("title"),
      UploadDate = m.Str("uploadDate")?.TryParseDateExact("yyyy-MM-dd", AssumeUniversal)?.ToUniversalTime(),
      Statistics = new(d.Str("viewCount")?.TryParseULong()) { AverageRating = d.Str("averageRating")?.TryParseDouble() },
      Category = m.Str("category"),
      IsLive = d.Token("isLive")?.Value<bool>(),
      Error = availableError,
      CaptionTracks = GetCaptionTracks(player).ToArray()
    };
  }

  public async Task<InnerTubeCfg> InnerTubeFromVideoPage(string videoId, ILogger log) {
    var watchPage = await GetVideoWatchPageHtmlAsync(videoId, log);
    return ParseInnerTube(watchPage.Headers, await JsonFromScript(log, watchPage.Html, watchPage.Url, ClientObject.Cfg));
  }

  /// <summary>Loads the watch page, and the video info dic to get: recommendations and video details (including errors)</summary>
  public async Task<ExtraAndParts>
    GetExtraFromWatchPage(ILogger log, string videoId, ExtraPart[] parts, int maxComments, string channelId = null, string channelTitle = null) {
    log = log.ForContext("VideoId", videoId);
    var watchPage = await GetVideoWatchPageHtmlAsync(videoId, log);

    var html = watchPage.Html;
    var initialData = await JsonFromScript(log, html, videoId, ClientObject.InitialData);
    var playerResponse = await JsonFromScript(log, html, videoId, ClientObject.PlayerResponse);
    var playerVideo = await ParsePlayerVideo(playerResponse, videoId, channelId, channelTitle, PlayerUrl().AppendPathSegment(videoId), log);
    var v = SupplementWatchVideo(playerVideo, html, initialData);
    if (v.Error == null) {
      var restrictedMode = html.QueryElements("head > meta[property=\"og:restrictions:age\"]").FirstOrDefault()?.GetAttribute("content")?.Value == "18+";
      if (restrictedMode) {
        v.Error = RestrictedVideoError;
        v.SubError = "Unable to find recommended video because it is age restricted and requires to log in";
      }
    }
    if (v.Error == null) {
      v.SubError = html.QueryElements("#unavailable-submessage").FirstOrDefault()?.GetInnerText();
      if (v.SubError == "") v.SubError = null;
      if (v.SubError.HasValue()) // all pages have the error, but not a sub-error
        v.Error = html.QueryElements("#unavailable-message").FirstOrDefault()?.GetInnerText();
    }
    if (v.Error == null) {
      var badgeLabels =
        initialData?.SelectTokens(
          "contents.twoColumnWatchNextResults.results.results.contents[*].videoPrimaryInfoRenderer.badges[*].metadataBadgeRenderer.label");
      if (badgeLabels?.Any(b => b.Value<string>() == "Unlisted") == true)
        v.Error = "Unlisted";
    }
    if (v.Error != null) return new(v);

    var recs = Array.Empty<Rec>();
    if (parts.Contains(ERec))
      recs = await GetRecs2(log, html, videoId);
    var comments = Array.Empty<VideoComment>();
    if (parts.Contains(EComment))
      comments = await GetComments(log, videoId, initialData, watchPage, maxComments).Then(c => c.ToArray());

    VideoCaption caption = null;
    if (parts.Contains(ECaption))
      caption = await GetCaption(v, log);

    return new(v) {
      Caption = caption,
      Comments = comments,
      Recs = recs
    };
  }

  public async Task<Rec[]> GetRecs2(ILogger log, HtmlDocument html, string videoId) {
    var jInit = await JsonFromScript(log, html, videoId, ClientObject.InitialData);
    if (jInit == null) return null;
    var resultsSel = "$.contents.twoColumnWatchNextResults.secondaryResults.secondaryResults.results";
    var jResults = (JArray)jInit.SelectToken(resultsSel);
    if (jResults == null) {
      log.Warning("WebScraper - Unable to find recs for {VideoId}", videoId);
      return new Rec[] { };
    }
    var recs = jResults
      .OfType<JObject>()
      .Select(j => j.SelectToken("compactAutoplayRenderer.contents[0].compactVideoRenderer") ?? j.SelectToken("compactVideoRenderer"))
      .Where(j => j != null)
      .Select((j, i) => {
        var viewText = (j.SelectToken("viewCountText.simpleText") ?? j.SelectToken("viewCountText.runs[0].text"))?.Value<string>();
        return new Rec {
          ToVideoId = j.Value<string>("videoId"),
          ToVideoTitle = j["title"]?.Value<string>("simpleText") ?? j.SelectToken("title.runs[0].text")?.Value<string>(),
          ToChannelId = j.Value<string>("channelId") ?? j.SelectToken("longBylineText.runs[0].navigationEndpoint.browseEndpoint.browseId")?.Value<string>(),
          ToChannelTitle = j.SelectToken("longBylineText.runs[0].text")?.Value<string>(),
          Rank = i + 1,
          Source = ScrapeSource.Web,
          ToViews = viewText?.ParseViews(),
          ToUploadDate = j.SelectToken("publishedTimeText.simpleText")?.Str().ParseAgo().Date(),
          ForYou = ParseForYou(viewText)
        };
      }).ToArray();
    return recs;
  }

  static readonly Regex ClientObjectsRe     = new(@"(?:window\[""(?<window>\w+)""\]|var\s+(?<var>\w+))\s*=\s*(?<json>{.*})\s*;", Compiled | Singleline);
  static readonly Regex ClientObjectCleanRe = new(@"{\w*?};", Compiled);
  static readonly Regex ClientObjectsRe2    = new(@"(?<var>\w+)\.set\((?<json>{.*?})\);", Compiled | Singleline);

  public static class ClientObject {
    public const string InitialData    = "ytInitialData";
    public const string Cfg            = "ytcfg";
    public const string PlayerResponse = "ytInitialPlayerResponse";
  }

  public async Task<JObject> JsonFromScript(ILogger log, HtmlDocument html, Url url, string clientObjectName) {
    var scripts = html.QueryElements("script")
      .SelectMany(s => s.Children.OfType<HtmlText>()).Select(h => h.Content).ToList();

    string Gv(Match m, string group) => m.Groups[group].Value.HasValue() ? m.Groups[group].Value : null;

    var objects = scripts
      .Select(s => ClientObjectCleanRe.Replace(s, "")).SelectMany(s => ClientObjectsRe.Matches(s)) // var = {} style
      .Concat(scripts.SelectMany(s => ClientObjectsRe2.Matches(s))) // window.var.set({}) style
      .Select(m => new { Var = Gv(m, "window") ?? Gv(m, "var"), Json = m.Groups["json"].Value })
      .Where(m => m.Var == clientObjectName)
      .Select(m => Def.Fun(() => m.Json.ParseJObject()).Try()).ToArray();

    //var errors = objects.Select(j => j.Ex).NotNull().ToArray();
    //if(errors.Any())
    //log.Debug("{Scope} - client scripts be parsed: {Errors}", Scope, errors.Select(e => e.Message).ToArray());

    var jObj = objects.Select(j => j.Value).NotNull().FirstOrDefault();
    if (jObj == null)
      await LogStore.LogParseError($"Unable to parse {clientObjectName} json from watch page", ex: null, url, html.ToHtml(), "html", log);
    return jObj;
  }

  static readonly Regex LikeDislikeRe = new(@"(?<num>[\d,]+)\s*(?<type>like|dislike)");

  /// <summary>gets video data from the video watch page</summary>
  /// <returns></returns>
  static VideoExtra SupplementWatchVideo(VideoExtra v, HtmlDocument html, JObject ytInit) {
    if (v.Error.HasValue()) return v;
    var metaDic = html.Els("head > div[itemtype=\"http://schema.org/VideoObject\"] > *[itemprop]")
      .Select(a => new { prop = a.Str("itemprop"), val = a.Str("content"), node = a })
      .KeyBy(a => a.prop);

    string MetaTag(string prop) => metaDic[prop]?.val;
    var likeDislikeMatches = ytInit?.SelectTokens("$..topLevelButtons[*].toggleButtonRenderer.defaultText..label")
      .Select(t => t.Value<string>().Match(LikeDislikeRe)).ToArray();
    ulong? LikeDislikeVal(string type) => likeDislikeMatches?.FirstOrDefault(t => t.Groups["type"].Value == type)?.Groups["num"].Value.TryParseULong();

    var res = v with {
      UploadDate = v.UploadDate ?? MetaTag("uploadDate")?.TryParseDateExact("yyyy-MM-dd", AssumeUniversal)?.ToUniversalTime(),
      Duration = v.Duration ?? MetaTag("duration").Dot(XmlConvert.ToTimeSpan), // 8061 standard timespan
      Statistics = new(v.Statistics.ViewCount, LikeDislikeVal("like"), LikeDislikeVal("dislike"))
    };
    return res;
  }

  #region Comments

  string GetCToken(JToken continueSection) => continueSection?.Str("continuations[0].nextContinuationData.continuation")
    ?? continueSection?.Str("contents[*].continuationItemRenderer.continuationEndpoint.continuationCommand.token");

  async Task<IReadOnlyCollection<VideoComment>> GetComments(ILogger log, string videoId, JObject ytInitialData, YtHtmlPage page, int maxComments) {
    var jCfg = await JsonFromScript(log, page.Html, page.Url, ClientObject.Cfg) ?? throw new("Can't load comments because no ytcfg was found on video page");

    CommentsCfg CommentCfgFromVideoPage() {
      var contSection = ytInitialData?.Tokens("$.contents.twoColumnWatchNextResults.results.results.contents[*].itemSectionRenderer")
        .FirstOrDefault(c => c.Str("sectionIdentifier") == "comment-item-section");
      var contEndpoint = contSection?.Token("");
      var cToken = GetCToken(contSection);
      var apiUrl = contEndpoint?.Str("contents[*].continuationItemRenderer.continuationEndpoint.commandMetadata.webCommandMetadata.apiUrl");
      return new(ParseInnerTube(page.Headers, jCfg), cToken, apiUrl);
    }

    var loadedComments = 0;
    var comments = await Comments(videoId, CommentCfgFromVideoPage(), log)
      .TakeWhileInclusive(b => Interlocked.Add(ref loadedComments, b.Length) < maxComments)
      .SelectManyList();
    log.Debug("YtWeb - loaded {Comments} comments for video {VideoId}", comments.Count, videoId);
    return comments;
  }

  record CommentsCfg(InnerTubeCfg InnerTube, string CToken, string ApiUrl);
  record CommentResult(VideoComment Comment, string ReplyContinuation = null);

  async IAsyncEnumerable<VideoComment[]> Comments(string videoId, CommentsCfg cfg, ILogger log) {
    log = log.ForContext("VideoId", videoId);

    async Task<(IFlurlResponse, IFlurlRequest req)> CommentRequest(CommentAction action, string continuation) {
      var req = $"https://www.youtube.com/comment_service_ajax?{action.EnumString()}=1&ctoken={continuation}&type=next".AsUrl()
        .WithHeader("x-youtube-client-name", "1")
        .WithHeader("x-youtube-client-version", cfg.InnerTube.ClientVersion)
        .WithCookies(cfg.InnerTube.Cookies);
      var res = await Send(log, "get comments", req, HttpMethod.Post, () => req.FormUrlContent(new { session_token = cfg.InnerTube.Xsrf }),
        r => HttpExtensions.IsTransientError(r.StatusCode) || r.StatusCode.In(400));
      return (res, req);
    }

    async Task<(IFlurlResponse, IFlurlRequest req)> CommentRequestV2(string cToken) {
      var req = YtUrl.AppendPathSegment(cfg.ApiUrl)
        .WithHeader("x-youtube-client-name", "1")
        .WithHeader("x-youtube-client-version", cfg.InnerTube.ClientVersion)
        .SetQueryParam("key", cfg.InnerTube.ApiKey)
        .WithCookies(cfg.InnerTube.Cookies);
      var res = await Send(log, "get comments", req, HttpMethod.Post, () => new StringContent(new {
        context = new {
          client = new { clientName = "WEB", clientVersion = cfg.InnerTube.ClientVersion }
        },
        continuation = cToken
      }.ToJson(new())));
      return (res, req);
    }

    async Task<(CommentResult[] Comments, string Continuation)> RequestComments(string continuation, VideoComment parent = null) {
      var action = parent == null ? AComments : AReplies;
      var (res, req) = cfg.ApiUrl.HasValue() ? await CommentRequestV2(continuation) : await CommentRequest(action, continuation);
      var getRootJ = cfg.ApiUrl.HasValue()
        ? res.JsonObject()
        : action switch {
          AComments => res.JsonObject(),
          AReplies => res.JsonArray().Then(a => a.Children<JObject>().FirstOrDefault(j => j["response"] != null)),
          _ => throw new NotImplementedException()
        };
      var rootJ = await getRootJ.Swallow(e => log.Warning(e, "YtWeb - couldn't load comments. {Curl}: {Error}", req.FormatCurl(), e.Message)) ??
        new JObject();
      var comments = (action switch {
        AComments => from t in rootJ.Tokens("$..commentThreadRenderer")
          let c = t.Token("comment.commentRenderer")
          let r = t.Token("replies.commentRepliesRenderer")
          where c != null
          select new CommentResult(ParseComment(videoId, c, parent), GetCToken(r)), // as of 15 july returns different format
        AReplies => rootJ.Tokens("$..commentRenderer").Select(c => new CommentResult(ParseComment(videoId, c, parent))),
        _ => throw new NotImplementedException()
      }).ToArray();
      var nextContinue = rootJ.Str("response.continuationContents.itemSectionContinuation.continuations[0].nextContinuationData.continuation")
        ?? rootJ.Str("onResponseReceivedEndpoints[*].reloadContinuationItemsCommand.continuationItems[*].continuationItemRenderer..continuationCommand.token")
        ?? rootJ.Str("onResponseReceivedEndpoints[*].appendContinuationItemsAction.continuationItems[*].continuationItemRenderer..continuationCommand.token");
      return (comments, nextContinue);
    }

    async IAsyncEnumerable<(CommentResult[] Comments, string Continuation)> AllComments(string continuation, VideoComment parent = null) {
      while (continuation != null) {
        var comments = await RequestComments(continuation, parent);
        continuation = comments.Continuation;
        if (comments.Comments.None()) yield break;
        yield return comments;
      }
    }

    await foreach (var ((comments, _), batch) in AllComments(cfg.CToken).Select((b, i) => (b, i))) {
      var threads = comments.Select(c => c.Comment).ToArray();
      log.Verbose("YtWeb - loaded {Threads} threads in batch {Batch} for video {Video}", threads.Length, batch, videoId);
      yield return threads;
      var allReplies = comments.Where(c => c.ReplyContinuation != null)
        .BlockDo(async t => await AllComments(t.ReplyContinuation, t.Comment).ToListAsync(), parallel: 4);
      await foreach (var replies in allReplies) {
        var replyComments = replies.SelectMany(r => r.Comments.Select(c => c.Comment)).ToArray();
        log.Verbose("YtWeb - loaded {Replies} replies in batch {Batch} for video {Video}", threads.Length, batch, videoId);
        yield return replyComments;
      }
    }
  }

  static VideoComment ParseComment(string videoId, JToken c, VideoComment parent) =>
    new() {
      CommentId = c!.Str("commentId"),
      VideoId = videoId,
      Author = c.Token("authorText.simpleText")?.Str(),
      AuthorChannelId = c.Token("authorEndpoint.browseEndpoint.browseId")?.Str(),
      Comment = c.Tokens("contentText.runs[*].text").Join(" "),
      Created = c.Token("publishedTimeText.runs[0].text")?.Str().ParseAgo().Date(),
      Likes = c.Str("likeCount")?.TryParseInt(),
      IsChannelOwner = c.Value<bool>("authorIsChannelOwner"),
      ReplyToCommentId = parent?.CommentId,
      Updated = DateTime.UtcNow
    };

  #endregion

  #endregion

  #region Captions

  async Task<VideoCaption> GetCaption(VideoExtra v, ILogger log) {
    var videoLog = log.ForContext("VideoId", v.VideoId);
    VideoCaption caption = new() {
      ChannelId = v.ChannelId,
      VideoId = v.VideoId,
      Updated = DateTime.Now
    };
    try {
      var enTrack = v.CaptionTracks?.Where(t => t.Language.Code.Split("-").First() == "en")
        .OrderByDescending(t => t.Default).ThenBy(t => t.IsAutoGenerated).FirstOrDefault(); // we want the default caption track, or manual if no default
      if (enTrack == null)
        return caption;
      var track = await GetClosedCaptionTrackAsync(enTrack, videoLog);
      return caption with {
        Info = track.Info,
        Captions = track.Captions
      };
    }
    catch (Exception ex) {
      ex.ThrowIfUnrecoverable();
      videoLog.Warning(ex, "Unable to get captions for {VideoID}: {Error}", v.VideoId, ex.Message);
      return null;
    }
  }

  CaptionTrackInfo[] GetCaptionTracks(JToken playerResponse) {
    var c = playerResponse.Token("captions.playerCaptionsTracklistRenderer");
    var defaultCaptionIndex = c?.Token($"audioTracks[{c.Token("defaultAudioTrackIndex")?.Value<int>() ?? 0}].defaultCaptionTrackIndex")?.Value<int>();
    return c?.Token("captionTracks")?.Select((j, i) => new CaptionTrackInfo(
      new UriBuilder(j.Str("baseUrl")).WithParameter("format", "3").ToString(),
      new(j.Str("languageCode"), j.YtTxt("name")),
      j.Str("vssId").StartsWith("a.", OrdinalIgnoreCase)
    ) { Default = i == defaultCaptionIndex }).ToArray() ?? Array.Empty<CaptionTrackInfo>();
  }

  public async Task<CaptionTrack> GetClosedCaptionTrackAsync(CaptionTrackInfo info, ILogger log) {
    var trackXml = await GetClosedCaptionTrackXmlAsync(info.Url, log);
    var captions = from captionXml in trackXml.Descendants("p")
      let text = (string)captionXml
      where !text.IsNullOrWhiteSpace()
      let offset = (double?)captionXml.Attribute("t")
      let duration = (double?)captionXml.Attribute("d")
      select new CaptionLine(text, offset?.Milliseconds(), duration?.Milliseconds());
    return new(info, captions.ToList());
  }

  // filters control characters but allows only properly-formed surrogate sequences
  static readonly Regex InvalidXml = new(
    @"(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F\uFEFF\uFFFE\uFFFF]",
    Compiled);

  /// <summary>removes any unusual unicode characters that can't be encoded into XML</summary>
  public static string RemoveInvalidXmlChars(string text) => text == null ? null : InvalidXml.Replace(text, "");

  async Task<XElement> GetClosedCaptionTrackXmlAsync(string url, ILogger log) {
    var raw = await GetHttp(url, "caption", log);
    var text = RemoveInvalidXmlChars(await raw.Content.ReadAsStringAsync());
    var xml = XElement.Parse(text, LoadOptions.PreserveWhitespace);
    return xml.StripNamespaces();
  }

  #endregion
}

public record InnerTubeCfg {
  public string Xsrf          { get; init; }
  public string ClientVersion { get; init; }
  public object Cookies       { get; init; }
  public string ApiKey        { get; init; }
}

public record YtHtmlPage(HtmlDocument Html, string Raw, string Url, HttpResponseHeaders Headers);

enum CommentAction {
  [EnumMember(Value = "action_get_comments")]
  AComments,
  [EnumMember(Value = "action_get_comment_replies")]
  AReplies
}

public record WebChannel {
  public string                                                   Id                 { get; init; }
  public string                                                   Title              { get; init; }
  public string                                                   LogoUrl            { get; init; }
  public ulong?                                                   Subs               { get; init; }
  public string                                                   Error              { get; init; }
  public string                                                   Keywords           { get; init; }
  public string[]                                                 AvailableCountries { get; set; }
  public Func<IAsyncEnumerable<IReadOnlyCollection<YtVideoItem>>> Videos             { get; init; }
  public Func<IAsyncEnumerable<ChannelSubscription>>              Subscriptions      { get; init; }
  public InnerTubeCfg                                             InnerTubeCfg       { get; init; }
}

public record Rec {
  public string        ToVideoId      { get; init; }
  public string        ToVideoTitle   { get; init; }
  public string        ToChannelTitle { get; init; }
  public string        ToChannelId    { get; init; }
  public ScrapeSource? Source         { get; init; }
  public int           Rank           { get; init; }
  public long?         ToViews        { get; init; }
  public DateTime?     ToUploadDate   { get; init; }
  public bool          ForYou         { get; init; }
}

public enum ScrapeSource {
  Web,
  Api,
  Chrome
}

public enum ExtraPart {
  [EnumMember(Value = "extra")]   EExtra,
  [EnumMember(Value = "rec")]     ERec,
  [EnumMember(Value = "comment")] EComment,
  [EnumMember(Value = "caption")] ECaption,
  /// <summary> If specified will perform transcription ourselves if needed </summary>
  [EnumMember(Value = "transcribe")] [CollectPart(Explicit = true)]
  ETranscribe
}