using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using Flurl;
using Flurl.Http;
using Flurl.Http.Content;
using Humanizer;
using LtGt;
using Mutuo.Etl.Blob;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using static System.StringComparison;
using static YtReader.Yt.CommentAction;
using static YtReader.Yt.YtWebExtensions;

// ReSharper disable InconsistentNaming

//// a modified version of https://github.com/Tyrrrz/YoutubeExplode

namespace YtReader.Yt {
  public record YtWeb(FlurlProxyClient Client, ISimpleFileStore LogStore) {
    public Task<IFlurlResponse> Send(ILogger log, string desc, IFlurlRequest req, HttpMethod verb = null, HttpContent content = null,
      Func<IFlurlResponse, bool> isTransient = null) =>
      Client.Send(desc, req, verb, content, isTransient, log);

    async Task<HtmlDocument> GetHtml(string desc, Url url, ILogger log) {
      var res = await GetHttp(url, desc, log);
      return Html.ParseDocument(await res.ContentAsString());
    }

    async Task<HttpResponseMessage> GetHttp(string url, string desc, ILogger log) {
      var res = await Send(log, desc, url.AsUrl().AsRequest());
      return res.ResponseMessage;
    }

    #region Public Static

    #endregion

    #region Channel

    const string YtUrl = "https://www.youtube.com";
    
    record BpBase(BpContext context);
    record BpContext(BpClient client);
    record BpClient(string hl = "en-US", string clientName = "WEB", string clientVersion = "2.20210210.08.00", int utcOffsetMinutes = 0);
    record BpFirst(string browse_id, string @params) : BpBase(new BpContext(new()));
    record BpContinue(string continuation) : BpBase(new BpContext(new()));

    static readonly string[] TimeFormats = {@"m\:ss", @"mm\:ss", @"h\:mm\:ss", @"hh\:mm\:ss"};
    static readonly Regex    ViewCountRe = new(@"(?<num>[\d,]*) view");

    record ChannelPage(string ChannelId, Url Url, HtmlDocument Doc, JObject Data, JObject Cfg) {
      public string InnertubeKey => Cfg?.Str("INNERTUBE_API_KEY");
    }

    public async Task<WebChannel> Channel(ILogger log, string channelId) {
      if (!ValidateChannelId(channelId)) throw new($"Invalid YouTube channel ID [{channelId}].");
      var channelUrl = YtUrl.AppendPathSegments("channel", channelId);
      var doc = await GetHtml("channel page", channelUrl, log);
      var page = new ChannelPage(channelId, channelUrl, doc,
        await JsonFromScript(log, doc, channelUrl, ClientObject.InitialData),
        await JsonFromScript(log, doc, channelUrl, ClientObject.Cfg));
      var chan = await ParseChannel(page, log) with {
        Subscriptions = () => ChannelSubscriptions(log, page),
        Videos = () => ChannelVideos(page, log)
      };
      return chan;
    }

    const string BrowsePath = "/youtubei/v1/browse";

    enum BrowseType {
      [EnumMember(Value = "channels")] Channel,
      [EnumMember(Value = "videos")]   Video
    }

    /// <summary>Iterates through all of the browse pages, parse the JObject to get what you need</summary>
    async IAsyncEnumerable<JObject> BrowseResults(ChannelPage page, BrowseType browseType, ILogger log) {
      var pathSuffix = browseType.EnumString();
      var browse = page.Data.SelectTokens(@"$..tabRenderer.endpoint")
        .OfType<JObject>().Select(e => {
          var cmd = e.SelectToken("commandMetadata.webCommandMetadata");
          return cmd == null ? null : new {ApiPath = cmd.Str("apiUrl"), Path = cmd.Str("url"), Param = e.SelectToken("browseEndpoint.params")?.Str()};
        }).NotNull()
        .FirstOrDefault(p => p.Path?.Split('/').LastOrDefault()?.ToLowerInvariant() == pathSuffix);

      if (browse == default) {
        var error = page.Data
          .SelectTokens("alerts[*].alertRenderer")
          .FirstOrDefault(t => t.Str("type") == "ERROR")
          ?.SelectToken("text.simpleText")?.Str();

        if (error != null) {
          log.Information("WebScraper - Can't get videos in channel {Channel} because it's dead: {Error}", page.ChannelId, error);
          yield break;
        }

        var ex = new InvalidOperationException("WebScraper - can't find browse endpoint");
        await LogParseError("error parsing channel page", ex, page.Url, page.Data.ToString(), log);
        throw ex;
      }

      if (browse.Param == null) throw new($"unable to find {pathSuffix} browse endpoint on page: {page.Url}");
      string continueToken = null;
      while (true) {
        object token = continueToken == null ? new BpFirst(page.ChannelId, browse.Param) : new BpContinue(continueToken);
        var req = YtUrl.AppendPathSegments(BrowsePath).SetQueryParam("key", page.InnertubeKey).AsRequest();
        var j = await Send(log, $"browse {pathSuffix}", req, HttpMethod.Post, new CapturedJsonContent(token.ToJson())).Then(r => r.JsonObject());
        continueToken = j.SelectToken("..continuationCommand.token").Str();
        yield return j;
        if (continueToken == null) break;
      }
    }

    IAsyncEnumerable<IReadOnlyCollection<ChannelSubscription>> ChannelSubscriptions(ILogger log, ChannelPage page) =>
      BrowseResults(page, BrowseType.Channel, log).Select(j => j.SelectTokens("..gridChannelRenderer")
        .Select(c => new ChannelSubscription(c.Str("channelId"), c.YtTxt("title")) {
          Subs = c.YtTxt("subscriberCountText")?.ParseSubs()
        }).ToList()).Select(chans => (IReadOnlyCollection<ChannelSubscription>) chans);

    async Task<WebChannel> ParseChannel(ChannelPage page, ILogger log) {
      var error = page.Data.Tokens("alerts[*].alertRenderer").FirstOrDefault(a => a.Str("type") == "ERROR")?.YtTxt("text");
      var d = page.Data?.Token("microformat.microformatDataRenderer");
      if (d == null && error == null) {
        await LogParseError("can't find channel data in initialData json", ex: null, page.Url, page.Data?.ToString(), log);
        throw new($"Unable to parse channel data from {page.Url}");
      }
      var res = new WebChannel {
        Id = page.ChannelId,
        Title = d?.Str("title"),
        LogoUrl = d?.Token("thumbnail.thumbnails")?.Select(t => t.Str("url")).LastOrDefault(),
        Subs = page.Data?.YtTxt("header..subscriberCountText")?.ParseSubs(),
        StatusMessage = error
      };
      return res;
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
      var httpRes = await GetHttp(url, "video watch", log);
      var headers = httpRes.Headers;
      var raw = await httpRes.ContentAsString();
      return new(Html.ParseDocument(raw), raw, url, headers); // think about using parser than can use stream to avoid large strings using mem
    }

    public const string RestrictedVideoError = "Restricted";

    /// <summary>Loads the watch page, and the video info dic to get: recommendations and video details (including errors)</summary>
    public async Task<ExtraAndParts> GetExtra(ILogger log, string videoId, ExtraPart[] parts, string channelId = null, string channelTitle = null) {
      log = log.ForContext("VideoId", videoId);
      var watchPage = await GetVideoWatchPageHtmlAsync(videoId, log);
      var html = watchPage.Html;
      var ytInitialData = await JsonFromScript(log, html, videoId, ClientObject.InitialData);
      var infoDic = await GetVideoInfoDicAsync(videoId, log);
      var videoItem = GetVideo(videoId, infoDic);
      var extra = VideoItemToExtra(videoId, channelId, channelTitle, videoItem);
      var ytInitPr = await JsonFromScript(log, html, videoId, ClientObject.PlayerResponse);
      if (ytInitPr != null && ytInitPr.Value<string>("status") != "OK") {
        var playerError = ytInitPr.SelectToken("playabilityStatus.errorScreen.playerErrorMessageRenderer");
        extra.Error = playerError?.SelectToken("reason.simpleText")?.Value<string>();
        extra.SubError = (playerError?.SelectToken("subreason.simpleText") ??
                          playerError?.SelectTokens("subreason.runs[*].text").Join(""))
          ?.Value<string>();
      }
      if (extra.Error == null) {
        var restrictedMode = html.QueryElements("head > meta[property=\"og:restrictions:age\"]").FirstOrDefault()?.GetAttribute("content")?.Value == "18+";
        if (restrictedMode) {
          extra.Error = RestrictedVideoError;
          extra.SubError = "Unable to find recommended video because it is age restricted and requires to log in";
        }
      }
      if (extra.Error == null) {
        extra.SubError = html.QueryElements("#unavailable-submessage").FirstOrDefault()?.GetInnerText();
        if (extra.SubError == "") extra.SubError = null;
        if (extra.SubError.HasValue()) // all pages have the error, but not a sub-error
          extra.Error = html.QueryElements("#unavailable-message").FirstOrDefault()?.GetInnerText();
      }
      if (extra.Error == null) {
        var badgeLabels =
          ytInitialData?.SelectTokens(
            "contents.twoColumnWatchNextResults.results.results.contents[*].videoPrimaryInfoRenderer.badges[*].metadataBadgeRenderer.label");
        if (badgeLabels?.Any(b => b.Value<string>() == "Unlisted") == true)
          extra.Error = "Unlisted";
      }
      if (extra.Error != null) return new(extra);

      var recs = Array.Empty<Rec>();
      if (parts.Contains(ExtraPart.ERec))
        recs = await GetRecs2(log, html, videoId);
      var comments = Array.Empty<VideoComment>();
      if (parts.Contains(ExtraPart.EComment))
        comments = await GetComments(log, videoId, ytInitialData, watchPage).Then(c => c.ToArray());

      VideoCaption caption = null;
      if (parts.Contains(ExtraPart.ECaption))
        caption = await GetCaption(channelId, videoId, infoDic, log);

      return new(extra) {
        Caption = caption,
        Comments = comments,
        Recs = recs
      };
    }

    static VideoExtra VideoItemToExtra(string videoId, string channelId, string channelTitle, YtVideo videoItem) =>
      new() {
        VideoId = videoId,
        Updated = DateTime.UtcNow,
        // some videos are listed under a channels playlist, but when you click on the vidoe, its channel is under enother (e.g. _iYT8eg1F8s)
        // Record them as the channelId of the playlist.
        ChannelId = channelId ?? videoItem?.ChannelId,
        ChannelTitle = channelTitle ?? videoItem?.ChannelTitle,
        Description = videoItem?.Description,
        Duration = videoItem?.Duration,
        Keywords = videoItem?.Keywords,
        Title = videoItem?.Title,
        UploadDate = videoItem?.UploadDate,
        Statistics = videoItem?.Statistics,
        Source = ScrapeSource.Web,
        Platform = Platform.YouTube,
        Error = videoItem?.Error,
        SubError = videoItem?.SubError
      };

    public async Task<Rec[]> GetRecs2(ILogger log, HtmlDocument html, string videoId) {
      var jInit = await JsonFromScript(log, html, videoId, ClientObject.InitialData);
      if (jInit == null) return null;
      var resultsSel = "$.contents.twoColumnWatchNextResults.secondaryResults.secondaryResults.results";
      var jResults = (JArray) jInit.SelectToken(resultsSel);
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

    static readonly Regex ClientObjectsRe = new(@"(window\[""(?<window>\w+)""\]|var\s+(?<var>\w+))\s*=\s*(?<json>{.*?})\s*;",
      RegexOptions.Compiled | RegexOptions.Singleline);
    static readonly Regex ClientObjectCleanRe = new(@"{\w*?};", RegexOptions.Compiled);
    static readonly Regex ClientObjectsRe2 = new(@"(?<var>\w+)\.set\((?<json>{.*?})\);",
      RegexOptions.Compiled | RegexOptions.Singleline);

    public static class ClientObject {
      public const string InitialData    = "ytInitialData";
      public const string Cfg            = "ytcfg";
      public const string PlayerResponse = "ytInitialPlayerResponse";
    }

    public async Task<JObject> JsonFromScript(ILogger log, HtmlDocument html, Url url, string clientObjectName) {
      var scripts = html.QueryElements("script")
        .SelectMany(s => s.Children.OfType<HtmlText>()).Select(h => h.Content).ToList();

      string Gv(Match m, string group) => m.Groups[group].Value.HasValue() ? m.Groups[group].Value : null;

      var jObj = scripts
        .Select(s => ClientObjectCleanRe.Replace(s, "")).SelectMany(s => ClientObjectsRe.Matches(s)) // var = {} style
        .Concat(scripts.SelectMany(s => ClientObjectsRe2.Matches(s))) // window.var.set({}) style
        .Select(m => new {Var = Gv(m, "window") ?? Gv(m, "var"), Json = m.Groups["json"].Value})
        .Where(m => m.Var == clientObjectName)
        .Select(m => Def.Fun(() => m.Json.ParseJObject()).Try().Value).NotNull()
        .FirstOrDefault();

      if (jObj == null)
        await LogParseError($"Unable to parse {clientObjectName} json from watch page", ex: null, url, html.ToHtml(), log);
      return jObj;
    }

    async Task LogParseError(string msg, Exception ex, Url url, string content, ILogger log) {
      var path = StringPath.Relative(DateTime.UtcNow.ToString("yyyy-MM-dd"), url.Path);
      var logUrl = LogStore.Url(path);
      await LogStore.Save(path, content.AsStream(), log);
      log.Warning(ex, "WebScraper - saved content that we could not parse '{Msg}' ({Url}). error: {Error}",
        msg, logUrl, ex?.ToString());
    }

    async Task<VideoCaption> GetCaption(string channelId, string videoId, IReadOnlyDictionary<string, string> videoInfoDic, ILogger log) {
      var videoLog = log.ForContext("VideoId", videoId);
      VideoCaption caption = new() {
        ChannelId = channelId,
        VideoId = videoId,
        Updated = DateTime.Now
      };
      try {
        var playerResponseJson = JToken.Parse(videoInfoDic["player_response"]);
        var tracks = GetCaptionTracks(playerResponseJson);
        var enInfo = tracks.FirstOrDefault(t => t.Language.Code == "en");
        if (enInfo == null)
          return caption;
        var track = await GetClosedCaptionTrackAsync(enInfo, videoLog);
        return caption with {
          Info = track.Info,
          Captions = track.Captions
        };
      }
      catch (Exception ex) {
        ex.ThrowIfUnrecoverable();
        videoLog.Warning(ex, "Unable to get captions for {VideoID}: {Error}", videoId, ex.Message);
        return null;
      }
    }

    async Task<IReadOnlyCollection<VideoComment>> GetComments(ILogger log, string videoId, JObject ytInitialData, YtHtmlPage page) {
      async Task<(InnerTubeCfg Cfg, string CToken)> CommentCfgFromVideoPage() {
        var contSection = ytInitialData?.Tokens("$.contents.twoColumnWatchNextResults.results.results.contents[*].itemSectionRenderer")
          .FirstOrDefault(c => c.Str("sectionIdentifier") == "comment-item-section");
        var cToken = contSection?.Token("continuations[*].nextContinuationData.continuation")?.Str();
        var resCookies = page.Headers.Cookies().ToKeyedCollection(c => c.Name);
        var jCfg = await JsonFromScript(log, page.Html, page.Url, ClientObject.Cfg);
        if (jCfg == null) throw new InvalidOperationException("Can't load comments because no ytcfg was found on video page");
        var xsrfToken = jCfg.Value<string>("XSRF_TOKEN");
        var clientVersion = jCfg.Token("INNERTUBE_CONTEXT.client")?.Str("clientVersion");
        var innerTube = new InnerTubeCfg(xsrfToken, clientVersion,
          new {YSC = resCookies["YSC"].Value, VISITOR_INFO1_LIVE = resCookies["VISITOR_INFO1_LIVE"].Value});
        return (innerTube, cToken);
      }

      var cfg = await CommentCfgFromVideoPage();
      var comments = await Comments(videoId, cfg.CToken, cfg.Cfg, log).SelectManyList();
      log.Debug("YtWeb - loaded {Comments} for video {VideoId}", comments.Count, videoId);
      return comments;
    }

    #region Comments

    record InnerTubeCfg(string Xsrf, string ClientVersion, object Cookies);
    record CommentResult(VideoComment Comment, string ReplyContinuation = null);

    async IAsyncEnumerable<VideoComment[]> Comments(string videoId, string mainContinuation, InnerTubeCfg cfg, ILogger log) {
      log = log.ForContext("VideoId", videoId);

      async Task<(IFlurlResponse, IFlurlRequest req)> CommentRequest(CommentAction action, string continuation) {
        var req = $"https://www.youtube.com/comment_service_ajax?{action.EnumString()}=1&ctoken={continuation}&type=next".AsUrl()
          .WithHeader("x-youtube-client-name", "1")
          .WithHeader("x-youtube-client-version", cfg.ClientVersion)
          .WithCookies(cfg.Cookies);
        var res = await Send(log, "get comments", req, HttpMethod.Post, req.FormUrlContent(new {session_token = cfg.Xsrf}),
          r => HttpExtensions.IsTransientError(r.StatusCode) || r.StatusCode.In(400));
        return (res, req);
      }

      async Task<(CommentResult[] Comments, string Continuation)> RequestComments(string continuation, VideoComment parent = null) {
        var action = parent == null ? AComments : AReplies;
        var (res, req) = await CommentRequest(action, continuation);
        var getRootJ = action switch {
          AComments => res.JsonObject(),
          AReplies => res.JsonArray().Then(a => a.Children<JObject>().FirstOrDefault(j => j["response"] != null)),
          _ => throw new NotImplementedException()
        };
        var rootJ = await getRootJ.Swallow(e => log.Warning(e, "YtWeb - couldn't load comments. {Curl}: {Error}", req.FormatCurl(), e.Message)) ??
                    new JObject();
        var comments = action switch {
          AComments => from t in rootJ.Tokens("$..commentThreadRenderer")
            let c = t.SelectToken("comment.commentRenderer")
            where c != null
            select new CommentResult(ParseComment(videoId, c, parent),
              t.Token("replies.commentRepliesRenderer.continuations[0].nextContinuationData.continuation")?.Str()),
          AReplies => rootJ.Tokens("$..commentRenderer").Select(c => new CommentResult(ParseComment(videoId, c, parent))),
          _ => throw new NotImplementedException()
        };
        var nextContinue = rootJ.Token("response.continuationContents.itemSectionContinuation.continuations[0].nextContinuationData.continuation")?.Str();
        return (comments.ToArray(), nextContinue);
      }


      async IAsyncEnumerable<(CommentResult[] Comments, string Continuation)> AllComments(string continuation, VideoComment parent = null) {
        while (continuation != null) {
          var comments = await RequestComments(continuation, parent);
          continuation = comments.Continuation;
          if (comments.Comments.None()) yield break;
          yield return comments;
        }
      }

      await foreach (var ((comments, _), batch) in AllComments(mainContinuation).Select((b, i) => (b, i))) {
        var threads = comments.Select(c => c.Comment).ToArray();
        log.Verbose("YtWeb - loaded {Threads} threads in batch {Batch} for video {Video}", threads.Length, batch, videoId);
        yield return threads;
        var allReplies = comments.Where(c => c.ReplyContinuation != null)
          .BlockTrans(async t => await AllComments(t.ReplyContinuation, t.Comment).ToListAsync(), parallel: 4);
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

    static readonly Regex LikeDislikeRe = new(@"(?<num>[\d,]+)\s*(?<type>like|dislike)");

    static YtVideo GetVideo(string videoId, IReadOnlyDictionary<string, string> videoInfoDic, JObject ytInitialData = null) {
      if (!videoInfoDic.ContainsKey("player_response"))
        return null;

      var responseJson = JToken.Parse(videoInfoDic["player_response"]);
      var renderer = responseJson.SelectToken("microformat.playerMicroformatRenderer");

      var video = new YtVideo {
        Id = videoId
      };

      var playability = responseJson["playabilityStatus"];
      if (playability?.Str("status")?.ToLowerInvariant() == "error")
        return video with {
          Error = playability.Str("reason"),
          SubError = playability.SelectToken("errorScreen.playerErrorMessageRenderer.subreason.simpleText")?.Str()
        };

      T Val<T>(string propName) {
        var token = responseJson.SelectToken($"videoDetails.{propName}");
        return token == null ? default : token.Value<T>();
      }

      var likeDislikeMatches = ytInitialData?.SelectTokens("$..topLevelButtons[*].toggleButtonRenderer.defaultText..label")
        .Select(t => t.Value<string>().Match(LikeDislikeRe)).ToArray();
      ulong? LikeDislikeVal(string type) => likeDislikeMatches?.FirstOrDefault(t => t.Groups["type"].Value == type)?.Groups["num"].Value.TryParseULong();
      var like = LikeDislikeVal("like");
      var dislike = LikeDislikeVal("dislike");

      return video with {
        ChannelId = Val<string>("channelId"),
        ChannelTitle = Val<string>("author"),
        Author = Val<string>("author"),
        UploadDate = renderer?.SelectToken("uploadDate")?.Value<string>().ParseExact("yyyy-MM-dd", style: DateTimeStyles.AssumeUniversal).ToUniversalTime() ??
                     default,
        Title = Val<string>("title"),
        Description = Val<string>("shortDescription"),
        Duration = TimeSpan.FromSeconds(Val<double>("lengthSeconds")),
        Keywords = responseJson.SelectToken("videoDetails.keywords").NotNull().Values<string>().ToArray(),
        Statistics = new(Val<ulong>("viewCount"), like, dislike)
      };
    }

    static IReadOnlyCollection<ClosedCaptionTrackInfo> GetCaptionTracks(JToken playerResponseJson) =>
      (from trackJson in playerResponseJson.SelectToken("..captionTracks").NotNull()
        let url = new UriBuilder(trackJson.Str("baseUrl")).WithParameter("format", "3")
        let languageCode = trackJson.Str("languageCode")
        let languageName = trackJson.Str("name.simpleText")
        let language = new Language(languageCode, languageName)
        let isAutoGenerated = trackJson.Str("vssId").StartsWith("a.", OrdinalIgnoreCase)
        select new ClosedCaptionTrackInfo(url.ToString(), language, isAutoGenerated)).ToList();

    async Task<IReadOnlyDictionary<string, string>> GetVideoInfoDicAsync(string videoId, ILogger log) {
      static IReadOnlyDictionary<string, string> SplitQuery(StreamReader query) {
        var dic = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var i = 0;
        foreach (var p in SplitStream(query, separator: '&')) {
          var paramEncoded = i == 0 ? p.TrimStart('?') : p;
          var param = paramEncoded.UrlDecode();
          // Look for the equals sign
          var equalsPos = param.IndexOf('=');
          if (equalsPos <= 0)
            continue;
          // Get the key and value
          var key = param.Substring(startIndex: 0, equalsPos);
          var value = equalsPos < param.Length
            ? param.Substring(equalsPos + 1)
            : string.Empty;
          // Add to dictionary
          dic[key] = value;
          i++;
        }
        return dic;
      }

      static IEnumerable<string> SplitStream(StreamReader sr, char separator) {
        var buffer = new char[1024];
        var trail = "";
        while (true) {
          var n = sr.Read(buffer);
          if (n == 0) break;
          var chars = buffer[..n];
          var split = new string(chars).Split(separator);
          if (split.Length == 1) {
            trail += split[0]; // no split char, append to trail
            continue;
          }
          yield return trail + split[0];
          foreach (var part in split[1..^1]) yield return part; // middle complete parts
          trail = split[^1];
        }
        if (trail != "") yield return trail;
      }

      // This parameter does magic and a lot of videos don't work without it
      var eurl = $"https://youtube.googleapis.com/v/{videoId}".UrlEncode();
      var res = await GetHttp($"https://youtube.com/get_video_info?video_id={videoId}&el=embedded&eurl={eurl}&hl=en", "video dictionary", log);
      using var sr = await res.ContentAsStream();
      var result = SplitQuery(sr);
      return result;
    }

    #endregion

    #region Captions

    public async Task<ClosedCaptionTrack> GetClosedCaptionTrackAsync(ClosedCaptionTrackInfo info, ILogger log) {
      var trackXml = await GetClosedCaptionTrackXmlAsync(info.Url, log);

      var captions = from captionXml in trackXml.Descendants("p")
        let text = (string) captionXml
        where !text.IsNullOrWhiteSpace()
        let offset = (double?) captionXml.Attribute("t")
        let duration = (double?) captionXml.Attribute("d")
        select new ClosedCaption(text, offset?.Milliseconds(), duration?.Milliseconds());

      return new(info, captions.ToList());
    }

    // filters control characters but allows only properly-formed surrogate sequences
    static readonly Regex InvalidXml = new(
      @"(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F\uFEFF\uFFFE\uFFFF]",
      RegexOptions.Compiled);

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

  public record YtHtmlPage(HtmlDocument Html, string Raw, string Url, HttpResponseHeaders Headers);

  enum CommentAction {
    [EnumMember(Value = "action_get_comments")]
    AComments,
    [EnumMember(Value = "action_get_comment_replies")]
    AReplies
  }
  
  public record WebChannel {
    public string                                                           Id            { get; init; }
    public string                                                           Title         { get; init; }
    public string                                                           LogoUrl       { get; init; }
    public long?                                                            Subs          { get; init; }
    public string                                                           StatusMessage { get; init; }
    public Func<IAsyncEnumerable<IReadOnlyCollection<YtVideoItem>>>         Videos        { get; init; }
    public Func<IAsyncEnumerable<IReadOnlyCollection<ChannelSubscription>>> Subscriptions { get; init; }
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
    [EnumMember(Value = "caption")] ECaption
  }
}