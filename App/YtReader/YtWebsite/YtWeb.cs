using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using Flurl;
using Flurl.Http;
using Humanizer;
using LtGt;
using Mutuo.Etl.Blob;
using Nest;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using static System.Net.HttpStatusCode;
using static System.StringComparison;
using static SysExtensions.Net.HttpExtensions;

// ReSharper disable InconsistentNaming

//// a modified version of https://github.com/Tyrrrz/YoutubeExplode

namespace YtReader.YtWebsite {
  public class YtWeb {
    readonly ProxyCfg                                      Proxy;
    readonly YtCollectCfg                                  CollectCfg;
    readonly ISimpleFileStore                              LogStore;
    readonly ResourceCycle<HttpClient, ProxyConnectionCfg> Clients;

    public YtWeb(ProxyCfg proxy, YtCollectCfg collectCfg, ISimpleFileStore logStore) {
      Proxy = proxy;
      CollectCfg = collectCfg;
      LogStore = logStore;
      Clients = new(proxy.DirectAndProxies(), p => Task.FromResult(p.CreateHttpClient()));
    }

    const int RequestAttempts = 3;

    public Task<IFlurlResponse> Send(string desc, Url url, ILogger log, Func<IFlurlRequest, Task<IFlurlResponse>> getResponse = null) =>
      Send(desc, url.AsRequest(), log, getResponse);

    /// <summary>Send a request with error handling and proxy fallback. Allows any type of request (e.g. post with headers
    ///   etc..)</summary>
    public async Task<IFlurlResponse> Send(string desc, IFlurlRequest request, ILogger log, Func<IFlurlRequest, Task<IFlurlResponse>> getResponse = null) {
      var attempts = 0;
      getResponse ??= r => r.GetAsync();
      while (true) {
        attempts++;
        var (http, proxy) = await Clients.Get();

        Task<IFlurlResponse> GetRes() => getResponse(request
          .WithClient(new FlurlClient(http))
          .WithHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36")
          .AllowAnyHttpStatus());

        try {
          var resTask = GetRes();
          if (await Task.WhenAny(resTask, Task.Delay((Proxy.TimeoutSeconds + 10).Seconds())) != resTask)
            throw new TaskCanceledException($"SendAsync on {request.Url} took to long without timing out itself");
          var res = await resTask;
          if (res.StatusCode == 404)
            EnsureSuccess(res.StatusCode, request.Url); // fail immediately for not found
          if (
            (proxy.IsDirect() || attempts > RequestAttempts) // fall back immediately for direct, 3 failures for proxies
            && res.StatusCode == 429) {
            log.Debug("WebScraper - TooManyRequests status, falling back to next proxy");
            await Clients.NextResource(http);
            attempts = 0;
            continue;
          }
          EnsureSuccess(res.StatusCode, request.Url);
          log.Verbose("WebScraper - {Desc} {Url}. Proxy: {Proxy}", desc, request.Url, proxy.Url ?? "Direct");
          return res;
        }
        catch (Exception ex) {
          log.Debug(ex, "WebScraper - error requesting {url} attempt {Attempt} : {Error} ", request.Url, attempts, ex.Message);
          
          // not found (i.e. bad url) and proxy auth (i.e. ran out o quota on smartproxy) are fatal for sure
          if (ex is HttpRequestException e && e.StatusCode?.In(NotFound, ProxyAuthenticationRequired) == true)
            throw;
          
          // throw for exception that aren't the expected transient-possible ones
          if (!(ex is HttpRequestException || ex is TaskCanceledException || ex is FlurlHttpTimeoutException)) 
            throw;
          
          if (attempts > RequestAttempts)
            throw;
        }
      }
    }

    async Task<HtmlDocument> GetHtml(string desc, Url url, ILogger log) {
      var res = await GetHttp(url, desc, log);
      return Html.ParseDocument(await res.ContentAsString());
    }

    async Task<HttpResponseMessage> GetHttp(string url, string desc, ILogger log) {
      log.Verbose("WebScraper -  {Desc} {Url}", desc, url);
      var res = await Send(desc, url.AsUrl(), log);
      return res.ResponseMessage;
    }

    #region Public Static

    /// <summary>Verifies that the given string is syntactically a valid YouTube channel ID.</summary>
    public static bool ValidateChannelId(string channelId) {
      if (channelId.IsNullOrWhiteSpace())
        return false;

      // Channel IDs should start with these characters
      if (!channelId.StartsWith("UC", Ordinal))
        return false;

      // Channel IDs are always 24 characters
      if (channelId.Length != 24)
        return false;

      return !Regex.IsMatch(channelId, @"[^0-9a-zA-Z_\-]");
    }

    public static bool ValidateVideoId(string videoId) {
      if (videoId.IsNullOrWhiteSpace())
        return false;

      // Video IDs are always 11 characters
      if (videoId.Length != 11)
        return false;

      return !Regex.IsMatch(videoId, @"[^0-9a-zA-Z_\-]");
    }

    /// <summary>Parses video ID from a YouTube video URL.</summary>
    public static string ParseVideoId(string videoUrl) =>
      TryParseVideoId(videoUrl, out var result)
        ? result
        : throw new FormatException($"Could not parse video ID from given string [{videoUrl}].");

    /// <summary>Tries to parse video ID from a YouTube video URL.</summary>
    public static bool TryParseVideoId(string videoUrl, out string videoId) {
      videoId = default;

      if (videoUrl.IsNullOrWhiteSpace())
        return false;

      // https://www.youtube.com/watch?v=yIVRs6YSbOM
      var regularMatch = Regex.Match(videoUrl, @"youtube\..+?/watch.*?v=(.*?)(?:&|/|$)").Groups[1].Value;
      if (!regularMatch.IsNullOrWhiteSpace() && ValidateVideoId(regularMatch)) {
        videoId = regularMatch;
        return true;
      }

      // https://youtu.be/yIVRs6YSbOM
      var shortMatch = Regex.Match(videoUrl, @"youtu\.be/(.*?)(?:\?|&|/|$)").Groups[1].Value;
      if (!shortMatch.IsNullOrWhiteSpace() && ValidateVideoId(shortMatch)) {
        videoId = shortMatch;
        return true;
      }

      // https://www.youtube.com/embed/yIVRs6YSbOM
      var embedMatch = Regex.Match(videoUrl, @"youtube\..+?/embed/(.*?)(?:\?|&|/|$)").Groups[1].Value;
      if (!embedMatch.IsNullOrWhiteSpace() && ValidateVideoId(embedMatch)) {
        videoId = embedMatch;
        return true;
      }

      return false;
    }

    #endregion

    #region Channel Videos

    const string YtUrl = "https://www.youtube.com";

    static readonly Regex ClientKeyRe = new(@"\""INNERTUBE_API_KEY\""\s*?:\s*?\""(?<key>\w*)\""");

    record BpBase(BpContext context);
    record BpContext(BpClient client);
    record BpClient(string hl = "en-US", string clientName = "WEB", string clientVersion = "2.20210210.08.00", int utcOffsetMinutes = 0);
    record BpFirst(string browse_id, string @params) : BpBase(new BpContext(new()));
    record BpContinue(string continuation) : BpBase(new BpContext(new()));

    static readonly string[] TimeFormats = {@"m\:ss", @"mm\:ss", @"h\:mm\:ss", @"hh\:mm\:ss"};
    static readonly Regex    ViewCountRe = new(@"(?<num>[\d,]*) view");

    public async IAsyncEnumerable<IReadOnlyCollection<YtVideoItem>> ChannelVideos(string channelId, ILogger log) {
      // this endpoint gives us a key to use making subsequent requests
      var keyTask = Send("get yt key", YtUrl.AppendPathSegment("sw.js").AsRequest(), log)
        .Then(r => r.GetStringAsync())
        .Then(s => s.Match(ClientKeyRe).Groups["key"].Value);

      // wasteful, but we need to grab the parameter we need to params for videos
      var channelUrl = YtUrl.AppendPathSegments("channel", channelId);
      var channelPageHtml = await GetHtml("videos page", channelUrl, log);
      var ytInitialData = await GetClientObjectFromWatchPage(log, channelPageHtml, channelUrl, "ytInitialData");
      var endpoints = ytInitialData.SelectTokens(@"$..tabRenderer.endpoint").OfType<JObject>();
      var browseParams = endpoints.Select(e => {
        var cmd = e.SelectToken("commandMetadata.webCommandMetadata");
        if (cmd == null) return null;
        if (cmd.Value<string>("apiUrl") != "/youtubei/v1/browse" || cmd.Value<string>("url")?.EndsWith("/videos") != true)
          return null;
        return e.SelectToken("browseEndpoint.params")?.Value<string>();
      }).NotNull().FirstOrDefault();
      
      if (browseParams == null) {
        var ex = new InvalidOperationException("can't find browse endpoint");
        await LogParseError("error parsing channel page", ex, channelUrl, ytInitialData.ToString(), log);
        throw ex;
      }
      var key = await keyTask;

      string continueToken = null;
      while (true) {
        object token = continueToken == null ? new BpFirst(channelId, browseParams) : new BpContinue(continueToken);

        var videoJ = await Send("get videos",
          YtUrl.AppendPathSegments("youtubei", "v1", "browse").SetQueryParam("key", key),
          log,
          r => r.PostJsonAsync(token)).Then(r => r.JsonObject());

        var videos = videoJ.SelectTokens("..gridVideoRenderer").Select(ParseVideo).ToList();
        yield return videos;

        continueToken = videoJ.SelectToken("..continuationCommand.token")?.Value<string>();
        if (continueToken == null) break;
      }


      YtVideoItem ParseVideo(JToken v) {
        string Str(string path) => v.SelectToken(path)?.Value<string>();
        string Txt(string path) => Str($"{path}.simpleText") ?? Str($"{path}.runs[0].text");

        var (ago, agoUnit) = ChromeScraper.ParseAgo(Txt("publishedTimeText"));
        var viewCountText = Txt("viewCountText");
        //No views
        var parsedVideo = new YtVideoItem {
          Id = Str("videoId"), Title = Txt("title"),
          Duration = Str("..thumbnailOverlayTimeStatusRenderer.text.simpleText").TryParseTimeSpanExact(TimeFormats) ?? TimeSpan.Zero,
          Statistics = new(viewCountText == "No views" ? 0 : viewCountText?.Match(ViewCountRe).Groups["num"].Value.TryParseULong(NumberStyles.AllowThousands)),
          UploadDate = DateTime.UtcNow - ago // this is very impresice. We rely on video extra for a reliable upload date
        };

        if (parsedVideo.Statistics.ViewCount == null)
          log.Debug("Can't find views for {Video} in {Json}", parsedVideo.Id, v.ToString());
        return parsedVideo;
      }
    }

    #endregion

    #region Channels

    Task<HtmlDocument> GetChannelPageHtmlAsync(string channelId, ILogger log) =>
      GetHtml("channel page", $"https://www.youtube.com/channel/{channelId}?hl=en", log);

    static readonly Regex SubRegex = new("(?'num'\\d+\\.?\\d*)(?'unit'[BMK]?)", RegexOptions.Compiled);

    public async Task<ChannelExtended> GetChannelAsync(string channelId, ILogger log) {
      if (!ValidateChannelId(channelId))
        throw new ArgumentException($"Invalid YouTube channel ID [{channelId}].", nameof(channelId));

      // Get channel page HTML
      var channelPageHtml = await GetChannelPageHtmlAsync(channelId, log);

      var alertMessage = channelPageHtml.QueryElements("div.yt-alert-message").FirstOrDefault()?.GetInnerText();
      if (alertMessage.HasValue())
        return new() {Id = channelId, StatusMessage = alertMessage};

      // Extract info
      var channelTitle = channelPageHtml.QueryElements("meta[property=\"og:title\"]")
        .FirstOrDefault()?.GetAttribute("content").Value;

      var channelLogoUrl = channelPageHtml.QueryElements("meta[property=\"og:image\"]")
        .FirstOrDefault()?.GetAttribute("content").Value;

      var subDesc = channelPageHtml.QueryElements("span.yt-subscription-button-subscriber-count-branded-horizontal.subscribed").FirstOrDefault()
        ?.GetInnerText();
      return new() {
        Id = channelId,
        Title = channelTitle,
        LogoUrl = channelLogoUrl,
        Subs = ParseSubscription(subDesc)
      };

      static long? ParseSubscription(string s) {
        if (s.NullOrEmpty()) return null;
        var m = SubRegex.Match(s);
        var subs = m.Groups["num"].Value.ParseDecimal();
        var multiplier = m.Groups["unit"].Value switch {
          "K" => 1_000,
          "M" => 1_000_000,
          "B" => 1_000_000_000,
          _ => 1
        };
        return (long) Math.Round(subs * multiplier);
      }
    }

    #endregion

    #region Videos

    public async Task<(HtmlDocument html, string raw, string url)> GetVideoWatchPageHtmlAsync(string videoId, ILogger log) {
      var url = $"https://youtube.com/watch?v={videoId}&bpctr=9999999999&hl=en-us";
      var raw = await (await GetHttp(url, "video watch", log)).ContentAsString();
      return (Html.ParseDocument(raw), raw, url); // think about using parser than can use stream to avoid large strings using mem
    }

    public const string RestrictedVideoError = "Restricted";

    public async Task<IReadOnlyCollection<VideoExtra>> GetExtra(IReadOnlyCollection<string> videos, ILogger log,
      string channelId = null, string channelTitle = null) =>
      await videos.BlockTrans(async (v, i) => {
        if (i % 100 == 0) log.Debug("YtWeb.GetExtra {Channel} - {Videos}/{Total}", channelTitle ?? "", i, videos.Count);
        var (extra, ex) = await GetExtra(log, v, channelId, channelTitle).Try();
        if(ex != null)
          log.Warning(ex, "YtWeb.GetExtra {Channel} - Error getting extra: {Message}", channelTitle ?? "", ex.Message);
        return extra;
      }, CollectCfg.WebParallel).Where(e => e != null).ToListAsync();

    /// <summary>Loads the video info dic to get video details. Doesn't find video errors like GetRecsAndExtra</summary>
    public async Task<VideoExtra> GetExtra(ILogger log, string videoId, string channelId = null, string channelTitle = null) {
      var infoDic = await GetVideoInfoDicAsync(videoId, log);
      var videoItem = GetVideo(videoId, infoDic);
      var extra = VideoItemToExtra(videoId, channelId, channelTitle, videoItem);
      return extra;
    }

    public async Task<IReadOnlyCollection<RecsAndExtra>> GetRecsAndExtra(IReadOnlyCollection<string> videos, ILogger log,
      string channelId = null, string channelTitle = null) =>
      await videos.BlockFunc(async v => await GetRecsAndExtra(log, v, channelId, channelTitle), CollectCfg.WebParallel);

    /// <summary>Loads the watch page, and the video info dic to get: recommendations and video details (including errors)</summary>
    public async Task<RecsAndExtra> GetRecsAndExtra(ILogger log, string videoId, string channelId = null, string channelTitle = null) {
      log = log.ForContext("VideoId", videoId);
      var watchPage = await GetVideoWatchPageHtmlAsync(videoId, log);
      var (html, _, _) = watchPage;
      var ytInitialData = await GetClientObjectFromWatchPage(log, html, videoId, "ytInitialData");
      var extra = await GetExtra(log, videoId, channelId, channelTitle);

      var ytInitPr = await GetClientObjectFromWatchPage(log, html, videoId, "ytInitialPlayerResponse");
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
      if (extra.Error != null) return new(extra, new Rec[] { });

      var recs = await GetRecs2(log, html, videoId);
      return new(extra, recs);
    }

    static VideoExtra VideoItemToExtra(string videoId, string channelId, string channelTitle, YtVideo videoItem) =>
      new VideoExtra {
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
        AddedDate = videoItem?.AddedDate,
        Statistics = videoItem?.Statistics,
        Source = ScrapeSource.Web
      };

    public async Task<Rec[]> GetRecs2(ILogger log, HtmlDocument html, string videoId) {
      var jInit = await GetClientObjectFromWatchPage(log, html, videoId, "ytInitialData");
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
            ToViews = ChromeScraper.ParseViews(viewText),
            ToUploadDate = ChromeScraper.ParseAgo(j.SelectToken("publishedTimeText.simpleText")?.Value<string>()).Dur.Before(DateTime.UtcNow),
            ForYou = ChromeScraper.ParseForYou(viewText)
          };
        }).ToArray();
      return recs;
    }

    static readonly Regex ClientObjectsRe = new(@"(window\[""(?<window>\w+)""\]|var\s+(?<var>\w+))\s*=\s*(?<json>{.*?})\s*;",
      RegexOptions.Compiled | RegexOptions.Singleline);
    static readonly Regex ClientObjectCleanRe = new(@"{\w*?};", RegexOptions.Compiled);

    public async Task<JObject> GetClientObjectFromWatchPage(ILogger log, HtmlDocument html, Url url, string name) {
      var scripts = html.QueryElements("script")
        .SelectMany(s => s.Children.OfType<HtmlText>()).Select(h => h.Content);

      string GroupValue(Match m, string group) => m.Groups[group].Value.HasValue() ? m.Groups[group].Value : null;

      var windowObjects = scripts
        .Select(s => ClientObjectCleanRe.Replace(s, ""))
        .SelectMany(s => ClientObjectsRe.Matches(s))
        .ToDictionary(m => GroupValue(m, "window") ?? GroupValue(m, "var"), m => m.Groups["json"].Value);

      var initData = windowObjects.TryGet(name);
      if (initData == null) return null;

      try {
        var jInit = JObject.Parse(initData);
        return jInit;
      }
      catch (Exception ex) {
        await LogParseError($"Unable to parse {name} json from watch page", ex, url, html.ToHtml(), log);
        return null;
      }
    }

    async Task LogParseError(string msg, Exception ex, Url url, string content, ILogger log) {
      var path = StringPath.Relative(DateTime.UtcNow.ToString("yyyy-MM-dd"), url.Path);
      var logUrl = LogStore.Url(path);
      await LogStore.Save(path, content.AsStream(), log);
      log.Warning(ex, "WebScraper - saved content that we could not parse '{Msg}' ({Url}). error: {Error}",
        msg, logUrl, ex?.ToString());
    }

    public async Task<IReadOnlyCollection<ClosedCaptionTrackInfo>> GetCaptionTracks(string videoId, ILogger log) {
      var videoInfoDic = await GetVideoInfoDicAsync(videoId, log);
      var playerResponseJson = JToken.Parse(videoInfoDic["player_response"]);
      var captions = GetCaptionTracks(playerResponseJson);
      return captions;
    }

    public async Task<YtVideo> GetVideo(string videoId, ILogger log) {
      if (!ValidateVideoId(videoId))
        throw new ArgumentException($"Invalid YouTube video ID [{videoId}].", nameof(videoId));
      var videoInfoDic = await GetVideoInfoDicAsync(videoId, log);
      var videoWatchPage = await GetVideoWatchPageHtmlAsync(videoId, log);
      var ytInitialData = await GetClientObjectFromWatchPage(log, videoWatchPage.html, videoId, "ytInitialData");
      return GetVideo(videoId, videoInfoDic, videoWatchPage, ytInitialData);
    }

    static readonly Regex LikeDislikeRe = new(@"(?<num>[\d,]+)\s*(?<type>like|dislike)");

    static YtVideo GetVideo(string videoId, IReadOnlyDictionary<string, string> videoInfoDic,
      (HtmlDocument html, string raw, string url) videoWatchPage = default, JObject ytInitialData = null) {
      if (!videoInfoDic.ContainsKey("player_response"))
        return null;

      var responseJson = JToken.Parse(videoInfoDic["player_response"]);
      var renderer = responseJson.SelectToken("microformat.playerMicroformatRenderer");

      if (string.Equals(responseJson.SelectToken("playabilityStatus.status")?.Value<string>(), "error", OrdinalIgnoreCase))
        return null;

      T Val<T>(string propName) {
        var token = responseJson.SelectToken($"videoDetails.{propName}");
        return token == null ? default : token.Value<T>();
      }

      var likeDislikeMatches = ytInitialData?.SelectTokens("$..topLevelButtons[*].toggleButtonRenderer.defaultText..label")
        .Select(t => t.Value<string>().Match(LikeDislikeRe)).ToArray();
      ulong? LikeDislikeVal(string type) => likeDislikeMatches?.FirstOrDefault(t => t.Groups["type"].Value == type)?.Groups["num"].Value.TryParseULong();
      var like = LikeDislikeVal("like");
      var dislike = LikeDislikeVal("dislike");

      return new() {
        Id = videoId,
        ChannelId = Val<string>("channelId"),
        ChannelTitle = Val<string>("author"),
        Author = Val<string>("author"),
        UploadDate = renderer?.SelectToken("uploadDate")?.Value<string>().ParseExact("yyyy-MM-dd", style: DateTimeStyles.AssumeUniversal).ToUniversalTime() ??
                     default,
        AddedDate = default,
        Title = Val<string>("title"),
        Description = Val<string>("shortDescription"),
        Duration = TimeSpan.FromSeconds(Val<double>("lengthSeconds")),
        Keywords = responseJson.SelectToken("videoDetails.keywords").NotNull().Values<string>().ToArray(),
        Statistics = new(Val<ulong>("viewCount"), like, dislike)
      };
    }

    static IReadOnlyCollection<ClosedCaptionTrackInfo> GetCaptionTracks(JToken playerResponseJson) =>
      (from trackJson in playerResponseJson.SelectToken("..captionTracks").NotNull()
        let url = new UriBuilder(trackJson.SelectToken("baseUrl").Value<string>()).WithParameter("format", "3")
        let languageCode = trackJson.SelectToken("languageCode").Value<string>()
        let languageName = trackJson.SelectToken("name.simpleText").Value<string>()
        let language = new Language(languageCode, languageName)
        let isAutoGenerated = trackJson.SelectToken("vssId")
          .Value<string>()
          .StartsWith("a.", OrdinalIgnoreCase)
        select new ClosedCaptionTrackInfo(url.ToString(), language, isAutoGenerated)).ToList();

    async Task<IReadOnlyDictionary<string, string>> GetVideoInfoDicAsync(string videoId, ILogger log) {
      // This parameter does magic and a lot of videos don't work without it
      var eurl = $"https://youtube.googleapis.com/v/{videoId}".UrlEncode();
      var res = await GetHttp($"https://youtube.com/get_video_info?video_id={videoId}&el=embedded&eurl={eurl}&hl=en", "video dictionary", log);
      using var sr = await res.ContentAsStream();
      var result = SplitQuery(sr);
      return result;
    }

    static IReadOnlyDictionary<string, string> SplitQuery(StreamReader query) {
      var dic = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
      var i = 0;
      foreach (var p in SplitStream(query, '&')) {
        var paramEncoded = i == 0 ? p.TrimStart('?') : p;

        var param = paramEncoded.UrlDecode();

        // Look for the equals sign
        var equalsPos = param.IndexOf('=');
        if (equalsPos <= 0)
          continue;

        // Get the key and value
        var key = param.Substring(0, equalsPos);
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

  public class ChannelExtended {
    public string Id            { get; set; }
    public string Title         { get; set; }
    public string LogoUrl       { get; set; }
    public long?  Subs          { get; set; }
    public string StatusMessage { get; set; }
  }

  public class Rec {
    public string        ToVideoId      { get; set; }
    public string        ToVideoTitle   { get; set; }
    public string        ToChannelTitle { get; set; }
    public string        ToChannelId    { get; set; }
    public ScrapeSource? Source         { get; set; }
    public int           Rank           { get; set; }
    public long?         ToViews        { get; set; }
    public DateTime?     ToUploadDate   { get; set; }
    public bool          ForYou         { get; set; }
  }

  public enum ScrapeSource {
    Web,
    Api,
    Chrome
  }

  public class RecsAndExtra {
    public RecsAndExtra(VideoExtra extra, Rec[] recs) {
      Extra = extra;
      Recs = recs;
    }

    public VideoExtra Extra { get; set; }
    public Rec[]      Recs  { get; set; }
  }
}