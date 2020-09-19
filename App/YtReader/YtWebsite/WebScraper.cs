using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
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

//// a modified version of https://github.com/Tyrrrz/YoutubeExplode

namespace YtReader.YtWebsite {
  public class WebScraper {
    const    string                                        MissingYtResourceMessage = "Received BadRequest response, which means YT resource is missing";
    readonly ProxyCfg                                      Proxy;
    readonly YtCollectCfg                                  CollectCfg;
    readonly ISimpleFileStore                              LogStore;
    readonly ResourceCycle<HttpClient, ProxyConnectionCfg> Clients;

    public WebScraper(ProxyCfg proxy, YtCollectCfg collectCfg, ISimpleFileStore logStore) {
      Proxy = proxy;
      CollectCfg = collectCfg;
      LogStore = logStore;
      Clients = new ResourceCycle<HttpClient, ProxyConnectionCfg>(proxy.DirectAndProxies(), p => Task.FromResult(CreateHttpClient(p)));
    }

    HttpClient CreateHttpClient(ProxyConnectionCfg proxy) =>
      new HttpClient(new HttpClientHandler {
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        UseCookies = false,
        Proxy = proxy.Url == null
          ? null
          : new WebProxy(proxy.Url, BypassOnLocal: true, new string[] { },
            proxy.Creds != null ? new NetworkCredential(proxy.Creds.Name, proxy.Creds.Secret) : null),
        UseProxy = proxy.Url != null,
      }) {
        Timeout = Proxy.TimeoutSeconds.Seconds()
      };

    async Task<HttpResponseMessage> GetHttp(string url, string desc, ILogger log) {
      log.Debug("WebScraper -  {Desc} {Url}", desc, url);

      var attempts = 0;
      while (true) {
        attempts++;
        var (http, proxy) = await Clients.Get();
        try {
          var resp = http.SendAsync(Get(url));
          if (await Task.WhenAny(resp, Task.Delay((Proxy.TimeoutSeconds + 10).Seconds())) != resp)
            throw new TaskCanceledException($"SendAsync on {url} took to long without timing out itself");
          var innerRes = await resp;
          ThrowMissingResourceInvalidOpIfNeeded(innerRes);
          if (
            (proxy.IsDirect() || attempts > 3) // fall back immediately for direct, 3 failures for proxies
            && innerRes.StatusCode == HttpStatusCode.TooManyRequests) {
            log.Debug("WebScraper - TooManyRequests status, falling back to next proxy");
            await Clients.NextResource(http);
            attempts = 0;
            continue;
          }
          innerRes.EnsureSuccessStatusCode();
          log.Debug("WebScraper - {Desc} {Url}. Proxy: {Proxy}", desc, url, proxy.Url ?? "Direct");
          return innerRes;
        }
        catch (Exception ex) {
          log.Warning(ex, "WebScraper - error requesting {url} attempt {Attempt} : {Error} ", url, attempts, ex.Message);
          if (ex is InvalidOperationException && ex.Message == MissingYtResourceMessage)
            throw;
          if (!(ex is HttpRequestException || ex is TaskCanceledException)) // continue on these
            throw;
          if (attempts > 3)
            throw;
        }
      }
    }

    static HttpRequestMessage Get(string url) =>
      url.AsUri().Get().AddHeader("User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36");

    static void ThrowMissingResourceInvalidOpIfNeeded(HttpResponseMessage directRes) {
      if (directRes.StatusCode == HttpStatusCode.BadRequest)
        throw new InvalidOperationException(MissingYtResourceMessage);
    }

    async Task<HtmlDocument> GetHtml(string url, string desc, ILogger log) {
      var res = await GetHttp(url, desc, log);
      return Html.ParseDocument(await res.ContentAsString());
    }

    #region Public Static

    /// <summary>Verifies that the given string is syntactically a valid YouTube channel ID.</summary>
    public static bool ValidateChannelId(string channelId) {
      if (channelId.IsNullOrWhiteSpace())
        return false;

      // Channel IDs should start with these characters
      if (!channelId.StartsWith("UC", StringComparison.Ordinal))
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

    /// <summary>Verifies that the given string is syntactically a valid YouTube playlist ID.</summary>
    public static bool ValidatePlaylistId(string playlistId) {
      if (playlistId.IsNullOrWhiteSpace())
        return false;

      // Watch later playlist is special
      if (playlistId == "WL")
        return true;

      // My Mix playlist is special
      if (playlistId == "RDMM")
        return true;

      // Other playlist IDs should start with these two characters
      if (!playlistId.StartsWith("PL", StringComparison.Ordinal) &&
          !playlistId.StartsWith("RD", StringComparison.Ordinal) &&
          !playlistId.StartsWith("UL", StringComparison.Ordinal) &&
          !playlistId.StartsWith("UU", StringComparison.Ordinal) &&
          !playlistId.StartsWith("PU", StringComparison.Ordinal) &&
          !playlistId.StartsWith("OL", StringComparison.Ordinal) &&
          !playlistId.StartsWith("LL", StringComparison.Ordinal) &&
          !playlistId.StartsWith("FL", StringComparison.Ordinal))
        return false;

      // Playlist IDs vary a lot in lengths, so we will just compare with the extremes
      if (playlistId.Length < 13 || playlistId.Length > 42)
        return false;

      return !Regex.IsMatch(playlistId, @"[^0-9a-zA-Z_\-]");
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

    #region Playlist with lazy iteration of pages

    public async IAsyncEnumerable<IReadOnlyCollection<VideoItem>> GetChannelUploadsAsync(string channelId, ILogger log) {
      if (!ValidateChannelId(channelId))
        throw new ArgumentException($"Invalid YouTube channel ID [{channelId}].", nameof(channelId));

      // Generate ID for the playlist that contains all videos uploaded by this channel
      var playlistId = "UU" + channelId.SubstringAfter("UC");

      // Get playlist
      var playlist = await GetPlaylistAsync(playlistId, log);
      await foreach (var videos in playlist.Videos) yield return videos;
    }

    async Task<JToken> GetPlaylistJsonAsync(string playlistId, int index, ILogger log) {
      using var res = await GetHttp($"https://youtube.com/list_ajax?style=json&action_get_list=1&list={playlistId}&index={index}&hl=en", "playlist", log);
      using var jr = await res.ContentAsJsonReader();
      var j = await JToken.LoadAsync(jr);
      return j;
    }

    async Task<Playlist> GetPlaylistAsync(string playlistId, ILogger log) {
      if (!ValidatePlaylistId(playlistId))
        throw new ArgumentException($"Invalid YouTube playlist ID [{playlistId}].", nameof(playlistId));

      var playlistJson = await GetPlaylistJsonAsync(playlistId, 1, log);

      var author = playlistJson.SelectToken("author")?.Value<string>() ?? ""; // system playlists have no author
      var title = playlistJson.SelectToken("title").Value<string>();
      var description = playlistJson.SelectToken("description")?.Value<string>() ?? "";
      var viewCount = playlistJson.SelectToken("views")?.Value<long>() ?? 0; // system playlists have no views
      var likeCount = playlistJson.SelectToken("likes")?.Value<long>() ?? 0; // system playlists have no likes
      var dislikeCount = playlistJson.SelectToken("dislikes")?.Value<long>() ?? 0; // system playlists have no dislikes
      var statistics = new Statistics(viewCount, likeCount, dislikeCount);

      return new Playlist(playlistId, author, title, description, statistics, PlaylistVideos(playlistId, playlistJson, log));
    }

    async IAsyncEnumerable<IReadOnlyCollection<VideoItem>> PlaylistVideos(string playlistId, JToken playlistJson, ILogger log) {
      var index = playlistId.StartsWith("PL", StringComparison.OrdinalIgnoreCase) ? 101 : 0;
      var videoIds = new HashSet<string>();
      do {
        // Get videos
        var newVideos = new List<VideoItem>();
        foreach (var videoJson in playlistJson.SelectToken("video").NotNull()) {
          var epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

          // Extract video info
          var videoId = videoJson.SelectToken("encrypted_id").Value<string>();
          var videoAuthor = videoJson.SelectToken("author").Value<string>();
          var videoUploadDate = epoch + TimeSpan.FromSeconds(videoJson.SelectToken("time_created").Value<long>());
          var videoTitle = videoJson.SelectToken("title").Value<string>();
          var videoDescription = videoJson.SelectToken("description").Value<string>();
          var videoDuration = TimeSpan.FromSeconds(videoJson.SelectToken("length_seconds").Value<double>());
          var videoViewCount = videoJson.SelectToken("views").Value<string>().StripNonDigit().ParseLong();
          var videoLikeCount = videoJson.SelectToken("likes").Value<long>();
          var videoDislikeCount = videoJson.SelectToken("dislikes").Value<long>();

          // Extract video keywords
          var videoKeywordsJoined = videoJson.SelectToken("keywords").Value<string>();
          var videoKeywords = Regex.Matches(videoKeywordsJoined, "\"[^\"]+\"|\\S+")
            .Select(m => m.Value)
            .Where(s => !s.IsNullOrWhiteSpace())
            .Select(s => s.Trim('"'))
            .ToArray();

          // Create statistics and thumbnails
          var videoStatistics = new Statistics(videoViewCount, videoLikeCount, videoDislikeCount);
          var videoThumbnails = new ThumbnailSet(videoId);

          // Add video to the list if it's not already there
          if (videoIds.Add(videoId)) {
            var video = new VideoItem(videoId, videoAuthor, videoUploadDate, videoTitle, videoDescription,
              videoThumbnails, videoDuration, videoKeywords, videoStatistics, null, null); // TODO: is channelId in the playlist?
            newVideos.Add(video);
          }
        }

        // If no distinct videos were added to the list - break
        if (newVideos.Count == 0) yield break;
        yield return newVideos;

        // Advance index
        index += 100;
        playlistJson = await GetPlaylistJsonAsync(playlistId, index, log);
      } while (true);
    }

    #endregion

    #region Channels

    Task<HtmlDocument> GetChannelPageHtmlAsync(string channelId, ILogger log) =>
      GetHtml($"https://www.youtube.com/channel/{channelId}?hl=en", "channel page", log);

    static readonly Regex SubRegex = new Regex("(?'num'\\d+\\.?\\d*)(?'unit'[BMK]?)", RegexOptions.Compiled);

    public async Task<ChannelExtended> GetChannelAsync(string channelId, ILogger log) {
      if (!ValidateChannelId(channelId))
        throw new ArgumentException($"Invalid YouTube channel ID [{channelId}].", nameof(channelId));

      // Get channel page HTML
      var channelPageHtml = await GetChannelPageHtmlAsync(channelId, log);

      var alertMessage = channelPageHtml.QueryElements("div.yt-alert-message").FirstOrDefault()?.GetInnerText();
      if (alertMessage.HasValue())
        return new ChannelExtended {Id = channelId, StatusMessage = alertMessage};

      // Extract info
      var channelTitle = channelPageHtml.QueryElements("meta[property=\"og:title\"]")
        .FirstOrDefault()?.GetAttribute("content").Value;

      var channelLogoUrl = channelPageHtml.QueryElements("meta[property=\"og:image\"]")
        .FirstOrDefault()?.GetAttribute("content").Value;

      var subDesc = channelPageHtml.QueryElements("span.yt-subscription-button-subscriber-count-branded-horizontal.subscribed").FirstOrDefault()
        ?.GetInnerText();
      return new ChannelExtended {
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

    public async Task<IReadOnlyCollection<RecsAndExtra>> GetRecsAndExtra(IReadOnlyCollection<string> videos, ILogger log) =>
      await videos.BlockFunc(async v => await GetRecsAndExtra(v, log), CollectCfg.WebParallel);

    //ytInitialPlayerResponse.responseContext.serviceTrackingParams.filter(p => p.service == "CSI")[0].params
    public async Task<RecsAndExtra> GetRecsAndExtra(string videoId, ILogger log) {
      var watchPage = await GetVideoWatchPageHtmlAsync(videoId, log);
      var (html, raw, url) = watchPage;
      var infoDic = await GetVideoInfoDicAsync(videoId, log);
      var videoItem = GetVideo(videoId, infoDic, watchPage);

      var extra = new VideoExtraStored2 {
        VideoId = videoId,
        Updated = DateTime.UtcNow,
        ChannelId = videoItem?.ChannelId,
        ChannelTitle = videoItem?.ChannelTitle,
        Description = videoItem?.Description,
        Duration = videoItem?.Duration,
        Keywords = videoItem?.Keywords,
        Title = videoItem?.Title,
        UploadDate = videoItem?.UploadDate.UtcDateTime,
        Statistics = videoItem?.Statistics,
        Source = ScrapeSource.Web,
        Thumbnail = VideoThumbnail.FromVideoId(videoId)
      };

      var ytInitPr = GetClientObjectFromWatchPage(html, "ytInitialPlayerResponse");
      if (ytInitPr != null && ytInitPr.Value<string>("status") != "OK") {
        var playerError = ytInitPr.SelectToken("playabilityStatus.errorScreen.playerErrorMessageRenderer");
        extra.Error = playerError?.SelectToken("reason.simpleText")?.Value<string>();
        extra.SubError = (playerError?.SelectToken("subreason.simpleText") ?? 
                          playerError?.SelectToken("subreason.runs[0].text"))
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
      if (extra.Error != null) return new RecsAndExtra(extra, new Rec[] { });

      var (recs, recEx) = Def.New(() => GetRecs2(html)).Try();
      if (recs?.Any() != true || recEx != null) {
        var uri = new Uri(url);
        var path = StringPath.Relative(DateTime.UtcNow.ToString("yyyy-MM-dd"), $"{uri.PathAndQuery}.html");
        var logUrl = LogStore.Url(path);
        await LogStore.Save(path, raw.AsStream(), log);
        log.Warning("WebScraper - Unable to find recs at ({Url}). error: {Error}", logUrl, recEx?.ToString());
      }

      return new RecsAndExtra(extra, recs);
    }

    static (string channelTitle, string channelId) ChannelInfoFromWatchPage(HtmlDocument html) {
      var userInfo = html.QueryElements("div.yt-user-info > a").FirstOrDefault();
      if (userInfo == null) return (null, null);

      var title = userInfo.GetInnerText();
      var url = userInfo.GetAttribute("href")?.Value;
      var id = url?.Split('/').Last();

      return (title, id);
    }

    static readonly Regex WindowObjectsRe = new Regex("^.*window\\[\"(?<name>\\w+)\"\\]\\s*=\\s*(?<json>{.*?});?$",
      RegexOptions.Compiled | RegexOptions.Multiline);

    public Rec[] GetRecs2(HtmlDocument html) {
      var jInit = GetClientObjectFromWatchPage(html) ?? throw new InvalidOperationException("can't find ytInitialData data script to get recs from");
      var resultsSel = "$.contents.twoColumnWatchNextResults.secondaryResults.secondaryResults.results";
      var jResults = (JArray) jInit.SelectToken(resultsSel) ?? throw new InvalidOperationException($"can't find {resultsSel}");
      var recs = jResults
        .OfType<JObject>()
        .Select(j => j.SelectToken("compactAutoplayRenderer.contents[0].compactVideoRenderer") ?? j.SelectToken("compactVideoRenderer"))
        .Where(j => j != null)
        .Select((j, i) => {
          var viewText = (j.SelectToken("viewCountText.simpleText") ?? j.SelectToken("viewCountText.runs[0].text"))?.Value<string>();
          return new Rec {
            ToVideoId = j.Value<string>("videoId"),
            ToVideoTitle = j["title"]?.Value<string>("simpleText") ?? j.SelectToken("title.runs[0].text")?.Value<string>(),
            ToChannelId = j.Value<string>("channelId"),
            ToChannelTitle = j.SelectToken("longBylineText.runs[0].text")?.Value<string>(),
            Rank = i + 1,
            Source = ScrapeSource.Web,
            ToViews = ChromeScraper.ParseViews(viewText),
            ToUploadDate = ChromeScraper.ParseAgo(DateTime.UtcNow, j.SelectToken("publishedTimeText.simpleText")?.Value<string>()),
            ForYou = ChromeScraper.ParseForYou(viewText)
          };
        }).ToArray();
      return recs;
    }

    static JObject GetClientObjectFromWatchPage(HtmlDocument html, string name = "ytInitialData") {
      var scripts = html.QueryElements("script")
        .SelectMany(s => s.Children.OfType<HtmlText>()).Select(h => h.Content);

      var windowObjects = scripts
        .SelectMany(t => WindowObjectsRe.Matches(t))
        .ToDictionary(m => m.Groups["name"].Value, m => m.Groups["json"].Value);

      var initData = windowObjects.TryGet(name);
      if (initData == null) return null;

      var jInit = JObject.Parse(initData);
      return jInit;
    }

    public async Task<IReadOnlyCollection<ClosedCaptionTrackInfo>> GetCaptions(string videoId, ILogger log) {
      var videoInfoDic = await GetVideoInfoDicAsync(videoId, log);
      var playerResponseJson = JToken.Parse(videoInfoDic["player_response"]);
      var captions = GetCaptions(playerResponseJson);
      return captions;
    }

    public async Task<VideoItem> GetVideo(string videoId, ILogger log) {
      if (!ValidateVideoId(videoId))
        throw new ArgumentException($"Invalid YouTube video ID [{videoId}].", nameof(videoId));
      var videoInfoDic = await GetVideoInfoDicAsync(videoId, log);
      var videoWatchPage = await GetVideoWatchPageHtmlAsync(videoId, log);
      return GetVideo(videoId, videoInfoDic, videoWatchPage);
    }

    static VideoItem GetVideo(string videoId, IReadOnlyDictionary<string, string> videoInfoDic,
      (HtmlDocument html, string raw, string url) videoWatchPage) {
      if (!videoInfoDic.ContainsKey("player_response"))
        return null;
      
      var responseJson = JToken.Parse(videoInfoDic["player_response"]);
      var renderer = responseJson.SelectToken("microformat.playerMicroformatRenderer");

      if (string.Equals(responseJson.SelectToken("playabilityStatus.status")?.Value<string>(), "error",
        StringComparison.OrdinalIgnoreCase))
        return null;

      T VideoValue<T>(string propName) {
        var token = responseJson.SelectToken($"videoDetails.{propName}");
        return token == null ? default : token.Value<T>();
      }

      var videoAuthor = VideoValue<string>("author");
      var videoTitle = VideoValue<string>("title");
      var videoDuration = TimeSpan.FromSeconds(VideoValue<double>("lengthSeconds"));
      var videoKeywords = responseJson.SelectToken("videoDetails.keywords").NotNull().Values<string>().ToArray();
      var videoDescription = VideoValue<string>("shortDescription");
      var videoViewCount = VideoValue<long>("viewCount"); // some videos have no views
      var channelId = VideoValue<string>("channelId");
      var channelTitle = VideoValue<string>("author");

      var videoUploadDate = renderer?.SelectToken("uploadDate")?.Value<string>().ParseDateTimeOffset("yyyy-MM-dd") ?? default;

      var videoLikeCountRaw = videoWatchPage.html.GetElementsByClassName("like-button-renderer-like-button")
        .FirstOrDefault()?.GetInnerText().StripNonDigit();
      var videoLikeCount = !videoLikeCountRaw.IsNullOrWhiteSpace() ? videoLikeCountRaw.ParseLong() : 0;
      var videoDislikeCountRaw = videoWatchPage.html.GetElementsByClassName("like-button-renderer-dislike-button")
        .FirstOrDefault()?.GetInnerText().StripNonDigit();
      var videoDislikeCount = !videoDislikeCountRaw.IsNullOrWhiteSpace() ? videoDislikeCountRaw.ParseLong() : 0;

      var statistics = new Statistics(videoViewCount, videoLikeCount, videoDislikeCount);
      var thumbnails = new ThumbnailSet(videoId);

      return new VideoItem(videoId, videoAuthor, videoUploadDate, videoTitle, videoDescription,
        thumbnails, videoDuration, videoKeywords, statistics, channelId, channelTitle);
    }

    static IReadOnlyCollection<ClosedCaptionTrackInfo> GetCaptions(JToken playerResponseJson) =>
      (from trackJson in playerResponseJson.SelectToken("..captionTracks").NotNull()
        let url = new UriBuilder(trackJson.SelectToken("baseUrl").Value<string>()).WithParameter("format", "3")
        let languageCode = trackJson.SelectToken("languageCode").Value<string>()
        let languageName = trackJson.SelectToken("name.simpleText").Value<string>()
        let language = new Language(languageCode, languageName)
        let isAutoGenerated = trackJson.SelectToken("vssId")
          .Value<string>()
          .StartsWith("a.", StringComparison.OrdinalIgnoreCase)
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

      return new ClosedCaptionTrack(info, captions.ToList());
    }

    async Task<XElement> GetClosedCaptionTrackXmlAsync(string url, ILogger log) {
      var raw = await GetHttp(url, "caption", log);
      using var s = await raw.Content.ReadAsStreamAsync();
      var xml = await XElement.LoadAsync(s, LoadOptions.PreserveWhitespace, CancellationToken.None);
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
    public RecsAndExtra(VideoExtraStored2 extra, Rec[] recs) {
      Extra = extra;
      Recs = recs;
    }

    public VideoExtraStored2 Extra { get; set; }
    public Rec[]             Recs  { get; set; }
  }
}