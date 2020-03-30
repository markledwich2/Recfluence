using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Humanizer;
using LtGt;
using Newtonsoft.Json.Linq;
using Polly;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

//// a modified version of https://github.com/Tyrrrz/YoutubeExplode

namespace YtReader.YtWebsite {
  public class YtScraper {
    readonly ScraperCfg Cfg;

    readonly HttpClient DirectHttp;
    readonly HttpClient ProxyHttp;

    public YtScraper(ScraperCfg scraperCfg) {
      Cfg = scraperCfg;
      DirectHttp = CreateHttpClient(false);
      ProxyHttp = CreateHttpClient(true);
    }

    HttpClient CreateHttpClient(bool useProxy) =>
      new HttpClient(new HttpClientHandler {
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        UseCookies = false,
        Proxy = useProxy ? new WebProxy("us.smartproxy.com:10000", true, new string[] { }, new NetworkCredential(Cfg.Creds.Name, Cfg.Creds.Secret)) : null,
        UseProxy = useProxy
      }) {
        Timeout = Cfg.TimeoutSeconds.Seconds()
      };

    long         DirectHttpFailures;
    const string MissingYtResourceMessage = "Received BadRequest response, which means YT resource is missing";
    long         _directRequests;
    long         _proxyRequests;

    public (long direct, long proxy) RequestStats => (_directRequests, _proxyRequests);

    async Task<string> GetRaw(string url, string desc, ILogger log) {
      log.Debug("Scraping {Desc} {Url}", desc, url);

      var useDirect = Interlocked.Read(ref DirectHttpFailures) == 0;

      if (useDirect)
        try {
          var directPolicy = Policy
            .HandleResult<HttpResponseMessage>(m => m.IsTransientError())
            .RetryWithBackoff(desc, Cfg.Retry, log);

          var directRes = await directPolicy.ExecuteAsync(async () => {
            var get = await DirectHttp.GetAsync(url);
            ThrowMissingResourceInvalidOpIfNeeded(get);
            return get;
          });

          var sw = Stopwatch.StartNew();
          directRes.EnsureSuccessStatusCode();
          var raw = await directRes.ContentAsString();
          Interlocked.Increment(ref _directRequests);
          log.Debug("Direct scraped {Desc} {Url} in {Duration}", desc, url, sw.Elapsed);
          return raw;
        }
        catch (Exception ex) {
          if (ex is InvalidOperationException && ex.Message == MissingYtResourceMessage)
            throw; // fail early, don't fall back to proxy for this error
          Interlocked.Increment(ref DirectHttpFailures);
          log.Debug(ex, "Direct connection failed with {Error}. Falling back to proxy", ex.Message);
        }

      var proxyPolicy = Policy
        .Handle<HttpRequestException>().Or<TaskCanceledException>()
        .RetryWithBackoff(desc, Cfg.Retry, log);

      var res = await proxyPolicy.ExecuteAsync(async () => {
        var get = ProxyHttp.GetAsync(url);
        if (await Task.WhenAny(get, Task.Delay((Cfg.TimeoutSeconds + 10).Seconds())) != get)
          throw new TaskCanceledException($"GetStringAsync on {url} took to long without timing out itself");

        var innerRes = await get;
        ThrowMissingResourceInvalidOpIfNeeded(innerRes);
        innerRes.EnsureSuccessStatusCode();
        return await innerRes.ContentAsString();
      }).WithDuration();

      Interlocked.Increment(ref _proxyRequests);
      log.Debug("Proxy scraped {Desc} {Url} in {Duration}", desc, url, res.Duration);
      return res.Result;
    }

    static void ThrowMissingResourceInvalidOpIfNeeded(HttpResponseMessage directRes) {
      if (directRes.StatusCode == HttpStatusCode.BadRequest)
        throw new InvalidOperationException(MissingYtResourceMessage);
    }

    async Task<HtmlDocument> GetHtml(string url, string desc, ILogger log) => Html.ParseDocument(await GetRaw(url, desc, log));

    #region Public Static

    /// <summary>
    ///   Verifies that the given string is syntactically a valid YouTube channel ID.
    /// </summary>
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

    /// <summary>
    ///   Verifies that the given string is syntactically a valid YouTube playlist ID.
    /// </summary>
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

    /// <summary>
    ///   Parses video ID from a YouTube video URL.
    /// </summary>
    public static string ParseVideoId(string videoUrl) =>
      TryParseVideoId(videoUrl, out var result)
        ? result
        : throw new FormatException($"Could not parse video ID from given string [{videoUrl}].");

    /// <summary>
    ///   Tries to parse video ID from a YouTube video URL.
    /// </summary>
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
      var raw = await GetRaw($"https://youtube.com/list_ajax?style=json&action_get_list=1&list={playlistId}&index={index}&hl=en", "playlist", log);
      return JToken.Parse(raw);
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

    async Task<(HtmlDocument html, string raw, string url)> GetVideoWatchPageHtmlAsync(string videoId, ILogger log) {
      var url = $"https://youtube.com/watch?v={videoId}&disable_polymer=true&bpctr=9999999999&hl=en-us";
      var raw = await GetRaw(url, "video watch", log);
      return (Html.ParseDocument(raw), raw, url);
    }

    static readonly Regex _ytAdRegex = new Regex(@"""yt_ad"":""??([0-1])""?", RegexOptions.Compiled);

    public const string RestrictedVideoError = "Restricted";

    //ytInitialPlayerResponse.responseContext.serviceTrackingParams.filter(p => p.service == "CSI")[0].params
    public async Task<(IReadOnlyCollection<Rec> recs, VideoExtraStored2 extra)> GetRecsAndExtra(string videoId, ILogger log) {
      var (html, raw, url) = await GetVideoWatchPageHtmlAsync(videoId, log);

      var channel = ChannelInfoFromWatchPage(html);
      var extra = new VideoExtraStored2 {
        Id = videoId,
        Updated = DateTime.UtcNow,
        ChannelId = channel.channelId,
        ChannelTitle = channel.channelTitle
      };

      var restrictedMode = html.QueryElements("head > meta[property=\"og:restrictions:age\"]").FirstOrDefault()?.GetAttribute("content")?.Value == "18+";
      if (restrictedMode) {
        extra.Error = RestrictedVideoError;
        extra.SubError = "Unable to find recommended video because it is age restricted and requires to log in";
      }
      else {
        extra.Error = html.QueryElements("#unavailable-message").FirstOrDefault()?.GetInnerText();
        extra.SubError = html.QueryElements("#unavailable-submessage").FirstOrDefault()?.GetInnerText();
      }
      if (extra.Error != null) return (new Rec[] { }, extra);

      var recs = GetRecs(html, url, log);
      if (!recs.Any())
        log.Warning("No error, but unable to find recommended videos: {Url}", url);

      var match = _ytAdRegex.Match(raw);
      extra.HasAd = match.Success && match.Groups[1].Value == "1";

      return (recs, extra);
    }

    static (string channelTitle, string channelId) ChannelInfoFromWatchPage(HtmlDocument html) {
      var userInfo = html.QueryElements("div.yt-user-info > a").FirstOrDefault();
      if (userInfo == null) return (null, null);

      var title = userInfo.GetInnerText();
      var url = userInfo.GetAttribute("href")?.Value;
      var id = url?.Split('/').Last();

      return (title, id);
    }

    IReadOnlyCollection<Rec> GetRecs(HtmlDocument html, string url, ILogger log) {
      var recs = (from d in html.QueryElements("li.video-list-item.related-list-item").Select((e, i) => (e, i))
        let titleSpan = d.e.QueryElements("span.title").FirstOrDefault()
        let channelSpan = d.e.QueryElements("span.stat.attribution > span").FirstOrDefault()
        let videoA = d.e.QueryElements("a.content-link").FirstOrDefault()
        where videoA != null && channelSpan != null && videoA != null
        select new Rec {
          ToVideoId = ParseVideoId($"https://youtube.com/{videoA.GetAttribute("href").Value}"),
          ToVideoTitle = titleSpan.GetInnerText(),
          ToChannelTitle = channelSpan.GetInnerText(),
          Rank = d.i + 1
        }).ToList();

      return recs;
    }

    public async Task<IReadOnlyCollection<ClosedCaptionTrackInfo>> GetCaptions(string videoId, ILogger log) {
      var videoInfoDic = await GetVideoInfoDicAsync(videoId, log);
      var playerResponseJson = JToken.Parse(videoInfoDic["player_response"]);
      var captions = GetCaptions(playerResponseJson);
      return captions;
    }

    public async Task<VideoItem> GetVideoAsync(string videoId, ILogger log) {
      if (!ValidateVideoId(videoId))
        throw new ArgumentException($"Invalid YouTube video ID [{videoId}].", nameof(videoId));

      var videoInfoDicTask = GetVideoInfoDicAsync(videoId, log);
      var videoWatchPageTask = GetVideoWatchPageHtmlAsync(videoId, log);

      var videoInfoDic = await videoInfoDicTask;
      var responseJson = JToken.Parse(videoInfoDic["player_response"]);

      if (string.Equals(responseJson.SelectToken("playabilityStatus.status")?.Value<string>(), "error",
        StringComparison.OrdinalIgnoreCase))
        return null;

      T VideoValue<T>(string propName) {
        var token = responseJson.SelectToken($"videoDetails.{propName}");
        return token == null ? default : token.Value<T>();
      }

      var videoAuthor = VideoValue<string>("author");
      var videoTitle = VideoValue<string>("title");
      var videoDuration = TimeSpan.FromSeconds(VideoValue<double>("videoDetails.lengthSeconds"));
      var videoKeywords = responseJson.SelectToken("videoDetails.keywords").NotNull().Values<string>().ToArray();
      var videoDescription = VideoValue<string>("shortDescription");
      var videoViewCount = VideoValue<long>("viewCount"); // some videos have no views
      var channelId = VideoValue<string>("channelId");
      var channelTitle = VideoValue<string>("author");

      var (videoWatchPageHtml, _, _) = await videoWatchPageTask;
      var videoUploadDate = videoWatchPageHtml.QueryElements("meta[itemprop=\"datePublished\"]")
                              .FirstOrDefault()?.GetAttribute("content").Value.ParseDateTimeOffset("yyyy-MM-dd") ??
                            throw new InvalidOperationException("No upload date found in page");
      var videoLikeCountRaw = videoWatchPageHtml.GetElementsByClassName("like-button-renderer-like-button")
        .FirstOrDefault()?.GetInnerText().StripNonDigit();
      var videoLikeCount = !videoLikeCountRaw.IsNullOrWhiteSpace() ? videoLikeCountRaw.ParseLong() : 0;
      var videoDislikeCountRaw = videoWatchPageHtml.GetElementsByClassName("like-button-renderer-dislike-button")
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
      var raw = await GetRaw($"https://youtube.com/get_video_info?video_id={videoId}&el=embedded&eurl={eurl}&hl=en", "video dictionary", log);
      var result = SplitQuery(raw);
      return result;
    }

    static IReadOnlyDictionary<string, string> SplitQuery(string query) {
      var dic = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
      var paramsEncoded = query.TrimStart('?').Split("&");
      foreach (var paramEncoded in paramsEncoded) {
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
      }

      return dic;
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
      var raw = await GetRaw(url, "caption", log);
      return XElement.Parse(raw, LoadOptions.PreserveWhitespace).StripNamespaces();
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
    public string    ToVideoId      { get; set; }
    public string    ToVideoTitle   { get; set; }
    public string    ToChannelTitle { get; set; }
    public string    ToChannelId    { get; set; } // only known if from the API
    public RecSource Source         { get; set; }
    public int       Rank           { get; set; }
  }

  public enum RecSource {
    Web,
    Api
  }
}