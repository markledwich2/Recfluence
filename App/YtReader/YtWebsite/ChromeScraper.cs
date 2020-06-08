using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Humanizer;
using Mutuo.Etl.Blob;
using PuppeteerSharp;
using Serilog;
using Serilog.Events;
using Serilog.Extensions.Logging;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Security;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;

namespace YtReader.YtWebsite {
  public class ChromeScraper {
    readonly ProxyCfg         ProxyCfg;
    readonly YtCollectCfg     CollectCfg;
    readonly ISimpleFileStore LogStore;

    public ChromeScraper(ProxyCfg proxyCfg, YtCollectCfg collectCfg, ISimpleFileStore logStore) {
      ProxyCfg = proxyCfg;
      CollectCfg = collectCfg;
      LogStore = logStore;
    }

    public async Task Init() => await new BrowserFetcher().DownloadAsync(BrowserFetcher.DefaultRevision);

    static readonly ILogger ConsoleLog = new LoggerConfiguration().WriteTo.Console().CreateLogger();

    async Task<Browser> CreateBrowser(bool proxy, ILogger log) {
      var browser = await Puppeteer.LaunchAsync(new LaunchOptions {
        Headless = true,
        DefaultViewport = new ViewPortOptions {Width = 1024, Height = 1200},
        Args = new[] {
          "--disable-dev-shm-usage", "--no-sandbox",
          proxy ? $"--proxy-server={ProxyCfg.Url}" : null
        }.NotNull().ToArray()
      }, new SerilogLoggerFactory(ConsoleLog)); // log to console only because it often has transient errors
      return browser;
    }

    public async Task<IReadOnlyCollection<RecsAndExtra>> GetRecsAndExtra(IReadOnlyCollection<string> videos, ILogger log) {
      var sw = Stopwatch.StartNew();
      log = log.ForContext("Module", nameof(ChromeScraper));
      var proxy = false;

      var browsers = CollectCfg.ChromeParallel;
      var videosPerBrowser = videos.Count / CollectCfg.ChromeParallel + 1;
      var res = await videos.Batch(videosPerBrowser).BlockFunc(async b => {
        var requests = new ConcurrentBag<(Request req, bool aborted)>();
        using var browser = await CreateBrowser(proxy, log);
        return await b.BlockFunc(async v => {
          //var context = await browser.CreateIncognitoBrowserContextAsync(); // opening windows is more reliable than tabs in practice
          log.Debug("loading video {Video}", v);
          using var page = await browser.NewPageAsync();
          if (proxy)
            await page.AuthenticateAsync(ProxyCfg.Creds.AsCreds());
          await page.SetCookieAsync(); // clears cookies
          await ConfigureRequests(log, page, v, (req, aborted) => requests.Add((req, aborted)));
          var attempt = 0;
          while (true) {
            try {
              var video = await GetVideo(page, v, log);
              log.Debug("ChromeScraper - finished loading video {Video} with {Comments} comments and {Recommendations} recs in {Duration}",
                v, video.Extra.Comments?.Length ?? 0, video.Recs?.Length ?? 0, sw.Elapsed.HumanizeShort());
              return video;
            }
            catch (Exception ex) {
              var lastAttempt = attempt >= CollectCfg.ChromeAttempts;
              log.Write(lastAttempt ? LogEventLevel.Error : LogEventLevel.Warning, ex,
                "ChromeScraper - failed to load video {Video}: {Error}", v, ex.Message);
              if (lastAttempt)
                throw;
            }
            attempt++;
          }
        });
      }, browsers);

      log.Information("ChromeScraper - loaded {Videos} in {Duration}", videos.Count, sw.HumanizeShort());
      return res.SelectMany().ToReadOnly();
    }

    #region Request handling

    static readonly Regex AbortPathRe = new Regex("\\.(woff2|png|ico)$", RegexOptions.Compiled);

    static bool AbortRequest(Request req) {
      if (req.ResourceType.In(ResourceType.Image, ResourceType.Media, ResourceType.Font, ResourceType.TextTrack, ResourceType.Ping, ResourceType.Manifest))
        return true;
      var uri = new Uri(req.Url);
      var path = uri.AbsolutePath;
      if (path.StartsWith("/videoplayback") ||
          AbortPathRe.IsMatch(path) ||
          path.In("ptracking", "/youtubei/v1/log_event") ||
          path.StartsWith("/api/stats") ||
          uri.Host.In("googleads.g.doubleclick.net",
            "static.doubleclick.net",
            "tpc.googlesyndication.com",
            "fonts.googleapis.com",
            "i.ytimg.com",
            "accounts.google.com")
      )
        return true;
      return false;
    }

    static async Task ConfigureRequests(ILogger log, Page page, string v, Action<Request, bool> onHandled) {
      await page.SetRequestInterceptionAsync(true);
      page.Request += async (sender, e) => {
        if (AbortRequest(e.Request)) {
          if (!e.Request.Failure.HasValue())
            await e.Request.AbortAsync();
          onHandled(e.Request, true);
        }
        else {
          //log.Debug("{@Request} {@Response} ({Type}) for {Video}", e.Request, e.Request.ResourceType, v);
          await e.Request.ContinueAsync();
          onHandled(e.Request, false);
        }
      };
    }

    #endregion

    const string CommentCountSel = "#comments #count";
    const string CommentErrorSel = "#comments #message";
    const string VideoErrorSel   = "#info > .reason.yt-player-error-message-renderer";

    async Task<RecsAndExtra> GetVideo(Page page, string videoId, ILogger log) {
      log = log.ForContext("Video", videoId);
      await page.GoToAsync($"https://youtube.com/watch?v={videoId}");
      // wait for either a single comment, or a message that the coments are turned off. This is the slowest part to load.
      await WaitForSelector(page,
        new[] {CommentCountSel, CommentErrorSel, VideoErrorSel}.Join(", "),
        2.Minutes(), log);
      var video = await VideoDetails(page, videoId);
      var error = await page.EvaluateFunctionAsync<string>(@$"() => {{
  var el = document.querySelector('{VideoErrorSel}')
  return el ? el.innerText : null
}}");

      var subError = await page.EvaluateFunctionAsync<string>(@"() => {
  var el = document.querySelector('#info > .subreason.yt-player-error-message-renderer')
  return el ? el.innerText : null
}");

      if (video == null) {
        if (error == null) throw new InvalidOperationException("can't find video or error message, probably a bug");
        return new RecsAndExtra(new VideoExtraStored2 {
          VideoId = videoId,
          Error = error,
          SubError = subError
        }, recs: default);
      }

      video.Error = error;
      video.SubError = subError;

      await ScrollDown(page, maxScroll: null, videoId, log);
      (video.Ad, video.HasAd) = await GetAd(page);
      var recs = await GetRecs(page, error.HasValue(), log);
      (video.Comments, video.CommentsMsg, video.CommentCount) = await GetComments(page, video);
      return new RecsAndExtra(video, recs);
    }

    static async Task ScrollDown(Page page, int? maxScroll, string videoId, ILogger log) {
      var scrollHeight = 0;
      while (true) {
        var newScrollHeight = await page.EvaluateExpressionAsync<int>("document.scrollingElement.scrollHeight");
        if (newScrollHeight == scrollHeight || maxScroll != null && newScrollHeight > maxScroll.Value) {
          log.Debug("scrolled down on {Video} to {ScrollHeight}px", videoId, newScrollHeight);
          break;
        }
        await page.EvaluateExpressionAsync(@"document.scrollingElement.scroll(0, document.scrollingElement.scrollHeight)");
        scrollHeight = newScrollHeight;
        await 1.Seconds().Delay();
      }
    }

    static async Task<(VideoCommentStored2[] comments, string msg, long? count)> GetComments(Page page, VideoExtraStored2 video) {
      try {
        await page.WaitForSelectorAsync(CommentCountSel);
      }
      catch (WaitTaskTimeoutException) { }

      var commentCount = await page.EvaluateFunctionAsync<long?>(@"() => {
    var el = document.querySelector('#comments #count')
    if(!el) return null
    var match =/(?<num>\d+) Comment/.exec(el.innerText.replace(/,/g, ''))
    if(!match) return null
    return parseInt(match.groups.num)
}");

      var commentSel = @"Array.from(document.querySelectorAll('ytd-comment-renderer#comment')).map(c => {
    var authorEl = c.querySelector('a#author-text')
    var channelMatch  = /\/channel\/(.*)/.exec(authorEl.href)
    return {
        author: authorEl.innerText, 
        authorChannelId: (channelMatch && channelMatch.length > 1) ? channelMatch[1] : null,
        comment: c.querySelector('#content-text').innerText,
        ago: c.querySelector('yt-formatted-string.published-time-text').innerText
     }
})";

      var videoComments = await page.EvaluateExpressionAsync<VideoComment[]>(commentSel);
      var comments = videoComments.Select(c => new VideoCommentStored2 {
        Author = c.author,
        AuthorChannelId = c.authorChannelId,
        Comment = c.comment,
        ChannelId = video.ChannelId,
        VideoId = video.VideoId,
        Created = ParseAgo(DateTime.UtcNow, c.ago)
      }).ToArray();

      if (comments.HasItems())
        return (comments, default, commentCount);

      var msg = await page.EvaluateFunctionAsync<string>(@$"() => {{
  var el = document.querySelector('{CommentErrorSel}')
  return el ? el.innerText : null
}}");

      if (commentCount < 3 || msg.HasValue())
        return (default, msg, commentCount);

      throw new InvalidOperationException("can't find comments, or a message about no-comments. Probably a bug");
    }

    async Task<Rec[]> GetRecs(Page page, bool hasError, ILogger log) {
      var recsSel = @"() => {
    var watchNext = ytInitialData.contents.twoColumnWatchNextResults
    if(!watchNext || !watchNext.secondaryResults || !watchNext.secondaryResults.secondaryResults) return null
    return watchNext.secondaryResults.secondaryResults.results
    .map(r => r.compactAutoplayRenderer && Object.assign({}, r.compactAutoplayRenderer.contents[0].compactVideoRenderer, {autoPlay:true}) 
        || r.compactVideoRenderer &&  Object.assign({}, r.compactVideoRenderer, {autoPlay:false}) 
        || null)
    .filter(r => r != null)
    .map((v,i) =>({
        videoId:v.videoId, 
        title: v.title && v.title.simpleText || v.title && v.title.runs && v.title.runs[0].text, 
        thumb: v.thumbnail && v.thumbnail.thumbnails[v.thumbnail.thumbnails.length -1].url,
        channelTitle: v.longBylineText && v.longBylineText.runs[0].text, 
        publishAgo: v.publishedTimeText && v.publishedTimeText.simpleText, 
        viewText: v.viewCountText && v.viewCountText.simpleText || v.viewCountText && v.viewCountText.runs && v.viewCountText.runs[0].text,
        duration:v.lengthText && v.lengthText.simpleText, 
        channelId:v.channelId,
        rank:i+1
    }))
}";

      var attempts = 3;
      while (true) {
        var recs = await page.EvaluateFunctionAsync<CompactVideoRenderer[]>(recsSel);
        if (recs != null) {
          var recRes = recs.Select(r => new Rec {
            Source = ScrapeSource.Chrome,
            ToVideoId = r.videoId,
            ToChannelId = r.channelId,
            ToChannelTitle = r.channelTitle,
            Rank = r.rank,
            ToVideoTitle = r.title,
            ToViews = ParseViews(r.viewText),
            ToUploadDate = ParseAgo(DateTime.UtcNow, r.publishAgo),
            ForYou = r.viewText == "Recommended for you",
          }).ToArray();

          return recRes;
        }
        if (hasError)
          return new Rec[] { };

        if (attempts <= 0) {
          var (html, img) = await SavePageDump(page, log);
          Log.Warning("ChromeScraper - unable to find recs in video at url {Url}. See {Html} {Img}", page.Url, html, img);
          return new Rec[] { };
        }
        await 1.Seconds().Delay();
        attempts--;
      }
    }

    async Task WaitForSelector(Page page, string selector, TimeSpan timeout, ILogger log) {
      try {
        await page.WaitForSelectorAsync(selector, new WaitForSelectorOptions {Timeout = (int) timeout.TotalMilliseconds});
      }
      catch (WaitTaskTimeoutException e) {
        log.Warning(e, "Waiting for '{Selector}' timed out on {Page}", selector, page.Uri().PathAndQuery);
      }
    }

    async Task<(Uri html, Uri image)> SavePageDump(Page page, ILogger log) {
      var dom = await page.MainFrame.GetContentAsync();
      var imgStream = await page.ScreenshotStreamAsync();
      var basePath = StringPath.Relative(DateTime.UtcNow.ToString("yyyy-MM-dd"));
      var pageName = page.Uri().PathAndQuery;
      var htmlPath = basePath.Add($"{pageName}.html");
      var imgPath = basePath.Add($"{pageName}.png");
      await Task.WhenAll(LogStore.Save(htmlPath, dom.AsStream(), log), LogStore.Save(imgPath, imgStream, log));
      return (LogStore.Url(htmlPath), LogStore.Url(imgPath));
    }

    static async Task<(string ad, bool hasAd)> GetAd(Page page) {
      var ad = await page.EvaluateFunctionAsync<string>(@"() => {
    var ad = document.querySelector('button[id^=visit-advertiser] > span.ytp-ad-button-text')
    return ad && ad.innerText || null
}");

      var hasAd = await page.EvaluateFunctionAsync<bool>(@"() => {
    var service = ytInitialPlayerResponse.responseContext.serviceTrackingParams.filter(p => p.service == 'CSI')
      if(!service || service.length == 0) return false
      var ytAd = service[0].params.filter(p => p.key =='yt_ad')
      return ytAd && ytAd.length > 0 && ytAd[0].value == '1' || false
    }");

      return (ad, hasAd);
    }

    async Task<VideoExtraStored2> VideoDetails(Page page, string videoId) {
      var details = await page.EvaluateFunctionAsync<VideDetails>(@"() => {
     var v = ytInitialPlayerResponse.videoDetails
     if(!v) return null
     return {
        author:v.author,
        channelId:v.channelId,
        keywords:v.keywords,
        lengthSeconds:v.lengthSeconds,
        shortDescription:v.shortDescription,
        thumbnail:v.thumbnail.thumbnails[v.thumbnail.thumbnails.length - 1].url,
        title:v.title,
        videoId:v.videoId,
        viewCount:v.viewCount
    }
}");
      if (details == null) return null;

      var detailsScript = await page.EvaluateExpressionAsync<VideoDetailsFromScript>(
        "JSON.parse(document.querySelector('script.ytd-player-microformat-renderer').innerText)");
      var likeDislike = await page.EvaluateExpressionAsync<LikesDislikes>(
        @"Object.assign({}, ...ytInitialData.contents.twoColumnWatchNextResults.results.results.contents
    .map(c => c.videoPrimaryInfoRenderer)
    .filter(c => c)
    .flatMap(c => c.videoActions.menuRenderer.topLevelButtons)
    .map(b => b.toggleButtonRenderer).filter(b => b)
    .map(b => b.defaultText.accessibility.accessibilityData.label)
    .map((l,i) => {
        var m = /^(\d*,?\d*|No) (likes|dislikes)/.exec(l)
        if(!m) {
            console.log('like/dislike label', l)
            return {}
        }
        var key = m.length >= 3 ? m[2] : `position ${i}`
        return {[key] : m[1] == 'No' ? 0 : parseInt(m[1].replace(/,/g, ''))}
    })
)");

      var video = new VideoExtraStored2 {
        VideoId = videoId,
        Description = details.shortDescription,
        Duration = details.lengthSeconds.Seconds(),
        Keywords = details.keywords,
        Title = details.title,
        ChannelId = details.channelId,
        ChannelTitle = details.author,
        UploadDate = detailsScript.uploadDate,
        Statistics = new Statistics(details.viewCount, likeDislike.likes, likeDislike.dislikes),
        Thumbnail = VideoThumbnail.FromVideoId(videoId),
        Updated = DateTime.UtcNow,
        Source = ScrapeSource.Chrome
      };

      return video;
    }

    class VideDetails {
      public string   shortDescription { get; set; }
      public string[] keywords         { get; set; }
      public string   channelId        { get; set; }
      public string   author           { get; set; }
      public string   title            { get; set; }
      public long     viewCount        { get; set; }
      public int      lengthSeconds    { get; set; }
      public string   thumbnail        { get; set; }
    }

    class LikesDislikes {
      public int? likes    { get; set; }
      public int? dislikes { get; set; }
    }

    class CompactVideoRenderer {
      public string videoId      { get; set; }
      public string title        { get; set; }
      public string thumb        { get; set; }
      public string channelTitle { get; set; }
      public string publishAgo   { get; set; }
      public string viewText     { get; set; }
      public string duration     { get; set; }
      public string channelId    { get; set; }
      public int    rank         { get; set; }
    }

    static long? ParseViews(string s) {
      if (s.NullOrEmpty()) return null;
      var m = Regex.Match(s, "^(\\d+,?\\d*) views");
      if (!m.Success) return null;
      var views = m.Groups[1].Value.ParseLong();
      return views;
    }

    static DateTime? ParseAgo(DateTime now, string ago) {
      if (ago == null) return null;
      var res = Regex.Match(ago, "(?<num>\\d)\\s(?<unit>minute|hour|day|week|month|year)[s]? ago");
      if (!res.Success) return null;
      var num = res.Groups["num"].Value.ParseInt();
      var date = res.Groups["unit"].Value switch {
        "minute" => now - num.Minutes(),
        "hour" => now - num.Hours(),
        "day" => now - num.Days(),
        "week" => now - num.Weeks(),
        "month" => now.AddMonths(-num),
        "year" => now.AddYears(-num),
        _ => throw new InvalidOperationException($"unexpected ago unit {res.Groups["unit"].Value}")
      };
      return date;
    }

    class VideoDetailsFromScript {
      public DateTime uploadDate { get; set; }
    }

    class VideoComment {
      public string author          { get; set; }
      public string authorChannelId { get; set; }
      public string comment         { get; set; }
      public string ago             { get; set; }
    }
  }

  public static class PageEx {
    public static Uri Uri(this Page page) => new Uri(page.Url);
    public static Credentials AsCreds(this NameSecret secret) => new Credentials {Username = secret.Name, Password = secret.Secret};
  }
}