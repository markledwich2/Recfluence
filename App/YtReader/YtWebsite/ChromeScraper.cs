using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Humanizer;
using Mutuo.Etl.Blob;
using Nito.AsyncEx;
using PuppeteerSharp;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Security;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using static YtReader.YtWebsite.AgoUnit;

namespace YtReader.YtWebsite {
  public class ChromeScraper {
    readonly ProxyCfg          ProxyCfg;
    readonly YtCollectCfg      CollectCfg;
    readonly ISimpleFileStore  LogStore;
    readonly AsyncLazy<string> ExecutablePath;

    public ChromeScraper(ProxyCfg proxyCfg, YtCollectCfg collectCfg, ISimpleFileStore logStore) {
      ProxyCfg = proxyCfg;
      CollectCfg = collectCfg;
      LogStore = logStore;
      ExecutablePath = new(async () => {
        var revisionInfo = await new BrowserFetcher().DownloadAsync(802497); //revision needs to be recent to be able to use optional chaining
        return revisionInfo.ExecutablePath;
      });
    }

    async Task<Browser> CreateBrowser(ILogger log, ProxyConnectionCfg proxy) {
      var browser = await Puppeteer.LaunchAsync(new() {
        Headless = CollectCfg.Headless,
        DefaultViewport = new() {Width = 1024, Height = 1200},
        ExecutablePath = await ExecutablePath,
        Args = new[] {
          "--disable-dev-shm-usage", "--no-sandbox",
          proxy != null ? $"--proxy-server={proxy.Url}" : null
        }.NotNull().ToArray()
      }); // logging has so much inconsequential errors we ignore it
      return browser;
    }

    int _lastUsedBrowserIdx; // re-use the same browser proxies across different calls to recs and extra

    public async Task<IReadOnlyCollection<RecsAndExtra>> GetRecsAndExtra(IReadOnlyCollection<string> videos, ILogger log) {
      if (videos.None()) return new RecsAndExtra[] { };

      var sw = Stopwatch.StartNew();
      log = log.ForContext("Module", nameof(ChromeScraper));
      var proxies = ProxyCfg.DirectAndProxies().Take(2)
        .ToArray(); // only use the first fallback (datacenter proxies). residential is too expensive for the volume of data we are loading.
      var parallel = CollectCfg.ChromeParallel;
      var videosPerBrowserPool = videos.Count / CollectCfg.ChromeParallel;

      var recs = await videos.Batch(videosPerBrowserPool).WithIndex().BlockFunc(async batch => {
        var (b, i) = batch;
        log.Debug("ChromeScraper - browser pool {i} for videos {Videos} starting", i, b.Join("|"));
        var (success, res) = await VideoBatch(log, proxies, b, sw, i).WithTimeout((videos.Count * 10).Minutes());
        if (!success)
          log.Warning("ChromeScraper - browser pool {i} for videos {Videos} took too long and timed out");
        return res;
      }, parallel);

      log.Information("ChromeScraper - finished loading all {Videos} videos in {Duration}", videos.Count, sw.HumanizeShort());
      return recs.NotNull().SelectMany().NotNull().ToReadOnly();
    }

    async Task<IReadOnlyCollection<RecsAndExtra>> VideoBatch(ILogger log, ProxyConnectionCfg[] proxies, IReadOnlyCollection<string> b, Stopwatch sw, int i) {
      await using var browsers = new ResourceCycle<Browser, ProxyConnectionCfg>(proxies, p => CreateBrowser(log, p), _lastUsedBrowserIdx);

      return await b.BlockFunc(async v => {
        //var context = await browser.CreateIncognitoBrowserContextAsync(); // opening windows is more reliable than tabs in practice
        var videoAttempt = 0;
        var videoLog = log.ForContext("Video", v);

        while (true) { // retry loop. ome transient errors seems to be caused by state in the page
          videoAttempt++;
          try {
            return await Video(browsers, v, sw, videoLog);
          }
          catch (Exception ex) {
            log.Warning(ex, "ChromeScraper - failed to load video {Video} (attempt {Attempt}): {Error}", v, videoAttempt, ex.Message);
            if (videoAttempt >= CollectCfg.ChromeAttempts)
              throw;
          }
        }
      }, parallel: 1, progressUpdate: p => log.Debug("ChromeScraper - browser pool {Pool} progress {Complete}/{Total}",
        i, p.CompletedTotal, b.Count));
    }

    async Task<Page> Page(Browser browser, ProxyConnectionCfg proxy) {
      var page = await browser.NewPageAsync();
      if (proxy.Creds != null)
        await page.AuthenticateAsync(proxy.Creds.AsCreds()); // workaround for chrome not supporting password proxies
      await page.SetCookieAsync(); // clears cookies
      await ConfigureRequests(page);
      return page;
    }

    async Task<RecsAndExtra> Video(ResourceCycle<Browser, ProxyConnectionCfg> browsers, string videoId, Stopwatch sw, ILogger log) {
      var (browser, proxy) = await browsers.Get();
      log.Debug("loading video {Video}. Proxy={Proxy}", videoId, proxy?.Url ?? "Direct");

      using var page = await Page(browser, proxy);

      var (video, notOkResponse) = await GetVideo(page, videoId, log);
      if (notOkResponse != null) {
        if (notOkResponse.Status == HttpStatusCode.TooManyRequests) {
          await browsers.NextResource(browser);
          _lastUsedBrowserIdx = browsers.Idx;
          log.Information("ChromeScraper - error response ({Status}) loading {Video}: using next proxy", notOkResponse.Status, videoId);
        }
        else {
          throw new InvalidOperationException($"Not OK response ({notOkResponse.Status})");
        }
      }

      if (video == null) return null;
      log.Debug("ChromeScraper - finished loading video {Video} with {Comments} comments and {Recommendations} recs in {Duration}",
        videoId, video.Extra.Comments?.Length ?? 0, video.Recs?.Length ?? 0, sw.Elapsed.HumanizeShort());
      return video;
    }

    #region Request handling

    static readonly Regex AbortPathRe = new("\\.(woff2|png|ico)$", RegexOptions.Compiled);

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

    static async Task ConfigureRequests(Page page) {
      await page.SetRequestInterceptionAsync(true);
      page.Request += async (sender, e) => {
        if (AbortRequest(e.Request)) {
          if (!e.Request.Failure.HasValue())
            await e.Request.AbortAsync();
        }
        else {
          //log.Debug("{@Request} {@Response} ({Type}) for {Video}", e.Request, e.Request.ResourceType, v);
          await e.Request.ContinueAsync();
        }
      };
    }

    #endregion

    const string CommentCountSel = "#comments #count";
    const string CommentErrorSel = "#comments #message";

    class VideoReason {
      public string reason    { get; set; }
      public string subReason { get; set; }
    }

    async Task<(RecsAndExtra video, Response notOkResponse)> GetVideo(Page page, string videoId, ILogger log) {
      log = log.ForContext("Video", videoId);
      var (response, dur) = await page.GoToAsync($"https://youtube.com/watch?v={videoId}", (int) 2.Minutes().TotalMilliseconds,
        new[] {WaitUntilNavigation.Networkidle2, WaitUntilNavigation.Load, WaitUntilNavigation.DOMContentLoaded}).WithDuration();

      log.Debug("ChromeScraper - received response {Status} for video {Video} in {Duration}",
        response.Status, videoId, dur.HumanizeShort());

      if (response.Status != HttpStatusCode.OK)
        return (default, response);

      var (video, error) = await VideoDetails(page, videoId);
      video ??= new() {VideoId = videoId};

      // keep this in sync with UserScrape/crawler.py get_video_unavailable()
      var reason = await page.EvaluateFunctionAsync<VideoReason>(@"() => { 
    var p = ytInitialPlayerResponse.playabilityStatus
    if(!p || p.status == 'OK') return null
    var reason = p.errorScreen?.playerErrorMessageRenderer?.reason?.simpleTex
        ?? p.errorScreen?.playerLegacyDesktopYpcOfferRenderer?.itemTitle
        ?? document.querySelector('#reason.yt-player-error-message-renderer')?.innerText
    var subReason = p.errorScreen?.subreason?.simpleText 
        ?? p.errorScreen?.subreason?.runs?.map(r => r.text).join('|') 
        ?? p.errorScreen?.playerLegacyDesktopYpcOfferRenderer?.offerDescription
        ?? document.querySelector('#subreason.yt-player-error-message-renderer')?.innerText
    return reason ? {reason, subReason} : null
  }
");
      video.Error = error ?? reason?.reason;
      video.SubError = reason?.subReason;
      if (video.Error.HasValue()) return (new(video, default), default);

      await ScrollDown(page, maxScroll: null, videoId, log);
      (video.Ad, video.HasAd) = await GetAd(page);
      var recs = await GetRecs(page, error.HasValue(), log);
      (video.Comments, video.CommentsMsg, video.CommentCount) = await GetComments(page, video, log);
      return (new(video, recs), default);
    }

    static async Task ScrollDown(Page page, int? maxScroll, string videoId, ILogger log) {
      async Task<int> ScrollHeight() => await page.EvaluateExpressionAsync<int>("document.scrollingElement.scrollHeight");
      Task ScrollPage() => page.EvaluateExpressionAsync(@"document.scrollingElement.scroll(0, document.scrollingElement.scrollTop + window.innerHeight * 0.9)");
      Task ScrollBottom() => page.EvaluateExpressionAsync(@"document.scrollingElement.scroll(0, document.scrollingElement.scrollHeight)");

      var logSw = Stopwatch.StartNew();
      var sw = Stopwatch.StartNew();

      var commentLoop = 0;
      while (true) {
        commentLoop++;
        var scrollTop = await page.EvaluateExpressionAsync<int>("document.scrollingElement.scrollTop");
        if (commentLoop <= 1) { // first time we scroll, wait for the comments section to load before continuing down
          await ScrollPage();
          try {
            log.Debug("ChromeScraper - waiting for comments to load on {Video}", videoId);
            var (_, dur) = await page.WaitForSelectorAsync($"{CommentCountSel}, {CommentErrorSel}").WithDuration();
            log.Debug("ChromeScraper - comments on {Video} found in {Duration}", videoId, dur.HumanizeShort());
          }
          catch (WaitTaskTimeoutException) { }
        }
        else {
          if (maxScroll != null && scrollTop > maxScroll.Value) {
            log.Debug("reached maxed scroll top {Scroll}px on {Video}", videoId, scrollTop);
            break;
          }

          var newContentAttempt = 0;
          var scrollHeight = await ScrollHeight();
          var newScrollHeight = scrollHeight;

          while (newContentAttempt < 4) {
            newContentAttempt++;
            await 600.Milliseconds().Delay();
            await ScrollBottom();
            newScrollHeight = await ScrollHeight();
            if (newScrollHeight > scrollHeight)
              break;
          }

          if (logSw.Elapsed > 30.Seconds()) {
            log.Debug("ChromeScraper - scrolling to {Scroll}px on {Video}", newScrollHeight, videoId);
            logSw.Restart();
          }

          if (newScrollHeight <= scrollHeight) {
            log.Debug("scrolled to bottom {Scroll}px on {Video} in {Duration}",
              newScrollHeight, videoId, sw.Elapsed.HumanizeShort());
            break;
          }

          if (sw.Elapsed > 2.Minutes()) {
            log.Debug("ChromeScraper - stopping scrolling at {Scroll}px on {Video} after {Duration}",
              newScrollHeight, videoId, sw.Elapsed.HumanizeShort());
            break;
          }
        }
      }
    }

    async Task<(VideoCommentStored2[] comments, string msg, long? count)> GetComments(Page page, VideoExtra video, ILogger log) {
      try {
        await page.WaitForSelectorAsync($"{CommentCountSel}, {CommentErrorSel}");
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
        Created = ParseAgo(c.ago).Date()
      }).ToArray();

      if (comments.HasItems())
        return (comments, default, commentCount);

      var msg = await page.EvaluateFunctionAsync<string>(@$"() => {{
  var el = document.querySelector('{CommentErrorSel}')
  return el ? el.innerText : null
}}");

      if (commentCount < 5 || msg.HasValue())
        return (default, msg, commentCount);

      var (url, image) = await SavePageDump(page, log);
      log.Warning("ChromeScraper - can't find comments, or a message about no-comments. Video {Video}. Url ({Url}). Image ({Image})",
        video.VideoId, url, image);
      return (default, default, commentCount);
    }

    async Task<Rec[]> GetRecs(Page page, bool hasError, ILogger log) {
      var recsSel = @"() => {
    var watchNext = document.querySelector('ytd-app')?.__data?.data?.response?.contents?.twoColumnWatchNextResults?.secondaryResults?.secondaryResults?.results
    if(!watchNext) return;    
    return watchNext.map(r => r.compactAutoplayRenderer && Object.assign({}, r.compactAutoplayRenderer.contents[0].compactVideoRenderer, {autoPlay:true}) 
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
            ToUploadDate = ParseAgo(r.publishAgo).Date(),
            ForYou = ParseForYou(r.viewText),
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
        await 2.Seconds().Delay();
        attempts--;
      }
    }

    public static bool ParseForYou(string viewText) => viewText == "Recommended for you";

    async Task WaitForSelector(Page page, string selector, TimeSpan timeout, ILogger log) {
      try {
        await page.WaitForSelectorAsync(selector, new() {Timeout = (int) timeout.TotalMilliseconds});
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

    const string VideoErrorUpcoming = "upcoming";

    async Task<(VideoExtra, string error)> VideoDetails(Page page, string videoId) {
      var details = await page.EvaluateFunctionAsync<VideDetails>(@"() => {
     var v = ytInitialPlayerResponse?.videoDetails || document.querySelector('ytd-app')?.__data?.data?.playerResponse?.videoDetails
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
        viewCount:v.viewCount,
        isLiveContent:v.isLiveContent,
        isUpcoming:v.isUpcoming
    }
}");
      if (details == null) return default;
      if (details.isUpcoming == true) return (default, VideoErrorUpcoming);

      var detailsScript = await page.EvaluateFunctionAsync<VideoDetailsFromScript>(@"() => {
    var el = document.querySelector('script.ytd-player-microformat-renderer')
    if(!el) return null
    return JSON.parse(document.querySelector('script.ytd-player-microformat-renderer').innerText)
}");
      if (detailsScript == null)
        return default;

      var likeDislike = await page.EvaluateExpressionAsync<LikesDislikes>(
        @"Array.from(document.querySelectorAll('#top-level-buttons #text'))
    .map(b => b.getAttribute('aria-label'))
    .filter(b => b)
    .map((l,i) => {
        var m = /^(\d*,?\d*|No) (likes|dislikes)/.exec(l)
        if(!m) {
            console.log('like/dislike label', l)
            return {}
        }
        var key = m.length >= 3 ? m[2] : `position ${i}`
        return {[key] : m[1] == 'No' ? 0 : parseInt(m[1].replace(/,/g, ''))}
    })
.reduce((acc, cur) => ({ ...acc, ...cur }), {})");

      var video = new VideoExtra {
        VideoId = videoId,
        Description = details.shortDescription,
        Duration = details.lengthSeconds.Seconds(),
        Keywords = details.keywords,
        Title = details.title,
        ChannelId = details.channelId,
        ChannelTitle = details.author,
        UploadDate = detailsScript.uploadDate,
        Statistics = new(details.viewCount, likeDislike.likes, likeDislike.dislikes),
        Updated = DateTime.UtcNow,
        Source = ScrapeSource.Chrome
      };

      return (video, default);
    }

    class VideDetails {
      public string   shortDescription { get; set; }
      public string[] keywords         { get; set; }
      public string   channelId        { get; set; }
      public string   author           { get; set; }
      public string   title            { get; set; }
      public ulong    viewCount        { get; set; }
      public int      lengthSeconds    { get; set; }
      public string   thumbnail        { get; set; }
      public bool?    isLiveContent    { get; set; }
      public bool?    isUpcoming       { get; set; }
    }

    class LikesDislikes {
      public ulong? likes    { get; set; }
      public ulong? dislikes { get; set; }
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

    public static long? ParseViews(string s) {
      if (s.NullOrEmpty()) return null;
      var m = Regex.Match(s, "^(\\d+,?\\d*) views");
      if (!m.Success) return null;
      var views = m.Groups[1].Value.ParseLong();
      return views;
    }

    public static (TimeSpan Dur, AgoUnit Unit) ParseAgo(string ago) {
      if (ago == null) return default;
      var res = Regex.Match(ago, "(?<num>\\d)\\s(?<unit>minute|hour|day|week|month|year)[s]? ago");
      if (!res.Success) return default;
      var num = res.Groups["num"].Value.ParseInt();
      var unit = res.Groups["unit"].Value.ParseEnum<AgoUnit>();
      var timeSpan = unit switch {
        Minute => num.Minutes(),
        Hour => num.Hours(),
        Day => num.Days(),
        Week => num.Weeks(),
        Month => TimeSpan.FromDays(365 / 12.0 * num),
        Year => TimeSpan.FromDays(365 * num),
        _ => throw new InvalidOperationException($"unexpected ago unit {res.Groups["unit"].Value}")
      };
      return (timeSpan, unit);
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

  public enum AgoUnit {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year
  }

  public static class PageEx {
    public static Uri Uri(this Page page) => new(page.Url);
    public static Credentials AsCreds(this NameSecret secret) => new() {Username = secret.Name, Password = secret.Secret};
    public static DateTime Date(this (TimeSpan Dur, AgoUnit Unit) ago) => ago.Dur.Before(DateTime.UtcNow);
  }
}