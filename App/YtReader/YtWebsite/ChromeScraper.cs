using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Humanizer;
using PuppeteerSharp;
using Serilog;
using Serilog.Extensions.Logging;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;

namespace YtReader.YtWebsite {
  public class ChromeScraper {
    readonly ScraperCfg Cfg;
    public ChromeScraper(ScraperCfg cfg) => Cfg = cfg;
    public async Task Init() => await new BrowserFetcher().DownloadAsync(BrowserFetcher.DefaultRevision);

    static readonly ILogger ConsoleLog = new LoggerConfiguration().WriteTo.Console().CreateLogger();

    static async Task<Browser> CreateBrowser(ILogger log) {
      var browser = await Puppeteer.LaunchAsync(new LaunchOptions {
        Headless = true,
        DefaultViewport = new ViewPortOptions {Width = 1024, Height = 1200},
        Args = new[] {"--disable-dev-shm-usage", "--no-sandbox"}
      }, new SerilogLoggerFactory(ConsoleLog)); // log to console only because it often has transient errors
      return browser;
    }

    public async Task<IReadOnlyCollection<RecsAndExtra>> GetRecsAndExtra(IReadOnlyCollection<string> videos, ILogger log) {
      var sw = Stopwatch.StartNew();
      log = log.ForContext("Module", nameof(ChromeScraper));

      // run 4 browsers that navigate one video at a time
      var browsers = 4;
      var res = await videos.Batch(videos.Count / 4).BlockFunc(async b => {
        var requests = new ConcurrentBag<(Request req, bool aborted)>();
        using var browser = await CreateBrowser(log);
        return await b.BlockFunc(async v => {
          //var context = await browser.CreateIncognitoBrowserContextAsync(); // opening windows is more reliable than tabs in practice
          log.Debug("loading video {Video}", v);

          using var page = await browser.NewPageAsync();
          await page.SetCookieAsync(); // clears cookies
          await ConfigureRequests(log, page, v, (req, aborted) => requests.Add((req, aborted)));
          var video = await GetVideo(page, v, log);

          /*var stats = requests.Where(r => !r.aborted)
            .Select(r => new {Uri = new Uri(r.req.Url), r.req.Response})
            .Select(r => $"{r.Uri.Host}{r.Uri.AbsolutePath}")
            .ToArray();*/

          log.Debug("finished loading video {Video} with {Comments} comments and {Recommendations} recs in {Duration}",
            v, video.Extra.Comments?.Length ?? 0, video.Recs?.Length ?? 0, sw.Elapsed.HumanizeShort());
          return video;
        });
      }, browsers);

      log.Information("loaded {Videos} in {Duration}", videos.Count, sw.HumanizeShort());
      return res.SelectMany().ToReadOnly();
    }

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

    const string CommentSel      = "ytd-comment-renderer#comment";
    const string CommentErrorSel = "#comments #message";
    const string VideoErrorSel   = "#info > .reason.yt-player-error-message-renderer";

    async Task<RecsAndExtra> GetVideo(Page page, string videoId, ILogger log) {
      log = log.ForContext("Video", videoId);
      await page.GoToAsync($"https://youtube.com/watch?v={videoId}");

      // wait for either a single comment, or a message that the coments are turned off. This is the slowest part to load.
      await page.WaitForSelectorAsync(new[] {CommentSel, CommentErrorSel, VideoErrorSel}.Join(", "), new WaitForSelectorOptions {Timeout = 60000});

      var video = await VideoDetails(page, videoId);

      // missing video return the error
      if (video == null) {
        var error = await page.EvaluateFunctionAsync<string>(@$"() => {{
  var el = document.querySelector('{VideoErrorSel}')
  return el ? el.innerText : null
}}");
        if (error == null) throw new InvalidOperationException("can't fin video or error message, probably a bug");
        var subError = await page.EvaluateFunctionAsync<string>(@"() => {
  var el = document.querySelector('#info > .subreason.yt-player-error-message-renderer')
  return el ? el.innerText : null
}");
        return new RecsAndExtra(new VideoExtraStored2 {
          VideoId = videoId,
          Error = error,
          SubError = subError
        }, default);
      }

      await ScrollDown(page, maxScroll: 8000, videoId, log);
      (video.Ad, video.HasAd) = await GetAd(page);
      var recs = await GetRecs(page);
      (video.Comments, video.CommentsMsg) = await GetComments(page, video);
      return new RecsAndExtra(video, recs);
    }

    static async Task<(VideoCommentStored2[] comments, string msg)> GetComments(Page page, VideoExtraStored2 video) {
      var videoComments = await page.EvaluateExpressionAsync<VideoComment[]>(
        $@"Array.from(document.querySelectorAll('{CommentSel}')).map(c => ({{ 
    author: c.querySelector('a#author-text').innerText, 
    authorChannelId: /\/channel\/(.*)/.exec(c.querySelector('a#author-text').href)[1],
    comment: c.querySelector('#content-text').innerText,
    ago: c.querySelector('yt-formatted-string.published-time-text').innerText
 }}))");
      var comments = videoComments.Select(c => new VideoCommentStored2 {
        Author = c.author,
        AuthorChannelId = c.authorChannelId,
        Comment = c.comment,
        ChannelId = video.ChannelId,
        VideoId = video.VideoId,
        Created = ParseAgo(DateTime.UtcNow, c.ago)
      }).ToArray();

      if (comments.HasItems())
        return (comments, default);

      var msg = await page.EvaluateFunctionAsync<string>(@$"() => {{
  var el = document.querySelector('{CommentErrorSel}')
  return el ? el.innerText : null
}}");

      if (msg.HasValue())
        return (default, msg);

      throw new InvalidOperationException("can't find comments, or a message about no-comments. Probably a bug");
    }

    static async Task<Rec[]> GetRecs(Page page) {
      var recsSel = @"ytInitialData.contents.twoColumnWatchNextResults.secondaryResults.secondaryResults.results
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
}))";
      var recs = await page.EvaluateExpressionAsync<CompactVideoRenderer[]>(recsSel);

      var recRes = recs.Select(r => new Rec {
        Source = ScrapeSource.Chrome,
        ToVideoId = r.videoId,
        ToChannelId = r.channelId,
        ToChannelTitle = r.channelTitle,
        Rank = r.rank,
        ToVideoTitle = r.title,
        ToViews = ParseViews(r.viewText),
        ToUploadDate = ParseAgo(DateTime.UtcNow, r.publishAgo),
        ForYou = r.viewText == "Recommended for you"
      }).ToArray();

      return recRes;
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

    static async Task ScrollDown(Page page, int maxScroll, string videoId, ILogger log) {
      var scrollHeight = 0;
      while (true) {
        var newScrollHeight = await page.EvaluateExpressionAsync<int>("document.scrollingElement.scrollHeight");
        if (newScrollHeight == scrollHeight || newScrollHeight > maxScroll) {
          log.Debug("scrolled down on {Video} to {ScrollHeight}px", videoId, newScrollHeight);
          break;
        }
        await page.EvaluateExpressionAsync(@"document.scrollingElement.scroll(0, document.scrollingElement.scrollHeight)");
        scrollHeight = newScrollHeight;
        await 1.Seconds().Delay();
      }
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
        @"Object.assign({}, ...ytInitialData.contents.twoColumnWatchNextResults.results.results.contents[0].videoPrimaryInfoRenderer.videoActions.menuRenderer.topLevelButtons
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
}