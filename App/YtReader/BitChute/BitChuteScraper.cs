using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using AngleSharp;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using Flurl;
using Flurl.Http;
using Humanizer;
using Polly;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.SimpleCollect;
using YtReader.Store;
using YtReader.Web;
using YtReader.Yt;
using static System.StringComparison;
using static System.Text.RegularExpressions.RegexOptions;
using static SysExtensions.Reflection.ReflectionExtensions;
using static SysExtensions.Threading.Def;
using Url = Flurl.Url;

// ReSharper disable UnusedAutoPropertyAccessor.Local
// ReSharper disable InconsistentNaming

namespace YtReader.BitChute {
  public record BitChuteScraper(FlurlProxyClient FlurlClient, BitChuteCfg Cfg, ProxyCfg ProxyCfg) : IScraper {
    static readonly string            Domain   = "https://www.bitchute.com";
    static readonly IConfiguration    AngleCfg = Configuration.Default.WithDefaultLoader().WithDefaultCookies();
    public          Platform          Platform        => Platform.BitChute;
    public          int               CollectParallel => Cfg.WebParallel;
    public          ICommonCollectCfg CollectCfg      => Cfg;
    public string SourceToFullId(string sourceId, LinkType type) => FullId(sourceId);

    static string FullId(string sourceId) => Platform.BitChute.FullId(sourceId);

    #region IO

    static bool IsBcTransient(HttpStatusCode code) => code.IsTransientError() || code.In(HttpStatusCode.Forbidden);

    /// <summary>Executes the given function with retries and proxy fallback. Returns document in non-transient error states</summary>
    async Task<IDocument> Open(string desc, ILogger log, Url url, Func<IBrowsingContext, Url, Task<IDocument>> getDoc = null) {
      getDoc ??= (b, u) => b.OpenAsync(u);
      var browser = AngleCfg.Browser();
      var retryTransient = Policy.HandleResult<IDocument>(d => {
        if (!IsBcTransient(d.StatusCode)) return false;
        log.Debug($"BcWeb angle transient error '{(int) d.StatusCode}'");
        return true;
      }).RetryWithBackoff("BcWeb angle open", retryCount: 5, log: log);

      var (doc, ex) = await Fun(() => retryTransient.ExecuteAsync(() => getDoc(browser, url))).Try();
      if ((doc?.StatusCode).Do(IsBcTransient) == false) return doc; // if there was a non-transient error, return the doc in that state
      FlurlClient.UseProxyOrThrow(log, desc, url, ex, (int?) doc?.StatusCode); // if we are already using the proxy, throw the error
      doc = await retryTransient.ExecuteAsync(() => getDoc(browser, url));
      doc.EnsureSuccess();
      return doc;
    }

    async Task<T> SubPost<T>(IDocument doc, Url url, ILogger log, object data = null) {
      var csrf = doc.CsrfToken();
      var req = url.WithBcHeaders(doc, csrf);
      return await FlurlClient.Send(typeof(T).Name, req, HttpMethod.Post,
          () => req.FormUrlContent(MergeDynamics(new {csrfmiddlewaretoken = csrf}, data ?? new ExpandoObject())), log: log)
        .ReceiveJson<T>();
    }

    #endregion

    #region Channel

    record ChanCountResponse(ulong subscriber_count, string about_view_count);

    public async Task<(Channel Channel, IAsyncEnumerable<Video[]> Videos)> ChannelAndVideos(string sourceId, ILogger log) {
      var chanUrl = Domain.AppendPathSegment($"channel/{sourceId}");
      var (chan, chanDoc) = await LoadChannel(chanUrl, log);
      var videoList = chanDoc.QuerySelector(".channel-videos-list") ?? throw new("Can't find video tab");
      var videos = Videos(log, chanDoc, videoList, VideoListType.ChannelVideos)
        .Select(b => b.Select(v => v with {ChannelId = chan.ChannelId, ChannelTitle = chan.ChannelTitle}).ToArray());
      return (Channel: chan, Videos: videos);
    }

    async Task<(Channel, IDocument doc)> LoadChannel(string chanUrl, ILogger log, bool loadStats = true) {
      var chanDoc = await Open("channel", log, chanUrl, (b, url) => b.OpenAsync(url));
      if (chanDoc.StatusCode == HttpStatusCode.NotFound) {
        var sourceId = chanUrl.AsUrl().Path.LastInPath();
        return (this.NewChan(sourceId) with {Status = ChannelStatus.NotFound, Updated = DateTime.UtcNow}, chanDoc);
      }
      chanDoc.EnsureSuccess();

      var chan = ParseChannel(chanDoc);
      if (chan.Status != ChannelStatus.Alive || !loadStats)
        return (chan, chanDoc);

      var (subscriberCount, aboutViewCount) = await SubPost<ChanCountResponse>(chanDoc, chanDoc.Url.AppendPathSegments("counts/"), log);
      chan = chan with {
        Subs = subscriberCount,
        ChannelViews = aboutViewCount.TryParseNumberWithUnits()?.RoundToULong()
      };
      return (chan, chanDoc);
    }

    Channel ParseChannel(IDocument doc, string idOrName = null) {
      IElement Qs(string s) => doc.Body.QuerySelector(s);

      var profileA = doc.El<IHtmlAnchorElement>(".channel-banner .details .name > a");
      var id = doc.El<IHtmlLinkElement>("link#canonical")?.Href.AsUri().LocalPath.LastInPath() ?? idOrName;
      var title = doc.QuerySelector(".page-title")?.TextContent;
      var status = title?.ToLowerInvariant() == "blocked content" ? ChannelStatus.Blocked : ChannelStatus.Alive;
      var chan = this.NewChan(id) with {
        ChannelName = id != idOrName ? idOrName : null,
        ChannelTitle = Qs("#channel-title")?.TextContent,
        Description = Qs("#channel-description")?.InnerHtml,
        ProfileId = profileA?.Href.LastInPath(),
        ProfileName = profileA?.TextContent,
        Created = Qs(".channel-about-details > p:first-child")?.TextContent.ParseCreated(),
        LogoUrl = doc.El<IHtmlImageElement>("img[alt=\"Channel Image\"]")?.Dataset["src"],
        Status = status,
        StatusMessage = status == ChannelStatus.Blocked ? doc.QuerySelector("#main-content #page-detail p")?.TextContent : null,
        Updated = DateTime.UtcNow
      };
      return chan;
    }

    #endregion

    #region VideoLists

    enum VideoListType {
      Cards,
      Results,
      ChannelVideos
    }

    public async IAsyncEnumerable<Video[]> HomeVideos(ILogger log, [EnumeratorCancellation] CancellationToken cancel) {
      var categories = new[] {"vlogging", "health", "news", "science", "spirituality", "family", "education"};
      var videos = new[] {(category: "", path: "")}.Concat(categories.Select(c => (category: c, path: $"category/{c}")))
        .BlockMap(async c => {
          var (category, path) = c;
          var doc = await Open($"page list {path}", log, Domain.AppendPathSegment(path), (b, url) => b.OpenAsync(url));
          doc.EnsureSuccess();
          return doc.QuerySelectorAll("#listing-tabs .tab-pane").Select(tab => {
            var tabName = tab.Id?.Split("-").LastOrDefault();
            if (tabName is null || tabName.In("subscribed", "week", "month", "day")) return default;
            var listType = tabName.In("trending") ? VideoListType.Results : VideoListType.Cards;
            return (doc, category, path, tab, listType, tabName);
          });
        }, Cfg.WebParallel, cancel: cancel).SelectMany().NotNull().BlockMap(async t => {
          var pageVids = await Videos(log, t.doc, t.tab, t.listType, t.tabName).NotNull().Select((b, i) => {
            log.Information("Collect {Platform} - crawled {Videos} videos on page {Page} from {Path} > {Tab} videos",
              Platform, b.Length, i + 1, t.path.NullOrEmpty() ? "Home" : t.path, t.tabName);
            return b.Select(v => {
              MultiValueDictionary<string, string> tags = v.Tags == null ? new() : new(v.Tags);
              if (t.path.HasValue()) tags.Add("category", t.category);
              tags.Add("source-tab", t.tabName);
              return v with {Tags = tags};
            });
          }).SelectMany().ToArrayAsync();
          return pageVids;
        }, Cfg.WebParallel, cancel: cancel);

      await foreach (var vids in videos)
        yield return vids;
    }

    record BcResponse(bool success);
    record ExtendResponse(string html, bool success) : BcResponse(success);

    async IAsyncEnumerable<Video[]> Videos(ILogger log, IDocument doc, IElement tab, VideoListType type, string tabName = null) {
      var selector = type switch {
        VideoListType.Cards => ".video-card",
        VideoListType.Results => ".video-result-container",
        VideoListType.ChannelVideos => ".channel-videos-container",
        _ => throw new($"type {type} not implemented")
      };
      var vids = tab.QuerySelectorAll(selector).Select(e => ParseVid(e, type)).ToArray();
      yield return vids;
      var offset = vids.Length;
      var bc = AngleCfg.Browser();
      while (true) {
        var extendUrl = doc.Url.AsUrl().WithPathAdded("extend/");
        var (res, ex) = await SubPost<ExtendResponse>(doc, extendUrl, log,
          new {offset, name = tabName, last = vids.LastOrDefault()?.SourceId}).Try();
        if (res?.success == false || ex != null) yield break;
        var extendDoc = await bc.OpenAsync(req => req.Content(res.html)); //  use anglesharp for parsing, not requesting
        vids = extendDoc.QuerySelectorAll(selector).Select(e => ParseVid(e, type)).ToArray();
        if (vids.Length <= 0) break;
        offset += vids.Length;
        yield return vids;
      }
    }

    static Video NewVid(string videoId) => new() {
      Platform = Platform.BitChute,
      VideoId = Platform.BitChute.FullId(videoId),
      SourceId = videoId,
      Updated = DateTime.UtcNow
    };

    static Video ParseVid(IElement e, VideoListType type) => type switch {
      VideoListType.ChannelVideos => ParseChanVid(e),
      VideoListType.Cards => ParseCardVid(e),
      VideoListType.Results => ParseResultVid(e),
      _ => throw new($"type {type} not implemented")
    };

    static Video ParseChanVid(IElement e) {
      var videoA = e.El<IHtmlAnchorElement>(".channel-videos-title .spa");
      var videoId = videoA?.Href.LastInPath();
      return NewVid(videoId) with {
        Title = videoA?.Text,
        UploadDate = e.QuerySelectorAll(".channel-videos-details.text-right")
          .Select(s => s.TextContent.Trim().TryParseDateExact("MMM dd, yyyy"))
          .NotNull().FirstOrDefault(),
        Description = e.El(".channel-videos-text")?.InnerHtml,
        Duration = ParseVidDuration(e),
        Statistics = ParseVidStats(e),
        Thumb = e.El<IHtmlImageElement>("img[alt=\"video image\"]")?.Dataset["src"]
      };
    }

    static Statistics ParseVidStats(IElement e) => new(e.El(".video-views")?.TextContent.Trim().TryParseNumberWithUnits()?.RoundToULong());

    static Video ParseCardVid(IElement e) {
      var videoId = ParseVidCardId(e);
      var channelP = e.El(".video-card-channel");
      var channelSourceId = channelP?.El<IHtmlAnchorElement>("a").Do(ParseIdFromA);
      var vid = NewVid(videoId) with {
        Title = e.El(".video-card-title")?.TextContent,
        UploadDate = e.El(".video-card-published")?.TextContent?.ParseAgo().Date(),
        Duration = ParseVidDuration(e),
        Statistics = ParseVidStats(e),
        Thumb = ParseVidThumb(e),
        ChannelSourceId = channelSourceId,
        ChannelId = Platform.BitChute.FullId(channelSourceId),
        ChannelTitle = channelP?.TextContent
      };
      return vid;
    }

    static Video ParseResultVid(IElement e) {
      var titleDiv = e.El(".video-result-title");
      var videoId = titleDiv?.Children.OfType<IHtmlAnchorElement>().FirstOrDefault().Do(ParseIdFromA);
      var channelA = e.El<IHtmlAnchorElement>(".video-result-channel a");
      var channelSourceId = channelA.Do(ParseIdFromA);
      var vid = NewVid(videoId) with {
        Title = titleDiv?.TextContent,
        UploadDate = e.El(".video-result-details.text-left")?.TextContent?.ParseAgo().Date(),
        Duration = ParseVidDuration(e),
        Statistics = ParseVidStats(e),
        Thumb = ParseVidThumb(e),
        ChannelSourceId = channelSourceId,
        ChannelId = Platform.BitChute.FullId(channelSourceId),
        ChannelTitle = channelA?.Text,
        Description = e.El(".video-result-text")?.InnerHtml
      };
      return vid;
    }

    static string ParseIdFromA(IHtmlAnchorElement e) => e.Href.AsUri().LocalPath.LastInPath();
    static string ParseVidThumb(IElement e) => e.El<IHtmlImageElement>("img[alt=\"video image\"]")?.Dataset["src"];
    static string ParseVidCardId(IElement e) => e.El(".video-card-id")?.TextContent.Trim();
    static TimeSpan? ParseVidDuration(IElement e) => e.El(".video-duration")?.TextContent.TryParseTimeSpan();

    #endregion

    #region Video Page & Comments

    //static readonly Regex JProp = new(@"""(?<prop>[\w]*)""\s*:\s*(?:(?:""(?<string>(?:\\""|[^\""])*)"")|(?<num>[\d\.]+))", Compiled);
    static readonly Regex RDate = new(@"(?<time>\d{2}:\d{2}) UTC on (?<month>\w*)\s+(?<day>\d+)\w*\, (?<year>\d*)", Compiled);
    record VidCountResponse(bool success, ulong view_count, ulong like_count, ulong dislike_count, ulong subscriber_count);

    public async Task<(VideoExtra Video, VideoComment[] Comments)> VideoAndExtra(string sourceId, ExtraPart[] parts, ILogger log, Channel channel = null) {
      var videoUrl = Domain.AppendPathSegment($"video/{sourceId}/");
      var doc = await Open("video", log, videoUrl, (b, url) => b.OpenAsync(url));

      var vid = this.NewVidExtra(sourceId);

      if (doc.StatusCode == HttpStatusCode.NotFound)
        return (vid with {Status = VideoStatus.NotFound}, null);
      doc.EnsureSuccess();
      var counts = await SubPost<VidCountResponse>(doc, videoUrl.WithPathAdded("counts/"), log);

      var chanA = doc.El<IHtmlAnchorElement>(".channel-banner .details .name > a.spa");
      var dateMatch = doc.QuerySelector(".video-publish-date")?.TextContent?.Match(RDate);
      string G(string group) => dateMatch?.Groups[group].Value;
      var dateString = $"{G("year")}-{G("month")}-{G("day")} {G("time")}";
      var created = dateString.TryParseDateExact("yyyy-MMMM-d HH:mm", DateTimeStyles.AssumeUniversal);

      var videoPs = doc.QuerySelectorAll("#video-watch p, #page-detail p");
      var title = doc.QuerySelector("h1.page-title")?.TextContent;
      var mediaUrl = doc.QuerySelector("video#player > source")?.GetAttribute("src");

      var tags = doc.Els("table.video-detail-list tr").Select(tr => {
        var (nameCol, valueCol, _) = tr.Children.OfType<IHtmlTableDataCellElement>().ToArray();
        var name = nameCol.TextContent;
        var val = name switch {
          "Category" => valueCol.El<IHtmlAnchorElement>("a").Href?.Split("/").LastOrDefault(s => s.HasValue()),
          "Sensitivity" => valueCol.TextContent.Trim(),
          _ => null
        };
        return (name, val);
      }).ToMultiValueDictionary(d => d.name.ToLower(), d => d.val);

      // its a bit soft to tell if there is an error. if we can't find the channel, and there isn't much text content, then assume it is.
      string error = null;
      VideoStatus? status = null;
      if (chanA == null) {
        // restricted example https://www.bitchute.com/video/AFIyaKIYgI6P/
        if (title?.Contains("RESTRICTED", OrdinalIgnoreCase) == true) {
          error = title;
          status = VideoStatus.Restricted;
        }
        else if (videoPs.Length.Between(from: 1, to: 3)) {
          // blocked example https://www.bitchute.com/video/memlIDAzcSQq/
          error = videoPs.First().TextContent;
          status = error?.Contains("blocked") == true ? VideoStatus.Removed : null;
        }
      }

      vid = vid with {
        Title = title ?? doc.Title,
        Thumb = doc.El<IHtmlMetaElement>("meta[name=\"twitter:image:src\"]")?.Content,
        Statistics = new(counts.view_count, counts.like_count, counts.dislike_count),
        MediaUrl = mediaUrl,
        ChannelTitle = chanA?.TextContent,
        ChannelId = channel?.ChannelId,
        ChannelSourceId = channel?.SourceId,
        UploadDate = created,
        Description = doc.QuerySelector("#video-description .full")?.InnerHtml,
        Keywords = doc.QuerySelectorAll<IHtmlAnchorElement>("#video-hashtags ul li a.spa").Select(a => a.Href?.LastInPath()).NotNull().ToArray(),
        Duration = doc.QuerySelector(".video-duration")?.TextContent.TryParseTimeSpanExact(@"h\:m\:s", @"m\:s"),
        Error = error,
        Status = status,
        Tags = tags
      };

      if (vid.ChannelId == null && chanA?.Href != null) {
        // bitchute doesn't use the channel's the canon url in links from videos, so we need to look up the real id.
        var (chan, _) = await LoadChannel(chanA.Href, log, loadStats: false);
        vid = vid with {
          ChannelId = chan.ChannelId,
          ChannelSourceId = chan.SourceId
        };
      }

      var comments = parts.ShouldRun(ExtraPart.EComment)
        ? await Comments(doc, log).Then(c => c.Select(s => s with {VideoId = vid.VideoId}).ToArray())
        : Array.Empty<VideoComment>();
      return (vid, comments);
    }

    static readonly Regex CommentAuthRe = new(@"initComments\(\s*'.*?'\s*,\s*'(?<auth>.*?)'");

    Task<T> CommentPost<T>(IDocument doc, string path, ILogger log, params (string name, string value)[] data) {
      var auth = doc.Scripts.Select(s => s.Text?.Match(CommentAuthRe))
        .Where(m => m?.Success == true).Select(m => m.Groups["auth"].Value).FirstOrDefault();
      var req = $"https://commentfreely.bitchute.com/api/{path}/".WithHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
      //var content = req.FormUrlContent(MergeDynamics(new {crf_auth = auth}, data ?? new ExpandoObject()));
      var formData = new[] {(name: "cf_auth", value: auth)}.Concat(data.NotNull());
      var content = formData.Join("&", d => $"{d.name}={d.value}");
      return FlurlClient.Send(typeof(T).Name, req, HttpMethod.Post, () => new StringContent(content, Encoding.UTF8), log: log, logRequests: true)
        .ReceiveJson<T>();
    }

    record BitChuteComment {
      public string    id                  { get; init; }
      public string    parent              { get; init; }
      public DateTime  created             { get; init; }
      public DateTime? modified            { get; init; }
      public string    content             { get; init; }
      public string    creator             { get; init; }
      public string    fullname            { get; init; }
      public int?      upvote_count        { get; init; }
      public string    profile_picture_url { get; init; }
    }

    public async Task<VideoComment[]> Comments(IDocument doc, ILogger log) {
      var comments = await CommentPost<BitChuteComment[]>(doc, "get_comments", log, ("commentCount", "1000"));
      return comments.Select(c => new VideoComment {
        CommentId = c.id,
        ReplyToCommentId = c.parent,
        Created = c.created,
        Modified = c.modified,
        Comment = c.content,
        Author = c.fullname,
        AuthorId = c.creator,
        Likes = c.upvote_count,
        AuthorThumb = c.profile_picture_url.AsUrl().IsRelative ? Domain.AppendPathSegment(c.profile_picture_url) : c.profile_picture_url,
        Updated = DateTime.Now,
        Platform = Platform.BitChute
      }).ToArray();
    }

    #endregion
  }

  public static class BcExtensions {
    static readonly Regex CreatedRe = new(@"(?<num>\d+)\s(?<unit>day|week|month|year)[s]?", Compiled | IgnoreCase);

    public static async Task<IFlurlResponse> BcPost(this IFlurlRequest req, string csrf, object formValues = null) =>
      await req.AllowAnyHttpStatus()
        .PostUrlEncodedAsync(MergeDynamics(new {csrfmiddlewaretoken = csrf}, formValues ?? new ExpandoObject()));

    public static IFlurlRequest WithBcHeaders(this Url url, IDocument originating, string csrf = null) => url
      .WithCookie("csrftoken", csrf ?? originating.CsrfToken())
      //.WithCookies(originating.GetCookies().Select(c => (c.Name, c.Value)))
      .WithHeader("referer", originating.Url);

    public static string CsrfToken(this IDocument originating) => originating.GetCookie("csrftoken").Value;

    public static IEnumerable<Cookie> GetCookies(this IDocument doc) {
      var cc = new CookieContainer();
      var url = doc.Url.AsUri();
      var domain = $"{url.Scheme}://{url.Host}".AsUri();
      cc.SetCookies(domain, doc.Cookie);
      return cc.GetCookies(domain);
    }

    public static Cookie GetCookie(this IDocument doc, string name) => doc.GetCookies().FirstOrDefault(c => c.Name == name);

    public static DateTime? ParseCreated(this string s) {
      if (s == null) return null;
      var ago = DateTime.UtcNow;
      var matches = CreatedRe.Matches(s);
      if (matches.Count == 0) return null;
      foreach (Match m in matches) {
        var num = m.Groups["num"].Value.ParseInt();
        ago = m.Groups["unit"].Value switch {
          "day" => ago - num.Days(),
          "week" => ago - num.Weeks(),
          "month" => ago.AddMonths(-num),
          "year" => ago.AddYears(-num),
          _ => throw new InvalidOperationException($"unexpected ago unit {m.Groups["unit"].Value}")
        };
      }
      return ago;
    }
  }
}