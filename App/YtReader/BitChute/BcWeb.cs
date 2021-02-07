using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
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
using YtReader.Store;
using static System.Text.RegularExpressions.RegexOptions;
using static SysExtensions.Net.HttpExtensions;
using static SysExtensions.Reflection.ReflectionExtensions;
using static SysExtensions.Threading.Def;
using Url = Flurl.Url;

// ReSharper disable InconsistentNaming

namespace YtReader.BitChute {
  record FlurlClients(FlurlClient Direct, FlurlClient Proxy);

  public class BcWeb {
    readonly        ProxyCfg       Proxy;
    static readonly string         Url      = "https://www.bitchute.com";
    static readonly IConfiguration AngleCfg = Configuration.Default.WithDefaultLoader().WithDefaultCookies();
    bool                           UseProxy;
    readonly FlurlClients          FlurlClients;

    public BcWeb(ProxyCfg proxy) {
      Proxy = proxy;
      FlurlClients = new(new(), new(proxy.Proxies.FirstOrDefault()?.CreateHttpClient()));
    }

    public async Task<(Channel channel, IAsyncEnumerable<VideoStored2[]> videos)> ChannelAndVideos(string idOrName, ILogger log) {
      var chanDoc = await Open(Url.AppendPathSegment($"channel/{idOrName}"), (b, url) => b.OpenAsync(url), log);
      if (chanDoc.StatusCode == HttpStatusCode.NotFound)
        return (new(Platform.BitChute, idOrName) {Status = ChannelStatus.NotFound, Updated = DateTime.UtcNow}, null);
      chanDoc.StatusCode.EnsureSuccess();
      var csrf = chanDoc.CsrfToken();

      Task<T> Post<T>(string path, object data = null) =>
        FurlAsync(Url.AppendPathSegment(path).WithBcHeaders(chanDoc, csrf), r => r.BcPost(csrf, data)).Then(r => r.ReceiveJson<T>());

      var chan = ParseChannel(chanDoc, idOrName);
      if (chan.Status != ChannelStatus.Alive)
        return (chan, null);

      var (subscriberCount, aboutViewCount) = await Post<CountResponse>($"channel/{chan.SourceId}/counts/");
      chan = chan with {
        Subs = subscriberCount,
        ChannelViews = aboutViewCount.TryParseNumberWithUnits()?.RoundToULong()
      };

      async IAsyncEnumerable<VideoStored2[]> Videos() {
        var chanVids = GetChanVids(chanDoc, chan, log);
        yield return chanVids;
        var offset = chanVids.Length;
        while (true) {
          var (html, success) = await Post<ExtendResponse>($"channel/{chan.SourceId}/extend/", new {offset});
          if (!success) break;
          var extendDoc = await GetBrowser().OpenAsync(req => req.Content(html));
          var videos = GetChanVids(extendDoc, chan, log);
          if (videos.Length <= 0) break;
          offset += videos.Length;
          yield return videos;
        }
      }

      return (chan, videos: Videos());
    }

    static readonly Regex JProp = new(@"""(?<prop>[\w]*)""\s*:\s*(?:(?:""(?<string>(?:\\""|[^\""])*)"")|(?<num>[\d\.]+))", Compiled);

    public async Task<VideoStored2> Video(string sourceId, ILogger log) {
      var doc = await Open(Url.AppendPathSegment($"video/{sourceId}/"), (b, url) => b.OpenAsync(url), log);
      var vid = new VideoStored2 {
        Platform = Platform.BitChute,
        SourceId = sourceId,
        VideoId = Platform.BitChute.FullId(sourceId),
        Updated = DateTime.UtcNow,
      };

      if (doc.StatusCode == HttpStatusCode.NotFound)
        return vid with {Status = VideoStatus.NotFound};
      doc.StatusCode.EnsureSuccess();

      var chanA = doc.Qs<IHtmlAnchorElement>(".channel-banner .details .name > a.spa");
      var jProps = doc.QuerySelectorAll("body script").SelectMany(s => JProp.Matches(s.TextContent)
          .Select(m => new {Prop = m.Groups["prop"].Value, Value = m.Groups["string"].Success ? m.Groups["string"].Value : m.Groups["num"].Value}))
        .ToKeyedCollection(m => m.Prop);

      ulong? GetStat(string selector) => doc.QuerySelector(selector)?.TextContent?.TryParseNumberWithUnits()?.RoundToULong();

      return vid with {
        Title = doc.Title,
        Thumb = doc.Qs<IHtmlMetaElement>("meta[name=\"twitter:image:src\"]")?.Content,
        Statistics = new(
          viewCount: GetStat(".video-view-count"),
          likeCount: GetStat("#video-like-count"),
          dislikeCount: GetStat("#video-dislike-count")
        ),
        ChannelTitle = chanA?.TextContent,
        ChannelId = chanA?.Href?.LastInPath(),
        UploadDate = jProps["pubDate"]?.Value.TryParseDate(style: DateTimeStyles.AdjustToUniversal),
        Description = doc.QuerySelector("#video-description .full")?.InnerHtml,
        Duration = jProps["duration"]?.Value.TryParseInt(NumberStyles.Integer)?.Seconds(),
      };
    }

    IBrowsingContext GetBrowser() => BrowsingContext.New(UseProxy && Proxy.Proxies.Any()
      ? AngleCfg.WithRequesters(new() {
        Proxy = Proxy.Proxies.First().CreateWebProxy(),
        PreAuthenticate = true,
        UseDefaultCredentials = false
      })
      : AngleCfg);

    /// <summary>Executes the given function with retries and proxy fallback. Returns documnet in non-transient error states</summary>
    async Task<IDocument> Open(Url url, Func<IBrowsingContext, Url, Task<IDocument>> getDoc, ILogger log) {
      var browser = GetBrowser();
      var retryTransient = Policy.HandleResult<IDocument>(d => {
        if (!d.StatusCode.IsTransient()) return false;
        log.Debug($"BcWeb angle transient error '{(int) d.StatusCode}'");
        return true;
      }).RetryWithBackoff("BcWeb angle open", 5, d => d.StatusCode.ToString(), log);

      var (doc, ex) = await F(() => retryTransient.ExecuteAsync(() => getDoc(browser, url))).Try();
      if (doc?.StatusCode.IsTransient() == false) return doc; // if there was a non-transient error, return the doc in that state 
      UseProxyOrThrow(ex, url, (int?) doc?.StatusCode); // if we are already using the proxy, throw the error
      doc = await retryTransient.ExecuteAsync(() => getDoc(browser, url));
      doc.StatusCode.EnsureSuccess();
      return doc;
    }

    /// <summary>Posts to Bitchute and retries and proxy fallback. Always ensure successful results</summary>
    public async Task<IFlurlResponse> FurlAsync(IFlurlRequest request, Func<IFlurlRequest, Task<IFlurlResponse>> getResponse, ILogger log = null) {
      Task<IFlurlResponse> GetRes() => getResponse(request.WithClient(UseProxy ? FlurlClients.Proxy : FlurlClients.Direct).AllowAnyHttpStatus());

      var retry = Policy.HandleResult<IFlurlResponse>(d => IsTransient(d.StatusCode))
        .RetryWithBackoff("BcWeb flurl transient error", 5, d => d.StatusCode.ToString(), log);
      var (res, ex) = await F(() => retry.ExecuteAsync(GetRes)).Try();
      if (res != null && IsSuccess(res.StatusCode)) return res;
      UseProxyOrThrow(ex, request.Url, res?.StatusCode);
      res = await retry.ExecuteAsync(GetRes);
      EnsureSuccess(res.StatusCode, request.Url.ToString());
      return res;
    }

    void UseProxyOrThrow(Exception ex, string url, int? statusCode) {
      if (statusCode != null && !IsTransient(statusCode.Value))
        EnsureSuccess(statusCode.Value, url); // throw for non-transient errors

      if (UseProxy) { // throw if there is an error and we are allready using proxy
        if (ex != null) throw ex;
        if (statusCode != null) EnsureSuccess(statusCode.Value, url);
      }

      UseProxy = true;
    }

    static Channel ParseChannel(IDocument doc, string idOrName) {
      IElement Qs(string s) => doc.Body.QuerySelector(s);

      var profileA = doc.Qs<IHtmlAnchorElement>(".channel-banner .details .name > a");
      var id = doc.Qs<IHtmlLinkElement>("link#canonical")?.Href.AsUri().LocalPath.LastInPath() ?? idOrName;
      var title = doc.QuerySelector(".page-title")?.TextContent;
      var status = title?.ToLowerInvariant() == "blocked content" ? ChannelStatus.Blocked : ChannelStatus.Alive;
      var chan = BcCollect.NewChan(id) with {
        ChannelName = id != idOrName ? idOrName : null,
        ChannelTitle = Qs("#channel-title")?.TextContent,
        Description = Qs("#channel-description")?.InnerHtml,
        ProfileId = profileA?.Href.LastInPath(),
        ProfileName = profileA?.TextContent,
        Created = Qs(".channel-about-details > p:first-child")?.TextContent.ParseCreated(),
        LogoUrl = doc.Qs<IHtmlImageElement>("img[alt=\"Channel Image\"]")?.Dataset["src"],
        Status = status,
        StatusMessage = status == ChannelStatus.Blocked ? doc.QuerySelector("#main-content #page-detail p")?.TextContent : null,
        Updated = DateTime.UtcNow
      };
      return chan;
    }

    static VideoStored2[] GetChanVids(IDocument doc, Channel c, ILogger log) {
      var videos = doc.Body.QuerySelectorAll(".channel-videos-container")
        .Select(e => ParseChanVid(e) with {ChannelId = c.ChannelId, ChannelTitle = c.ChannelTitle}).ToArray();
      log.Debug("BcWeb - {Channel}: loaded {Videos} videos", videos.Length, c.ChannelTitle);
      return videos;
    }

    static VideoStored2 ParseChanVid(IElement c) {
      IElement Qs(string s) => c.QuerySelector(s);

      var videoA = c.Qs<IHtmlAnchorElement>(".channel-videos-title .spa");
      var videoId = videoA?.Href.LastInPath();
      return new() {
        Platform = Platform.BitChute,
        VideoId = Platform.BitChute.FullId(videoId),
        SourceId = videoId,
        Title = videoA?.Text,
        UploadDate = c.QuerySelectorAll(".channel-videos-details.text-right")
          .Select(s => s.TextContent.Trim().TryParseDateExact("MMM dd, yyyy"))
          .NotNull().FirstOrDefault(),
        Description = Qs(".channel-videos-text")?.InnerHtml,
        Duration = Qs(".video-duration")?.TextContent.TryParseTimeSpan(),
        Statistics = new(Qs(".video-views")?.TextContent.Trim().TryParseNumberWithUnits()?.RoundToULong()),
        Thumb = c.Qs<IHtmlImageElement>("img[alt=\"video image\"]")?.Dataset["src"],
        Updated = DateTime.UtcNow
      };
    }
  }

  record BcResponse(bool success);

  record ExtendResponse(string html, bool success) : BcResponse(success);

  record CountResponse(ulong subscriber_count, string about_view_count);

  public static class BcExtensions {
    public static async Task<IFlurlResponse> BcPost(this IFlurlRequest req, string csrf, object formValues = null) =>
      await req.AllowAnyHttpStatus()
        .PostUrlEncodedAsync(MergeDynamics(new {csrfmiddlewaretoken = csrf}, formValues ?? new ExpandoObject()));

    public static IFlurlRequest WithBcHeaders(this Url url, IDocument originating, string csrf = null) => url
      .WithCookie("csrftoken", csrf ?? originating.CsrfToken())
      .WithHeader("referer", originating.Url);

    public static string CsrfToken(this IDocument originating) => originating.GetCookie("csrftoken").Value;

    public static Cookie GetCookie(this IDocument document, string name) {
      var cc = new CookieContainer();
      var url = document.Url.AsUri();
      var domain = $"{url.Scheme}://{url.Host}".AsUri();
      cc.SetCookies(domain, document.Cookie);
      return cc.GetCookies(domain).FirstOrDefault(c => c.Name == name);
    }

    public static string LastInPath(this string path) => path?.Split('/').LastOrDefault(t => !t.Trim().NullOrEmpty());

    static readonly Regex CreatedRe = new(@"(?<num>\d+)\s(?<unit>day|week|month|year)[s]?", Compiled | IgnoreCase);

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