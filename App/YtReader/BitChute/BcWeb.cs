﻿using System;
using System.Collections.Generic;
using System.Dynamic;
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
      var (subscriberCount, aboutViewCount) = await Post<CountResponse>($"channel/{chan.SourceId}/counts/");
      chan = chan with {
        Subs = subscriberCount,
        ChannelViews = aboutViewCount.ParseBcNumber()
      };

      async IAsyncEnumerable<VideoStored2[]> Videos() {
        var chanVids = GetVideos(chanDoc, chan, log);
        yield return chanVids;

        var offset = chanVids.Length;
        while (true) {
          var (html, success) = await Post<ExtendResponse>($"channel/{chan.SourceId}/extend/", new {offset});
          if (!success) break;
          var extendDoc = await GetBrowser().OpenAsync(req => req.Content(html));
          var videos = GetVideos(extendDoc, chan, log);
          if (videos.Length <= 0) break;
          offset += videos.Length;
          yield return videos;
        }
      }

      return (chan, videos: Videos());
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
      UseProxyOrThrow(ex, url, (int?)doc?.StatusCode); // if we are already using the proxy, throw the error
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
        if(ex != null) throw ex;
        if(statusCode != null) EnsureSuccess(statusCode.Value, url);
      }
      
      UseProxy = true;
    }

    static Channel ParseChannel(IDocument doc, string idOrName) {
      IElement Qs(string s) => doc.Body.QuerySelector(s);

      var profileA = doc.QuerySelector<IHtmlAnchorElement>(".channel-banner .details .name > a");
      var id = doc.QuerySelector<IHtmlLinkElement>("link#canonical")?.Href.AsUri().LocalPath.LastInPath() ?? idOrName;
      return new(Platform.BitChute, id) {
        ChannelName = id != idOrName ? idOrName : null,
        ChannelTitle = Qs("#channel-title")?.TextContent,
        Description = Qs("#channel-description")?.InnerHtml,
        ProfileId = profileA?.Href.LastInPath(),
        ProfileName = profileA?.TextContent,
        Created = Qs(".channel-about-details > p:first-child")?.TextContent.ParseCreated(),
        LogoUrl = doc.QuerySelector<IHtmlImageElement>("img[alt=\"Channel Image\"]")?.Dataset["src"],
        Updated = DateTime.UtcNow
      };
    }

    static VideoStored2[] GetVideos(IDocument doc, Channel c, ILogger log) {
      var videos = doc.Body.QuerySelectorAll(".channel-videos-container")
        .Select(e => Video(e) with {ChannelId = c.ChannelId, ChannelTitle = c.ChannelTitle}).ToArray();
      log.Debug("BcWeb loaded {Videos} for {Channel}", videos.Length, c.ChannelTitle);
      return videos;
    }

    static VideoStored2 Video(IElement c) {
      IElement Qs(string s) => c.QuerySelector(s);

      var videoA = c.QuerySelector<IHtmlAnchorElement>(".channel-videos-title .spa");
      return new() {
        Platform = Platform.BitChute,
        VideoId = Platform.BitChute.FullId(videoA?.Href.LastInPath()),
        Title = videoA?.Text,
        UploadDate = c.QuerySelectorAll(".channel-videos-details.text-right")
          .Select(s => s.TextContent.Trim().TryParseDateExact("MMM dd, yyyy"))
          .NotNull().FirstOrDefault(),
        Description = Qs(".channel-videos-text")?.InnerHtml,
        Duration = Qs(".video-duration")?.TextContent.TryParseTimeSpan(),
        Statistics = new(Qs(".video-views")?.TextContent.Trim().ParseBcNumber()),
        Thumb = c.QuerySelector<IHtmlImageElement>("img[alt=\"video image\"]")?.Dataset["src"],
        Updated = DateTime.UtcNow
      };
    }
  }

  record BcResponse(bool success);

  record ExtendResponse(string html, bool success) : BcResponse(success);

  record CountResponse(ulong subscriber_count, string about_view_count);

  public static class BcExtensions {
    
    public static async Task<IFlurlResponse> BcPost(this IFlurlRequest req, string csrf,  object formValues = null) {
      return await req.AllowAnyHttpStatus()
        .PostUrlEncodedAsync(MergeDynamics(new {csrfmiddlewaretoken = csrf}, formValues ?? new ExpandoObject()));
    }

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

    static readonly Regex ViewsRe = new(@"(?<num>\d+\.?\d*)\s?(?<unit>[KMB]?)", Compiled | IgnoreCase);

    public static ulong? ParseBcNumber(this string s) {
      var m = ViewsRe.Match(s);
      if (!m.Success) return null;
      var num = m.Groups["num"].Value.TryParseDouble() ?? 0;
      var unitNum = m.Groups["unit"].Value.ToLowerInvariant() switch {
        "b" => num.Billions(),
        "m" => num.Millions(),
        "k" => num.Thousands(),
        _ => num
      };
      return (ulong) Math.Round(unitNum);
    }

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