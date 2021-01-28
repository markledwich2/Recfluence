using System;
using System.Collections.Generic;
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
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Reflection;
using SysExtensions.Text;
using YtReader.Store;
using static System.Text.RegularExpressions.RegexOptions;
using Url = Flurl.Url;

// ReSharper disable InconsistentNaming

namespace YtReader.BitChute {
  public class BcWeb {
    static readonly string         BcUrl    = "https://www.bitchute.com";
    static readonly IConfiguration AngleCfg = Configuration.Default.WithDefaultLoader().WithDefaultCookies();

    public async Task<(ChannelStored2 channel, IAsyncEnumerable<VideoStored2[]> videos)> ChannelAndVideos(string channelId, ILogger log) {
      var context = BrowsingContext.New(AngleCfg);
      var chanPath = $"channel/{channelId}";
      var chanDoc = await context.OpenAsync(BcUrl.AppendPathSegment(chanPath));
      var csrf = chanDoc.CsrfToken();

      var chanCount = await BcUrl.AppendPathSegments(chanPath, "counts/").BcPost<CountResponse>(chanDoc, csrf);
      var channel = ParseChannel(chanDoc, channelId) with {
        Subs = chanCount.subscriber_count,
        ChannelViews = chanCount.about_view_count.ParseBcNumber()
      };

      async IAsyncEnumerable<VideoStored2[]> Videos() {
        var chanVids = GetVideos(chanDoc);
        yield return chanVids;

        var offset = chanVids.Length;
        while (true) {
          var extendRes = await BcUrl.AppendPathSegments(chanPath, "extend/").BcPost<ExtendResponse>(chanDoc, new {offset}, csrf);
          var extendDoc = await context.OpenAsync(req => req.Content(extendRes.html));
          var videos = GetVideos(extendDoc);
          if (videos.Length <= 0) break;
          offset += videos.Length;
          yield return videos;
        }
      }

      return (channel, videos: Videos());
    }

    static ChannelStored2 ParseChannel(IDocument doc, string channelId) {
      IElement Qs(string s) => doc.Body.QuerySelector(s);

      var profileA = doc.QuerySelector<IHtmlAnchorElement>(".channel-banner .details .name > a");
      return new() {
        ChannelId = channelId,
        ChannelTitle = Qs("#channel-title")?.TextContent,
        ProfileId = profileA?.Href.LastInPath(),
        ProfileName = profileA?.TextContent,
        Videos = Qs(".channel-about-details svg.fa-video + span")?.TextContent.ParseBcNumber(),
        Created = Qs(".channel-about-details > p:first-child")?.TextContent.ParseCreated(),
        Updated = DateTime.UtcNow
      };
    }

    static VideoStored2[] GetVideos(IDocument doc) => doc.Body.QuerySelectorAll(".channel-videos-container").Select(Video).ToArray();

    static VideoStored2 Video(IElement c) {
      IElement Qs(string s) => c.QuerySelector(s);

      var videoA = c.QuerySelector<IHtmlAnchorElement>(".channel-videos-title .spa");
      return new() {
        Platform = Platform.BitChute,
        VideoId = videoA?.Href.LastInPath(),
        Title = videoA?.Text,
        UploadDate = c.QuerySelectorAll(".channel-videos-details.text-right")
          .Select(s => s.TextContent.Trim().TryParseDateExact("MMM dd, yyyy"))
          .NotNull().FirstOrDefault(),
        Description = Qs(".channel-videos-text")?.InnerHtml,
        Duration = Qs(".video-duration")?.TextContent.TryParseTimeSpan(),
        Statistics = new(Qs(".video-views")?.TextContent.Trim().ParseBcNumber()),
        Thumb = c.QuerySelector<IHtmlImageElement>(".channel-videos-image > img")?.Source,
        Updated = DateTime.UtcNow
      };
    }
  }

  record BcResponse {
    public bool success { get; set; }
  }

  record ExtendResponse : BcResponse {
    public string html { get; set; }
  }

  record CountResponse {
    public ulong  subscriber_count { get; set; }
    public string about_view_count { get; set; }
  }

  public static class BcExtensions {
    public static async Task<T> BcPost<T>(this Url url, IDocument originating, object formValues, string csrf = null) {
      csrf ??= originating.CsrfToken();
      var furl = await url.WithBcHeaders(originating)
        .PostUrlEncodedAsync(ReflectionExtensions.MergeDynamics(new {csrfmiddlewaretoken = csrf}, formValues));
      return await furl.GetJsonAsync<T>();
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