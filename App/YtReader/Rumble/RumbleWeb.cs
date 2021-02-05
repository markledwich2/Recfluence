using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using AngleSharp;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using Flurl;
using Serilog;
using SysExtensions.Net;
using SysExtensions;
using SysExtensions.Text;
using YtReader.Store;
using Url = Flurl.Url;

namespace YtReader.Rumble {
  public record RumbleWeb {
    public const string RumbleDotCom = "https://rumble.com/";
    static readonly IConfiguration AngleCfg     = Configuration.Default.WithDefaultLoader();

    public async Task<(Channel Channel, IAsyncEnumerable<VideoStored2[]> Videos)> ChannelAndVideos(string urlOrId, ILogger log) {
      var chanUrl = Url.IsValid(urlOrId) ? urlOrId.AsUrl() : RumbleDotCom.AppendPathSegments("c", urlOrId);
      var bc = BrowsingContext.New(AngleCfg);
      var doc = await bc.OpenAsync(chanUrl);
      doc.StatusCode.EnsureSuccess();

      var chanId = doc.Qs<IHtmlLinkElement>("link[rel=canonical]")?.Href;
      var sourceId = chanId.AsUrl().PathSegments.LastOrDefault();
      var chan = new Channel {
        Updated = DateTime.Now,
        Platform = Platform.Rumble,
        SourceId = sourceId,
        ChannelId = chanId,
        ChannelTitle = doc.Title,
        Subs = doc.QuerySelector(".subscribe-button-count")?.TextContent.TryParseNumberWithUnits()?.RoundToULong(),
        LogoUrl = doc.Qs<IHtmlImageElement>(".listing-header--thumb")?.Source,
        Status = ChannelStatus.Alive
      };

      async IAsyncEnumerable<VideoStored2[]> Videos() {
        VideoStored2[] ParseVideos(IDocument d) => d.QuerySelectorAll(".video-listing-entry").Select(e => ParseVideo(e, chan)).ToArray();
        string NextUrl(IDocument d) => d.Qs<IHtmlLinkElement>("link[rel=next]")?.Href;

        yield return ParseVideos(doc);
        var next = NextUrl(doc);
        while (next.HasValue()) {
          var page = await bc.OpenAsync(next);
          page.StatusCode.EnsureSuccess();
          yield return ParseVideos(page);
          next = NextUrl(page);
        }
      }

      return (Channel: chan, Videos: Videos());
    }

    VideoStored2 ParseVideo(IElement e, Channel chan) {
      var url = e.Qs<IHtmlAnchorElement>(".video-item--a")?.Href?.AsUrl();
      if (url?.IsRelative == true)
        url = RumbleDotCom.AppendPathSegment(url.Path);

      string Data(string name) => e.Qs<IHtmlSpanElement>($".video-item--{name}")?.Dataset["value"];

      var video = new VideoStored2 {
        Updated = DateTime.UtcNow,
        Platform = Platform.Rumble,
        ChannelId = chan.ChannelId,
        ChannelTitle = chan.ChannelTitle,
        VideoId = url?.ToString(),
        Title = e.QuerySelector(".video-item--title")?.TextContent,
        Thumb = e.Qs<IHtmlImageElement>("img.video-item--img")?.Source,
        Statistics = new(Data("views")?.ParseULong()),
        UploadDate = e.Qs<IHtmlTimeElement>(".video-item--time")?.DateTime.ParseDate(style: DateTimeStyles.AssumeUniversal),
        Duration = Data("duration")?.TryParseTimeSpanExact(@"h\:m\:s", @"m\:s"),
        Earned = Data("earned")?.TryParseDecimal()
      };
      return video;
    }
  }
}