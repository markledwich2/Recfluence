using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using AngleSharp;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using Flurl;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.Store;
using YtReader.Web;
using Url = Flurl.Url;

namespace YtReader.Rumble {
  public record RumbleWeb(RumbleCfg Cfg) : IScraper {
    public const    string         RumbleDotCom = "https://rumble.com/";
    static readonly IConfiguration AngleCfg     = Configuration.Default.WithDefaultLoader();

    public async Task<(Channel Channel, IAsyncEnumerable<Video[]> Videos)> ChannelAndVideos(string sourceId, ILogger log) {
      var bc = BrowsingContext.New(AngleCfg);
      var doc = await bc.OpenAsync(ChannelUrl(sourceId));
      doc.EnsureSuccess();

      var chanUrl = doc.El<IHtmlLinkElement>("link[rel=canonical]")?.Href.AsUrl();
      string[] altIds = null;
      if (chanUrl != null) {
        // use the canonical link to fix up ones where have a url that redirects. e.g.c/c-346475 redirects to c/RedpillProject, so we use c/RedpillProject
        var canonicalId = chanUrl.Path.TrimPath();
        if (sourceId != canonicalId) {
          altIds = new[] {sourceId};
          sourceId = canonicalId;
        }
      }

      var chan = this.NewChan(sourceId) with {
        SourceIdAlts = altIds,
        ChannelTitle = doc.Title,
        Subs = doc.QuerySelector(".subscribe-button-count")?.TextContent.TryParseNumberWithUnits()?.RoundToULong(),
        LogoUrl = doc.El<IHtmlImageElement>(".listing-header--thumb")?.Source,
        Status = ChannelStatus.Alive
      };

      async IAsyncEnumerable<Video[]> Videos() {
        Video[] ParseVideos(IDocument d) => d.QuerySelectorAll(".video-listing-entry").Select(e => ParseVideo(e, chan)).ToArray();
        string NextUrl(IDocument d) => d.El<IHtmlLinkElement>("link[rel=next]")?.Href;

        yield return ParseVideos(doc);
        var next = NextUrl(doc);
        while (next.HasValue()) {
          var page = await bc.OpenAsync(next);
          page.EnsureSuccess();
          yield return ParseVideos(page);
          next = NextUrl(page);
        }
      }

      return (Channel: chan, Videos: Videos());
    }

    /// <summary>Path is the path from rumble.com to the channel (e.g. c/funnychannel or user/viraluser) Rumble video's can be
    ///   on users or channel pages. We treat users and channels the same. Channel URL's are paths to</summary>
    static Url ChannelUrl(string path) => path == null ? null : RumbleDotCom.AppendPathSegments(path);

    static Url VideoUrl(string path) => path == null ? null : RumbleDotCom.AppendPathSegments(path);

    static readonly Regex ViewsRe     = new(@"(?<views>[\d,]+) Views", RegexOptions.Compiled);
    static readonly Regex EarnedRe    = new(@"\$(?<earned>[\d.\d]+) earned", RegexOptions.Compiled);
    static readonly Regex PublishedRe = new(@"Published[\s]*(?<date>.*)", RegexOptions.Compiled);

    public async Task<VideoExtra> Video(string sourceId, ILogger log) {
      var bc = BrowsingContext.New(AngleCfg);
      var doc = await bc.OpenAsync(VideoUrl(sourceId));
      var vid = this.NewVidExtra(sourceId);
      if (doc.StatusCode == HttpStatusCode.NotFound)
        return vid with {Status = VideoStatus.NotFound};
      doc.EnsureSuccess();

      string MetaProp(string prop) => MetaProps(prop).FirstOrDefault();
      IEnumerable<string> MetaProps(string prop) => doc.QuerySelectorAll<IHtmlMetaElement>($"meta[property=\"og:{prop}\"]").Select(e => e.Content);

      var mediaByDiv = doc.El<IHtmlDivElement>("div.media-by");
      var chanA = doc.El<IHtmlAnchorElement>(".media-by--a[rel=author]");
      var chanUrl = chanA?.Href.AsUrl();
      var channelSourceId = chanUrl?.Path.TrimPath();
      var ldJson = doc.QuerySelectorAll<IHtmlScriptElement>("script[type=\"application/ld+json\"]").SelectMany(e => JArray.Parse(e.Text).Children<JObject>());
      var durationText = ldJson.FirstOrDefault(j => j.Value<string>("@type") == "VideoObject")?["duration"]?.Value<string>();
      var duration = durationText?.TryParseTimeSpanExact(@"\P\Thh\Hmm\Mss\S");
      var contentDiv = doc.QuerySelector(".content.media-description");
      contentDiv?.QuerySelector("span.breadcrumbs").Remove(); // clean up description
      var viewsText = mediaByDiv?.QuerySelectorAll(".media-heading-info")
        .Select(s => s.TextContent.Match(ViewsRe)).FirstOrDefault(m => m.Success)?.Groups["views"].Value;

      var publishedText = mediaByDiv?.QuerySelector(".media-heading-published")?.TextContent?.Trim();
      vid = vid with {
        Title = MetaProp("title")?.Trim(),
        ChannelId = ChannelUrl(channelSourceId),
        ChannelSourceId = channelSourceId,
        ChannelTitle = chanA?.QuerySelector(".media-heading-name")?.TextContent.Trim(),
        UploadDate = publishedText?.Match(PublishedRe).Groups["date"].Value.TryParseDateExact("MMMM d, yyyy", DateTimeStyles.AssumeUniversal),
        Statistics = new(viewsText.TryParseULong()),
        Thumb = MetaProp("image"),
        Keywords = MetaProp("tag")?.Split(" ").ToArray() ?? MetaProps("video:tag").ToArray(),
        Description = doc.QuerySelector(".content.media-description")?.TextContent.Trim(),
        Earned = mediaByDiv?.QuerySelector(".media-earnings")?.TextContent.Match(EarnedRe)?.Groups["earned"].Value.NullIfEmpty()?.ParseDecimal(),
        Duration = duration
      };
      return vid;
    }

    public string SourceToFullId(string sourceId, LinkType type) => type switch {
      LinkType.Channel => ChannelUrl(sourceId),
      LinkType.Video => VideoUrl(sourceId),
      _ => throw new ArgumentOutOfRangeException(nameof(type), type, message: null)
    };

    public          int      CollectParallel => Cfg.CollectParallel;
    public          Platform Platform        => Platform.Rumble;
    static readonly Regex    VideoIdRe = new(@"(?<id>v\w{5})-.*");

    Video ParseVideo(IElement e, Channel chan) {
      var url = e.El<IHtmlAnchorElement>(".video-item--a")?.Href?.AsUrl();
      var sourceId = url?.Path.Match(VideoIdRe).Groups["id"].Value.NullIfEmpty();

      string Data(string name) => e.El<IHtmlSpanElement>($".video-item--{name}")?.Dataset["value"];

      var video = this.NewVid(sourceId) with {
        ChannelId = chan.ChannelId,
        ChannelTitle = chan.ChannelTitle,
        Title = e.QuerySelector(".video-item--title")?.TextContent,
        Thumb = e.El<IHtmlImageElement>("img.video-item--img")?.Source,
        Statistics = new(Data("views")?.ParseULong()),
        UploadDate = e.El<IHtmlTimeElement>(".video-item--time")?.DateTime.ParseDate(style: DateTimeStyles.AssumeUniversal),
        Duration = Data("duration")?.TryParseTimeSpanExact(@"h\:m\:s", @"m\:s"),
        Earned = Data("earned")?.TryParseDecimal()
      };
      return video;
    }
  }
}