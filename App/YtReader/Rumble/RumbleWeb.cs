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
using AngleSharp.Io;
using Flurl;
using Flurl.Http;
using Humanizer;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.SimpleCollect;
using YtReader.Store;
using YtReader.Web;
using HttpMethod = System.Net.Http.HttpMethod;
using Url = Flurl.Url;

namespace YtReader.Rumble {
  public record RumbleWeb(RumbleCfg Cfg, FlurlProxyClient Http) : IScraper {
    public const    string         RumbleDotCom = "https://rumble.com/";
    static readonly IConfiguration AngleCfg     = Configuration.Default.WithDefaultLoader();
    

    public async Task<(Channel Channel, IAsyncEnumerable<Video[]> Videos)> ChannelAndVideos(string sourceId, ILogger log) {
      var bc = BrowsingContext.New(AngleCfg);
      var channelUrl = ChannelUrl(sourceId);
      var doc = await bc.OpenAsync(channelUrl);
      var chan = this.NewChan(sourceId);
      if (doc.StatusCode == HttpStatusCode.NotFound) 
        return (Channel: chan with {Status = ChannelStatus.Dead, StatusMessage = $"{channelUrl} returned 404"}, Videos: AsyncEnumerable.Empty<Video[]>());
      doc.EnsureSuccess();

      var chanUrl = doc.El<IHtmlLinkElement>("link[rel=canonical]")?.Href.AsUrl();
      string[] altIds = null;
      if (chanUrl != null) {
        // use the canonical link to fix up ones where have a url that redirects. e.g.c/c-346475 redirects to c/RedpillProject, so we use c/RedpillProject
        var canonicalId = chanUrl.Path.TrimPath();
        if (sourceId != canonicalId) {
          altIds = new[] {sourceId};
          chan = this.NewChan(canonicalId);
        }
      }

      chan = chan with {
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

    static readonly Regex EarnedRe    = new(@"\$(?<earned>[\d.\d]+) earned", RegexOptions.Compiled);

    async Task<JObject> EmbeddedVideo(string embedId, ILogger log) {
      var url = RumbleDotCom.AppendPathSegment("embedJS/u3/")
        .SetQueryParams(new {request = "video", ver = "2", v = embedId});
      var req = url.AsRequest();
      const string desc = "rumble video json";
      var res = await Http.Send(desc, req, HttpMethod.Get, log:log);
      res.EnsureSuccess(log, desc, req);
      var j = await res.JsonObject();
      return j;
    }
    
    public async Task<VideoExtra> Video(string sourceId, ILogger log) {
      var bc = BrowsingContext.New(AngleCfg);
      var doc = await bc.OpenAsync(VideoUrl(sourceId)).WhenStable();
      var vid = this.NewVidExtra(sourceId);
      if (doc.StatusCode == HttpStatusCode.NotFound)
        return vid with {Status = VideoStatus.NotFound};
      doc.EnsureSuccess();

      string MetaProp(string prop) => MetaProps(prop).FirstOrDefault();
      IEnumerable<string> MetaProps(string prop) => doc.QuerySelectorAll<IHtmlMetaElement>($"meta[property=\"og:{prop}\"]").Select(e => e.Content);
      
      var vidScript = doc.QuerySelectorAll<IHtmlScriptElement>("script[type=\"application/ld+json\"]")
        .SelectMany(e => JArray.Parse(e.Text).Children<JObject>()).FirstOrDefault(j => j.Str("@type") == "VideoObject") 
        ?? throw new("Can't find video objects in the page script");
      
      var mediaByDiv = doc.El<IHtmlDivElement>("div.media-by");
      var contentDiv = doc.QuerySelector(".content.media-description");
      contentDiv?.QuerySelector("span.breadcrumbs").Remove(); // clean up description

      var embedId = vidScript.Str("embedUrl")?.AsUrl().PathSegments.LastOrDefault() ?? throw new ("can't find embed video id");
      var vidEmbed = await EmbeddedVideo(embedId, log); // need this to get the url
      var chanUrl = vidEmbed.Str("author.url")?.AsUrl();
      
      vid = vid with {
        Title = vidScript.Str("name"),
        Description = vidScript.Str("description"),
        ChannelId = chanUrl,
        ChannelSourceId = chanUrl?.Path,
        ChannelTitle = vidEmbed.Str("author.name"),
        UploadDate = vidScript.Value<DateTime>("uploadDate"),
        Statistics = new(vidScript.SelectToken("interactionStatistic.userInteractionCount")?.Value<ulong>()),
        Thumb = vidScript.Str("thumbnailUrl"),
        Duration =  vidEmbed.SelectToken("duration")?.Value<int>().Seconds(),
        Keywords = MetaProp("tag")?.Split(" ").ToArray() ?? MetaProps("video:tag").ToArray(),
        Earned = mediaByDiv?.QuerySelector(".media-earnings")?.TextContent.Match(EarnedRe)?.Groups["earned"].Value.NullIfEmpty()?.ParseDecimal(),
        MediaUrl = vidEmbed.Str("u.mp4.url")
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