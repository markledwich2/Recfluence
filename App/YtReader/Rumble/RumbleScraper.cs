using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using AngleSharp;
using AngleSharp.Css.Dom;
using AngleSharp.Css.Parser;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using Flurl;
using Humanizer;
using Newtonsoft.Json.Linq;
using Polly;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.SimpleCollect;
using YtReader.Store;
using YtReader.Web;
using Url = Flurl.Url;

namespace YtReader.Rumble {
  public record RumbleScraper(RumbleCfg Cfg, FlurlProxyClient Http) : IScraper {
    public const    string         RumbleDotCom = "https://rumble.com/";
    static readonly IConfiguration AngleCfg     = Configuration.Default.WithDefaultLoader();
    public          int            CollectParallel => Cfg.CollectParallel;
    public          Platform       Platform        => Platform.Rumble;

    #region Discover & Video Lists

    IBrowsingContext Bc() => BrowsingContext.New(AngleCfg);

    public async IAsyncEnumerable<Video[]> HomeVideos(ILogger log) {
      var home = await Bc().OpenAsync(RumbleDotCom);
      home.EnsureSuccess();
      await foreach (var b in home
        .QuerySelectorAll<IHtmlAnchorElement>(".mediaList-link-more > a").Select(a => a.Href).NotNull().ToArray()
        .BlockMap(async catUrl => {
          var bc = Bc(); // not sure if bc is thread safe to make seperate contexts
          var catDoc = await bc.OpenAsync(catUrl);
          var catName = catUrl.AsUrl().Path.LastInPath();
          var videos = await Videos(catDoc).Select((b, i) => {
            log.Information("Collect {Platform} - crawled {Videos} videos on page {Page} in category {Category}",
              Platform, b.Length, i + 1, catName);
            return b.Select(v => v with {Tags = new[] {("Category", catName)}.ToMultiValueDictionary()});
          }).SelectMany().ToArrayAsync();
          return videos;
        }, Cfg.CollectParallel))
        yield return b;
    }

    static readonly CssParser Css = new(new());

    async IAsyncEnumerable<Video[]> Videos(IDocument doc) {
      Video[] ParseVideos(IDocument d) => d.QuerySelectorAll(".video-listing-entry").Select(ParseVideo).ToArray();

      string NextUrl(IDocument d) => d.El<IHtmlLinkElement>("link[rel=next]")?.Href;
      yield return ParseVideos(doc);
      var next = NextUrl(doc);
      while (next.HasValue()) {
        var page = await doc.Context.OpenAsync(next);
        page.EnsureSuccess();
        yield return ParseVideos(page);
        next = NextUrl(page);
      }
    }

    static readonly Regex VideoIdRe      = new(@"(?<id>v\w{5})-.*");
    static readonly Regex ChannelClassId = new(@"video-item--by-a--c(?<channelNum>\d+)");

    Video ParseVideo(IElement e) {
      var url = e.El<IHtmlAnchorElement>(".video-item--a")?.Href?.AsUrl();
      var sourceId = url?.Path.Match(VideoIdRe).Groups["id"].Value.NullIfEmpty();
      string Data(string name) => e.El<IHtmlSpanElement>($".video-item--{name}")?.Dataset["value"];
      var chanEl = e.El<IHtmlAnchorElement>(".video-item--by > a[rel='author']");
      var chanSourceId = $"c-{chanEl?.GetAttribute("class").Match(ChannelClassId)?.Groups["channelNum"].Value}";
      var chanTitle = chanEl?.TextContent.Trim();

      var video = this.NewVid(sourceId) with {
        Title = e.QuerySelector(".video-item--title")?.TextContent,
        Thumb = e.El<IHtmlImageElement>("img.video-item--img")?.Source,
        Statistics = new(Data("views")?.TryParseULong()) {Rumbles = Data("rumbles")?.TryParseULong()},
        UploadDate = e.El<IHtmlTimeElement>(".video-item--time")?.DateTime.ParseDate(style: DateTimeStyles.AssumeUniversal),
        Duration = Data("duration")?.TryParseTimeSpanExact(@"h\:m\:s", @"m\:s"),
        Earned = Data("earned")?.TryParseDecimal(),
        ChannelSourceId = chanSourceId,
        ChannelTitle = chanTitle,
        ChannelId = SourceToFullId(chanSourceId, LinkType.Channel)
      };
      return video;
    }

    #endregion

    #region Channel & Channel-Videos

    /// <summary>Path is the path from rumble.com to the channel (e.g. c/funnychannel or user/viraluser) Rumble video's can be
    ///   on users or channel pages. We treat users and channels the same. Channel URL's are paths to</summary>
    static Url ChannelUrl(string path) => path == null ? null : RumbleDotCom.AppendPathSegments(path);

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

      return (Channel: chan with {
        SourceIdAlts = altIds,
        ChannelTitle = doc.Title,
        Subs = doc.QuerySelector(".subscribe-button-count")?.TextContent.TryParseNumberWithUnits()?.RoundToULong(),
        LogoUrl = doc.El<IHtmlImageElement>(".listing-header--thumb")?.Source,
        Status = ChannelStatus.Alive
      }, Videos: Videos(doc).Select(b => b.Select(v => v with {
        ChannelId = chan.ChannelId,
        ChannelTitle = chan?.ChannelTitle,
        ChannelSourceId = chan.SourceId
      }).ToArray()));
    }

    async Task<JObject> EmbeddedVideo(string embedId, ILogger log) {
      var url = RumbleDotCom.AppendPathSegment("embedJS/u3/")
        .SetQueryParams(new {request = "video", ver = "2", v = embedId});
      var req = url.AsRequest();
      const string desc = "rumble video json";
      var res = await Http.Send(desc, req, HttpMethod.Get, log: log);
      res.EnsureSuccess(log, desc, req);
      var j = await res.JsonObject();
      return j;
    }

    #endregion

    #region Video Extra

    static Url VideoUrl(string path) => path == null ? null : RumbleDotCom.AppendPathSegments(path);
    static readonly Regex EarnedRe = new(@"\$(?<earned>[\d.\d]+) earned", RegexOptions.Compiled);

    public async Task<(VideoExtra Video, VideoComment[] Comments)> VideoAndExtra(string sourceId, ILogger log) {
      var bc = BrowsingContext.New(AngleCfg);

      var vid = this.NewVidExtra(sourceId);
      var (_, doc) = await Open(VideoUrl(sourceId), log, bc);
      if (doc == null) throw new("doc null after retries");
      if (doc.StatusCode == HttpStatusCode.NotFound)
        return (vid with {Status = VideoStatus.NotFound}, null);
      doc.EnsureSuccess();

      string MetaProp(string prop) => MetaProps(prop).FirstOrDefault();
      IEnumerable<string> MetaProps(string prop) => doc.QuerySelectorAll<IHtmlMetaElement>($"meta[property=\"og:{prop}\"]").Select(e => e.Content);

      var vidScript = doc.QuerySelectorAll<IHtmlScriptElement>("script[type=\"application/ld+json\"]")
          .SelectMany(e => JArray.Parse(e.Text).Children<JObject>()).FirstOrDefault(j => j.Str("@type") == "VideoObject")
        ?? throw new("Can't find video objects in the page script");

      var mediaByDiv = doc.El<IHtmlDivElement>("div.media-by");
      var contentDiv = doc.QuerySelector(".content.media-description");
      contentDiv?.QuerySelector("span.breadcrumbs").Remove(); // clean up description

      var embedId = vidScript.Str("embedUrl")?.AsUrl().PathSegments.LastOrDefault() ?? throw new("can't find embed video id");
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
        Duration = vidEmbed.SelectToken("duration")?.Value<int>().Seconds(),
        Keywords = MetaProp("tag")?.Split(" ").ToArray() ?? MetaProps("video:tag").ToArray(),
        Earned = mediaByDiv?.QuerySelector(".media-earnings")?.TextContent.Match(EarnedRe)?.Groups["earned"].Value.NullIfEmpty()?.ParseDecimal(),
        MediaUrl = vidEmbed.Str("u.mp4.url")
      };

      var classToThumb = doc.Els<IHtmlStyleElement>("style").SelectMany(e => Css.ParseStyleSheet(e.TextContent).Rules)
        .Where(r => r.Type == CssRuleType.Style).Select(s => s.CssText.Match(AuthorThumbRe)).Where(m => m.Success)
        .ToDictionarySafe(m => m.Groups["class"].Value, m => m.Groups["ulr"].Value);

      var comments = doc.Els("#video-comments .comment-item").Where(e => e?.Id != "comment-create-1")
        .SelectMany(e => ParseComment(e, classToThumb)).ToArray();
      return (vid, comments);
    }

    static readonly Regex AuthorThumbRe = new(@"i\.(?<class>user-image--img--id-\w*?) {\s*background-image:\surl\((?<ulr>.*?)\);", RegexOptions.Compiled);

    static IEnumerable<VideoComment> ParseComment(IElement commentEl, IDictionary<string, string> classToThumb, string replyToId = null) {
      var root = ParseCommentRaw(commentEl, classToThumb, replyToId);
      yield return root;
      foreach (var reply in commentEl.Els(".comments > .comment-item").SelectMany(e => ParseComment(e, classToThumb, root.CommentId)))
        yield return reply;
    }

    /// <summary>Parses comments. Note this has not been tested. To run this it will require a login</summary>
    static VideoComment ParseCommentRaw(IElement e, IDictionary<string, string> classToThumb, string replyToId) {
      var thumb = e.Children.FirstOrDefault(i => i.ClassList.Contains("user-image--img")) // thumbnail images are in css. Look them up
        ?.ClassList.FirstOrDefault(c => c.StartsWith("user-image--img--id-")).Do(n => classToThumb.TryGet(n));

      var meta = e.El(":scope > comments-meta"); // use :scope and > selectors to ensure we don't get child comments data
      var authorA = meta.El<IHtmlAnchorElement>(":scope > .comments-meta-author");

      var comment = new VideoComment {
        Platform = Platform.Rumble,
        Updated = DateTime.UtcNow,
        CommentId = e.GetAttribute("data-comment-id"),
        AuthorThumb = thumb,
        Author = authorA.TextContent,
        AuthorId = authorA.Href.LastInPath(),
        Created = meta.Els("span").Select(s => s.GetAttribute("title")
          ?.TryParseDateExact("dddd, MMM d, yyyy hh:mm tt zz", DateTimeStyles.AllowWhiteSpaces)).FirstOrDefault(),
        Comment = e.El(":scope > .comment-text")?.TextContent,
        Likes = e.El(":scope > .rumbles-vote > .rumbles-count").TextContent?.TryParseInt(),
        ReplyToCommentId = replyToId
      };
      return comment;
    }

    async Task<(bool finished, IDocument doc)> Open(Url url, ILogger log, IBrowsingContext bc) =>
      await Policy.HandleResult<(bool finished, IDocument doc)>(
          d => { // todo make this re-usable. Needed a higher level than http send for handling bot errors
            var (finished, document) = d;
            if (!finished) return true; // retry on timeout
            if (document == null) throw new("doc null, usually this means anglesharp is misconfigured");
            if (document.StatusCode == HttpStatusCode.TooManyRequests) throw new("ruble is blocking us. implement proxy fallback");
            return document.StatusCode.IsTransientError() || document.Body?.Children.Length <= 0;
          }).RetryWithBackoff("Rumble Video", Cfg.Retries, (_, attempt, delay) =>
          log.Debug("Rumble - Retrying in {Duration}, attempt {Attempt}/{Total}", delay.HumanizeShort(), attempt, Cfg.Retries), log)
        .ExecuteAsync(() => bc.OpenAsync(url).WithTimeout(30.Seconds()));

    public string SourceToFullId(string sourceId, LinkType type) => type switch {
      LinkType.Channel => ChannelUrl(sourceId),
      LinkType.Video => VideoUrl(sourceId),
      _ => throw new ArgumentOutOfRangeException(nameof(type), type, message: null)
    };

    #endregion
  }
}