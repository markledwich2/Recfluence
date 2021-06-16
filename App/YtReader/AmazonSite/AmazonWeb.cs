using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AngleSharp;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using Humanizer;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Polly;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using YtReader.Web;
using PT = YtReader.AmazonSite.AmazonPageType;

// ReSharper disable PossibleLossOfFraction
// ReSharper disable StringLiteralTypo
// ReSharper disable InconsistentNaming

namespace YtReader.AmazonSite {
  public record AmazonCfg(int WebParallel = 32, int BatchSize = 100, int Retries = 8, ProxyType ProxyType = ProxyType.Residential) {
    public TimeSpan RequestTimeout { get; init; } = 2.Minutes();
  }

  public record AmazonWeb(SnowflakeConnectionProvider Conn, FlurlProxyClient FlurlClient, YtStore Store, AmazonCfg Cfg, VersionInfo Version,
    BlobStores Stores, IPipeCtx Pipe) {
    readonly ISimpleFileStore LogStore = Stores.Store(DataStoreType.Logs);

    public static readonly Dictionary<string, string> NamedSql = new() {
      {
        "Activewear Links", @"
 with vids as (
  select distinct video_id
  from activewear_mentions
)
select distinct u.value:url::string url
from vids m
       join video_latest v on v.video_id=m.video_id
      , table (flatten(matchurls(description))) u
where u.value:host::string like any ('%amazon.%', '%amzn.to%')
"
      }
    };

    record UrlRow(string url);

    public async Task GetProductLinkInfo(ILogger log, CancellationToken cancel, string queryName = null, int? limit = null, bool forceLocal = false) {
      var sql = @$"with l as ({NamedSql[queryName ?? "Activewear Links"]})
select url from l
where not exists (select * from link_meta_stage s where s.v:SourceUrl = l.url)
{limit.Do(l => $"limit {l}")}
";
      IReadOnlyCollection<string> urls;
      using (var db = await Conn.Open(log))
        urls = await db.QueryAsync<UrlRow>("amazon links", sql).Select(l => l.url).ToListAsync();
      if (forceLocal)
        await ProcessLinks(urls, log, cancel);
      else
        await urls.Pipe(Pipe, b => ProcessLinks(b, PipeArg.Inject<ILogger>(), PipeArg.Inject<CancellationToken>()), cancel: cancel);
    }

    public record LoadFromUrlRes(AmazonLink Link, HttpStatusCode Status = HttpStatusCode.OK, string ErrorMsg = null);

    [Pipe]
    public async Task<long> ProcessLinks(IReadOnlyCollection<string> urls, ILogger log, CancellationToken cancel) {
      log = log.ForContext("Module", "Amazon");
      var unhandledErrors = 0;
      var res = await urls.BlockMap(async (url, urlNo) => {
          var urlLog = log.ForContext("Url", url);
          //log.Debug("Amazon - #{Row}/{Total} ({Url}) - starting", i, urls.Count, l);
          var retryTransient = Policy
            .HandleResult<LoadFromUrlRes>(
              d => { // todo make this re-usable. Needed a higher level than http send for handling bot errors
                if (!d.Status.IsTransientError()) return false;
                if (d.Status == HttpStatusCode.TooManyRequests && !FlurlClient.UseProxy) {
                  FlurlClient.UseProxy = true;
                  urlLog.Debug("Amazon - falling back to using proxy");
                }
                return true;
              }).Or<HttpRequestException>(e => e.Message.Contains("SSL"))
            .RetryWithBackoff("Amazon - product", Cfg.Retries, (r, attempt, delay) =>
              urlLog.Debug("Amazon - retriable error: '{Error}'. Retrying in {Duration}, attempt {Attempt}/{Total}",
                r.Result?.ErrorMsg ?? r.Exception?.Message ?? "Unknown error", delay.HumanizeShort(), attempt, Cfg.Retries), urlLog);
          var (meta, ex) = await retryTransient.ExecuteAsync(() => LoadLinkMeta(url, urlLog)).Try();
          //log.Debug("Amazon - #{Row}/{Total} ({Url}) - done", i, urls.Count, l);
          if (ex == null) return meta;
          Interlocked.Increment(ref unhandledErrors);
          if (unhandledErrors > 5 && unhandledErrors / (1 + urlNo) > 0.1) {
            urlLog.Warning(ex, "Amazon - too many unhandled errors");
            throw ex;
          }
          urlLog.Debug(ex, "Amazon - unhandled error: {Error}", ex.Message);
          return null;
        }, Cfg.WebParallel, cancel: cancel)
        .Batch(Cfg.BatchSize)
        .BlockMap(async (b, i) => {
          log.Debug("Aamazon - about to save batch {Batch}", i + 1);
          var metas = b.NotNull().Where(l => l.Status.In(HttpStatusCode.OK, HttpStatusCode.NotFound) && l.Link != null).Select(l => l.Link).ToArray();
          log.Debug("Aamazon - saved {Videos} video link metadata. Batch {Batch}/{BatchTotal}", metas.Length, i + 1,
            Math.Ceiling(urls.Count / (double) Cfg.BatchSize));
          await Store.AmazonLink.Append(metas);
          return metas.Length;
        })
        .ToListAsync().Then(completed => completed.Sum());
      return res;
    }

    /// <summary>Loads the given url. Handles basic loading issues and retries content requested refreshing</summary>
    async Task<(IDocument Doc, HttpStatusCode Status, string Msg)> LoadLink(string url) {
      var browse = Configuration.Default
        //.WithProxyRequester(FlurlClient, new []{ ("Cache-Control", "no-cache"), ("Accept-Language", "en-US")}, Cfg.RequestTimeout, Cfg.ProxyType, log)
        .WithProxyRequester(FlurlClient, Cfg.ProxyType)
        .WithDefaultLoader()
        .WithTemporaryCookies()
        .Browser();

      var refreshAttempts = 0;
      var requestUrl = url;
      while (refreshAttempts < 3) {
        refreshAttempts++;
        var (finished, doc) = await browse.OpenAsync(requestUrl).WithTimeout(Cfg.RequestTimeout);
        if (!finished) return new(item1: null, HttpStatusCode.GatewayTimeout, "request timed out, reating as 504");
        var status = (Code: doc.StatusCode, Msg: ((int) doc.StatusCode).ToString());

        if (doc.Source.Text == "")
          status = (HttpStatusCode.TooManyRequests, "emtpy response, treating as 429");
        if (doc.Head.Children.None())
          status = (HttpStatusCode.TooManyRequests, "response was unable to be decoded, treating as 429");
        else if (doc.El("head > title")?.TextContent == "Sorry! Something went wrong!")
          status = (HttpStatusCode.TooManyRequests, "response was typical for a bot error, treating as 429");

        var refreshContent = doc.El<IHtmlMetaElement>("meta[http-equiv='refresh']")?.Content;
        if (refreshContent == null)
          return (doc, status.Code, status.Msg);

        var split = refreshContent.Split(";");
        requestUrl = split.Skip(1).Select(s => {
          var (name, value, _) = s.UnJoin('=');
          return new {Name = name.Trim(), Value = value.Trim()};
        }).FirstOrDefault(s => s.Name == "url")?.Value ?? doc.Url;
        await 1.Seconds().Delay();
      }
      return (null, HttpStatusCode.NotFound, "tried refreshing too many times, treating as 404");
    }

    public async Task<LoadFromUrlRes> LoadLinkMeta(string url, ILogger log) {
      var (doc, status, statusMsg) = await LoadLink(url);
      AmazonLink r = new() {
        Updated = DateTime.UtcNow,
        SourceUrl = url
      };

      if (!status.IsSuccess()) {
        log.Debug("Amazon - error requesting {Url}: {Error}", doc?.Url ?? url, statusMsg);
        return new(r with {Error = statusMsg}, status, statusMsg);
      }

      var pageType = DetectPageType(doc);
      r = r with {
        Url = doc.Url,
        CanonUrl = doc.El<IHtmlLinkElement>("link[rel='canonical']")?.Href,
        Title = doc.El<IHtmlMetaElement>("meta[name='title']")?.Content.Trim(),
        Description = doc.El<IHtmlMetaElement>("meta[name='description']")?.Content.Trim(),
        PageType = pageType
      };

      if (r.PageType.NotIn(PT.Unknown, PT.Product)) // while we are working out which pages are product, try to detect as product even if unknown
        return new(r);

      r = ParseProduct(r, url, doc);

      if (!r.ProductTitle.HasValue()) {
        var alert = doc.El(".a-icon-alert")?.ParentElement.Text()?.Trim() ?? "missing product title";
        if (alert.Contains("robot"))
          return new(r, HttpStatusCode.TooManyRequests, alert);
        if (r.PageType == AmazonPageType.Product) {
          var outerHtml = doc.ToHtml();
          await LogStore.LogParseError("Error getting amazon product information", new(alert), doc.Url, outerHtml, log);
          return new(r with {Error = alert}, HttpStatusCode.BadRequest, alert);
        }
      }

      log.Debug("Amazon - loaded product {Row}", r.ToJson());
      return new(r);
    }

    static AmazonLink ParseProduct(AmazonLink basic, string url, IDocument doc) {
      static JObject JFromTuples(IEnumerable<(string, string)> props) => new(props
        .NotNull().Where(p => p.Item1.HasValue())
        .Select(p => new JProperty(p.Item1.Trim(), p.Item2?.Trim()))
        .GroupBy(p => p.Name).Select(g => g.First())); // take first of same property name

      return basic with {
        SourceUrl = url,
        Url = doc.Url,
        CanonUrl = doc.El<IHtmlLinkElement>("link[rel='canonical']")?.Href,
        Title = doc.El<IHtmlMetaElement>("meta[name='title']")?.Content.Trim(),
        Description = doc.El<IHtmlMetaElement>("meta[name='description']")?.Content.Trim(),
        ProductTitle = doc.El("#productTitle")?.Text().Trim(),
        Breadcrumb = doc.Els<IHtmlAnchorElement>("#wayfinding-breadcrumbs_container a")
          .Select(e => new AmazonA(e.Text?.Trim(), e.Href)).ToArray(),
        //Price = doc.El("#priceblock_ourprice")?.Text().TryParseDecimal(), // pricing will take some work 
        CreativeAsin = doc.Url.AsUrl().QueryParams.Str("creativeASIN"),
        BiLine = doc.El<IHtmlAnchorElement>("a#bylineInfo").Do(a => new AmazonA(a.Text, a.Href)),
        ImageUrl = doc.El<IHtmlImageElement>("#main-image-container img").Do(img => img.GetAttribute("data-old-hires").NullIfEmpty() ?? img.Source),
        Props = JFromTuples(doc.El<IHtmlTableElement>("table.prodDetTable")
            ?.Rows.Select(row => (row.El(".prodDetSectionEntry")?.Text(), row.El(".prodDetAttrValue")?.Text() ?? row.El("td")?.Text()))
          ?? doc.Els("#detailBullets_feature_div > ul span.a-list-item").Select(e => {
            var (nameEl, valueEl, _) = e.Els("span").ToArray();
            return nameEl == null ? default : (nameEl.Text().Trim(':', '\n'), valueEl?.Text());
          }))
      };
    }

    static readonly (string Selector, AmazonPageType PageType)[] SelectorsToPageTypes = {
      ("div#authorPageContainer", PT.Author),
      ("div#search", PT.Search),
      ("div.a-page > .av-page-desktop", PT.Prime),
      ("div#dp", PT.Product)
    };

    static AmazonPageType DetectPageType(IDocument doc) {
      var url = doc.Url.AsUrl();
      var p = url.PathSegments;
      if (p.None()) return PT.Home;
      var pathType = p.FirstOrDefault() switch {
        "shop" => PT.Store,
        "gp" => PT.Product,
        "live" => PT.Live,
        "stores" => PT.Store,
        _ => default
      };
      // sometimes a search url a sub-path of product
      if (pathType.In(PT.Unknown, PT.Product)) {
        if (p.Contains("search")) return PT.Search;
        if (p.Contains("wishlist")) return PT.Wishlist;
      }
      if (pathType == default)
        if (p.Contains("audible"))
          return PT.Audible;
      if (pathType != PT.Unknown) return pathType;
      return SelectorsToPageTypes.FirstOrDefault(s => doc.El(s.Selector) != null).PageType;
    }
  }

  public record AmazonA(string Txt, string Url);

  /// <summary>The typeof page detected to extract metadata from it</summary>
  public enum AmazonPageType {
    Unknown,
    Product,
    Store,
    Live,
    Author,
    Search,
    Audible,
    Home,
    Wishlist,
    Prime
  }

  public record AmazonLink : IHasUpdated {
    public DateTime       Updated   { get; init; }
    public string         SourceUrl { get; init; }
    public AmazonPageType PageType  { get; init; }

    public string    CanonUrl     { get; init; }
    public string    Title        { get; init; }
    public string    Description  { get; init; }
    public string    ProductTitle { get; init; }
    public AmazonA[] Breadcrumb   { get; init; }
    public string    Url          { get; init; }
    public string    CreativeAsin { get; init; }
    public AmazonA   BiLine       { get; init; }
    public string    ImageUrl     { get; init; }
    public string    Error        { get; init; }
    public JObject   Props        { get; init; }
  }
}