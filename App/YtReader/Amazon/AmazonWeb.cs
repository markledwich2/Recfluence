using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using AngleSharp;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using AngleSharp.Io;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Polly;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.IO;
using SysExtensions.Net;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using YtReader.Web;

// ReSharper disable StringLiteralTypo

// ReSharper disable InconsistentNaming

namespace YtReader.Amazon {
  public record AmazonCfg(int WebParallel = 16, int BatchSize = 1000);

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
select m.video_id, u.value:url::string url
from vids m
       join video_latest v on v.video_id=m.video_id
      , table (flatten(matchurls(description))) u
where u.value:host::string like any ('%amazon%', '%amzn%')
"
      }
    };

    public record VideoUrl(string video_id, string url);

    public async Task GetProductLinkInfo(ILogger log, string queryName = null) {
      var sql = @$"with l as ({NamedSql[queryName ?? "Activewear Links"]})
select video_id, url from l
--where not exists (select * from amazon_link_stage...)
";
      using var db = await Conn.Open(log);

      var links = await db.QueryAsync<VideoUrl>("amazon links", sql).ToListAsync();
      await links.Process(Pipe, b => ProcessLinks(b, PipeArg.Inject<ILogger>()));
    }

    public record LoadFromUrlRes(AmazonLink Link, HttpStatusCode Status = HttpStatusCode.OK, string ErrorMsg = null);

    [Pipe]
    public async Task<long> ProcessLinks(IReadOnlyCollection<VideoUrl> urls, ILogger log) =>
      await urls.BlockTrans(async l => {
          var retryTransient = Policy.HandleResult<LoadFromUrlRes>(
            d => { // todo make this re-usable. Needed a higher level than http send for handling bot errors
              if (!d.Status.IsTransientError()) return false;
              if (d.Status == HttpStatusCode.TooManyRequests && !FlurlClient.UseProxy)
                FlurlClient.UseProxy = true;
              log.Debug("Amazon - transient error {@Result}", d);
              return true;
            }).RetryWithBackoff("Amazon - product", retryCount: 5, log: log);
          return await retryTransient.ExecuteAsync(() => LoadLinkMeta(l, log));
        }, Cfg.WebParallel)
        .Batch(Cfg.BatchSize)
        .BlockAction(async (b, i) => { await Store.AmazonLink.Append(b.Select(l => l.Link).NotNull().ToArray()); });

    public async Task<LoadFromUrlRes> LoadLinkMeta(VideoUrl v, ILogger log) {
      var requester = new DefaultHttpRequester($"Recfluence/{Version.Version}") {
        Headers = {
          {"Accept", "text/html"},
          {"Cache-Control", "no-cache"}
        }
      };

      AmazonLink r = new() {
        Updated = DateTime.UtcNow,
        VideoId = v.video_id
      };
      var doc = await Configuration.Default.WithRequester(requester).WithDefaultLoader().WithDefaultCookies().Browser(FlurlClient).OpenAsync(v.url);
      if (!doc.StatusCode.IsSuccess()) {
        log.Warning("AmazonWeb - error requesting {Url}: error", v.url, doc.StatusCode);
        return new(r with {Error = doc.StatusCode.ToString()}, doc.StatusCode);
      }

      var testFile = $"{ShortGuid.Create()}.html".AsPath().InAppData("recfluence");
      testFile.EnsureDirectoryExists();
      using (var fs = testFile.Open(FileMode.CreateNew)) await doc.ToHtmlAsync(fs);

      var url = doc.Url.AsUrl();
      r = r with {
        SourceUrl = v.url,
        Url = doc.Url,
        CanonUrl = doc.El<IHtmlLinkElement>("link[rel='canonical']")?.Href,
        Title = doc.El<IHtmlMetaElement>("meta[name='title']")?.Content.Trim(),
        Description = doc.El<IHtmlMetaElement>("meta[name='description']")?.Content.Trim(),
        LinkType = url.PathSegments.FirstOrDefault() switch {
          "shop" => "shop",
          "gp" => "product",
          _ => null
        }
      };

      if (r.LinkType != null && r.LinkType != "product")
        return new(r);

      r = r with {
        SourceUrl = v.url,
        Url = doc.Url,
        CanonUrl = doc.El<IHtmlLinkElement>("link[rel='canonical']")?.Href,
        Title = doc.El<IHtmlMetaElement>("meta[name='title']")?.Content.Trim(),
        Description = doc.El<IHtmlMetaElement>("meta[name='description']")?.Content.Trim(),
        ProductTitle = doc.El("#productTitle")?.Text().Trim(),
        Breadcrumb = doc.Els<IHtmlAnchorElement>("#wayfinding-breadcrumbs_container a")
          .Select(e => new AmazonA(e.Text?.Trim(), e.Href)).ToArray(),
        Price = doc.El("#priceblock_ourprice")?.Text().TryParseDecimal(),
        CreativeAsin = doc.Url.AsUrl().QueryParams.Str("creativeASIN"),
        BiLine = doc.El<IHtmlAnchorElement>("a#bylineInfo").Do(a => new AmazonA(a.Text, a.Href)),
        ImageUrl = doc.El<IHtmlImageElement>("#main-image-container img")?.GetAttribute("data-old-hires")
      };

      if (!r.ProductTitle.HasValue()) {
        var alert = doc.El(".a-icon-alert")?.ParentElement.Text() ?? "missing product title";
        if (alert.Contains("robot"))
          return new(r, HttpStatusCode.TooManyRequests, alert);
        var outerHtml = doc.ToHtml();
        await LogStore.LogParseError("Error getting amazon product information", new(alert), doc.Url, outerHtml, log);
        return new(r with {Error = alert}, HttpStatusCode.BadRequest, alert);
      }

      log.Debug("Amazon - loaded product {@Product}", r);
      return new(r);
    }
  }

  public record AmazonA(string Txt, string Url);

  public record AmazonLink : IHasUpdated {
    public string   VideoId   { get; init; }
    public DateTime Updated   { get; init; }
    public string   SourceUrl { get; init; }
    public string   LinkType  { get; init; }

    public string    CanonUrl     { get; init; }
    public string    Title        { get; init; }
    public string    Description  { get; init; }
    public string    ProductTitle { get; init; }
    public AmazonA[] Breadcrumb   { get; init; }
    public string    Url          { get; init; }
    public decimal?  Price        { get; init; }
    public string    CreativeAsin { get; init; }
    public AmazonA   BiLine       { get; init; }
    public string    ImageUrl     { get; init; }
    public string    Error        { get; init; }
  }
}