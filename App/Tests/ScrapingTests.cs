using System.Linq;
using System.Threading.Tasks;
using Autofac;
using FluentAssertions;
using Flurl.Http.Testing;
using Humanizer;
using LtGt;
using NUnit.Framework;
using SysExtensions.Collections;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader;
using YtReader.AmazonSite;
using YtReader.BitChute;
using YtReader.Rumble;
using YtReader.Yt;

namespace Tests {
  public static class ScrapingTests {
    [Test]
    public static async Task VideoComments() {
      // get comments, does watch page html have it
      using var ctx = await TestSetup.TextCtx();
      var ws = ctx.Scope.Resolve<YtWeb>();
      var video = await ws.GetExtra(ctx.Log, "NjJ2YEBK3Ic", new[] {ExtraPart.EComment});
      await video.Comments.ToJsonl("comments.jsonl");
    }

    [Test]
    public static async Task WebRecsAndExtra() {
      using var ctx = await TestSetup.TextCtx();
      var plans = new VideoExtraPlans(new[] {
        "Su1FQUkMojU", // JP video with lots of comments
        //"V8kxdw0UASE", // should work. looks like ti was errored and then re-instated
        //"XztR0CnVKNo", // normal
        // "JPiiySjShng", //nbc suspected parsing problem
        //"OijWK4Y6puI", //unlisted
        //"-sc6JCu5rZk",
        //"y3oMtX8NyqY", //copyright2
        //"EqulyMs_M2M", // copyright1
        //"-6oswxLuRyk",
        /*
        "tdUxfq6DYXY", // when retreived was var ytInitialData instead of window["ytInitialData"]
        "gRJnTYHID3w", // var ytInitialData instead of window["ytInitialData"]
        "MbXbFchrTgw",
        "rBu0BRTx2x8", // region restricted (not available in AU, but is in US)*/
        //"-ryPLVEExA0", // private
      });
      var collector = ctx.Scope.Resolve<YtCollector>();
      var extra = await collector.GetExtras(plans, ctx.Log).ToListAsync();
    }

    [Test]
    public static async Task ExtraParts() {
      using var ctx = await TestSetup.TextCtx();
      var scraper = ctx.Scope.Resolve<YtWeb>();
      var extra = await scraper.GetExtra(ctx.Log, "O63NEnuJupU", new[] {ExtraPart.EComment});
    }

    [Test]
    public static async Task WatchPageParsing() {
      using var x = await TestSetup.TextCtx();
      var docs = Setup.SolutionDir.Combine("Tests", "WatchPageHtml")
        .Files("*.html")
        .Select(f => Html.ParseDocument(f.OpenText().ReadToEnd()));
      var scrape = x.Resolve<YtWeb>();
      var clientObjects = docs.Select(d => scrape.GetRecs2(x.Log, d, "(fake video id)")).ToList();
    }

    [Test]
    public static async Task Channels() {
      using var x = await TestSetup.TextCtx();
      var ws = x.Scope.Resolve<YtWeb>();
      var chans = await new[] {
        "UCROjSBCTEqNRLFeAIAOyWLA", // failed not sure why
        /*"UCdfQFG50Hu88-1CpRmPDA2A", // user channel with pagination of subs
        "UChN7H3JFqeFC-WB8NCxhn7g", // error - unavaialbe
        "UCaJ8FsMMnefU7NXdMaXW8WQ", // error - terminated
        "UCdQ5jrBSBEOUKr91f6zucag", // user
        "UCUowFWIWGw6Pv2JqfEj8njQ", // channel*/
      }.BlockMap(async c => {
        var chan = await ws.Channel(x.Log, c);
        return new {
          Chan = chan,
          Subscriptions = await chan.Subscriptions().ToListAsync(),
          Vids = await chan.Videos().ToListAsync()
        };
      }).ToListAsync();
    }

    [Test]
    public static async Task ChannelData() {
      using var ctx = await TestSetup.TextCtx();
      var api = ctx.Resolve<YtClient>();
      var data = await new[] {"UCMDxbhGcsE7EnknxPEzC_Iw", "UCHEf6T_gVq4tlW5i91ESiWg", "UCYeF244yNGuFefuFKqxIAXw"}
        .BlockFunc(c => api.ChannelData(c, full: true));
    }

    [Test]
    public static async Task TestProxyFallback() {
      using var x = await TestSetup.TextCtx();
      var scraper = x.Scope.Resolve<YtWeb>();
      using var httpTest = new HttpTest();
      var rw = httpTest.ForCallsTo("*youtube.com*").RespondWith("mock too many requests failure", status: 429);
      var getExtra = scraper.GetExtra(x.Log, "Su1FQUkMojU", new[] {ExtraPart.EComment});
      await 5.Seconds().Delay();
      rw.AllowRealHttp();
      var extra = await getExtra; // this should have fallen back to proxy and retried a once or twice in the 5 seconds.
      scraper.Client.UseProxy.Should().Be(true);
    }

    [Test]
    public static async Task TestAmazonProduct() {
      using var x = await TestSetup.TextCtx();
      var aw = x.Scope.Resolve<AmazonWeb>();
      aw.FlurlClient.UseProxy = true;
      var links = new[] {
        "https://www.amazon.com/shop/sadiealdis", // unable to be decoded
        //"https://amzn.to/2DGXN96", // empty response
        //"https://amzn.to/39tl3nX", // empty response
        //"https://www.amazon.com/Manfrotto-MKCOMPACTACN-BK-Compact-Action-Tripod/dp/B07JMQJKC8?th=1", // comments leaking into props
        //"https://www.amazon.com/gp/product/B005VYCFXA", // product details in bullet form
        //"https://www.amazon.com/gp/product/B07TC76671",
        //"https://amzn.to/2ZMojrd"
      };
      var completed = await aw.ProcessLinks(links, x.Log, cancel: default);
    }

    [Test]
    public static async Task TestRumbleVideo() {
      using var ctx = await TestSetup.TextCtx();
      var web = ctx.Resolve<RumbleScraper>();
      var video = await web.VideoAndExtra("vhh2sj", ctx.Log);
    }

    [Test]
    public static async Task TestBitChuteVideo() {
      using var ctx = await TestSetup.TextCtx();
      var web = ctx.Resolve<BitChuteScraper>();
      var (video, comments) = await web.VideoAndExtra("4KzcvUhGHFSS", ctx.Log);
    }
  }
}