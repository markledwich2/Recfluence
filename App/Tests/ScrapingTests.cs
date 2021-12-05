using Autofac;
using FluentAssertions;
using Flurl.Http.Testing;
using LtGt;
using NUnit.Framework;
using SysExtensions.IO;
using YtReader;
using YtReader.Store;
using YtReader.Yt;
using static YtReader.Yt.ExtraPart;

namespace Tests;

public static class YtScrapingTests {
  [Test]
  public static async Task VideoComments() {
    // get comments, does watch page html have it
    using var ctx = await TestSetup.TextCtx();
    var ws = ctx.Scope.Resolve<YtWeb>();
    var video = await ws.GetExtraFromWatchPage(ctx.Log, "NjJ2YEBK3Ic", new[] { EComment }, maxComments: 100);
    await video.Comments.ToJsonl("comments.jsonl");
  }

  [Test]
  public static async Task ExtraPartsCollect() {
    using var ctx = await TestSetup.TextCtx();
    var proxyCfg = ctx.Scope.Resolve<ProxyCfg>() with { AlwaysUseProxy = true };
    var scope = ctx.Scope.BeginLifetimeScope(b => b.RegisterInstance(proxyCfg).SingleInstance());
    var plans = new VideoExtraPlans(new[] {
      "FUqR6KzsPv4", // cnn caption missing
      //"dICrBvTlqsg", // manually translated en-us captions and auto-generated en. should choose en
      //"z-hn5YVi2OE" // indian
      /*"-ryPLVEExA0", // private#1#
      "Su1FQUkMojU", // JP video with lots of comments
      "V8kxdw0UASE", // should work. looks like ti was errored and then re-instated
      "XztR0CnVKNo", // normal
      "JPiiySjShng", //nbc suspected parsing problem
      "OijWK4Y6puI", //unlisted
      "-sc6JCu5rZk",
      "y3oMtX8NyqY", //copyright2
      "EqulyMs_M2M", // copyright1
      "-6oswxLuRyk",
      "tdUxfq6DYXY", // when retreived was var ytInitialData instead of window["ytInitialData"]
      "gRJnTYHID3w", // var ytInitialData instead of window["ytInitialData"]
      "MbXbFchrTgw",
      "rBu0BRTx2x8", // region restricted (not available in AU, but is in US)#3#*/
    }, EExtra, ECaption);
    var collector = scope.Resolve<YtCollector>();
    var extra = await collector.GetExtras(plans, ctx.Log, "").ToListAsync();
  }

  [Test]
  public static async Task ChannelWithExtras() {
    using var ctx = await TestSetup.TextCtx();
    var collector = ctx.Scope.Resolve<YtCollector>();
    var lastUpdate = DateTime.UtcNow - 1.Days();
    var res = await collector.ProcessChannels(
      new ChannelUpdatePlan[] {
        new() {
          Channel = new(Platform.YouTube, "UC7oPkqeHTwuOZ5CZ-R9f-6w"),
          ChannelUpdate = ChannelUpdateType.Full, LastExtraUpdate = lastUpdate, LastCaptionUpdate = lastUpdate
        }
      },
      new[] { EExtra, ECaption, EComment, ERec },
      ctx.Log);
  }

  [Test]
  public static async Task ExtraV2Regression() {
    using var ctx = await TestSetup.TextCtx();
    var videos = new[] {
      "7anY3nc3Scw", // no captions 
      "ETi6oA2GIsg", // with captions
      "-ryPLVEExA0", // private#1#
      "y3oMtX8NyqY", //copyright2
      "rBu0BRTx2x8", // region restricted (not available in AU, but is in US)#3#*/
      "GHDBG0bHJVQ" // random from recent update
    };
    var collector = ctx.Resolve<YtCollector>();
    var extra1 = await collector.GetExtras(new(videos, EExtra, ECaption), ctx.Log).OrderBy(e => e.Extra.VideoId).ToListAsync();
    var j1 = extra1.ToJson();
    var extra2 = await collector.GetExtras(new(videos, EExtra, ECaption, ERec), ctx.Log).OrderBy(e => e.Extra.VideoId).ToListAsync();
    var j2 = extra2.ToJson();
  }

  [Test]
  public static async Task ExtraParts() {
    using var ctx = await TestSetup.TextCtx();
    var scraper = ctx.Scope.Resolve<YtWeb>();
    //Su1FQUkMojU // JP video with comments
    // 1LPpwk2Zb24 // video with new style of comment - i.e. youtubei/next
    // y3oMtX8NyqY // copyright error
    //vZ5L_Fs2zJg missing upload date
    var extra = await scraper.GetExtraFromWatchPage(ctx.Log, "Qt3oQSSZv-o", new[] { EExtra }, maxComments: 100);
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
      "UCKZfi_hHY_mwv7ETUj4s1tw", // unable to parse
      //"UCuxa-jqB3K7vpP-_UOXVrXA", // unable to parse
      //"UCFQ6Gptuq-sLflbJ4YY3Umw",// working
      //"UCOpNcN46UbXVtpKMrmU4Abg" // playlist
      //"UCPCk_8dtVyR1lLHMBEILW4g", // in us restricted audience
      //"UCbCmjCuTUZos6Inko4u57UQ" // upload date to recent
      //"UC4WPFQWSvsDuUKsIjXrgCEw", // can't find browse endpoint
      //"UCROjSBCTEqNRLFeAIAOyWLA" // failed not sure why
      /*"UCdfQFG50Hu88-1CpRmPDA2A", // user channel with pagination of subs
      "UChN7H3JFqeFC-WB8NCxhn7g", // error - unavaialbe
      "UCaJ8FsMMnefU7NXdMaXW8WQ", // error - terminated
      "UCdQ5jrBSBEOUKr91f6zucag", // user*/
      //"UCOpNcN46UbXVtpKMrmU4Abg" // playlist - no channel info
    }.BlockDo(async c => {
      var chan = await ws.Channel(x.Log, c, expectingSubs: true);
      return new {
        Chan = chan,
        Subscriptions = await chan.Subscriptions().ToListAsync(),
        Vids = await chan.Videos().ToListAsync()
      };
    }).ToListAsync();
  }

  [Test]
  public static async Task ApiChannel() {
    using var ctx = await TestSetup.TextCtx();
    var api = ctx.Resolve<YtClient>();
    var data = await new[] { "UCMDxbhGcsE7EnknxPEzC_Iw", "UCHEf6T_gVq4tlW5i91ESiWg", "UCYeF244yNGuFefuFKqxIAXw" }
      .BlockMapList(c => api.ChannelData(c, full: true));
  }

  [Test]
  public static async Task WebChannel() {
    using var ctx = await TestSetup.TextCtx();
    var collector = ctx.Resolve<YtCollector>();
    var data = await new[] { "UCOpNcN46UbXVtpKMrmU4Abg" } // playlist
      .Select(c => new Channel { ChannelId = c })
      .BlockDo(c => collector.GetWebChannel(c, ctx.Log, expectingSubs: true))
      .ToArrayAsync();
  }

  [Test]
  public static async Task TestProxyFallback() {
    using var x = await TestSetup.TextCtx();
    var scraper = x.Scope.Resolve<YtWeb>();
    using var httpTest = new HttpTest();
    var rw = httpTest.ForCallsTo("*youtube.com*").RespondWith("mock too many requests failure", status: 429);
    var getExtra = scraper.GetExtraFromWatchPage(x.Log, "Su1FQUkMojU", new[] { EComment }, maxComments: 100);
    await 5.Seconds().Delay();
    rw.AllowRealHttp();
    var extra = await getExtra; // this should have fallen back to proxy and retried a once or twice in the 5 seconds.
    scraper.Client.UseProxy.Should().Be(true);
  }
}