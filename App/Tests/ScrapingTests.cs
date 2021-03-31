using System;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using LtGt;
using NUnit.Framework;
using Serilog;
using SysExtensions.IO;
using SysExtensions.Threading;
using YtReader;
using YtReader.YtApi;
using YtReader.YtWebsite;
using static YtReader.YtWebsite.ExtraPart;

namespace Tests {


  public static class ScrapingTests {
    
    [Test]
    public static async Task VideoComments() {
      // get comments, does watch page html have it
      using var ctx = await TestSetup.TextCtx();
      var ws = ctx.Scope.Resolve<YtWeb>();
      var video = await ws.GetVideo(ctx.Log, "Su1FQUkMojU", loadComments: true);
    }
    
    [Test]
    public static async Task WebRecsAndExtra() {
      using var ctx = await TestSetup.TextCtx();
      var ws = ctx.Scope.Resolve<YtWeb>();
      var extra = await ws.GetExtra(new[] {
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
      }, new [] {Comments, Recs}, ctx.Log);
    }

    [Test]
    public static async Task Captions() {
      using var ctx = await TestSetup.TextCtx();
      var scraper = ctx.Scope.Resolve<YtWeb>();
      var tracks = await scraper.GetCaptionTracks("yu_C_K3TuyY", ctx.Log);
      var en = tracks.First(t => t.Language.Code == "en");
      var captions = await scraper.GetClosedCaptionTrackAsync(en, ctx.Log);
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
    public static async Task ChannelVideos() {
      using var x = await TestSetup.TextCtx();
      var ws = x.Scope.Resolve<YtWeb>();
      var res = await ws.ChannelVideos("UChN7H3JFqeFC-WB8NCxhn7g", x.Log).ToListAsync();
    }

    [Test]
    public static async Task ChannelData() {
      using var ctx = await TestSetup.TextCtx();
      var api = ctx.Resolve<YtClient>();
      var data = await new[] {"UCMDxbhGcsE7EnknxPEzC_Iw", "UCHEf6T_gVq4tlW5i91ESiWg", "UCYeF244yNGuFefuFKqxIAXw"}
        .BlockFunc(c => api.ChannelData(c, full: true));
    }
  }
}