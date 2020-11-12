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

namespace Tests {
  public static class ScrapingTests {
    [Test]
    public static async Task ChromeRecsAndExtra() {
      // get comments, does watch page html have it
      using var ctx = await TextCtx();
      var chrome = ctx.Resolve<ChromeScraper>();
      var vids = new[] {
        "rBu0BRTx2x8", // region restricted (not available in AU, but is in US)
        "-ryPLVEExA0", // private 
        /*"Ms9WOSXU5tY", "n_vzBGB3F_Y",
        "xxQOtOCbASs", // tall
        "DLq1DUcMh1Q"*/
      };
      var chromeExtras = await chrome.GetRecsAndExtra(vids, ctx.Log);
    }

    [Test]
    public static async Task WebRecsAndExtra() {
      using var ctx = await TextCtx();
      var ws = ctx.Scope.Resolve<WebScraper>();
      var extra = await ws.GetRecsAndExtra(new[] {
        "OijWK4Y6puI", //unlisted
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
      }, ctx.Log);
    }

    [Test]
    public static async Task WatchPageParsing() {
      using var x = await TextCtx();
      var docs = Setup.SolutionDir.Combine("Tests", "WatchPageHtml")
        .Files("*.html")
        .Select(f => Html.ParseDocument(f.OpenText().ReadToEnd()));

      var scrape = x.Resolve<WebScraper>();

      var clientObjects = docs.Select(d => scrape.GetRecs2(x.Log, d, "(fake video id)")).ToList();
    }

    [Test]
    public static async Task ChannelVideos() {
      using var x = await TextCtx();
      var ws = x.Scope.Resolve<WebScraper>();
      var res = await ws.GetChannelUploadsAsync("UCl49yaTf95CnN_w-Y3yY5fg", x.Log).ToListAsync();
    }

    [Test]
    public static async Task ChannelData() {
      using var ctx = await TextCtx();
      var api = ctx.Resolve<YtClient>();
      var data = await new[] {"UCMDxbhGcsE7EnknxPEzC_Iw", "UCHEf6T_gVq4tlW5i91ESiWg", "UCYeF244yNGuFefuFKqxIAXw"}
        .BlockFunc(c => api.ChannelData(c, full: true));
    }

    static async Task<TestCtx> TextCtx() {
      var (cfg, rootCfg, version) = await Setup.LoadCfg(basePath: Setup.SolutionDir.Combine("YtCli").FullPath);
      var log = Setup.CreateTestLogger();
      log.Information("Starting {TestName}", TestContext.CurrentContext.Test.Name);
      var appCtx = Setup.PipeAppCtxEmptyScope(rootCfg, cfg, version.Version);
      return new TestCtx {Scope = Setup.MainScope(rootCfg, cfg, appCtx, version, log), Log = log, App = cfg, Root = rootCfg};
    }
  }

  class TestCtx : IDisposable {
    public ILifetimeScope Scope { get; set; }
    public ILogger        Log   { get; set; }
    public AppCfg         App   { get; set; }
    public RootCfg        Root  { get; set; }
    public void Dispose() => Scope?.Dispose();
    public T Resolve<T>() => Scope.Resolve<T>();
  }
}