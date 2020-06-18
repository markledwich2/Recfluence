using System;
using System.Threading.Tasks;
using Autofac;
using NUnit.Framework;
using Serilog;
using YtReader;
using YtReader.YtWebsite;

namespace Tests {
  public static class ScrapingTests {
    [Test]
    public static async Task ChromeRecsAndExtra() {
      // get comments, does watch page html have it
      using var ctx = await TextCtx();
      var chrome = ctx.Resolve<ChromeScraper>();
      var vids = new[] {"xxQOtOCbASs"}; //, "BxFHm1tXwlM", "Fay6parYkrw", "KskhAiNJGYI"};
      var chromeExtras = await chrome.GetRecsAndExtra(vids, ctx.Log, proxy: false);
    }

    [Test]
    public static async Task WebRecsAndExtra() {
      using var ctx = await TextCtx();
      var ws = ctx.Scope.Resolve<WebScraper>();
      var extra = await ws.GetRecsAndExtra("DLq1DUcMh1Q", ctx.Log);
    }

    static async Task<TestCtx> TextCtx() {
      var (cfg, rootCfg, version ) = await Setup.LoadCfg(basePath: Setup.SolutionDir.Combine("YtCli").FullPath);
      using var log = Setup.CreateTestLogger();
      log.Debug("Starting {TestName}", nameof(TestContext.Test.Name));
      var appCtx = Setup.PipeAppCtxEmptyScope(rootCfg, cfg);
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