using System;
using System.Threading.Tasks;
using Autofac;
using NUnit.Framework;
using Serilog;
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
      var vids = new[] {"n_vzBGB3F_Y", "xxQOtOCbASs" /* (tall video) */, "DLq1DUcMh1Q", "n_vzBGB3F_Y", "xxQOtOCbASs"};
      var chromeExtras = await chrome.GetRecsAndExtra(vids, ctx.Log);
    }

    [Test]
    public static async Task WebRecsAndExtra() {
      using var ctx = await TextCtx();
      var ws = ctx.Scope.Resolve<WebScraper>();
      var extra = await ws.GetRecsAndExtra(new[] {"n_vzBGB3F_Y", "xxQOtOCbASs" /* (tall video) */, "DLq1DUcMh1Q", "n_vzBGB3F_Y", "xxQOtOCbASs"}, ctx.Log);
    }

    [Test]
    public static async Task ChannelData() {
      using var ctx = await TextCtx();
      var api = ctx.Resolve<YtClient>();
      var data = await new[] {"UCMDxbhGcsE7EnknxPEzC_Iw", "UCHEf6T_gVq4tlW5i91ESiWg", "UCYeF244yNGuFefuFKqxIAXw"}
        .BlockFunc(c => api.ChannelData(c));
    }

    static async Task<TestCtx> TextCtx() {
      var (cfg, rootCfg, version ) = await Setup.LoadCfg(basePath: Setup.SolutionDir.Combine("YtCli").FullPath);
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