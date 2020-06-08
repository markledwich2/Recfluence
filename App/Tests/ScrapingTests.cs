using System;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Mutuo.Etl.Blob;
using NUnit.Framework;
using SysExtensions;
using SysExtensions.IO;
using SysExtensions.Serialization;
using YtReader;
using YtReader.Store;
using YtReader.YtWebsite;

namespace Tests {
  public static class ScrapingTests {
    [Test]
    public static async Task ScrapeVid() {
      // get comments, does watch page html have it
      var (cfg, rootCfg, version ) = await Setup.LoadCfg(basePath: Setup.SolutionDir.Combine("YtCli").FullPath);
      using var log = Setup.CreateTestLogger();
      var appCtx = Setup.PipeAppCtxEmptyScope(rootCfg, cfg);
      var scope = Setup.MainScope(rootCfg, cfg, appCtx, version, log);
      var chrome = scope.Resolve<ChromeScraper>();
      var web = scope.Resolve<WebScraper>();
      var vids = new[] {"Fay6parYkrw", "KskhAiNJGYI"};
      var webExtras = await web.GetRecsAndExtra(vids, log);
      var chromeExtras = await chrome.GetRecsAndExtra(vids, log);
      var allExtras = chromeExtras.Concat(webExtras).OrderBy(e => e.Extra.VideoId).ToArray();
      var allRecs = YtCollector.ToRecStored(allExtras, DateTime.UtcNow);
      var dir = TestContext.CurrentContext.WorkDirectory.AsPath().Combine(".data");
      var localStore = new LocalSimpleFileStore(dir);
      var recsStore = new JsonlStore<RecStored2>(localStore, "recs", e => e.Updated.FileSafeTimestamp(), log);
      var extraStore = new JsonlStore<VideoExtraStored2>(localStore, "extra", e => e.Updated.FileSafeTimestamp(), log);
      await extraStore.Append(allExtras.Select(e => e.Extra).ToArray());
      await recsStore.Append(allRecs);
    }
  }
}