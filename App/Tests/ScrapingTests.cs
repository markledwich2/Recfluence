using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SysExtensions;
using SysExtensions.IO;
using SysExtensions.Serialization;
using YtReader;
using YtReader.YtWebsite;

namespace Tests {
  public static class ScrapingTests {
    [Test]
    public static async Task ScrapeVid() {
      // get comments, does watch page html have it
      var (cfg, _, _ ) = await Setup.LoadCfg(basePath: Setup.SolutionDir.Combine("YtCli").FullPath);
      using var log = Setup.CreateTestLogger();
      var chrome = new ChromeScraper(cfg.Scraper);
      var web = new WebScraper(cfg.Scraper);
      var vids = new[] {"Fay6parYkrw", "KskhAiNJGYI"};
      var webExtras = await web.GetRecsAndExtra(vids, log);
      var chromeExtras = await chrome.GetRecsAndExtra(vids, log);
      var chromeSansComments = chromeExtras.JsonClone();
      foreach (var e in chromeSansComments) e.Extra.Comments = null;
      var forCompare = chromeSansComments.Concat(webExtras).OrderBy(e => e.Extra.VideoId).ToArray();
      foreach (var c in forCompare) {
        var path = TestContext.CurrentContext.WorkDirectory.AsPath()
          .Combine(".data", $"{c.Extra.VideoId}.{c.Extra.Source.EnumString()}.json").FullPath;
        c.ToJsonFile(path);
      }
    }
  }
}