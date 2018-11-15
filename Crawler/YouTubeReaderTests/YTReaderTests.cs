using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using YouTubeReader;

namespace YouTubeReaderTests {
    [TestClass]
    public class YTReaderTests {
        [TestMethod]
        public async Task SaveChannelRelationData() {
            var log = Setup.CreateLogger();
            var cfg = Setup.LoadCfg(log);
            var store = new YtStore(new YtReader(cfg, log));
            var analysis = new YtAnaysis(store, cfg, log);
            await analysis.SaveChannelRelationData();
            //var crawler = new YtDataUpdater(store, cfg, log);
            //var seeds = crawler.SeedChannels();
        }
    }
}