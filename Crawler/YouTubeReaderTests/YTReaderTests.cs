using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using YouTubeReader;

namespace YouTubeReaderTests {
    [TestClass]
    public class YTReaderTests {
        [TestMethod]
        public async Task SaveChannelRelationData() {
            var cfg = await Setup.LoadCfg();
            var log = Setup.CreateTestLogger();
            
            var store = new YtStore(new YtReader(cfg.App, log), cfg.FileStore());
            var analysis = new YtCollect(store, cfg.FileStore(cfg.App.AnalysisPath), cfg.App, log);
            await analysis.SaveChannelRelationData();
        }
    }
}