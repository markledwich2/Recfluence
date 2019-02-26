using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Parquet;
using YtReader;

namespace YouTubeReaderTests {
    [TestClass]
    public class YTReaderTests {
        [TestMethod]
        public async Task SaveChannelRelationData() {
            var cfg = await Setup.LoadCfg();
            var log = Setup.CreateTestLogger();

            log.Information("started test");

            var store = new YtStore(new YtClient(cfg.App, log), cfg.DataStore());
            var analysis = new YtCollect(store, cfg.DataStore(cfg.App.Storage.AnalysisPath), cfg.App, log);
            await analysis.SaveChannelRelationData();
        }


        [TestMethod]
        public async Task SaveCaptions() {
            var cfg = await Setup.LoadCfg();
            var log = Setup.CreateTestLogger();

            var videoIds = new[] { "S4hq5uVsb5k" };
            var store = new YtStore(new YtClient(cfg.App, log), cfg.DataStore());

            foreach (var videoId in videoIds) {
                var txt = await store.GetAndUpdateVideoCaptions(videoId);
            }
        }


        [TestMethod]
        public async Task OpenVideos() {
            using (var s = File.OpenRead("C:\\Users\\mark\\Downloads\\Videos.0.parquet")) {
                var rows = ParquetConvert.Deserialize<VideoRow>(s);
            }
        }
    }
}