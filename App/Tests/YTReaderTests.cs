using System.Collections.Async;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Parquet;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
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
      
      var store = new YtStore(new YtClient(cfg.App, log), cfg.DataStore());
      var channelCfg = await cfg.App.LoadChannelConfig();
      
      foreach (var c in channelCfg.Seeds.Randomize()) {
        var existingCaptionIds = (await store.Store.List(StringPath.Relative("VideoCaptions", c.Id)).ToListAsync()).SelectMany()
          .Select(b => b.Path.NameSansExtension).ToHashSet();
        if(existingCaptionIds.Any())
          continue;
        var cvc = await store.ChannelVideosCollection.Get(c.Id);
        var toUpdate = cvc.Vids.OrderByDescending(v => v.PublishedAt).Take(50)
          .Where(v => !existingCaptionIds.Contains(v.VideoId)).ToList();
        await toUpdate.BlockAction(v => store.GetAndUpdateVideoCaptions(c.Id, v.VideoId, log), cfg.App.ParallelCollect);
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