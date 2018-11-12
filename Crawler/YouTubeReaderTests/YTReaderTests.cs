using System.Collections.Async;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SysExtensions.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SysExtensions.Text;
using YouTubeReader;
using FluentAssertions;
using SysExtensions.Threading;

namespace YouTubeReaderTests {
    [TestClass]
    public class YTReaderTests {
        [TestMethod]
        public async Task TestList() {
            var log = Setup.CreateLogger();
            var cfg = Setup.LoadCfg(log);
            var store = new YtStore(new YtReader(cfg, log));
            var s3 = new S3Store(cfg.S3, "YouTube");
            var stream = s3.ListKeys("Channels");
            var list = await stream.ToListAsync();
            var count = list.Sum(l => l.Count);
        }
    }
}
