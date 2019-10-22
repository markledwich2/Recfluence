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
    public async Task OpenVideos() {
      using (var s = File.OpenRead("C:\\Users\\mark\\Downloads\\Videos.0.parquet")) {
        var rows = ParquetConvert.Deserialize<VideoRow>(s);
      }
    }
  }
}