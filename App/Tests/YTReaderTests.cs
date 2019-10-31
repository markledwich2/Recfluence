using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Humanizer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Parquet;
using SysExtensions.Threading;
using YtReader;

namespace YouTubeReaderTests {
  [TestClass]
  public class YTReaderTests {
    [TestMethod]
    public async Task TestBlockTransform() {
      var source = Enumerable.Range(1, 10000).ToList();
      var random = new Random();

      var results = await source.BlockTransform2(async l => {
        await Task.Delay(random.NextDouble().Seconds()).ConfigureAwait(false);
        return l + "p";
      }, 80, 
        capacity:120,
        progressUpdate:p => Debug.WriteLine($"completed {p.CompletedTotal}/{source.Count}"), 
        progressPeriod:5.Seconds());
    }
  }
}