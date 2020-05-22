using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Humanizer;
using NUnit.Framework;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;

namespace Tests {
  public class BlockTest {
    
    static IEnumerable<Item> MakeItems(int number, ILogger log) {
      foreach (var item in 0.RangeTo(number).Select(i => new Item { Id = Guid.NewGuid().ToShortString(), Num = Rand.Next()}))
        yield return item;
      log.Information("MakeItems completed");
    }

    [Test]
    public static async Task TestBatchBlock() {
      var log = Setup.CreateTestLogger();
      log.Information("TestBatchBlock started");
      var numItems = 10_000_000;
      var (res, dur) = await MakeItems(numItems, log)
        .BlockBatch(async (b, i) => {
          await 1.Seconds().Delay();
          log.Debug("batch {Batch} processed", i);
          return b.Length;
        }, 100_000, 8).WithDuration();
      log.Information("Processing {Items} took {Duration} {Speed}", 
        numItems, dur.HumanizeShort(), (numItems/1000).Speed("K items", dur).Humanize());
      res.Sum().Should().Be(numItems);
    }

    static readonly Random Rand = new Random();

    class Item {
      public string Id { get; set; }
      public int Num { get; set; }
    }
  }
}