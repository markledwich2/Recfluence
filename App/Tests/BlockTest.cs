using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using NUnit.Framework;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Threading;
using static SysExtensions.Threading.Def;
using static Tests.TestSetup;
using SysExtensions.Text;

namespace Tests {
  public class BlockTest {
    static IEnumerable<Item> MakeItems(int number, ILogger log) {
      foreach (var item in 0.RangeTo(number).Select(i => new Item {Id = Guid.NewGuid().ToShortString(), Num = Rand.Next()}))
        yield return item;
      log.Information("MakeItems completed");
    }

    [Test]
    public static async Task TestFlatMap() {
      var ctx = await TextCtx();
      var log = ctx.Log;
      log.Information("TestBatchBlock started");

      async IAsyncEnumerable<string> AsyncItems(int count, string desc) {
        await foreach (var i in Enumerable.Range(start: 0, count).Batch(4).BlockMap(async b => {
          await 1.Seconds().Delay();
          return b;
        })) {
          await 1.Seconds().Delay();
          yield return $"{desc} says hello {i.Join(",")}";
        }
      }
      await foreach (var s in new[] {"a", "b"}.Select(s => AsyncItems(int.MaxValue - 5, s)).ToArray().BlockFlatMap(Task.FromResult, parallel: 4))
        log.Information(s);
    }

    static readonly Random Rand = new ();

    class Item {
      public string Id  { get; set; }
      public int    Num { get; set; }
    }
  }
}