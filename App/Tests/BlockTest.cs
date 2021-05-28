using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using NUnit.Framework;
using SysExtensions.Collections;
using SysExtensions.Threading;
using static Tests.TestSetup;
using SysExtensions.Text;

namespace Tests {
  public class BlockTest {
    [Test]
    public static async Task TestFlatMap() {
      var ctx = await TextCtx();
      var log = ctx.Log;
      log.Information("TestBatchBlock started");

      async IAsyncEnumerable<string> AsyncItems(int count, string desc) {
        await foreach (var i in Enumerable.Range(start: 0, count).Batch(4).BlockMap(async (b,i) => {
          await 1.Seconds().Delay();
          if (i == 3 && desc == "a") {
            log.Debug("error thrown");
            throw new("does this stop the thing?");
          }
          return b;
        })) {
          await 1.Seconds().Delay();
          yield return $"{desc} says hello {i.Join(",")}";
        }
      }

      //var listA = await AsyncItems(100, "a").ToListAsync();
      var list = new[] {"a", "b"}.Select(s => AsyncItems(count: 20, s)).ToArray().BlockFlatMap(Task.FromResult, parallel: 4);
      var res = await list.ToListAsync();
    }

    static readonly Random Rand = new ();

    class Item {
      public string Id  { get; set; }
      public int    Num { get; set; }
    }
  }
}