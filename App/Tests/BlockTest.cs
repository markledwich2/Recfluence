using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using NUnit.Framework;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using static Tests.TestSetup;

namespace Tests; 

public class BlockTest {
  static readonly Random Rand = new();

  [Test]
  public static async Task TestFlatMap() {
    var ctx = await TextCtx();
    var log = ctx.Log;
    log.Information("TestBatchBlock started");

    async IAsyncEnumerable<string> AsyncItems(int count, string desc) {
      await foreach (var i in Enumerable.Range(start: 0, count).Batch(4).BlockMap(async (b, i) => {
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

#pragma warning disable 1998
  static async IAsyncEnumerable<int> AsyncRange(int count) {
#pragma warning restore 1998
    foreach (var i in Enumerable.Range(start: 0, count)) yield return i;
  }

  [Test]
  public static async Task TestChainedBlocks() {
    using var ctx = await TextCtx();
    var log = ctx.Log;

    var res = await AsyncRange(10).BlockMap(async i => {
        await 1.Seconds().Delay();
        log.Information("{i}a", i);
        if (i == 2) throw new("a block error");
        return $"{i}a";
      }, parallel: 4).BlockMap(async a => {
        await 1.Seconds().Delay();
        log.Information("{a}b", a);
        /*if (a == "20a") {
          log.Information("b is throws error now");
          throw new("b error");
        }*/
        return $"{a}b";
      }, parallel: 4)
      .NotNull()
      //.Batch(10)
      //.Select(g => g.Select(b => $"{b}c").ToArray())
      .BlockDo(async s => {
        await 1.Seconds().Delay();
        log.Information("Batch {Res}", s);
      });
  }

  class Item {
    public string Id  { get; set; }
    public int    Num { get; set; }
  }
}