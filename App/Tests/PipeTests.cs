using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using FluentAssertions;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using NUnit.Framework;
using Serilog;
using SysExtensions;
using YtReader;

namespace Tests {
  public static class PipeTests {
    [Test]
    public static async Task TestPipeApp() {
      var log = Setup.CreateTestLogger();
      var b = new ContainerBuilder();
      b.RegisterType<PipeApp>();
      b.Register(_ => log).As<ILogger>();
      var scope = b.Build();
      var store = new AzureBlobFileStore("UseDevelopmentStorage=true", "pipe", log);
      var pipeCtx = new PipeCtx(new PipeAppCfg(), new PipeAppCtx(scope, typeof(PipeApp)), store, log);
      var res = await pipeCtx.Run((PipeApp app) => app.MakeAndSum(200), location: PipeRunLocation.Local);
      res.Metadata.Error.Should().BeFalse();
    }

    [Test]
    public static void TestContainerStateUnknown() {
      var state = "PendingX".ToEnum<ContainerState>(false);
      state.Should().Be(ContainerState.Unknown);
    }
  }

  public class PipeApp {
    readonly IPipeCtx Ctx;
    readonly ILogger  Log;

    public PipeApp(ILogger log, IPipeCtx ctx) {
      Log = log;
      Ctx = ctx;
    }

    [Pipe]
    public async Task<int[]> MakeAndSum(int size) {
      Log.Information("MakeAndSum Started - {Size}", size);
      var things = 0.RangeTo(size).Select(i => new Thing {Number = i});
      var res = await things.Process(Ctx, b => CountThings(b), new PipeRunCfg {MaxParallel = 2});
      Log.Information("MakeAndSum Complete {Batches}", res.Count);
      return res.Select(r => r.OutState).ToArray();
    }

    [Pipe]
    public Task<int> CountThings(IReadOnlyCollection<Thing> things) {
      Log.Information("Counting things: {Things}", things.Select(t => t.Number));
      return Task.FromResult(things.Sum(t => t.Number));
    }
  }

  public class Thing {
    public int Number { get; set; }
  }
}