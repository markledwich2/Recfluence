using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using FluentAssertions;
using Humanizer;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using NUnit.Framework;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader;

namespace Tests {
  public class PipeTests {
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

    List<Guid> generated;
    string[]   shortened;

    async Task Generate(ILogger log, bool shouldError) {
      if (shouldError) throw new InvalidOperationException("Generate Errored");
      generated = new List<Guid>();
      foreach (var i in 0.RangeTo(5)) {
        await 100.Milliseconds().Delay();
        generated.Add(Guid.NewGuid());
        log.Information("Generated {i}", i);
      }
    }

    [DependsOn(nameof(Generate))]
    async Task Shorten(ILogger log) {
      await 100.Milliseconds().Delay();
      shortened = generated.Select(g => g.ToShortString()).ToArray();
      log.Information("Shortened");
    }

    Task NotDependent(ILogger log) {
      log.Information("Not dependent");
      return Task.CompletedTask;
    }

    [Test]
    public async Task TestGraphRunner() {
      using var log = Setup.CreateTestLogger();

      log.Information("hey there");

      var res = await TaskGraph.FromMethods(
          c => Shorten(log),
          c => Generate(log, true),
          c => NotDependent(log))
        .Run(parallel: 2, log, CancellationToken.None);

      var resByName = res.ToKeyedCollection(r => r.Name);
      resByName[nameof(Generate)].FinalStatus.Should().Be(GraphTaskStatus.Error);
      resByName[nameof(Shorten)].FinalStatus.Should().Be(GraphTaskStatus.Cancelled);
      resByName[nameof(NotDependent)].FinalStatus.Should().Be(GraphTaskStatus.Success);

      log.Information("Res {Res}, Shortened {Values}", res.Join("\n"), shortened);
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