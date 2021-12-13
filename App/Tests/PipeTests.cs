using Autofac;
using FluentAssertions;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using NUnit.Framework;
using YtReader.Store;

namespace Tests;

public class PipeTests {
  [Test]
  public static void TestPipeArgDeserialization() {
    var jsonV1 = new PipeArgs(new[] { new PipeArg("cancel", ArgMode.Inject, Value: 2) }).ToJson(Pipes.ArgJCfg);
    var jsonV0 = @"{
  ""$type"": ""Mutuo.Etl.Pipe.PipeArg[], Mutuo.Etl"",
  ""$values"": [
    {
      ""$type"": ""Mutuo.Etl.Pipe.PipeArg, Mutuo.Etl"",
      ""name"": ""options"",
      ""value"": {
        ""$type"": ""YtReader.UpdateOptions, YtReader""
      }
    },
    {
      ""$type"": ""Mutuo.Etl.Pipe.PipeArg, Mutuo.Etl"",
      ""name"": ""cancel"",
      ""argMode"": 2
    }
  ]
}".ParseJObject();

    var v2 = Pipes.LoadInArgs(jsonV0);
    v2.Version.Should().Be(PipeArgs.Versions.V1);
    v2.Values.First(v => v.Name == "cancel").ArgMode.Should().Be(ArgMode.Inject);
  }

  [Test]
  public static async Task TestPipeApp() {
    var ctx = await TestSetup.TextCtx();
    var b = new ContainerBuilder();
    b.RegisterType<PipeApp>();
    b.Register(_ => ctx.Log).As<ILogger>();
    var scope = b.Build();
    // relies on a local dev isntance. use vscode to start an Azurite blob service with a container called pipe
    var store = new AzureBlobFileStore("UseDevelopmentStorage=true", "pipe", ctx.Log);
    var pipeCtx = new PipeCtx(new(), new(scope, typeof(PipeApp)), store, ctx.Log);
    var res = await pipeCtx.Run((PipeApp app) => app.MakeAndSum((int)15L, 1.Thousands(), DataStoreType.Results), new() { Location = PipeRunLocation.Local });
    res.Metadata.Error.Should().BeFalse();
  }

  [Test]
  public static void TestContainerStateUnknown() {
    var state = "PendingX".ParseEnum<ContainerState>(false);
    state.Should().Be(ContainerState.Unknown);
  }

  List<Guid> generated;
  string[]   shortened;

  async Task Generate(ILogger log, bool shouldError) {
    if (shouldError) throw new InvalidOperationException("Generate Errored");
    generated = new();
    foreach (var i in 0.RangeTo(5)) {
      await 100.Milliseconds().Delay();
      generated.Add(Guid.NewGuid());
      log.Information("Generated {i}", i);
    }
  }

  [GraphTask(nameof(Generate))]
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
    var ctx = await TestSetup.TextCtx();

    var res = await TaskGraph.FromMethods(
        (l, c) => Shorten(l),
        (l, c) => Generate(l, true),
        (l, c) => NotDependent(l))
      .Run(parallel: 2, ctx.Log, CancellationToken.None);

    var resByName = res.KeyBy(r => r.Name);
    resByName[nameof(Generate)].FinalStatus.Should().Be(GraphTaskStatus.Error);
    resByName[nameof(Shorten)].FinalStatus.Should().Be(GraphTaskStatus.Cancelled);
    resByName[nameof(NotDependent)].FinalStatus.Should().Be(GraphTaskStatus.Success);

    ctx.Log.Information("Res {Res}, Shortened {Values}", res.Join("\n"), shortened);
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
  public async Task<int[]> MakeAndSum(int size, int shift, DataStoreType dataStoreType) {
    Log.Information("MakeAndSum Started - {Size} - Enum Parameter {DataStoreType}", size, dataStoreType);
    var things = (0 + shift).RangeTo(size + shift).Select(i => new Thing { Number = i });
    var res = await things.Pipe(Ctx, b => CountThings(b), new() { MaxParallel = 2 });
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