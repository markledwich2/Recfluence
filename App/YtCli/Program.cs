using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using CommandLine;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Serilog;
using Serilog.Core;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using YtReader;
using YtReader.YtWebsite;

namespace YouTubeCli {
  [Verb("update", HelpText = "refresh new data from YouTube and collects it into results")]
  public class UpdateOption : CommonOption {
    [Option('c', "channels", HelpText = "optional '|' separated list of channels to process")]
    public string ChannelIds { get; set; }

    [Option('t', "type", HelpText = "Control what parts of the update process to run")]
    public UpdateType UpdateType { get; set; }
  }

  [Verb("fix", HelpText = "try to fix missing/inconsistent data")]
  public class FixOption : CommonOption { }

  [Verb("sync", HelpText = "synchronize two blobs")]
  public class SyncOption : CommonOption {
    [Option('a', Required = true, HelpText = "storage connection string to source storage a")]
    public string CsA { get; set; }

    [Option(Required = true, HelpText = "The path in the form container/dir1/dir2 for a ")]
    public string PathA { get; set; }

    [Option('b', Required = true, HelpText = "storage connection string to destination storage b")]
    public string CsB { get; set; }

    [Option(Required = false, HelpText = "The path in the form container/dir1/dir2 for b (if different to a)")]
    public string PathB { get; set; }
  }

  [Verb("ChannelInfo", HelpText = "Show channel information (ID,Name) given a video ID")]
  public class ChannelInfoOption : CommonOption {
    [Option('v', Required = true, HelpText = "the ID of a video")]
    public string VideoId { get; set; }
  }

  public interface ICommonOption {
    bool LaunchContainer { get; set; }
  }

  public abstract class CommonOption : ICommonOption {
    [Option('z', "cloudinstance", HelpText = "run this command in a container instance")]
    public bool LaunchContainer { get; set; }
  }

  [Verb("fleet")] public class UpdateFleetOption : UpdateOption { }

  [Verb("results")]
  public class ResultsOption : CommonOption {
    [Option('q', HelpText = "list of query names to run. All if empty")]
    public IEnumerable<string> QueryNames { get; set; }
  }

  [Verb("traffic", HelpText = "Process source traffic data for comparison")]
  public class TrafficOption : CommonOption { }

  public class PipeOption : PipeArgs {
    [Option('z', "cloudinstance", HelpText = "run this command in a container instance")]
    public bool LaunchContainer { get; set; }
  }

  public class TaskCtx<TOption> : IDisposable {
    public TaskCtx(AppCfg cfg, Logger log, TOption option, ILifetimeScope scope, string[] originalArgs) {
      Cfg = cfg;
      Log = log;
      Option = option;
      Scope = scope;
      OriginalArgs = originalArgs;
    }

    public AppCfg         Cfg          { get; }
    public ILogger        Log          { get; }
    public TOption        Option       { get; }
    public ILifetimeScope Scope        { get; }
    public string[]       OriginalArgs { get; }

    public void Dispose() {
      (Log as Logger)?.Dispose();
      Scope?.Dispose();
    }
  }

  class Program {
    static async Task<int> Main(string[] args) {
      var res = Parser.Default
        .ParseArguments<PipeOption, UpdateFleetOption, UpdateOption, SyncOption, ChannelInfoOption, FixOption, ResultsOption, TrafficOption>(args)
        .MapResult(
          (PipeOption p) => RunPipe(p, args),
          (UpdateFleetOption f) => Run(f, args, Fleet),
          (UpdateOption u) => Run(u, args, Update),
          (SyncOption s) => Run(s, args, Sync),
          (ChannelInfoOption v) => Run(v, args, ChannelInfo),
          (FixOption f) => Run(f, args, Fix),
          (ResultsOption f) => Run(f, args, Results),
          (TrafficOption t) => Run(t, args, Traffic),
          errs => Task.FromResult(ExitCode.Error)
        );
      return (int) await res;
    }

    static IPipeCtxScoped PipeCtx(object option, AppCfg cfg, IComponentContext scope, ILogger log) {
      var pipe = cfg.Pipe.JsonClone();
      pipe.Container ??= cfg.Container;
      pipe.Store ??= new PipeAppStorageCfg {
        Cs = cfg.Storage.DataStorageCs,
        Path = cfg.Storage.PipePath
      };
      pipe.Azure ??= new PipeAzureCfg {
        ResourceGroup = cfg.ResourceGroup,
        ServicePrincipal = cfg.ServicePrincipal,
        SubscriptionId = cfg.SubscriptionId
      };
      
      var runId = option switch {
        PipeOption p => p.RunId.HasValue() ? PipeRunId.FromString(p.RunId) : PipeRunId.Create(p.Pipe),
        _ => null
      };

      var pipeCtx = Pipes.CreatePipeCtx(pipe, runId, log, scope, new[] {typeof(YtDataUpdater).Assembly});
      return pipeCtx;
    }

    static TaskCtx<TOption> TaskCtx<TOption>(TOption option, string[] args) {
      var cfg = Setup.LoadCfg2();
      var log = Setup.CreateLogger(Setup.Env, cfg);
      return new TaskCtx<TOption>(cfg, log, option, BaseScope(cfg, option, log), args);
    }

    class ScopeHolder {
      public IComponentContext Scope { get; set; }
    }
    
    static ILifetimeScope BaseScope(AppCfg cfg, object option, ILogger log) {
      var scopeHolder = new ScopeHolder();
      var b = new ContainerBuilder();
      b.Register(_ => log).SingleInstance();
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => cfg.YtStore(log)).SingleInstance();
      b.Register<Func<Task<DbConnection>>>(_ => async () => (DbConnection) await cfg.Snowflake.OpenConnection());
      b.Register<Func<IPipeCtxScoped>>(c => () => PipeCtx(option, cfg, scopeHolder.Scope, log));
      b.RegisterType<YtDataUpdater>(); // this will resolve IPipeCtx
      var scope = b.Build().BeginLifetimeScope();
      scopeHolder.Scope = scope;
      return scope;
    }

    static async Task<ExitCode> RunPipe(PipeOption option, string[] args) {
      using var ctx = TaskCtx(option, args);
      var pipeCtx = ctx.Scope.Resolve<Func<IPipeCtxScoped>>()();
      return await pipeCtx.RunPipe();
    }

    static async Task<ExitCode> Run<TOption>(TOption option, string[] args, Func<TaskCtx<TOption>, Task<ExitCode>> task) {
      using var ctx = TaskCtx(option, args);

      try {
        if ((option as ICommonOption)?.LaunchContainer == true) {
          await YtContainerRunner.Start(ctx.Log, ctx.Cfg, args.Where(a => a != "-z").ToArray());
          return ExitCode.Success;
        }
        return await task(ctx);
      }
      catch (Exception ex) {
        var flatEx = ex switch {AggregateException a => a.Flatten(), _ => ex};
        ctx.Log.Error(flatEx, "Unhandled error: {Error}", flatEx.Message);
        return ExitCode.Error;
      }
    }

    static async Task<ExitCode> Update(TaskCtx<UpdateOption> ctx) {
      if (ctx.Option.ChannelIds.HasValue())
        ctx.Cfg.LimitedToSeedChannels = ctx.Option.ChannelIds.UnJoin('|').ToHashSet();
      await ctx.Scope.Resolve<YtDataUpdater>().UpdateData(ctx.Option.UpdateType);
      return ExitCode.Success;
    }

    static async Task<ExitCode> Sync(TaskCtx<SyncOption> ctx) {
      await SyncBlobs.Sync(ctx.Option.CsA, ctx.Option.CsB, ctx.Option.PathA, ctx.Option.PathB, ctx.Cfg.DefaultParallel, ctx.Log);
      return ExitCode.Success;
    }

    static async Task<ExitCode> ChannelInfo(TaskCtx<ChannelInfoOption> ctx) {
      var yt = ctx.Cfg.YtClient(ctx.Log);
      var v = await yt.VideoData(ctx.Option.VideoId);
      ctx.Log.Information("{ChannelId},{ChannelTitle}", v.ChannelId, v.ChannelTitle);
      return ExitCode.Success;
    }

    static async Task<ExitCode> Fix(TaskCtx<FixOption> ctx) {
      await new StoreUpgrader(ctx.Cfg, ctx.Cfg.DataStore(), ctx.Log).UpgradeStore();
      return ExitCode.Success;
    }

    static async Task<ExitCode> Fleet(TaskCtx<UpdateFleetOption> ctx) {
      await YtContainerRunner.StartFleet(ctx.Log, ctx.Cfg, ctx.Option.UpdateType);
      return ExitCode.Success;
    }

    static async Task<ExitCode> Results(TaskCtx<ResultsOption> ctx) {
      var store = ctx.Cfg.DataStore(ctx.Cfg.Storage.ResultsPath);
      var result = new YtResults(ctx.Cfg.Snowflake, ctx.Cfg.Results, store, ctx.Log);
      await result.SaveResults(ctx.Option.QueryNames.NotNull().ToList());
      return ExitCode.Success;
    }

    static async Task<ExitCode> Traffic(TaskCtx<TrafficOption> ctx) {
      var store = ctx.Cfg.DataStore(ctx.Cfg.Storage.PrivatePath);
      await TrafficSourceExports.Process(store, ctx.Cfg, new YtScraper(ctx.Cfg.Scraper), ctx.Log);
      return ExitCode.Success;
    }
  }
}