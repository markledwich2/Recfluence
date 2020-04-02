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
using SysExtensions.Text;
using YtReader;
using YtReader.YtWebsite;

namespace YtCli {


  class Program {
    static async Task<int> Main(string[] args) {
      var res = Parser.Default
        .ParseArguments<PipeCmd, UpdateFleetCmd, UpdateCmd, SyncCmd, ChannelInfoOption, FixCmd, ResultsCmd, TrafficCmd,
          PublishContainerCmd>(args)
        .MapResult(
          (PipeCmd p) => Run(p, args, PipeCmd.RunPipe),
          (UpdateFleetCmd f) => Run(f, args, UpdateFleetCmd.Fleet),
          (UpdateCmd u) => Run(u, args, UpdateCmd.Update),
          (SyncCmd s) => Run(s, args, SyncCmd.Sync),
          (ChannelInfoOption v) => Run(v, args, ChannelInfoOption.ChannelInfo),
          (FixCmd f) => Run(f, args, FixCmd.Fix),
          (ResultsCmd f) => Run(f, args, ResultsCmd.Results),
          (TrafficCmd t) => Run(t, args, TrafficCmd.Traffic),
          (PublishContainerCmd p) => Run(p, args, PublishContainerCmd.PublishContainer),
          errs => Task.FromResult(ExitCode.Error)
        );
      return (int) await res;
    }

    static async Task<CmdCtx<TOption>> TaskCtx<TOption>(TOption option, string[] args) {
      var cfg = await Setup.LoadCfg2();
      var log = Setup.CreateLogger(Setup.Env, cfg.App);
      return new CmdCtx<TOption>(cfg.Root, cfg.App, log, option, BaseScope(cfg.Root, cfg.App, option, log), args);
    }

    class ScopeHolder {
      public IComponentContext Scope { get; set; }
    }

    static ILifetimeScope BaseScope(RootCfg rootCfg, AppCfg cfg, object option, ILogger log) {
      var scopeHolder = new ScopeHolder();
      var b = new ContainerBuilder();
      b.Register(_ => log).SingleInstance();
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => cfg).SingleInstance();
      b.Register(_ => cfg.YtStore(log)).SingleInstance();
      b.Register<Func<Task<DbConnection>>>(_ => async () => (DbConnection) await cfg.Snowflake.OpenConnection());
      b.Register<Func<IPipeCtx>>(c => () => PipeCmd.PipeCtx(option, rootCfg, cfg, scopeHolder.Scope, log));
      b.RegisterType<YtDataUpdater>(); // this will resolve IPipeCtx
      var scope = b.Build().BeginLifetimeScope();
      scopeHolder.Scope = scope;
      return scope;
    }

    static async Task<ExitCode> Run<TOption>(TOption option, string[] args, Func<CmdCtx<TOption>, Task<ExitCode>> task) {
      using var ctx = await TaskCtx(option, args);

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
  }
  
    [Verb("update", HelpText = "refresh new data from YouTube and collects it into results")]
  public class UpdateCmd : CommonCmd {
    [Option('c', "channels", HelpText = "optional '|' separated list of channels to process")]
    public string ChannelIds { get; set; }

    [Option('t', "type", HelpText = "Control what parts of the update process to run")]
    public UpdateType UpdateType { get; set; }
    
    public static async Task<ExitCode> Update(CmdCtx<UpdateCmd> ctx) {
      if (ctx.Option.ChannelIds.HasValue())
        ctx.Cfg.LimitedToSeedChannels = ctx.Option.ChannelIds.UnJoin('|').ToHashSet();
      await ctx.Scope.Resolve<YtDataUpdater>().UpdateData(ctx.Option.UpdateType);
      return ExitCode.Success;
    }
  }

  [Verb("fix", HelpText = "try to fix missing/inconsistent data")]
  public class FixCmd : CommonCmd {
    public static async Task<ExitCode> Fix(CmdCtx<FixCmd> ctx) {
      await new StoreUpgrader(ctx.Cfg, ctx.Cfg.DataStore(), ctx.Log).UpgradeStore();
      return ExitCode.Success;
    }
  }

  [Verb("sync", HelpText = "synchronize two blobs")]
  public class SyncCmd : CommonCmd {
    [Option('a', Required = true, HelpText = "SAS Uri to source storage service a")]
    public Uri SasA { get; set; }

    [Option(Required = true, HelpText = "The path in the form container/dir1/dir2 for a ")]
    public string PathA { get; set; }

    [Option('b', Required = true, HelpText = "SAS Uri destination storage b")]
    public Uri SasB { get; set; }

    [Option(Required = false, HelpText = "The path in the form container/dir1/dir2 for b (if different to a)")]
    public string PathB { get; set; }
    
    public static async Task<ExitCode> Sync(CmdCtx<SyncCmd> ctx) {
      await SyncBlobs.Sync(ctx.Option.SasA, ctx.Option.SasB, ctx.Option.PathA, ctx.Option.PathB, ctx.Cfg.DefaultParallel, ctx.Log);
      return ExitCode.Success;
    }
  }

  [Verb("ChannelInfo", HelpText = "Show channel information (ID,Name) given a video ID")]
  public class ChannelInfoOption : CommonCmd {
    [Option('v', Required = true, HelpText = "the ID of a video")]
    public string VideoId { get; set; }
    
    public static async Task<ExitCode> ChannelInfo(CmdCtx<ChannelInfoOption> ctx) {
      var yt = ctx.Cfg.YtClient(ctx.Log);
      var v = await yt.VideoData(ctx.Option.VideoId);
      ctx.Log.Information("{ChannelId},{ChannelTitle}", v.ChannelId, v.ChannelTitle);
      return ExitCode.Success;
    }
  }

  public interface ICommonOption {
    bool LaunchContainer { get; set; }
  }

  public abstract class CommonCmd : ICommonOption {
    [Option('z', "cloudinstance", HelpText = "run this command in a container instance")]
    public bool LaunchContainer { get; set; }
  }

  [Verb("fleet")]
  public class UpdateFleetCmd : UpdateCmd {
    public static async Task<ExitCode> Fleet(CmdCtx<UpdateFleetCmd> ctx) {
      await YtContainerRunner.StartFleet(ctx.Log, ctx.Cfg, ctx.Option.UpdateType);
      return ExitCode.Success;
    }
  }

  [Verb("results")]
  public class ResultsCmd : CommonCmd {
    [Option('q', HelpText = "list of query names to run. All if empty")]
    public IEnumerable<string> QueryNames { get; set; }
    
    public static async Task<ExitCode> Results(CmdCtx<ResultsCmd> ctx) {
      var store = ctx.Cfg.DataStore(ctx.Cfg.Storage.ResultsPath);
      var result = new YtResults(ctx.Cfg.Snowflake, ctx.Cfg.Results, store, ctx.Log);
      await result.SaveResults(ctx.Option.QueryNames.NotNull().ToList());
      return ExitCode.Success;
    }
  }

  [Verb("traffic", HelpText = "Process source traffic data for comparison")]
  public class TrafficCmd : CommonCmd {
    public static async Task<ExitCode> Traffic(CmdCtx<TrafficCmd> ctx) {
      var store = ctx.Cfg.DataStore(ctx.Cfg.Storage.PrivatePath);
      await TrafficSourceExports.Process(store, ctx.Cfg, new YtScraper(ctx.Cfg.Scraper), ctx.Log);
      return ExitCode.Success;
    }
  }

  public class CmdCtx<TOption> : IDisposable {
    public CmdCtx(RootCfg root, AppCfg cfg, Logger log, TOption option, ILifetimeScope scope, string[] originalArgs) {
      RootCfg = root;
      Cfg = cfg;
      Log = log;
      Option = option;
      Scope = scope;
      OriginalArgs = originalArgs;
    }

    public RootCfg        RootCfg      { get; }
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
}