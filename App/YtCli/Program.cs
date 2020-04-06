using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using CommandLine;
using Google.Apis.Util;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Serilog;
using Serilog.Core;
using SysExtensions.Collections;
using SysExtensions.Text;
using Troschuetz.Random;
using YtReader;
using YtReader.YtWebsite;

namespace YtCli {
  class Program {
    static async Task<int> Main(string[] args) {
      var res = Parser.Default
        .ParseArguments<PipeCmd, UpdateCmd, SyncCmd, ChannelInfoOption, FixCmd, ResultsCmd, TrafficCmd,
          PublishContainerCmd>(args)
        .MapResult(
          (PipeCmd p) => Run(p, args, PipeCmd.RunPipe),
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
      var (app, root) = await Setup.LoadCfg2();
      var log = Setup.CreateLogger(Setup.Env, app);
      var scope = Setup.BaseScope(root, app, log);
      return new CmdCtx<TOption>(root, app, log, option, scope, args);
    }

    static async Task<ExitCode> Run<TOption>(TOption option, string[] args, Func<CmdCtx<TOption>, Task<ExitCode>> task) {
      using var ctx = await TaskCtx(option, args);

      try {
        var verb = option.GetType().GetCustomAttribute<VerbAttribute>()?.Name ?? option.GetType().Name;
        ctx.Log.Information("Starting cmd {Command} in {Env} environment", verb, ctx.RootCfg.Env);
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
  public class UpdateCmd : ICommonCmd {
    [Option('c', "channels", HelpText = "optional '|' separated list of channels to process")]
    public string ChannelIds { get; set; }

    [Option('t', "type", HelpText = "Control what parts of the update process to run")]
    public UpdateType UpdateType { get; set; }

    static readonly Region[] Regions = {Region.USEast, Region.USWest, Region.USWest2, Region.USEast2, Region.USSouthCentral};
    static readonly TRandom  Rand    = new TRandom();

    public static async Task<ExitCode> Update(CmdCtx<UpdateCmd> ctx) {
      if (ctx.Option.ChannelIds.HasValue())
        ctx.Cfg.LimitedToSeedChannels = ctx.Option.ChannelIds.UnJoin('|').ToHashSet();

      var pipeCtx = ctx.Scope.ResolvePipeCtx();
      pipeCtx.CustomRegion = () => Rand.Choice(Regions);
      var id = PipeRunId.FromName("Update");
      await pipeCtx.DoPipeWork(id);
      return ExitCode.Success;
    }
  }

  [Verb("fix", HelpText = "try to fix missing/inconsistent data")]
  public class FixCmd : ICommonCmd {
    public static async Task<ExitCode> Fix(CmdCtx<FixCmd> ctx) {
      await new StoreUpgrader(ctx.Cfg, ctx.Cfg.DataStore(ctx.Log), ctx.Log).UpgradeStore();
      return ExitCode.Success;
    }
  }

  [Verb("sync", HelpText = "synchronize two blobs")]
  public class SyncCmd : ICommonCmd {
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
  public class ChannelInfoOption : ICommonCmd {
    [Option('v', Required = true, HelpText = "the ID of a video")]
    public string VideoId { get; set; }

    public static async Task<ExitCode> ChannelInfo(CmdCtx<ChannelInfoOption> ctx) {
      var yt = ctx.Cfg.YtClient(ctx.Log);
      var v = await yt.VideoData(ctx.Option.VideoId);
      ctx.Log.Information("{ChannelId},{ChannelTitle}", v.ChannelId, v.ChannelTitle);
      return ExitCode.Success;
    }
  }

  public enum RunLocation {
    Normal,
    ContainerLaunch,
    ContainerComplete
  }

  public interface ICommonCmd { }

  [Verb("results")]
  public class ResultsCmd : ICommonCmd {
    [Option('q', HelpText = "list of query names to run. All if empty")]
    public IEnumerable<string> QueryNames { get; set; }

    public static async Task<ExitCode> Results(CmdCtx<ResultsCmd> ctx) {
      var store = ctx.Cfg.DataStore(ctx.Log, ctx.Cfg.Storage.ResultsPath);
      var result = new YtResults(ctx.Cfg.Snowflake, ctx.Cfg.Results, store, ctx.Log);
      await result.SaveResults(ctx.Option.QueryNames.NotNull().ToList());
      return ExitCode.Success;
    }
  }

  [Verb("traffic", HelpText = "Process source traffic data for comparison")]
  public class TrafficCmd : ICommonCmd {
    public static async Task<ExitCode> Traffic(CmdCtx<TrafficCmd> ctx) {
      var store = ctx.Cfg.DataStore(ctx.Log, ctx.Cfg.Storage.PrivatePath);
      await TrafficSourceExports.Process(store, ctx.Cfg, new YtScraper(ctx.Cfg.Scraper), ctx.Log);
      return ExitCode.Success;
    }
  }

  public interface ICmdCtx<out TOption> {
    RootCfg        RootCfg { get; }
    AppCfg         Cfg     { get; }
    ILogger        Log     { get; }
    TOption        Option  { get; }
    ILifetimeScope Scope   { get; }
  }

  public class CmdCtx<TOption> : IDisposable, ICmdCtx<TOption> {
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