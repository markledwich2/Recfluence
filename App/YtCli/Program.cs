using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CommandLine;
using Mutuo.Etl;
using Serilog;
using SysExtensions.Collections;
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

  public abstract class CommonOption {
    [Option('p', "parallelism", HelpText = "The number of operations to run at once")]
    public int? Parallel { get; set; }

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

  [Verb("backfill_video_extra", HelpText = "Process source traffic data for comparison")]
  public class BackfillVideoExtra : CommonOption { }

  public class TaskCtx<TOption> {
    public Cfg      Cfg          { get; set; }
    public ILogger  Log          { get; set; }
    public TOption  Option       { get; set; }
    public string[] OriginalArgs { get; set; }
  }

  class Program {
    static int Main(string[] args) {
      var res = Parser.Default
        .ParseArguments<UpdateOption, FixOption, SyncOption, ChannelInfoOption, UpdateFleetOption, ResultsOption, TrafficOption, BackfillVideoExtra>(args)
        .MapResult(
          (UpdateFleetOption f) => Run(f, args, Fleet),
          (UpdateOption u) => Run(u, args, Update),
          (SyncOption s) => Run(s, args, Sync),
          (ChannelInfoOption v) => Run(v, args, ChannelInfo),
          (FixOption f) => Run(f, args, Fix),
          (ResultsOption f) => Run(f, args, Results),
          (TrafficOption t) => Run(t, args, Traffic),
          (BackfillVideoExtra b) => Run(b, args, BackfillVideoExtra),
          errs => (int) ExitCode.UnknownError
        );
      return res;
    }

    static int Run<TOption>(TOption option, string[] args, Func<TaskCtx<TOption>, Task<ExitCode>> task) where TOption : CommonOption {
      var cfg = Setup.LoadCfg().Result;

      if (option.Parallel.HasValue)
        cfg.App.DefaultParallel = cfg.App.ParallelChannels = option.Parallel.Value;

      using var log = Setup.CreateLogger(cfg.App);
      var envLog = log.ForContext("Env", cfg.Root.Env);
      try {
        if (option.LaunchContainer) {
          YtContainerRunner.Start(envLog, cfg, args.Where(a => a != "-z").ToArray()).Wait();
          return (int) ExitCode.Success;
        }
        return (int) task(new TaskCtx<TOption> {Cfg = cfg, Log = envLog, Option = option, OriginalArgs = args}).Result;
      }
      catch (Exception ex) {
        var flatEx = ex switch {AggregateException a => a.Flatten(), _ => ex};
        envLog.Error(flatEx, "Unhandled error: {Error}", flatEx.Message);
        return (int) ExitCode.UnknownError;
      }
    }

    static async Task<ExitCode> Update(TaskCtx<UpdateOption> ctx) {
      if (ctx.Option.ChannelIds.HasValue())
        ctx.Cfg.App.LimitedToSeedChannels = ctx.Option.ChannelIds.UnJoin('|').ToHashSet();

      var ytStore = ctx.Cfg.YtStore(ctx.Log);
      var ytUpdater = new YtDataUpdater(ytStore, ctx.Cfg.App, ctx.Option.UpdateType, async () => await ctx.Cfg.App.Snowflake.OpenConnection(), ctx.Log);
      await ytUpdater.UpdateData();
      return ExitCode.Success;
    }

    static async Task<ExitCode> BackfillVideoExtra(TaskCtx<BackfillVideoExtra> ctx) {
      var ytStore = ctx.Cfg.YtStore(ctx.Log);
      var ytUpdater = new YtDataUpdater(ytStore, ctx.Cfg.App, UpdateType.All, async () => await ctx.Cfg.App.Snowflake.OpenConnection(), ctx.Log);
      await ytUpdater.BackfillVideoExtra();
      return ExitCode.Success;
    }

    static async Task<ExitCode> Sync(TaskCtx<SyncOption> ctx) {
      await SyncBlobs.Sync(ctx.Option.CsA, ctx.Option.CsB, ctx.Option.PathA, ctx.Option.PathB, ctx.Cfg.App.DefaultParallel, ctx.Log);
      return ExitCode.Success;
    }

    static async Task<ExitCode> ChannelInfo(TaskCtx<ChannelInfoOption> ctx) {
      var yt = ctx.Cfg.YtClient(ctx.Log);
      var v = await yt.VideoData(ctx.Option.VideoId);
      ctx.Log.Information("{ChannelId},{ChannelTitle}", v.ChannelId, v.ChannelTitle);
      return ExitCode.Success;
    }

    static async Task<ExitCode> Fix(TaskCtx<FixOption> ctx) {
      await new StoreUpgrader(ctx.Cfg.App, ctx.Cfg.DataStore(), ctx.Log).UpgradeStore();
      return ExitCode.Success;
    }

    static async Task<ExitCode> Fleet(TaskCtx<UpdateFleetOption> ctx) {
      await YtContainerRunner.StartFleet(ctx.Log, ctx.Cfg, ctx.Option.UpdateType);
      return ExitCode.Success;
    }

    static async Task<ExitCode> Results(TaskCtx<ResultsOption> ctx) {
      var store = ctx.Cfg.DataStore(ctx.Cfg.App.Storage.ResultsPath);
      var result = new YtResults(ctx.Cfg.App.Snowflake, ctx.Cfg.App.Results, store, ctx.Log);
      await result.SaveResults(ctx.Option.QueryNames.NotNull().ToList());
      return ExitCode.Success;
    }

    static async Task<ExitCode> Traffic(TaskCtx<TrafficOption> ctx) {
      var store = ctx.Cfg.DataStore(ctx.Cfg.App.Storage.PrivatePath);
      await TrafficSourceExports.Process(store, ctx.Cfg.App, new YtScraper(ctx.Cfg.App.Scraper), ctx.Log);
      return ExitCode.Success;
    }
  }

  enum ExitCode {
    Success         = 0,
    InvalidLogin    = 1,
    InvalidFilename = 2,
    UnknownError    = 10
  }
}