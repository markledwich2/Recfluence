using System;
using System.Linq;
using System.Threading.Tasks;
using CommandLine;
using Serilog;
using SysExtensions.Text;
using YtReader;

namespace YouTubeCli {
  [Verb("collect", HelpText = "read all channel, video, recommendation data and flatten into parquet files")]
  public class CollectOption : CommonOption { }

  [Verb("update", HelpText = "refresh new data from YouTube and collects it into results")]
  public class UpdateOption : CommonOption { }

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

  [Verb(name: "ChannelInfo", HelpText = "Show channel information (ID,Name) given a video ID")]
  public class ChannelInfoOption : CommonOption {
    [Option('v', Required = true, HelpText = "the ID of a video")]
    public string VideoId { get; set; }
  }

  public abstract class CommonOption {
    [Option('c', "channels", HelpText = "optional '|' separated list of channels to process")]
    public string ChannelIds { get; set; }

    [Option('p', "parallelism", HelpText = "The number of operations to run at once")]
    public int? Parallel { get; set; }

    [Option('z', "cloudinstance", HelpText = "run this command in a container instance")]
    public bool LaunchContainer { get; set; }
  }

  public class TaskCtx<TOption> {
    public Cfg Cfg { get; set; }
    public ILogger Log { get; set; }
    public TOption Option { get; set; }
    public string[] OriginalArgs { get; set; }
  }

  class Program {
    static int Main(string[] args) {
      var res = Parser.Default.ParseArguments<CollectOption, UpdateOption, FixOption, SyncOption, ChannelInfoOption>(args).MapResult(
        (CollectOption c) => Run(c, args, Collect),
        (UpdateOption u) => Run(u, args, Update),
        (FixOption f) => Run(f, args, Fix),
        (SyncOption s) => Run(s, args, Sync),
        (ChannelInfoOption v) => Run(v, args, ChannelInfo),
        errs => (int) ExitCode.UnknownError
      );
      return res;
    }

    static int Run<TOption>(TOption option, string[] args, Func<TaskCtx<TOption>, Task<ExitCode>> task) where TOption : CommonOption {
      var cfg = Setup.LoadCfg().Result;

      if (option.ChannelIds.HasValue())
        cfg.App.LimitedToSeedChannels = option.ChannelIds.UnJoin('|').ToList();
      if (option.Parallel.HasValue)
        cfg.App.Parallel = option.Parallel.Value;

      using (var log = Setup.CreateLogger(cfg.App))
        try {
          if (option.LaunchContainer) {
            YtContainerRunner.Start(log, cfg, args.Where(a => a != "-z").ToArray()).Wait();
            return (int)ExitCode.Success;
          }
          return (int) task(new TaskCtx<TOption> {Cfg = cfg, Log = log, Option = option, OriginalArgs = args}).Result;
        }
        catch (Exception ex) {
          log.Error(ex, "Unhandled error: {Error}", ex.Message);
          return (int) ExitCode.UnknownError;
        }
    }

    static async Task<ExitCode> Collect(TaskCtx<CollectOption> ctx) {
      var cfg = Setup.LoadCfg().Result;
      var ytCollect = new YtCollect(cfg.YtStore(ctx.Log), cfg.DataStore(cfg.App.Storage.AnalysisPath), cfg.App, ctx.Log);
      ytCollect.SaveChannelRelationData().Wait();
      return ExitCode.Success;
    }

    static async Task<ExitCode> Update(TaskCtx<UpdateOption> ctx) {
      var ytStore = ctx.Cfg.YtStore(ctx.Log);
      var ytUpdater = new YtDataUpdater(ytStore, ctx.Cfg.App, ctx.Log);
      await ytUpdater.UpdateData();
      var ytCollect = new YtCollect(ytStore, ctx.Cfg.DataStore(ctx.Cfg.App.Storage.AnalysisPath), ctx.Cfg.App, ctx.Log);
      await ytCollect.SaveChannelRelationData();
      return ExitCode.Success;
    }

    static async Task<ExitCode> Fix(TaskCtx<FixOption> ctx) {
      var ytStore = ctx.Cfg.YtStore(ctx.Log);
      var ytUpdater = new YtDataUpdater(ytStore, ctx.Cfg.App, ctx.Log);
      await ytUpdater.RefreshMissingVideos();
      return ExitCode.Success;
    }

    static async Task<ExitCode> Sync(TaskCtx<SyncOption> ctx) {
      await SyncBlobs.Sync(ctx.Option.CsA, ctx.Option.CsB, ctx.Option.PathA, ctx.Option.PathB, ctx.Cfg.App.Parallel, ctx.Log);
      return ExitCode.Success;
    }

    static async Task<ExitCode> ChannelInfo(TaskCtx<ChannelInfoOption> ctx) {
      var yt = ctx.Cfg.YtClient(ctx.Log);
      var v = await yt.VideoData(ctx.Option.VideoId);
      ctx.Log.Information("{ChannelId},{ChannelTitle}", v.ChannelId, v.ChannelTitle);
      return ExitCode.Success;
    }
  }

  enum ExitCode {
    Success = 0,
    InvalidLogin = 1,
    InvalidFilename = 2,
    UnknownError = 10
  }
}