using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CliFx;
using CliFx.Attributes;
using CliFx.Exceptions;
using Medallion.Shell;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Build;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;
using YtReader;
using YtReader.BitChute;
using YtReader.Store;
using YtReader.YtApi;
using YtReader.YtWebsite;

namespace YtCli {
  [Command("channel-info", Description = "Show channel information (ID,Name) given a video ID")]
  public class ChannelInfoCmd : ICommand {
    readonly YtClient YtClient;
    readonly ILogger  Log;

    public ChannelInfoCmd(YtClient ytClient, ILogger log) {
      YtClient = ytClient;
      Log = log;
    }

    [CommandOption('v', Description = "the ID of a video")]
    public string VideoId { get; set; }

    [CommandOption('c', Description = "the ID of a channel")]
    public string ChannelId { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) {
      if (VideoId.HasValue()) {
        var v = await YtClient.VideoData(VideoId);
        Log.Information("{ChannelId},{ChannelTitle}", v.ChannelId, v.ChannelTitle);
      }
      if (ChannelId.HasValue()) {
        var c = await YtClient.ChannelData(ChannelId);
        Log.Information("{ChannelTitle}", c.Title);
      }

      if (VideoId.NullOrEmpty() && ChannelId.NullOrEmpty())
        throw new CommandException("you must provide a channel ID or video ID");
    }
  }

  [Command("upgrade-partitions")]
  public class UpgradePartitionsCmd : ICommand {
    readonly BlobStores   Stores;
    readonly ILogger      Log;
    readonly WarehouseCfg Cfg;
    readonly Stage        Stage;
    readonly IPipeCtx     Ctx;

    [CommandOption('d', Description = "| delimited list of dirs that have partitions that are to be removed")]
    public string Dirs { get; set; }

    public UpgradePartitionsCmd(BlobStores stores, ILogger log, WarehouseCfg cfg, Stage stage, IPipeCtx ctx) {
      Stores = stores;
      Log = log;
      Cfg = cfg;
      Stage = stage;
      Ctx = ctx;
    }

    public async ValueTask ExecuteAsync(IConsole console) {
      var includedDirs = Dirs?.UnJoin('|');
      var dirs = new[] {"videos", "recs", "captions"}.Where(d => includedDirs == null || includedDirs.Contains(d));
      var store = Stores.Store(DataStoreType.DbStage);
      foreach (var dir in dirs) {
        Log.Information("upgrade-partitions - {Dir} started", dir);
        var files = await store.Files(dir, allDirectories: true).SelectMany()
          .Where(f => f.Path.Tokens.Count == 3) // only optimise from within partitions
          .ToListAsync();

        var plan = JsonlStoreExtensions.OptimisePlan(dir, files, Cfg.Optimise, Log);

        if (plan.Count < 10) // if the plan is small, run locally, otherwise on many machines
          await store.Optimise(Cfg.Optimise, plan, Log);
        else
          await plan.Process(Ctx,
            b => Stage.ProcessOptimisePlan(b, store, PipeArg.Inject<ILogger>()),
            new() {MaxParallel = 12, MinWorkItems = 1},
            log: Log, cancel: console.GetCancellationToken());
      }
    }
  }

  [Command("results", Description = "Query snowflake to create result data in blob storage for use by other apps and recfluence.net")]
  public class ResultsCmd : ICommand {
    readonly YtResults Results;
    readonly ILogger   Log;

    [CommandOption('q', Description = "| delimited list of query names to run. All if empty")]
    public string QueryNames { get; set; }

    public ResultsCmd(YtResults results, ILogger log) {
      Results = results;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) => await Results.SaveBlobResults(Log, QueryNames?.Split("|").ToArray());
  }

  [Command("stage", Description = "creates/updates the staging data in snowflake from blob storage")]
  public class StageCmd : ICommand {
    readonly Stage   Stage;
    readonly ILogger Log;
    [CommandOption('t', Description = "| delimited list of tables to restrict warehouse update to")]
    public string Tables { get; set; }

    [CommandOption('f', Description = "if true, will clear and load data")]
    public bool FullLoad { get; set; }

    public StageCmd(Stage stage, ILogger log) {
      Stage = stage;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) => await Stage.StageUpdate(Log, FullLoad, Tables?.Split('|').ToArray());
  }

  [Command("traffic", Description = "Process source traffic data for comparison")]
  public class TrafficCmd : ICommand {
    readonly BlobStores Stores;
    readonly WebScraper Scraper;
    readonly ILogger    Log;

    public TrafficCmd(BlobStores stores, WebScraper scraper, ILogger log) {
      Stores = stores;
      Scraper = scraper;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) {
      var privateStore = Stores.Store(DataStoreType.Private);
      await TrafficSourceExports.Process(privateStore, Scraper, Log);
    }
  }

  [Command("backup", Description = "Backup database")]
  public class BackupCmd : ICommand {
    readonly YtBackup Backup;
    readonly ILogger  Log;

    public BackupCmd(YtBackup backup, ILogger log) {
      Backup = backup;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) => await Backup.Backup(Log);
  }

  [Command("clean", Description = "Clean expired resources")]
  public class CleanCmd : ICommand {
    readonly AzureCleaner Cleaner;
    readonly ILogger      Log;

    public CleanCmd(AzureCleaner cleaner, ILogger log) {
      Cleaner = cleaner;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) => await Cleaner.DeleteExpiredResources(Log);
  }

  [Command("create-env", Description = "Create a branch environment for testing")]
  public class CreateEnvCmd : ICommand {
    readonly BranchEnvCreator Creator;
    readonly ILogger          Log;

    [CommandOption('m', Description = "the mode to copy the database Fresh|Clone|CloneBasic")]
    public BranchState Mode { get; set; }

    [CommandOption('p', Description = "| separated list of staging db paths to copy")]
    public string StagePaths { get; set; }

    public CreateEnvCmd(BranchEnvCreator creator, ILogger log) {
      Creator = creator;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) =>
      await Creator.Create(Mode, StagePaths.UnJoin('|'), Log);
  }

  [Command("update", Description = "Update all the data: collect > warehouse > (results, search index, backup etc..)")]
  public record UpdateCmd(YtUpdater Updater, IPipeCtx PipeCtx, YtContainerRunner ContainerRunner, ContainerCfg ContainerCfg, ILogger Log) : ICommand, IContainerCommand {
    [CommandOption('a', Description = "| delimited list of action to run (empty for all)")]
    public string Actions { get; set; }

    [CommandOption('f', Description = "will force a refresh of collect, and full load of staging files + warehouse. Does not impact search")]
    public bool FullLoad { get; set; }

    [CommandOption('t', Description = "| delimited list of tables to restrict updates to")]
    public string Tables { get; set; }

    [CommandOption('s', Description = "| delimited list of staging tables to restrict updates to")]
    public string StageTables { get; set; }

    [CommandOption('r', Description = "| delimited list of query names to restrict results to")]
    public string Results { get; set; }

    [CommandOption('i', Description = "| delimited list of indexes to limit indexing to")]
    public string Indexes { get; set; }

    [CommandOption('c', Description = "| delimited list of channels to collect")]
    public string Channels { get; set; }

    [CommandOption("collect-parts", shortName: 'p', Description = "optional '|' separated list of collect parts to run")]
    public string Parts { get; set; }

    [CommandOption("us-init", Description = "Run userscrape in init mode (additional seed videos)")]
    public bool UserScrapeInit { get; set; }

    [CommandOption("us-trial", Description = "Run userscrape with an existing trial")]
    public string UserScrapeTrial { get; set; }

    [CommandOption("us-account", Description = "Run userscrape with a list of | separated accounts")]
    public string UserScrapeAccounts { get; set; }

    [CommandOption("disable-discover", Description = "when collecting, don't go and find new channels to classify")]
    public bool DisableChannelDiscover { get; set; }

    [CommandOption("search-condition", Description = @"filter for tables when updating search indexes (channel|video|caption). 
(e.g. 'channel:channel_id=2|video:video_field is null:caption:false' ) ")]
    public string SearchConditions { get; set; }

    [CommandOption("search-index", Description = @"| separated list of indexes to update. leave empty for all indexes")]
    public string SearchIndexes { get; set; }

    [CommandOption("collect-videos",
      Description = @"path in the data blob container a file with newline separated video id's. e.g. import/videos/pop_all_1m_plus_last_30.vid_ids.tsv.gz")]
    public string CollectVideos { get; set; }

    [CommandOption("dataform-deps", Description = "when specified, dataform will run with dependencies included", IsRequired = false)]
    public bool DataformDeps { get; set; }
    
    [CommandOption(IContainerCommand.ContainerOption, IsRequired = false)]
    public bool RunOnContainer { get; set; }
    
    [CommandOption("container-tag")]
    public string ContainerTag {get;set;}

    public async ValueTask ExecuteAsync(IConsole console) {
      if (RunOnContainer) {
        var image = ContainerTag.HasValue() ? ContainerCfg.FullContainerImageName(ContainerTag) : null;
        await ContainerRunner.Run("update", image, console.GetCancellationToken());
      } else {
        console.GetCancellationToken().Register(() => Log.Information("Cancellation requested"));
        var options = new UpdateOptions {
          Actions = Actions?.UnJoin('|'),
          Channels = Channels?.UnJoin('|'),
          Parts = Parts?.UnJoin('|').Where(p => p.TryParseEnum<CollectPart>(out _)).Select(p => p.ParseEnum<CollectPart>()).ToArray(),
          StandardParts = Parts?.UnJoin('|').Where(p => p.TryParseEnum<StandardCollectPart>(out _)).Select(p => p.ParseEnum<StandardCollectPart>()).ToArray(),
          Tables = Tables?.UnJoin('|'),
          StageTables = StageTables?.UnJoin('|'),
          Results = Results?.UnJoin('|'),
          Indexes = Indexes?.UnJoin('|'),
          FullLoad = FullLoad,
          DisableChannelDiscover = DisableChannelDiscover,
          SearchConditions = SearchConditions?.UnJoin('|').Select(t => {
            var (index, condition, _) = t.UnJoin(':');
            return (index, condition);
          }).ToArray(),
          SearchIndexes = SearchIndexes?.UnJoin('|'),
          UserScrapeInit = UserScrapeInit,
          UserScrapeTrial = UserScrapeTrial,
          UserScrapeAccounts = UserScrapeAccounts?.UnJoin('|'),
          CollectVideosPath = CollectVideos,
          DataformDeps = DataformDeps
        };
        await Updater.Update(options, console.GetCancellationToken());
      }
    }
  }

  [Command("test-chrome-scraper")]
  public class TestChromeScraperCmd : ICommand {
    readonly ChromeScraper Scraper;
    readonly ILogger       Log;

    public TestChromeScraperCmd(ChromeScraper scraper, ILogger log) {
      Scraper = scraper;
      Log = log;
    }

    [CommandOption('v', Description = "| separated video id's")]
    public string VideoIds { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) {
      var res = await Scraper.GetRecsAndExtra(VideoIds.UnJoin('|'), Log);
      Log.Information("Scraping of {VideoIds} complete", VideoIds, res);
    }
  }

  [Command("build-container")]
  public class BuildContainerCmd : ICommand {
    readonly SemVersion   Version;
    readonly ContainerCfg Cfg;
    readonly ILogger      Log;
    [CommandOption('p', Description = "Publish to registry, otherwise a local build only")]
    public bool PublishToRegistry { get; set; }

    public BuildContainerCmd(SemVersion version, ContainerCfg cfg, ILogger log) {
      Version = version;
      Cfg = cfg;
      Log = log;
    }

    static async Task<Command> RunShell(Shell shell, ILogger log, string cmd, params object[] args) {
      var process = await StartShell(shell, log, cmd, args);
      return await EnsureComplete(cmd, process);
    }

    static async Task<Command> EnsureComplete(string cmd, Command process) {
      var res = await process.Task;
      if (res.Success) return process;
      await Console.Error.WriteLineAsync($"command failed with exit code {res.ExitCode}: {res.StandardError}");
      throw new CommandException($"command ({cmd}) failed");
    }

    static async Task<Command> StartShell(Shell shell, ILogger log, string cmd, params object[] args) {
      log.Information($"Running command: {cmd} {args.Select(a => a.ToString()).Join(" ")}");
      var process = shell.Run(cmd, args);
      await process.StandardOutput.PipeToAsync(Console.Out);
      return process;
    }

    public async ValueTask ExecuteAsync(IConsole console) {
      var sw = Stopwatch.StartNew();
      var slnName = "Recfluence.sln";
      var sln = FPath.Current.ParentWithFile(slnName, true);
      if (sln == null) throw new CommandException($"Can't find {slnName} file to organize build");
      var image = $"{Cfg.Registry}/{Cfg.ImageName}";

      var tagVersions = new List<string> {Version.ToString()};
      if (Version.Prerelease.NullOrEmpty())
        tagVersions.Add("latest");

      Log.Information("Building & publishing container {Image}", image);

      var appDir = sln.FullPath;
      var shell = new Shell(o => o.WorkingDirectory(appDir));
      List<object> args = new() {"build"};
      args.AddRange(tagVersions.SelectMany(t => new[] {"-t", $"{image}:{t}"}));
      args.AddRange("--build-arg", $"SEMVER={Version}", "--build-arg", $"ASSEMBLY_SEMVER={Version.MajorMinorPatch()}", ".");
      await RunShell(shell, Log, "docker", args.ToArray());


      if (PublishToRegistry)
        foreach (var t in tagVersions)
          await RunShell(shell, Log, "docker", "push", $"{image}:{t}");

      Log.Information("Completed building docker image {Image} in {Duration}", image, sw.Elapsed.HumanizeShort());
    }
  }

  [Command("run-container")]
  public record ParlerLoad(Parler Parler, AzureContainers Az, ContainerCfg ContainerCfg, PipeAppCtx Ctx, SemVersion Version, CliEntry Cli,
    ILogger Log) : ICommand {
    
    [CommandOption('z', IsRequired = false)]
    public bool RunOnContainer { get;              set; }
    
    [CommandOption('s')] public string Sets { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) {
      Log.Information("Starting commend: {Command}", "parler-load");
      if (RunOnContainer)
        await Az.RunContainer("parler", ContainerCfg.FullContainerImageName(Version.PipeTag()), Ctx.EnvironmentVariables,
          Cli.Args?.Where(a => a != "-z").ToArray(), "./recfluence", log: Log, cancel: console.GetCancellationToken());
      else
        await Parler.Load(Sets?.UnJoin('|'));
    }
  }



}