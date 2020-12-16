using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AngleSharp.Text;
using Autofac;
using CliFx;
using CliFx.Attributes;
using CliFx.Exceptions;
using Medallion.Shell;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Build;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;
using YtReader;
using YtReader.Db;
using YtReader.Store;
using YtReader.YtApi;
using YtReader.YtWebsite;

namespace YtCli {
  class Program {
    public static async Task<int> Main(string[] args) {
      var (cfg, root, version) = await Setup.LoadCfg(rootLogger: Setup.ConsoleLogger());
      using var log = Setup.CreateLogger(root.Env, "Recfluence", version, cfg);
      using var scope = Setup.MainScope(root, cfg, Setup.PipeAppCtxEmptyScope(root, cfg, version.Version), version, log);
      using var cmdScope = scope.BeginLifetimeScope(c => { c.RegisterAssemblyTypes(typeof(Program).Assembly).AssignableTo<ICommand>(); });
      var app = new CliApplicationBuilder()
        .AddCommandsFromThisAssembly()
        .UseTypeActivator(t => cmdScope.Resolve(t))
        .UseTitle("Recfluence")
        .UseVersionText(version.Version.ToString())
        .Build();

      log.Information("Starting cmd (recfluence {Args}) {Env} {Version}", args.Join(" "), root.Env, version.Version);
      var res = await app.RunAsync(args);
      log.Information("Completed cmd (recfluence {Args}) {Env} {Version}", args.Join(" "), root.Env, version.Version);
      return res;
    }
  }

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
    readonly YtStage Stage;
    readonly ILogger Log;
    [CommandOption('t', Description = "| delimited list of tables to restrict warehouse update to")]
    public string Tables { get; set; }

    [CommandOption('f', Description = "if true, will clear and load data")]
    public bool FullLoad { get; set; }

    public StageCmd(YtStage stage, ILogger log) {
      Stage = stage;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) => await Stage.StageUpdate(Log, FullLoad, Tables?.Split('|').ToArray());
  }

  [Command("traffic", Description = "Process source traffic data for comparison")]
  public class TrafficCmd : ICommand {
    readonly YtStores   Stores;
    readonly WebScraper Scraper;
    readonly ILogger    Log;

    public TrafficCmd(YtStores stores, WebScraper scraper, ILogger log) {
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

    [CommandOption('c', Description = "| separated list of staged channels to copy")]
    public string Channels { get; set; }
    
    [CommandOption('p', Description = "| separated list of staging db paths to copy")]
    public string StagePaths { get; set; }

    public CreateEnvCmd(BranchEnvCreator creator, ILogger log) {
      Creator = creator;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) =>
      await Creator.Create(Mode, Channels.UnJoin('|'), StagePaths.UnJoin('|'), Log);
  }

  [Command("update", Description = "Update all the data: collect > warehouse > (results, search index, backup etc..)")]
  public class UpdateCmd : ICommand {
    readonly YtUpdater Updater;
    readonly IPipeCtx  PipeCtx;
    readonly ILogger   Log;

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

    [CommandOption('l', Description = "The location to run the update")]
    public PipeRunLocation Location { get; set; }

    [CommandOption('c', Description = "| delimited list of channels to collect")]
    public string Channels { get; set; }
    
    [CommandOption("collect-parts", shortName: 'p', Description = "optional '|' separated list of parts to run")]
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

    public UpdateCmd(YtUpdater updater, IPipeCtx pipeCtx, ILogger log) {
      Updater = updater;
      PipeCtx = pipeCtx;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) {
      console.GetCancellationToken().Register(() => Log.Information("Cancellation requested"));
      var options = new UpdateOptions {
        Actions = Actions?.UnJoin('|'),
        Channels = Channels?.UnJoin('|'),
        Parts = Parts?.UnJoin('|').Select(p => p.ParseEnum<CollectPart>()).ToArray(),
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
        UserScrapeAccounts = UserScrapeAccounts?.UnJoin('|')
      };

      await PipeCtx.Run((YtUpdater u) => u.Update(options, PipeArg.Inject<CancellationToken>()),
        new PipeRunOptions {Location = Location, Exclusive = true}, Log, console.GetCancellationToken());
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
  
  [Command("sandbox")]
  public class SandboxCmd : ICommand {
    readonly SnowflakeConnectionProvider Sf;
    readonly ILogger                     Log;

    public SandboxCmd(SnowflakeConnectionProvider sf, ILogger log) {
      Sf = sf;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) {
      using var conn = await Sf.OpenConnection(Log);
      //await conn.SetSessionParams((SfParam.ClientTimestampTypeMapping, "TIMESTAMP_NTZ"));
      var res = await conn.Query<DbVideo2>("test", "select video_id, channel_id, updated from video_latest where video_id = '7eRR7j1OCOs'");
    }

    public class DbVideo2 {
      public string VIDEO_ID   { get; set; }
      public string CHANNEL_ID { get; set; }
      public DateTime UPDATED    { get; set; }
    }
  }

  [Command("build-container")]
  public class BuildContainerCmd : ICommand {
    readonly SemVersion   Version;
    readonly ContainerCfg ContainerCfg;
    readonly ILogger      Log;
    [CommandOption('p', Description = "Publish to registry, otherwise a local build only")]
    public bool PublishToRegistry { get; set; }

    public BuildContainerCmd(SemVersion version, ContainerCfg containerCfg, ILogger log) {
      Version = version;
      ContainerCfg = containerCfg;
      Log = log;
    }

    static async Task<Command> RunShell(Shell shell, ILogger log, string cmd, params object[] args) {
      log.Information($"Running command: {cmd} {args.Select(a => a.ToString()).Join(" ")}");
      var process = shell.Run(cmd, args);
      await process.StandardOutput.PipeToAsync(Console.Out);
      var res = await process.Task;
      if (!res.Success) {
        await Console.Error.WriteLineAsync($"command failed with exit code {res.ExitCode}: {res.StandardError}");
        throw new CommandException($"command ({cmd}) failed");
      }
      return process;
    }

    public async ValueTask ExecuteAsync(IConsole console) {
      var sw = Stopwatch.StartNew();
      var slnName = "Recfluence.sln";
      var sln = FPath.Current.ParentWithFile(slnName, true);
      if (sln == null) throw new CommandException($"Can't find {slnName} file to organize build");
      var image = $"{ContainerCfg.Registry}/{ContainerCfg.ImageName}";
      var tagVersion = $"{image}:{Version}";
      var tagLatest = $"{image}:latest";

      Log.Information("Building & publishing container {Image}", image);

      var appDir = sln.FullPath;
      var shell = new Shell(o => o.WorkingDirectory(appDir));
      await RunShell(shell, Log, "docker", "build", "-t", tagVersion, "-t", tagLatest,
        "--build-arg", $"SEMVER={Version}",
        "--build-arg", $"ASSEMBLY_SEMVER={Version.MajorMinorPatch()}",
        ".");

      if (PublishToRegistry)
        await RunShell(shell, Log, "docker", "push", image);

      Log.Information("Completed building docker image {Image} in {Duration}", image, sw.Elapsed.HumanizeShort());
    }
  }
}