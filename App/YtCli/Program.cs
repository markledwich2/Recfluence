using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using CliFx;
using CliFx.Attributes;
using CliFx.Exceptions;
using Medallion.Shell;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Build;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;
using SysExtensions.Threading;
using Troschuetz.Random;
using YtReader;
using YtReader.Db;
using YtReader.Search;
using YtReader.Store;
using YtReader.YtApi;
using YtReader.YtWebsite;
using DbCaption = YtReader.Db.DbCaption;

namespace YtCli {
  class Program {
    public static async Task<int> Main(string[] args) {
      var (cfg, root, version) = await Setup.LoadCfg(rootLogger: Setup.ConsoleLogger());
      using var log = Setup.CreateLogger(root.Env, null, version, cfg);
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

  [Command("collect", Description = "refresh new data from YouTube and collects it into results")]
  public class CollectCmd : ICommand {
    readonly AppCfg     Cfg;
    readonly PipeAppCtx AppCtx;
    readonly PipeAppCfg PipeAppCfg;
    readonly IPipeCtx   PipeCtx;

    public CollectCmd(AppCfg cfg, PipeAppCtx appCtx, PipeAppCfg pipeAppCfg, IPipeCtx pipeCtx) {
      Cfg = cfg;
      AppCtx = appCtx;
      PipeAppCfg = pipeAppCfg;
      PipeCtx = pipeCtx;
    }

    static readonly Region[] Regions = {Region.USEast, Region.USWest, Region.USWest2, Region.USEast2, Region.USSouthCentral};
    static readonly TRandom  Rand    = new TRandom();

    [CommandOption("channels", shortName: 'c', Description = "optional '|' separated list of channels to process")]
    public string ChannelIds { get; set; }

    [CommandOption("force", 'f', Description = "Force update of channels, so stats are refreshed even if they have been updated recently")]
    public bool ForceUpdate { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) {
      var channels = ChannelIds?.UnJoin('|').ToArray();

      // make a new app context with a custom region defined
      var appCtx = new PipeAppCtx(AppCtx) {CustomRegion = () => Rand.Choice(Regions)};

      // run the work using the pipe entry point, forced to be local
      PipeAppCfg.Location = PipeRunLocation.Local;
      var pipeCtx = new PipeCtx(PipeAppCfg, appCtx, PipeCtx.Store, PipeCtx.Log);
      await pipeCtx.Run((YtCollector d) => d.Collect(PipeArg.Inject<ILogger>(), ForceUpdate, false, channels, PipeArg.Inject<CancellationToken>()));
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

  [Command("sync-db")]
  public class SyncDbCmd : ICommand {
    readonly YtSync    Sync;
    readonly SyncDbCfg Cfg;
    readonly ILogger   Log;

    [CommandOption('t', Description = "list of tables to sync")]
    public IEnumerable<string> Tables { get; set; }

    [CommandOption('l', Description = "limit rows. For Debugging")]
    public int Limit { get; set; }

    [CommandOption('f', Description = "if true, will clear and load data")]
    public bool FullLoad { get; set; }

    public SyncDbCmd(YtSync sync, SyncDbCfg cfg, ILogger log) {
      Sync = sync;
      Cfg = cfg;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) => await Sync.SyncDb(Cfg, Log, Tables.ToReadOnly(), FullLoad, Limit);
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

    [CommandOption('c', Description = "will copy prod data to this new environment, otherwise it will be fresh")]
    public bool CopyProd { get; set; }

    public CreateEnvCmd(BranchEnvCreator creator, ILogger log) {
      Creator = creator;
      Log = log;
    }

    public async ValueTask ExecuteAsync(IConsole console) =>
      await Creator.Create(CopyProd ? BranchState.CopyProd : BranchState.Fresh, Log);
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

    [CommandOption('r', Description = "| delimited list of query names to restrict results to")]
    public string Results { get; set; }

    [CommandOption('l', Description = "The location to run the update")]
    public PipeRunLocation Location { get; set; }

    [CommandOption('c', Description = "| delimited list of channels to collect")]
    public string Channels { get; set; }

    [CommandOption("us-init", Description = "Run userscrape in init mode (additional seed videos)")]
    public bool UserScrapeInit { get; set; }

    [CommandOption("disable-discover", Description = "when collecting, don't go and find new channels to classify")]
    public bool DisableChannelDiscover { get; set; }

    [CommandOption("search-conditions", Description = @"filter for tables when updating search indexes (channel|video|caption). 
(e.g. 'channel:channel_id=2|video:video_field is null:caption:false' ) ")]
    public string SearchConditions { get; set; }

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
        Tables = Tables?.UnJoin('|'),
        Results = Results?.UnJoin('|'),
        FullLoad = FullLoad,
        DisableChannelDiscover = DisableChannelDiscover,
        SearchConditions = SearchConditions?.UnJoin('|').Select(t => {
          var (index, condition, _) = t.UnJoin(':');
          return (index.ParseEnum<SearchIndex>(), condition);
        }).ToArray(),
      };

      await PipeCtx.Run((YtUpdater u) => u.Update(options, PipeArg.Inject<CancellationToken>()),
        new PipeRunOptions {Location = Location, Exclusive = true}, Log, console.GetCancellationToken());
    }
  }
  
  [Command("fix-reviews")]
  public class FixReviewsCmd : ICommand {
    readonly SheetsCfg SheetsCfg;
    readonly ILogger   Log;
    readonly YtStore   Store;
    readonly SnowflakeConnectionProvider Sf;
    
    static string[] FixTags = {"Mainstream News", "Politician"};

    public FixReviewsCmd(SheetsCfg sheetsCfg, ILogger log, YtStore store, SnowflakeConnectionProvider sf) {
      SheetsCfg = sheetsCfg;
      Log = log;
      Store = store;
      Sf = sf;
    }

    public async ValueTask ExecuteAsync(IConsole console) {
      using var db = await Sf.OpenConnection(Log);
      var oldTags = (await db.Query<(string channelId, string hardTags)>("channels", "select channel_id, hard_tags from channel_latest"))
        .ToDictionary(c => c.channelId, c => c.hardTags.NullOrEmpty() ? new string[] {} : JArray.Parse(c.hardTags).Values<string>().ToArray() );
      var reviews = (await Store.ChannelReviews.Items().SelectManyList())
        .GroupBy(r => (r.Email, r.ChannelId)).Select(g => g.OrderByDescending(i => i.Updated).First()).ToArray();
      
      var toSave = new List<UserChannelReview>();
      foreach (var r in reviews) {
        var old = oldTags[r.ChannelId];
        if (old == default) continue;
        var toAdd = FixTags.Where(f => old.Contains(f) && !r.SoftTags.Contains(f)).ToArray();
        if (!toAdd.Any()) continue;
        r.SoftTags = r.SoftTags.Concat(toAdd).ToArray();
        r.Updated = DateTime.UtcNow;
        toSave.Add(r);
      }
      await Store.ChannelReviews.Append(toSave);
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