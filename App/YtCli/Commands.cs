using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using CliFx;
using CliFx.Attributes;
using CliFx.Exceptions;
using CliFx.Infrastructure;
using Humanizer;
using Medallion.Shell;
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
using YtReader;
using YtReader.Airtable;
using YtReader.AmazonSite;
using YtReader.Db;
using YtReader.Reddit;
using YtReader.Search;
using YtReader.SimpleCollect;
using YtReader.Store;
using YtReader.Transcribe;
using YtReader.Yt;
using static YtCli.UpdateCmd;

namespace YtCli {
  [Command("channel-info", Description = "Show channel information (ID,Name) given a video ID")]
  public class ChannelInfoCmd : ICommand {
    readonly ILogger  Log;
    readonly YtClient YtClient;

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

    [CommandOption("mode", Description = "the cleaning behavior. Standard, DeleteCompleted or DeleteAll")]
    public CleanContainerMode Mode { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) => await Cleaner.DeleteExpiredResources(Mode, Log);
  }

  [Command("create-env", Description = "Create a branch environment for testing")]
  public class CreateEnvCmd : ICommand {
    readonly BranchEnvCreator Creator;
    readonly ILogger          Log;

    public CreateEnvCmd(BranchEnvCreator creator, ILogger log) {
      Creator = creator;
      Log = log;
    }

    [CommandOption('m', Description = "the mode to copy the database Fresh|Clone|CloneDb")]
    public BranchState Mode { get; set; }

    [CommandOption('p', Description = "| separated list of staging db paths to copy")]
    public string StagePaths { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) =>
      await Creator.Create(Mode, StagePaths.UnJoin('|'), Log);
  }

  [Command("update", Description = "Update all the data: collect > warehouse > (results, search index, backup etc..)")]
  public record UpdateCmd(YtUpdater Updater, IPipeCtx PipeCtx, YtContainerRunner ContainerRunner, AzureContainers Az, ContainerCfg ContainerCfg, ILogger Log)
    : ContainerCommand(ContainerCfg, ContainerRunner, Log) {
    [CommandOption("actions", shortName: 'a', Description = @"| delimited list of action to run. If not specified all action are run. 
Available actions:  Collect|BitChuteCollect|RumbleCollect|Stage|Dataform|Search|Result|Index|DataScripts", IsRequired = false)]
    public string Actions { get; set; }

    [CommandOption("full", shortName: 'f', Description = "will force a refresh of collect, and full load of staging files + warehouse. Does not impact search")]
    public bool FullLoad { get; set; }

    [CommandOption("dataform-tables", shortName: 'w', Description = "| delimited list of warehouse tables to restrict dataform updates to (e.g. video_latest)")]
    public string WarehouseTables { get; set; }

    [CommandOption("tags", shortName: 't', Description = "| delimited list of tags to restrict updates to. Note: Currently applies only to Index action.")]
    public string Tags { get; set; }

    [CommandOption("staging-tables", shortName: 's', Description = "| delimited list of staging tables to restrict updates to (e.g. video_stage|user_stage)")]
    public string StageTables { get; set; }

    [CommandOption("results", shortName: 'r',
      Description = "| delimited list of query names to restrict results to (e.g. ttube_channels). See YtReader/Store/YtResults.cs for full list")]
    public string Results { get; set; }

    [CommandOption("indexes", shortName: 'i',
      Description = "| delimited list of indexes to limit indexing to (e.g. video_removed). See YtReader/Store/YtIndexResults.cs for full list")]
    public string Indexes { get; set; }

    [CommandOption("channels", shortName: 'c', Description = "| delimited list of channels (source id's) to collect (e.g. UCJm5yR1KFcysl_0I3x-iReg)")]
    public string Channels { get; set; }

    [CommandOption("videos", shortName: 'v', Description = "| delimited list of videos (source id's)")]
    public string Videos { get; set; }

    [CommandOption("collect-parts", shortName: 'p', Description = "| delimited list of collect parts to run (e.g. channel|channel-video|user|extra)")]
    public string Parts { get; set; }

    [CommandOption("extra-parts", shortName: 'e', Description = "| delimited list of extra parts to run (e.g. extra|comment|rec|caption)")]
    public string ExtraParts { get; set; }

    [CommandOption("collect-mode")] public SimpleCollectMode CollectMode { get; set; }

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

    [CommandOption("search-mode")] public SearchMode SearchMode { get; set; }

    [CommandOption("dataform-deps", Description = "when specified, dataform will run with dependencies included", IsRequired = false)]
    public bool DataformDeps { get; set; }

    [CommandOption("ds-run", Description = "a previous runid to run scripts on", IsRequired = false)]
    public string DataScriptsRunId { get; set; }

    [CommandOption("ds-part", Description = "| separated list of data-script parts to collect.")]
    public string DataScriptParts { get; set; }

    [CommandOption("ds-video-view", Description = "a view name to get a custom list of videos to update")]
    public string DataScriptVideosView { get; set; }

    protected override string GroupName => "update";

    protected override async ValueTask ExecuteLocal(IConsole console) {
      var cancel = console.RegisterCancellationHandler();
      cancel.Register(() => Log.Information("Cancellation requested"));
      var options = new UpdateOptions {
        Actions = Actions?.UnJoin('|'),
        Collect = new() {
          LimitChannels = Channels?.UnJoin('|'),
          Parts = ParseEnums<CollectPart>(Parts),
          ExtraParts = ParseEnums<ExtraPart>(ExtraParts),
          CollectMode = CollectMode
        },
        Videos = Videos?.UnJoin('|'),
        StandardParts = ParseEnums<StandardCollectPart>(Parts),
        WarehouseTables = WarehouseTables?.UnJoin('|'),
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
        Tags = Tags?.UnJoin('|'),
        DataformDeps = DataformDeps,
        SearchMode = SearchMode,
        DataScript = new(DataScriptsRunId, DataScriptParts.UnJoin('|').Select(p => p.ToLower()).ToArray(), DataScriptVideosView)
      };
      await Updater.Update(options, cancel);
    }

    public static T[] ParseEnums<T>(string s) where T : Enum =>
      s?.UnJoin('|').Where(p => p.TryParseEnum<T>(out _)).Select(p => p.ParseEnum<T>()).ToArray();
  }

  [Command("collect-list", Description = "Refresh video/channel information for a given list")]
  public record CollectListCmd(CollectList Col, YtContainerRunner ContainerRunner, ContainerCfg ContainerCfg, ILogger Log)
    : ContainerCommand(ContainerCfg, ContainerRunner, Log) {
    [CommandParameter(0, Description = "The type of list to run (i.e. ChannelPath, VideoPath, View, Named)")]
    public CollectFromType Mode { get; set; }

    [CommandParameter(1, Description = @"The path/name of the list.
channel-path: path in the data blob container a file with newline separated channel id's. e.g. import/channels/full_12M_chan_info.txt.gz
video-path: path in the data blob container a file with newline separated channel id's. e.g. import/channels/full_12M_chan_info.txt.gz
view: name of a view in the warehouse that has a column video_id, channel_id videos or channels to collect data for e.g. collect_video_sans_extra
named: name of an sql statement CollectListSql. This will use parameters if specified
 ")]
    public string Value { get; set; }

    [CommandOption("channels", shortName: 'c', IsRequired = false,
      Description = "| delimited list of channels (source id's) to collect. This is an additional filter to the list given")]
    public string Channels { get; set; }

    [CommandOption("parts", shortName: 'p', IsRequired = false, Description = @"| list of parts to collect (e.g. video|channel|channel-video)")]
    public string Parts { get; set; }

    [CommandOption("extra-parts", shortName: 'e', IsRequired = false, Description = @"| delimited list of extra parts to run (e.g. extra|comment|rec|caption)")]
    public string ExtraParts { get; set; }

    [CommandOption("stale-hrs", shortName: 'f', IsRequired = false,
      Description = @"e.g. 24. use when re-running failed lists. Before this period has elapsed, channels/videos won't be updated again")]
    public int? StaleHrs { get; set; }

    [CommandOption("sql-args", shortName: 'a', IsRequired = false,
      Description = "Json object representing params to pass to the query. Check yu query for what it needs. e.g. \"{\"\"older_than_days\"\": 10 }")]
    public string Args { get; set; }

    [CommandOption("platform", Description = "| delimited list of platforms to restrict collection to")]
    public string Platforms { get; set; }

    [CommandOption("limit", shortName: 'l', Description = "Max rows to update in airtable")]
    public int? Limit { get; set; }

    protected override string GroupName => "collect-list";

    protected override async ValueTask ExecuteLocal(IConsole console) {
      var opts = new CollectListOptions {
        CollectFrom = (Mode, Value),
        Parts = ParseEnums<CollectListPart>(Parts),
        ExtraParts = ParseEnums<ExtraPart>(ExtraParts),
        LimitChannels = Channels?.UnJoin('|'),
        StaleAgo = StaleHrs?.Hours() ?? 2.Days(),
        Args = Args.Do(JObject.Parse),
        Platforms = ParseEnums<Platform>(Platforms),
        Limit = Limit
      };
      await Col.Run(opts, Log, console.RegisterCancellationHandler());
    }
  }

  [Command("build-container", Description = "build the recfluence docker container")]
  public class BuildContainerCmd : ICommand {
    readonly ContainerCfg Cfg;
    readonly ILogger      Log;
    readonly SemVersion   Version;

    public BuildContainerCmd(SemVersion version, ContainerCfg cfg, ILogger log) {
      Version = version;
      Cfg = cfg;
      Log = log;
    }

    [CommandOption('p', Description = "Publish to registry, otherwise a local build only")]
    public bool PublishToRegistry { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) {
      var sw = Stopwatch.StartNew();
      var slnName = "Recfluence.sln";
      var sln = FPath.Current.ParentWithFile(slnName, includeIfDir: true);
      if (sln == null) throw new CommandException($"Can't find {slnName} file to organize build");
      var image = $"{Cfg.Registry}/{Cfg.ImageName}";

      var tagVersions = new List<string> {Version.ToString()};
      if (Version.Prerelease.NullOrEmpty())
        tagVersions.Add("latest");

      Log.Information("Building & publishing container {Image}", image);

      var appDir = sln.FullPath;
      var shell = new Shell(o => o.WorkingDirectory(appDir));

      shell.Run("docker", "login", "--username", Cfg.RegistryCreds.Name, "--password", Cfg.RegistryCreds.Secret, Cfg.Registry);
      List<object> args = new() {"build"};
      args.AddRange(tagVersions.SelectMany(t => new[] {"-t", $"{image}:{t}"}));
      args.AddRange("--build-arg", $"SEMVER={Version}", "--build-arg", $"ASSEMBLY_SEMVER={Version.MajorMinorPatch()}", ".");
      await RunShell(shell, Log, "docker", args.ToArray());


      if (PublishToRegistry)
        foreach (var t in tagVersions)
          await RunShell(shell, Log, "docker", "push", $"{image}:{t}");

      Log.Information("Completed building docker image {Image} in {Duration}", image, sw.Elapsed.HumanizeShort());
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
  }

  [Command("pushshift", Description = "Loads data from pushshift. A fee elastic search database for reddit")]
  public record PushshiftCmd(ILogger Log, Pushshift Push) : ICommand {
    public async ValueTask ExecuteAsync(IConsole console) {
      await Push.Process(Log);
      Log.Information("Pulling of posts from pushshift complete");
    }
  }

  [Command("airtable", Description = "Merge rows matching a filter into an airtable sheet for manual labeling")]
  public record AirtableCmd(ILogger Log, AtLabel Covid) : ICommand {
    [CommandParameter(0, Description = "The name of the narrative to sync with airtable. e.g. (Activewear|Vaccine)")]
    public string QueryName { get; set; }

    [CommandParameter(1, Description = "The base id from airtable. To get this, open https://airtable.com/api and select your base")]
    public string Base { get; set; }

    [CommandOption("parts", shortName: 'p', Description = "| separated airtable to updated (Mention|Channel|Video)")]
    public string Parts { get; set; }

    [CommandOption("limit", shortName: 'l', Description = "Max rows to update in airtable")]
    public int? Limit { get; set; }

    [CommandOption("videos", shortName: 'v', Description = "| separated videos to limit update to")]
    public string Videos { get; set; }

    [CommandOption("mode", shortName: 'm', Description = "| separated videos to limit update to")]
    public AtUpdateMode Mode { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) {
      await Covid.MargeIntoAirtable(new(Base, QueryName, Limit, ParseEnums<AtLabelPart>(Parts), Videos?.UnJoin('|'), Mode), Log);
      Log.Information("CovidNarrativeCmd - complete");
    }
  }

  [Command("amazon", Description = "Scrapes amazon product link information")]
  public record AmazonCmd(ILogger Log, AmazonWeb Amazon) : ICommand {
    [CommandOption("query", shortName: 'q', Description = "The name of the query to sync with airtable")]
    public string MentionQuery { get; set; }

    [CommandOption("limit", shortName: 'l', Description = "Max rows to update in airtable")]
    public int? Limit { get; set; }

    [CommandOption("force-local", Description = "if true, won't launch any containers to process")]
    public bool ForceLocal { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) {
      await Amazon.GetProductLinkInfo(Log, console.RegisterCancellationHandler(), MentionQuery, Limit, ForceLocal);
      Log.Information("amazon - complete");
    }
  }

  [Command("transcribe", Description = "download videos and transcribe them. maybe should be part of update cmd once done")]
  public record TranscribeCmd(ILogger Log, Transcriber Transcriber, YtContainerRunner ContainerRunner, ContainerCfg ContainerCfg)
    : ContainerCommand(ContainerCfg, ContainerRunner, Log) {
    [CommandOption("platform")] public Platform? Platform { get; set; }

    [CommandOption("limit", shortName: 'l')]
    public int? Limit { get; set; }

    [CommandOption("query", shortName: 'q')]
    public string QueryName { get; set; }

    [CommandOption("parts", shortName: 'p')]
    public string Parts { get; set; }

    [CommandOption("mode", shortName: 'm', Description = "| separated videos to limit update to")]
    public TranscribeMode Mode { get; set; }

    [CommandOption("source-ids", Description = "| seperated list of video source_id's to update")]
    public string SourceIds { get; set; }

    protected override string GroupName => "transcribe";

    protected override async ValueTask ExecuteLocal(IConsole console) {
      await Transcriber.Transcribe(
        new(Platform, Limit, QueryName, ParseEnums<TranscribeParts>(Parts), Mode, SourceIds?.UnJoin('|')),
        Log, console.RegisterCancellationHandler());
      Log.Information("Completed downloading videos");
    }
  }

  [Command("sync-description", Description = "read descriptions from a dataform project and update tables & views to add those")]
  public record SyncDescription(ILogger Log, DataformDescriptions Desc) : ICommand {
    [CommandOption("tables", shortName: 't', Description = "| separated list of table names to update descriptions for")]
    public string TableNames { get; set; }

    public async ValueTask ExecuteAsync(IConsole console) {
      await console.Output.WriteLineAsync("enter a personal access token for GitHub:");
      var token = await console.Input.ReadLineAsync();
      await Desc.Sync(new() {AccessToken = token, TableNames = TableNames?.UnJoin('|')}, Log).Swallow(e => Log.Error(e, "unhandled error"));
    }
  }
}