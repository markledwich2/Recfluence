using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using CliFx.Exceptions;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json;
using Polly;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using YtReader.Yt;

namespace YtReader {
  public class UserScrapeCfg {
    public ContainerCfg Container { get; set; } = new() {
      Cores = 1,
      Mem = 6,
      ImageName = "userscrape",
      Exe = "python"
    };

    public int MaxContainers { get; set; } = 20;
    public int SeedsPerTag   { get; set; } = 50;
    public int Tests         { get; set; } = 100;
    public int Retries       { get; set; } = 6;
  }

  public class UserScrape {
    readonly AzureContainers  Containers;
    readonly RootCfg          RootCfg;
    readonly UserScrapeCfg    Cfg;
    readonly SemVersion       Version;
    readonly ISimpleFileStore Store;

    public UserScrape(AzureContainers containers, RootCfg rootCfg, UserScrapeCfg cfg, SemVersion version, YtStore store) {
      Containers = containers;
      RootCfg = rootCfg;
      Cfg = cfg;
      Version = version;
      Store = store.Store;
    }

    public async Task Run(ILogger log, bool init, string trial, string[] limitAccounts, CancellationToken cancel) {
      // var storage = CloudStorageAccount.Parse(RootCfg.AppStoreCs);
      var client = new BlobServiceClient(RootCfg.AppStoreCs);
      var container = client.GetBlobContainerClient(Setup.CfgContainer);

      async Task<BlobClient> CfgBlob() {
        var branchBlob = Version.Prerelease.HasValue() ? container.GetBlobClient($"userscrape-{Version.Prerelease}.json") : null;
        var standardBlob = container.GetBlobClient("userscrape.json");
        return branchBlob != null && await branchBlob.ExistsAsync() ? branchBlob : standardBlob;
      }

      // use branch env cfg if it exists
      var cfgBlob = await CfgBlob();
      var sas = cfgBlob.GenerateSasUri(new(BlobContainerSasPermissions.Read, DateTimeOffset.UtcNow.AddDays(2)));
      var usCfg = (await cfgBlob.LoadAsText()).ParseJObject();
      var cfgAccounts = usCfg.SelectTokens("$.users[*]")
        .Select(t => t.Value<string>("tag")).ToArray();
      var accounts = cfgAccounts.Where(c => limitAccounts == null || limitAccounts.Contains(c)).ToArray();
      var env = new (string name, string value)[] {
        ("cfg_sas", $"{cfgBlob.Uri}{sas}"),
        ("env", RootCfg.Env),
        ("branch_env", Version.Prerelease)
      };
      var args = new[] {"app.py"};
      if (init)
        args = args.Concat("-i").ToArray();


      if (trial.HasValue()) {
        await RunTrial(cancel, trial, env, args, null, log);
      }
      else {
        var blobs = await Store.List("userscrape/run/incomplete_trial", allDirectories: false, log).SelectManyList();
        var incompleteTrials = (await blobs.BlockFunc(f => Store.Get<IncompleteTrial>(f.Path.WithoutExtension(), zip: false)))
          .Where(t => limitAccounts == null || limitAccounts.Any(a => t.accounts?.Contains(a) == true)).ToArray();

        log.Information("UserScrape - about to run {Trials} incomplete trials", incompleteTrials.Length);
        await incompleteTrials.BlockAction(async incompleteTrial => { await RunTrial(cancel, incompleteTrial.trial_id, env, args, null, log); },
          Cfg.MaxContainers, cancel: cancel);

        log.Information("UserScrape - about to new trails for accounts {Accounts}", accounts.Join("|"));
        await accounts.Batch(batchSize: 1, maxBatches: Cfg.MaxContainers)
          .BlockAction(async b => {
            trial = $"{DateTime.UtcNow:yyyy-MM-dd_HH-mm-ss}_{Guid.NewGuid().ToShortString(4)}";
            await RunTrial(cancel, trial, env, args, b, log);
          }, Cfg.MaxContainers, cancel: cancel);
      }
    }

    async Task RunTrial(CancellationToken cancel, string trial, (string name, string value)[] env, string[] args,
      IReadOnlyCollection<string> accounts, ILogger log) {
      var fullName = Cfg.Container.FullContainerImageName("latest");

      var trialLog = log.ForContext("Trail", trial);
      await Policy.Handle<CommandException>().RetryAsync(Cfg.Retries,
          (e, i) => trialLog.Warning(e, "UserScrape - trial {Trial} failed (attempt {Attempt}/{Attempts}): Error: {Error}",
            trial, i, Cfg.Retries, e.Message))
        .ExecuteAsync(async c => {
          var groupName = $"userscrape-{ShortGuid.Create(5).ToLower().Replace(oldChar: '_', newChar: '-')}";
          var groupLog = trialLog.ForContext("ContainerGroup", groupName);
          const string containerName = "userscrape";
          var finalArgs = new List<string>(args);
          if (trial.HasValue()) finalArgs.AddRange("-t", trial);
          if (accounts != null) finalArgs.AddRange("-a", accounts.Join("|"));
          var (group, dur) = await Containers.Launch( // TODO: RunContainer using ContainerLauncher that will also do local docker
            Cfg.Container, groupName, containerName, fullName,
            env,
            finalArgs.ToArray(),
            customRegion: YtCollectorRegion.RandomUsRegion,
            log: groupLog,
            cancel: c
          ).WithDuration();
          await group.EnsureSuccess(containerName, groupLog);
          groupLog.Information("UserScrape - container completed in {Duration}", dur.HumanizeShort());
        }, cancel);
    }

    class IncompleteTrial {
      public string   trial_id { get; set; }
      public string[] accounts { get; set; }
    }

    public async Task UpgradeIncompleteTrials(ILogger log) {
      var inCfg = await Store.List("userscrape/run/cfg", allDirectories: true, log).SelectMany()
        .Where(f => f.Path.Extensions.Last() == "json" && f.Modified > new DateTimeOffset(new DateTime(2020, 9, 17), TimeSpan.Zero))
        .SelectAwait(async f => await Store.Get<IncompleteTrial>(f.Path.WithoutExtension(), zip: false))
        .ToListAsync();

      await inCfg.BlockAction(async cfg => {
        var name = $"userscrape/run/incomplete_trial/{cfg.trial_id}.json";
        var stream = cfg.ToJsonStream(
          new() {Formatting = Formatting.None},
          new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: false)); // python library balks at BOM encoding by default
        await Store.Save(name, stream);
        log.Information("upgraded incomplete trial to {File}", name);
      });
    }
  }
}