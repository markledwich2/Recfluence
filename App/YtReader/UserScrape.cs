using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CliFx.Exceptions;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Polly;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
  public class UserScrapeCfg {
    public ContainerCfg Container { get; set; } = new ContainerCfg {
      Cores = 1,
      Mem = 6,
      ImageName = "userscrape",
      Exe = "python"
    };

    public int MaxContainers { get; set; } = 10;
  }

  public class UserScrape {
    readonly AzureContainers Containers;
    readonly RootCfg         RootCfg;
    readonly UserScrapeCfg   Cfg;
    readonly SemVersion Version;

    public UserScrape(AzureContainers containers, RootCfg rootCfg, UserScrapeCfg cfg, SemVersion version) {
      Containers = containers;
      RootCfg = rootCfg;
      Cfg = cfg;
      Version = version;
    }

    public async Task Run(ILogger log, bool init, CancellationToken cancel) {
      var storage = CloudStorageAccount.Parse(RootCfg.AppStoreCs);
      var client = new CloudBlobClient(storage.BlobEndpoint, storage.Credentials);
      var container = client.GetContainerReference(Setup.CfgContainer);

      async Task<CloudBlob> CfgBlob() {
        var branchBlob = Version.Prerelease.HasValue() ? container.GetBlobReference($"userscrape-{Version.Prerelease}.json") : null;
        var standardBlob = container.GetBlobReference("userscrape.json");
        return branchBlob != null && await branchBlob.ExistsAsync() ? branchBlob : standardBlob;
      }

      // use branch env cfg if it exists
      var cfgBlob = await CfgBlob();;
      var sas = cfgBlob.GetSharedAccessSignature(new SharedAccessBlobPolicy {
        SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddDays(2),
        Permissions = SharedAccessBlobPermissions.Read
      });

      var usCfg = (await cfgBlob.LoadAsText()).ParseJObject();
      var accounts = usCfg.SelectTokens("$.users[*]")
        .Select(t => t.Value<string>("ideology")).ToArray();

      var fullName = Cfg.Container.FullContainerImageName("latest");
      var env = new (string name, string value)[] {
        ("cfg_sas", $"{cfgBlob.Uri}{sas}"),
        ("env", RootCfg.Env),
        ("branch_env", Version.Prerelease)
      };

      var args = new[] {"app.py"};
      if (init)
        args = args.Concat("-i").ToArray();

      await accounts.Batch(batchSize: 1, maxBatches: Cfg.MaxContainers)
        .BlockAction(async b => {
          var trial = $"{DateTime.UtcNow:yyyy-MM-dd_HH-mm-ss}_{Guid.NewGuid().ToShortString(4)}";
          var trialLog = log.ForContext("Trail", trial);

          await Policy.Handle<CommandException>().RetryAsync(retryCount: 3,
              (e, i) => trialLog.Warning(e, "UserScrape - trial {Trial} failed ({Attempt}): Error: {Error}", trial, i, e.Message))
            .ExecuteAsync(async c => {
              trialLog.Debug("UserScrape - launching container");
              var groupName = $"userscrape-{ShortGuid.Create(5).ToLower().Replace(oldChar: '_', newChar: '-')}";
              var (group, dur) = await Containers.Launch(
                Cfg.Container, groupName, fullName,
                env,
                args.Concat("-t", trial, "-a", b.Join("|")).ToArray(),
                log: trialLog,
                cancel:c
              ).WithDuration();
              await group.EnsureSuccess(groupName, trialLog);
              trialLog.Information("UserScrape - container completed in {Duration}", dur.HumanizeShort());
            }, cancel);
        }, Containers.AzureCfg.Parallel, cancel:cancel);
    }

    const int RetryErrorCode = 13;
  }
}