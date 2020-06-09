using System;
using System.Linq;
using System.Threading.Tasks;
using CliFx.Exceptions;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Polly;
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

    public UserScrape(AzureContainers containers, RootCfg rootCfg, UserScrapeCfg cfg) {
      Containers = containers;
      RootCfg = rootCfg;
      Cfg = cfg;
    }

    public async Task Run(ILogger log, bool init) {
      var storage = CloudStorageAccount.Parse(RootCfg.AppStoreCs);
      var client = new CloudBlobClient(storage.BlobEndpoint, storage.Credentials);
      var container = client.GetContainerReference(Setup.CfgContainer);
      var blob = container.GetBlobReference("userscrape.json");
      var sas = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy {
        SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddDays(2),
        Permissions = SharedAccessBlobPermissions.Read
      });

      var usCfg = (await blob.LoadAsText()).ParseJObject();
      var accounts = usCfg.SelectTokens("$.users[*]")
        .Select(t => t.Value<string>("ideology")).ToArray();

      var fullName = Cfg.Container.FullContainerImageName("latest");
      var env = new (string name, string value)[] {
        ("cfg_sas", $"{blob.Uri}{sas}"),
        ("env", RootCfg.Env)
      };

      var args = new[] {"app.py"};
      if (init)
        args = args.Concat("-i").ToArray();

      await accounts.Batch(batchSize: 1, maxBatches: Cfg.MaxContainers)
        .BlockAction(async b => {
          var trial = $"{DateTime.UtcNow:yyyy-MM-dd_HH-mm-ss}_{Guid.NewGuid().ToShortString(4)}";
          var trialLog = log.ForContext("Trail", trial);

          await Policy.Handle<CommandException>().RetryAsync(3,
              (e, i) => trialLog.Warning(e, "UserScrape - trial {Trial} failed ({Attempt}): Error: {Error}", trial, i, e.Message))
            .ExecuteAsync(async () => {
              trialLog.Debug("UserScrape - launching container");
              var groupName = $"userscrape-{ShortGuid.Create(5).ToLower().Replace(oldChar: '_', newChar: '-')}";
              var (group, dur) = await Containers.Launch(
                Cfg.Container, groupName, fullName,
                env,
                args.Concat("-t", trial, "-a", b.Join("|")).ToArray(),
                log: trialLog
              ).WithDuration();
              await group.EnsureSuccess(groupName, trialLog);
              trialLog.Information("UserScrape - container completed in {Duration}", dur.HumanizeShort());
            });
        }, Containers.AzureCfg.Parallel);
    }

    const int RetryErrorCode = 13;
  }
}