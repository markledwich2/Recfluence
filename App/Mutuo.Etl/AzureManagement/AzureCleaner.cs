using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.DockerRegistry;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Threading;
using static Mutuo.Etl.AzureManagement.CleanContainerMode;

namespace Mutuo.Etl.AzureManagement {
  public enum CleanContainerMode {
    Standard,
    DeleteCompleted,
    DeleteAll
  }

  public class AzureCleaner {
    readonly AzureCleanerCfg Cfg;
    readonly PipeAzureCfg    AzureCfg;
    readonly ContainerCfg    ContainerCfg;
    readonly RegistryClient  RegistryClient;
    readonly ILogger         Log;
    readonly Lazy<IAzure>    Az;

    public AzureCleaner(AzureCleanerCfg cfg, PipeAzureCfg azureCfg, ContainerCfg containerCfg, RegistryClient registryClient, ILogger log) {
      Cfg = cfg;
      AzureCfg = azureCfg;
      ContainerCfg = containerCfg;
      RegistryClient = registryClient;
      Log = log;
      Az = new(azureCfg.GetAzure);
    }

    public static (string key, string value) ExpireTag(DateTime utcDate) => ("expire", utcDate.ToString("o", DateTimeFormatInfo.InvariantInfo));

    public async Task DeleteExpiredResources(CleanContainerMode mode = Standard, ILogger log = null) {
      log ??= Log;
      var az = Az.Value;
      await DelContainerImages(log);
      await DelContainerGroups(az, mode, log);
      // no need to do this for blobs. They support setting policies for expiry. 
    }

    async Task DelContainerImages(ILogger log) {
      var catalogs = await RegistryClient.Catalogs();
      foreach (var name in catalogs) {
        var tags = await RegistryClient.TagList(name);
        var images = (await tags.Tags
            .Where(t => t.Contains("-"))
            .BlockFunc(async tag => {
              var manifest = await RegistryClient.Manifest(name, tag);
              var created = manifest.TagCreated();
              return (tag, manifest, created);
            }, Cfg.Parallel))
          .NotNull().ToArray();

        var expired = images.Where(i => DateTime.UtcNow - i.created > Cfg.Expires).ToArray();

        await expired
          .BlockAction(async t => {
            var digest = await RegistryClient.ManifestContentDigestV2(name, t.tag);
            await RegistryClient.DeleteImage(name, digest);
            log.Information("Deleted - {Name}:{Tag}", name, t.tag);
          }, Cfg.Parallel);
      }
    }

    async ValueTask DelContainerGroups(IAzure azure, CleanContainerMode mode, ILogger log) {
      var (allGroups, listEx) = await Def.Fun(() => azure.ContainerGroups.ListAsync()).Try();
      if (listEx != null) log.Warning(listEx, "AzureCleaner - error deleting container groups: {Error}`", listEx.Message);
      var toDelete = allGroups.NotNull().Where(g => mode switch {
        DeleteAll => true,
        DeleteCompleted => g.State().IsCompletedState(),
        _ => g.State().IsCompletedState() && g.IsExpired()
      }).ToArray();
      if (toDelete.Any())
        try {
          await azure.ContainerGroups.DeleteByIdsAsync(toDelete.Select(g => g.Id).ToArray());
          log.Information("AzureCleaner - deleted expired container groups: {@toDelete}", toDelete.Select(g => g.Name).ToArray());
        }
        catch (Exception ex) {
          log.Warning(ex, "AzureCleaner - error deleting container groups: {Error}`", ex.Message);
        }
      else
        log.Debug("AzureCleaner - No expired container groups to delete");
    }
  }

  public static class AzureCleanerEx {
    public static bool IsExpired(this IResource resource) {
      if (resource.Tags.TryGetValue("expire", out var expire))
        return expire.ParseDate(DateTimeFormatInfo.InvariantInfo, DateTimeStyles.RoundtripKind) < DateTime.UtcNow;
      return false;
    }
  }

  public class AzureCleanerCfg {
    public int      Parallel { get; set; } = 10;
    public TimeSpan Expires  { get; set; } = 7.Days();
  }
}