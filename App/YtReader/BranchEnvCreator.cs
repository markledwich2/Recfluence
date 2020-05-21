using System;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Storage.Blob;
using Serilog;
using YtReader.Db;
using YtReader.Store;

namespace YtReader {
  public class BranchEnvCfg {
    public TimeSpan Expiry { get; set; } = 2.Days();
    public string   Email  { get; set; }
  }

  public class BranchEnvCreator {
    readonly StorageCfg       StorageCfg;
    readonly VersionInfo      VersionInfo;
    readonly WarehouseCreator WhCreator;

    public BranchEnvCreator(StorageCfg storageCfg, VersionInfo versionInfo, WarehouseCreator whCreator) {
      StorageCfg = storageCfg;
      VersionInfo = versionInfo;
      WhCreator = whCreator;
    }

    /// <summary>True if this branch environments exists. Check's the azure blob container and nothing else.</summary>
    public async Task<bool> Exists() {
      var container = StorageCfg.Container(VersionInfo.Version);
      return await container.ExistsAsync();
    }

    /// <summary>Creates an empty environment for a branch</summary>
    /// <returns></returns>
    public async Task Create(ILogger log) {
      if (VersionInfo.Version.Prerelease == null) throw new InvalidOperationException("can't create environment, it needs to be a pre-release");
      await CreateContainer();
      await WhCreator.CreateIfNotExists(log);
    }

    async Task CreateContainer() {
      var container = StorageCfg.Container(VersionInfo.Version);
      await container.CreateIfNotExistsAsync();
      await container.SetPermissionsAsync(new BlobContainerPermissions {PublicAccess = BlobContainerPublicAccessType.Container});
    }
  }
}