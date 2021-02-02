using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using Humanizer;
using Microsoft.Azure.Storage.Blob;
using Mutuo.Etl.Blob;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static YtReader.BranchState;
using static YtReader.Store.StoreTier;

namespace YtReader {
  public class BranchEnvCfg {
    public TimeSpan Expiry { get; set; } = 2.Days();
    public string   Email  { get; set; }
  }

  public enum BranchState {
    Fresh,
    Clone,
    CloneDb
  }

  public class BranchEnvCreator {
    readonly VersionInfo      VersionInfo;
    readonly WarehouseCreator WhCreator;
    readonly BlobStores       Stores;

    public BranchEnvCreator(VersionInfo versionInfo, WarehouseCreator whCreator, BlobStores stores) {
      VersionInfo = versionInfo;
      WhCreator = whCreator;
      Stores = stores;
    }

    /// <summary>Creates an empty environment for a branch</summary>
    /// <returns></returns>
    public async Task Create(BranchState state, string[] paths, ILogger log) {
      if (VersionInfo.Version.Prerelease.NullOrEmpty()) throw new InvalidOperationException("can't create environment, it needs to be a pre-release");
      await Task.WhenAll(
        CreateContainers(state, paths, log),
        WhCreator.CreateOrReplace(state, log));
    }

    Task CreateContainers(BranchState state, string[] paths, ILogger log) =>
      new[] {Premium, Standard}.BlockAction(async tier => {
        var s = Stores.Store(tier: tier);
        var c = s.Container;
        var exists = await c.ExistsAsync();
        if (!exists) {
          await c.CreateAsync();
          await c.SetAccessPolicyAsync(PublicAccessType.BlobContainer);
        }
        await PopulateContainer(tier, state, paths, log);
      });

    async Task PopulateContainer(StoreTier tier, BranchState state, string[] paths, ILogger log) {
      if (state.In(CloneDb, Fresh)) return;

      async Task<(AzureBlobFileStore container, CloudBlobContainer legacy, StringPath[] rooDirs)> GetStorePrep(SemVersion version) {
        var container = Stores.Store(tier: tier, version: version);
        var legacy = container.LegacyContainer();
        var rooDirs = await container.ListDirs("").ToArrayAsync();
        return (container, legacy, rooDirs);
      }

      var source = await GetStorePrep(VersionInfo.ProdVersion);
      var dest = await GetStorePrep(VersionInfo.Version);

      foreach (var path in source.rooDirs.Where(d => paths == null || paths.Contains(d.ToString()))) {
        var sourceBlob = source.legacy.GetDirectoryReference(path);
        var destBlob = dest.legacy.GetDirectoryReference(path);
        await YtBackup.CopyBlobs(nameof(BranchEnvCreator), sourceBlob, destBlob, log);
      }
    }
  }
}