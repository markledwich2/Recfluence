using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Storage.Blob;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;
using YtReader.Db;
using YtReader.Store;
using static YtReader.BranchState;

namespace YtReader {
  public class BranchEnvCfg {
    public TimeSpan Expiry { get; set; } = 2.Days();
    public string   Email  { get; set; }
  }

  public enum BranchState {
    Fresh,
    Clone,
    CloneBasic,
    CloneDb
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
    public async Task Create(BranchState state, string[] dbPaths, ILogger log) {
      if (VersionInfo.Version.Prerelease.NullOrEmpty()) throw new InvalidOperationException("can't create environment, it needs to be a pre-release");
      await Task.WhenAll(
        CreateContainer(state, dbPaths, log),
        WhCreator.CreateOrReplace(state, log));
    }

    async Task CreateContainer(BranchState state, string[] dbPaths, ILogger log) {
      var branchContainer = StorageCfg.Container(VersionInfo.Version);
      var containerExists = await branchContainer.ExistsAsync();
      if (containerExists) log.Information("container {Container} exists, leaving as is", branchContainer.Uri);

      if (!containerExists) {
        await branchContainer.CreateAsync();
        await branchContainer.SetPermissionsAsync(new BlobContainerPermissions {PublicAccess = BlobContainerPublicAccessType.Container});
      }

      var db = StorageCfg.DbPath;
      var basicDbPaths = new[] {"channels", "video_extra", "channel_reviews", "videos" , "captions" }.Where(p => dbPaths == null || dbPaths.Contains(p));
      List<string> paths;
      switch (state) {
        case Clone:
          paths = new List<string> { db, StorageCfg.ImportPath };
          break;
        case CloneBasic:
          paths = new List<string> { StorageCfg.ImportPath };
          paths.AddRange(basicDbPaths.Select(p => $"{db}/{p}"));
          break;
        default:
          paths = null;
          break;
      }
      if (paths == null) return;

      var prodContainer = StorageCfg.Container(VersionInfo.ProdVersion);
      foreach (var path in paths) {
        var sourceBlob = prodContainer.GetDirectoryReference(path);
        var destBlob = branchContainer.GetDirectoryReference(path);
        await YtBackup.CopyBlobs(nameof(BranchEnvCreator), sourceBlob, destBlob, log);
      }
    }
  }
}