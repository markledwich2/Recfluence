﻿using System;
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

  public enum BranchState {
    Fresh,
    CopyProd
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
    public async Task Create(BranchState state, ILogger log) {
      if (VersionInfo.Version.Prerelease == null) throw new InvalidOperationException("can't create environment, it needs to be a pre-release");
      await Task.WhenAll(
        CreateContainer(state, log),
        WhCreator.CreateIfNotExists(state, log));
    }

    async Task CreateContainer(BranchState state, ILogger log) {
      var branchContainer = StorageCfg.Container(VersionInfo.Version);

      var exists = await branchContainer.ExistsAsync();
      if (exists) {
        log.Information("container {Container} exists, leaving as is", branchContainer.Uri);
        return;
      }

      await branchContainer.CreateAsync();
      await branchContainer.SetPermissionsAsync(new BlobContainerPermissions {PublicAccess = BlobContainerPublicAccessType.Container});

      if (state == BranchState.CopyProd) {
        var prodContainer = StorageCfg.Container(VersionInfo.ProdVersion);
        var sourceBlob = prodContainer.GetDirectoryReference(StorageCfg.DbPath);
        var destBlob = branchContainer.GetDirectoryReference(StorageCfg.DbPath);
        await YtBackup.CopyBlobs(nameof(BranchEnvCreator), sourceBlob, destBlob, log);
      }
    }
  }
}