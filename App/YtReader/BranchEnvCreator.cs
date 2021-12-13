using Azure.Storage.Blobs.Models;
using CliFx;
using CliFx.Attributes;
using CliFx.Infrastructure;
using JetBrains.Annotations;
using Microsoft.Azure.Storage.Blob;
using Mutuo.Etl.Blob;
using Semver;
using YtReader.Collect;
using YtReader.Db;
using YtReader.Store;
using static YtReader.Store.StoreTier;
using static YtReader.Store.AccessType;

namespace YtReader;

public class BranchEnvCfg {
  public TimeSpan Expiry { get; set; } = 2.Days();
  public string   Email  { get; set; }
}

public enum CreateMode {
  Create,
  Clone
}

public enum EnvPart {
  Blob,
  [RunPart(Explicit = true)] Db
}

[Command("create-env", Description = "Create a branch environment for testing")]
public class CreateEnvCmd : ICommand {
  readonly BranchEnvCreator Creator;
  readonly ILogger          Log;

  public CreateEnvCmd(BranchEnvCreator creator, ILogger log) {
    Creator = creator;
    Log = log;
  }

  [CommandOption('p', Description = "| seperated list of parts of the environment to create Blob|Db")]
  public string Parts { get; set; }

  [CommandOption('m', Description = "the mode to copy the database Create|Clone")]
  public CreateMode Mode { get; set; }

  [CommandOption("stage-paths", Description = "| separated list of staging db paths to copy")]
  public string StagePaths { get; set; }

  [CommandOption("schema", Description = "if specified will use this warehouse schema instead of the configured one")]
  public string Schema { get; set; }

  [CommandOption("non-branch")] public bool NonBranch { get; set; } = false;

  public async ValueTask ExecuteAsync(IConsole console) =>
    await Creator.Create(Parts?.UnJoin('|').Select(p => p.ParseEnum<EnvPart>()).ToArray(), Mode, StagePaths.UnJoin('|'), NonBranch, Schema, Log);
}

public record BranchEnvCreator(VersionInfo VersionInfo, WarehouseCreator WhCreator, BlobStores Stores) {
  /// <summary>Creates an empty environment for a branch</summary>
  /// <returns></returns>
  public async Task Create(EnvPart[] parts, CreateMode mode, string[] paths, bool nonBranch, [CanBeNull] string schema, ILogger log) {
    if (VersionInfo.Version.Prerelease.NullOrEmpty() && !nonBranch)
      throw new InvalidOperationException("can't create environment, it needs to be a pre-release");
    if (parts.ShouldRun(EnvPart.Blob)) await CreateContainers(mode, paths, log);
    if (parts.ShouldRun(EnvPart.Db))
      await WhCreator.CreateOrReplace(mode switch {
        CreateMode.Clone => WarehouseCreateMode.CloneProd,
        CreateMode.Create => WarehouseCreateMode.CreateAndReplace,
        _ => throw new($"create mode {mode} not supported")
      }, schema, log);
  }

  Task<long> CreateContainers(CreateMode state, string[] paths, ILogger log) =>
    new[] { Public, Default }.BlockDo(async access => {
      var s = (AzureBlobFileStore)Stores.Store(access: access);
      var c = s.Container;
      var exists = await c.ExistsAsync();
      if (!exists) {
        await c.CreateAsync();
        await c.SetAccessPolicyAsync(access == Public ? PublicAccessType.Blob : PublicAccessType.None);
      }
      await PopulateContainer(access, Standard, state, paths, log);
    });

  async Task PopulateContainer(AccessType access, StoreTier tier, CreateMode state, string[] paths, ILogger log) {
    if (state.In(CreateMode.Create)) return;

    async Task<(AzureBlobFileStore container, CloudBlobContainer legacy, SPath[] rooDirs)> GetStorePrep(SemVersion version) {
      var container = (AzureBlobFileStore)Stores.Store(access: access, tier: tier, version: version);
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