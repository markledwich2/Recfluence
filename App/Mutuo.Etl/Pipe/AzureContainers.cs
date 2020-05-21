﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.ContainerInstance.Fluent.ContainerGroup.Definition;
using Microsoft.Azure.Management.ContainerInstance.Fluent.Models;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.DockerRegistry;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Build;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  public class AzureContainers : IPipeWorkerStartable {
    readonly PipeAzureCfg   AzureCfg;
    readonly SemVersion     Version;
    readonly RegistryClient RegistryClient;

    public AzureContainers(PipeAzureCfg azureCfg, SemVersion version, RegistryClient registryClient) {
      AzureCfg = azureCfg;
      Version = version;
      RegistryClient = registryClient;
      Az = new Lazy<IAzure>(azureCfg.GetAzure);
    }

    Lazy<IAzure> Az { get; }

    public Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) => Launch(ctx, ids, false, log);

    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    public async Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning, ILogger log) {
      var res = await ids.BlockFunc(async runId => {
        var runCfg = runId.PipeCfg(ctx.PipeCfg); // id is for the sub-pipe, ctx is for the root

        var tag = await FindPipeTag(runCfg.Container.ImageName);
        var fullImageName = runCfg.Container.FullContainerImageName(tag);
        var pipeLog = log.ForContext("Image", fullImageName).ForContext("Pipe", runId.Name);

        var containerGroup = runId.ContainerGroupName();
        var (launch, launchDur) = await Launch(runCfg.Container, containerGroup, fullImageName, ctx.AppCtx.EnvironmentVariables,
          runId.PipeArgs(), returnOnRunning, ctx.AppCtx.CustomRegion, pipeLog).WithDuration();

        var logTxt = await launch.GetLogContentAsync(containerGroup);
        var logPath = new StringPath($"{runId.StatePath()}.log.txt");

        var errorMsg = launch.State().In(ContainerState.Failed, ContainerState.Unknown)
          ? $"The container is in an error state '{launch.State}', see {logPath}"
          : null;
        if (errorMsg.HasValue())
          pipeLog.Error("{RunId} - failed: {Log}", runId.ToString(), logTxt);

        var md = new PipeRunMetadata {
          Id = runId,
          Duration = launchDur,
          Containers = launch.Containers.Select(c => c.Value).ToArray(),
          RawState = launch.State,
          State = launch.State(),
          RunCfg = runCfg,
          ErrorMessage = errorMsg
        };
        await Task.WhenAll(
          ctx.Store.Save(logPath, logTxt.AsStream(), pipeLog),
          md.Save(ctx.Store, pipeLog));

        // delete succeeded containers. Failed, and rutned on running will be cleaned up by another process
        if (launch.State() == ContainerState.Succeeded) await DeleteContainer(runId.ContainerGroupName(), log);

        return md;
      }, ctx.PipeCfg.Azure.Parallel);

      return res;
    }

    public async Task<IContainerGroup> Launch(ContainerCfg cfg, string groupName, string fullImageName, (string name, string value)[] envVars, string[] args,
      bool returnOnStart = false, Func<Region> customRegion = null, ILogger log = null) {
      var groupDef = await ContainerGroup(cfg, groupName, groupName, fullImageName, envVars, args, customRegion);
      var group = await Create(groupDef, log);
      var run = await Run(group, returnOnStart, log);
      return run;
    }

    async Task DeleteContainer(string groupName, ILogger log) {
      await Az.Value.ContainerGroups.DeleteByResourceGroupAsync(AzureCfg.ResourceGroup, groupName);
      log.Debug("Deleted container {Container}", groupName);
    }

    static async Task<IContainerGroup> Create(IWithCreate groupDef, ILogger log) {
      var group = await groupDef.CreateAsync().WithDuration();
      log.Information("{ContainerGroup} - group created in {Duration}", group.Result.Name, group.Duration);
      return group.Result;
    }

    public async Task<IContainerGroup> Run(IContainerGroup group, bool returnOnRunning, ILogger log) {
      var sw = Stopwatch.StartNew();
      var running = false;

      while (true) {
        group = await group.RefreshAsync();
        var state = group.State();

        if (!running && state == ContainerState.Running) {
          log.Information("{ContainerGroup} - container started in {Duration}", group.Name, sw.Elapsed);
          running = true;
          if (returnOnRunning) return group;
        }
        if (!state.IsCompletedState()) {
          await Task.Delay(500);
          continue;
        }
        break;
      }
      log.Information("{ContainerGroup} - container ({Status}) in {Duration}", group.Name, group.State, sw.Elapsed);
      return group;
    }

    static async Task EnsureNotRunning(string groupName, IAzure azure, string rg) {
      var group = await azure.ContainerGroups.GetByResourceGroupAsync(rg, groupName);
      if (group != null) {
        if (group.State.HasValue() && group.State == "Running")
          throw new InvalidOperationException("Won't start container - it's not terminated");
        await azure.ContainerGroups.DeleteByIdAsync(group.Id);
      }
    }

    async Task<string> FindPipeTag(string imageName) {
      var findTags = Version.Prerelease.HasValue() ? new[] {Version.PipeTag(), Version.MajorMinorPatch()} : new[] {Version.MajorMinorPatch()};
      var existingTags = (await RegistryClient.TagList(imageName)).Tags.ToHashSet();
      var tag = findTags.Select(t => existingTags.Contains(t) ? t : null).NotNull().FirstOrDefault()
                ?? throw new InvalidOperationException($"Could not find any of tags {findTags.Join("|")}");
      return tag;
    }

    async Task<IWithCreate> ContainerGroup(ContainerCfg container, string groupName, string containerName, string fullImageName,
      IEnumerable<(string name, string value)> envVars,
      string[] args, Func<Region> customRegion = null
    ) {
      var rg = AzureCfg.ResourceGroup;
      await EnsureNotRunning(groupName, Az.Value, rg);

      var registryCreds = container.RegistryCreds ?? throw new InvalidOperationException("no registry credentials");


      var group = Az.Value.ContainerGroups.Define(groupName)
        .WithRegion(customRegion?.Invoke().Name ?? container.Region)
        .WithExistingResourceGroup(rg)
        .WithLinux()
        .WithPrivateImageRegistry(container.Registry, registryCreds.Name, registryCreds.Secret)
        .WithoutVolume()
        .DefineContainerInstance(containerName)
        .WithImage(fullImageName)
        .WithoutPorts()
        .WithCpuCoreCount(container.Cores)
        .WithMemorySizeInGB(container.Mem)
        .WithEnvironmentVariables(envVars.ToDictionary(e => e.name, e => e.value));

      if (container.Exe.HasValue())
        group = group.WithStartingCommandLine(container.Exe, args);

      var createGroup = group
        .Attach()
        .WithRestartPolicy(ContainerGroupRestartPolicy.Never)
        .WithTag("expire", (DateTime.UtcNow + 2.Days()).ToString("o"));

      return createGroup;
    }
  }
}