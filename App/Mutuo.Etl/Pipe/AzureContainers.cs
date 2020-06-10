using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CliFx.Exceptions;
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
    readonly SemVersion     Version;
    readonly RegistryClient RegistryClient;

    public AzureContainers(PipeAzureCfg azureCfg, SemVersion version, RegistryClient registryClient) {
      AzureCfg = azureCfg;
      Version = version;
      RegistryClient = registryClient;
      Az = new Lazy<IAzure>(azureCfg.GetAzure);
    }

    public PipeAzureCfg AzureCfg { get; }

    Lazy<IAzure> Az { get; }

    public Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log, CancellationToken cancel) => 
      Launch(ctx, ids, false, log, cancel);

    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    public async Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning, 
      ILogger log, CancellationToken cancel) {
      var res = await ids.BlockFunc(async runId => {
        var runCfg = runId.PipeCfg(ctx.PipeCfg); // id is for the sub-pipe, ctx is for the root

        var tag = await FindPipeTag(runCfg.Container.ImageName);
        var fullImageName = runCfg.Container.FullContainerImageName(tag);
        var pipeLog = log.ForContext("Image", fullImageName).ForContext("Pipe", runId.Name);

        var containerGroup = runId.ContainerGroupName();
        var (launch, launchDur) = await Launch(runCfg.Container, containerGroup, fullImageName, ctx.AppCtx.EnvironmentVariables,
          runId.PipeArgs(), returnOnRunning, ctx.AppCtx.CustomRegion, pipeLog, cancel).WithDuration();

        var logTxt = await launch.GetLogContentAsync(containerGroup);
        var logPath = new StringPath($"{runId.StatePath()}.log.txt");

        var launchState = launch.State();

        var errorMsg = launchState.In(ContainerState.Failed, ContainerState.Terminated, ContainerState.Unknown)
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
      }, ctx.PipeCfg.Azure.Parallel, cancel:cancel);

      return res;
    }

    public async Task<IContainerGroup> Launch(ContainerCfg cfg, string groupName, string fullImageName, 
      (string name, string value)[] envVars, string[] args,
      bool returnOnStart = false, Func<Region> customRegion = null, ILogger log = null, CancellationToken cancel = default) {
      var sw = Stopwatch.StartNew();
      var groupDef = await ContainerGroup(cfg, groupName, groupName, fullImageName, envVars, args, customRegion);
      var group = await Create(groupDef, log);
      var run = await Run(group, returnOnStart, sw, log, cancel);
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

    public async Task<IContainerGroup> Run(IContainerGroup @group, bool returnOnRunning, Stopwatch sw, ILogger log, CancellationToken cancel = default) {
      var running = false;
      var loggedWaiting = Stopwatch.StartNew();

      while (true) {
        group = await group.RefreshAsync();
        var state = group.State();

        if (!running && state == ContainerState.Running) {
          log.Information("{ContainerGroup} - container started in {Duration}", group.Name, sw.Elapsed.HumanizeShort());
          running = true;
          if (returnOnRunning) return group;
        }
        if (!state.IsCompletedState()) {
          if (cancel.IsCancellationRequested) {
            log.Information("{ContainerGroup} - cancellation requested - stopping", group.Name);
            await group.StopAsync();
            await group.WaitForState(ContainerState.Stopped, ContainerState.Failed, ContainerState.Terminated);
            return group;
          }
          if (loggedWaiting.Elapsed > 1.Minutes()) {
            log.Debug("{ContainerGroup} - waiting to complete. Current state {State}", group.Name, group.State);
            loggedWaiting.Restart();
          }
          await Task.Delay(5.Seconds());
          continue;
        }
        break;
      }
      log.Information("{ContainerGroup} - container ({Status}) in {Duration}", group.Name, group.State, sw.Elapsed.HumanizeShort());
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

  public static class AzureContainersEx {
    public static async Task EnsureSuccess(this IContainerGroup group, string containerName, ILogger log) {
      if (!group.State().In(ContainerState.Succeeded)) {
        var content = await group.GetLogContentAsync(containerName);
        var exitCode = group.Containers[containerName].InstanceView?.CurrentState.ExitCode;
        Log.Warning("Container {Container} did not succeed State ({State}), ExitCode ({ErrorCode}), Logs: {Logs}", 
          group.Name, group.State, exitCode, content);
        throw new CommandException($"Container {group.Name} did not succeed ({group.State}), exit code ({exitCode}). Logs: {content}", exitCode: exitCode ?? 0);
      }
    }
  }
}