using System;
using System.Collections.Generic;
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
using Stopwatch = System.Diagnostics.Stopwatch;

namespace Mutuo.Etl.Pipe {
  public class AzureContainers : IPipeWorkerStartable, IContainerLauncher {
    readonly SemVersion     Version;
    readonly RegistryClient RegistryClient;
    readonly ContainerCfg   ContainerCfg;

    public AzureContainers(PipeAzureCfg azureCfg, SemVersion version, RegistryClient registryClient, ContainerCfg containerCfg) {
      AzureCfg = azureCfg;
      Version = version;
      RegistryClient = registryClient;
      ContainerCfg = containerCfg;
      Az = new Lazy<IAzure>(azureCfg.GetAzure);
    }

    public static readonly string ContainerNameEnv = $"{nameof(AzureContainers)}_Container";
    public static string GetContainerEnv() => Environment.GetEnvironmentVariable(ContainerNameEnv);
    public static ILogger Enrich(ILogger log) => log.ForContext("Container", GetContainerEnv());

    public PipeAzureCfg AzureCfg { get; }

    Lazy<IAzure> Az { get; }

    public Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log, CancellationToken cancel) =>
      Launch(ctx, ids, returnOnRunning: false, exclusive: false, log: log, cancel: cancel);

    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    public async Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning,
      bool exclusive, ILogger log, CancellationToken cancel) {
      var res = await ids.BlockFunc(async runId => {
        var runCfg = runId.PipeCfg(ctx.PipeCfg); // id is for the sub-pipe, ctx is for the root

        var tag = await FindPipeTag(runCfg.Container.ImageName);
        var fullImageName = runCfg.Container.FullContainerImageName(tag);
        var pipeLog = log.ForContext("Image", fullImageName).ForContext("Pipe", runId.Name);

        var containerGroup = runId.ContainerGroupName(exclusive, Version);
        var containerName = runCfg.Container.ImageName.ToLowerInvariant();

        var (group, launchDur) = await Launch(runCfg.Container, containerGroup, containerName,
          fullImageName, ctx.AppCtx.EnvironmentVariables,
          runId.PipeArgs(), returnOnRunning, ctx.AppCtx.CustomRegion, pipeLog, cancel).WithDuration();

        var (logTxt, _) = await group.GetLogContentAsync(containerName).Try("");
        var logPath = new StringPath($"{runId.StatePath()}.log.txt");

        var launchState = group.State();

        var errorMsg = launchState.In(ContainerState.Failed, ContainerState.Terminated, ContainerState.Unknown)
          ? $"The container is in an error state '{group.State}', see {logPath}"
          : null;
        if (errorMsg.HasValue())
          pipeLog.Error("{RunId} - failed: {Log}", runId.ToString(), logTxt);

        var md = new PipeRunMetadata {
          Id = runId,
          Duration = launchDur,
          Containers = group.Containers.Select(c => c.Value).ToArray(),
          RawState = group.State,
          State = group.State(),
          RunCfg = runCfg,
          ErrorMessage = errorMsg
        };
        await Task.WhenAll(
          ctx.Store.Save(logPath, logTxt.AsStream(), pipeLog),
          md.Save(ctx.Store, pipeLog));

        // delete succeeded non-exclusive containers. Failed, and rutned on running will be cleaned up by another process
        if (group.State() == ContainerState.Succeeded && !(exclusive && runId.Num > 0)) await DeleteContainer(containerGroup, log);

        return md;
      }, returnOnRunning ? ctx.PipeCfg.Azure.Parallel : ids.Count);

      return res;
    }

    public async Task<IContainerGroup> Launch(ContainerCfg cfg, string groupName, string containerName, string fullImageName,
      (string name, string value)[] envVars, string[] args,
      bool returnOnStart = false, Func<Region> customRegion = null, ILogger log = null, CancellationToken cancel = default) {
      var sw = Stopwatch.StartNew();
      var options = new GroupOptions {
        ContainerName = containerName,
        Region = customRegion?.Invoke().Name ?? cfg.Region,
        Image = fullImageName,
        Cores = cfg.Cores,
        Mem = cfg.Mem,
        Env = envVars.Concat((name: ContainerNameEnv, value: groupName)).ToDictionary(e => e.name, e => e.value),
        Exe = cfg.Exe,
        Args = args
      };
      await EnsureNotRunning(groupName, options, Az.Value, AzureCfg.ResourceGroup);

      log?.Information("Launching container group {Container} ({FullImage}), args {Args}, region {Region}",
        groupName, options.Image, args.Join(" "), options.Region);
      var groupDef = ContainerGroup(cfg, groupName, options);
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
      return group.Result;
    }

    public async Task RunContainer(string containerName, string fullImageName, (string name, string value)[] envVars,
      string[] args, string groupName = null, ILogger log = null, CancellationToken cancel = default) {
      groupName ??= containerName;
      var group = await Launch(ContainerCfg with { Exe = null }, groupName, containerName, fullImageName, envVars, args, returnOnStart: false, log: log, cancel: cancel);
      var dur = await group.EnsureSuccess(containerName, log).WithWrappedException("Container failed").WithDuration();
      log?.Information("Container {Container} completed in {Duration}", groupName, dur);
    }

    public async Task<IContainerGroup> Run(IContainerGroup group, bool returnOnRunning, Stopwatch sw, ILogger log, CancellationToken cancel = default) {
      var running = false;
      var loggedWaiting = Stopwatch.StartNew();

      while (true) {
        group = await group.RefreshAsync();
        var state = group.State();

        if (!running && state == ContainerState.Running) {
          log.Debug("{Container} - container started in {Duration}", group.Name, sw.Elapsed.HumanizeShort());
          running = true;
          if (returnOnRunning) return group;
        }
        if (!state.IsCompletedState()) {
          if (cancel.IsCancellationRequested) {
            log.Information("{Container} - cancellation requested - stopping", group.Name);
            await group.StopAsync();
            await group.WaitForState(ContainerState.Stopped, ContainerState.Failed, ContainerState.Terminated);
            return group;
          }
          if (loggedWaiting.Elapsed > 1.Minutes()) {
            log.Debug("{Container} - waiting to complete. Current state {State}", group.Name, group.State);
            loggedWaiting.Restart();
          }
          await Task.Delay(5.Seconds());
          continue;
        }
        break;
      }
      log.Information("{Container} - container ({Status}) in {Duration}", group.Name, group.State, sw.Elapsed.HumanizeShort());
      return group;
    }

    static async Task EnsureNotRunning(string groupName, GroupOptions options, IAzure azure, string rg) {
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

    class GroupOptions {
      public string                     Region        { get; set; }
      public int                        Cores         { get; set; }
      public double                     Mem           { get; set; }
      public Dictionary<string, string> Env           { get; set; }
      public string                     Image         { get; set; }
      public string                     ContainerName { get; set; }
      public string                     Exe           { get; set; }
      public string[]                   Args          { get; set; }
    }

    IWithCreate ContainerGroup(ContainerCfg container, string groupName, GroupOptions options) {
      var rg = AzureCfg.ResourceGroup;
      var registryCreds = container.RegistryCreds ?? throw new InvalidOperationException("no registry credentials");
      var group = Az.Value.ContainerGroups.Define(groupName)
        .WithRegion(options.Region)
        .WithExistingResourceGroup(rg)
        .WithLinux()
        .WithPrivateImageRegistry(container.Registry, registryCreds.Name, registryCreds.Secret)
        .WithoutVolume()
        .DefineContainerInstance(options.ContainerName)
        .WithImage(options.Image)
        .WithoutPorts()
        .WithCpuCoreCount(options.Cores)
        .WithMemorySizeInGB(options.Mem)
        .WithEnvironmentVariables(options.Env);

      if (container.Exe.HasValue())
        group = group.WithStartingCommandLine(options.Exe, options.Args);
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