using System;
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
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  public class AzurePipeWorker : IPipeWorkerStartable {
    public AzurePipeWorker(PipeAppCfg cfg) {
      Cfg = cfg;
      Az = new Lazy<IAzure>(() => Cfg.Azure.GetAzure());
    }

    PipeAppCfg   Cfg { get; }
    Lazy<IAzure> Az  { get; }

    public Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) => Launch(ctx, ids, false, log);

    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    public async Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning, ILogger log) {
      var azure = Az.Value;
      var res = await ids.BlockFunc(async runId => {
        var runCfg = runId.PipeCfg(ctx); // id is for the sub-pipe, ctx is for the root
        var groupName = runId.ContainerGroupName();
        await EnsureNotRunning(groupName, azure, ctx.Cfg.Azure.ResourceGroup);
        var groupDef = await ContainerGroup(runCfg.Container, runId.ContainerGroupName(), runId.ContainerName(), ctx.AppCtx.EnvironmentVariables,
          runId.PipeArgs(),
          ctx.AppCtx.CustomRegion);
        var pipeLog = log.ForContext("Image", runCfg.Container.ContainerImageName()).ForContext("Pipe", runId.Name);
        var group = await Create(groupDef, pipeLog);
        var run = await Run(@group, returnOnRunning, pipeLog).WithDuration();

        var logTxt = await run.Result.GetLogContentAsync(runId.ContainerName());
        var logPath = new StringPath($"{runId.StatePath()}.log.txt");

        var errorMsg = run.Result.State().In(ContainerState.Failed, ContainerState.Unknown)
          ? $"The container is in an error state '{run.Result.State}', see {logPath}"
          : null;
        if (errorMsg.HasValue())
          pipeLog.Error("{RunId} - failed: {Log}", runId.ToString(), logTxt);

        var md = new PipeRunMetadata {
          Id = runId,
          Duration = run.Duration,
          Containers = run.Result.Containers.Select(c => c.Value).ToArray(),
          RawState = run.Result.State,
          State = run.Result.State(),
          RunCfg = runCfg,
          ErrorMessage = errorMsg
        };
        await Task.WhenAll(
          ctx.Store.Save(logPath, logTxt.AsStream(), pipeLog),
          md.Save(ctx.Store, pipeLog));

        // delete succeeded containers. Failed, and rutned on running will be cleaned up by another process
        if (run.Result.State() == ContainerState.Succeeded) await DeleteContainer(ctx, log, runId, azure);

        return md;
      });

      return res;
    }

    static async Task DeleteContainer(IPipeCtx ctx, ILogger log, PipeRunId c, IAzure azure) {
      var groupName = c.ContainerGroupName();
      await azure.ContainerGroups.DeleteByResourceGroupAsync(ctx.Cfg.Azure.ResourceGroup, groupName);
      log.Debug("Deleted container {Container} for {Pipe}", groupName, c.Name);
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

    async Task<IWithCreate> ContainerGroup(ContainerCfg container, string groupName, string containerName, IEnumerable<(string name, string value)> envVars,
      string[] args, Func<Region> customRegion = null
    ) {
      var rg = Cfg.Azure.ResourceGroup;
      await EnsureNotRunning(groupName, Az.Value, rg);

      var registryCreds = container.RegistryCreds ?? throw new InvalidOperationException("no registry credentials");

      var group = Az.Value.ContainerGroups.Define(groupName)
        .WithRegion(customRegion?.Invoke().Name ?? container.Region)
        .WithExistingResourceGroup(rg)
        .WithLinux()
        .WithPrivateImageRegistry(container.Registry, registryCreds.Name, registryCreds.Secret)
        .WithoutVolume()
        .DefineContainerInstance(containerName)
        .WithImage(container.ContainerImageName())
        .WithoutPorts()
        .WithCpuCoreCount(container.Cores)
        .WithMemorySizeInGB(container.Mem)
        .WithEnvironmentVariables(envVars.ToDictionary(e => e.name, e => e.value))
        .WithStartingCommandLine(container.Exe, args);

      var createGroup = group
        .Attach()
        .WithRestartPolicy(ContainerGroupRestartPolicy.Never)
        .WithTag("expire", (DateTime.UtcNow + 2.Days()).ToString("o"));

      return createGroup;
    }
  }
}