using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Medallion.Shell;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.ContainerInstance.Fluent.ContainerGroup.Definition;
using Microsoft.Azure.Management.ContainerInstance.Fluent.Models;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  public interface IPipeWorker {
    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log);
  }

  public interface IPipeWorkerStartable : IPipeWorker {
    Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning, ILogger log);
  }

  public enum ContainerState {
    Unknown,
    Running,
    Succeeded,
    Failed,
    Stopped,
    Updating
  }

  public static class PipeWorkerEx {
    public static string ContainerName(this PipeRunId runId) => runId.Name.ToLowerInvariant();
    public static string ContainerGroupName(this PipeRunId runid) => $"{runid.Name}-{runid.GroupId}-{runid.Num}".ToLowerInvariant();
    public static string ContainerImageName(this ContainerCfg cfg) => $"{cfg.Registry}/{cfg.ImageName}:{cfg.Tag}";
    public static string[] PipeArgs(this PipeRunId runId) => new[] {"pipe", "-r", runId.ToString()};

    public static async Task Save(this PipeRunMetadata md, ISimpleFileStore store, ILogger log) =>
      await store.Set($"{md.Id.StatePath()}.RunMetadata", md, false, log);

    public static ContainerState State(this IContainerGroup group) => group.State.ToEnum<ContainerState>(false);
    public static bool IsCompletedState(this ContainerState state) => state.In(ContainerState.Succeeded, ContainerState.Failed);

    public static async Task<IContainerGroup> WaitForState(this IContainerGroup group, params ContainerState[] states) {
      while (true) {
        if (group.State().In(states))
          return group;
        await Task.Delay(1.Seconds());
        group = await group.RefreshAsync();
      }
    }

    public static async Task<PipeRunMetadata> RunWork(this IPipeWorker worker, IPipeCtx ctx, string pipe, ILogger log) =>
      (await worker.RunWork(ctx, new[] {PipeRunId.FromName(pipe)}, log)).First();

    public static async Task<PipeRunMetadata> RunWork(this IPipeWorkerStartable worker, IPipeCtx ctx, string pipe, bool returnOnStarting, ILogger log) =>
      (await worker.RunWork(ctx, new[] {PipeRunId.FromName(pipe)}, returnOnStarting, log)).First();
  }

  public class LocalPipeWorker : IPipeWorker {
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) =>
      await ids.BlockTransform(async id => {
        var runCfg = id.PipeCfg(ctx);
        var image = runCfg.Container.ContainerImageName();
        var args = new[] {"run"}
          .Concat(ctx.EnvVars.SelectMany(e => new[] {"--env", $"{e.Key}={e.Value}"}))
          .Concat("--rm", "-i", image)
          .Concat(runCfg.Container.Exe)
          .Concat(id.PipeArgs())
          .ToArray<object>();
        var cmd = Command.Run("docker", args).RedirectTo(Console.Out);
        var res = await cmd.Task;
        var md = res.Success
          ? new PipeRunMetadata {
            Id = id
          }
          : new PipeRunMetadata {
            Id = id,
            ErrorMessage = await cmd.StandardError.ReadToEndAsync()
          };
        await md.Save(ctx.Store, log);
        return md;
      });
  }

  public class ThreadPipeWorker : IPipeWorker {
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) {
      var res = await ids.BlockTransform(async id => {
        await ctx.DoPipeWork(id);
        var md = new PipeRunMetadata {
          Id = id
        };
        await md.Save(ctx.Store, log);
        return md;
      });
      return res;
    }
  }

  public class PipeRunMetadata {
    public PipeRunId   Id           { get; set; }
    public string      FinalState   { get; set; }
    public TimeSpan    Duration     { get; set; }
    public Container[] Containers   { get; set; }
    public string      ErrorMessage { get; set; }

    public bool Success => !Error;
    public bool Error   => ErrorMessage.HasValue();
  }

  public class AzurePipeWorker : IPipeWorkerStartable {
    PipeAppCfg   Cfg { get; }
    Lazy<IAzure> Az  { get; }

    public AzurePipeWorker(PipeAppCfg cfg) {
      Cfg = cfg;
      Az = new Lazy<IAzure>(() => Cfg.Azure.GetAzure());
    }

    public Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) => RunWork(ctx, ids, false, log);

    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning, ILogger log) {
      var azure = Az.Value;
      var res = await ids.BlockTransform(async runId => {
        var runCfg = runId.PipeCfg(ctx); // id is for the sub-pipe, ctx is for the root
        var groupName = runId.ContainerGroupName();
        await EnsureNotRunning(groupName, azure, ctx.Cfg.Azure.ResourceGroup);
        var groupDef = await ContainerGroup(runCfg.Container, runId.ContainerGroupName(), runId.ContainerName(), ctx.EnvVars, runId.PipeArgs(),
          ctx.CustomRegion);
        var pipeLog = log.ForContext("Image", runCfg.Container.ContainerImageName()).ForContext("Pipe", runId.Name);
        var group = await Create(groupDef, pipeLog);
        var run = await Run(group, returnOnRunning, pipeLog).WithDuration();

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
          FinalState = run.Result.State,
          ErrorMessage = errorMsg
        };
        await Task.WhenAll(
          ctx.Store.Save(logPath, logTxt.AsStream(), pipeLog),
          md.Save(ctx.Store, pipeLog));
        return md;
      }, 10);

      await ids.BlockAction(async c => {
        var groupName = c.ContainerGroupName();
        await azure.ContainerGroups.DeleteByResourceGroupAsync(ctx.Cfg.Azure.ResourceGroup, groupName);
        log.Debug("Deleted container {Container} for {Pipe}", groupName, c.Name);
      }, 10);

      return res;
    }

    static async Task<IContainerGroup> Create(IWithCreate groupDef, ILogger log) {
      log.Information("{ContainerGroup} - creating", groupDef.Name);
      var group = await groupDef.CreateAsync();
      log.Information("{ContainerGroup} - created", group.Name);
      return group;
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
          if (!returnOnRunning) return group;
        }
        if (!state.IsCompletedState()) {
          await Task.Delay(500);
          continue;
        }
        break;
      }
      log.Information("{ContainerGroup} - completed ({Status}) in {Duration}", group.Name, group.State, sw.Elapsed);
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

    async Task<IWithCreate> ContainerGroup(ContainerCfg container, string groupName, string containerName, IDictionary<string, string> envVars,
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
        .WithEnvironmentVariables(envVars)
        .WithStartingCommandLine(container.Exe, args);

      var createGroup = group
        .Attach().WithRestartPolicy(ContainerGroupRestartPolicy.Never);

      return createGroup;
    }
  }
}