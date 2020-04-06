using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Medallion.Shell;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.ContainerInstance.Fluent.ContainerGroup.Definition;
using Microsoft.Azure.Management.ContainerInstance.Fluent.Models;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

//using Microsoft.Azure.Management.Fluent;

namespace Mutuo.Etl.Pipe {
  public interface IPipeWorker {
    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids);
  }

  public interface IPipeWorkerStartable : IPipeWorker {
    Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning);
  }

  public enum ContainerState {
    Unknown,
    Running,
    Succeeded,
    Failed
  }

  public static class PipeWorkerEx {
    public static string ContainerName(this PipeRunId runId) => runId.Name.ToLowerInvariant();
    public static string ContainerGroupName(this PipeRunId runid) => $"{runid.Name}-{runid.GroupId}-{runid.Num}".ToLowerInvariant();
    public static string ContainerImageName(this ContainerCfg cfg) => $"{cfg.Registry}/{cfg.ImageName}:{cfg.Tag}";
    public static string[] PipeArgs(this PipeRunId runId) => new[] {"pipe", "-r", runId.ToString()};

    public static async Task Save(this PipeRunMetadata md, ISimpleFileStore store) =>
      await store.Set($"{md.Id.StatePath()}.RunMetadata", md, false);

    public static ContainerState State(this IContainerGroup group) => group.State.ToEnum<ContainerState>(false);
    public static bool IsCompletedState(this ContainerState state) => state.In(ContainerState.Succeeded, ContainerState.Failed);

    public static async Task<PipeRunMetadata> RunWork(this IPipeWorker worker, IPipeCtx ctx, string pipe) =>
      (await worker.RunWork(ctx, new[] {PipeRunId.FromName(pipe)})).First();

    public static async Task<PipeRunMetadata> RunWork(this IPipeWorkerStartable worker, IPipeCtx ctx, string pipe, bool returnOnStarting) =>
      (await worker.RunWork(ctx, new[] {PipeRunId.FromName(pipe)}, returnOnStarting)).First();
  }

  public class LocalPipeWorker : IPipeWorker {
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids) =>
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
        await md.Save(ctx.Store);
        return md;
      });
  }

  public class ThreadPipeWorker : IPipeWorker {
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids) {
      var res = await ids.BlockTransform(async id => {
        await ctx.DoPipeWork(id);
        var md = new PipeRunMetadata {
          Id = id
        };
        await md.Save(ctx.Store);
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
    ILogger      Log { get; }
    Lazy<IAzure> Az  { get; }

    public AzurePipeWorker(PipeAppCfg cfg, ILogger log) {
      Cfg = cfg;
      Log = log;
      Az = new Lazy<IAzure>(() => GetAzure(Cfg.Azure));
    }

    static IAzure GetAzure(PipeAzureCfg cfg) {
      var sp = cfg.ServicePrincipal;
      var creds = new AzureCredentialsFactory().FromServicePrincipal(sp.ClientId, sp.Secret, sp.TennantId, AzureEnvironment.AzureGlobalCloud);
      var azure = Azure.Authenticate(creds).WithSubscription(cfg.SubscriptionId);
      return azure;
    }

    public Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids) => RunWork(ctx, ids, false);

    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunWork(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning) {
      var azure = Az.Value;
      var res = await ids.BlockTransform(async runId => {
        var runCfg = runId.PipeCfg(ctx); // id is for the sub-pipe, ctx is for the root
        var groupName = runId.ContainerGroupName();
        await EnsureNotRunning(groupName, azure, ctx.Cfg.Azure.ResourceGroup);
        var groupDef = await ContainerGroup(runCfg.Container, runId.ContainerGroupName(), runId.ContainerName(), ctx.EnvVars, runId.PipeArgs(),
          ctx.CustomRegion);
        var pipeLog = Log.ForContext("Image", runCfg.Container.ContainerImageName()).ForContext("Pipe", runId.Name);
        var group = await Create(groupDef, pipeLog);
        var run = await Run(group, returnOnRunning).WithDuration();

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
          ctx.Store.Save(logPath, logTxt.AsStream()),
          md.Save(ctx.Store));
        return md;
      }, 10);

      await ids.BlockAction(async c => {
        var groupName = c.ContainerGroupName();
        await azure.ContainerGroups.DeleteByResourceGroupAsync(ctx.Cfg.Azure.ResourceGroup, groupName);
        Log.Debug("Deleted container {Container} for {Pipe}", groupName, c.Name);
      }, 10);

      return res;
    }

    static async Task<IContainerGroup> Create(IWithCreate groupDef, ILogger log) {
      log.Information("{ContainerGroup} - creating", groupDef.Name);
      var group = await groupDef.CreateAsync();
      log.Information("{ContainerGroup} - created", group.Name);
      return group;
    }

    public async Task<IContainerGroup> Run(IContainerGroup group, bool returnOnRunning = false, ILogger log = null) {
      log ??= Log;
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

    public async Task<IContainerGroup> Create(ContainerCfg container, string name, IDictionary<string, string> envVars, params string[] args) {
      var groupName = $"{name.ToLowerInvariant()}-{DateTime.UtcNow.FileSafeTimestamp()}-{Guid.NewGuid().ToShortString(4)}";
      var groupDef = await ContainerGroup(container, groupName, name.ToLowerInvariant(), envVars, args);
      var group = await Create(groupDef, Log);
      return group;
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