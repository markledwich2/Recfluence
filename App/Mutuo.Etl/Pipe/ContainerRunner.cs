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
using Microsoft.Azure.Management.Storage.Fluent.Models;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

//using Microsoft.Azure.Management.Fluent;

namespace Mutuo.Etl.Pipe {
  public interface IContainerRunner {
    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    Task<IReadOnlyCollection<PipeRunMetadata>> RunBatch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log);
  }
  
  public enum ContainerState {
    Unknown,
    Running,
    Succeeded,
    Failed
  }

  public static class ContainerRunnerEx {
    public static string ContainerName(this PipeRunId runId) => runId.Name.ToLowerInvariant();
    public static string ContainerGroupName(this PipeRunId runid) => $"{runid.Name}-{runid.GroupId}-{runid.Num}".ToLowerInvariant();
    public static string ContainerImageName(this ContainerCfg cfg) => $"{cfg.Registry}/{cfg.ImageName}:{cfg.Tag}";
    public static string[] PipeArgs(this PipeRunId runId) => new[] {"pipe", "-r", runId.ToString()};
    public static async Task Save(this PipeRunMetadata md, ISimpleFileStore store) =>
      await store.Set($"{md.Id.StatePath()}.RunMetadata", md, false);
    public static ContainerState State(this IContainerGroup group) => group.State.ToEnum<ContainerState>(false);
    public static bool IsCompletedState(this ContainerState state) => state.In(ContainerState.Succeeded, ContainerState.Failed);
  }

  public class LocalContainerRunner : IContainerRunner {
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunBatch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) =>
      await ids.BlockTransform(async id => {
        var image = ctx.Cfg.Container.ContainerImageName();
        var args = new[] {"run"}
          .Concat(ctx.EnvVars.SelectMany(e => new[] {"--env", $"{e.Key}={e.Value}"}))
          .Concat("--rm", "-i", image)
          .Concat(id.PipeArgs())
          .ToArray<object>();
        var cmd = Command.Run("docker", args).RedirectTo(Console.Out);
        var res = await cmd.Task;
        var md = res.Success
          ? new PipeRunMetadata {
            Id = id,
            Success = true
          }
          : new PipeRunMetadata {
            Id = id,
            Success = false,
            ErrorMessage = await cmd.StandardError.ReadToEndAsync()
          };
        await md.Save(ctx.Store);
        return md;
      });
  }

  public class ThreadContainerRunner : IContainerRunner {
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunBatch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) {
      var res = await ids.BlockTransform(async b => {
        await new PipeCtx(ctx, b).RunPipe();
        var md = new PipeRunMetadata {
          Id = b,
          Success = true
        };
        await md.Save(ctx.Store);
        return md;
      }, ctx.Cfg.LocalParallel);
      return res;
    }
  }

  public class PipeRunMetadata {
    public bool         Success      { get; set; }
    public PipeRunId    Id           { get; set; }
    public string       FinalState   { get; set; }
    public TimeSpan     Duration     { get; set; }
    public Container[] Containers { get; set; }
    public string       ErrorMessage { get; set; }
  }

  public class AzureContainerRunner : IContainerRunner {
    static IAzure GetAzure(PipeAppCfg cfg) {
      var sp = cfg.Azure.ServicePrincipal;
      var creds = new AzureCredentialsFactory().FromServicePrincipal(sp.ClientId, sp.Secret, sp.TennantId, AzureEnvironment.AzureGlobalCloud);
      var azure = Azure.Authenticate(creds).WithSubscription(cfg.Azure.SubscriptionId);
      return azure;
    }

    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    public async Task<IReadOnlyCollection<PipeRunMetadata>> RunBatch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) {
      var azure = GetAzure(ctx.Cfg);
      var res = await ids.BlockTransform(async runId => {
        var groupName = runId.ContainerGroupName();
        await EnsureNotRunning(groupName, azure, ctx.Cfg.Azure.ResourceGroup);
        var groupDef = await ContainerGroup(ctx.Cfg, azure, runId, ctx.EnvVars);
        var imageName = ctx.Cfg.Container.ContainerImageName();
        var pipeLog = log.ForContext("Image", imageName).ForContext("Pipe", runId.Name);
        pipeLog.Information("{RunId} - launching  {Image}", runId.ToString(), imageName);
        var sw = Stopwatch.StartNew();
        bool running = false;
        var group = await groupDef.CreateAsync();
        while (true) {
          group = await group.RefreshAsync();
          var state = group.State();

          if (!running && state != ContainerState.Running) {
            pipeLog.Information("{RunId} - container started in {Duration}", runId.ToString(), sw.Elapsed);
            running = true;
          }
          if (!state.IsCompletedState()) {
            await Task.Delay(500);
            continue;
          }

          pipeLog.Information("{RunId} - completed ({Status}) in {Duration}", runId.ToString(), group.State, sw.Elapsed);

          var logTxt = await group.GetLogContentAsync(runId.ContainerName());
          var md = new PipeRunMetadata {
            Id = runId,
            Duration = sw.Elapsed,
            Containers = group.Containers.Select(c => c.Value).ToArray(),
            FinalState = group.State,
            Success = state == ContainerState.Succeeded
          };
          await md.Save(ctx.Store);
          
          await ctx.Store.Save($"{runId.StatePath()}.log.txt", logTxt.AsStream());

          if (state != ContainerState.Succeeded)
            log.Error("{RunId} - failed: {Log}", runId.ToString(), logTxt);

          return md;
        }
      });

      await ids.BlockAction(async c => {
        var groupName = c.ContainerGroupName();
        await azure.ContainerGroups.DeleteByResourceGroupAsync(ctx.Cfg.Azure.ResourceGroup, groupName);
        log.Debug("Deleted container {Container} for {Pipe}", groupName, c.Name);
      });

      return res;
    }

    static async Task EnsureNotRunning(string groupName, IAzure azure, string rg) {
      var group = await azure.ContainerGroups.GetByResourceGroupAsync(rg, groupName);
      if (group != null) {
        if (group.State.HasValue() && group.State == "Running")
          throw new InvalidOperationException("Won't start container - it's not terminated");
        await azure.ContainerGroups.DeleteByIdAsync(group.Id);
      }
    }

    static async Task<IWithCreate> ContainerGroup(PipeAppCfg cfg, IAzure azure, PipeRunId id, IDictionary<string, string> envVars) {
      var container = cfg.Container;
      var rg = cfg.Azure.ResourceGroup;
      var groupName = id.ContainerGroupName();
      await EnsureNotRunning(groupName, azure, rg);

      var args = id.PipeArgs();
      var group = azure.ContainerGroups.Define(groupName)
        .WithRegion(cfg.Container.Region)
        .WithExistingResourceGroup(rg)
        .WithLinux()
        .WithPrivateImageRegistry(container.Registry, container.RegistryCreds.Name, container.RegistryCreds.Secret)
        .WithoutVolume()
        .DefineContainerInstance(id.ContainerName())
        .WithImage(cfg.Container.ContainerImageName())
        .WithoutPorts()
        .WithCpuCoreCount(container.Cores)
        .WithMemorySizeInGB(container.Mem)
        .WithEnvironmentVariables(envVars)
        .WithStartingCommandLine(cfg.Container.Exe, id.PipeArgs())
        .Attach().WithRestartPolicy(ContainerGroupRestartPolicy.Never);

      return group;
    }
  }
}