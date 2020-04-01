using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Medallion.Shell;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.ContainerInstance.Fluent.ContainerGroup.Definition;
using Microsoft.Azure.Management.ContainerInstance.Fluent.Models;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  public interface IContainerRunner {
    /// <summary>
    ///   Run a batch of containers. Must have already created state for them.
    ///   Waits till the batch is complete and returns the status.
    /// </summary>
    Task RunBatch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log);
  }

  public static class ContainerRunnerEx {
    public static string ContainerGroupName(this PipeRunId runid) => runid.ToString();
    public static string ContainerImageName(this ContainerCfg cfg) => $"{cfg.Registry}/{cfg.ImageName}:{cfg.Tag}";
    public static string[] PipeArgs(this PipeRunId runId) => new[] {"pipe", "-r", runId.ToString()};
  }

  public class LocalContainerRunner : IContainerRunner {
    public async Task RunBatch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) =>
      await ids.BlockAction(async id => {
        var args = new[] {"run", "--rm", "-i", ctx.Cfg.Container.ContainerImageName()}.Concat(id.PipeArgs()).ToArray<object>();
        var cmd = Command.Run("docker", args).RedirectTo(Console.Out);

        var res = await cmd.Task;

        if (!res.Success) {
          var error = await cmd.StandardError.ReadToEndAsync();
          throw new InvalidOperationException($"'docker ({args.Join(" ")})' failed with: {error}");
        }
      });
  }

  public class ThreadContainerRunner : IContainerRunner {
    public async Task RunBatch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) =>
      await ids.BlockAction(async b => await new PipeCtx(ctx, b).RunPipe(), ctx.Cfg.LocalParallel);
  }

  public class AzureContainerRunner : IContainerRunner {
    static IAzure GetAzure(PipeAppCfg cfg) {
      var sp = cfg.Azure.ServicePrincipal;
      var creds = new AzureCredentialsFactory().FromServicePrincipal(sp.ClientId, sp.Secret, sp.TennantId, AzureEnvironment.AzureGlobalCloud);
      var azure = Azure.Authenticate(creds).WithSubscription(cfg.Azure.SubscriptionId);
      return azure;
    }

    /// <summary>
    ///   Run a batch of containers. Must have already created state for them.
    ///   Waits till the batch is complete and returns the status.
    /// </summary>
    public async Task RunBatch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) {
      var azure = GetAzure(ctx.Cfg);


      await ids.BlockAction(async c => {
        var groupName = c.ContainerGroupName();
        await EnsureNotRunning(groupName, azure, ctx.Cfg.Azure.ResourceGroup);
        var groupDef = await ContainerGroup(ctx.Cfg, azure, c);
        log.Information("Starting {Container} {Pipe}", groupName, c.Name);
        var group = await groupDef.CreateAsync();
        while (true) {
          group = await group.RefreshAsync();
          if (!IsCompletedState(group)) continue;

          log.Information("Completed ({Status}) {Container} {Pipe} ", group.State, groupName, c.Name);
          if (group.State != "Succeeded")
            throw new InvalidOperationException("One of the processes in the batch failed.");
        }
      });

      await ids.BlockAction(async c => {
        var groupName = c.ContainerGroupName();
        await azure.ContainerGroups.DeleteByResourceGroupAsync(ctx.Cfg.Azure.ResourceGroup, groupName);
        log.Debug("Deleted container {Container} for {Pipe}", groupName, c.Name);
      });
    }

    static bool IsCompletedState(IContainerGroup group) => group.State.In("Succeeded", "Failed");

    static async Task EnsureNotRunning(string groupName, IAzure azure, string rg) {
      var group = await azure.ContainerGroups.GetByResourceGroupAsync(rg, groupName);
      if (group != null) {
        if (group.State.HasValue() && group.State == "Running")
          throw new InvalidOperationException("Won't start container - it's not terminated");
        await azure.ContainerGroups.DeleteByIdAsync(group.Id);
      }
    }

    static async Task<IWithCreate> ContainerGroup(PipeAppCfg cfg, IAzure azure, PipeRunId id) {
      var container = cfg.Container;
      var rg = cfg.Azure.ResourceGroup;
      var groupName = id.ContainerGroupName();
      await EnsureNotRunning(id.ContainerGroupName(), azure, rg);

      var args = id.PipeArgs();
      var group = azure.ContainerGroups.Define(groupName)
        .WithRegion(cfg.Container.Region)
        .WithExistingResourceGroup(rg)
        .WithLinux()
        .WithPrivateImageRegistry(container.Registry, container.RegistryCreds.Name, container.RegistryCreds.Secret)
        .WithoutVolume()
        .DefineContainerInstance(groupName)
        .WithImage($"{container.Registry}/{container.ImageName}:{container.Tag}")
        .WithoutPorts()
        .WithCpuCoreCount(container.Cores)
        .WithMemorySizeInGB(container.Mem)
        .WithEnvironmentVariables(container.ForwardEnvironmentVariables.ToDictionary(e => e, Environment.GetEnvironmentVariable))
        .WithStartingCommandLine(args.First(), id.PipeArgs().Skip(1).ToString())
        .Attach().WithRestartPolicy(ContainerGroupRestartPolicy.Never);

      return group;
    }
  }
}