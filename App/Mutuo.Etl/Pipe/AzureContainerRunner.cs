using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
  public class AzureContainerRunner {
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
    public static async Task RunBatch(IPipeCtx ctx, IEnumerable<PipeRunId> ids, ILogger log) {
      var azure = GetAzure(ctx.Cfg);

     var states = await ids.GroupBy(id => (id.Name, id.RunId)).BlockTransform(async g => {
        var groupName = $"{g.Key.Name}_{g.Key.RunId}";
        await EnsureNotRunning(groupName, azure, ctx.Cfg.Azure.ResourceGroup);
        var groupDef = await ContainerGroup(ctx.Cfg, azure, groupName, g.ToArray());
        log.Information("Starting batch of {Containers} containers for {Pipe} {RunId} ", g.Count(), g.First().Name, g.Key.Name, g.Key.RunId);
        var group = await groupDef.CreateAsync();
        while (true) {
          group = await group.RefreshAsync();
          if (!IsCompletedState(@group)) continue;
          
          log.Information("Completed ({Status}) batch of {Containers} containers for {Pipe} {RunId} ", 
            group.State, g.Count(), g.First().Name, g.Key.Name, g.Key.RunId);
          if(@group.State != "Succeeded")
            throw  new InvalidOperationException("One of the processes in the batch failed.");
          return Task.CompletedTask;
        }
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

    static async Task<IWithCreate> ContainerGroup(PipeAppCfg cfg, IAzure azure, string groupName, IReadOnlyCollection<PipeRunId> ids) {
      var container = cfg.Container;
      var rg = cfg.Azure.ResourceGroup;
      await EnsureNotRunning(groupName, azure, rg);
      var groupDef = azure.ContainerGroups.Define(groupName)
        .WithRegion(cfg.Container.Region)
        .WithExistingResourceGroup(rg)
        .WithLinux()
        .WithPrivateImageRegistry(container.Registry, container.RegistryCreds.Name, container.RegistryCreds.Secret)
        .WithoutVolume();

      IWithNextContainerInstance withInstance = null;

      foreach (var id in ids) {
        var instanceName = $"{groupName}_{id.Num}";
        withInstance = (withInstance == null ? groupDef.DefineContainerInstance(instanceName) : withInstance.DefineContainerInstance(instanceName))
          .WithImage($"{container.Registry}/{container.ImageName}:{container.Tag}")
          .WithoutPorts()
          .WithCpuCoreCount(container.Cores)
          .WithMemorySizeInGB(container.Mem)
          .WithEnvironmentVariables(container.ForwardEnvironmentVariables.ToDictionary(e => e, Environment.GetEnvironmentVariable))
          .WithStartingCommandLine("dotnet", new[] {container.EntryAssemblyPath}.Concat(new[] {
            "pipe",
            "-i", id.ToString()
          }).ToArray())
          .Attach();
      }
      if (withInstance == null) throw new InvalidOperationException("ids needs at least one entry");

      var group = withInstance.WithRestartPolicy(ContainerGroupRestartPolicy.Never);

      return group;
    }
  }
}