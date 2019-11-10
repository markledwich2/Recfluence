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
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;

namespace YtReader {
  public static class YtContainerRunner {
    public static async Task<IContainerGroup> StartFleet(ILogger log, Cfg cfg) {
      var sheets = (await ChannelSheets.MainChannels(cfg.App.Sheets, log)).ToList();
      var containerGroup = await ContainerGroup(cfg.App, cfg.App.Container.Name + "-fleet");

      if (sheets.IsEmpty())
        throw new InvalidOperationException("Must have sheets to start fleet");

      IWithNextContainerInstance withInstances = null;
      foreach (var (b, i) in sheets.Batch(cfg.App.ChannelsPerContainer).Select((b, i) => (b, i))) {
        var name = $"{cfg.App.Container.Name}-{i}";
        var args = new[] {"update", "-c", b.Join("|", c => c.Id)};
        withInstances = withInstances == null
          ? containerGroup.DefineFirstInstance(name, cfg, args)
          : withInstances.DefineNextInstance(name, cfg, args);
      }

      log.Information("starting container fleet {Image}", cfg.App.Container.ImageName);
      return await withInstances.WithRestartPolicy(ContainerGroupRestartPolicy.Never).CreateAsync();
    }

    public static async Task<IContainerGroup> Start(ILogger log, Cfg cfg, string[] args) {
      
      log.Information("starting container {Image} {Args}", cfg.App.Container.ImageName, args.Join(" "));

      var containerGroup = await ContainerGroup(cfg.App, cfg.App.Container.Name);

      var withInstances = await containerGroup.DefineFirstInstance(cfg.App.Container.Name, cfg, args)
        .WithRestartPolicy(ContainerGroupRestartPolicy.Never)
        .CreateAsync();

      return withInstances;
    }

    static async Task<IWithFirstContainerInstance> ContainerGroup(AppCfg cfg, string groupName) {
      var sp = cfg.ServicePrincipal;
      var container = cfg.Container;
      var creds = new AzureCredentialsFactory().FromServicePrincipal(sp.ClientId, sp.Secret, sp.TennantId,
        AzureEnvironment.AzureGlobalCloud);
      var azure = Azure.Authenticate(creds).WithSubscription(cfg.SubscriptionId);

      var rg = cfg.ResourceGroup;
      var group = await azure.ContainerGroups.GetByResourceGroupAsync(rg, groupName);
      if (group != null) {
        if (group.State.HasValue() && group.State == "Running")
          throw new InvalidOperationException("Won't start container - it's not terminated");
        await azure.ContainerGroups.DeleteByIdAsync(group.Id);
      }

      var containerGroup = azure.ContainerGroups.Define(groupName)
        .WithRegion(Region.USWest)
        .WithExistingResourceGroup(rg)
        .WithLinux()
        .WithPrivateImageRegistry(container.Registry, container.RegistryCreds.Name, container.RegistryCreds.Secret)
        .WithoutVolume();

      return containerGroup;
    }

    static IWithNextContainerInstance DefineNextInstance(this IWithNextContainerInstance cg, string instanceName, Cfg cfg, string[] args) {
      var container = cfg.App.Container;
      return cg.DefineContainerInstance(instanceName)
        .WithImage($"{container.Registry}/{container.ImageName}")
        .WithoutPorts()
        .WithCpuCoreCount(container.Cores)
        .WithMemorySizeInGB(container.Mem)
        .WithEnvironmentVariables(new Dictionary<string, string> {
          {$"YtNetworks_{nameof(RootCfg.AzureStorageCs)}", cfg.Root.AzureStorageCs},
          {$"YtNetworks_{nameof(RootCfg.Env)}", cfg.Root.Env}
        })
        .WithStartingCommandLine("dotnet", args)
        .Attach();
    }

    static IWithNextContainerInstance DefineFirstInstance(this IWithFirstContainerInstance cg, string instanceName, Cfg cfg, string[] args) {
      var container = cfg.App.Container;
      return cg.DefineContainerInstance(instanceName)
        .WithImage($"{container.Registry}/{container.ImageName}")
        .WithoutPorts()
        .WithCpuCoreCount(container.Cores)
        .WithMemorySizeInGB(container.Mem)
        .WithEnvironmentVariables(new Dictionary<string, string> {
          {$"YtNetworks_{nameof(RootCfg.AzureStorageCs)}", cfg.Root.AzureStorageCs},
          {$"YtNetworks_{nameof(RootCfg.Env)}", cfg.Root.Env}
        })
        .WithStartingCommandLine("dotnet", new[] {"/app/ytnetworks.dll"}.Concat(args).ToArray())
        .Attach();
    }
  }
}