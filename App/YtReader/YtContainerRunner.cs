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
using SysExtensions.Threading;

namespace YtReader {
  public static class YtContainerRunner {
    public static async Task<IReadOnlyCollection<IContainerGroup>> StartFleet(ILogger log, Cfg cfg) {
      var sheets = (await ChannelSheets.MainChannels(cfg.App.Sheets, log)).ToList();
      var evenBatchSize = (int)Math.Ceiling(sheets.Count / Math.Ceiling( sheets.Count / (double)cfg.App.ChannelsPerContainer));
      
      var batches = sheets.Batch(evenBatchSize).Select((b, i) => (batch:b.ToList(), name: $"{cfg.App.Container.Name}-fleet-{i}")).ToList();

      var azure = GetAzure(cfg);

      // before starting feel. Ensure they are all not already running
      await batches.BlockAction(async b => await EnsureNotRunning(b.name, azure, cfg.App.ResourceGroup), cfg.App.DefaultParallel);

      var fleet = await batches.BlockTransform(async b => {
        var (batch, fleetName) = b;
        var args = new[] {"update", "-c", batch.Join("|", c => c.Id)};
        var group = await ContainerGroup(cfg, azure, fleetName, args);
        return await group.CreateAsync();
      }, cfg.App.DefaultParallel);
      
      log.Information("Started fleet  containers: ", fleet.Join(", ", f => f.Name));
      return fleet;
    }

    public static async Task<IContainerGroup> Start(ILogger log, Cfg cfg, string[] args) {
      log.Information("starting container {Image} {Args}", cfg.App.Container.ImageName, args.Join(" "));
      var containerGroup = await ContainerGroup(cfg, GetAzure(cfg), cfg.App.Container.Name, args);
      return await containerGroup.CreateAsync();
    }

    static async Task<IWithCreate> ContainerGroup(Cfg cfg, IAzure azure, string groupName, string[] args) {
      var sp = cfg.App.ServicePrincipal;
      var container = cfg.App.Container;

      var rg = cfg.App.ResourceGroup;
      await EnsureNotRunning(groupName, azure, rg);

      var containerGroup = azure.ContainerGroups.Define(groupName)
        .WithRegion(Region.USWest)
        .WithExistingResourceGroup(rg)
        .WithLinux()
        .WithPrivateImageRegistry(container.Registry, container.RegistryCreds.Name, container.RegistryCreds.Secret)
        .WithoutVolume()
        .DefineContainerInstance(groupName)
        .WithImage($"{container.Registry}/{container.ImageName}")
        .WithoutPorts()
        .WithCpuCoreCount(container.Cores)
        .WithMemorySizeInGB(container.Mem)
        .WithEnvironmentVariables(new Dictionary<string, string> {
          {$"YtNetworks_{nameof(RootCfg.AzureStorageCs)}", cfg.Root.AzureStorageCs},
          {$"YtNetworks_{nameof(RootCfg.Env)}", cfg.Root.Env}
        })
        .WithStartingCommandLine("dotnet", new[] {"/app/ytnetworks.dll"}.Concat(args).ToArray())
        .Attach()
        .WithRestartPolicy(ContainerGroupRestartPolicy.Never);
      return containerGroup;
    }

    static IAzure GetAzure(Cfg cfg) {
      var sp = cfg.App.ServicePrincipal;
      var creds = new AzureCredentialsFactory().FromServicePrincipal(sp.ClientId, sp.Secret, sp.TennantId, AzureEnvironment.AzureGlobalCloud);
      var azure = Azure.Authenticate(creds).WithSubscription(cfg.App.SubscriptionId);
      return azure;
    }

    static async Task EnsureNotRunning(string groupName, IAzure azure, string rg) {
      var group = await azure.ContainerGroups.GetByResourceGroupAsync(rg, groupName);
      if (@group != null) {
        if (@group.State.HasValue() && @group.State == "Running")
          throw new InvalidOperationException("Won't start container - it's not terminated");
        await azure.ContainerGroups.DeleteByIdAsync(@group.Id);
      }
    }
  }
}