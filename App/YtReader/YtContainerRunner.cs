using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.ContainerInstance.Fluent.Models;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Serilog;
using SysExtensions.Text;

namespace YtReader {
    public static class YtContainerRunner {

        public static Task<IContainerGroup> Start(ILogger log, Cfg cfg) => Start(log, cfg, new string[] {});

        public static async Task<IContainerGroup> Start(ILogger log, Cfg cfg, string[] args) {

            var sp = cfg.App.ServicePrincipal;
            var container = cfg.App.Container;
            var creds = new AzureCredentialsFactory().FromServicePrincipal(sp.ClientId, sp.Secret, sp.TennantId, AzureEnvironment.AzureGlobalCloud);
            var azure = Azure.Authenticate(creds).WithSubscription(cfg.App.SubscriptionId);

            var sub = azure.GetCurrentSubscription();

            var rg = cfg.App.ResourceGroup;
            var group = await azure.ContainerGroups.GetByResourceGroupAsync(rg, container.Name);
            if (group != null)
                await azure.ContainerGroups.DeleteByIdAsync(group.Id);

            var cArgs = new string[] {"/app/ytnetworks.dll", "update"}.Concat(args).ToArray();
            log.Information("starting container {Image} {Args}", container.ImageName, cArgs.Join(" "));
            var containerGroup = await azure.ContainerGroups.Define(container.Name)
                .WithRegion(Region.USWest)
                .WithExistingResourceGroup(rg)
                .WithLinux()
                .WithPrivateImageRegistry(container.Registry, container.RegistryCreds.Name, container.RegistryCreds.Secret)
                .WithoutVolume()
                .DefineContainerInstance(container.Name)
                    .WithImage($"{container.Registry}/{container.ImageName}")
                    .WithoutPorts()
                    .WithCpuCoreCount(container.Cores)
                    .WithMemorySizeInGB(container.Mem)
                    .WithEnvironmentVariable($"YtNetworks_{nameof(RootCfg.AzureStorageCs)}", cfg.Root.AzureStorageCs)
                    .WithStartingCommandLine("dotnet", cArgs)
                    .Attach()
                .WithRestartPolicy(ContainerGroupRestartPolicy.Never)
                .CreateAsync();

            return containerGroup;
        }
    }
}
