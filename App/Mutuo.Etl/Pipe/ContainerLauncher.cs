using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Serilog;

namespace Mutuo.Etl.Pipe {
  public interface IContainerLauncher {
    /// <summary>Runs the given container untill completion</summary>
    Task RunContainer(string containerName, string fullImageName, (string name, string value)[] envVars,
      string[] args = null,
      bool returnOnStart = false,
      string exe = null,
      string groupName = null,
      ContainerCfg cfg = null,
      ILogger log = null,
      CancellationToken cancel = default);
  }

  public class ContainerLauncher : IContainerLauncher {
    readonly PipeAppCfg Cfg;
    readonly IPipeCtx   Ctx;

    public ContainerLauncher(PipeAppCfg cfg, IPipeCtx ctx) {
      Cfg = cfg;
      Ctx = ctx;
    }

    public async Task RunContainer(string containerName, string fullImageName, (string name, string value)[] envVars,
      string[] args = null,
      bool returnOnStart = false,
      string exe = null,
      string groupName = null,
      ContainerCfg cfg = null,
      ILogger log = null,
      CancellationToken cancel = default) {
      IContainerLauncher launcher = Cfg.Location switch {
        PipeRunLocation.Container => Ctx.Scope.Resolve<AzureContainers>(),
        _ => Ctx.Scope.Resolve<LocalPipeWorker>()
      };
      await launcher.RunContainer(containerName, fullImageName, envVars, args, returnOnStart, exe, groupName, cfg, log, cancel);
    }
  }
}