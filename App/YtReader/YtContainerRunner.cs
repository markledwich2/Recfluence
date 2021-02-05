using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CliFx;
using CliFx.Attributes;
using Mutuo.Etl.Pipe;
using Serilog;

namespace YtReader {
  public record YtContainerRunner(AzureContainers Az, ContainerCfg ContainerCfg, PipeAppCtx Ctx, CliEntry Cli, ILogger Log) {
    public async Task Run(string name, string fullImageName = null, CancellationToken cancel = default, bool returnOnStart = false) =>
      await Az.RunContainer(name, fullImageName ?? ContainerCfg.FullContainerImageName(await Az.FindImageTag(ContainerCfg.ImageName)), Ctx.EnvironmentVariables,
        Cli.Args?.Where(a => a != $"-{IContainerCommand.ContainerOption}").ToArray(), returnOnStart, "./recfluence", log: Log, cancel: cancel);
  }

  public interface IContainerCommand : ICommand {
    public const char ContainerOption = 'z';

    [CommandOption(ContainerOption, IsRequired = false)]
    public bool RunOnContainer { get; set; }
  }
}