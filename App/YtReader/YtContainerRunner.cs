using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CliFx.Attributes;
using Mutuo.Etl.Pipe;
using Semver;
using Serilog;

namespace YtReader {
  public record YtContainerRunner(AzureContainers Az, ContainerCfg ContainerCfg, PipeAppCtx Ctx, SemVersion Version, CliEntry Cli, ILogger Log) {
    public async Task Run(string name, string fullImageName, CancellationToken cancel) => await Az.RunContainer(
      name, fullImageName ?? ContainerCfg.FullContainerImageName(Version.PipeTag()), Ctx.EnvironmentVariables,
      Cli.Args?.Where(a => a != $"-{IContainerCommand.ContainerOption}").ToArray(), "./recfluence", log: Log, cancel: cancel);
  }

  public interface IContainerCommand {
    public const char ContainerOption = 'z';
    
    [CommandOption(ContainerOption, IsRequired = false)]
    public bool RunOnContainer { get; set; }
  }
}