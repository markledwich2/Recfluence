using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
  public class DataformCfg {
    [Required]
    public ContainerCfg Container { get; set; } = new ContainerCfg {
      Cores = 2,
      Mem = 3,
      ImageName = "dataform",
      Exe = ""
    };
  }

  public class YtDataform {
    readonly AzureContainers Containers;
    readonly DataformCfg     Cfg;
    readonly SnowflakeCfg    SfCfg;
    readonly SeqCfg          SeqCfg;

    public YtDataform(AzureContainers containers, DataformCfg cfg, SnowflakeCfg sfCfg, SeqCfg seqCfg) {
      Containers = containers;
      Cfg = cfg;
      SfCfg = sfCfg;
      SeqCfg = seqCfg;
    }

    public async Task Update(ILogger log) {
      var sfCfg = SfCfg.JsonClone();
      sfCfg.Role = "dataform"; // ensure dataform run in its own lower-credentialed role
      var env = new (string name, string value)[] {
        ("SNOWFLAKE_JSON", sfCfg.ToJson()),
        ("REPO", "https://github.com/markledwich2/YouTubeNetworks_Dataform.git"),
        ("BRANCH", "master"),
        ("DATAFORM_RUN_ARGS", "--include-deps --tags standard"),
        ("SEQ", SeqCfg.SeqUrl.ToString())
      };

      log.Debug("Dataform - launching container");
      var containerName = "dataform";
      var fullName = Cfg.Container.FullContainerImageName("latest");
      var (res, dur) = await Containers.Launch(Cfg.Container, containerName, fullName, env, new string[] { }, log: log).WithDuration();
      if (res.State() == ContainerState.Failed) {
        var content = await res.GetLogContentAsync(containerName);
        log.Error("Dataform - container failed in {Duration}: {LogContent}", dur.HumanizeShort(), content);
      }
      else {
        log.Information("Dataform - container completed in {Duration}", dur.HumanizeShort());
      }
    }
  }
}