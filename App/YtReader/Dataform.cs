using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions.Serialization;

namespace YtReader {
  public class DataformCfg {
    [Required]
    public ContainerCfg Container { get; set; } = new ContainerCfg {
      Cores = 2,
      Mem = 3,
      Exe = "ts-node-script",
      Tag = "latest",
      ImageName = "yt_dataform"
    };
  }

  public class Dataform {
    readonly AzureContainers Containers;
    readonly DataformCfg     Cfg;
    readonly SnowflakeCfg    SfCfg;
    readonly SeqHostCfg      SeqCfg;

    public Dataform(AzureContainers containers, DataformCfg cfg, SnowflakeCfg sfCfg, SeqHostCfg seqCfg) {
      Containers = containers;
      Cfg = cfg;
      SfCfg = sfCfg;
      SeqCfg = seqCfg;
    }

    public async Task Update(ILogger log) {
      var env = new (string name, string value)[] {
        ("SNOWFLAKE_JSON", SfCfg.ToJson()),
        ("REPO", "https://github.com/markledwich2/YouTubeNetworks_Dataform.git"),
        ("BRANCH", "master"),
        ("DATAFORM_RUN_ARGS", "--tags standard"),
        ("SEQ", SeqCfg.SeqUrl.ToString())
      };
      await Containers.Launch(Cfg.Container, "yt_dataform", env, new string[] { }, log: log);
    }
  }
}