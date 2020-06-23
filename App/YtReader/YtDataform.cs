﻿using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;

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

    public async Task Update(ILogger log, bool fullLoad, string[] tables, CancellationToken cancel) {
      var sfCfg = SfCfg.JsonClone();
      sfCfg.Role = "dataform"; // ensure dataform run in its own lower-credentialed role
      sfCfg.Db = sfCfg.DbName(); // serialize the environment specific db name

      var args = new[] {
        fullLoad ? " --full-refresh " : null,
        "--include-deps",
        tables?.Any() == true ? $"--actions {tables.Join(" ", t => t.ToUpperInvariant())}" : "--tags standard"
      }.NotNull().Join(" ");

      var env = new (string name, string value)[] {
        ("SNOWFLAKE_JSON", sfCfg.ToJson()),
        ("REPO", "https://github.com/markledwich2/YouTubeNetworks_Dataform.git"),
        ("BRANCH", "master"),
        ("DATAFORM_RUN_ARGS", args),
        ("SEQ", SeqCfg.SeqUrl.ToString())
      };

      log.Information("Dataform - launching container to update {Db}. dataform {Args}", sfCfg.Db, args);
      var containerName = "dataform";
      var fullName = Cfg.Container.FullContainerImageName("latest");
      var (group, dur) = await Containers.Launch(Cfg.Container, containerName, containerName, fullName,
        env, new string[] { }, log: log, cancel: cancel).WithDuration();
      await group.EnsureSuccess(containerName, log).WithWrappedException("Dataform - container failed");
      log.Information("Dataform - container completed in {Duration}", dur.HumanizeShort());
    }
  }
}