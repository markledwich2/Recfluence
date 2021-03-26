using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Mutuo.Etl.Pipe;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;

// ReSharper disable InconsistentNaming

namespace YtReader {
  public record DataScriptsCfg(int Containers = 24, int VideosPerFile = 50_000, int Cores = 4, int Mem = 8, int SpacyBatchSize = 800,
    int? VideoLimit = 500_000, DateTime? Stale = null);

  public record DataScriptRunState(string[] VideoPaths);

  record EntityVideoRow(string video_id);

  public record DataScripts(DataScriptsCfg ScriptsCfg, BlobStores Stores, SnowflakeConnectionProvider Db, AzureContainers containers, SemVersion Version,
    RootCfg RootCfg, ContainerCfg ContainerCfg, AppCfg AppCfg) {
    public async Task Run(ILogger log, CancellationToken cancel, string runId = null) {
      var store = Stores.Store(DataStoreType.Root);
      var env = new (string name, string value)[] {
        ("cfg_sas", GetAppCfgSas()),
        ("env", RootCfg.Env),
        ("branch_env", Version.Prerelease)
      };

      var existingFiles = runId != null;
      runId ??= $"{DateTime.UtcNow.FileSafeTimestamp()}.{ShortGuid.Create(5)}";

      log.Information("DataScripts - runId {runId} ({existing})", runId, existingFiles ? "existing" : "new");

      async Task<List<StringPath>> LoadNewEntityFiles() {
        using var db = await Db.Open(log);
        return await db.QueryBlocking<EntityVideoRow>("new entities", @$"
with ents as (
  select video_id, max(updated) updated
  from video_entity
  group by 1
)
select v.video_id
from video_latest v
       left join ents e on e.video_id = v.video_id
where e.video_id is null {(ScriptsCfg.Stale == null ? "" : "or e.updated < :stale")}
{(ScriptsCfg.VideoLimit == null ? "" : $"limit {ScriptsCfg.VideoLimit}")}
", new {stale = ScriptsCfg.Stale})
          .Batch(ScriptsCfg.VideosPerFile)
          .BlockTrans(async (vids, i) => {
            var path = RunPath(runId).Add($"videos.{i:00000}.jsonl.gz");
            await store.Save(path, await vids.ToJsonlGzStream());
            return path;
          }, AppCfg.DefaultParallel, cancel: cancel).ToListAsync();
      }

      var filesToProcess = existingFiles
        ? await store.List(RunPath(runId), allDirectories: false, log).SelectMany().Select(f => f.Path).ToListAsync()
        : await LoadNewEntityFiles();

      var batches = filesToProcess.Batch(batchSize: 1, ScriptsCfg.Containers).ToArray();
      await batches
        .BlockAction(async (paths, i) => {
          var containerCfg = ContainerCfg with {Cores = ScriptsCfg.Cores, Mem = ScriptsCfg.Mem, ImageName = "datascripts"};
          await containers.RunContainer(
            $"{containerCfg.ImageName}-{DateTime.UtcNow:yyyy-MM-dd-hh-mm}-{i:00}-{ShortGuid.Create(3).Replace("_", "-")}".ToLowerInvariant(),
            containerCfg.FullContainerImageName("latest"),
            env.Concat(("run_state", new DataScriptRunState(paths.Select(p => p.ToString()).ToArray()).ToJson())).ToArray(),
            returnOnStart: false,
            cfg: containerCfg, log: log, cancel: cancel);
          await paths.BlockAction(async p => await store.Delete(p), AppCfg.DefaultParallel);
        }, ScriptsCfg.Containers, cancel: cancel);
    }

    static StringPath RunPath(string runId) => $"pipe/DataScripts/video_entities/{runId}";

    string GetAppCfgSas() {
      var container = new BlobServiceClient(RootCfg.AppStoreCs).GetBlobContainerClient(Setup.CfgContainer);
      var blob = container.GetBlobClient($"{RootCfg.Env}.appcfg.json");
      var sas = container.GenerateSasUri(new(BlobContainerSasPermissions.Read, DateTimeOffset.UtcNow.AddDays(2)) {
        BlobContainerName = blob.BlobContainerName
      });
      return $"{blob.Uri}{sas.Query}";
    }
  }
}