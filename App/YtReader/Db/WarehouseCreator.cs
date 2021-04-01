using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using static Azure.Storage.Sas.BlobContainerSasPermissions;
using static YtReader.Db.ScriptMode;
using static YtReader.BranchState;

namespace YtReader.Db {
  public class WarehouseCreator {
    readonly WarehouseCfg                WhCfg;
    readonly StorageCfg                  StorageCfg;
    readonly BranchEnvCfg                EnvCfg;
    readonly VersionInfo                 Version;
    readonly SnowflakeConnectionProvider Sf;
    readonly BlobStores                  Stores;

    public WarehouseCreator(WarehouseCfg whCfg, StorageCfg storageCfg, BranchEnvCfg envCfg, VersionInfo version, SnowflakeConnectionProvider sf,
      BlobStores stores) {
      WhCfg = whCfg;
      StorageCfg = storageCfg;
      EnvCfg = envCfg;
      Version = version;
      Sf = sf;
      Stores = stores;
    }

    static readonly JsonSerializerSettings JCfg = JsonExtensions.DefaultSettings(Formatting.None);

    public async Task CreateOrReplace(BranchState state, ILogger log) {
      var sw = Stopwatch.StartNew();

      var schema = Sf.Cfg.Schema;
      var store = Stores.Store(DataStoreType.DbStage);
      var container = store.Container;
      var sasUri = container.GenerateSasUri(List | Read, DateTimeOffset.UtcNow.AddYears(100));
      var stageUrl = $"azure://{sasUri.Host}{sasUri.AbsolutePath}";
      var sasToken = sasUri.Query;

      using var conn = await Sf.Open(log, "", ""); // connection sans db & schema. If you specify ones that doesn't exist, all queries hang.

      var db = Sf.Cfg.DbName();
      var dbComment = new DbComment {
        Expires = DateTime.UtcNow.AddDays(2),
        Email = EnvCfg.Email
      }.ToJson(JCfg);

      var scripts = (state.In(Clone, CloneDb)
        ? new[] {new Script("db copy", @$"create or replace database {db} clone {Sf.Cfg.Db} comment='{dbComment}'")}
        : new[] {
            new Script("db create", @$"create or replace database {db} comment='{dbComment}'"),
            new Script("schema", $"create schema if not exists {db}.{schema}"),
          })
          .Concat(new Script("stage",
            $"create or replace stage {db}.{schema}.yt_data url='{stageUrl}' credentials=(azure_sas_token='{sasToken}') file_format=(type=json compression=gzip)",
            $"create or replace file format {db}.{schema}.json type = 'json'",
            $"create or replace file format {db}.{schema}.json_zst type = 'json' compression = ZSTD",
            $"create or replace file format {db}.{schema}.tsv type = 'csv' field_delimiter = '\t' validate_UTF8 = false  NULL_IF=('')",
            $"create or replace file format {db}.{schema}.tsv_header type = 'csv' field_delimiter = '\t' validate_UTF8 = false  NULL_IF=('') skip_header=1 field_optionally_enclosed_by ='\"'",
            $"create or replace file format {db}.{schema}.tsv_header_no_enclose type = 'csv' field_delimiter = '\t' validate_UTF8 = false  NULL_IF=('') skip_header=1"
          ))
          .Concat(
            WhCfg.AdminRoles.Select(r =>
              new Script($"init role {r}", ScriptMode.Parallel,
                $"grant all on database {db} to role {r}",
                $"grant all on schema {db}.{schema} to role {r}",
                $"grant all on all tables in schema {db}.{schema} to role {r}",
                $"grant all on future tables in schema {db}.{schema} to role {r}",
                $"grant all on all views in schema {db}.{schema} to role {r}",
                $"grant all on future views in schema {db}.{schema} to role {r}",
                $"grant all on all stages in database {db} to role {r}"
              )))
          .Concat(WhCfg.ReadRoles.Select(r =>
            new Script($"init role {r}", ScriptMode.Parallel,
              $"grant usage,monitor on database {db} to role {r}",
              $"grant usage, monitor on all schemas in database {db} to role {r}",
              $"grant select on future tables in database {db} to role {r}",
              $"grant select on future views in database {db} to role {r}",
              $"grant select on all tables in database {db} to role {r}",
              $"grant select on all views in database {db} to role {r}",
              $"grant usage on all stages in database {db} to role {r}"
            )));

      foreach (var s in scripts)
        await s.Sqls.BlockAction(q => conn.Execute(s.Name, q), s.Mode == Sequential ? 1 : WhCfg.MetadataParallel);

      log.Information("Create Warehouse - {Db} created/updated in {Duration}", db, sw.Elapsed.HumanizeShort());
    }
  }

  class DbComment {
    public DateTime? Expires { get; set; }
    public string    Email   { get; set; }
  }

  class Script {
    public string     Name { get; }
    public ScriptMode Mode { get; }
    public string[]   Sqls { get; }

    public Script(string name, ScriptMode mode, params string[] sqls) {
      Name = name;
      Mode = mode;
      Sqls = sqls;
    }

    public Script(string name, params string[] sqls) : this(name, Sequential, sqls) { }
  }

  enum ScriptMode {
    Sequential,
    Parallel
  }
}