using System.Diagnostics;
using JetBrains.Annotations;
using Mutuo.Etl.Blob;
using Newtonsoft.Json;
using YtReader.Data;
using YtReader.Store;
using static Azure.Storage.Sas.BlobContainerSasPermissions;
using static YtReader.Db.ScriptMode;
using static YtReader.Db.WarehouseCreateMode;
using static YtReader.Store.DataStoreType;

namespace YtReader.Db;

public enum WarehouseCreateMode {
  CreateAndReplace,
  CloneProd,
  CreateSchemaIfNotExists
}

public class WarehouseCreator {
  static readonly JsonSerializerSettings      JCfg = JsonExtensions.DefaultSettings(Formatting.None);
  readonly        BranchEnvCfg                EnvCfg;
  readonly        SnowflakeConnectionProvider Sf;
  readonly        BlobStores                  Stores;
  readonly        WarehouseCfg                WhCfg;

  static readonly string Scope = nameof(WarehouseCreator);

  public WarehouseCreator(WarehouseCfg whCfg, BranchEnvCfg envCfg, SnowflakeConnectionProvider sf, BlobStores stores) {
    WhCfg = whCfg;
    EnvCfg = envCfg;
    Sf = sf;
    Stores = stores;
  }

  public async Task CreateOrReplace(WarehouseCreateMode mode, [CanBeNull] string schema, ILogger log) {
    if (mode == CloneProd && Sf.Cfg.DbSuffix.NullOrEmpty())
      throw new(
        "DbSuffix needs to be set when cloning a db. Typically you should set warehouse.mode to \"Branch\" in local.appcfg.json to create a dev warehouse");

    var sw = Stopwatch.StartNew();
    schema ??= Sf.Cfg.Schema;

    var stages = new[] { DbStage, Private }.Select(type => {
      var store = Stores.Store(type);
      var container = ((AzureBlobFileStore)store).Container;
      var sasUri = container.GenerateSasUri(List | Read, DateTimeOffset.UtcNow.AddYears(100));
      var name = type switch {
        Private => "yt_private",
        DbStage => "yt_data",
        _ => throw new($"store type {type} has no stage")
      };
      return new { Uri = $"azure://{sasUri.Host}{sasUri.AbsolutePath}", Sas = sasUri.Query, Name = name };
    }).ToArray();

    if (mode == CreateSchemaIfNotExists) {
      using var dbConn = await Sf.Open(log, schema: "");
      var schemaExists = await dbConn.ExecuteScalar<bool>($"{Scope} - schema exits",
        $"select exists(select * from information_schema.schemata where catalog_name = current_database() and schema_name ilike '{schema}')");
      if (schemaExists) return;
    }

    // use a non-contextual connection for creating databases/schema's
    using var conn = await Sf.Open(log, "", "");

    var db = Sf.Cfg.DbName();
    var dbComment = new DbComment {
      Expires = DateTime.UtcNow.AddDays(2),
      Email = EnvCfg.Email
    }.ToJson(JCfg);


    var scripts = (mode.In(CloneProd)
        ? new[] { new Script("db copy", @$"create or replace database {db} clone {Sf.Cfg.Db} comment='{dbComment}'") }
        : new[] {
          new Script("db create", @$"create database if not exists {db} comment='{dbComment}'"), // don't replace
          new Script("schema", $"create schema if not exists {db}.{schema}")
        })
      .Concat(new Script("file formats",
        $"create or replace file format {db}.{schema}.json type = 'json'",
        $"create or replace file format {db}.{schema}.json_zst type = 'json' compression = ZSTD",
        $"create or replace file format {db}.{schema}.tsv type = 'csv' field_delimiter = '\t' validate_UTF8 = false  NULL_IF=('')",
        $"create or replace file format {db}.{schema}.tsv_header type = 'csv' field_delimiter = '\t' validate_UTF8 = false  NULL_IF=('') skip_header=1 field_optionally_enclosed_by ='\"'",
        $"create or replace file format {db}.{schema}.tsv_header_no_enclose type = 'csv' field_delimiter = '\t' validate_UTF8 = false  NULL_IF=('') skip_header=1"))
      .Concat(stages.Select(s => new Script($"stage {s.Name}",
        $"create or replace stage {db}.{schema}.{s.Name} url='{s.Uri}' credentials=(azure_sas_token='{s.Sas}') file_format=(type=json compression=gzip)"
      )))
      .Concat(WhCfg.AdminRoles.Select(r => new Script($"init role {r}", ScriptMode.Parallel,
        $"grant all on database {db} to role {r}",
        $"grant all on schema {db}.{schema} to role {r}",
        $"grant all on all tables in schema {db}.{schema} to role {r}",
        $"grant all on future tables in schema {db}.{schema} to role {r}",
        $"grant all on all views in schema {db}.{schema} to role {r}",
        $"grant all on future views in schema {db}.{schema} to role {r}",
        $"grant all on all stages in database {db} to role {r}",
        $"grant all on all functions in database {db} to role {r}"
      )))
      .Concat(WhCfg.ReadRoles.Select(r => new Script($"init role {r}", ScriptMode.Parallel,
        $"grant usage,monitor on database {db} to role {r}",
        $"grant usage, monitor on all schemas in database {db} to role {r}",
        $"grant select on future tables in schema {db}.{schema} to role {r}",
        $"grant select on future views in schema {db}.{schema} to role {r}",
        $"grant select on all tables in schema {db}.{schema} to role {r}",
        $"grant select on all views in schema {db}.{schema} to role {r}",
        $"grant usage on all stages in schema {db}.{schema} to role {r}",
        $"grant usage on all functions in schema {db}.{schema} to role {r}",
        $"grant usage on all file formats in schema {db}.{schema} to role {r}"
      )));

    foreach (var s in scripts)
      await s.Sqls.BlockDo<string>(q => conn.Execute(s.Name, q), s.Mode == Sequential ? 1 : WhCfg.MetadataParallel);

    log.Information("Create Warehouse - {Db} created/updated in {Duration}", db, sw.Elapsed.HumanizeShort());
  }
}

class DbComment {
  public DateTime? Expires { get; set; }
  public string    Email   { get; set; }
}

class Script {
  public Script(string name, ScriptMode mode, params string[] sqls) {
    Name = name;
    Mode = mode;
    Sqls = sqls;
  }

  public Script(string name, params string[] sqls) : this(name, Sequential, sqls) { }
  public string     Name { get; }
  public ScriptMode Mode { get; }
  public string[]   Sqls { get; }
}

enum ScriptMode {
  Sequential,
  Parallel
}