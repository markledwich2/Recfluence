﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;
using Newtonsoft.Json;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using static YtReader.Db.ScriptMode;

namespace YtReader.Db {
  public class WarehouseCreator {
    readonly WarehouseCfg                WhCfg;
    readonly StorageCfg                  StorageCfg;
    readonly BranchEnvCfg                EnvCfg;
    readonly VersionInfo                 Version;
    readonly SnowflakeConnectionProvider Sf;

    public WarehouseCreator(WarehouseCfg whCfg, StorageCfg storageCfg, BranchEnvCfg envCfg, VersionInfo version, SnowflakeConnectionProvider sf) {
      WhCfg = whCfg;
      StorageCfg = storageCfg;
      EnvCfg = envCfg;
      Version = version;
      Sf = sf;
    }

    static readonly JsonSerializerSettings JCfg = JsonExtensions.DefaultSettings(Formatting.None);

    public async Task CreateIfNotExists(BranchState state, ILogger log) {
      var sw = Stopwatch.StartNew();

      var schema = Sf.Cfg.Schema;
      var container = StorageCfg.Container(Version.Version);
      var sas = container.GetSharedAccessSignature(new SharedAccessBlobPolicy {
        SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddYears(100),
        Permissions = SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read
      });
      var stageUrl = $"azure://{container.Uri.Host}{container.Uri.AbsolutePath}";

      using var conn = await Sf.OpenConnection(log, "", ""); // connection sans db & schema. If you specify ones that doesn't exist, all queries hang.

      var db = Sf.Cfg.DbName();
      IEnumerable<Script> scripts;
      var dbComment = new DbComment {
        Expires = DateTime.UtcNow.AddDays(2),
        Email = EnvCfg.Email
      }.ToJson(JCfg);
      if (state == BranchState.CopyProd)
        scripts = new[] {
            new Script("db copy", @$"create database if not exists {db} clone {Sf.Cfg.Db} comment='{dbComment}'")
          }
          .Concat(WhCfg.Roles.Select(r =>
            new Script($"init role {r}", $"grant all on database {db} to role {r}")
          ));
      else
        scripts = new[] {
            new Script("db create", @$"create database if not exists {db} comment='{dbComment}'"),
            new Script("schema", $"create schema if not exists {db}.{schema}")
          }
          .Concat(WhCfg.Roles.Select(r =>
            new Script($"init role {r}", ScriptMode.Parallel,
              $"grant all on database {db} to role {r}",
              $"grant all on schema {db}.{schema} to role {r}",
              $"grant all on all tables in schema {db}.{schema} to role {r}",
              $"grant all on future tables in schema {db}.{schema} to role {r}",
              $"grant all on all views in schema {db}.{schema} to role {r}",
              $"grant all on future views in schema {db}.{schema} to role {r}"
            )));

      scripts = scripts
        .Concat(new Script("stage",
          $"create or replace stage {db}.{schema}.yt_data url='{stageUrl}' credentials=(azure_sas_token='{sas}') file_format=(type=json compression=gzip)"
        ));

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