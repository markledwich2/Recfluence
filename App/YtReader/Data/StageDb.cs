using System.Data;
using Humanizer.Bytes;
using JetBrains.Annotations;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using YtReader.Collect;
using YtReader.Db;

// ReSharper disable InconsistentNaming

namespace YtReader.Data;

public static class StageDb {
  public static string DbName(this SnowflakeCfg cfg) => cfg.DbSuffix.HasValue() ? $"{cfg.Db}_{cfg.DbSuffix}" : cfg.Db;

  public static async Task<DateTime?> LatestTimestamp(this StageTableCfg t, ILoggedConnection<IDbConnection> db) =>
    await db.ExecuteScalar<DateTime?>("latest timestamp", $"select max(v:{t.TsCol ?? "Updated"}::timestamp_ntz) from {t.Table}");

  public static async Task<SfCol[]> TableCols(this StageTableCfg t, ILoggedConnection<IDbConnection> db) {
    var cols = await db.QueryAsync<SfCol>("show columns", $"show columns in table {t.Table}").ToArrayAsync();
    return cols;
  }

  /// <summary>Copies the given files into staging tables MOTE: filesToCopy is relative to the store path. The resulting
  ///   LoadHistoryRow.File paths are converted to be equivalent</summary>
  public static async Task<LoadHistoryRow[]> CopyInto(this StageTableCfg t, ISimpleFileStore store, string sfStage, ILoggedConnection<IDbConnection> db,
    [CanBeNull] SPath[] filesToCopy, ILogger log) {
    if (filesToCopy?.Length > 1000) throw new("copying 1k+ files not implemented");

    var stagePath = new string[] { sfStage, store.BasePathSansContainer().Dot(c => c.IsEmpty ? null : c) }.Concat(t.Dir.Tokens).NotNull().Join("/");

    var cols = await t.TableCols(db); // support subsets of columns (e.g. no loaded or updated columns
    var selectCols = cols.Join(",", c => c.column_name.ToLowerInvariant() switch {
      "v" => "$1 v",
      "loaded" => "sysdate() loaded",
      "updated" => "v:Updated::timestamp_ntz updated",
      _ => throw new($"stage column {c.column_name} not supported")
    });

    await db.Execute("copy into", @$"
copy into {t.Table} from (select {selectCols} from @{stagePath}/)
{filesToCopy.Dot(b => $"files = ({b.Select(f => f.Name).SqlList()})")}
file_format = json
on_error = CONTINUE
");

    var history = await t.LoadHistory(db, store, filesToCopy); // convert to store-relative paths to match filesToCopy
    var loadedFiles = history.Select(h => h.File).ToHashSet();
    var missing = filesToCopy.NotNull().Where(f => !loadedFiles.Contains(f)).Select(f => f.ToString()).ToArray();
    if (missing.Any()) log.Warning("Some files don't look like they have copy history records: {Missing}", missing);
    return history;
  }

  public static CopyIntoResult Summary(this LoadHistoryRow[] history) =>
    new(history.Select(r => r.File).ToArray(), history.Sum(r => r.Rows), history.Sum(r => r.Size),
      history.Any() ? history.Max(r => r.LoadTime) : null);

  /// <summary>Returns load history status for the given files relative to the given store's base path. This is limited to
  ///   history younger than 14 days</summary>
  public static async Task<LoadHistoryRow[]> LoadHistory(this StageTableCfg t, ILoggedConnection<IDbConnection> db, ISimpleFileStore store,
    SPath[] dbPaths = null,
    bool checkIfExists = false) {
    if (checkIfExists) {
      var exists = await db.QueryAsync<SfTable>("LoadHistory - exists", $"show tables like '{t.Table}'").AnyAsync();
      if (!exists) return Array.Empty<LoadHistoryRow>();
    }

    var res = await db.QueryAsync<(string file, long rows, long size, DateTime loadTime)>("LoadHistory", $@"
with all_files as (
    select file_name, row_count, file_size, convert_timezone('UTC', last_load_time)::timestamp_ntz last_load_time_utc
    from table(information_schema.copy_history(table_name=>'{t.Table}', start_time=>to_timestamp_ltz(0)))
)
select * from all_files {(dbPaths == null ? "" : $"where {dbPaths.Select(p => store.BasePathSansContainer().ToAbsolute().Add(p)).SqlInList("file_name")}")}")
      .Select(r => new LoadHistoryRow(r.file.AsSPath().RelativePath(store.BasePathSansContainer()), r.rows, r.size.Bytes(), r.loadTime))
      .ToArrayAsync();
    return res;
  }

  public static async Task CreateTable(this StageTableCfg t, ILoggedConnection<IDbConnection> db, bool ifNotExists = false, bool orReplace = false) =>
    await db.Execute("create table",
      $"create {(orReplace ? "or replace" : "")} table {(ifNotExists ? "if not exists" : "")} {t.Table} (v Variant, loaded timestamp_ntz, updated timestamp_ntz)");

  public static async Task TruncateTable(this StageTableCfg t, ILoggedConnection<IDbConnection> db) => await db.Execute("truncate table",
    $"truncate table {t.Table}");

  public static int SourceFiles(this OptimiseBatch[] plan) => plan.Sum(p => p.SourceFileCount);
}

public record SfCol {
  public string table_name  { get; init; }
  public string schema_name { get; init; }
  public string column_name { get; init; }
  public string data_type   { get; init; }
  public string kind        { get; init; }
}

public record SfTable {
  public string name        { get; init; }
  public string schema_name { get; init; }
  public string kind        { get; init; }
}

public record CopyIntoResult(string[] Files, long Rows, ByteSize Size, DateTime? LastLoadTime);
public record LoadHistoryRow(string File, long Rows, ByteSize Size, DateTime LoadTime);