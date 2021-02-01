using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Humanizer.Bytes;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Db {
  public class DbSync {
    readonly IDestDb            Dest;
    readonly AzureBlobFileStore Store;
    readonly ISourceDb          Source;

    public DbSync(ISourceDb source, IDestDb dest, AzureBlobFileStore store) {
      Source = source;
      Dest = dest;
      Store = store;
    }

    /// <summary>Legacy bulk copy, tmp table switching version</summary>
    public async Task UpdateTable(SyncTableCfg tableCfg, ILogger log, CancellationToken cancel, bool fullLoad = false, int limit = 0,
      SyncMode mode = SyncMode.Blob) {
      var sw = Stopwatch.StartNew();


      await Dest.Init(Store.ContainerUrl);

      var sourceSql = tableCfg.Sql ?? $"select * from {Source.DefaultSchema}.tableCfg.Name";

      var destTable = new TableId(Dest.DefaultSchema, tableCfg.Name);
      var destSchema = await Dest.Schema(destTable);
      var destExists = destSchema != null;

      var syncType = fullLoad || destSchema == null ? SyncType.Full : tableCfg.SyncType;
      if (syncType != SyncType.Full &&
          destSchema?.Columns.Any(c => c.ColumnName.Equals(tableCfg.TsCol, StringComparison.InvariantCultureIgnoreCase)) == false)
        syncType = SyncType.Full;
      if (syncType.IsIncremental() && tableCfg.TsCol.NullOrEmpty())
        throw new InvalidOperationException("table configured for incremental, but no ts column was found");
      var maxTs = syncType.IsIncremental()
        ? await Dest.Conn.ExecuteScalar<object>(nameof(UpdateTable), $"select max({Dest.Sql(tableCfg.TsCol)}) from {Dest.Sql(destTable)}")
        : null;

      // start reading and get schema. if we are blowwing, do this to get the schema without loading any rows
      using var reader = await Source.Read(sourceSql, tableCfg, maxTs, mode == SyncMode.Blob ? 0 : limit);
      var querySchema = reader.Schema();
      destSchema ??= querySchema; // if there is no final destination schema, then it should match the source
      // apply overrides to dest schema
      destSchema = new TableSchema(destSchema.Columns.Select(c => {
        var cfg = tableCfg.Cols[c.ColumnName];
        return new ColumnSchema(c.ColumnName, c.DataType) {
          ProviderTypeExpression = cfg?.SqlType,
          Key = cfg?.Id,
          AllowDBNull = cfg?.Null
        };
      }));

      // create table if not exists
      if (!destExists) await CreateDestTable(destTable, tableCfg, destSchema, log);

      // prepare tmp table if required
      var tmpTable = destTable.WithTable($"{destTable.Table}_tmp");
      var loadTable = destExists ? tmpTable : destTable;

      if (loadTable == tmpTable)
        await CreateTmpTable(tmpTable, querySchema);

      // copy data
      var newRows = 0L;
      var newBytes = 0.Bytes();
      var loadId = DateTime.UtcNow.FileSafeTimestamp();
      if (mode == SyncMode.Blob) {
        newBytes += await LoadBLobData(tableCfg, log, loadId, sourceSql, maxTs, loadTable);
      }
      else {
        newRows = await Dest.BulkCopy(reader, loadTable, log, cancel);
        log.Debug("Sync {Table} - loaded {Rows} into {LoadTable} ({SyncType})", tableCfg.Name, newRows, loadTable, syncType);
      }

      // if we loaded in to temp table, work out best way to switch this in without downtime
      if (loadTable == tmpTable) {
        if (newRows == 0 && newBytes == 0.Bytes()) {
          await Dest.DropTable(tmpTable); // no new rows, nothing to do
        }
        else if (syncType.IsIncremental() || tableCfg.ManualSchema) { // incremental load, or manual schema. Move the rows into the desitntion table
          var cols = destSchema.Columns;
          var mergeRes = await Dest.Merge(destTable, tmpTable, tableCfg.IdCols, cols);
          log.Debug("Sync {Table} - merged {Records} from {TempTable}", tableCfg.Name, mergeRes, tmpTable);
          await Dest.DropTable(tmpTable);
        }
        else {
          // there may be moments where the table dissapears.I removed the transaction to get past this error: BeginExecuteNonQuery requires the command to have a transaction when the connection assigned to the command is in a pending local transaction.  The Transaction property of the command has not been initialized.
          //using (var trans = await Dest.Conn.Conn.BeginTransactionAsync(IsolationLevel.ReadUncommitted, cancel)) {
          await Dest.DropTable(destTable);
          await Dest.RenameTable(tmpTable, destTable);
          /*await trans.CommitAsync();
        }*/
          log.Debug("Sync {Table} - switch out temp table {TempTable}", tableCfg.Name, tmpTable);
        }
      }

      log.Information("Sync {Table} - completed loading {Size} in {Duration}",
        tableCfg.Name, newBytes > 0.Bytes() ? newBytes.Humanize("#,#") : newRows.ToString("#,#"), sw.Elapsed.HumanizeShort());
    }

    async Task<ByteSize> LoadBLobData(SyncTableCfg tableCfg, ILogger log, string loadId, string sourceSql, object maxTs, TableId loadTable) {
      var path = StringPath.Relative("sync", tableCfg.Name, loadId);
      var copyTask = Source.CopyTo(path, sourceSql, tableCfg, maxTs);
      var loadedFiles = new KeyedCollection<StringPath, FileListItem>(f => f.Path);
      while (true) { // load as the files are created
        if (copyTask.IsFaulted) break;
        var toLoad = (await Store.List(path).SelectManyList())
          .Where(f => !loadedFiles.ContainsKey(f.Path)).ToArray();
        if (toLoad.None()) {
          if (copyTask.IsCompleted)
            break;
          await 5.Seconds().Delay();
          continue;
        }
        log.Debug("Sync {Table} - loading: {Files}", tableCfg.Name, toLoad.Join("|", l => l.Path.ToString()));
        await Dest.LoadFrom(toLoad.Select(f => f.Path), loadTable);
        loadedFiles.AddRange(toLoad);
        await toLoad.BlockAction(f => Store.Delete(f.Path, log), parallel: 8);
      }

      log.Information("Sync {Table} - copied {Files} files ({Size})", tableCfg.Name, loadedFiles.Count,
        loadedFiles.Sum(f => f.Bytes ?? 0).Bytes().Humanize("#,#"));
      return loadedFiles.Sum(f => f.Bytes ?? 0).Bytes();
    }

    async Task CreateTmpTable(TableId tmpTable, TableSchema querySchema) {
      if (await Dest.Schema(tmpTable) != null)
        await Dest.DropTable(tmpTable);
      await Dest.CreateTable(querySchema, tmpTable, false); // temp tables created without indexes or sans-pk containts
    }

    async Task CreateDestTable(TableId destTable, SyncTableCfg tableCfg, TableSchema destSchema, ILogger log) {
      await Dest.CreateTable(destSchema, destTable, tableCfg.ColStore);
      var descColCfgs = destSchema.Columns.Select(c => tableCfg.Cols[c.ColumnName]).ToArray();
      await descColCfgs.Where(c => c?.Index == true)
        .BlockAction(c => Dest.CreateIndex(destTable, new[] {c.Name}));


      log.Debug("Sync {Table} - created because it didn't exist", tableCfg.Name);
    }
  }

  public class TableId {
    public TableId(string schema, string table) {
      Schema = schema ?? throw new ArgumentNullException(nameof(schema));
      Table = table ?? throw new ArgumentNullException(nameof(table));
    }

    public TableId() { }

    public string Schema { get; set; }
    public string Table  { get; set; }

    public TableId WithTable(string table) => new TableId(Schema, table);
    public override string ToString() => Schema.HasValue() ? $"{Schema}.{Table}" : $"{Table}";
  }

  public interface ICommonDb {
    public ILoggedConnection<IDbConnection> Conn          { get; }
    public string                           DefaultSchema { get; }
    string Sql(string name);
  }

  public enum SyncMode {
    Blob,
    BulkCopy
  }
}