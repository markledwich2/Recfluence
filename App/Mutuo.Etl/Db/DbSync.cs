using System;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;

namespace Mutuo.Etl.Db {
  public class DbSync {
    readonly IDestDb   Dest;
    readonly ISourceDb Source;

    public DbSync(ISourceDb source, IDestDb dest) {
      Source = source;
      Dest = dest;
    }

    public async Task UpdateTable(SyncTableCfg tableCfg, ILogger log, bool fullLoad = false, int limit = 0) {
      var sw = Stopwatch.StartNew();
      var sourceTable = new TableId(Source.DefaultSchema, tableCfg.Name);
      var destTable = new TableId(Dest.DefaultSchema, tableCfg.Name);
      var existingDestSchema = await Dest.Schema(destTable);

      var syncType = fullLoad || existingDestSchema == null ? SyncType.Full : tableCfg.SyncType;
      if (syncType != SyncType.Full &&
          existingDestSchema?.Columns.Any(c => c.ColumnName.Equals(tableCfg.TsCol, StringComparison.InvariantCultureIgnoreCase)) == false)
        syncType = SyncType.Full;
      if (syncType.IsIncremental() && tableCfg.TsCol.NullOrEmpty())
        throw new InvalidOperationException("table configured for incremental, but no ts column was found");
      var maxTs = syncType.IsIncremental()
        ? await Dest.Connection.ExecuteScalarAsync($"select max({Dest.Sql(tableCfg.TsCol)}) from {Dest.Sql(destTable)}")
        : null;

      // start reading
      using var reader = await Source.Read(sourceTable, tableCfg, maxTs, limit);

      // create table if required
      var sourceSchema = reader.Schema();
      if (existingDestSchema == null) {
        await Dest.CreateTable(sourceSchema, destTable);
        log.Debug("{Table} - created because it didn't exist", destTable);
      }

      // prepare tmp table if required
      var tmpTable = destTable.WithTable($"{destTable.Table}_tmp");
      var loadTable = existingDestSchema != null ? tmpTable : destTable;
      if (loadTable == tmpTable) {
        if (await Dest.Schema(tmpTable) != null)
          await Dest.DropTable(tmpTable);
        await Dest.CreateTable(sourceSchema, tmpTable);
      }

      // copy data
      var newRows = await Dest.BulkCopy(reader, loadTable, log);
      log.Debug("{Table} - loaded {Rows} into {LoadTable} ({SyncType})", destTable, newRows, loadTable, syncType);

      // shuffle for temp loads
      if (loadTable == tmpTable) {
        if(newRows == 0)
          await Dest.DropTable(tmpTable);
        else if (syncType.IsIncremental()) {
          var cols = sourceSchema.Columns;
          var mergeRes = await Dest.Merge(destTable, tmpTable, tableCfg.IdCol, cols);
          log.Debug("{Table} - merged {Records} from {TempTable}", destTable, mergeRes, tmpTable);
          await Dest.DropTable(tmpTable);
        }
        else {
          using (var trans = Dest.Connection.BeginTransaction()) {
            await Dest.DropTable(destTable, trans);
            await Dest.RenameTable(tmpTable, destTable, trans);
            await trans.CommitAsync();
            log.Debug("{Table} - switch out temp table {TempTable}", destTable, tmpTable);
          }
        }
      }

      log.Information("{Table} - completed loading {Rows} in {Duration}", destTable, newRows, sw.Elapsed.HumanizeShort());
    }
  }

  public enum SyncType {
    Incremental,
    Full
  }

  public class SyncTableCfg {
    public SyncTableCfg(string name, params SyncColCfg[] cols) {
      Name = name;
      Cols.AddRange(cols);
    }

    public SyncTableCfg() { }

    public            SyncType                             SyncType     { get; set; }
    [Required] public string                               Name         { get; set; }
    [Required] public IKeyedCollection<string, SyncColCfg> Cols         { get; set; } = new KeyedCollection<string, SyncColCfg>(c => c.Name);
    public            string[]                             SelectedCols { get; set; } = { };

    public string TsCol => Cols.FirstOrDefault(c => c.Ts)?.Name;
    public string IdCol => Cols.FirstOrDefault(c => c.Id)?.Name;
  }

  public class SyncColCfg {
    public string Name         { get; set; }
    public bool   Ts           { get; set; }
    public bool   Id           { get; set; }
    public string TypeOverride { get; set; }
  }

  public interface ICommonDb {
    public DbConnection Connection    { get; }
    public string       DefaultSchema { get; }
    string Sql(string name);
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
}