using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer.Localisation;
using Serilog;
using SysExtensions.Text;
using SysExtensions.Threading;

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
      var destSchema = await Dest.Schema(destTable);
      var destExists = destSchema != null;

      var syncType = fullLoad || destSchema == null ? SyncType.Full : tableCfg.SyncType;
      if (syncType != SyncType.Full &&
          destSchema?.Columns.Any(c => c.ColumnName.Equals(tableCfg.TsCol, StringComparison.InvariantCultureIgnoreCase)) == false)
        syncType = SyncType.Full;
      if (syncType.IsIncremental() && tableCfg.TsCol.NullOrEmpty())
        throw new InvalidOperationException("table configured for incremental, but no ts column was found");
      var maxTs = syncType.IsIncremental()
        ? await Dest.Connection.ExecuteScalar<object>(nameof(UpdateTable), $"select max({Dest.Sql(tableCfg.TsCol)}) from {Dest.Sql(destTable)}")
        : null;

      // start reading and get schema
      using var reader = await Source.Read(sourceTable, tableCfg, maxTs, limit);
      var querySchema = reader.Schema();
      destSchema ??= querySchema; // if there is no final destination schema, then it should match the source
      // apply overrides to dest schema
      destSchema = new TableSchema(destSchema.Columns.Select(c => {
        var cfg = tableCfg.Cols[c.ColumnName];
        return new ColumnSchema(c.ColumnName, c.DataType) {
          ProviderTypeExpression = cfg?.TypeOverride,
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
      var newRows = await Dest.BulkCopy(reader, loadTable, log);
      log.Debug("{Table} - loaded {Rows} into {LoadTable} ({SyncType})", tableCfg.Name, newRows, loadTable, syncType);

      // if we loaded in to temp table, work out best way to switch this in without downtime
      if (loadTable == tmpTable) {
        if (newRows == 0) {
          await Dest.DropTable(tmpTable); // no new rows, nothing to do
        }
        else if (syncType.IsIncremental() || tableCfg.ManualSchema) { // incremental load, or manual schema. Move the rows into the desitntion table
          var cols = destSchema.Columns;
          var mergeRes = await Dest.Merge(destTable, tmpTable, tableCfg.IdCol, cols);
          log.Debug("{Table} - merged {Records} from {TempTable}", tableCfg.Name, mergeRes, tmpTable);
          await Dest.DropTable(tmpTable);
        }
        else {
          using (var trans = Dest.Connection.Conn.BeginTransaction()) {
            await Dest.DropTable(destTable, trans);
            await Dest.RenameTable(tmpTable, destTable, trans);
            await trans.CommitAsync();
            log.Debug("{Table} - switch out temp table {TempTable}", tableCfg.Name, tmpTable);
          }
        }
      }

      log.Information("{Table} - completed loading {Rows} in {Duration}", tableCfg.Name, newRows, sw.Elapsed.HumanizeShort(minUnit: TimeUnit.Millisecond));
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
        .BlockAction(c => Dest.CreateIndex(destTable, IndexType.Default, new[] {c.Name}));

      var fullTextCols = descColCfgs.Where(c => c?.FullText == true).ToArray();
      if (fullTextCols.Any())
        await Dest.CreateIndex(destTable, IndexType.FullText, fullTextCols.Select(t => t.Name).ToArray());

      log.Debug("{Table} - created because it didn't exist", tableCfg.Name);
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
    public LoggedConnection Connection    { get; }
    public string           DefaultSchema { get; }
    string Sql(string name);
  }

  public enum IndexType {
    Default,
    FullText
  }
}