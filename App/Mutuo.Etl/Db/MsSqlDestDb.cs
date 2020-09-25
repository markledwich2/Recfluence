using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;

namespace Mutuo.Etl.Db {
  public interface IDestDb : ICommonDb {
    public string DataSource { get; }
    Task RenameTable(TableId from, TableId to, DbTransaction transaction = null);
    Task DropTable(TableId table, DbTransaction transaction = null);
    Task<TableSchema> Schema(TableId table);
    Task CreateTable(TableSchema schema, TableId table, bool withColumnStore = true, DbTransaction transaction = null);
    Task<long> BulkCopy(IDataReader reader, TableId table, ILogger log, CancellationToken cancel);
    string CreateTableSql(TableSchema schema, TableId table, bool withColumnStore = true);
    Task<long> Merge(TableId destTable, TableId tmpTable, string[] idCols, IReadOnlyCollection<ColumnSchema> cols);
    Task CreateIndex(TableId table, string[] cols);
    Task LoadFrom(IEnumerable<StringPath> paths, TableId destTable);
    Task Init(Uri container);
  }

  public class MsSqlDestDb : IDestDb {
    // https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
    static readonly IDictionary<Type, (Type Type, string ProviderName, string[] TypeArgs)> TypeToProvider = new[] {
      (typeof(string), "nvarchar", new[] {"max"}),
      (typeof(DateTime), "datetime2", null),
      (typeof(int), "int", null),
      (typeof(double), "float", null),
      (typeof(bool), "bit", null),
      (typeof(long), "bigint", null),
      (typeof(TimeSpan), "time", null),
      (typeof(byte), "tinyint", null),
      (typeof(decimal), "decimal", null)
    }.ToDictionary(k => k.Item1, v => v);

    static readonly Dictionary<string, (Type Type, string ProviderName, string[] TypeArgs)> ProviderToType =
      TypeToProvider.ToDictionary(t => t.Value.ProviderName, t => t.Value);

    static readonly int              DeaultVarcharSize = 400;
    static readonly int              MaxVarcharSize    = 8000;
    ILoggedConnection<SqlConnection> _c;

    public MsSqlDestDb(SqlConnection conn, string defaultSchema, ILogger log) {
      _c = conn.AsLogged(log);
      DefaultSchema = defaultSchema;
    }

    public ILoggedConnection<IDbConnection> Conn          => _c;
    public string                           DefaultSchema { get; }

    public string Sql(string name) => name.SquareBrackets();

    public string DataSource => $"{DefaultSchema}_blob";

    public async Task RenameTable(TableId from, TableId to, DbTransaction transaction = null) =>
      await Conn.Execute(nameof(RenameTable), $"EXEC sp_rename '{this.Sql(from)}', '{to.Table}'", transaction);

    public async Task DropTable(TableId table, DbTransaction transaction = null) =>
      await Conn.Execute(nameof(DropTable), $"drop table {this.Sql(table)}", transaction);

    public async Task Init(Uri container) {
      if (await Conn.ExecuteScalar<int>("schema exists", $"select count(*) from sys.schemas where name = '{DefaultSchema}'") == 0)
        await Conn.Execute(nameof(Init), $"create schema {DefaultSchema}");
      if (await Conn.ExecuteScalar<int>("data source exists", $"select count(*) from sys.external_data_sources where name = '{DataSource}'") == 0)
        await Conn.Execute(nameof(Init), $"create external data source {DataSource} with ( type = BLOB_STORAGE, location = '{container}')");
    }

    public async Task<TableSchema> Schema(TableId table) {
      var exists = await Conn.ExecuteScalar<int>(nameof(Schema), @$"select count(*) from information_schema.tables 
        where table_name='{table.Table}' and table_schema='{table.Schema}'");

      if (exists == 0)
        return null;

      var cols = await Conn.Query<(string name, string type)>(nameof(Schema),
        @$"select column_name, data_type from information_schema.columns 
        where table_name='{table.Table}' and table_schema='{table.Schema}'");

      var schema = new TableSchema {
        Columns = cols.Select(c => new ColumnSchema {
          ColumnName = c.name,
          ProviderTypeName = c.type,
          DataType = ProviderToType.TryGet(c.type).Type
        }).ToArray()
      };
      return schema;
    }

    public async Task CreateTable(TableSchema schema, TableId table, bool withColumnStore = true, DbTransaction transaction = null) =>
      await Conn.Execute(nameof(CreateTable), CreateTableSql(schema, table, withColumnStore), transaction);

    public string CreateTableSql(TableSchema schema, TableId table, bool withColumnStore = true) {
      var statements = schema.Columns.Select(c => ColumnSql(table, c)).ToList();
      if (withColumnStore)
        statements.Add($"index [{table.Table}_colstore] clustered columnstore");
      var createStatement = $"create table {table} (\n\t{statements.Join(",\n\t")})";
      return createStatement;
    }

    public async Task CreateIndex(TableId table, string[] cols) {
      var sql = @$"create index {Sql($"{table.Table}_{cols.Join("_")}_idx")} on {this.Sql(table)} ({cols.Join(", ", Sql)})";
      await Conn.Execute(nameof(CreateIndex), sql);
    }

    public async Task LoadFrom(IEnumerable<StringPath> paths, TableId destTable) {
      foreach (var path in paths) {
        var sql = @$"bulk insert {this.Sql(destTable)}
        from '{path}'
        with (data_source = '{DataSource}', format='CSV', tablock)";
        await Conn.Execute(nameof(LoadFrom), sql, timeout: 1.Hours());
      }
    }

    public async Task<long> Merge(TableId destTable, TableId tmpTable, string[] idCols, IReadOnlyCollection<ColumnSchema> cols) {
      if (idCols.None()) throw new ArgumentNullException(nameof(idCols), "merge needs and id column");
      var mergeSql = @$"
merge {this.Sql(destTable)} t using {this.Sql(tmpTable)} s 
on {idCols.Join("and", i => $"t.{Sql(i)} = s.{Sql(i)}")}
when matched then update set 
  {cols.Join(",\n\t", c => $"t.{this.Sql(c)} = s.{this.Sql(c)}")}
when not matched by target then 
insert ({cols.Join(",", c => this.Sql(c))})
values ({cols.Join(",", c => $"s.{this.Sql(c)}")})
;";
      // mege operations can take a v long time when large
      return await Conn.ExecuteScalar<int>(nameof(Merge), mergeSql, timeout: 2.Hours());
    }

    public async Task<long> BulkCopy(IDataReader reader, TableId table, ILogger log, CancellationToken cancel) {
      using var bc = new SqlBulkCopy(_c.Conn) {
        EnableStreaming = true,
        BatchSize = 100_000,
        DestinationTableName = this.Sql(table),
        NotifyAfter = 20_000,
        BulkCopyTimeout = 0 // no timeout
      };
      bc.SqlRowsCopied += (sender, args) => log.Debug("{Table} - bulk copied {Rows}", table, args.RowsCopied);

      await bc.WriteToServerAsync(reader, cancel);
      return bc.GetRowsCopied();
    }

    string PkName(TableId table) => Sql($"{table.Table}_pk");

    string ColumnSql(TableId table, ColumnSchema col) {
      // populate any size params
      var sqlType = col.ProviderTypeExpression ??
                    (col.ProviderTypeName ?? MsSqlType(col.DataType)) switch {
                      "nvarchar" => "nvarchar(" + VarcharSize(col) + ")",
                      { } providerType => providerType, // TODO support sized/precision for decimal etc...
                      _ => throw new InvalidOperationException($"no type found for col {col}")
                    };
      var sqlNull = col.AllowDBNull == false ? "not null" : null;
      //var sqlConstraint = col.Key == true ? $"constraint {PkName(table)}  primary key nonclustered" : null;
      return new[] {col.ColumnName, sqlType, sqlNull}.Join(" ");
    }

    static string VarcharSize(ColumnSchema col) {
      if (col.ColumnSize > 0 && col.ColumnSize < MaxVarcharSize) return col.ColumnSize.ToString();
      if (col.Key == true) return col.ColumnSize == 0 ? DeaultVarcharSize.ToString() : MaxVarcharSize.ToString();
      return "max";
    }

    public static string MsSqlType(Type t) =>
      TypeToProvider.TryGet(t).ProviderName ?? throw new InvalidOperationException($"Column data type {t} not implimented");
  }
}