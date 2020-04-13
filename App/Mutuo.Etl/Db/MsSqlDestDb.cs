using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;

namespace Mutuo.Etl.Db {
  public interface IDestDb : ICommonDb {
    Task RenameTable(TableId from, TableId to, DbTransaction transaction = null);
    Task DropTable(TableId table, DbTransaction transaction = null);
    Task<TableSchema> Schema(TableId table);
    Task CreateTable(TableSchema schema, TableId table, bool withColumnStore = true, DbTransaction transaction = null);
    Task<long> BulkCopy(IDataReader reader, TableId table, ILogger log);
    string CreateTableSql(TableSchema schema, TableId table, bool withColumnStore = true);
    Task<long> Merge(TableId destTable, TableId tmpTable, string idCol, IReadOnlyCollection<ColumnSchema> cols);
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

    public MsSqlDestDb(SqlConnection conn, string defaultSchema) {
      Connection = conn;
      DefaultSchema = defaultSchema;
    }

    public DbConnection Connection    { get; }
    public string       DefaultSchema { get; }

    public string Sql(string name) => name.SquareBrackets();

    public async Task RenameTable(TableId from, TableId to, DbTransaction transaction = null) =>
      await Connection.ExecuteAsync($"EXEC sp_rename '{this.Sql(from)}', '{to.Table}'", transaction: transaction);

    public async Task DropTable(TableId table, DbTransaction transaction = null) =>
      await Connection.ExecuteAsync($"drop table {this.Sql(table)}", transaction: transaction);

    public async Task<TableSchema> Schema(TableId table) {
      var exists = await Connection.ExecuteScalarAsync<int>(@$"select count(*) from information_schema.tables 
        where table_name='{table.Table}' and table_schema='{table.Schema}'");

      if (exists == 0)
        return null;

      var cols = await Connection.QueryAsync<(string name, string type)>(
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
      await Connection.ExecuteAsync(CreateTableSql(schema, table, withColumnStore), transaction: transaction);

    public string CreateTableSql(TableSchema schema, TableId table, bool withColumnStore = true) {
      var statements = schema.Columns.Select(SqlType).ToList();
      if (withColumnStore)
        statements.Add($"index [{table.Table}_idx] clustered columnstore");
      var createStatement = $"create table {table} (\n\t{statements.Join(",\n\t")})";
      return createStatement;
    }

    public async Task<long> Merge(TableId destTable, TableId tmpTable, string idCol, IReadOnlyCollection<ColumnSchema> cols) {
      if (idCol.NullOrEmpty()) throw new ArgumentNullException(nameof(idCol), "merge needs and id column");
      var mergeSql = @$"
merge {this.Sql(destTable)} t using {this.Sql(tmpTable)} s 
on t.{Sql(idCol)} = s.{Sql(idCol)}
when matched then update set 
  {cols.Join(",\n\t", c => $"t.{this.Sql(c)} = s.{this.Sql(c)}")}
when not matched by target then 
insert ({cols.Join(",", c => this.Sql(c))})
values ({cols.Join(",", c => $"s.{this.Sql(c)}")})
;";
      return await Connection.ExecuteScalarAsync<int>(mergeSql);
    }

    public async Task<long> BulkCopy(IDataReader reader, TableId table, ILogger log) {
      using var bc = new SqlBulkCopy((SqlConnection) Connection) {
        EnableStreaming = true,
        BatchSize = 500_000,
        DestinationTableName = this.Sql(table),
        NotifyAfter = 100_000
      };
      bc.SqlRowsCopied += (sender, args) => log.Debug("{Table} - bulk copied {Rows}", table, args.RowsCopied);

      await bc.WriteToServerAsync(reader);
      return bc.GetRowsCopied();
    }

    static string SqlType(ColumnSchema col) {
      var providerType = MsSqlType(col.DataType);
      var sqlType = providerType switch {
        "nvarchar" => "nvarchar(" + (col.ColumnSize == 0 || col.ColumnSize > 4000 ? "max" : col.ColumnSize.ToString()) + ")",
        _ => providerType
      };
      return $"{col.ColumnName} {sqlType}";
    }

    public static string MsSqlType(Type t) =>
      TypeToProvider.TryGet(t).ProviderName ?? throw new InvalidOperationException($"Column data type {t} not implimented");
  }
}