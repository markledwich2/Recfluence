using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using SysExtensions.Reflection;
using SysExtensions.Text;

namespace Mutuo.Etl.Db {
  public static class DbExtensions {
    static FieldInfo _rowsCopiedField;
    public static string SquareBrackets(this string name) => $"[{name}]";
    public static string DoubleQuote(this string s) => $"\"{s.Replace("\"", "\"\"")}\"";
    public static string SingleQuote(this string s) => $"'{s.Replace("'", "''")}'";

    public static TableSchema Schema(this IDataReader reader) {
      var schemaTable = reader.GetSchemaTable();
      var colNames = schemaTable.Columns.OfType<DataColumn>().Select(c => c.ColumnName).ToHashSet();
      var cols = schemaTable.Rows.Cast<DataRow>().Select(r => {
        var col = new ColumnSchema();
        foreach (var prop in col.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
          if (colNames.Contains(prop.Name))
            col.SetPropValue(prop.Name, r[prop.Name]);
        return col;
      });

      return new TableSchema {
        Columns = cols.ToArray()
      };
    }

    public static DbCommand DbCommand(this IDbConnection conn, string sql) {
      var sfConn = conn as DbConnection ?? throw new InvalidOperationException("requires a snowflake connection");
      var cmd = sfConn.CreateCommand();
      cmd.CommandText = sql;
      return cmd;
    }

    public static async Task<IDataReader> DbExecuteReader(this IDbConnection conn, string sql, object parameter = null) {
      var reader = await DbCommand(conn, sql).ExecuteReaderAsync();
      return reader;
    }

    public static async IAsyncEnumerable<IDictionary<string, object>> AsyncEnumerable(this DbDataReader reader) {
      while (await reader.ReadAsync().ConfigureAwait(false))
        yield return Enumerable.Range(0, reader.FieldCount)
          .ToDictionary(reader.GetName, reader.GetValue);
    }

    public static string Sql(this ICommonDb db, ColumnSchema c) => db.Sql(c.ColumnName);

    public static string Sql(this ICommonDb db, TableId table) =>
      table.Schema.HasValue() ? $"{db.Sql(table.Schema)}.{db.Sql(table.Table)}" : $"{db.Sql(table.Table)}";

    public static bool IsIncremental(this SyncType syncType) => syncType != SyncType.Full;

    public static int GetRowsCopied(this SqlBulkCopy bulkCopy) {
      _rowsCopiedField ??= typeof(SqlBulkCopy).GetField("_rowsCopied", BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Instance);
      return (int) _rowsCopiedField.GetValue(bulkCopy);
    }
  }

  public class TableSchema {
    public TableSchema() { }
    public TableSchema(IEnumerable<ColumnSchema> columns) => Columns = columns.ToArray();

    public IReadOnlyCollection<ColumnSchema> Columns { get; set; }

    public override string ToString() => Columns.Join(", ", c => $"{c.ColumnName} {c.DataType}");
  }

  public class ColumnSchema {
    public ColumnSchema() { }

    public ColumnSchema(string columnName, Type dataType) {
      ColumnName = columnName;
      DataType = dataType;
    }

    public string ColumnName       { get; set; }
    public int    ColumnOrdinal    { get; set; }
    public int    ColumnSize       { get; set; }
    public int?   NumericPrecision { get; set; }
    public int?   NumericScale     { get; set; }
    public bool?  AllowDBNull      { get; set; }
    public Type   DataType         { get; set; }
    public int    ProviderType     { get; set; }
    public string ProviderTypeName { get; set; }
  }
}