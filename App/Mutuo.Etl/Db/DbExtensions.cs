using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using SysExtensions.Reflection;
using SysExtensions.Text;

namespace Mutuo.Etl.Db; 

public static class DbExtensions {
  public static string InParentheses(this string name) => $"({name})";
  public static string InSquareBrackets(this string name) => $"[{name}]";
  public static string InDoubleQuote(this string s) => $"\"{s.Replace("\"", "\"\"")}\"";

  /// <summary>Surrounds in single quotes, escapes according to escape chart</summary>
  public static string SingleQuote(this string s, char escape = '\'') => $"'{s.Replace("'", $"{escape}'")}'";

  public static TableSchema Schema(this IDataReader reader) {
    var schemaTable = reader.GetSchemaTable();
    if (schemaTable == null) return null;
    var colNames = schemaTable.Columns.OfType<DataColumn>().Select(c => c.ColumnName).ToHashSet();
    var cols = schemaTable.Rows.Cast<DataRow>().Select(r => {
      var col = new ColumnSchema();
      foreach (var prop in col.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
        if (colNames.Contains(prop.Name))
          col.SetPropValue(prop.Name, r[prop.Name]);
      return col;
    });

    return new() {
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
      yield return Enumerable.Range(start: 0, reader.FieldCount)
        .ToDictionary(reader.GetName, reader.GetValue);
  }
}

public class TableSchema {
  public TableSchema() { }
  public TableSchema(IEnumerable<ColumnSchema> columns) => Columns = columns.ToArray();

  public IReadOnlyCollection<ColumnSchema> Columns { get; set; }

  public override string ToString() => Columns.Join(", ", c => $"{c.ColumnName} {c.DataType}");
}

/// <summary>This is a representation of a columns schema.</summary>
public class ColumnSchema {
  public ColumnSchema() { }

  public ColumnSchema(string columnName, Type dataType) {
    ColumnName = columnName;
    DataType = dataType;
  }

  public bool? Key { get; set; }

  /// <summary>Populated for table schema information in databses</summary>
  public string ProviderTypeName { get; set; }

  /// <summary>Set manually by sync config overrides. Takes precedence over DataType & ProviderTypeName</summary>
  public string ProviderTypeExpression { get; set; }

  public override string ToString() => $"{ColumnName} {ProviderTypeExpression ?? DataType.ToString()}";

  #region standard properties form data reader schema

  public string ColumnName       { get; set; }
  public int    ColumnOrdinal    { get; set; }
  public int    ColumnSize       { get; set; }
  public int?   NumericPrecision { get; set; }
  public int?   NumericScale     { get; set; }
  public bool?  AllowDBNull      { get; set; }
  public Type   DataType         { get; set; }
  /// <summary>set by queries. I beleive corresponds ot type enums in those databse libraries, but we aren't looking at these</summary>
  public int ProviderType { get; set; }

  #endregion
}