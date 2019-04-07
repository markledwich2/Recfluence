using System.Collections.Generic;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using SysExtensions.Fluent.IO;

namespace SysExtensions.IO {
  public static class CsvExtensions {
    public static void WriteToCsv<T>(this IEnumerable<T> values, FPath path, Configuration cfg = null) {
      using (var fs = path.Open(FileMode.Create))
      using (var tw = new StreamWriter(fs)) {
        cfg = cfg ?? new Configuration();
        var csv = new CsvWriter(tw, cfg);
        csv.WriteRecords(values);
      }
    }

    static Configuration DefaultConfig => new Configuration
      {AllowComments = true, IgnoreBlankLines = true, TrimOptions = TrimOptions.Trim, MissingFieldFound = null};

    public static ICollection<T> ReadFromCsv<T>(this FPath path, Configuration cfg = null) {
      cfg = cfg ?? DefaultConfig;
      using (var fs = path.OpenText()) {
        var csv = new CsvReader(fs, cfg);
        return csv.GetRecords<T>().ToList();
      }
    }

    public static ICollection<T> ReadFromCsv<T>(string data, Configuration cfg = null) {
      cfg = cfg ?? DefaultConfig;
      using (var tr = new StringReader(data)) {
        var csv = new CsvReader(tr, cfg);
        return csv.GetRecords<T>().ToList();
      }
    }
  }
}