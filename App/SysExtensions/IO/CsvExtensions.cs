using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;

namespace SysExtensions.IO;

public static class CsvExtensions {
  public static void WriteToCsv<T>(this IEnumerable<T> values, FPath path, CsvConfiguration cfg = null) {
    using (var fs = path.Open(FileMode.Create))
    using (var tw = new StreamWriter(fs)) {
      cfg ??= new CsvConfiguration(CultureInfo.InvariantCulture);
      var csv = new CsvWriter(tw, cfg);
      csv.WriteRecords(values);
    }
  }

  public static CsvConfiguration DefaultConfig => new CsvConfiguration(CultureInfo.InvariantCulture)
    { AllowComments = true, IgnoreBlankLines = true, TrimOptions = TrimOptions.Trim, MissingFieldFound = null };

  public static ICollection<T> ReadFromCsv<T>(this FPath path, CsvConfiguration cfg = null) {
    cfg ??= DefaultConfig;
    using (var fs = path.OpenText()) {
      var csv = new CsvReader(fs, cfg);
      return csv.GetRecords<T>().ToList();
    }
  }

  public static ICollection<T> ReadFromCsv<T>(string data, CsvConfiguration cfg = null) {
    cfg = cfg ?? DefaultConfig;
    using (var tr = new StringReader(data)) {
      var csv = new CsvReader(tr, cfg);
      return csv.GetRecords<T>().ToList();
    }
  }
}