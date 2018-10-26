using System.Collections.Generic;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using FPath = SysExtensions.Fluent.IO.FPath;

namespace SysExtensions.IO
{
    public static class CsvExtensions
    {
        public static void WriteToCsv<T>(this IEnumerable<T> values, FPath path) {
            using (var fs = path.Open(FileMode.Create))
            using (var tw = new StreamWriter(fs)) {
                var csv = new CsvWriter(tw);
                csv.WriteRecords(values);
            }
        }

        static Configuration DefaultConfig = new Configuration {AllowComments = true, IgnoreBlankLines = true, TrimOptions = TrimOptions.Trim};

        public static ICollection<T> ReadFromCsv<T>(this FPath path, Configuration cfg = null) {
            cfg = cfg ?? DefaultConfig;
            using (var fs = path.OpenText()) {
                var csv = new CsvReader(fs, cfg);
                return csv.GetRecords<T>().ToList();
            }
        }
    }
}
