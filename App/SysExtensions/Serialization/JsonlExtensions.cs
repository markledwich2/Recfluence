using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace SysExtensions.Serialization {
  public static class JsonlExtensions {
    public static void ToJsonl<T>(this IEnumerable<T> items, TextWriter tw) {
      var serializer = new JsonSerializer {Formatting = Formatting.None};
      foreach (var row in items) {
        serializer.Serialize(tw, row);
        tw.WriteLine();
      }
    }

    public static void ToJsonl<T>(this IEnumerable<T> items, string filePath) {
      using var tw = new StreamWriter(filePath, false);
      items.ToJsonl(tw);
    }

    public static IEnumerable<T> FromJsonL<T>(TextReader tr) {
      while (true) {
        var line = tr.ReadLine();
        if (line == null)
          break;
        yield return JsonConvert.DeserializeObject<T>(line);
      }
    }
  }
}