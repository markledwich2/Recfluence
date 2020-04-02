using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
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

    public static IEnumerable<T> LoadJsonlGz<T>(this TextReader tr) {
      while (true) {
        var line = tr.ReadLine();
        if (line == null)
          break;
        yield return JsonConvert.DeserializeObject<T>(line);
      }
    }

    public static IReadOnlyCollection<T> LoadJsonlGz<T>(this Stream stream) {
      using var zr = new GZipStream(stream, CompressionMode.Decompress);
      using var tr = new StreamReader(zr);
      return tr.LoadJsonlGz<T>().ToList();
    }

    public static Stream ToJsonlGzStream<T>(this IEnumerable<T> items) {
      var memStream = new MemoryStream();
      using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true)) {
        using var tw = new StreamWriter(zipWriter);
        items.ToJsonl(tw);
      }
      memStream.Seek(0, SeekOrigin.Begin);
      return memStream;
    }
  }
}