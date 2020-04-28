using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Newtonsoft.Json;

namespace SysExtensions.Serialization {
  public static class JsonlExtensions {
    public static void ToJsonl<T>(this IEnumerable<T> items, TextWriter tw, JsonSerializerSettings settings = null) {
      var serializer = settings?.Serializer() ?? JsonExtensions.DefaultSerializer;
      serializer.Formatting = Formatting.None;
      foreach (var row in items) {
        serializer.Serialize(tw, row);
        tw.WriteLine();
      }
    }

    public static void ToJsonl<T>(this IEnumerable<T> items, string filePath, JsonSerializerSettings settings = null) {
      using var tw = new StreamWriter(filePath, false);
      items.ToJsonl(tw, settings);
    }

    public static void ToJsonlGz<T>(this IEnumerable<T> items, string filePath, JsonSerializerSettings settings = null) {
      using var fw = File.OpenWrite(filePath);
      using var zipWriter = new GZipStream(fw, CompressionLevel.Optimal, true);
      using var tw = new StreamWriter(zipWriter);
      items.ToJsonl(tw, settings);
    }

    public static IEnumerable<T> LoadJsonlGz<T>(this TextReader tr, JsonSerializerSettings settings = null) {
      settings ??= JsonExtensions.DefaultSettings();
      if (settings.Formatting != Formatting.None) settings.Formatting = Formatting.None;
      while (true) {
        var line = tr.ReadLine();
        if (line == null)
          break;
        yield return line.ToObject<T>(settings);
      }
    }

    public static IReadOnlyCollection<T> LoadJsonlGz<T>(this Stream stream, JsonSerializerSettings settings = null) {
      using var zr = new GZipStream(stream, CompressionMode.Decompress);
      using var tr = new StreamReader(zr);
      return tr.LoadJsonlGz<T>(settings).ToList();
    }

    public static Stream ToJsonlGzStream<T>(this IEnumerable<T> items, JsonSerializerSettings settings = null) {
      var memStream = new MemoryStream();
      using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true)) {
        using var tw = new StreamWriter(zipWriter);
        items.ToJsonl(tw, settings);
      }
      memStream.Seek(0, SeekOrigin.Begin);
      return memStream;
    }
  }
}