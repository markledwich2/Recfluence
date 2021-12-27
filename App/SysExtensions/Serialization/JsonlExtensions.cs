using System.IO;
using System.IO.Compression;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using SysExtensions.Collections;
using SysExtensions.IO;

namespace SysExtensions.Serialization;

public static class JsonlExtensions {
  public static async Task ToJsonl<T>(this IEnumerable<T> items, TextWriter tw, JsonSerializerSettings settings = null) {
    if (typeof(JObject) is T) {
      var jw = new JsonTextWriter(tw) { Formatting = Formatting.None }; //ignore settings if it is json allready
      foreach (var row in items.Cast<JObject>()) {
        row.WriteTo(jw);
        await tw.WriteLineAsync();
      }
    }
    else {
      var serializer = settings?.Serializer() ?? JsonExtensions.DefaultSerializer;
      serializer.Formatting = Formatting.None;
      foreach (var row in items) {
        serializer.Serialize(tw, row);
        await tw.WriteLineAsync();
      }
    }
  }

  public static async Task ToJsonl<T>(this IAsyncEnumerable<T> items, TextWriter tw, JsonSerializerSettings settings = null) {
    if (typeof(JObject) is T) {
      var jw = new JsonTextWriter(tw) { Formatting = Formatting.None }; //ignore settings if it is json already
      await foreach (var row in items.NotNull()) {
        var jObject = row switch {
          JObject j => j,
          _ => JObject.FromObject(row)
        };
        jObject.WriteTo(jw);
        await tw.WriteLineAsync();
      }
    }
    else {
      var serializer = settings?.Serializer() ?? JsonExtensions.DefaultSerializer;
      serializer.Formatting = Formatting.None;
      await foreach (var row in items) {
        serializer.Serialize(tw, row);
        await tw.WriteLineAsync();
      }
    }
  }

  public static async Task ToJsonl<T>(this IEnumerable<T> items, string filePath, JsonSerializerSettings settings = null) {
    using var tw = new StreamWriter(filePath, append: false);
    await items.ToJsonl(tw, settings);
  }

  public static async Task ToJsonlGz<T>(this IEnumerable<T> items, string filePath, JsonSerializerSettings settings = null) {
    using var fw = File.OpenWrite(filePath);
    using var zipWriter = new GZipStream(fw, CompressionLevel.Optimal, leaveOpen: true);
    using var tw = new StreamWriter(zipWriter);
    await items.ToJsonl(tw, settings);
  }

  public static IEnumerable<T> LoadJsonl<T>(this TextReader tr, JsonSerializerSettings settings = null) where T : class {
    settings ??= JsonExtensions.DefaultSettings();
    if (settings.Formatting != Formatting.None) settings.Formatting = Formatting.None;
    while (true) {
      var line = tr.ReadLine();
      if (line == null)
        break;

      if (typeof(JObject) is T)
        yield return JObject.Parse(line) as T;
      else
        yield return line.ToObject<T>(settings);
    }
  }

  public static JsonSerializerSettings DefaultSettingsForJs() => new() {
    NullValueHandling = NullValueHandling.Ignore,
    DefaultValueHandling = DefaultValueHandling.Include,
    Formatting = Formatting.None,
    Converters = { new StringEnumConverter() },
    ContractResolver = new DefaultContractResolver { NamingStrategy = new CamelCaseNamingStrategy(processDictionaryKeys: true, overrideSpecifiedNames: true) }
  };

  public static IReadOnlyCollection<T> LoadJsonlGz<T>(this Stream stream, JsonSerializerSettings settings = null) where T : class {
    using var zr = new GZipStream(stream, CompressionMode.Decompress);
    using var tr = new StreamReader(zr);
    return tr.LoadJsonl<T>(settings).ToList();
  }

  public static IEnumerable<string> LoadJsonlGzLines(this Stream stream) {
    using var zr = new GZipStream(stream, CompressionMode.Decompress);
    using var tr = new StreamReader(zr);
    while (true) {
      var line = tr.ReadLine();
      if (line == null)
        break;
      yield return line;
    }
  }

  public static async Task<Stream> ToJsonlGzStream<T>(this IEnumerable<T> items, JsonSerializerSettings settings = null) {
    var memStream = new MemoryStream();
    using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, leaveOpen: true)) {
      using var tw = new StreamWriter(zipWriter);
      await items.ToJsonl(tw, settings);
    }
    memStream.Seek(offset: 0, SeekOrigin.Begin);
    return memStream;
  }

  static FPath TempFile(string extension) {
    var path = Path.GetTempPath().AsFPath().Combine("recfluence", $"{ShortGuid.Create()}.{extension}");
    var dir = path.Parent();
    if (!dir.Exists)
      dir.CreateDirectory();
    return path;
  }

  /// <summary>Consumes the stream to a file, then streams the file. Useful to consume large amounts of data detached from the
  ///   source as soon as possible</summary>
  public static async IAsyncEnumerable<T> ConsumeViaJsonl<T>(this IAsyncEnumerable<T> source, JsonSerializerSettings settings = null) where T : class {
    var filePath = TempFile("jsonl");
    using (var tw = filePath.CreateText()) await source.ToJsonl(tw, settings);
    using (var sr = filePath.OpenText())
      foreach (var r in sr.LoadJsonl<T>())
        yield return r;
    filePath.Delete();
  }
}