using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Serilog;
using SysExtensions.Fluent.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace Mutuo.Etl.Blob {
  public interface ISimpleFileStore {
    Task Save(StringPath path, FPath file, ILogger log = null);
    Task Save(StringPath path, Stream contents, ILogger log = null);
    Task<Stream> Load(StringPath path, ILogger log = null);
    IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(StringPath path, bool allDirectories = false, ILogger log = null);
    Task<bool> Delete(StringPath path, ILogger log = null);
    Task<Stream> OpenForWrite(StringPath path, ILogger log = null);
  }

  public static class SimpleStoreExtensions {
    public static StringPath AddJsonExtention(this StringPath path, bool zip = true) =>
      new StringPath(path + (zip ? ".json.gz" : ".json"));

    public static async Task<T> GetOrCreate<T>(this ISimpleFileStore store, StringPath path, Func<T> create = null) where T : class, new() {
      var o = await store.Get<T>(path);
      if (o == null) {
        o = create == null ? new T() : create();
        await store.Set(path, o);
      }
      return o;
    }

    public static async Task<T> Get<T>(this ISimpleFileStore store, StringPath path, bool zip = true, ILogger log = null) where T : class {
      using var stream = await store.Load(path.AddJsonExtention(zip), log);
      if (!zip) return stream.ToObject<T>();
      await using var zr = new GZipStream(stream, CompressionMode.Decompress, true);
      return zr.ToObject<T>();
    }

    /// <summary>Serializes item into the object store</summary>
    /// <param name="store"></param>
    /// <param name="path">The path to the object (no extensions)</param>
    /// <param name="item"></param>
    /// <param name="zip"></param>
    /// <param name="log"></param>
    public static async Task Set<T>(this ISimpleFileStore store, StringPath path, T item, bool zip = true, ILogger log = null) {
      await using var memStream = new MemoryStream();

      if (zip)
        await using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true)) {
          await using var tw = new StreamWriter(zipWriter, Encoding.UTF8);
          JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);
        }
      else
        await using (var tw = new StreamWriter(memStream, Encoding.UTF8, leaveOpen: true))
          JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);

      var fullPath = path.AddJsonExtention(zip);
      memStream.Seek(0, SeekOrigin.Begin);

      await store.Save(fullPath, memStream, log);
    }
  }
}