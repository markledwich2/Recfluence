using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl {
  /// <summary>
  ///   Ready/write to storage for a keyed collection of items
  /// </summary>
  /// <typeparam name="T"></typeparam>
  public class KeyedCollectionStore<T> where T : class {
    const string Extension = ".json.gz";

    public KeyedCollectionStore(ISimpleFileStore store, Func<T, string> getId, StringPath path) {
      Store = store;
      GetId = getId;
      Path = path;
    }

    ISimpleFileStore Store { get; }
    Func<T, string> GetId { get; }
    StringPath Path { get; }

    public async Task<T> Get(string id) => await Store.Get<T>(Path.Add(id).WithExtension(Extension));
    public async Task Set(T item) => await Store.Set(Path.Add(GetId(item)).WithExtension(Extension), item);
  }

  /// <summary>
  ///   Read/write to storage for an append-only immutable collection of items sored as jsonl.
  ///   Support arbitraty metadata about the collection to allow efficient access?
  /// </summary>
  public class AppendCollectionStore<T> {
    readonly ISimpleFileStore Store;
    readonly StringPath Path;
    readonly Func<T, string> GetTs;
    readonly ILogger Log;

    public AppendCollectionStore(ISimpleFileStore store, StringPath path, Func<T, string> getTs, ILogger log) {
      Store = store;
      Path = path;
      GetTs = getTs;
      Log = log;
    }


    public async Task Append(IReadOnlyCollection<T> items) {
      await using var memStream = new MemoryStream();
      await using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true)) {
        await using var tw = new StreamWriter(zipWriter);
        items.ToJsonl(tw);
      }
      memStream.Seek(0, SeekOrigin.Begin);
      var ts = items.Max(GetTs);
      var path = Path.Add(FileName(ts)).WithExtension(".jsonl.gz");
      var res = await Store.Save(path, memStream).WithDuration();
      Log.Debug("Store - Saved '{Path}' in {Duration}", path, res);
    }

    string FileName(string ts) => $"{ts}.{GuidExtensions.NewShort()}";
    string Ts(StringPath path) => path.NameSansExtension.Split(".").FirstOrDefault();

    /// <summary>
    ///   Returns the most recent appended collection
    /// </summary>
    async Task<StringPath> LatestFile() {
      var files = (await Store.List(Path).SelectManyList()).Where(p => !p.Path.Name.StartsWith("_"));
      var latest = files.OrderByDescending(f => Ts(f.Path)).FirstOrDefault()?.Path;
      return latest;
    }

    public async Task<string> LatestTimestamp() {
      var file = await LatestFile();
      var ts = file?.NameSansExtension.Split(".").FirstOrDefault();
      return ts;
    }

    public async Task<IReadOnlyCollection<T>> LatestItems() {
      var file = await LatestFile();
      return await LoadJsonl(file);
    }

    async Task<IReadOnlyCollection<T>> LoadJsonl(StringPath path) {
      using var stream = await Store.Load(path);
      using var zr = new GZipStream(stream, CompressionMode.Decompress);
      using var tr = new StreamReader(zr);
      return JsonlExtensions.FromJsonL<T>(tr).ToList();
    }
  }
}