using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualBasic.CompilerServices;
using Serilog;
using Serilog.Core;
using SysExtensions;
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
    readonly string Version;
    
    readonly ILogger Log;

    public AppendCollectionStore(ISimpleFileStore store, StringPath path, Func<T, string> getTs, ILogger log , string version = "") {
      Store = store;
      Path = path;
      GetTs = getTs;
      Log = log;
      Version = version;
    }

    /// <summary>
    ///   Returns the most recent appended collection
    /// </summary>
    async Task<FileListItem> LatestFile() {
      var files = (await Store.List(Path).SelectManyList()).Where(p => !p.Path.Name.StartsWith("_"));
      var latest = files.OrderByDescending(f => StoreFileMd.GetTs(f.Path)).FirstOrDefault();
      return latest;
    }

    public async Task<IReadOnlyCollection<StoreFileMd>> Files() {
      var list = (await Store.List(Path).SelectManyList()).Where(p => !p.Path.Name.StartsWith("_"));
      return list.Select(StoreFileMd.FromFileItem).ToList();
    }

    public async Task<StoreFileMd> LatestFileMetadata() {
      var file = await LatestFile();
      return file == null ? null : StoreFileMd.FromFileItem(file);
    }

    public async Task<IReadOnlyCollection<T>> Items(StringPath path) => await LoadJsonl(path);

    public async Task Append(IReadOnlyCollection<T> items) {
      var ts = items.Max(GetTs);
      var path = StoreFileMd.FilePath(Path, ts, Version);

      await using var memStream = items.ToJsonlGzStream();
      var res = await Store.Save(path, memStream).WithDuration();
      Log?.Debug("Store - Saved '{Path}' in {Duration}", path, res);
    }

    async Task<IReadOnlyCollection<T>> LoadJsonl(StringPath path) {
      await using var stream = await Store.Load(path);
      return stream.LoadJsonlGz<T>();
    }
  }

  public class StoreFileMd {
    public StringPath Path { get; }
    public string Ts { get; }
    public DateTime Modified { get; }
    public string Version { get; }

    public StoreFileMd(StringPath path, string ts, DateTime modified, string version = "0") {
      Path = path;
      Ts = ts;
      Modified = modified;
      Version = version;
    }

    public static StoreFileMd FromFileItem(FileListItem file) {
      var tokens = file.Path.Name.Split(".");
      var ts = tokens.FirstOrDefault();
      var version = tokens.Length >= 4 ? tokens[1] : null;
      return new StoreFileMd(file.Path, ts, file.Modified?.UtcDateTime ?? DateTime.MinValue, version);
    }

    public static StringPath FilePath(StringPath path, string ts, string version) =>
      path.Add(FileName(ts, version));

    public static string FileName(string ts, string version) => 
      $"{ts}.{version}.{GuidExtensions.NewShort()}.jsonl.gz";

    public static string GetTs(StringPath path) => path.Name.Split(".").FirstOrDefault();
  }
}