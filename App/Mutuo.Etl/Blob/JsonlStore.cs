using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Newtonsoft.Json;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Blob {

  public interface IJsonlStore {
    StringPath Path { get; }
    ISimpleFileStore Store { get; }

    /// <summary>returns the latest file (either in landing or staging) within the given partition</summary>
    /// <param name="partition"></param>
    /// <returns></returns>
    Task<StoreFileMd> LatestFile(string partition = null);

    Task<IReadOnlyCollection<StoreFileMd>> Files(StringPath path, bool allDirectories = false);
    
  }
  
  /// <summary>Read/write to storage for an append-only immutable collection of items sored as jsonl</summary>
  public class JsonlStore<T> : IJsonlStore {
    static readonly long          TargetBytes = (long)10.Megabytes().Bytes;
    readonly        Func<T, string> GetPartition;
    readonly        Func<T, string> GetTs;

    readonly ILogger          Log;
    readonly int              Parallel;
    public ISimpleFileStore Store { get; }
    readonly string           Version;
    
    public StringPath Path { get; }

    /// <summary></summary>
    /// <param name="getTs">A function to get a timestamp for this file. This must always be greater for new records using an
    ///   invariant string comparer</param>
    public JsonlStore(ISimpleFileStore store, StringPath path, Func<T, string> getTs,
      ILogger log, string version = "", Func<T, string> getPartition = null, int parallel = 8) {
      Store = store;
      Path = path;
      GetTs = getTs;
      Log = log;
      GetPartition = getPartition;
      Parallel = parallel;
      Version = version;
    }

    string Partition(T item) => GetPartition?.Invoke(item);

    /// <summary>The land path for a given partition is where files are first put before being optimised. Default -
    ///   [Path]/[Partition], LandAndStage - [Path]/land/[partition]</summary>
    StringPath FilePath(string partition = null) => partition.NullOrEmpty() ? Path : Path.Add(partition);

    /// <summary>returns the latest file (either in landing or staging) within the given partition</summary>
    /// <param name="partition"></param>
    /// <returns></returns>
    public async Task<StoreFileMd> LatestFile(string partition = null) => await LatestFile(FilePath(partition));

    /// <summary>Returns the most recent file within this path (any child directories)</summary>
    async Task<StoreFileMd> LatestFile(StringPath path) {
      var files = await Files(path, true);
      var latest = files.OrderByDescending(f => StoreFileMd.GetTs(f.Path)).FirstOrDefault();
      return latest;
    }

    public Task<IReadOnlyCollection<StoreFileMd>> Files(StringPath path, bool allDirectories = false) => Store.Files(path, allDirectories);
    
    public async Task<IReadOnlyCollection<T>> Items(StringPath path) => await LoadJsonl(path);

    public async Task Append(IReadOnlyCollection<T> items, ILogger log = null) {
      log ??= Log;
      if (items.None()) return;
      await items.GroupBy(Partition).BlockAction(async g => {
        var ts = items.Max(GetTs);
        var path = JsonlStoreExtensions.FilePath(FilePath(g.Key), ts, Version);
        using var memStream = await items.ToJsonlGzStream(new JsonSerializerSettings());
        await Store.Save(path, memStream, log).WithDuration();
      }, Parallel);
    }

    async Task<IReadOnlyCollection<T>> LoadJsonl(StringPath path) {
      await using var stream = await Store.Load(path);
      return stream.LoadJsonlGz<T>();
    }
  }

  public class StoreFileMd {
    public StoreFileMd(StringPath path, string ts, DateTime modified, long bytes, string version = null) {
      Path = path;
      Ts = ts;
      Modified = modified;
      Bytes = bytes;
      Version = version;
    }

    public StringPath Path     { get; }
    public string     Ts       { get; }
    public DateTime   Modified { get; }
    public string     Version  { get; }
    public long       Bytes    { get; }

    public static StoreFileMd FromFileItem(FileListItem file) {
      var tokens = file.Path.Name.Split(".");
      var ts = tokens.FirstOrDefault();
      var version = tokens.Length >= 4 ? tokens[1] : null;
      return new StoreFileMd(file.Path, ts, file.Modified?.UtcDateTime ?? DateTime.MinValue, file.Bytes, version);
    }

    public static string GetTs(StringPath path) => path.Name.Split(".").FirstOrDefault();
  }
}