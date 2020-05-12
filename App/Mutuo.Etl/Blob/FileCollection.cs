using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Humanizer.Localisation;
using Newtonsoft.Json;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Blob {
  /// <summary>Ready/write to storage for a keyed collection of items</summary>
  /// <typeparam name="T"></typeparam>
  public class KeyedCollectionStore<T> where T : class {
    public KeyedCollectionStore(ISimpleFileStore store, Func<T, string> getId, StringPath path) {
      Store = store;
      GetId = getId;
      Path = path;
    }

    ISimpleFileStore Store { get; }
    Func<T, string>  GetId { get; }
    StringPath       Path  { get; }

    public async Task<T> Get(string id) => await Store.Get<T>(Path.Add(id));
    public async Task Set(T item) => await Store.Set(Path.Add(GetId(item)), item);
  }

  public interface IAppendCollectionStore {
    /// <summary>Process new files in land into stage. Note on the different modes: - LandAndStage: Immutable operation, load
    ///   data downstream from the stage directory - Default: Mutates the files in the same directory. Downstream operations:
    ///   don't run while processing, and fully reload after this is complete</summary>
    Task Optimise(ILogger log);
  }

  /// <summary>Read/write to storage for an append-only immutable collection of items sored as jsonl</summary>
  public class AppendCollectionStore<T> : IAppendCollectionStore {
    static readonly double          TargetBytes = 10.Megabytes().Bytes;
    readonly        Func<T, string> GetPartition;
    readonly        Func<T, string> GetTs;

    readonly ILogger          Log;
    readonly int              Parallel;
    readonly StringPath       Path;
    readonly ISimpleFileStore Store;
    readonly string           Version;

    /// <summary></summary>
    /// <param name="store"></param>
    /// <param name="path"></param>
    /// <param name="partition"></param>
    /// <param name="getTs">A function to get a timestamp for this file. This must always be greater for new records using an
    ///   invariant string comparer</param>
    /// <param name="log"></param>
    /// <param name="version"></param>
    /// <param name="mode"></param>
    public AppendCollectionStore(ISimpleFileStore store, StringPath path, Func<T, string> getTs,
      ILogger log, string version = "", Func<T, string> getPartition = null, int parallel = 8) {
      Store = store;
      Path = path;
      GetTs = getTs;
      Log = log;
      GetPartition = getPartition;
      Parallel = parallel;
      Version = version;
    }

    /// <summary>Process new files in land into stage. Note on the different modes: - LandAndStage: Immutable operation, load
    ///   data downstream from the stage directory - Default: Mutates the files in the same directory. Downstream operations:
    ///   don't run while processing, and fully reload after this is complete</summary>
    public async Task Optimise(ILogger log) {
      var landPath = FilePath();

      log = log.ForContext("Path", landPath);

      log.Information("Optimise {Path} - started", landPath);
      var sw = Stopwatch.StartNew();

      // all partition landing files (will group using directories)
      log.Debug("Optimise {Path} - reading current files", landPath);
      var (byPartition, duration) = await FilesByPartition(landPath).WithDuration();
      log.Debug("Optimise {Path} - read {Files} files across {Partitions} partitions in {Duration}",
        landPath, byPartition.Sum(p => p.Count()), byPartition.Length, duration.HumanizeShort());

      var res = await byPartition.BlockTransform(p => Optimise(p.Key, p, log), Parallel);
      var stats = (optimiseIn: res.Sum(r => r.optimisedIn), optimiseOut: res.Sum(r => r.optimisedOut));

      log.Information("Optimise {Path} - optimised {FilesIn} into {FilesOut} files in {Duration}",
        landPath, stats.optimiseIn, stats.optimiseOut, sw.Elapsed.HumanizeShort());
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

    public async Task<IReadOnlyCollection<StoreFileMd>> Files(StringPath path, bool allDirectories = false) {
      var list = (await Store.List(path, allDirectories).SelectManyList()).Where(p => !p.Path.Name.StartsWith("_"));
      return list.Select(StoreFileMd.FromFileItem).ToList();
    }

    public async Task<IReadOnlyCollection<T>> Items(StringPath path) => await LoadJsonl(path);

    public async Task Append(IReadOnlyCollection<T> items, ILogger log = null) {
      log ??= Log;
      if (items.None()) return;
      await items.GroupBy(Partition).BlockAction(async g => {
        var ts = items.Max(GetTs);
        var path = StoreFileMd.FilePath(FilePath(g.Key), ts, Version);
        await using var memStream = items.ToJsonlGzStream(new JsonSerializerSettings());
        await Store.Save(path, memStream, log).WithDuration();
      }, Parallel);
    }

    /// <summary>Join ts (timestamp) contiguous files together until they are > MaxBytes</summary>
    async Task<(long optimisedIn, long optimisedOut)> Optimise(string partition, IEnumerable<StoreFileMd> files, ILogger log) {
      var toProcess = files.OrderBy(f => f.Ts).ToQueue();
      var destPath = FilePath(partition);

      log.Debug("Optimise {Path} - Processing {Files} files in partition {Partition}",
        destPath, toProcess.Count, partition);

      var currentBatch = new List<StoreFileMd>();
      var optimisePlan = new List<StoreFileMd[]>();

      if (toProcess.None()) return (0, 0);

      while (toProcess.Any()) {
        var file = toProcess.Dequeue();

        var (nextBytes, nextIsFurther) = BatchSize(file);
        if (nextBytes > TargetBytes && nextIsFurther) // if adding this file will make it too big, optimise the current batch as is
          PlanCurrentBatch();

        currentBatch.Add(file);
        if (toProcess.None() || currentBatch.Sum(f => f.Bytes) > TargetBytes) // join if big enough, or this is the last batch
          PlanCurrentBatch();
      }

      (long nextBytes, bool nextIsFurther) BatchSize(StoreFileMd file) {
        var bytes = currentBatch.Sum(f => f.Bytes);
        var nextBytes = bytes + file.Bytes;
        var nextIsFurther = nextBytes - TargetBytes > TargetBytes - bytes;
        return (nextBytes, nextIsFurther);
      }

      void PlanCurrentBatch() {
        if (currentBatch.Count > 1) // only plan a batch if there is more than one file in it
          optimisePlan.Add(currentBatch.ToArray());
        currentBatch.Clear();
      }

      if (optimisePlan.None()) {
        log.Debug("Optimise {Path} - already optimal", destPath);
      }
      else {
        log.Debug("Optimise {Path} - staring to execute optimisation plan", destPath);
        await optimisePlan.Select((b, i) => (b, i)).BlockAction(async b => {
          var (batch, i) = b;
          var optimiseRes = await JoinFiles(batch, destPath).WithDuration();
          log.Debug("Optimise {Path} - optimised file {OptimisedFile} from {FilesIn} in {Duration}. batch {Batch}/{Total}",
            destPath, optimiseRes.Result, batch.Length, optimiseRes.Duration.HumanizeShort(), i+1, optimisePlan.Count);
        }, Parallel);
      }
      return (optimisePlan.Sum(p => p.Length), optimisePlan.Count);
    }

    async Task<StringPath> JoinFiles(IReadOnlyCollection<StoreFileMd> toOptimise, StringPath destPath) {
      var optimisedFile = StoreFileMd.FilePath(destPath, toOptimise.Last().Ts, Version);
      using (var joinedStream = new MemoryStream()) {
        using (var zipWriter = new GZipStream(joinedStream, CompressionLevel.Optimal, true)) {
          var inStreams = await toOptimise.BlockTransform(async s => {
            var inStream = await Store.Load(s.Path, Log).WithDuration();
            return inStream.Result;
          }, Parallel);
          foreach (var s in inStreams) {
            using var zr = new GZipStream(s, CompressionMode.Decompress, false);
            zr.CopyTo(zipWriter);
          }
        }
        joinedStream.Seek(0, SeekOrigin.Begin);
        await Store.Save(optimisedFile, joinedStream);
      }

      // when in-place, this is dirty if we fail now. There is no transaction capability in cloud storage, so downstream process must handle duplicates
      // successfully staged files, delete from land. Incremental using TS will work without delete, but it's more efficient to delete process landed files.
      await toOptimise.BlockAction(f => Store.Delete(f.Path), Parallel)
        .WithWrappedException(e => "Failed to delete optimised files. Duplicate records need to be handled downstream");
      Log.Debug("Optimise {Path} - deleted {Files} that were optimised into {OptimisedFile}",
        destPath, toOptimise.Count, optimisedFile);

      return optimisedFile;
    }

    async Task<IGrouping<string, StoreFileMd>[]> FilesByPartition(StringPath landPath) =>
      (await Files(landPath, true))
      .GroupBy(f => GetPartition == null ? null : f.Path.Parent.Name)
      .ToArray();

    async Task<IReadOnlyCollection<T>> LoadJsonl(StringPath path) {
      await using var stream = await Store.Load(path);
      return stream.LoadJsonlGz<T>();
    }
  }

  public class StoreFileMd {
    public StoreFileMd(StringPath path, string ts, DateTime modified, long bytes, string version = "0") {
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

    public static StringPath FilePath(StringPath path, string ts, string version) =>
      path.Add(FileName(ts, version));

    public static string FileName(string ts, string version) =>
      $"{ts}.{version}.{GuidExtensions.NewShort()}.jsonl.gz";

    public static string GetTs(StringPath path) => path.Name.Split(".").FirstOrDefault();
  }
}