using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Blob {
  public class OptimiseCfg {
    public long TargetBytes { get; set; } = (long) 200.Megabytes().Bytes;
    public int  Parallel    { get; set; } = 8;
  }

  public static class JsonlStoreExtensions {
    public const string Extension = "jsonl.gz";

    public static async IAsyncEnumerable<IReadOnlyCollection<StoreFileMd>> Files(this ISimpleFileStore store, StringPath path, bool allDirectories = false) {
      await foreach (var p in store.List(path, allDirectories))
        yield return p
          .Where(p => !p.Path.Name.StartsWith("_") && p.Path.Name.EndsWith(Extension))
          .Select(StoreFileMd.FromFileItem).ToArray();
    }

    public static StringPath FilePath(StringPath path, string ts, string version = null) =>
      path.Add(FileName(ts, version));

    public static string FileName(string ts, string version) =>
      $"{ts}.{version ?? ""}.{ShortGuid.Create(6)}.{Extension}";

    public static Task
      Optimise(this IJsonlStore store, OptimiseCfg cfg, StringPath partition, string ts = null, ILogger log = null) =>
      store.Store.Optimise(cfg, partition != null ? store.Path.Add(partition) : store.Path, ts, log);

    /// <summary>Process new files in land into stage. Note on the different modes: - LandAndStage: Immutable operation, load
    ///   data downstream from the stage directory - Default: Mutates the files in the same directory. Downstream operations:
    ///   don't run while processing, and fully reload after this is complete</summary>
    /// <returns>stats about the optimisation, and all new files (optimised or not) based on the timestamp</returns>
    public static async Task
      Optimise(this ISimpleFileStore store, OptimiseCfg cfg, StringPath rootPath, string ts = null, ILogger log = null) {
      // all partition landing files (will group using directories)
      var sw = Stopwatch.StartNew();

      log?.Debug("Optimise {Path} - reading current files", rootPath);
      var (byDir, duration) = await ToOptimiseByDir(store, rootPath, ts).WithDuration();
      log?.Debug("Optimise {Path} - read {Files} files across {Partitions} partitions in {Duration}",
        rootPath, byDir.Sum(p => p.Count()), byDir.Length, duration.HumanizeShort());

      var optimiseRes = await byDir.BlockFunc(p => Optimise(store, p.Key, p, cfg, log), cfg.Parallel);
      var optimiseIn = optimiseRes.Sum(r => r.optimisedIn);
      var optimiseOut = optimiseRes.Sum(r => r.optimisedOut);

      log?.Information("Optimise {Path} - optimised {FilesIn} into {FilesOut} files in {Duration}",
        rootPath, optimiseIn, optimiseOut, sw.Elapsed.HumanizeShort());
    }

    /// <summary>Join ts (timestamp) contiguous files together until they are > MaxBytes</summary>
    public static async Task<(long optimisedIn, long optimisedOut)> Optimise(this ISimpleFileStore store, StringPath destPath, IEnumerable<StoreFileMd> files,
      OptimiseCfg cfg, ILogger log) {
      var toProcess = files.OrderBy(f => f.Ts).ToQueue();

      log.Debug("Optimise {Path} - Processing {Files} files in partition {Partition}",
        destPath, toProcess.Count, destPath);

      var currentBatch = new List<StoreFileMd>();
      var optimisePlan = new List<StoreFileMd[]>();

      if (toProcess.None()) return (0, 0);

      while (toProcess.Any()) {
        var file = toProcess.Dequeue();

        var (nextBytes, nextIsFurther) = BatchSize(file);
        if (nextBytes > cfg.TargetBytes && nextIsFurther) // if adding this file will make it too big, optimise the current batch as is
          PlanCurrentBatch();

        currentBatch.Add(file);
        if (toProcess.None() || currentBatch.Sum(f => f.Bytes) > cfg.TargetBytes) // join if big enough, or this is the last batch
          PlanCurrentBatch();
      }

      (long nextBytes, bool nextIsFurther) BatchSize(StoreFileMd file) {
        var bytes = currentBatch.Sum(f => f.Bytes);
        var nextBytes = bytes + file.Bytes;
        var nextIsFurther = nextBytes - cfg.TargetBytes > cfg.TargetBytes - bytes;
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
          var optimiseRes = await JoinFiles(store, batch, destPath, cfg.Parallel, log).WithDuration();
          log.Debug("Optimise {Path} - optimised file {OptimisedFile} from {FilesIn} in {Duration}. batch {Batch}/{Total}",
            destPath, optimiseRes.Result, batch.Length, optimiseRes.Duration.HumanizeShort(), i, optimisePlan.Count);
        }, cfg.Parallel);
      }
      return (optimisePlan.Sum(p => p.Length), optimisePlan.Count);
    }

    static async Task<StringPath> JoinFiles(ISimpleFileStore store, IReadOnlyCollection<StoreFileMd> toOptimise, StringPath destPath, int parallel,
      ILogger log) {
      var optimisedFile = FilePath(destPath, toOptimise.Last().Ts);
      using (var joinedStream = new MemoryStream()) {
        using (var zipWriter = new GZipStream(joinedStream, CompressionLevel.Optimal, true)) {
          var inStreams = await toOptimise.BlockFunc(async s => {
            var inStream = await store.Load(s.Path, log).WithDuration();
            log.Debug("Optimise {Path} - loaded file {SourceFile} to be optimised in {Duration}",
              destPath, s.Path, inStream.Duration.HumanizeShort());
            return inStream.Result;
          }, parallel);
          foreach (var s in inStreams) {
            using var zr = new GZipStream(s, CompressionMode.Decompress, false);
            await zr.CopyToAsync(zipWriter);
          }
        }
        joinedStream.Seek(0, SeekOrigin.Begin);
        await store.Save(optimisedFile, joinedStream);
      }

      // when in-place, this is dirty if we fail now. There is no transaction capability in cloud storage, so downstream process must handle duplicates
      // successfully staged files, delete from land. Incremental using TS will work without delete, but it's more efficient to delete process landed files.
      await toOptimise.BlockAction(f => store.Delete(f.Path), parallel)
        .WithWrappedException(e => "Failed to delete optimised files. Duplicate records need to be handled downstream");
      log.Debug("Optimise {Path} - deleted {Files} that were optimised into {OptimisedFile}",
        destPath, toOptimise.Count, optimisedFile);

      return optimisedFile;
    }

    static async Task<IGrouping<string, StoreFileMd>[]> ToOptimiseByDir(ISimpleFileStore store, StringPath landPath, string ts) =>
      (await store.Files(landPath, allDirectories: true).SelectManyList())
      .Where(f => ts == null || string.CompareOrdinal(f.Ts, ts) > 0)
      .GroupBy(f => f.Path.Parent.ToString())
      .ToArray();
  }
}