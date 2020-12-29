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
using SysExtensions.IO;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Blob {
  public class OptimiseCfg {
    public long TargetBytes     { get; set; } = (long) 200.Megabytes().Bytes;
    public int  ParallelFiles   { get; set; } = 4;
    public int  ParallelBatches { get; set; } = 3;
  }

  public record OptimiseBatch {
    public StoreFileMd[]     Files { get; set; }
    public StringPath Dest  { get; set; }
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

      var optimiseRes = await byDir.BlockFunc(p => Optimise(store, p.Key, p, cfg, log), cfg.ParallelFiles);
      var optimiseIn = optimiseRes.Sum(r => r.optimisedIn);
      var optimiseOut = optimiseRes.Sum(r => r.optimisedOut);

      log?.Information("Optimise {Path} - optimised {FilesIn} into {FilesOut} files in {Duration}",
        rootPath, optimiseIn, optimiseOut, sw.Elapsed.HumanizeShort());
    }

    /// <summary>Process new files in land into stage. Note on the different modes: - LandAndStage: Immutable operation, load
    ///   data downstream from the stage directory - Default: Mutates the files in the same directory. Downstream operations:
    ///   don't run while processing, and fully reload after this is complete</summary>
    /// <returns>stats about the optimisation, and all new files (optimised or not) based on the timestamp</returns>
    public static async Task<IReadOnlyCollection<OptimiseBatch>> OptimisePlan(this ISimpleFileStore store, OptimiseCfg cfg, StringPath rootPath, string ts = null, ILogger log = null) {
      log?.Debug("Optimise {Path} - reading current files", rootPath);
      var (byDir, duration) = await ToOptimiseByDir(store, rootPath, ts).WithDuration();
      log?.Debug("Optimise {Path} - read {Files} files across {Partitions} partitions in {Duration}",
        rootPath, byDir.Sum(p => p.Count()), byDir.Length, duration.HumanizeShort());
      return byDir.SelectMany(p => OptimisePlan(p.Key, p, cfg, log)).ToArray();
    }

    public static IReadOnlyCollection<OptimiseBatch> OptimisePlan(StringPath destPath, IEnumerable<StoreFileMd> files, OptimiseCfg cfg, ILogger log) {
      var toProcess = files.OrderBy(f => f.Ts).ToQueue();

      log.Debug("Optimise {Path} - Processing {Files} files in partition {Partition}",
        destPath, toProcess.Count, destPath);

      var currentBatch = new List<StoreFileMd>();
      var optimisePlan = new List<StoreFileMd[]>();

      if (toProcess.None()) return default;

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
        if (currentBatch.Count > 1) // only plan a batch if there is more than one file Vin it
          optimisePlan.Add(currentBatch.ToArray());
        currentBatch.Clear();
      }

      return optimisePlan.Select(p => new OptimiseBatch { Dest = destPath, Files = p}).ToArray();
    }

    /// <summary>Join ts (timestamp) contiguous files together until they are > MaxBytes</summary>
    public static async Task<(long optimisedIn, long optimisedOut)> Optimise(this ISimpleFileStore store, StringPath destPath, IEnumerable<StoreFileMd> files,
      OptimiseCfg cfg, ILogger log) {
      var plan = OptimisePlan(destPath, files, cfg, log);
      if (plan.None()) {
        log.Debug("Optimise {Path} - already optimal", destPath);
      }
      else {
        log.Debug("Optimise {Path} - starting to execute optimisation plan", destPath);
        await Optimise(store, cfg, plan, log);
      }
      return (plan.Sum(p => p.Files.Length), plan.Count);
    }

    public static async Task Optimise(this ISimpleFileStore store, OptimiseCfg cfg, IReadOnlyCollection<OptimiseBatch> plan, ILogger log) =>
      await plan.Select((b, i) => (b, i)).BlockAction(async b => {
        var (batch, i) = b;
        var optimiseRes = await JoinFiles(store, batch.Files, batch.Dest, cfg.ParallelFiles, log).WithDuration();
        log.Debug("Optimise {Path} - optimised file {OptimisedFile} from {FilesIn} in {Duration}. batch {Batch}/{Total}",
          batch.Dest, optimiseRes.Result, batch.Files.Length, optimiseRes.Duration.HumanizeShort(), i, plan.Count);
      }, cfg.ParallelBatches);

    static async Task<StringPath> JoinFiles(ISimpleFileStore store, IReadOnlyCollection<StoreFileMd> toOptimise, StringPath destPath, int parallel,
      ILogger log) {
      var optimisedFileName = FilePath(destPath, toOptimise.Last().Ts);
      var localTmpDir = Path.GetTempPath().AsPath().Combine("Mutuo.Etl", "JoinFiles", ShortGuid.Create(8));

      localTmpDir.EnsureDirectoryExists();

      var inFiles = await toOptimise.BlockFunc(async s => {
        var inPath = localTmpDir.Combine($"{ShortGuid.Create(6)}.{s.Path.Name}"); // flatten dir structure locally. ensure unique with GUID
        var dur = await store.LoadToFile(s.Path, inPath, log).WithDuration();
        log.Debug("Optimise {Path} - loaded file {SourceFile} for optimisation in {Duration}", destPath, s.Path, dur.HumanizeShort());
        return inPath;
      }, parallel);

      var outFile = localTmpDir.Combine($"out.{optimisedFileName.Name}");

      using (var fileStream = outFile.Open(FileMode.Create)) {
        using var zipWriter = new GZipStream(fileStream, CompressionLevel.Optimal, leaveOpen: false);
        foreach (var s in inFiles) {
          using var zr = new GZipStream(s.Open(FileMode.Open), CompressionMode.Decompress, leaveOpen: false);
          await zr.CopyToAsync(zipWriter);
        }
      }

      log.Debug("Optimise {Path} - joined {SourceFiles} source files. About to upload", destPath, inFiles.Count);
      await store.Save(optimisedFileName, outFile);
      log.Debug("Optimise {Path} - saved optimised file {OptimisedFile}", destPath, optimisedFileName);

      // when in-place, this is dirty if we fail now. There is no transaction capability in cloud storage, so downstream process must handle duplicates
      // successfully staged files, delete from land. Incremental using TS will work without delete, but it's more efficient to delete process landed files.
      await toOptimise.BlockAction(f => store.Delete(f.Path), parallel)
        .WithWrappedException(e => "Failed to delete optimised files. Duplicate records need to be handled downstream");
      log.Debug("Optimise {Path} - deleted {Files} that were optimised into {OptimisedFile}", destPath, toOptimise.Count, optimisedFileName);

      localTmpDir.Delete(recursive: true); // delete local tmp files
      return optimisedFileName;
    }

    static async Task<IGrouping<string, StoreFileMd>[]> ToOptimiseByDir(ISimpleFileStore store, StringPath landPath, string ts) =>
      (await store.Files(landPath, allDirectories: true).SelectManyList())
      .Where(f => ts == null || string.CompareOrdinal(f.Ts, ts) > 0)
      .GroupBy(f => f.Path.Parent.ToString())
      .ToArray();
  }
}