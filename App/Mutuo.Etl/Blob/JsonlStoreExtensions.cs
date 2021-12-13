using System.Diagnostics;
using System.IO;
using System.IO.Compression;

namespace Mutuo.Etl.Blob; 

public class OptimiseCfg {
  public long TargetBytes     { get; set; } = (long)200.Megabytes().Bytes;
  public int  ParallelFiles   { get; set; } = 4;
  public int  ParallelBatches { get; set; } = 3;
}

public record OptimiseBatch {
  public OptimiseBatch() { }

  public OptimiseBatch(SPath destFile, StoreFileMd[] files) {
    DestFile = destFile;
    Files = files;
  }

  public SPath         DestFile { get; init; }
  public StoreFileMd[] Files    { get; init; }

  /// <summary>number of files un-optimized</summary>
  public int SourceFileCount => (Files?.Length ?? 0).Max(1);
}

public static class JsonlStoreExtensions {
  public const string Extension = "jsonl.gz";

  public static async IAsyncEnumerable<IReadOnlyCollection<StoreFileMd>>
    JsonStoreFiles(this ISimpleFileStore store, SPath path, bool allDirectories = false) {
    var allFiles = await store.List(path, allDirectories).ToArrayAsync();
    foreach (var b in allFiles.Select(b => b
                 .Where(p => !p.Path.Name.StartsWith("_") && p.Path.Name.EndsWith(Extension))
                 .Select(StoreFileMd.FromFileItem).ToArray())
               .Select(dummy => (IReadOnlyCollection<StoreFileMd>)dummy))
      yield return b;
  }

  public static SPath FilePath(SPath path, string ts, string version = null) =>
    path.Add(FileName(ts, version));

  public static string FileName(string ts, string version) =>
    $"{ts}.{version ?? ""}.{ShortGuid.Create(6)}.{Extension}";

  public static Task
    Optimise(this IJsonlStore store, OptimiseCfg cfg, SPath partition, string ts = null, ILogger log = null) =>
    store.Store.Optimise(cfg, partition != null ? store.Path.Add(partition) : store.Path, ts, log);

  /// <summary>Process new files in land into stage. Note on the different modes: - LandAndStage: Immutable operation, load
  ///   data downstream from the stage directory - Default: Mutates the files in the same directory. Downstream operations:
  ///   don't run while processing, and fully reload after this is complete</summary>
  /// <returns>stats about the optimisation, and all new files (optimised or not) based on the timestamp</returns>
  public static async Task
    Optimise(this ISimpleFileStore store, OptimiseCfg cfg, SPath rootPath, string ts = null, ILogger log = null) {
    // all partition landing files (will group using directories)
    var sw = Stopwatch.StartNew();

    log?.Debug("Optimise {Path} - reading current files", rootPath);
    var (byDir, duration) = await ToOptimiseByDir(store, rootPath, ts).WithDuration();
    log?.Debug("Optimise {Path} - read {Files} files across {Partitions} partitions in {Duration}",
      rootPath, byDir.Sum(p => p.Count()), byDir.Length, duration.HumanizeShort());

    var optimiseRes = await byDir.BlockDo(p => Optimise(store, p.Key, p, cfg, log), cfg.ParallelFiles).ToArrayAsync();
    var optimiseIn = optimiseRes.Sum(r => r.optimisedIn);
    var optimiseOut = optimiseRes.Sum(r => r.optimisedOut);

    log?.Information("Optimise {Path} - optimised {FilesIn} into {FilesOut} files in {Duration}",
      rootPath, optimiseIn, optimiseOut, sw.Elapsed.HumanizeShort());
  }

  /// <summary>Process new files in land into stage. Note on the different modes: - LandAndStage: Immutable operation, load
  ///   data downstream from the stage directory - Default: Mutates the files in the same directory. Downstream operations:
  ///   don't run while processing, and fully reload after this is complete</summary>
  /// <returns>stats about the optimisation, and all new files (optimised or not) based on the timestamp</returns>
  public static async Task<IReadOnlyCollection<OptimiseBatch>> OptimisePlan(this ISimpleFileStore store, OptimiseCfg cfg, SPath rootPath,
    string ts = null, ILogger log = null) {
    log?.Debug("Optimise {Path} - reading current files", rootPath);
    var (byDir, duration) = await ToOptimiseByDir(store, rootPath, ts).WithDuration();
    log?.Debug("Optimise {Path} - read {Files} files across {Partitions} partitions in {Duration}",
      rootPath, byDir.Sum(p => p.Count()), byDir.Length, duration.HumanizeShort());
    return byDir.SelectMany(p => OptimisePlan(p, p.Key, cfg)).ToArray();
  }

  public static IReadOnlyCollection<OptimiseBatch> OptimisePlan(this IEnumerable<StoreFileMd> files, SPath destPath, OptimiseCfg cfg) {
    var toProcess = files.OrderBy(f => f.Ts).ToQueue();

    var batch = new List<StoreFileMd>();
    var optimisePlan = new List<OptimiseBatch>();

    if (toProcess.None()) return Array.Empty<OptimiseBatch>();

    while (toProcess.Any()) {
      var file = toProcess.Dequeue();

      var (nextBytes, nextIsFurther) = BatchSize(file);
      if (nextBytes > cfg.TargetBytes && nextIsFurther) // if adding this file will make it too big, optimise the current batch as is
        PlanCurrentBatch();

      batch.Add(file);
      if (toProcess.None() || batch.Sum(f => f.Bytes) > cfg.TargetBytes) // join if big enough, or this is the last batch
        PlanCurrentBatch();
    }

    (long nextBytes, bool nextIsFurther) BatchSize(StoreFileMd file) {
      var bytes = batch.Sum(f => f.Bytes);
      var nextBytes = bytes + file.Bytes;
      var nextIsFurther = nextBytes - cfg.TargetBytes > cfg.TargetBytes - bytes;
      if (!nextBytes.HasValue) throw new InvalidOperationException($"Optimisation requires the file bytes are know. Missing on file {file.Path}");
      return (nextBytes.Value, nextIsFurther);
    }

    void PlanCurrentBatch() {
      OptimiseBatch batchPlan = batch.Count switch {
        1 => new(batch.First().Path, Array.Empty<StoreFileMd>()), // leave file as is
        > 1 => new(FilePath(destPath, batch.Last().Ts), batch.ToArray()),
        _ => null
      };
      if (batchPlan != null)
        optimisePlan.Add(batchPlan);
      batch.Clear();
    }

    return optimisePlan;
  }

  /// <summary>Join ts (timestamp) contiguous files together until they are > MaxBytes</summary>
  public static async Task<(long optimisedIn, long optimisedOut)> Optimise(this ISimpleFileStore store, SPath destPath, IEnumerable<StoreFileMd> files,
    OptimiseCfg cfg, ILogger log) {
    var plan = OptimisePlan(files, destPath, cfg);
    var (filesIn, filesOut) = (plan.Sum(p => p.SourceFileCount), plan.Count);
    if (plan.None()) {
      log.Debug("Optimise {Path} - already optimal", destPath);
    }
    else {
      log?.Debug("Optimise {Path} - starting optimization of {FilesIn} to {FilesOut} files", destPath, filesIn, filesOut);
      await Optimise(store, cfg, plan, log);
    }
    return (filesIn, filesOut);
  }

  public static async Task Optimise(this ISimpleFileStore store, OptimiseCfg cfg, IReadOnlyCollection<OptimiseBatch> plan, ILogger log) =>
    await plan.Select((b, i) => (b, i)).BlockDo(async b => {
      var (batch, i) = b;
      if (batch.Files.Any()) {
        var dur = await JoinFiles(store, batch, cfg.ParallelFiles, log).WithDuration();
        log.Debug("Optimise {Path} - optimised file {OptimisedFile} from {FilesIn} in {Duration}. batch {Batch}/{Total}",
          batch.DestFile, batch.DestFile, batch.Files.Length, dur.HumanizeShort(), i, plan.Count);
      }
    }, cfg.ParallelBatches);

  static async Task JoinFiles(ISimpleFileStore store, OptimiseBatch batch, int parallel, ILogger log) {
    if (batch.Files.Length <= 1) return;

    var localTmpDir = Path.GetTempPath().AsFPath().Combine("Recfluence", "JoinFiles", ShortGuid.Create(8));
    localTmpDir.EnsureDirectoryExists();

    var destDir = batch.DestFile.Parent;
    var inFiles = await batch.Files.BlockDo(async s => {
      var inPath = localTmpDir.Combine($"{ShortGuid.Create(6)}.{s.Path.Name}"); // flatten dir structure locally. ensure unique with GUID
      var dur = await store.LoadToFile(s.Path, inPath, log).WithDuration();
      log.Debug("Optimise {Path} - loaded file {SourceFile} for optimisation in {Duration}", destDir, s.Path, dur.HumanizeShort());
      return inPath;
    }, parallel).ToListAsync();

    var outFile = localTmpDir.Combine($"out.{batch.DestFile.Name}");

    using (var fileStream = outFile.Open(FileMode.Create)) {
      using var zipWriter = new GZipStream(fileStream, CompressionLevel.Optimal, leaveOpen: false);
      foreach (var s in inFiles) {
        using var zr = new GZipStream(s.Open(FileMode.Open), CompressionMode.Decompress, leaveOpen: false);
        await zr.CopyToAsync(zipWriter);
      }
    }

    log.Debug("Optimise {Path} - joined {SourceFiles} source files. About to upload", destDir, inFiles.Count);
    await store.Save(batch.DestFile, outFile);
    log.Debug("Optimise {Path} - saved optimised file {OptimisedFile}", destDir, batch.DestFile);

    await batch.Files.BlockDo<StoreFileMd>(f => store.Delete(f.Path), parallel)
      .WithWrappedException(e => "Failed to delete optimised files. Duplicate records need to be handled downstream");
    log.Debug("Optimise {Path} - deleted {Files} that were optimised into {OptimisedFile}", destDir, batch.Files, batch.DestFile);
    localTmpDir.Delete(recursive: true); // delete local tmp files
  }

  static async Task<IGrouping<string, StoreFileMd>[]> ToOptimiseByDir(ISimpleFileStore store, SPath landPath, string ts) =>
    (await store.JsonStoreFiles(landPath, allDirectories: true).SelectManyList())
    .Where(f => ts == null || string.CompareOrdinal(f.Ts, ts) > 0)
    .GroupBy(f => f.Path.Parent.ToString())
    .ToArray();
}