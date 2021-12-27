using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Channels;
using Nito.AsyncEx;
using Open.ChannelExtensions;
using static SysExtensions.Threading.Def;

namespace Mutuo.Etl.Blob;

public record JsonSinkOptions {
  public int      BufferSize { get; init; } = 100_000;
  public TimeSpan FlushAfter { get; init; } = 5.Minutes();
}

public interface IJsonSink : IAsyncDisposable {
  Task Flush();
  bool  Started { get; }
  SPath Path    { get; }
}

public record JsonlSink<T> : RawJsonlStore, IJsonSink where T : class {
  readonly Func<T, string> GetTs;
  readonly JsonSinkOptions Options;

  /// <summary></summary>
  /// <param name="getTs">A function to get a timestamp for this file. This must always be greater for new records using an
  ///   invariant string comparer</param>
  public JsonlSink(ISimpleFileStore store, SPath path, Func<T, string> getTs, JsonSinkOptions options, ILogger log) : base(store, path, log) {
    GetTs = getTs;
    Log = log.Scope($"sink {path}");
    Options = options ?? new();
  }

  record SinkUpload(SPath Path, FPath LocalFile);

  readonly AsyncLock _pipeLock = new();
  SinkPipe           _pipe;

  // lazily start pipe when we first access it
  async Task<SinkPipe> Pipe() {
    if (_pipe != null) return _pipe;

    using (await _pipeLock.LockAsync()) { // use lock to prevent multiple new pipes being created concurrently
      if (_pipe != null) return _pipe; // check in case a pipe was already created
      _pipe = new(this);
      return _pipe;
    }
  }

  public bool Started => _pipe != null;

  public async Task Flush() {
    if (!Started) return;
    Log.Verbose("flush called waiting for process to finish");
    var oldSink = _pipe;
    _pipe = null;
    await oldSink.DisposeAsync(); // dispose/flush the old one and wait for it to finnish
  }

  public async ValueTask DisposeAsync() {
    if (!Started) return;
    await _pipe.DisposeAsync();
  }

  #region Store

  public IAsyncEnumerable<IReadOnlyCollection<T>> Items(string partition = null) => Files(partition, allDirectories: true)
    .SelectMany(dir => dir.BlockDo(f => LoadJsonl(f.Path), Parallel, capacity: 10));

  async Task<IReadOnlyCollection<T>> LoadJsonl(SPath path) {
    await using var stream = await Store.Load(path);
    return stream.LoadJsonlGz<T>(IJsonlStore.JCfg);
  }

  public async Task Append(IEnumerable<T> items) => await Append(items.NotNull().ToArray());

  public async Task Append(params T[] items) {
    if (items.Any()) {
      var pipe = await Pipe();
      await pipe.Append(items.NotNull().ToArray());
    }
  }

  /// <summary>Errors the running pipe if it exists</summary>
  public void Error(Exception ex) => _pipe?.Error(ex);

  #endregion

  #region SinkPipe

  /// <summary>Manages the pipeline of appending channel vs via a local file to uploading to blog storage. NOTE: an instance
  ///   inside JsonlSink because we want to flush and be able to wait for the pipeline to finnish</summary>
  record SinkPipe : IAsyncDisposable {
    readonly JsonlSink<T> Sink;
    readonly ILogger      Log;
    /// <summary>Producer/consumer to append rows to local files</summary>
    readonly Channel<IReadOnlyCollection<T>>
      AppendChan = Channel.CreateBounded<IReadOnlyCollection<T>>(new BoundedChannelOptions(10) { SingleReader = true });
    readonly Task ProcessTask;
    /// <summary>Producer/consumer channel to upload files ot blob storage</summary>
    readonly Channel<SinkUpload> UploadChan = Channel.CreateBounded<SinkUpload>(4);
    // local files are uploaded concurrently
    Task<long> UploadTask { get; }
    /// <summary>When cancelled, will write the current local sink file to blob storage</summary>
    CancellationTokenSource FlushCancel { get; } = new();

    public SinkPipe(JsonlSink<T> sink) {
      Sink = sink;
      Log = sink.Log;
      ProcessTask = Task.Factory.StartNew(ProcessPipeline, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
      UploadTask = UploadChan.ReadAllConcurrentlyAsync(maxConcurrency: 4, ReadUpload);
    }

    public async ValueTask DisposeAsync() {
      Log.Verbose("dispose called");
      AppendChan.Writer.Complete();
      Log.Verbose("waiting on append task");
      await ProcessTask;
      Log.Verbose("waiting on upload task");
      await 5.Seconds().Delay();
      await UploadTask;
      Log.Verbose("complete");
    }

    SPath           Path    => Sink.Path;
    JsonSinkOptions Options => Sink.Options;
    string          Scope   => $"JsonlSink {Path}";

    public async Task Append(params T[] items) {
      if (ProcessEx != null) throw ProcessEx;
      if (ProcessTask.IsFaulted) throw ProcessTask.Exception ?? new("ProcessTask faulted but without exception");
      await AppendChan.Writer.WriteAsync(items);
    }

    public void Error(Exception ex) {
      UploadChan.Writer.Complete(ex);
      ProcessEx = ex;
    }

    Exception ProcessEx;

    /// <summary>Processes appended items into local files which are then uploaded</summary>
    async ValueTask ProcessPipeline() {
      try {
        Log.Verbose("starting sink process {Path}, {@Options}", Path.ToString(), Options);
        var localDir = TempDir();
        while (await AppendChan.Reader.WaitToReadAsync()) {
          var added = 0;
          string maxTs = null;
          var localFile = localDir.Combine($"{ShortGuid.Create(8)}.jsonl.gz");
          using (var fw = File.OpenWrite(localFile.FullPath))
          using (var zipWriter = new GZipStream(fw, CompressionLevel.Optimal, leaveOpen: true)) {
            // don't fail for invalid UTF8, replace and continue
            var encoding = Encoding.GetEncoding("utf-8", new EncoderReplacementFallback("*"), new DecoderExceptionFallback());
            using (var tw = new StreamWriter(zipWriter, encoding)) {
              var fileSw = Stopwatch.StartNew();
              Log.Debug("created local sink file {File}", localFile.FullPath);
              while (true) {
                // timeout for timer-flushing this file
                Log.Verbose("append {File} - waiting for more rows or cancel/flush", localFile.FileNameWithoutExtension);
                var (waitToReadCompleted, hasItems) = await AppendChan.Reader.WaitToReadAsync()
                  .AsTask().WithTimeout(Options.FlushAfter - fileSw.Elapsed, FlushCancel.Token);
                var completed = waitToReadCompleted && !hasItems;
                var cancelOrFlushTimeout = !waitToReadCompleted;
                Log.Verbose("append {File} - continuing ({Status})",
                  localFile.FileNameWithoutExtension, completed ? "completed" : hasItems ? "more items" : "timeout");
                if (completed) break; // AppendChan is done, no more items ever.  checkCompleted is used because the call could have timed out

                while (AppendChan.Reader.TryRead(out var rows)) {
                  if (!rows.Any()) continue;
                  added += rows.Count;
                  foreach (var item in rows) {
                    await tw!.WriteLineAsync(item.ToJson(IJsonlStore.JCfg));
                    maxTs = MaxTs(maxTs, Sink.GetTs(item));
                  }
                  Log.Verbose(" appended rows to local file {File}: {Rows} rows, {Ts} ts", localFile.FileNameWithoutExtension, added, maxTs);
                }

                if (added >= Options.BufferSize || cancelOrFlushTimeout) break;
              }
            }
          }
          Log.Debug("completed local file {File}: {Rows} rows, {Ts} ts", localFile.FileNameWithoutExtension, added, maxTs);

          if (added > 0)
            await UploadChan.Writer.WriteAsync(new(JsonlStoreExtensions.FilePath(Path, maxTs, Sink.Version), localFile));
        }
        UploadChan.Writer.Complete(); // we hae finished writing everything, cascae completion
      }
      catch (Exception ex) {
        ex = ex.Unwrap();
        Log.Error(ex, "process failed no more files will be written: {Error}", ex.Message);
        Error(ex);
        throw;
      }
    }

    async ValueTask ReadUpload(SinkUpload up) {
      Log.Verbose("starting to uploaded blob file {Path}", up.Path.ToString());
      await Sink.Store.Save(up.Path, up.LocalFile, Log);
      Log.Debug("uploaded blob file {Path}", up.Path.ToString());
      Fun(() => up.LocalFile?.Delete()).Try();
    }

    static FPath TempDir() {
      var path = System.IO.Path.GetTempPath().AsFPath().Combine("recfluence", "sink", ShortGuid.Create());
      if (!path.Exists)
        path.CreateDirectory();
      return path;
    }

    static string MaxTs(string max, string ts) => max == null || string.CompareOrdinal(ts, max) > 0 ? ts : max;
  }

  #endregion
}